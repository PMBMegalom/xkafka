/*
 * Copyright (c) 2026 xkafka contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package xkafka
package internal
package streams

import cats.MonadThrow
import cats.syntax.all.*
import fs2.Chunk
import cats.data.Ior
import xkafka.streams.{
  Chain, End, Flow, Joiner, Origin, Release, Segment, Side, Source, Step, StreamJoin, TableKey, TableValue, TableView, Window, Windowed
}

/** A record as a topic holds it, read by a task. */
private[xkafka] final case class TaskInput(
    key: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]],
    timestamp: Long,
    headers: Headers,
    topic: String = "",
    partition: Int = -1,
    offset: Long = -1L
)

/** A record a task writes: to `partition` where one is given, as a changelog is, to the partition the Java producer picks for `partitionBy` where
  * that is given, as for a windowed key, which Java partitions by the key alone, and otherwise wherever the producer's partitioner puts its key.
  */
private[xkafka] final case class TaskOutput(
    topic: Topic,
    partition: Option[Int],
    key: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]],
    timestamp: Long,
    headers: Headers,
    partitionBy: Option[Chunk[Byte]] = None
)

/** The latest value a store holds for a key, and its timestamp, as the bytes its changelog carries. */
private[xkafka] final case class Stored(value: Chunk[Byte], timestamp: Long)

/** What a task keeps between records: each store's contents by serialized key, the partition time, the largest timestamp it has read, and for each
  * window store, the stream time its windowing step has seen, which starts again on each member as Java's does, the store's clock (see
  * [[WindowStore]]), taken from its contents where the task has not yet kept it, and where its step emits windows once they close, the close time it
  * last emitted for, committed with the task's offsets, and the wall-clock time before which it does not look for closed windows again.
  */
private[xkafka] final case class TaskState(
    stores: Map[String, Map[Chunk[Byte], Stored]],
    partitionTimes: Map[String, Long],
    streamTimes: Map[String, Long] = Map.empty,
    clocks: Map[String, Long] = Map.empty,
    lastEmitted: Map[String, Long] = Map.empty,
    nextEmit: Map[String, Long] = Map.empty,
    dirty: Map[String, Dirty] = Map.empty,
    buffers: Map[String, Buffered] = Map.empty,
    sequences: Map[String, Int] = Map.empty,
    joinTimes: Map[String, JoinTime] = Map.empty,
    reading: Option[TaskInput] = None
):
  def store(name: String): Map[Chunk[Byte], Stored] = stores.getOrElse(name, Map.empty)

  /** The partition time of the task's partition of `topic`: the largest timestamp read from it, or -1 before any. */
  def partitionTime(topic: String): Long = partitionTimes.getOrElse(topic, -1L)

/** The times a left or outer stream-stream join keeps for a task, in memory only, as Java's join keeps them and starts again on each member: the
  * latest record time either side has read, the earliest time of a record held as matching nothing since the join last looked for those whose windows
  * have closed, and the wall-clock time before which it does not look again.
  */
private[xkafka] final case class JoinTime(streamTime: Long, minTime: Long, nextEmit: Long)

private[xkafka] object JoinTime:
  /** Java's times before any record: no stream time, no record held, and a look at the first chance. */
  val initial: JoinTime = JoinTime(WindowStore.NoTime, Long.MaxValue, 0L)

  /** How long Java waits between looks, by the wall clock, by default (`__emit.interval.ms.kstreams.outer.join.spurious.results.fix__`). */
  val EmitInterval: Long = 1000L

private[xkafka] object TaskState:
  /** No store holds anything yet, and no partition time is known. */
  val empty: TaskState = TaskState(Map.empty, Map.empty)

/** One sub-topology on one partition: what it writes for each record it reads, as Java Streams writes it, without Java's record cache, so every
  * change to a store goes to its changelog and on downstream.
  */
private[xkafka] final class Task[F[_]](application: String, segment: Segment[F], partition: Int)(using F: MonadThrow[F]):
  /** A record going from step to step; a table's change also carries the value it replaced, where Java passes one on, and a change a suppression
    * held, where it came from, which Java passes on with it.
    */
  private final case class Travelling[K, V](
      key: K,
      value: V,
      timestamp: Long,
      headers: Headers,
      old: Option[V] = None,
      origin: Option[HeldContext] = None
  )

  /** The topics each of the sub-topology's segments reads, with the segment. */
  private val readers: Map[String, Segment[F]] = segment.all.flatMap(read => read.origin.sources.map(Task.topicOf(application, _) -> read)).toMap

  /** Runs `input` through the segment that reads its topic at wall-clock time `now`, in epoch milliseconds. */
  def process(state: TaskState, input: TaskInput, now: Long): F[(TaskState, Vector[TaskOutput])] =
    val times = state.partitionTimes.updated(input.topic, math.max(state.partitionTime(input.topic), input.timestamp))
    val read  = state.copy(partitionTimes = times, reading = Some(input))
    // A record read outside the engine names no topic, and goes to the segment the sub-topology is named after.
    segmentOf(readers.getOrElse(input.topic, segment), read, input, now)

  /** What the task's suppression buffers write to their changelogs at a commit, as Java's buffer logs on commit: each key changed since the last
    * commit, with what it holds and the header `v=3`, or a tombstone where it no longer holds the key. The changelog's broker gives each its time.
    */
  def flush(state: TaskState): F[(TaskState, Vector[TaskOutput])] =
    segment.buffers.toList.sorted.flatTraverse: store =>
      topicNamed(s"$application-$store-changelog").map: changelog =>
        state.dirty.getOrElse(store, Dirty.empty).ordered.toList.map: key =>
          state.store(store).get(key) match
            case Some(held) => TaskOutput(changelog, Some(partition), Some(key), Some(held.value), Task.NoTimestamp, Held.Version)
            case None       => TaskOutput(changelog, Some(partition), Some(key), None, Task.NoTimestamp, Headers.empty)
    .map(written => (state.copy(dirty = state.dirty.view.mapValues(_.cleared).toMap), written.toVector))

  private def segmentOf(segment: Segment[F], state: TaskState, input: TaskInput, now: Long): F[(TaskState, Vector[TaskOutput])] =
    for
      fedIn <- fed(segment.origin, input, state, now, Task.requests(segment.chain, false))
      (read, records, logged) = fedIn
      ran <- chained(segment.chain, records, read, now)
      (after, out, written) = ran
      ended <- out.flatTraverse(leaving(segment.end, _))
    yield (after, logged ++ written ++ ended)

  /** The records `origin` gives for `input`: the record read, or for tables joined, what the join makes of the change the record makes to one side.
    * `requested` says whether what reads them asks for old values, as Java's processors after a table ask with `enableSendingOldValues`.
    */
  private def fed[K, V](
      origin: Origin[F, K, V],
      input: TaskInput,
      state: TaskState,
      now: Long,
      requested: Boolean
  ): F[(TaskState, Vector[Travelling[K, V]], Vector[TaskOutput])] =
    def read(topic: F[Topic], key: xkafka.streams.Serde[F, K], value: xkafka.streams.Serde[F, V]) =
      for
        source <- topic
        k      <- key.deserializer.deserialize(source, input.headers, input.key)
        v      <- value.deserializer.deserialize(source, input.headers, input.value)
      yield (state, Vector(Travelling(k, v, input.timestamp, input.headers)), Vector.empty[TaskOutput])
    origin match
      case Origin.Input(topic, key, value)                        => read(F.pure(topic), key, value)
      case Origin.Repartitioned(repartition, key, value)          => read(topicNamed(s"$application-${repartition.name}-repartition"), key, value)
      case Origin.Joined(left, right, joiner, store)              => joinedFed(left, right, joiner, store, input, state, now, requested)
      case joining @ Origin.StreamsJoined(_, _, _, _, _, _, _, _) => streamsJoined(joining, input, state, now)

  /** Runs each of `records` through `steps`, in order. */
  private def chained[K, V, K2, V2](
      steps: Chain[F, K, V, K2, V2],
      records: Vector[Travelling[K, V]],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])] =
    records.foldLeftM((state, Vector.empty[Travelling[K2, V2]], Vector.empty[TaskOutput])):
      case ((current, done, out), record) => chain(steps, record, current, now).map((after, more, logged) => (after, done ++ more, out ++ logged))

  /** The changes one side of a join makes for `input`, read through its steps. Each keeps its old value: a join reads one only where Java's side
    * passes it on, since it reads it only for a side its kind asks, or once what reads the join asks, which asks both sides.
    */
  private def sideFed[K, A, K0, V0, C](
      side: Side[F, K, A, K0, V0, C],
      input: TaskInput,
      state: TaskState,
      now: Long,
      requested: Boolean
  ): F[(TaskState, Vector[Task.Change[K, A]], Vector[TaskOutput])] =
    for
      fedIn <- fed(side.origin, input, state, now, Task.requests(side.chain, requested))
      (read, records, logged) = fedIn
      ran <- chained(side.chain, records, read, now)
      (after, changes, written) = ran
    yield (
      after,
      changes.map: change =>
        Task.Change(change.key, side.present(change.value), change.old.flatMap(side.present), change.timestamp, change.headers),
      logged ++ written
    )

  /** What two tables joined make of the change `input` makes to one side, as Java's join processors for that side make it, each looking up the other
    * side's value through its view, then, as Java's merger does, kept in the join's store where it has one, and passed on with an old value where
    * what reads the result asks for one.
    */
  private def joinedFed[K, A, B, R, K1, V1, C1, K2, V2, C2](
      left: Side[F, K, A, K1, V1, C1],
      right: Side[F, K, B, K2, V2, C2],
      joiner: Joiner[A, B, R],
      store: Option[(xkafka.streams.Store, xkafka.streams.Serde[F, K], xkafka.streams.Serde[F, R])],
      input: TaskInput,
      state: TaskState,
      now: Long,
      requested: Boolean
  ): F[(TaskState, Vector[Travelling[K, Option[R]]], Vector[TaskOutput])] =
    val (leftOld, rightOld) = joiner.outer
    val fromLeft            = left.origin.sources.exists(Task.topicOf(application, _) == input.topic)
    // Each change goes on as the result's new value, its old one, and its time, keyed and with the headers of the change, or not at all.
    val made: F[(TaskState, Vector[Travelling[K, Option[R]]], Vector[TaskOutput])] =
      def result(key: K, headers: Headers)(value: Option[R], old: Option[R], timestamp: Long) =
        Travelling[K, Option[R]](key, value, timestamp, headers, old.map(Some(_)))
      if fromLeft then
        sideFed(left, input, state, now, leftOld || requested).flatMap: (after, changes, written) =>
          changes.flatTraverse: change =>
            lookup(right.view, change.key, after)
              .map(Task.joinLeft(joiner, change, _, requested).map(result(change.key, change.headers).tupled).toVector)
          .map((after, _, written))
      else
        sideFed(right, input, state, now, rightOld || requested).flatMap: (after, changes, written) =>
          changes.flatTraverse: change =>
            lookup(left.view, change.key, after)
              .map(Task.joinRight(joiner, change, _, requested).map(result(change.key, change.headers).tupled).toVector)
          .map((after, _, written))
    made.flatMap: (after, results, written) =>
      results.foldLeftM((after, Vector.empty[Travelling[K, Option[R]]], written)):
        case ((current, out, logged), result) => store match
            case None                        => F.pure((current, out :+ result, logged))
            case Some((named, keys, values)) => topicNamed(s"$application-${named.name}-changelog").flatMap: changelog =>
                val timestamp = result.timestamp
                (
                  keys.serializer.serialize(changelog, Headers.empty, result.key),
                  result.value.flatTraverse(values.serializer.serialize(changelog, Headers.empty, _))
                ).tupled.map:
                  case (None, _)              => (current, out :+ result, logged)
                  case (Some(bytes), written) =>
                    val held     = current.store(named.name)
                    val contents = written.fold(held - bytes)(v => held.updated(bytes, Stored(v, timestamp)))
                    val log      = TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)
                    (current.copy(stores = current.stores.updated(named.name, contents)), out :+ result, logged :+ log)

  /** The records one side of a stream-stream join gives for `input`, read through its steps. */
  private def flowed[K, A, K0, V0](
      flow: Flow[F, K, A, K0, V0],
      input: TaskInput,
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K, A]], Vector[TaskOutput])] =
    for
      fedIn <- fed(flow.origin, input, state, now, Task.requests(flow.chain, false))
      (read, records, logged) = fedIn
      ran <- chained(flow.chain, records, read, now)
      (after, out, written) = ran
    yield (after, out, logged ++ written)

  /** One side of a stream-stream join as Java's processor for it sees it: its own window store and the other side's, the serdes of their values, how
    * far before and after a record's time it looks in the other side's store, whether its records that match nothing go on, whether it is the left
    * side, and what the joiner makes of a match and of a record that matches nothing.
    */
  private final case class JoinSide[X, Y, R](
      own: String,
      other: String,
      values: xkafka.streams.Serde[F, X],
      others: xkafka.streams.Serde[F, Y],
      before: Long,
      after: Long,
      outer: Boolean,
      left: Boolean,
      both: (X, Y) => R,
      alone: X => Option[R]
  )

  /** What two streams joined make of the records `input` gives one side, each as Java's join processor for that side makes it. */
  private def streamsJoined[K, A, B, R, K1, V1, K2, V2](
      join: Origin.StreamsJoined[F, K, A, B, R, K1, V1, K2, V2],
      input: TaskInput,
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    import join.{joiner, windows}
    val names                   = StreamJoin.names(join.store, joiner)
    val (leftOuter, rightOuter) = joiner.outer

    def each[X, Y](side: JoinSide[X, Y, R])(read: (TaskState, Vector[Travelling[K, X]], Vector[TaskOutput])) =
      val (after, records, written) = read
      records.foldLeftM((after, Vector.empty[Travelling[K, R]], written)):
        case ((current, out, logged), record) => joinedRecord(join, side, record, current, now)
            .map((next, more, log) => (next, out ++ more, logged ++ log))
    if join.left.origin.sources.exists(Task.topicOf(application, _) == input.topic) then
      val side =
        JoinSide(
          names.left,
          names.right,
          join.value,
          join.otherValue,
          windows.beforeMillis,
          windows.afterMillis,
          leftOuter,
          true,
          joiner.both,
          joiner.leftAlone
        )
      flowed(join.left, input, state, now).flatMap(each(side))
    else
      // Java's right processor looks the other way, so its before and after swap, and calls the joiner with its arguments reversed.
      val side =
        JoinSide[B, A, R](
          names.right,
          names.left,
          join.otherValue,
          join.value,
          windows.afterMillis,
          windows.beforeMillis,
          rightOuter,
          false,
          (b, a) => joiner.both(a, b),
          joiner.rightAlone
        )
      flowed(join.right, input, state, now).flatMap(each(side))

  /** What `side`'s processor makes of `record`, as Java's does (`KStreamKStreamJoin`): the join's stream time moves to the record's; a record whose
    * key is written as none goes on alone at once where the side's records that match nothing go on, and is otherwise dropped. A record at the stream
    * time first sends on the held records whose windows have closed. The record then joins each of the other side's records with its key in its
    * window, earliest first, each result at the later of their times, and each match no longer held as matching nothing; a record that matches
    * nothing, on a side whose such records go on, goes on alone at once if its window has already closed, or is held until it closes. Last, the
    * record goes in its side's window store, and to its changelog with its own time and headers.
    */
  private def joinedRecord[K, A, B, R, K1, V1, K2, V2, X, Y](
      join: Origin.StreamsJoined[F, K, A, B, R, K1, V1, K2, V2],
      side: JoinSide[X, Y, R],
      record: Travelling[K, X],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    val shared    = StreamJoin.names(join.store, join.joiner).shared
    val retention = join.windows.retentionMillis
    val timestamp = record.timestamp
    val ticked    =
      shared.fold(state): name =>
        val time = state.joinTimes.getOrElse(name, JoinTime.initial)
        state.copy(joinTimes = state.joinTimes.updated(name, time.copy(streamTime = math.max(time.streamTime, timestamp))))
    val streamTime                 = shared.flatMap(ticked.joinTimes.get).fold(WindowStore.NoTime)(_.streamTime)
    def result(value: R, at: Long) = Travelling(record.key, value, at, record.headers)
    def alone                      = side.alone(record.value).map(result(_, timestamp)).toVector
    (
      topicNamed(s"$application-${side.own}-changelog"),
      topicNamed(s"$application-${side.other}-changelog"),
      shared.traverse(name => topicNamed(s"$application-$name-changelog").map(name -> _))
    ).tupled.flatMap: (own, other, sharing) =>
      join.key.serializer.serialize(own, record.headers, record.key).flatMap:
        case None        => F.pure((ticked, if side.outer then alone else Vector.empty, Vector.empty))
        case Some(bytes) =>
          val from    = math.max(0L, timestamp - side.before)
          val to      = math.max(0L, timestamp + side.after)
          val emitted =
            sharing match
              case Some((name, log)) if timestamp == streamTime => nonJoined(join, name, log, side.after, record.headers, timestamp, ticked, now)
              case _                                            => F.pure((ticked, Vector.empty[Travelling[K, R]], Vector.empty[TaskOutput]))
          emitted.flatMap: (afterEmitting, sent, flushed) =>
            joinedOthers(afterEmitting, side.other, bytes, from, to)
              .traverse((at, held) => side.others.deserializer.deserialize(other, Headers.empty, Some(held.value)).map(at -> _)).flatMap: matches =>
                val unheld =
                  sharing.fold((afterEmitting, Vector.empty[TaskOutput])): (name, log) =>
                    matches.foldLeft((afterEmitting, Vector.empty[TaskOutput])):
                      case ((current, logged), (at, _)) => unhold(current, name, log, SharedJoinStore.key(at, !side.left, bytes), timestamp) match
                          case Some((after, tombstone)) => (after, logged :+ tombstone)
                          case None                     => (current, logged)
                val (afterMatching, unlogged) = unheld
                val joined                    = matches.map((at, value) => result(side.both(record.value, value), math.max(timestamp, at)))
                val unmatched: F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
                  if !side.outer || matches.nonEmpty then F.pure((afterMatching, Vector.empty, Vector.empty))
                  else
                    sharing match
                      case Some((name, log)) if to >= streamTime => hold(afterMatching, name, log, side, bytes, record)
                      case _                                     => F.pure((afterMatching, alone, Vector.empty))
                unmatched.flatMap: (afterHolding, lone, holdLog) =>
                  windowJoinPut(afterHolding, side, own, retention, bytes, record).map: (after, put) =>
                    (after, sent ++ joined ++ lone, flushed ++ unlogged ++ holdLog :+ put)

  /** The records of the key written as `bytes` in a join's window store `store` from time `from` to `to`, earliest first and then in the order they
    * were put, as Java's window store fetches them; the store holds none its clock has expired, since it drops them as the clock moves and once
    * restored.
    */
  private def joinedOthers(state: TaskState, store: String, bytes: Chunk[Byte], from: Long, to: Long): Vector[(Long, Stored)] =
    state.store(store).toVector.collect:
      case (held, stored) if held.size == bytes.size + 12 && held.take(bytes.size) == bytes =>
        (WindowStore.startOf(held), JoinWindowStore.sequenceOf(held), stored)
    .filter((at, _, _) => at >= from && at <= to).sortBy((at, sequence, _) => (at, sequence)).map((at, _, stored) => (at, stored))

  /** The join's shared store without the records held at `key`, and the tombstone its changelog gets, where it holds any, as Java's
    * `putIfAbsent(key, null)` removes them.
    */
  private def unhold(state: TaskState, shared: String, changelog: Topic, key: Chunk[Byte], timestamp: Long): Option[(TaskState, TaskOutput)] =
    Option.when(state.store(shared).contains(key)):
      (
        state.copy(stores = state.stores.updated(shared, state.store(shared) - key)),
        TaskOutput(changelog, Some(partition), Some(key), None, timestamp, Headers.empty)
      )

  /** `record`, which matches nothing, held in the join's shared store until its window closes: appended to the list of its time and key, as Java's
    * list-valued store appends it, the whole list written to the store's changelog with the record's time, and the earliest time held moved to the
    * record's where it is earlier.
    */
  private def hold[K, X, Y, R](
      state: TaskState,
      shared: String,
      changelog: Topic,
      side: JoinSide[X, Y, R],
      bytes: Chunk[Byte],
      record: Travelling[K, X]
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    val key = SharedJoinStore.key(record.timestamp, side.left, bytes)
    for
      written <- side.values.serializer.serialize(changelog, record.headers, record.value)
      value   <- F.fromOption(written, new IllegalArgumentException(s"the join store '$shared' cannot hold a value its serializer writes as none"))
      held    <- state.store(shared).get(key).traverse(stored => F.fromOption(SharedJoinStore.elements(stored.value), unreadableJoin(shared)))
    yield
      val list  = SharedJoinStore.list(held.getOrElse(Vector.empty) :+ SharedJoinStore.element(side.left, value))
      val time  = state.joinTimes.getOrElse(shared, JoinTime.initial)
      val after =
        state.copy(
          stores = state.stores.updated(shared, state.store(shared).updated(key, Stored(list, record.timestamp))),
          joinTimes = state.joinTimes.updated(shared, time.copy(minTime = math.min(time.minTime, record.timestamp)))
        )
      (after, Vector.empty, Vector(TaskOutput(changelog, Some(partition), Some(key), Some(list), record.timestamp, Headers.empty)))

  private def unreadableJoin(shared: String) = new IllegalStateException(s"the join store '$shared' holds a list it cannot read")

  /** `record` put in its side's window store, as Java's window store for a join puts it: under the next sequence number, the store's clock moving to
    * the record's time if later, kept unless the clock has expired it, expired records going as the clock moves; the changelog gets the record either
    * way, with its time and headers.
    */
  private def windowJoinPut[K, X, Y, R](
      state: TaskState,
      side: JoinSide[X, Y, R],
      changelog: Topic,
      retention: Long,
      bytes: Chunk[Byte],
      record: Travelling[K, X]
  ): F[(TaskState, TaskOutput)] =
    side.values.serializer.serialize(changelog, record.headers, record.value).map: written =>
      val entries  = state.store(side.own)
      val sequence = JoinWindowStore.next(state.sequences.getOrElse(side.own, 0))
      val clock    = state.clocks.getOrElse(side.own, WindowStore.clockOf(entries))
      val ticked   = math.max(clock, record.timestamp)
      val key      = JoinWindowStore.key(bytes, record.timestamp, sequence)
      val kept     =
        if WindowStore.expired(record.timestamp, ticked, retention) then entries
        else written.fold(entries)(value => entries.updated(key, Stored(value, record.timestamp)))
      val contents = if ticked > clock then WindowStore.pruned(kept, ticked, retention) else kept
      val after    =
        state.copy(
          stores = state.stores.updated(side.own, contents),
          clocks = state.clocks.updated(side.own, ticked),
          sequences = state.sequences.updated(side.own, sequence)
        )
      (after, TaskOutput(changelog, Some(partition), Some(key), written, record.timestamp, record.headers))

  /** The held records whose windows have closed, sent on alone and removed from the join's shared store, as Java's join sends them
    * (`emitNonJoinedOuterRecords`): only once the earliest time held, with the processor's `after` and the grace, is behind the stream time, and no
    * sooner by the wall clock than the interval after the last look. Java goes through the store in its order, the earliest time held moving to each
    * record's; a record whose window is still open marks its side as open and is passed over, and once both sides are, the look ends. Each record
    * sent goes on with its own time and key and the headers of the record that made the look; each key sent from is removed, its changelog getting a
    * tombstone with that record's time.
    */
  private def nonJoined[K, A, B, R, K1, V1, K2, V2](
      join: Origin.StreamsJoined[F, K, A, B, R, K1, V1, K2, V2],
      shared: String,
      changelog: Topic,
      after: Long,
      headers: Headers,
      timestamp: Long,
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    import join.windows
    import SharedJoinStore.given
    val time = state.joinTimes.getOrElse(shared, JoinTime.initial)
    // Java adds these as longs, so an earliest time of none wraps below the stream time.
    if time.minTime + after + windows.graceMillis >= time.streamTime || now < time.nextEmit then F.pure((state, Vector.empty, Vector.empty))
    else
      state.store(shared).toVector.sortBy(_._1).flatTraverse: (key, stored) =>
        F.fromOption(SharedJoinStore.elements(stored.value), unreadableJoin(shared)).map(_.map(key -> _))
      .flatMap: elements =>
        val looked =
          elements.foldLeft(Task.Look(Long.MaxValue, Set.empty, false, Vector.empty)):
            case (look, _) if look.ended => look
            case (look, (key, element))  =>
              val at   = SharedJoinStore.timestampOf(key)
              val left = SharedJoinStore.isLeft(key)
              val open = at + (if left then windows.afterMillis else windows.beforeMillis) + windows.graceMillis >= time.streamTime
              if look.open.size == 2 then look.copy(minTime = at, ended = true)
              else if open then look.copy(minTime = at, open = look.open + left)
              else look.copy(minTime = at, sent = look.sent :+ (key -> element))
        val removed = looked.sent.map(_._1).distinct
        looked.sent.flatTraverse: (key, element) =>
          join.key.deserializer.deserialize(changelog, Headers.empty, Some(SharedJoinStore.keyOf(key))).flatMap: k =>
            val value = element.drop(1)
            val made  =
              if element.head.contains(1.toByte) then
                join.value.deserializer.deserialize(changelog, Headers.empty, Some(value)).map(join.joiner.leftAlone)
              else join.otherValue.deserializer.deserialize(changelog, Headers.empty, Some(value)).map(join.joiner.rightAlone)
            made.map(_.map(Travelling(k, _, SharedJoinStore.timestampOf(key), headers)).toVector)
        .map: results =>
          val kept  = state.store(shared) -- removed
          val after =
            state.copy(
              stores = state.stores.updated(shared, kept),
              joinTimes = state.joinTimes.updated(shared, time.copy(minTime = looked.minTime, nextEmit = now + JoinTime.EmitInterval))
            )
          (after, results, removed.map(key => TaskOutput(changelog, Some(partition), Some(key), None, timestamp, Headers.empty)))
  private def chain[K, V, K2, V2](
      steps: Chain[F, K, V, K2, V2],
      record: Travelling[K, V],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])] =
    steps match
      case Chain.Start()          => F.pure((state, Vector(record), Vector.empty))
      case Chain.Then(init, last) => chain(init, record, state, now).flatMap: (before, records, written) =>
          records.foldLeftM((before, Vector.empty[Travelling[K2, V2]], written)):
            case ((current, done, out), next) => step(last, next, current, now).map((after, more, logged) => (after, done ++ more, out ++ logged))

  /** The record with what `step` makes of it and its key's value in the table, as Java's stream-table join makes it when the record comes. */
  private def joined[K, V, TV, R](
      step: Step.Join[F, K, V, TV, R],
      record: Travelling[K, V],
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    valueOf(step.view, record.key, state)
      .map(table => (state, step.join(record.value, table.map(_._1)).toVector.map(v => record.copy(value = v, old = None)), Vector.empty))

  /** The other side's value for `key` and its time, where its view finds one; a windowed table has no view, and its joins are refused. */
  private def lookup[K, V](view: Option[TableView[F, K, V]], key: K, state: TaskState): F[Option[(V, Long)]] =
    view.flatTraverse(valueOf(_, key, state))

  private def step[K, V, K2, V2](
      step: Step[F, K, V, K2, V2],
      record: Travelling[K, V],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])] =
    def passing(records: Vector[Travelling[K2, V2]]) = F.pure((state, records, Vector.empty[TaskOutput]))
    step match
      case Step.Filter(keep) => passing(if keep(record.key, record.value) then Vector(record) else Vector.empty)
      case Step.Map(change)  =>
        val (k, v) = change(record.key, record.value)
        passing(Vector(Travelling(k, v, record.timestamp, record.headers)))
      case Step.MapValues(change) => passing(Vector(record.copy(value = change(record.value), old = None)))
      case Step.FlatMap(change)   =>
        passing(change(record.key, record.value).toVector.map((k, v) => Travelling(k, v, record.timestamp, record.headers)))
      case Step.Peek(effect)          => effect(record.key, record.value) *> passing(Vector(record))
      case Step.EvalMapValues(change) => change(record.value).flatMap(v => passing(Vector(record.copy(value = v, old = None))))
      case Step.EvalFilter(keep)      => keep(record.key, record.value).flatMap(kept => passing(if kept then Vector(record) else Vector.empty))
      case Step.EvalMap(change)       => change(record.key, record.value)
          .flatMap((k, v) => passing(Vector(Travelling(k, v, record.timestamp, record.headers))))
      case Step.ToStream()                                               => passing(Vector(record))
      case folding @ Step.WindowedAggregate(_, _, _, _, _, _, _, _, _)   => windowed(folding, record, state, now)
      case folding @ Step.SlidingAggregate(_, _, _, _, _, _, _, _, _)    => sliding(folding, record, state, now)
      case folding @ Step.SessionAggregate(_, _, _, _, _, _, _, _, _, _) => session(folding, record, state, now)
      case holding @ Step.Suppress(_, _, _, _)                           => suppress(holding, record, state)
      case joining @ Step.Join(_, _)                                     => joined(joining, record, state)
      case Step.TableSource(store, key, value)                           => topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
          record.key.flatTraverse(k => key.serializer.serialize(changelog, Headers.empty, k).map(_.map(k -> _))).flatMap:
            // Java skips a record whose key is null: one read without a key, or one its serializer writes as none.
            case None             => F.pure((state, Vector.empty, Vector.empty))
            case Some((k, bytes)) => state.store(store.name).get(bytes)
                .traverse(held => value.deserializer.deserialize(changelog, Headers.empty, Some(held.value))).flatMap: replaced =>
                  record.value.flatTraverse(value.serializer.serialize(changelog, Headers.empty, _)).map: written =>
                    val contents =
                      written.fold(state.store(store.name) - bytes)(v => state.store(store.name).updated(bytes, Stored(v, record.timestamp)))
                    val after  = state.copy(stores = state.stores.updated(store.name, contents))
                    val logged = TaskOutput(changelog, Some(partition), Some(bytes), written, record.timestamp, Headers.empty)
                    (after, Vector(Travelling(k, record.value, record.timestamp, record.headers, Some(replaced))), Vector(logged))
      case Step.Aggregate(store, key, value, initial, start, add) => topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
          key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
            // Java skips a record whose key is null, which here is a key its serializer writes as none.
            case None        => F.pure((state, Vector.empty, Vector.empty))
            case Some(bytes) =>
              val held    = state.store(store.name).get(bytes)
              val updated =
                held match
                  case None      => F.pure((start(record.key, record.value), record.timestamp, initial))
                  case Some(old) => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value)).map: previous =>
                      (add(record.key, record.value, previous), math.max(old.timestamp, record.timestamp), Some(previous))
              updated.flatMap: (aggregate, timestamp, replaced) =>
                value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
                  val contents = written.fold(state.store(store.name) - bytes)(v => state.store(store.name).updated(bytes, Stored(v, timestamp)))
                  val after    = state.copy(stores = state.stores.updated(store.name, contents))
                  val logged   = TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)
                  (after, Vector(Travelling(record.key, aggregate, timestamp, record.headers, replaced)), Vector(logged))

  /** What a window step gives: the task's state, the records it passes on, and what it writes to its store's changelog. */
  private type Windowing[K, A] = (TaskState, Vector[Travelling[Windowed[K], A]], Vector[TaskOutput])

  /** The windows of the key written as `bytes` in window store `store` starting from `from` to `to`, earliest first, without those its clock has
    * expired, as Java's window store fetches them.
    */
  private def windowsOf(state: TaskState, store: String, retention: Long, bytes: Chunk[Byte], from: Long, to: Long): Vector[(Long, Stored)] =
    val entries = state.store(store)
    val least   = math.max(from, state.clocks.getOrElse(store, WindowStore.clockOf(entries)) - retention + 1)
    entries.toVector.collect:
      case (held, stored) if held.size == bytes.size + 12 && held.take(bytes.size) == bytes => (WindowStore.startOf(held), stored)
    .filter((start, _) => start >= least && start <= to).sortBy(_._1)

  /** Puts `written` as the key's window starting at `start` in window store `store`, as Java's in-memory window store puts it: the clock moves to the
    * window's start if later, the window is kept unless the clock has expired it, expired windows go as the clock moves, and the changelog gets the
    * write either way.
    */
  private def windowPut(
      state: TaskState,
      store: String,
      retention: Long,
      changelog: Topic,
      bytes: Chunk[Byte],
      start: Long,
      written: Option[Chunk[Byte]],
      timestamp: Long
  ): (TaskState, TaskOutput) =
    val entries  = state.store(store)
    val clock    = state.clocks.getOrElse(store, WindowStore.clockOf(entries))
    val windowed = WindowStore.key(bytes, start)
    val ticked   = math.max(clock, start)
    val kept     =
      if WindowStore.expired(start, ticked, retention) then entries
      else written.fold(entries - windowed)(v => entries.updated(windowed, Stored(v, timestamp)))
    val contents = if ticked > clock then WindowStore.pruned(kept, ticked, retention) else kept
    val after    = state.copy(stores = state.stores.updated(store, contents), clocks = state.clocks.updated(store, ticked))
    (after, TaskOutput(changelog, Some(partition), Some(windowed), written, timestamp, Headers.empty))

  /** The windows `step` updates for `record`, as Java's windowed aggregation does: each window the record falls in that has not closed, by the step's
    * stream time less its grace, is folded from its value, or started where the store has none, then kept unless expired by the store's clock, and
    * written to the changelog either way. The store never holds an expired window, since it drops them as its clock moves and once restored, so one
    * reads as absent, as Java's store reads it. A record whose key its serializer writes as none is skipped, as Java skips a null key.
    */
  private def windowed[K, V, A](step: Step.WindowedAggregate[F, K, V, A], record: Travelling[K, V], state: TaskState, now: Long): F[Windowing[K, A]] =
    import step.{add, emitOnClose, key, retention, store, value, windows}
    topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val streamTime = math.max(state.streamTimes.getOrElse(store.name, WindowStore.NoTime), record.timestamp)
          val timed      = state.copy(streamTimes = state.streamTimes.updated(store.name, streamTime))
          val closeTime  = streamTime - windows.graceMillis
          val open       = windows.windowsFor(record.timestamp).filter(_.end > closeTime)
          val updated    =
            open.foldLeftM((timed, Vector.empty[Travelling[Windowed[K], A]], Vector.empty[TaskOutput])):
              case ((current, forwarded, logged), window) =>
                val folded =
                  current.store(store.name).get(WindowStore.key(bytes, window.start)) match
                    case None      => F.pure((step.start(record.key, record.value), record.timestamp, step.initial))
                    case Some(old) => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value)).map: previous =>
                        (add(record.key, record.value, previous), math.max(old.timestamp, record.timestamp), Some(previous))
                folded.flatMap: (aggregate, timestamp, replaced) =>
                  value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
                    val (after, log) = windowPut(current, store.name, retention, changelog, bytes, window.start, written, timestamp)
                    (after, forwarded :+ Travelling(Windowed(record.key, window), aggregate, timestamp, record.headers, replaced), logged :+ log)
          if !emitOnClose then updated
          else
            val size = windows.sizeMillis
            // An emission ends at the window starting at the close time less the size, the next starts just after it, and Java fetches nothing
            // where the windows it would end at are those it ended at last time.
            def range(last: Long) =
              val upper = closeTime - size
              val lower = if last == WindowStore.NoTime then 0L else math.max(0L, last - size) + 1
              val moved = last == WindowStore.NoTime || windows.windowsFor(upper) != windows.windowsFor(lower - 1)
              Option.when(upper >= 0 && moved)((lower, upper))
            updated.flatMap: (after, _, logged) =>
              closed(store.name, key, value, after, closeTime, size, range, record, changelog, now).map((emitted, state) => (state, emitted, logged))

  /** The sliding windows `step` updates or makes for `record`, as Java's sliding window aggregation does (`KStreamSlidingWindowAggregate`, over a
    * store it can read backwards): a record past its window's grace is dropped; one in the first time difference after 0 goes in the window from 0
    * and makes no look for closed windows; any other updates every window it falls in, latest first, and makes the window ending at it, the window
    * starting just after it where later records are within the time difference, and the window starting just after the record before it where it
    * falls in that one.
    */
  private def sliding[K, V, A](step: Step.SlidingAggregate[F, K, V, A], record: Travelling[K, V], state: TaskState, now: Long): F[Windowing[K, A]] =
    import step.{add, emitOnClose, key, retention, store, value, windows}
    val difference = windows.differenceMillis
    val timestamp  = record.timestamp
    topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val streamTime               = math.max(state.streamTimes.getOrElse(store.name, WindowStore.NoTime), record.timestamp)
          val timed                    = state.copy(streamTimes = state.streamTimes.updated(store.name, streamTime))
          val closeTime                = streamTime - windows.graceMillis
          def read(held: Stored): F[A] = value.deserializer.deserialize(changelog, Headers.empty, Some(held.value))
          // Puts a window and passes it on, as Java puts a window it makes from an aggregate it found.
          def put(ran: Windowing[K, A], start: Long, aggregate: A, at: Long, replaced: Option[A]): F[Windowing[K, A]] =
            value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
              val (after, log) = windowPut(ran._1, store.name, retention, changelog, bytes, start, written, at)
              val window       = Windowed(record.key, Window(start, start + difference))
              (after, ran._2 :+ Travelling(window, aggregate, at, record.headers, replaced), ran._3 :+ log)
          // A window made from another's aggregate replaces nothing, as Java passes it on.
          def write(ran: Windowing[K, A], start: Long, aggregate: A, at: Long): F[Windowing[K, A]] = put(ran, start, aggregate, at, None)
          // Java's updateWindowAndForward: folds the record into a window that has not closed, from what it held, or from nothing.
          def update(ran: Windowing[K, A], start: Long, held: Option[(A, Long)]): F[Windowing[K, A]] =
            if start + difference < closeTime then F.pure(ran)
            else
              held match
                case None            => put(ran, start, step.start(record.key, record.value), timestamp, step.initial)
                case Some((old, at)) => put(ran, start, add(record.key, record.value, old), math.max(timestamp, at), Some(old))
          val nothing: Windowing[K, A] = (timed, Vector.empty, Vector.empty)
          if timestamp + difference < closeTime then F.pure(nothing)
          else if timestamp < difference then
            // Emitting only once windows close, updates are not passed on, early ones included.
            early(nothing, bytes, read, write, update, timestamp, retention, store.name)
              .map((after, updates, logged) => (after, if emitOnClose then Vector.empty else updates, logged))
          else
            val reversed = reverse(nothing, bytes, read, write, update, difference, timestamp, retention, store.name)
            if !emitOnClose then reversed
            else
              // Sliding windows end where they start plus the time difference, both included, so an emission ends a millisecond earlier, and
              // the next starts where the last one could not yet reach.
              def range(last: Long) =
                val upper = closeTime - difference - 1
                val lower = if last == WindowStore.NoTime then 0L else math.max(0L, last - difference)
                Option.when(upper >= 0)((lower, upper))
              reversed.flatMap: (after, _, logged) =>
                closed(store.name, key, value, after, closeTime, difference, range, record, changelog, now)
                  .map((emitted, state) => (state, emitted, logged))

  /** Writes `written` as the key's session from `start` to `end` in session store `store`, or removes it where there is nothing to write, as Java's
    * in-memory session store does: the clock moves to the session's end if later, the session is kept unless the clock has expired it, expired
    * sessions go as the clock moves, and the changelog gets the write, or a tombstone, either way, at the time of the record being processed.
    */
  private def sessionWrite(
      state: TaskState,
      store: String,
      retention: Long,
      changelog: Topic,
      bytes: Chunk[Byte],
      start: Long,
      end: Long,
      written: Option[Chunk[Byte]],
      timestamp: Long
  ): (TaskState, TaskOutput) =
    val entries = state.store(store)
    val clock   = state.clocks.getOrElse(store, SessionStore.clockOf(entries))
    val session = SessionStore.key(bytes, start, end)
    val ticked  = written.fold(clock)(_ => math.max(clock, end))
    val kept    =
      written match
        case None                                                   => entries - session
        case Some(_) if WindowStore.expired(end, ticked, retention) => entries
        case Some(value)                                            => entries.updated(session, Stored(value, timestamp))
    val contents = if ticked > clock then SessionStore.pruned(kept, ticked, retention) else kept
    val after    = state.copy(stores = state.stores.updated(store, contents), clocks = state.clocks.updated(store, ticked))
    (after, TaskOutput(changelog, Some(partition), Some(session), written, timestamp, Headers.empty))

  /** The sessions `step` changes for `record`, as Java's session window aggregation does: the sessions of the record's key ending no earlier than the
    * inactivity gap before it and starting no later than the gap after it, by end and then start, are merged with it into one session, unless that
    * session ends before the step's stream time less the grace and the gap, when the record is dropped. Merged sessions are removed and go on as none
    * at their end, and the new session goes on with its value at its end, both without headers, as Java passes them on as records of their own. A
    * record whose key its serializer writes as none is skipped.
    */
  private def session[K, V, A](
      step: Step.SessionAggregate[F, K, V, A],
      record: Travelling[K, V],
      state: TaskState,
      now: Long
  ): F[Windowing[K, Option[A]]] =
    import step.{add, emitOnClose, initial, key, merge, retention, store, value, windows}
    val gap       = windows.gapMillis
    val timestamp = record.timestamp
    topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val streamTime = math.max(state.streamTimes.getOrElse(store.name, WindowStore.NoTime), timestamp)
          val timed      = state.copy(streamTimes = state.streamTimes.updated(store.name, streamTime))
          val closeTime  = streamTime - windows.graceMillis - gap
          val entries    = timed.store(store.name)
          val found      =
            entries.toVector.collect:
              case (held, stored) if held.size == bytes.size + 16 && held.take(bytes.size) == bytes =>
                (SessionStore.startOf(held), SessionStore.endOf(held), stored)
            .filter((start, end, _) => end >= timestamp - gap && start <= timestamp + gap).sortBy((start, end, _) => (end, start))
          val changed =
            found.traverse((_, _, stored) => value.deserializer.deserialize(changelog, Headers.empty, Some(stored.value))).flatMap: aggregates =>
              val (from, to) = found.foldLeft((timestamp, timestamp))((window, held) => (math.min(window._1, held._1), math.max(window._2, held._2)))
              val folded     = aggregates.foldLeft(initial)((merged, next) => Some(merged.fold(next)(merge(record.key, _, next))))
              if to < closeTime then F.pure((timed, Vector.empty[Travelling[Windowed[K], Option[A]]], Vector.empty[TaskOutput]))
              else
                // A record that joins no other session makes its own, which removes nothing, even where one like it was already kept.
                val merged  = if from == timestamp && to == timestamp then Vector.empty else found.zip(aggregates)
                val removed =
                  merged.foldLeft((timed, Vector.empty[Travelling[Windowed[K], Option[A]]], Vector.empty[TaskOutput])):
                    case ((current, forwarded, logged), ((start, end, _), gone)) =>
                      val (after, log) = sessionWrite(current, store.name, retention, changelog, bytes, start, end, None, timestamp)
                      val session      = Windowed(record.key, Window(start, end))
                      (after, forwarded :+ Travelling(session, None, end, Headers.empty, Some(Some(gone))), logged :+ log)
                val aggregate = folded.fold(step.start(record.key, record.value))(add(record.key, record.value, _))
                value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
                  val (current, forwarded, logged) = removed
                  val (after, log)                 = sessionWrite(current, store.name, retention, changelog, bytes, from, to, written, timestamp)
                  (after, forwarded :+ Travelling(Windowed(record.key, Window(from, to)), Some(aggregate), to, Headers.empty), logged :+ log)
          if !emitOnClose then changed
          else
            // Sessions end where they end, included, so an emission ends a millisecond before the close time, and the next starts where it ended.
            def range(last: Long) =
              val upper = closeTime - 1
              val lower = math.max(0L, last)
              Option.when(upper >= 0 && upper >= lower)((lower, upper))
            changed.flatMap: (after, _, logged) =>
              sessionsClosed(store.name, key, value, after, closeTime, range, record, changelog, now)
                .map((emitted, state) => (state, emitted, logged))

  /** The sessions of session store `store` that a step emitting them once they close emits, as Java's session window aggregation emits final results:
    * at most once per emit interval of wall-clock time, every session ending in the range `range` gives from the close time last emitted for, by end,
    * then key and then start, each with its value at its end and the headers of the record that closed it.
    */
  private def sessionsClosed[K, V, A](
      store: String,
      key: xkafka.streams.Serde[F, K],
      value: xkafka.streams.Serde[F, A],
      state: TaskState,
      closeTime: Long,
      range: Long => Option[(Long, Long)],
      record: Travelling[K, V],
      changelog: Topic,
      now: Long
  ): F[(Vector[Travelling[Windowed[K], Option[A]]], TaskState)] =
    if now < state.nextEmit.getOrElse(store, 0L) then F.pure((Vector.empty, state))
    else
      val throttled = state.copy(nextEmit = state.nextEmit.updated(store, now + Task.EmitIntervalMillis))
      range(state.lastEmitted.getOrElse(store, WindowStore.NoTime)) match
        case None                 => F.pure((Vector.empty, throttled))
        case Some((lower, upper)) =>
          val due =
            throttled.store(store).toVector.map: (held, stored) =>
              (SessionStore.endOf(held), held.take(held.size - 16), SessionStore.startOf(held), stored)
            .filter((end, _, _, _) => end >= lower && end <= upper).sortWith: (a, b) =>
              a._1 < b._1 || (a._1 == b._1 && (Task.unsigned.compare(a._2, b._2) < 0 || (a._2 == b._2 && a._3 < b._3)))
          due.traverse: (end, serialized, start, stored) =>
            (
              key.deserializer.deserialize(changelog, Headers.empty, Some(serialized)),
              value.deserializer.deserialize(changelog, Headers.empty, Some(stored.value))
            ).mapN((k, v) => Travelling(Windowed(k, Window(start, end)), Option(v), end, record.headers))
          .map(emitted => (emitted, throttled.copy(lastEmitted = throttled.lastEmitted.updated(store, closeTime))))

  /** The serde a suppression's buffer writes a table's keys with: Java's windowed serdes for windowed tables. */
  private def bufferKey[K](key: TableKey[F, K]): xkafka.streams.Serde[F, K] =
    key match
      case TableKey.Plain(plain)              => plain
      case TableKey.TimeWindowed(inner, size) => xkafka.streams.Serde.timeWindowed(inner, scala.concurrent.duration.FiniteDuration(size, "ms"))
      case TableKey.SessionWindowed(inner)    => xkafka.streams.Serde.sessionWindowed(inner)

  /** The serde a suppression's buffer writes a table's values with, none written as a tombstone. */
  private def bufferValue[V](value: TableValue[F, V]): xkafka.streams.Serde[F, V] =
    value match
      case TableValue.Plain(plain)    => plain
      case TableValue.Optional(inner) => inner.option

  /** A table's change held back, as Java's `KTableSuppressProcessor` holds it: kept in the buffer under its key, in place of any change held before
    * but held until the time the first was held until, which is the window's end or the record's time; then, by the step's stream time, every change
    * whose time is the suppression's wait behind it goes on, earliest first and then by key, with the time and headers it was held with, a tombstone
    * dropped where the windows close; then, while the buffer holds more than its limit, the earliest go on early, or the task stops.
    */
  private def suppress[K, V](
      step: Step.Suppress[F, K, V],
      record: Travelling[K, V],
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K, V]], Vector[TaskOutput])] =
    import step.suppressed
    val store                    = s"${suppressed.name}-store"
    val keys                     = bufferKey(step.key)
    val values                   = bufferValue(step.value)
    val (until, wait, dropEmpty) =
      step.release match
        case Release.WindowCloses(end, grace) => (end(record.key), grace, true)
        case Release.TimeLimit(waited)        => (record.timestamp, waited, false)
    def unreadable = new IllegalStateException(s"the suppression '${suppressed.name}' buffer holds a change it cannot read")
    topicNamed(s"$application-$store-changelog").flatMap: changelog =>
      (
        keys.serializer.serialize(changelog, record.headers, record.key),
        record.old.flatTraverse(values.serializer.serialize(changelog, record.headers, _)),
        values.serializer.serialize(changelog, record.headers, record.value)
      ).tupled.flatMap:
        case (None, _, _)            => F.pure((state, Vector.empty, Vector.empty))
        case (Some(key), old, value) =>
          val streamTime = math.max(state.streamTimes.getOrElse(store, WindowStore.NoTime), record.timestamp)
          val source     = state.reading
          val context    =
            record.origin.fold(
              HeldContext(record.timestamp, source.fold(-1L)(_.offset), source.fold("")(_.topic), source.fold(-1)(_.partition), record.headers)
            )(_.copy(timestamp = record.timestamp, headers = record.headers))
          val contents = state.store(store)
          (state.buffers.get(store).orElse(Buffered.of(contents)), contents.get(key).traverse(stored => Held.decode(stored.value))).tupled
            .liftTo[F](unreadable).flatMap: (index, before) =>
              // The key's value before it was first held stays with it until it goes on, and so does the time it is held until.
              val held     = Held(before.fold(until)(_.time), context, before.fold(old)(_.prior), old, value)
              val indexed  = before.fold(index)(index.excluding(key, _)).including(key, held)
              val buffered =
                state.copy(
                  streamTimes = state.streamTimes.updated(store, streamTime),
                  stores = state.stores.updated(store, contents.updated(key, Stored(held.encode, record.timestamp))),
                  dirty = state.dirty.updated(store, state.dirty.getOrElse(store, Dirty.empty) + key)
                )
              def emitted(key: Chunk[Byte], held: Held): F[Vector[Travelling[K, V]]] =
                val headers = held.context.headers
                if held.value.isEmpty && dropEmpty then F.pure(Vector.empty)
                else
                  (
                    keys.deserializer.deserialize(changelog, headers, Some(key)),
                    values.deserializer.deserialize(changelog, headers, held.value),
                    held.old.traverse(old => values.deserializer.deserialize(changelog, headers, Some(old)))
                  ).mapN((k, v, old) => Vector(Travelling(k, v, held.context.timestamp, headers, old, Some(held.context))))
              // Passes on the earliest change while `due` holds of the buffer, and takes it out.
              def evicting(
                  now: TaskState,
                  index: Buffered,
                  out: Vector[Travelling[K, V]],
                  due: Buffered => Boolean
              ): F[(TaskState, Buffered, Vector[Travelling[K, V]])] =
                index.order.headOption.filter(_ => due(index)) match
                  case None           => F.pure((now, index, out))
                  case Some((_, key)) => Held.decode(now.store(store)(key).value).liftTo[F](unreadable).flatMap: held =>
                      val taken =
                        now.copy(
                          stores = now.stores.updated(store, now.store(store) - key),
                          dirty = now.dirty.updated(store, now.dirty.getOrElse(store, Dirty.empty) + key)
                        )
                      emitted(key, held).flatMap(more => evicting(taken, index.excluding(key, held), out ++ more, due))
              def over(index: Buffered) = index.order.size > suppressed.limit.maxRecords || index.bytes > suppressed.limit.maxBytes
              val expiry                = streamTime - wait
              evicting(buffered, indexed, Vector.empty, _.order.headOption.exists(_._1 <= expiry)).flatMap: (expired, left, sent) =>
                val evicted =
                  if !over(left) then F.pure((expired, left, sent))
                  else if suppressed.limit.emitEarly then evicting(expired, left, sent, over)
                  else
                    F.raiseError(new IllegalStateException(s"the suppression '${suppressed.name}' buffer exceeded its limit: ${left.order
                        .size}/${suppressed.limit.maxRecords} " + s"records and ${left.bytes}/${suppressed.limit.maxBytes} bytes"))
                evicted.map((after, index, out) => (after.copy(buffers = after.buffers.updated(store, index)), out, Vector.empty))

  /** What Java's sliding window aggregation finds among a key's windows as it reads them backwards. */
  private final case class Found[A](
      starts: Set[Long],
      left: Option[(A, Long)],
      right: Option[(A, Long)],
      leftMade: Boolean,
      rightMade: Boolean,
      previous: Option[Long]
  )

  /** Java's processReverse, then createWindows unless it ended early. */
  private def reverse[K, A](
      begun: Windowing[K, A],
      bytes: Chunk[Byte],
      read: Stored => F[A],
      write: (Windowing[K, A], Long, A, Long) => F[Windowing[K, A]],
      update: (Windowing[K, A], Long, Option[(A, Long)]) => F[Windowing[K, A]],
      difference: Long,
      timestamp: Long,
      retention: Long,
      store: String
  ): F[Windowing[K, A]] =
    val held = windowsOf(begun._1, store, retention, bytes, math.max(0L, timestamp - 2 * difference), timestamp + 1).reverse.toList
    def scan(windows: List[(Long, Stored)], ran: Windowing[K, A], found: Found[A]): F[(Windowing[K, A], Found[A], Boolean)] =
      windows match
        case Nil                     => F.pure((ran, found, false))
        case (start, stored) :: rest =>
          val seen = found.copy(starts = found.starts + start)
          val end  = start + difference
          if start == timestamp + 1 then scan(rest, ran, seen.copy(rightMade = true))
          else if end > timestamp then
            read(stored).flatMap: old =>
              update(ran, start, Some((old, stored.timestamp)))
                .flatMap(next => scan(rest, next, seen.copy(right = seen.right.orElse(Some((old, stored.timestamp))))))
          else if end == timestamp then
            read(stored).flatMap: old =>
              update(ran, start, Some((old, stored.timestamp))).flatMap: next =>
                val made = seen.copy(leftMade = true)
                // A window ending at the record that already holds records as late ends the scan, and nothing more is made.
                if stored.timestamp < timestamp then scan(rest, next, made.copy(previous = Some(stored.timestamp))) else F.pure((next, made, true))
          else read(stored).map(old => (ran, seen.copy(left = Some((old, stored.timestamp)), previous = Some(stored.timestamp)), false))
    scan(held, begun, Found(Set.empty, None, None, false, false, None)).flatMap: (ran, found, ended) =>
      if ended then F.pure(ran)
      else
        val previousRight =
          found.previous.filter(previous => !found.starts.contains(previous + 1) && previous + 1 + difference >= timestamp) match
            case Some(previous) => update(ran, previous + 1, None)
            case None           => F.pure(ran)
        previousRight.flatMap: next =>
          val left =
            if found.leftMade then F.pure(next)
            else
              val from = found.previous.filter(timestamp - difference <= _).flatMap(_ => found.left).map((old, _) => (old, timestamp))
              update(next, timestamp - difference, from)
          left.flatMap: made =>
            found.right.filter(_._2 > timestamp) match
              case Some((aggregate, at)) if !found.rightMade => write(made, timestamp + 1, aggregate, at)
              case _                                         => F.pure(made)

  /** Java's processEarly: a record within the first time difference after 0 goes in the window from 0, which holds every such record. */
  private def early[K, A](
      begun: Windowing[K, A],
      bytes: Chunk[Byte],
      read: Stored => F[A],
      write: (Windowing[K, A], Long, A, Long) => F[Windowing[K, A]],
      update: (Windowing[K, A], Long, Option[(A, Long)]) => F[Windowing[K, A]],
      timestamp: Long,
      retention: Long,
      store: String
  ): F[Windowing[K, A]] =
    val held = windowsOf(begun._1, store, retention, bytes, 0L, timestamp + 1).toList
    def scan(
        windows: List[(Long, Stored)],
        ran: Windowing[K, A],
        combined: Option[(A, Long)],
        found: Found[A]
    ): F[(Windowing[K, A], Option[(A, Long)], Found[A])] =
      windows match
        case Nil                     => F.pure((ran, combined, found))
        case (start, stored) :: rest =>
          val seen = found.copy(starts = found.starts + start)
          if start == 0 then
            read(stored).flatMap: old =>
              val previous = Option.when(stored.timestamp < timestamp)(stored.timestamp).orElse(seen.previous)
              scan(rest, ran, Some((old, stored.timestamp)), seen.copy(previous = previous))
          else if start <= timestamp then
            read(stored).flatMap: old =>
              update(ran, start, Some((old, stored.timestamp)))
                .flatMap(next => scan(rest, next, combined, seen.copy(right = Some((old, stored.timestamp)))))
          else scan(rest, ran, combined, seen.copy(rightMade = seen.rightMade || start == timestamp + 1))
    scan(held, begun, None, Found(Set.empty, None, None, false, false, None)).flatMap: (ran, combined, found) =>
      val right     = found.right.orElse(combined.filter(_._2 > timestamp))
      val withRight =
        right.filter(_._2 > timestamp) match
          case Some((aggregate, at)) if !found.rightMade => write(ran, timestamp + 1, aggregate, at)
          case _                                         => F.pure(ran)
      withRight.flatMap: next =>
        val previousRight =
          found.previous.filter(previous => !found.starts.contains(previous + 1)) match
            case Some(previous) => update(next, previous + 1, None)
            case None           => F.pure(next)
        previousRight.flatMap(made => update(made, 0L, combined))

  /** The windows of window store `store` that a step emitting them once they close emits, as Java's windowed aggregations emit final results: at most
    * once per emit interval of wall-clock time, every window whose start is in the range `range` gives from the close time last emitted for, by
    * window start and then by key, each `width` long, with its value, its timestamp and the headers of the record that closed it.
    */
  private def closed[K, V, A](
      store: String,
      key: xkafka.streams.Serde[F, K],
      value: xkafka.streams.Serde[F, A],
      state: TaskState,
      closeTime: Long,
      width: Long,
      range: Long => Option[(Long, Long)],
      record: Travelling[K, V],
      changelog: Topic,
      now: Long
  ): F[(Vector[Travelling[Windowed[K], A]], TaskState)] =
    if now < state.nextEmit.getOrElse(store, 0L) then F.pure((Vector.empty, state))
    else
      // The next look waits an interval from now, so a clock that jumps ahead does not make every record look.
      val throttled = state.copy(nextEmit = state.nextEmit.updated(store, now + Task.EmitIntervalMillis))
      range(state.lastEmitted.getOrElse(store, WindowStore.NoTime)) match
        case None                 => F.pure((Vector.empty, throttled))
        case Some((lower, upper)) =>
          val due =
            throttled.store(store).toVector.map((stored, held) => (WindowStore.startOf(stored), stored.take(stored.size - 12), held))
              .filter((start, _, _) => start >= lower && start <= upper).sortWith: (a, b) =>
                a._1 < b._1 || (a._1 == b._1 && Task.unsigned.compare(a._2, b._2) < 0)
          due.traverse: (start, serialized, held) =>
            (
              key.deserializer.deserialize(changelog, Headers.empty, Some(serialized)),
              value.deserializer.deserialize(changelog, Headers.empty, Some(held.value))
            ).mapN((k, v) => Travelling(Windowed(k, Window(start, start + width)), v, held.timestamp, record.headers))
          .map(emitted => (emitted, throttled.copy(lastEmitted = throttled.lastEmitted.updated(store, closeTime))))

  /** The value the table `view` reads holds for `key`, with its time, as Java's value getter reads it: a suppressed table's buffer first, where a key
    * it holds gives the value from before the change it holds, at an unknown time, none where there was none, and otherwise the store's value; for
    * tables joined without a store, what the joiner makes of their values, as Java's join value getters make it.
    */
  private def valueOf[K, V](view: TableView[F, K, V], key: K, state: TaskState): F[Option[(V, Long)]] =
    view match
      case TableView.Stored(store, keys, values) => topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
          keys.serializer.serialize(changelog, Headers.empty, key).flatMap:
            _.flatMap(state.store(store.name).get)
              .traverse(held => values.deserializer.deserialize(changelog, Headers.empty, Some(held.value)).map(_ -> held.timestamp))
      case TableView.Held(buffer, parent) =>
        def unreadable = new IllegalStateException(s"the suppression buffer '$buffer' holds a change it cannot read")
        topicNamed(s"$application-$buffer-changelog").flatMap: changelog =>
          parent.keys.serializer.serialize(changelog, Headers.empty, key).flatMap: bytes =>
            bytes.flatMap(state.store(buffer).get) match
              case None         => valueOf(parent, key, state)
              case Some(stored) => (Held.decode(stored.value), Task.valuesOf(parent)).tupled.liftTo[F](unreadable).flatMap: (held, values) =>
                  held.prior.traverse(prior => values.deserializer.deserialize(changelog, held.context.headers, Some(prior)).map(_ -> Task.Unknown))
      case TableView.Joined(left, right, joiner) => joinedValueOf(left, right, joiner, key, state)

  private def joinedValueOf[K, A, B, R](
      left: TableView[F, K, A],
      right: TableView[F, K, B],
      joiner: Joiner[A, B, R],
      key: K,
      state: TaskState
  ): F[Option[(R, Long)]] =
    joiner match
      case Joiner.Inner(join) => valueOf(left, key, state).flatMap:
          case None          => F.pure(None)
          case Some((a, at)) => valueOf(right, key, state).map(_.map((b, bt) => (join(a, b), math.max(at, bt))))
      case Joiner.Left(join) => valueOf(left, key, state).flatMap:
          case None          => F.pure(None)
          case Some((a, at)) => valueOf(right, key, state).map(b => Some((join(a, b.map(_._1)), b.fold(at)(found => math.max(at, found._2)))))
      case Joiner.Outer(join) => (valueOf(left, key, state), valueOf(right, key, state)).mapN: (a, b) =>
          Ior.fromOptions(a.map(_._1), b.map(_._1)).map(both => (join(both), math.max(a.fold(Task.Unknown)(_._2), b.fold(Task.Unknown)(_._2))))

  private def leaving[K, V](exit: End[F, K, V], record: Travelling[K, V]): F[Vector[TaskOutput]] =
    exit match
      case End.Stored()                                => F.pure(Vector.empty)
      case End.Repartitioning(repartition, key, value) =>
        written(topicNamed(s"$application-${repartition.name}-repartition"), key, value, true, record)
      case End.Output(topic, key, value) => written(F.pure(topic), key, value, false, record)

  private def written[K, V](
      topic: F[Topic],
      key: xkafka.streams.Serde[F, K],
      value: xkafka.streams.Serde[F, V],
      repartitioning: Boolean,
      record: Travelling[K, V]
  ): F[Vector[TaskOutput]] =
    // Java partitions a windowed key by the key alone, whichever topic it goes to.
    def base(target: Topic): F[Option[Chunk[Byte]]] = key.serializer.partitionedBy(target, record.headers, record.key).sequence.map(_.flatten)
    for
      target <- topic
      k      <- key.serializer.serialize(target, record.headers, record.key)
      v      <- value.serializer.serialize(target, record.headers, record.value)
      by     <- base(target)
    // Java drops a record with a null key before its repartition topic, which here is a key its serializer writes as none.
    yield if repartitioning && k.isEmpty then Vector.empty else Vector(TaskOutput(target, None, k, v, record.timestamp, record.headers, by))

  private def topicNamed(name: String): F[Topic] = Topic.from(name).leftMap(error => new KafkaException.InvalidValue(error)).liftTo[F]

private[xkafka] object Task:
  /** A look through a stream-stream join's held records: the earliest time it has reached, the sides, by whether they are the left, found with a
    * record whose window is open, whether it has ended, and the held records it sends on, with their keys, in order.
    */
  private final case class Look(minTime: Long, open: Set[Boolean], ended: Boolean, sent: Vector[(Chunk[Byte], Chunk[Byte])])

  /** The topic `source` names, as the application names its repartition topics. */
  def topicOf(application: String, source: Source): String =
    source match
      case Source.Input(topic)         => topic.value
      case Source.Repartitioned(named) => s"$application-${named.name}-repartition"

  /** The serde a table view's values are written with, which the result of joining tables kept in no store does not have. */
  def valuesOf[F[_], K, V](view: TableView[F, K, V]): Option[xkafka.streams.Serde[F, V]] =
    view match
      case TableView.Stored(_, _, value) => Some(value)
      case TableView.Held(_, parent)     => valuesOf(parent)
      case TableView.Joined(_, _, _)     => None

  /** Whether the first table step of `chain` asks the table before it for old values, as Java's suppression does, or where it has no steps, whether
    * what follows it asks.
    */
  def requests[F[_], K, V, K2, V2](chain: Chain[F, K, V, K2, V2], outer: Boolean): Boolean =
    chain match
      case Chain.Start()                                        => outer
      case Chain.Then(Chain.Start(), Step.Suppress(_, _, _, _)) => true
      case Chain.Then(Chain.Start(), _)                         => false
      case Chain.Then(init, _)                                  => requests(init, outer)

  /** What Java's join processor for the left side makes of `change` and the right side's value `other`, for each kind of join: the result's new
    * value, its old value where `sendOld`, and its time, or nothing where Java forwards nothing.
    */
  def joinLeft[K, A, B, R](
      joiner: Joiner[A, B, R],
      change: Change[K, A],
      other: Option[(B, Long)],
      sendOld: Boolean
  ): Option[(Option[R], Option[R], Long)] =
    val b = other.map(_._1)
    joiner match
      case Joiner.Inner(join) => other.map: (b, bt) =>
          (change.value.map(join(_, b)), change.old.filter(_ => sendOld).map(join(_, b)), math.max(change.timestamp, bt))
      case Joiner.Left(join) => Option.unless(other.isEmpty && change.value.isEmpty && change.old.isEmpty):
          (change.value.map(join(_, b)), change.old.filter(_ => sendOld).map(join(_, b)), math.max(change.timestamp, other.fold(Unknown)(_._2)))
      case Joiner.Outer(join) => Option.unless(other.isEmpty && change.value.isEmpty && change.old.isEmpty):
          (
            Ior.fromOptions(change.value, b).map(join),
            Option.when(sendOld)(Ior.fromOptions(change.old, b).map(join)).flatten,
            other.fold(change.timestamp)(found => math.max(change.timestamp, found._2))
          )

  /** What Java's join processor for the right side makes of `change` and the left side's value `other`: an inner join's, a left join's right join,
    * which joins even a delete, and an outer join's.
    */
  def joinRight[K, A, B, R](
      joiner: Joiner[A, B, R],
      change: Change[K, B],
      other: Option[(A, Long)],
      sendOld: Boolean
  ): Option[(Option[R], Option[R], Long)] =
    val a = other.map(_._1)
    joiner match
      case Joiner.Inner(join) => other.map: (a, at) =>
          (change.value.map(join(a, _)), change.old.filter(_ => sendOld).map(join(a, _)), math.max(change.timestamp, at))
      case Joiner.Left(join) => other.map: (a, at) =>
          (Some(join(a, change.value)), Option.when(sendOld)(join(a, change.old)), math.max(change.timestamp, at))
      case Joiner.Outer(join) => Option.unless(other.isEmpty && change.value.isEmpty && change.old.isEmpty):
          (
            Ior.fromOptions(a, change.value).map(join),
            Option.when(sendOld)(Ior.fromOptions(a, change.old).map(join)).flatten,
            other.fold(change.timestamp)(found => math.max(change.timestamp, found._2))
          )

  /** A change to one side of a table-table join, as Java's join processors see it: the new value, or none for a delete, and the old value, none where
    * there was none.
    */
  final case class Change[K, A](key: K, value: Option[A], old: Option[A], timestamp: Long, headers: Headers)

  /** A time Java does not know, as it gives the value a suppression held from before its change. */
  val Unknown: Long = -1L

  /** The time of a record the broker or its producer stamps, as Java leaves a suppression buffer's changelog records unstamped. */
  val NoTimestamp: Long = -1L

  /** How often a step that emits windows once they close looks for closed windows, in wall-clock milliseconds: Java's
    * `emit.interval.ms.kstreams.windowed.aggregation`, an internal setting of a second.
    */
  val EmitIntervalMillis: Long = 1000L

  /** Bytes compared as unsigned, from the first, as Java orders a store's keys. */
  val unsigned: Ordering[Chunk[Byte]] =
    (a, b) =>
      a.toArray.zipAll(b.toArray, 0: Byte, 0: Byte).zipWithIndex.collectFirst {
        case ((x, y), _) if x != y => java.lang.Byte.toUnsignedInt(x) - java.lang.Byte.toUnsignedInt(y)
      }.getOrElse(a.size - b.size)
