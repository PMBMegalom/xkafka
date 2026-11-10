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
import cats.effect.{Concurrent, Ref}
import cats.syntax.all.*
import fs2.Chunk
import cats.data.Ior
import xkafka.streams.{
  Consumer, Entry, ForeignKeyJoined, HostedCall, HostedNode, HostedTask, Joiner, Origin, Processor, Punctuating, Punctuation, Record, RecordOrigin,
  Release, Source, Step, Store, StoreAccess, StreamJoin, StreamsJoined, Subtopology, TableKey, TablesJoined, TableValue, TableView, Window, Windowed
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
    partitionBy: Option[Chunk[Byte]] = None,
    choose: Option[Int => Option[Int]] = None
)

/** The latest value a store holds for a key, and its timestamp, as the bytes its changelog carries. */
private[xkafka] final case class Stored(value: Chunk[Byte], timestamp: Long)

/** What a task keeps between records: what it has written to each store since it last wrote to the stores, the partition time, the largest timestamp
  * it has read, and for each window store, the stream time its windowing steps have seen, each side of a cogroup its own, which starts again on each
  * member as Java's does, the store's clock (see [[TaskStore]]), taken from the store where the task has not yet kept it, and where its step emits
  * windows once they close, the close time it last emitted for, committed with the task's offsets, and the wall-clock time before which it does not
  * look for closed windows again, and when each punctuator runs next, those going by the clock apart from those going by stream time, in memory only,
  * as Java's schedules are.
  */
private[xkafka] final case class TaskState(
    pending: Map[String, StoreChanges],
    partitionTimes: Map[String, Long],
    streamTimes: Map[String, Long] = Map.empty,
    clocks: Map[String, Long] = Map.empty,
    lastEmitted: Map[String, Long] = Map.empty,
    nextEmit: Map[String, Long] = Map.empty,
    dirty: Map[String, Dirty] = Map.empty,
    buffers: Map[String, Buffered] = Map.empty,
    sequences: Map[String, Int] = Map.empty,
    joinTimes: Map[String, JoinTime] = Map.empty,
    reading: Option[TaskInput] = None,
    punctuations: Map[Boolean, PunctuationQueue] = Map.empty,
    hostedSchedules: Map[(String, Int), Punctuation] = Map.empty
):
  /** The state with `change` written to `key` of `store`. */
  def written(store: String, key: Chunk[Byte], change: Option[Stored]): TaskState =
    copy(pending = pending.updated(store, pending.getOrElse(store, StoreContents.unchanged).updated(key, change)))

  /** The state with `keys` removed from `store`. */
  def removed(store: String, keys: IterableOnce[Chunk[Byte]]): TaskState = keys.iterator.foldLeft(this)(_.written(store, _, None))

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

/** When punctuators run next, each named by its processor and its place in it, as a binary heap by time alone, as Java's `PriorityQueue` of them is,
  * so that those due together run in the order Java's do.
  */
private[xkafka] final case class PunctuationQueue(heap: Vector[((String, Int), Long)]):
  def next: Option[((String, Int), Long)] = heap.headOption

  /** With `added`, moved up past each later parent, as `PriorityQueue.offer` moves it. */
  def added(added: ((String, Int), Long)): PunctuationQueue =
    @scala.annotation.tailrec
    def up(placed: Vector[((String, Int), Long)], at: Int): Vector[((String, Int), Long)] =
      val parent = (at - 1) / 2
      if at == 0 || added._2 >= placed(parent)._2 then placed.updated(at, added) else up(placed.updated(at, placed(parent)), parent)
    PunctuationQueue(up(heap :+ added, heap.size))

  /** Without its first, the last moved down from there past each earlier child, the earlier child if tied the left, as `PriorityQueue.poll` moves it.
    */
  def polled: PunctuationQueue =
    val last = heap.last
    val left = heap.init
    @scala.annotation.tailrec
    def down(placed: Vector[((String, Int), Long)], at: Int): Vector[((String, Int), Long)] =
      val first = 2 * at + 1
      if first >= left.size then placed.updated(at, last)
      else
        val child = if first + 1 < left.size && placed(first)._2 > placed(first + 1)._2 then first + 1 else first
        if last._2 <= placed(child)._2 then placed.updated(at, last) else down(placed.updated(at, placed(child)), child)
    PunctuationQueue(if left.isEmpty then Vector.empty else down(left, 0))

private[xkafka] object TaskState:
  /** Nothing is written yet, and no partition time is known. */
  val empty: TaskState = TaskState(Map.empty, Map.empty)

/** One sub-topology on one partition: what it writes for each record it reads, as Java Streams writes it, without Java's record cache, so every
  * change to a store goes to its changelog and on downstream.
  */
private[xkafka] final class Task[F[_]](
    application: String,
    subtopology: Subtopology[F],
    partition: Int,
    stores: Map[String, TaskStore[F]] = Map.empty[String, TaskStore[F]],
    number: Int = 0,
    globals: Map[String, TaskStore[F]] = Map.empty[String, TaskStore[F]]
)(using F: MonadThrow[F], refs: Ref.Make[F]):
  /** The task as its hosted processors know it, whose instances they keep under it alone. */
  private val hostedTask = HostedTask(application, number, partition)

  /** How to read the time each key of the task's window, session and join window stores names. */
  private val timesOf: Map[String, TaskStore.TimeOf] = WindowStore.timesOf(List(subtopology))

  /** The hosted processors, by the name of their node. */
  private val hostedNodes: Map[String, HostedNode[F]] = subtopology.hosted.map(node => node.named -> node).toMap

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

  /** What reads each of the sub-topology's topics. */
  private val readers: Map[String, Entry[F]] =
    subtopology.entries.flatMap(entry => entry.origin.sources.map(Task.topicOf(application, _) -> entry)).toMap

  /** Runs `input` through what reads its topic at wall-clock time `now`, in epoch milliseconds. */
  def process(state: TaskState, input: TaskInput, now: Long): F[(TaskState, Vector[TaskOutput])] =
    val times = state.partitionTimes.updated(input.topic, math.max(state.partitionTime(input.topic), input.timestamp))
    val read  = state.copy(partitionTimes = times, reading = Some(input))
    // A record read outside the engine names no topic, and goes to the topic the sub-topology is named after.
    entered(readers.getOrElse(input.topic, subtopology.entries.head), read, input, now)

  /** Each punctuator with its place among its processor's. */
  private val scheduled: Map[(String, Int), (Punctuating[F], Int, Punctuation)] =
    subtopology.punctuating.flatMap(node => node.schedules.zipWithIndex.map((schedule, index) => (node.named, index) -> (node, index, schedule)))
      .toMap

  /** The punctuators in the order Java's processors schedule them. */
  private val declared: List[((String, Int), Punctuation)] =
    subtopology.punctuating.flatMap(node => node.schedules.zipWithIndex.map((schedule, index) => (node.named, index) -> schedule))

  /** Whether it has anything to run but what arrives: punctuators it declares, or hosted processors, which may schedule some as they run. */
  val punctuates: Boolean = declared.nonEmpty || hostedNodes.nonEmpty

  /** The state once each hosted processor's instance has started, at wall-clock time `now`, with the punctuators each scheduled as it started. Its
    * stores can be read as it starts, and not written to.
    */
  def opened(state: TaskState, now: Long): F[TaskState] =
    subtopology.hosted.foldLeftM(started(state, now)): (current, node) =>
      for
        working <- Ref.of[F, (TaskState, Vector[TaskOutput])]((current.copy(reading = None), Vector.empty))
        access  <- node.stores.traverse(store => accessTo(store, now, working).map(store -> _))
        _       <- node.hosting.start(hostedTask, hostedCall(access.toMap, current, now))
        ended   <- working.get
        _       <- F.raiseWhen(ended._2.nonEmpty)(new UnsupportedOperationException(s"the processor '${node.named}' wrote to a store as it started"))
        synced  <- schedulesOf(node, ended._1, now)
      yield synced

  /** Closes each hosted processor's instance. */
  def close: F[Unit] = subtopology.hosted.traverse_(_.hosting.close(hostedTask))

  /** What a hosted processor's instance is given for a call, with the task's stream time, or -1 before it has one, as Java gives it. */
  private def hostedCall(access: Map[String, StoreAccess[F]], state: TaskState, now: Long): HostedCall[F] =
    HostedCall(access, streamTime(state).getOrElse(-1L), now, state.reading.flatMap(originOf))

  /** The state with the punctuators `node`'s instance has as it stands: one it newly scheduled queued as Java queues one, one going by stream time at
    * 0, so that it runs as soon as there is a stream time, and one going by the clock an interval away; one it cancelled dropped, and skipped when
    * its time comes, as Java's queue skips it.
    */
  private def schedulesOf(node: HostedNode[F], state: TaskState, now: Long): F[TaskState] =
    node.hosting.schedules(hostedTask).map: live =>
      val mine   = state.hostedSchedules.keySet.filter(_._1 == node.named)
      val added  = live.filter((id, _) => !mine((node.named, id)))
      val queues =
        added.toList.sortBy(_._1).foldLeft(state.punctuations): (queues, entry) =>
          val (id, scheduled) = entry
          val schedule        = scheduled.punctuation
          val first           = scheduled.start.getOrElse(if schedule.wallClock then now + schedule.millis else 0L)
          queues.updated(schedule.wallClock, queues.getOrElse(schedule.wallClock, PunctuationQueue(Vector.empty)).added((node.named, id) -> first))
      val kept =
        state.hostedSchedules.filter((id, _) => id._1 != node.named) ++ live.map((id, scheduled) => (node.named, id) -> scheduled.punctuation)
      state.copy(punctuations = queues, hostedSchedules = kept)

  /** The time a task's stream-time punctuators go by, the latest time of a record read from any of its topics, if it has read one. */
  private def streamTime(state: TaskState): Option[Long] = state.partitionTimes.values.maxOption.filter(_ >= 0)

  /** The state with its punctuators scheduled at wall-clock time `now`, unless they are, as Java schedules them when it starts a task: one going by
    * stream time at 0, to run as soon as there is a stream time and at multiples of its interval after, and one going by the clock an interval away.
    */
  def started(state: TaskState, now: Long): TaskState =
    def queued(wallClock: Boolean, first: Punctuation => Long) =
      declared.filter(_._2.wallClock == wallClock)
        .foldLeft(PunctuationQueue(Vector.empty))((queue, entry) => queue.added(entry._1 -> first(entry._2)))
    if !punctuates || state.punctuations.nonEmpty then state
    else state.copy(punctuations = Map(false -> queued(false, _ => 0L), true -> queued(true, now + _.millis)))

  /** Whether a punctuator is due at wall-clock time `now`. */
  def due(state: TaskState, now: Long): Boolean =
    val ready                                   = started(state, now)
    def reached(wallClock: Boolean, time: Long) = ready.punctuations.get(wallClock).flatMap(_.next).exists(_._2 <= time)
    streamTime(ready).exists(reached(false, _)) || reached(true, now)

  /** Runs the punctuators due at wall-clock time `now`, as Java's task does after a record and whenever it finds none: those going by stream time and
    * then those going by the clock, each once, at the time now reached, and next at the first multiple of its interval from when it last ran that is
    * later.
    */
  def punctuate(state: TaskState, now: Long): F[(TaskState, Vector[TaskOutput])] =
    def run(ready: TaskState, written: Vector[TaskOutput], wallClock: Boolean, at: Long): F[(TaskState, Vector[TaskOutput])] =
      (ready, written).tailRecM: (current, out) =>
        val queue = current.punctuations(wallClock)
        queue.next.filter(_._2 <= at) match
          case None             => F.pure(Right((current, out)))
          case Some((id, time)) =>
            val moved                                             = current.copy(punctuations = current.punctuations.updated(wallClock, queue.polled))
            def again(schedule: Punctuation)                      = time + ((at - time) / schedule.millis + 1) * schedule.millis
            def requeued(after: TaskState, schedule: Punctuation) =
              after.copy(punctuations = after.punctuations.updated(wallClock, after.punctuations(wallClock).added(id -> again(schedule))))
            scheduled.get(id) match
              case Some((node, index, schedule)) => fired(node, index, at, moved, now)
                  .map((after, more) => Left((requeued(after, schedule), out ++ more)))
              case None => moved.hostedSchedules.get(id) match
                  // A punctuator its instance cancelled is dropped once its time comes, as Java drops it.
                  case None           => F.pure(Left((moved, out)))
                  case Some(schedule) => hostedFired(hostedNodes(id._1), id._2, at, moved, now).map: (after, more) =>
                      Left((requeued(after, schedule), out ++ more))
    if !punctuates then F.pure((state, Vector.empty))
    else
      val ready = started(state, now)
      streamTime(ready).fold(F.pure((ready, Vector.empty[TaskOutput])))(run(ready, Vector.empty, false, _)).flatMap: (after, written) =>
        run(after, written, true, now)

  /** What the punctuator at `index` of `node` makes at `at`: the records it returns, as it runs with what reads them, and what it wrote to its stores
    * at that time. No record is being read, so nothing downstream sees one.
    */
  private def fired(node: Punctuating[F], index: Int, at: Long, state: TaskState, now: Long): F[(TaskState, Vector[TaskOutput])] =
    for
      working <- Ref.of[F, (TaskState, Vector[TaskOutput])]((state.copy(reading = None), Vector.empty))
      access  <- node.stores.traverse(store => accessTo(store, at, working).map(store -> _))
      results <- node.punctuators(access.toMap)(index).run(Timestamp.fromEpochMillis(at))
      ended   <- working.get
      going = results.toVector.map(travelling)
      pushed <- allPushed(node.next, going, ended._1, ended._2, now)
    yield pushed

  /** What the punctuator `number` of the hosted processor `node` makes at `at`, as `fired` makes for a declared one, with the instance's punctuators
    * as they stand after it.
    */
  private def hostedFired(node: HostedNode[F], number: Int, at: Long, state: TaskState, now: Long): F[(TaskState, Vector[TaskOutput])] =
    for
      working <- Ref.of[F, (TaskState, Vector[TaskOutput])]((state.copy(reading = None), Vector.empty))
      access  <- node.stores.traverse(store => accessTo(store, at, working).map(store -> _))
      results <- node.hosting.punctuate(hostedTask, number, Timestamp.fromEpochMillis(at), hostedCall(access.toMap, state.copy(reading = None), now))
      ended   <- working.get
      synced  <- schedulesOf(node, ended._1, now)
      pushed  <- allPushed(node.next, results.toVector.map(travelling), synced, ended._2, now)
    yield pushed

  /** Writes what the task wrote since it last did to its stores, once the group has committed it. */
  def stored(state: TaskState): F[TaskState] =
    state.pending.toList.traverse_ : (name, changes) =>
      F.fromOption(stores.get(name), new IllegalStateException(s"the task has no store '$name'")).flatMap(_.write(changes))
    .as(state.copy(pending = Map.empty))

  /** What `store` holds, with what the task wrote since it last stored. */
  def contents(state: TaskState, store: String): F[StoreContents] = storedRange(state, store, None, None).map(StoreContents.of)

  /** The keys of `store` from `from`, included, to `to`, left out, and what they hold, in order, with what the task wrote since it last stored; a
    * bound not given leaves that side open.
    */
  private def storedRange(state: TaskState, store: String, from: Option[Chunk[Byte]], to: Option[Chunk[Byte]]): F[Vector[(Chunk[Byte], Stored)]] =
    stores.get(store).fold(F.pure(Vector.empty[(Chunk[Byte], Stored)]))(_.range(from, to)).map: held =>
      state.pending.get(store).fold(held): changes =>
        val changed = to.fold(from.fold(changes)(changes.rangeFrom))(until => from.fold(changes)(changes.rangeFrom).rangeUntil(until))
        if changed.isEmpty then held else StoreContents.of(held).after(changed).toVector

  /** What `store` holds at `key`, with what the task wrote since it last stored. */
  private def storedAt(state: TaskState, store: String, key: Chunk[Byte]): F[Option[Stored]] =
    state.pending.get(store).flatMap(_.get(key)).fold(stores.get(store).flatTraverse(_.get(key)))(F.pure)

  /** The keys of `store` starting with `prefix` and what they hold, in order, with what the task wrote since it last stored. */
  private def storedUnder(state: TaskState, store: String, prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Stored)]] =
    storedRange(state, store, Some(prefix), StoreContents.past(prefix))

  /** The keys of `store` naming a time from `from` to `to`, both included, and what they hold, in order of time and then of key, with what the task
    * wrote since it last stored.
    */
  private def storedBetween(state: TaskState, store: String, from: Long, to: Long): F[Vector[(Chunk[Byte], Stored)]] =
    stores.get(store).fold(F.pure(Vector.empty[(Chunk[Byte], Stored)]))(_.timed(from, to)).map: held =>
      (state.pending.get(store), timesOf.get(store)).tupled.fold(held): (changes, time) =>
        val changed = changes.filter((key, _) => time(key) >= from && time(key) <= to)
        val kept    = held.filterNot((key, _) => changed.contains(key))
        (kept ++ changed.toVector.collect { case (key, Some(stored)) => key -> stored })
          .sortBy((key, _) => (time(key), key))(using Ordering.Tuple2(using Ordering.Long, StoreContents.keys))

  /** The clock of `store`: the task's where it keeps one, or else the store's. Each step writing to a window, session or join store keeps the clock
    * it moves the store to, so the store's own is read only before the task first writes to it.
    */
  private def clockOf(state: TaskState, store: String): F[Long] =
    state.clocks.get(store).fold(stores.get(store).fold(F.pure(WindowStore.NoTime))(_.clock))(F.pure)

  /** The state with the keys of `store` naming a time of `until` or earlier removed. */
  private def swept(state: TaskState, store: String, until: Long): F[TaskState] =
    storedBetween(state, store, Long.MinValue, until).map(held => state.removed(store, held.iterator.map(_._1)))

  /** What the task's suppression buffers write to their changelogs at a commit, as Java's buffer logs on commit: each key changed since the last
    * commit, with what it holds and the header `v=3`, or a tombstone where it no longer holds the key. The changelog's broker gives each its time.
    */
  def flush(state: TaskState): F[(TaskState, Vector[TaskOutput])] =
    subtopology.buffers.toList.sorted.flatTraverse: store =>
      topicNamed(s"$application-$store-changelog").flatMap: changelog =>
        state.dirty.getOrElse(store, Dirty.empty).ordered.toList.traverse: key =>
          storedAt(state, store, key).map:
            case Some(held) => TaskOutput(changelog, Some(partition), Some(key), Some(held.value), Task.NoTimestamp, Held.Version)
            case None       => TaskOutput(changelog, Some(partition), Some(key), None, Task.NoTimestamp, Headers.empty)
    .map(written => (state.copy(dirty = state.dirty.view.mapValues(_.cleared).toMap), written.toVector))

  private def entered(entry: Entry[F], state: TaskState, input: TaskInput, now: Long): F[(TaskState, Vector[TaskOutput])] =
    read(entry.origin, input).flatMap(pushed(entry.consumer, _, state, now))

  /** The record `origin` gives for `input`. */
  private def read[K, V](origin: Origin[F, K, V], input: TaskInput): F[Travelling[K, V]] =
    def from(topic: F[Topic], key: xkafka.Serde[F, K], value: xkafka.Serde[F, V]) =
      for
        source <- topic
        k      <- key.deserializer.deserialize(source, input.headers, input.key)
        v      <- value.deserializer.deserialize(source, input.headers, input.value)
      yield Travelling(k, v, input.timestamp, input.headers)
    origin match
      case Origin.Input(_, topics, key, value)                 => from(F.pure(topics.find(_.value == input.topic).getOrElse(topics.head)), key, value)
      case Origin.Repartitioned(repartition, key, value, _, _) => from(topicNamed(s"$application-${repartition.value}-repartition"), key, value)
      case Origin.Exchanged(_, _)                              => F.pure(Travelling(input.key, input.value, input.timestamp, input.headers))

  /** `record` through `consumer` and all that reads what it gives, each record a step gives going all the way through before the next, as Java
    * forwards them, and what it writes in the order it writes it.
    */
  private def pushed[K, V](consumer: Consumer[F, K, V], record: Travelling[K, V], state: TaskState, now: Long): F[(TaskState, Vector[TaskOutput])] =
    consumer.expanded match
      case Consumer.Then(named, last, next) => step(last, record, state, now, next.requests, node = named.value).flatMap: (after, records, written) =>
          allPushed(next, records, after, written, now)
      case Consumer.Output(_, topic, key, value, partitioner) => written(F.pure(topic), key, value, false, record, partitioner).map((state, _))
      case Consumer.Repartitioning(repartition, key, value, _, _, _, aggregate, _, partitioner) =>
        written(topicNamed(s"$application-${repartition.value}-repartition"), key, value, true, record, partitioner)
          // Java drops a value that is null before a grouped table's repartition topic, which here is a change its serializer writes as none.
          .map(out => (state, if aggregate.isDefined then out.filter(_.value.isDefined) else out))
      case Consumer.Beside(_, _, next)  => pushed(next, record, state, now)
      case Consumer.Merged(_, next)     => pushed(next, record, state, now)
      case Consumer.Cogroup(_, _, next) => pushed(next, record, state, now)
      case Consumer.Copartitioned(next) => pushed(next, record, state, now)
      case Consumer.Refused(_, next)    => pushed(next, record, state, now)
      case Consumer.Fan(children)       => children.foldLeftM((state, Vector.empty[TaskOutput])):
          case ((current, out), child) => pushed(child, record, current, now).map((after, more) => (after, out ++ more))
      case Consumer.Splitting(_, branches) => branches.find(_.keep(record.key, record.value))
          .fold(F.pure((state, Vector.empty[TaskOutput])))(branch => pushed(branch.next, record, state, now))
      case Consumer.LeftTable(join) =>
        val change = Task.Change(record.key, join.left.present(record.value), record.old.flatMap(join.left.present), record.timestamp, record.headers)
        lookup(join.right.view, record.key, state)
          .flatMap(other => tablesJoined(join, record.key, record.headers, Task.joinLeft(join.joiner, change, other, join.next.requests), state, now))
      case Consumer.RightTable(join) =>
        val change =
          Task.Change(record.key, join.right.present(record.value), record.old.flatMap(join.right.present), record.timestamp, record.headers)
        lookup(join.left.view, record.key, state).flatMap(other =>
          tablesJoined(join, record.key, record.headers, Task.joinRight(join.joiner, change, other, join.next.requests), state, now)
        )
      case Consumer.LeftStream(join)         => streamsJoined(join, leftSide(join), record, state, now)
      case Consumer.RightStream(join)        => streamsJoined(join, rightSide(join), record, state, now)
      case Consumer.ForeignLeft(join)        => foreignSent(join, record, state)
      case Consumer.ForeignRight(join)       => foreignChanged(join, record, state)
      case Consumer.ForeignSubscribed(join)  => foreignSubscribed(join, record, state)
      case Consumer.ForeignAnswered(join)    => foreignAnswered(join, record, state, now)
      case Consumer.Done()                   => F.pure((state, Vector.empty))
      case mapped @ Consumer.Mapped(_, _, _) => pushed(mapped.expanded, record, state, now)

  /** Each of `records` through `consumer`, in order, after what has been `written`. */
  private def allPushed[K, V](
      consumer: Consumer[F, K, V],
      records: Vector[Travelling[K, V]],
      state: TaskState,
      written: Vector[TaskOutput],
      now: Long
  ): F[(TaskState, Vector[TaskOutput])] =
    records.foldLeftM((state, written)):
      case ((current, out), record) => pushed(consumer, record, current, now).map((after, more) => (after, out ++ more))

  /** What a table join's processor made of a change, going on as the result's new value, its old one and its time, keyed and with the headers of the
    * change, or not at all; as Java's merger does, kept in the join's store where it has one, and passed on.
    */
  private def tablesJoined[K, A, B, R, C1, C2](
      join: TablesJoined[F, K, A, B, R, C1, C2],
      key: K,
      headers: Headers,
      made: Option[(Option[R], Option[R], Long)],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[TaskOutput])] =
    made match
      case None                          => F.pure((state, Vector.empty))
      case Some((value, old, timestamp)) =>
        val result = Travelling[K, Option[R]](key, value, timestamp, headers, old.map(Some(_)))
        join.store match
          case None                        => pushed(join.next, result, state, now)
          case Some((named, keys, values)) => topicNamed(s"$application-${named.value}-changelog").flatMap: changelog =>
              (keys.serializer.serialize(changelog, Headers.empty, key), value.flatTraverse(values.serializer.serialize(changelog, Headers.empty, _)))
                .tupled.flatMap:
                  case (None, _)              => pushed(join.next, result, state, now)
                  case (Some(bytes), written) =>
                    val log = TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)
                    allPushed(join.next, Vector(result), state.written(named.value, bytes, written.map(Stored(_, timestamp))), Vector(log), now)

  /** A change to a foreign-key join's left table: the subscriptions it sends, as Java's `SubscriptionSendProcessorSupplier` sends them. Moving to
    * another key first deletes the subscription to the old one, on what may be another task. A row answers with a result only where its join needs
    * one.
    */
  private def foreignSent[K, V, KO, VO, R, C1, C2](
      join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2],
      record: Travelling[K, C1],
      state: TaskState
  ): F[(TaskState, Vector[TaskOutput])] =
    import ForeignKeyWire.*
    val row                                    = join.left.present(record.value)
    val old                                    = record.old.flatMap(join.left.present)
    def keyOf(value: Option[V])                = value.flatMap(join.foreignKey(record.key, _))
    def bytesOf(key: Option[KO], topic: Topic) = key.flatTraverse(join.otherKeys.serializer.serialize(topic, record.headers, _))
    val (before, now)                          = (keyOf(old), keyOf(row))
    (
      topicNamed(s"$application-${join.subscriptions}"),
      topicNamed(s"$application-${join.subscriptions}-fk"),
      topicNamed(s"$application-${join.subscriptions}-pk"),
      topicNamed(s"$application-${join.subscriptions}-vh")
    ).tupled.flatMap: (topic, keyTopic, primaryTopic, valueTopic) =>
      for
        moved <- (bytesOf(before, keyTopic), bytesOf(now, keyTopic)).mapN(_ != _)
        sends =
          if join.keepsUnmatched then
            if old.isEmpty && row.isEmpty then Nil
            else (if before.isDefined && moved then List(before -> DeleteNoPropagate) else Nil) :+ (now -> PropagateNullIfMissing)
          else
            (row, now, before) match
              case (None, _, Some(_))                   => List(before -> DeleteAndPropagate)
              case (None, _, None)                      => Nil
              case (Some(_), None, Some(_))             => List(before -> DeleteAndPropagate)
              case (Some(_), None, None)                => Nil
              case (Some(_), Some(_), Some(_)) if moved => List(before -> DeleteNoPropagate, now -> PropagateNullIfMissing)
              case (Some(_), Some(_), _)                => List(now -> PropagateOnlyIfFound)
        hash    <- row.flatTraverse(join.values.serializer.serialize(valueTopic, record.headers, _)).map(_.map(Murmur3.hash128))
        primary <- join.keys.serializer.serialize(primaryTopic, record.headers, record.key)
        out     <-
          primary.toVector.flatMap(key => sends.map(_ -> key)).traverse:
            case ((target, instruction), key) => bytesOf(target, topic).map: bytes =>
                val value = Subscription(hash, instruction, key, partition).encoded
                // A subscription goes to the partition that holds its foreign key in the other table's topic, however that topic is partitioned.
                val chosen =
                  target.flatMap(foreign =>
                    join.otherPartitioner.map(chooser => (count: Int) => chooser.chosen(topic, foreign, (), record.headers, count))
                  )
                TaskOutput(topic, None, bytes, Some(value), record.timestamp, record.headers, choose = chosen)
      yield (state, out)

  /** A change to a foreign-key join's other table: an answer to each subscription to its key, at its time and with its headers, as Java's
    * `ForeignTableJoinProcessorSupplier` answers them.
    */
  private def foreignChanged[K, V, KO, VO, R, C1, C2](
      join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2],
      record: Travelling[KO, C2],
      state: TaskState
  ): F[(TaskState, Vector[TaskOutput])] =
    import ForeignKeyWire.*
    (topicNamed(s"$application-${join.subscriptions}-fk"), topicNamed(s"$application-${join.responses}")).tupled.flatMap: (keyTopic, topic) =>
      join.otherKeys.serializer.serialize(keyTopic, record.headers, record.key).flatMap:
        case None        => F.pure((state, Vector.empty))
        case Some(bytes) =>
          for
            found <- join.right.present(record.value).flatTraverse(join.otherValues.serializer.serialize(topic, record.headers, _))
            held  <- storedUnder(state, join.subscribed.value, prefixOf(bytes))
          yield
            val answers =
              held.flatMap: (combined, stored) =>
                Subscription.decoded(stored.value).map: subscription =>
                  val answer = Response(subscription.hash, found).encoded
                  TaskOutput(
                    topic,
                    Some(subscription.primaryPartition),
                    Some(primaryOf(combined, bytes)),
                    Some(answer),
                    record.timestamp,
                    record.headers
                  )
            (state, answers)

  /** A subscription as it reaches the other table's side: kept or deleted, and answered as it asks, as Java's `SubscriptionReceiveProcessorSupplier`
    * and `SubscriptionJoinProcessorSupplier` do.
    */
  private def foreignSubscribed[K, V, KO, VO, R, C1, C2](
      join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2],
      record: Travelling[Option[Chunk[Byte]], Option[Chunk[Byte]]],
      state: TaskState
  ): F[(TaskState, Vector[TaskOutput])] =
    import ForeignKeyWire.*
    def unreadable = new IllegalStateException(s"the foreign-key join '${join.named.value}' read a subscription it cannot read")
    val store      = join.subscribed.value
    record.value.flatMap(Subscription.decoded).liftTo[F](unreadable).flatMap: subscription =>
      val deleting = subscription.instruction == DeleteAndPropagate || subscription.instruction == DeleteNoPropagate
      (topicNamed(s"$application-$store-changelog"), topicNamed(s"$application-${join.subscriptions}"), topicNamed(s"$application-${join.responses}"))
        .tupled.flatMap: (changelog, topic, responses) =>
          // Java drops a subscription without a foreign key unless it needs an answer, which that key then finds nothing for.
          record.key match
            case None if subscription.instruction != PropagateNullIfMissing => F.pure((state, Vector.empty))
            case key                                                        =>
              val kept            = key.map(combinedOf(_, subscription.primaryKey))
              val (after, logged) =
                kept.fold((state, Vector.empty[TaskOutput])): combined =>
                  val value = Option.when(!deleting)(record.value).flatten
                  (
                    state.written(store, combined, value.map(Stored(_, record.timestamp))),
                    Vector(TaskOutput(changelog, Some(partition), Some(combined), value, record.timestamp, record.headers))
                  )
              for
                foreign <-
                  key.flatTraverse(bytes =>
                    join.otherKeys.deserializer.deserialize(topic, record.headers, Some(bytes)).flatMap(lookup(join.right.view, _, after))
                  )
                bytes <- foreign.flatTraverse(found => join.otherValues.serializer.serialize(responses, record.headers, found._1))
              yield
                def answer(found: Option[Chunk[Byte]]) =
                  val at = foreign.fold(record.timestamp)(found => math.max(record.timestamp, found._2))
                  Vector(TaskOutput(
                    responses,
                    Some(subscription.primaryPartition),
                    Some(subscription.primaryKey),
                    Some(Response(subscription.hash, found).encoded),
                    at,
                    record.headers
                  ))
                val answered =
                  subscription.instruction match
                    case DeleteAndPropagate     => answer(None)
                    case PropagateNullIfMissing => answer(bytes)
                    case PropagateOnlyIfFound   => if foreign.isDefined then answer(bytes) else Vector.empty
                    case _                      => Vector.empty
                (after, logged ++ answered)

  /** An answer as it reaches the left table's side: dropped where the row has changed since it was asked for, and otherwise made into a result, as
    * Java's `ResponseJoinProcessorSupplier` does.
    */
  private def foreignAnswered[K, V, KO, VO, R, C1, C2](
      join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2],
      record: Travelling[Option[Chunk[Byte]], Option[Chunk[Byte]]],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[TaskOutput])] =
    import ForeignKeyWire.*
    def unreadable = new IllegalStateException(s"the foreign-key join '${join.named.value}' read an answer it cannot read")
    (record.key, record.value.flatMap(Response.decoded)).tupled.liftTo[F](unreadable).flatMap: (primary, response) =>
      (topicNamed(s"$application-${join.responses}"), topicNamed(s"$application-${join.subscriptions}-vh")).tupled.flatMap: (topic, valueTopic) =>
        for
          key     <- join.keys.deserializer.deserialize(topic, record.headers, Some(primary))
          current <- lookup(join.left.view, key, state)
          hash    <- current.flatTraverse(row => join.values.serializer.serialize(valueTopic, record.headers, row._1)).map(_.map(Murmur3.hash128))
          done    <-
            if hash != response.hash then F.pure((state, Vector.empty[TaskOutput]))
            else
              response.foreign.traverse(bytes => join.otherValues.deserializer.deserialize(topic, record.headers, Some(bytes))).flatMap: found =>
                val made = current.flatMap(row => join.joiner(row._1, found))
                foreignResult(join, key, made, primary, record, state, now)
        yield done

  /** A foreign-key join's result going on, kept in its store where it has one, with the value it replaces. */
  private def foreignResult[K, V, KO, VO, R, C1, C2](
      join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2],
      key: K,
      made: Option[R],
      primary: Chunk[Byte],
      record: Travelling[Option[Chunk[Byte]], Option[Chunk[Byte]]],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[TaskOutput])] =
    def result(old: Option[R]) = Travelling[K, Option[R]](key, made, record.timestamp, record.headers, Some(old))
    join.store match
      case None                  => pushed(join.next, result(None), state, now)
      case Some((named, values)) => topicNamed(s"$application-${named.value}-changelog").flatMap: changelog =>
          for
            held    <- storedAt(state, named.value, primary)
            old     <- held.traverse(stored => values.deserializer.deserialize(changelog, Headers.empty, Some(stored.value)))
            written <- made.flatTraverse(values.serializer.serialize(changelog, Headers.empty, _))
            log = TaskOutput(changelog, Some(partition), Some(primary), written, record.timestamp, Headers.empty)
            done <-
              allPushed(
                join.next,
                Vector(result(old)),
                state.written(named.value, primary, written.map(Stored(_, record.timestamp))),
                Vector(log),
                now
              )
          yield done

  /** One side of a stream-stream join as Java's processor for it sees it: its own window store and the other side's, the serdes of their values, how
    * far before and after a record's time it looks in the other side's store, whether its records that match nothing go on, whether it is the left
    * side, and what the joiner makes of a match and of a record that matches nothing.
    */
  private final case class JoinSide[X, Y, R](
      own: String,
      other: String,
      values: xkafka.Serde[F, X],
      others: xkafka.Serde[F, Y],
      before: Long,
      after: Long,
      outer: Boolean,
      left: Boolean,
      both: (X, Y) => R,
      alone: X => Option[R]
  )

  /** The left side of streams joined, as Java's processor for it sees it. */
  private def leftSide[K, A, B, R](join: StreamsJoined[F, K, A, B, R]): JoinSide[A, B, R] =
    val names = StreamJoin.names(join.store, join.joiner)
    JoinSide(
      names.left,
      names.right,
      join.value,
      join.otherValue,
      join.windows.beforeMillis,
      join.windows.afterMillis,
      join.joiner.outer._1,
      true,
      join.joiner.both,
      join.joiner.leftAlone
    )

  /** The right side of streams joined: Java's right processor looks the other way, so its before and after swap, and calls the joiner with its
    * arguments reversed.
    */
  private def rightSide[K, A, B, R](join: StreamsJoined[F, K, A, B, R]): JoinSide[B, A, R] =
    val names = StreamJoin.names(join.store, join.joiner)
    JoinSide[B, A, R](
      names.right,
      names.left,
      join.otherValue,
      join.value,
      join.windows.afterMillis,
      join.windows.beforeMillis,
      join.joiner.outer._2,
      false,
      (b, a) => join.joiner.both(a, b),
      join.joiner.rightAlone
    )

  /** What two streams joined make of `record` of one side, each result going on to what reads the join. */
  private def streamsJoined[K, A, B, R, X, Y](
      join: StreamsJoined[F, K, A, B, R],
      side: JoinSide[X, Y, R],
      record: Travelling[K, X],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[TaskOutput])] =
    joinedRecord(join, side, record, state, now).flatMap((after, results, written) => allPushed(join.next, results, after, written, now))

  /** What `side`'s processor makes of `record`, as Java's does (`KStreamKStreamJoin`): the join's stream time moves to the record's; a record whose
    * key is written as none goes on alone at once where the side's records that match nothing go on, and is otherwise dropped. A record at the stream
    * time first sends on the held records whose windows have closed. The record then joins each of the other side's records with its key in its
    * window, earliest first, each result at the later of their times, and each match no longer held as matching nothing; a record that matches
    * nothing, on a side whose such records go on, goes on alone at once if its window has already closed, or is held until it closes. Last, the
    * record goes in its side's window store, and to its changelog with its own time and headers.
    */
  private def joinedRecord[K, A, B, R, X, Y](
      join: StreamsJoined[F, K, A, B, R],
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
              .flatMap(_.traverse((at, held) => side.others.deserializer.deserialize(other, Headers.empty, Some(held.value)).map(at -> _))).flatMap:
                matches =>
                  val unheld =
                    sharing.fold(F.pure((afterEmitting, Vector.empty[TaskOutput]))): (name, log) =>
                      matches.foldLeftM((afterEmitting, Vector.empty[TaskOutput])):
                        case ((current, logged), (at, _)) => unhold(current, name, log, SharedJoinStore.key(at, !side.left, bytes), timestamp).map:
                            case Some((after, tombstone)) => (after, logged :+ tombstone)
                            case None                     => (current, logged)
                  unheld.flatMap: (afterMatching, unlogged) =>
                    val joined = matches.map((at, value) => result(side.both(record.value, value), math.max(timestamp, at)))
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
  private def joinedOthers(state: TaskState, store: String, bytes: Chunk[Byte], from: Long, to: Long): F[Vector[(Long, Stored)]] =
    storedUnder(state, store, bytes).map: held =>
      held.collect:
        case (found, stored) if found.size == bytes.size + 12 => (WindowStore.startOf(found), JoinWindowStore.sequenceOf(found), stored)
      .filter((at, _, _) => at >= from && at <= to).sortBy((at, sequence, _) => (at, sequence)).map((at, _, stored) => (at, stored))

  /** The join's shared store without the records held at `key`, and the tombstone its changelog gets, where it holds any, as Java's
    * `putIfAbsent(key, null)` removes them.
    */
  private def unhold(state: TaskState, shared: String, changelog: Topic, key: Chunk[Byte], timestamp: Long): F[Option[(TaskState, TaskOutput)]] =
    storedAt(state, shared, key).map: held =>
      held.map(_ => (state.written(shared, key, None), TaskOutput(changelog, Some(partition), Some(key), None, timestamp, Headers.empty)))

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
      found   <- storedAt(state, shared, key)
      held    <- found.traverse(stored => F.fromOption(SharedJoinStore.elements(stored.value), unreadableJoin(shared)))
    yield
      val list  = SharedJoinStore.list(held.getOrElse(Vector.empty) :+ SharedJoinStore.element(side.left, value))
      val time  = state.joinTimes.getOrElse(shared, JoinTime.initial)
      val after =
        state.written(shared, key, Some(Stored(list, record.timestamp)))
          .copy(joinTimes = state.joinTimes.updated(shared, time.copy(minTime = math.min(time.minTime, record.timestamp))))
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
    (side.values.serializer.serialize(changelog, record.headers, record.value), clockOf(state, side.own)).tupled.flatMap: (written, clock) =>
      val sequence = JoinWindowStore.next(state.sequences.getOrElse(side.own, 0))
      val ticked   = math.max(clock, record.timestamp)
      val key      = JoinWindowStore.key(bytes, record.timestamp, sequence)
      val kept     =
        if WindowStore.expired(record.timestamp, ticked, retention) then state
        else written.fold(state)(value => state.written(side.own, key, Some(Stored(value, record.timestamp))))
      val pruned = if ticked > clock then swept(kept, side.own, ticked - retention) else F.pure(kept)
      pruned.map: after =>
        val moved = after.copy(clocks = after.clocks.updated(side.own, ticked), sequences = after.sequences.updated(side.own, sequence))
        (moved, TaskOutput(changelog, Some(partition), Some(key), written, record.timestamp, record.headers))

  /** The held records whose windows have closed, sent on alone and removed from the join's shared store, as Java's join sends them
    * (`emitNonJoinedOuterRecords`): only once the earliest time held, with the processor's `after` and the grace, is behind the stream time, and no
    * sooner by the wall clock than the interval after the last look. Java goes through the store in its order, the earliest time held moving to each
    * record's; a record whose window is still open marks its side as open and is passed over, and once both sides are, the look ends. Each record
    * sent goes on with its own time and key and the headers of the record that made the look; each key sent from is removed, its changelog getting a
    * tombstone with that record's time.
    */
  private def nonJoined[K, A, B, R](
      join: StreamsJoined[F, K, A, B, R],
      shared: String,
      changelog: Topic,
      after: Long,
      headers: Headers,
      timestamp: Long,
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    import join.windows
    val time = state.joinTimes.getOrElse(shared, JoinTime.initial)
    // Java adds these as longs, so an earliest time of none wraps below the stream time.
    if time.minTime + after + windows.graceMillis >= time.streamTime || now < time.nextEmit then F.pure((state, Vector.empty, Vector.empty))
    else
      contents(state, shared).flatMap: held =>
        held.toVector.flatTraverse: (key, stored) =>
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
          val after =
            state.removed(shared, removed)
              .copy(joinTimes = state.joinTimes.updated(shared, time.copy(minTime = looked.minTime, nextEmit = now + JoinTime.EmitInterval)))
          (after, results, removed.map(key => TaskOutput(changelog, Some(partition), Some(key), None, timestamp, Headers.empty)))

  /** The record with what `step` makes of it and its key's value in the table, as Java's stream-table join makes it when the record comes. */
  private def joined[K, V, TV, R](
      step: Step.Join[F, K, V, TV, R],
      record: Travelling[K, V],
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    valueOf(step.view, record.key, state)
      .map(table => (state, step.join(record.value, table.map(_._1)).toVector.map(v => record.copy(value = v, old = None)), Vector.empty))

  /** The record with what `step` makes of it and the value its global table holds for the key it selects, read as the table stands when the record
    * comes. A key its serde writes as none finds nothing, as Java's finds nothing for a null key.
    */
  private def globallyJoined[K, V, GK, GV, R](
      step: Step.GlobalJoin[F, K, V, GK, GV, R],
      record: Travelling[K, V],
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K, R]], Vector[TaskOutput])] =
    import step.table
    table.key.serializer.serialize(table.topic, Headers.empty, step.select(record.key, record.value)).flatMap: written =>
      written.flatTraverse(key => globals.get(table.store.value).flatTraverse(_.get(key)))
        .flatMap(_.traverse(held => table.value.deserializer.deserialize(table.topic, Headers.empty, Some(held.value))))
        .map(found => (state, step.join(record.value, found).toVector.map(v => record.copy(value = v, old = None)), Vector.empty))

  /** The other side's value for `key` and its time, where its view finds one; a windowed table has no view, and its joins are refused. */
  private def lookup[K, V](view: Option[TableView[F, K, V]], key: K, state: TaskState): F[Option[(V, Long)]] =
    view.flatTraverse(valueOf(_, key, state))

  private def step[K, V, K2, V2](
      step: Step[F, K, V, K2, V2],
      record: Travelling[K, V],
      state: TaskState,
      now: Long,
      asked: => Boolean,
      clock: Option[Int] = None,
      node: String = ""
  ): F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])] =
    def passing(records: Vector[Travelling[K2, V2]]) = F.pure((state, records, Vector.empty[TaskOutput]))
    step match
      case Step.Filter(keep) => passing(if keep(record.key, record.value) then Vector(record) else Vector.empty)
      case Step.Map(change)  =>
        val (k, v) = change(record.key, record.value)
        passing(Vector(Travelling(k, v, record.timestamp, record.headers)))
      case Step.MapValues(change)     => passing(Vector(record.copy(value = change(record.value), old = None)))
      case Step.FlatMapValues(change) => passing(change(record.value).iterator.map(value => record.copy(value = value, old = None)).toVector)
      case Step.FlatMap(change)       =>
        passing(change(record.key, record.value).toVector.map((k, v) => Travelling(k, v, record.timestamp, record.headers)))
      case Step.Peek(effect)          => effect(record.key, record.value) *> passing(Vector(record))
      case Step.EvalMapValues(change) => change(record.value).flatMap(v => passing(Vector(record.copy(value = v, old = None))))
      case Step.EvalFilter(keep)      => keep(record.key, record.value).flatMap(kept => passing(if kept then Vector(record) else Vector.empty))
      case Step.EvalMap(change)       => change(record.key, record.value)
          .flatMap((k, v) => passing(Vector(Travelling(k, v, record.timestamp, record.headers))))
      case Step.RecordFilter(keep)            => passing(if keep(asRead(record, state)) then Vector(record) else Vector.empty)
      case Step.RecordPeek(effect)            => effect(asRead(record, state)) *> passing(Vector(record))
      case Step.EvalRecordFilter(keep)        => keep(asRead(record, state)).flatMap(kept => passing(if kept then Vector(record) else Vector.empty))
      case Step.RecordMap(change)             => passing(change(asRead(record, state)).iterator.map(travelling).toVector)
      case Step.EvalRecordMap(change, result) => change(asRead(record, state)).flatMap(made => passing(Vector(travelling(result(made)))))
      case Step.ToStream()                    => passing(Vector(record))
      case folding @ Step.WindowedAggregate(_, _, _, _, _, _, _, _, _)   => windowed(folding, record, state, now, clock)
      case folding @ Step.SlidingAggregate(_, _, _, _, _, _, _, _, _)    => sliding(folding, record, state, now, clock)
      case folding @ Step.SessionAggregate(_, _, _, _, _, _, _, _, _, _) => session(folding, record, state, now, clock)
      case Step.Cogrouped(side, folding)                                 => this.step(folding, record, state, now, asked, Some(side))
      case Step.Process(processor, _)                                    => processed(processor, record, state, node, now)
      case holding @ Step.Suppress(_, _, _, _)                           => suppress(holding, record, state)
      case joining @ Step.Join(_, _)                                     => joined(joining, record, state)
      case joining @ Step.GlobalJoin(_, _, _)                            => globallyJoined(joining, record, state)
      case filtering @ Step.TableFilter(_, _, _)                         => tableFiltered(filtering, record, state)
      case mapping @ Step.TableMapValues(_, _, _, _)                     => tableMapped(mapping, record, state, asked)
      case Step.ToTable(lift, store, key, value)                         => topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
          key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
            // Java skips a record whose key is null, which here is a key its serializer writes as none.
            case None        => F.pure((state, Vector.empty, Vector.empty))
            case Some(bytes) => tableKept(store, value, changelog, record.key, bytes, lift(record.value), record.timestamp, record.headers, state)
      case Step.TableSource(store, key, value) => topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
          record.key.flatTraverse(k => key.serializer.serialize(changelog, Headers.empty, k).map(_.map(k -> _))).flatMap:
            // Java skips a record whose key is null: one read without a key, or one its serializer writes as none.
            case None             => F.pure((state, Vector.empty, Vector.empty))
            case Some((k, bytes)) => tableKept(store, value, changelog, k, bytes, record.value, record.timestamp, record.headers, state)
      case Step.Aggregate(store, key, value, initial, start, add) => topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
          key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
            // Java skips a record whose key is null, which here is a key its serializer writes as none.
            case None        => F.pure((state, Vector.empty, Vector.empty))
            case Some(bytes) => storedAt(state, store.value, bytes).flatMap: held =>
                val updated =
                  held match
                    case None      => F.pure((start(record.key, record.value), record.timestamp, initial))
                    case Some(old) => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value)).map: previous =>
                        (add(record.key, record.value, previous), math.max(old.timestamp, record.timestamp), Some(previous))
                updated.flatMap: (aggregate, timestamp, replaced) =>
                  value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
                    val after  = state.written(store.value, bytes, written.map(Stored(_, timestamp)))
                    val logged = TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)
                    (after, Vector(Travelling(record.key, aggregate, timestamp, record.headers, replaced)), Vector(logged))
      case Step.TableGroupBy(grouped) =>
        passing(grouped(record.key, record.value, record.old).map((k, v) => Travelling(k, v, record.timestamp, record.headers)))
      case Step.TableAggregate(store, key, value, start, add, subtract) => topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
          key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
            case None        => F.pure((state, Vector.empty, Vector.empty))
            case Some(bytes) => storedAt(state, store.value, bytes).flatMap: held =>
                held.traverse(old => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value))).flatMap: replaced =>
                  val taken   = record.value.old.flatMap(left => replaced.map(subtract(record.key, left, _))).orElse(replaced)
                  val updated = record.value.current.fold(taken)(joined => Some(taken.fold(start(record.key, joined))(add(record.key, joined, _))))
                  val at      = held.fold(record.timestamp)(old => math.max(old.timestamp, record.timestamp))
                  tableKept(store, value, changelog, record.key, bytes, updated, at, record.headers, state)

  /** The change `next` makes to the key written as `bytes` in a table's `store`, logged to `changelog`, going on with the value it replaced. */
  private def tableKept[K, V](
      store: xkafka.streams.Store,
      value: xkafka.Serde[F, V],
      changelog: Topic,
      key: K,
      bytes: Chunk[Byte],
      next: Option[V],
      timestamp: Long,
      headers: Headers,
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K, Option[V]]], Vector[TaskOutput])] =
    storedAt(state, store.value, bytes).flatMap(_.traverse(held => value.deserializer.deserialize(changelog, Headers.empty, Some(held.value))))
      .flatMap: replaced =>
        next.flatTraverse(value.serializer.serialize(changelog, Headers.empty, _)).map: written =>
          val after  = state.written(store.value, bytes, written.map(Stored(_, timestamp)))
          val logged = TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)
          (after, Vector(Travelling(key, next, timestamp, headers, Some(replaced))), Vector(logged))

  /** What a change to `key` does to the store a filter or `mapValues` keeps its result in, as Java's does: the value it replaced, then the state with
    * the change kept and logged; or nothing where its key is written as none, or where `dropEmpty` says a change from no value to no value goes no
    * further, so it is not kept either.
    */
  private def keptResult[K, V](
      kept: (xkafka.streams.Store, TableKey[F, K], xkafka.Serde[F, V]),
      key: K,
      next: Option[V],
      timestamp: Long,
      state: TaskState,
      dropEmpty: Boolean
  ): F[Option[(TaskState, Option[V], TaskOutput)]] =
    val (store, keys, value) = kept
    topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
      bufferKey(keys).serializer.serialize(changelog, Headers.empty, key).flatMap:
        case None        => F.pure(None)
        case Some(bytes) => storedAt(state, store.value, bytes)
            .flatMap(_.traverse(held => value.deserializer.deserialize(changelog, Headers.empty, Some(held.value)))).flatMap: replaced =>
              if dropEmpty && replaced.isEmpty && next.isEmpty then F.pure(None)
              else
                next.flatTraverse(value.serializer.serialize(changelog, Headers.empty, _)).map: written =>
                  val after = state.written(store.value, bytes, written.map(Stored(_, timestamp)))
                  Some((after, replaced, TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)))

  /** What `processor` makes of `record`: the records it returns, and what it wrote to its stores, each write kept and logged as it is made, as Java's
    * logging store does, with the record's time. Nothing it did counts if it fails.
    */
  private def processed[K, V, K2, V2](
      processor: Processor[F, K, V, K2, V2],
      record: Travelling[K, V],
      state: TaskState,
      node: String,
      now: Long
  ): F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])] =
    for
      working <- Ref.of[F, (TaskState, Vector[TaskOutput])]((state, Vector.empty))
      access  <- processor.kept.traverse(kept => accessTo(kept.store.value, record.timestamp, working).map(kept.store.value -> _))
      results <-
        processor.hosting.fold(processor.handling(access.toMap).onRecord(asRead(record, state))): hosting =>
          hosting.process(hostedTask, asRead(record, state), hostedCall(access.toMap, state, now))
      ended  <- working.get
      synced <- hostedNodes.get(node).filter(_ => processor.hosting.isDefined).fold(F.pure(ended._1))(schedulesOf(_, ended._1, now))
    yield (synced, results.toVector.map(travelling), ended._2)

  /** Where the record being read came from. */
  private def originOf(input: TaskInput): Option[RecordOrigin] =
    (Topic.from(input.topic).toOption, Partition.from(input.partition).toOption, Offset.from(input.offset).toOption).mapN(RecordOrigin(_, _, _))

  /** `record` as a program's own code reads it, with where it came from. */
  private def asRead[K, V](record: Travelling[K, V], state: TaskState): Record[K, V] =
    Record(record.key, record.value, Timestamp.fromEpochMillis(record.timestamp), record.headers, state.reading.flatMap(originOf))

  /** What a program's own code returned, going on to the next step. */
  private def travelling[K, V](result: Record[K, V]): Travelling[K, V] =
    Travelling(result.key, result.value, result.timestamp.epochMillis, result.headers)

  /** `store` as a processor reads and writes it, through `working`, each write at `timestamp`. */
  private def accessTo(store: String, timestamp: Long, working: Ref[F, (TaskState, Vector[TaskOutput])]): F[StoreAccess[F]] =
    topicNamed(s"$application-$store-changelog").map: changelog =>
      new StoreAccess[F]:
        val topic: Topic = changelog

        def get(key: Chunk[Byte]): F[Option[Chunk[Byte]]] = working.get.flatMap((state, _) => storedAt(state, store, key)).map(_.map(_.value))

        def put(key: Chunk[Byte], value: Option[Chunk[Byte]]): F[Unit] =
          working.update: (state, logged) =>
            (
              state.written(store, key, value.map(Stored(_, timestamp))),
              logged :+ TaskOutput(changelog, Some(partition), Some(key), value, timestamp, Headers.empty)
            )

        def between(from: Chunk[Byte], to: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]] =
          working.get.flatMap((state, _) => storedRange(state, store, Some(from), Some(to ++ Chunk.singleton(0.toByte))))
            .map(_.map((key, held) => (key, held.value)))

        def all: F[Vector[(Chunk[Byte], Chunk[Byte])]] =
          working.get.flatMap((state, _) => storedRange(state, store, None, None)).map(_.map((key, held) => (key, held.value)))

        def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]] =
          working.get.flatMap((state, _) => storedUnder(state, store, prefix)).map(_.map((key, held) => (key, held.value)))

        def timed(key: Chunk[Byte]): F[Option[(Chunk[Byte], Long)]] =
          working.get.flatMap((state, _) => storedAt(state, store, key)).map(_.map(held => (held.value, held.timestamp)))

        def putAt(key: Chunk[Byte], value: Option[Chunk[Byte]], at: Long): F[Unit] =
          working.update: (state, logged) =>
            (
              state.written(store, key, value.map(Stored(_, at))),
              logged :+ TaskOutput(changelog, Some(partition), Some(key), value, at, Headers.empty)
            )

        def timedScan(prefix: Option[Chunk[Byte]]): F[Vector[(Chunk[Byte], Chunk[Byte], Long)]] =
          working.get.flatMap((state, _) => prefix.fold(storedRange(state, store, None, None))(storedUnder(state, store, _)))
            .map(_.map((key, held) => (key, held.value, held.timestamp)))

        def windowPut(key: Chunk[Byte], start: Long, value: Option[Chunk[Byte]], retention: Long): F[Unit] =
          working.access.flatMap:
            case ((state, logged), set) => Task.this.windowPut(state, store, retention, changelog, key, start, value, timestamp).flatMap:
                (after, log) => set((after, logged :+ log)).flatMap(done => if done then F.unit else windowPut(key, start, value, retention))

        def windowsOf(key: Chunk[Byte], from: Long, to: Long, retention: Long): F[Vector[(Long, Chunk[Byte])]] =
          working.get.flatMap((state, _) => Task.this.windowsOf(state, store, retention, key, from, to))
            .map(_.map((start, held) => (start, held.value)))

        def windows(from: Long, to: Long, keys: Option[(Chunk[Byte], Chunk[Byte])]): F[Vector[(Chunk[Byte], Long, Chunk[Byte])]] =
          val held =
            working.get.flatMap: (state, _) =>
              keys.fold(storedBetween(state, store, from, to))((first, last) => storedRange(state, store, Some(first), StoreContents.past(last)))
          held.map: entries =>
            def spanned(key: Chunk[Byte]) = keys.forall((first, last) => StoreContents.keys.gteq(key, first) && StoreContents.keys.lteq(key, last))
            entries.flatMap: (windowed, stored) =>
              val key   = windowed.take(windowed.size - 12)
              val start = WindowStore.startOf(windowed)
              Option.when(start >= from && start <= to && spanned(key))((key, start, stored.value))
            .sortBy((key, start, _) => (start, key))(using Ordering.Tuple2(using Ordering.Long, StoreContents.keys))

        def sessionWrite(key: Chunk[Byte], start: Long, end: Long, value: Option[Chunk[Byte]], moving: Boolean, retention: Long): F[Unit] =
          working.access.flatMap:
            case ((state, logged), set) => Task.this.sessionWrite(state, store, retention, changelog, key, start, end, value, timestamp, Some(moving))
                .flatMap: (after, log) =>
                  set((after, logged :+ log)).flatMap(done => if done then F.unit else sessionWrite(key, start, end, value, moving, retention))

        def sessions(
            keys: Option[(Chunk[Byte], Chunk[Byte])],
            earliestEnd: Long,
            latestEnd: Long,
            latestStart: Long
        ): F[Vector[(Chunk[Byte], Long, Long, Chunk[Byte])]] =
          val held =
            working.get.flatMap: (state, _) =>
              keys match
                case Some((first, last)) if first == last => storedUnder(state, store, first)
                case Some((first, last))                  => storedRange(state, store, Some(first), StoreContents.past(last))
                case None                                 => storedBetween(state, store, earliestEnd, latestEnd)
          held.map: entries =>
            def spanned(key: Chunk[Byte]) = keys.forall((first, last) => StoreContents.keys.gteq(key, first) && StoreContents.keys.lteq(key, last))
            entries.flatMap: (session, stored) =>
              val key   = session.take(session.size - 16)
              val start = SessionStore.startOf(session)
              val end   = SessionStore.endOf(session)
              Option.when(end >= earliestEnd && end <= latestEnd && start <= latestStart && spanned(key))((key, start, end, stored.value))
            .sortBy((key, start, end, _) => (end, key, -start))(using Ordering.Tuple3(using Ordering.Long, StoreContents.keys, Ordering.Long))

  /** What Java's table filter makes of a change: its value where `keep` keeps it, and otherwise none, with the old value filtered too; a change from
    * no value to no value goes no further. Java's filter always asks for old values, which every table here can give. Kept in no store, the old value
    * comes from the change.
    */
  private def tableFiltered[K, C, V](
      step: Step.TableFilter[F, K, C, V],
      record: Travelling[K, C],
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K, Option[V]]], Vector[TaskOutput])] =
    val value = step.present(record.value).filter(step.keep(record.key, _))
    step.store match
      case None =>
        val old  = record.old.flatMap(step.present).filter(step.keep(record.key, _))
        val goes = if old.isEmpty && value.isEmpty then Vector.empty else Vector(record.copy(value = value, old = Some(old)))
        F.pure((state, goes, Vector.empty))
      case Some(kept) => keptResult(kept, record.key, value, record.timestamp, state, true).map:
          case None                         => (state, Vector.empty, Vector.empty)
          case Some((after, replaced, log)) => (after, Vector(record.copy(value = value, old = Some(replaced))), Vector(log))

  /** What Java's table `mapValues` makes of a change: its value changed, with the old value changed too where what reads it asks for one. Kept in no
    * store, the old value comes from the change.
    */
  private def tableMapped[K, CA, CB, B](
      step: Step.TableMapValues[F, K, CA, CB, B],
      record: Travelling[K, CA],
      state: TaskState,
      asked: => Boolean
  ): F[(TaskState, Vector[Travelling[K, CB]], Vector[TaskOutput])] =
    val value = step.mapped(record.value)
    step.store match
      case None       => F.pure((state, Vector(record.copy(value = value, old = if asked then record.old.map(step.mapped) else None)), Vector.empty))
      case Some(kept) => keptResult(kept, record.key, step.present(value), record.timestamp, state, false).map:
          case None                         => (state, Vector.empty, Vector.empty)
          case Some((after, replaced, log)) =>
            (after, Vector(record.copy(value = value, old = if asked then replaced.map(step.pure) else None)), Vector(log))

  /** What a window step gives: the task's state, the records it passes on, and what it writes to its store's changelog. */
  private type Windowing[K, A] = (TaskState, Vector[Travelling[Windowed[K], A]], Vector[TaskOutput])

  /** The windows of the key written as `bytes` in window store `store` starting from `from` to `to`, earliest first, without those its clock has
    * expired, as Java's window store fetches them.
    */
  private def windowsOf(state: TaskState, store: String, retention: Long, bytes: Chunk[Byte], from: Long, to: Long): F[Vector[(Long, Stored)]] =
    (clockOf(state, store), storedUnder(state, store, bytes)).mapN: (clock, entries) =>
      val least = math.max(from, clock - retention + 1)
      entries.collect:
        case (held, stored) if held.size == bytes.size + 12 => (WindowStore.startOf(held), stored)
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
  ): F[(TaskState, TaskOutput)] =
    clockOf(state, store).flatMap: clock =>
      val windowed = WindowStore.key(bytes, start)
      val ticked   = math.max(clock, start)
      val kept   = if WindowStore.expired(start, ticked, retention) then state else state.written(store, windowed, written.map(Stored(_, timestamp)))
      val pruned = if ticked > clock then swept(kept, store, ticked - retention) else F.pure(kept)
      pruned.map(after =>
        (
          after.copy(clocks = after.clocks.updated(store, ticked)),
          TaskOutput(changelog, Some(partition), Some(windowed), written, timestamp, Headers.empty)
        )
      )

  /** The windows `step` updates for `record`, as Java's windowed aggregation does: each window the record falls in that has not closed, by the step's
    * stream time less its grace, is folded from its value, or started where the store has none, then kept unless expired by the store's clock, and
    * written to the changelog either way. The store never holds an expired window, since it drops them as its clock moves and once restored, so one
    * reads as absent, as Java's store reads it. A record whose key its serializer writes as none is skipped, as Java skips a null key.
    */
  private def windowed[K, V, A](
      step: Step.WindowedAggregate[F, K, V, A],
      record: Travelling[K, V],
      state: TaskState,
      now: Long,
      clock: Option[Int]
  ): F[Windowing[K, A]] =
    import step.{add, emitOnClose, key, retention, store, value, windows}
    topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val timer      = Task.timer(store, clock)
          val streamTime = math.max(state.streamTimes.getOrElse(timer, WindowStore.NoTime), record.timestamp)
          val timed      = state.copy(streamTimes = state.streamTimes.updated(timer, streamTime))
          val closeTime  = streamTime - windows.graceMillis
          val open       = windows.windowsFor(record.timestamp).filter(_.end > closeTime)
          val updated    =
            open.foldLeftM((timed, Vector.empty[Travelling[Windowed[K], A]], Vector.empty[TaskOutput])):
              case ((current, forwarded, logged), window) =>
                val folded =
                  storedAt(current, store.value, WindowStore.key(bytes, window.start)).flatMap:
                    case None      => F.pure((step.start(record.key, record.value), record.timestamp, step.initial))
                    case Some(old) => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value)).map: previous =>
                        (add(record.key, record.value, previous), math.max(old.timestamp, record.timestamp), Some(previous))
                folded.flatMap: (aggregate, timestamp, replaced) =>
                  value.serializer.serialize(changelog, Headers.empty, aggregate).flatMap: written =>
                    windowPut(current, store.value, retention, changelog, bytes, window.start, written, timestamp).map: (after, log) =>
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
              closed(store.value, key, value, after, closeTime, size, range, record, changelog, now).map((emitted, state) => (state, emitted, logged))

  /** The sliding windows `step` updates or makes for `record`, as Java's sliding window aggregation does (`KStreamSlidingWindowAggregate`, over a
    * store it can read backwards): a record past its window's grace is dropped; one in the first time difference after 0 goes in the window from 0
    * and makes no look for closed windows; any other updates every window it falls in, latest first, and makes the window ending at it, the window
    * starting just after it where later records are within the time difference, and the window starting just after the record before it where it
    * falls in that one.
    */
  private def sliding[K, V, A](
      step: Step.SlidingAggregate[F, K, V, A],
      record: Travelling[K, V],
      state: TaskState,
      now: Long,
      clock: Option[Int]
  ): F[Windowing[K, A]] =
    import step.{add, emitOnClose, key, retention, store, value, windows}
    val difference = windows.differenceMillis
    val timestamp  = record.timestamp
    topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val timer                    = Task.timer(store, clock)
          val streamTime               = math.max(state.streamTimes.getOrElse(timer, WindowStore.NoTime), record.timestamp)
          val timed                    = state.copy(streamTimes = state.streamTimes.updated(timer, streamTime))
          val closeTime                = streamTime - windows.graceMillis
          def read(held: Stored): F[A] = value.deserializer.deserialize(changelog, Headers.empty, Some(held.value))
          // Puts a window and passes it on, as Java puts a window it makes from an aggregate it found.
          def put(ran: Windowing[K, A], start: Long, aggregate: A, at: Long, replaced: Option[A]): F[Windowing[K, A]] =
            value.serializer.serialize(changelog, Headers.empty, aggregate).flatMap: written =>
              windowPut(ran._1, store.value, retention, changelog, bytes, start, written, at).map: (after, log) =>
                val window = Windowed(record.key, Window(start, start + difference))
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
            early(nothing, bytes, read, write, update, timestamp, retention, store.value)
              .map((after, updates, logged) => (after, if emitOnClose then Vector.empty else updates, logged))
          else
            val reversed = reverse(nothing, bytes, read, write, update, difference, timestamp, retention, store.value)
            if !emitOnClose then reversed
            else
              // Sliding windows end where they start plus the time difference, both included, so an emission ends a millisecond earlier, and
              // the next starts where the last one could not yet reach.
              def range(last: Long) =
                val upper = closeTime - difference - 1
                val lower = if last == WindowStore.NoTime then 0L else math.max(0L, last - difference)
                Option.when(upper >= 0)((lower, upper))
              reversed.flatMap: (after, _, logged) =>
                closed(store.value, key, value, after, closeTime, difference, range, record, changelog, now)
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
      timestamp: Long,
      moving: Option[Boolean] = None
  ): F[(TaskState, TaskOutput)] =
    clockOf(state, store).flatMap: clock =>
      val session = SessionStore.key(bytes, start, end)
      val ticked  = if moving.getOrElse(written.isDefined) then math.max(clock, end) else clock
      val kept    =
        written match
          case None                                                   => state.written(store, session, None)
          case Some(_) if WindowStore.expired(end, ticked, retention) => state
          case Some(value)                                            => state.written(store, session, Some(Stored(value, timestamp)))
      val pruned = if ticked > clock then swept(kept, store, ticked - retention) else F.pure(kept)
      pruned.map(after =>
        (
          after.copy(clocks = after.clocks.updated(store, ticked)),
          TaskOutput(changelog, Some(partition), Some(session), written, timestamp, Headers.empty)
        )
      )

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
      now: Long,
      clock: Option[Int]
  ): F[Windowing[K, Option[A]]] =
    import step.{add, emitOnClose, initial, key, merge, retention, store, value, windows}
    val gap       = windows.gapMillis
    val timestamp = record.timestamp
    topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val timer      = Task.timer(store, clock)
          val streamTime = math.max(state.streamTimes.getOrElse(timer, WindowStore.NoTime), timestamp)
          val timed      = state.copy(streamTimes = state.streamTimes.updated(timer, streamTime))
          val closeTime  = streamTime - windows.graceMillis - gap
          val changed    =
            storedUnder(timed, store.value, bytes).flatMap: entries =>
              val found =
                entries.collect:
                  case (held, stored) if held.size == bytes.size + 16 => (SessionStore.startOf(held), SessionStore.endOf(held), stored)
                .filter((start, end, _) => end >= timestamp - gap && start <= timestamp + gap).sortBy((start, end, _) => (end, start))
              found.traverse((_, _, stored) => value.deserializer.deserialize(changelog, Headers.empty, Some(stored.value))).flatMap: aggregates =>
                val (from, to) =
                  found.foldLeft((timestamp, timestamp))((window, held) => (math.min(window._1, held._1), math.max(window._2, held._2)))
                val folded = aggregates.foldLeft(initial)((merged, next) => Some(merged.fold(next)(merge(record.key, _, next))))
                if to < closeTime then F.pure((timed, Vector.empty[Travelling[Windowed[K], Option[A]]], Vector.empty[TaskOutput]))
                else
                  // A record that joins no other session makes its own, which removes nothing, even where one like it was already kept.
                  val merged = if from == timestamp && to == timestamp then Vector.empty else found.zip(aggregates)
                  merged.foldLeftM((timed, Vector.empty[Travelling[Windowed[K], Option[A]]], Vector.empty[TaskOutput])):
                    case ((current, forwarded, logged), ((start, end, _), gone)) =>
                      sessionWrite(current, store.value, retention, changelog, bytes, start, end, None, timestamp).map: (after, log) =>
                        val session = Windowed(record.key, Window(start, end))
                        (after, forwarded :+ Travelling(session, None, end, Headers.empty, Some(Some(gone))), logged :+ log)
                  .flatMap: (current, forwarded, logged) =>
                    val aggregate = folded.fold(step.start(record.key, record.value))(add(record.key, record.value, _))
                    value.serializer.serialize(changelog, Headers.empty, aggregate).flatMap: written =>
                      sessionWrite(current, store.value, retention, changelog, bytes, from, to, written, timestamp).map: (after, log) =>
                        (after, forwarded :+ Travelling(Windowed(record.key, Window(from, to)), Some(aggregate), to, Headers.empty), logged :+ log)
          if !emitOnClose then changed
          else
            // Sessions end where they end, included, so an emission ends a millisecond before the close time, and the next starts where it ended.
            def range(last: Long) =
              val upper = closeTime - 1
              val lower = math.max(0L, last)
              Option.when(upper >= 0 && upper >= lower)((lower, upper))
            changed.flatMap: (after, _, logged) =>
              sessionsClosed(store.value, key, value, after, closeTime, range, record, changelog, now)
                .map((emitted, state) => (state, emitted, logged))

  /** The sessions of session store `store` that a step emitting them once they close emits, as Java's session window aggregation emits final results:
    * at most once per emit interval of wall-clock time, every session ending in the range `range` gives from the close time last emitted for, by end,
    * then key and then start, each with its value at its end and the headers of the record that closed it.
    */
  private def sessionsClosed[K, V, A](
      store: String,
      key: xkafka.Serde[F, K],
      value: xkafka.Serde[F, A],
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
        case Some((lower, upper)) => storedBetween(throttled, store, lower, upper).flatMap: entries =>
            val due =
              entries.map: (held, stored) =>
                (SessionStore.endOf(held), held.take(held.size - 16), SessionStore.startOf(held), stored)
              .filter((end, _, _, _) => end >= lower && end <= upper).sortWith: (a, b) =>
                a._1 < b._1 || (a._1 == b._1 && (StoreContents.keys.compare(a._2, b._2) < 0 || (a._2 == b._2 && a._3 < b._3)))
            due.traverse: (end, serialized, start, stored) =>
              (
                key.deserializer.deserialize(changelog, Headers.empty, Some(serialized)),
                value.deserializer.deserialize(changelog, Headers.empty, Some(stored.value))
              ).mapN((k, v) => Travelling(Windowed(k, Window(start, end)), Option(v), end, record.headers))
          .map(emitted => (emitted, throttled.copy(lastEmitted = throttled.lastEmitted.updated(store, closeTime))))

  /** The serde a suppression's buffer writes a table's keys with: Java's windowed serdes for windowed tables. */
  private def bufferKey[K](key: TableKey[F, K]): xkafka.Serde[F, K] =
    key match
      case TableKey.Plain(plain, _)           => plain
      case TableKey.TimeWindowed(inner, size) => xkafka.streams.Windowed.timeSerde(inner, scala.concurrent.duration.FiniteDuration(size, "ms"))
      case TableKey.SessionWindowed(inner)    => xkafka.streams.Windowed.sessionSerde(inner)

  /** The serde a suppression's buffer writes a table's values with, none written as a tombstone. */
  private def bufferValue[V](value: TableValue[F, V]): xkafka.Serde[F, V] =
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
          (state.buffers.get(store).fold(contents(state, store).map(Buffered.of))(index => F.pure(Some(index))), storedAt(state, store, key)).tupled
            .flatMap((index, stored) => (index, stored.traverse(held => Held.decode(held.value))).tupled.liftTo[F](unreadable)).flatMap:
              (index, before) =>
                // The key's value before it was first held stays with it until it goes on, and so does the time it is held until.
                val held     = Held(before.fold(until)(_.time), context, before.fold(old)(_.prior), old, value)
                val indexed  = before.fold(index)(index.excluding(key, _)).including(key, held)
                val buffered =
                  state.written(store, key, Some(Stored(held.encode, record.timestamp))).copy(
                    streamTimes = state.streamTimes.updated(store, streamTime),
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
                    case Some((_, key)) => storedAt(now, store, key).flatMap(_.flatMap(stored => Held.decode(stored.value)).liftTo[F](unreadable))
                        .flatMap: held =>
                          val taken =
                            now.written(store, key, None).copy(dirty = now.dirty.updated(store, now.dirty.getOrElse(store, Dirty.empty) + key))
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
    windowsOf(begun._1, store, retention, bytes, math.max(0L, timestamp - 2 * difference), timestamp + 1)
      .flatMap(held => scan(held.reverse.toList, begun, Found(Set.empty, None, None, false, false, None))).flatMap: (ran, found, ended) =>
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
    windowsOf(begun._1, store, retention, bytes, 0L, timestamp + 1)
      .flatMap(held => scan(held.toList, begun, None, Found(Set.empty, None, None, false, false, None))).flatMap: (ran, combined, found) =>
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
      key: xkafka.Serde[F, K],
      value: xkafka.Serde[F, A],
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
        case Some((lower, upper)) => storedBetween(throttled, store, lower, upper).flatMap: entries =>
            val due =
              entries.map((stored, held) => (WindowStore.startOf(stored), stored.take(stored.size - 12), held))
                .filter((start, _, _) => start >= lower && start <= upper).sortWith: (a, b) =>
                  a._1 < b._1 || (a._1 == b._1 && StoreContents.keys.compare(a._2, b._2) < 0)
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
      case TableView.Stored(store, keys, values) => topicNamed(s"$application-${store.value}-changelog").flatMap: changelog =>
          keys.serializer.serialize(changelog, Headers.empty, key).flatMap:
            _.flatTraverse(storedAt(state, store.value, _))
              .flatMap(_.traverse(held => values.deserializer.deserialize(changelog, Headers.empty, Some(held.value)).map(_ -> held.timestamp)))
      case TableView.Held(buffer, parent) =>
        def unreadable = new IllegalStateException(s"the suppression buffer '$buffer' holds a change it cannot read")
        topicNamed(s"$application-$buffer-changelog").flatMap: changelog =>
          parent.keys.serializer.serialize(changelog, Headers.empty, key).flatMap: bytes =>
            bytes.flatTraverse(storedAt(state, buffer, _)).flatMap:
              case None         => valueOf(parent, key, state)
              case Some(stored) => (Held.decode(stored.value), Task.valuesOf(parent)).tupled.liftTo[F](unreadable).flatMap: (held, values) =>
                  held.prior.traverse(prior => values.deserializer.deserialize(changelog, held.context.headers, Some(prior)).map(_ -> Task.Unknown))
      case TableView.Joined(left, right, joiner)              => joinedValueOf(left, right, joiner, key, state)
      case TableView.Filtered(parent, keep)                   => valueOf(parent, key, state).map(_.filter((value, _) => keep(key, value)))
      case mapped @ TableView.Mapped(_, _)                    => mappedValueOf(mapped, key, state)
      case windowed @ TableView.WindowedStored(_, _, _, _, _) => windowedValueOf(windowed, key, state)

  /** One window's value, at the time it was stored, or for a session at its end, as Java's windowed value getters give it. A window past the store's
    * retention is not there to find, as the store removes each such window once its clock passes it.
    */
  private def windowedValueOf[K, V](view: TableView.WindowedStored[F, K, V], key: Windowed[K], state: TaskState): F[Option[(V, Long)]] =
    val store = view.store.value
    topicNamed(s"$application-$store-changelog").flatMap: changelog =>
      view.key.serializer.serialize(changelog, Headers.empty, key.key).flatMap:
        case None        => F.pure(None)
        case Some(bytes) =>
          val stored = if view.session then SessionStore.key(bytes, key.window.start, key.window.end) else WindowStore.key(bytes, key.window.start)
          storedAt(state, store, stored).flatMap:
            _.traverse: found =>
              view.value.deserializer.deserialize(changelog, Headers.empty, Some(found.value))
                .map(_ -> (if view.session then key.window.end else found.timestamp))

  private def mappedValueOf[K, A, B](view: TableView.Mapped[F, K, A, B], key: K, state: TaskState): F[Option[(B, Long)]] =
    valueOf(view.parent, key, state).map(_.map((value, timestamp) => (view.change(value), timestamp)))

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

  private def written[K, V](
      topic: F[Topic],
      key: xkafka.Serde[F, K],
      value: xkafka.Serde[F, V],
      repartitioning: Boolean,
      record: Travelling[K, V],
      partitioner: Option[xkafka.streams.Partitioner[K, V]]
  ): F[Vector[TaskOutput]] =
    // Java partitions a windowed key by the key alone, whichever topic it goes to.
    def base(target: Topic): F[Option[Chunk[Byte]]] = key.serializer.partitionedBy(target, record.headers, record.key).sequence.map(_.flatten)
    for
      target <- topic
      k      <- key.serializer.serialize(target, record.headers, record.key)
      v      <- value.serializer.serialize(target, record.headers, record.value)
      by     <- base(target)
    // Java drops a record with a null key before its repartition topic, which here is a key its serializer writes as none.
    yield
      val chosen = partitioner.map(chooser => (count: Int) => chooser.chosen(target, record.key, record.value, record.headers, count))
      if repartitioning && k.isEmpty then Vector.empty else Vector(TaskOutput(target, None, k, v, record.timestamp, record.headers, by, chosen))

  private def topicNamed(name: String): F[Topic] = Topic.from(name).leftMap(error => new KafkaException.InvalidValue(error)).liftTo[F]

private[xkafka] object Task:
  /** A task over each store its sub-topology declares, opened with `open`, holding what `held` gives for it. */
  def over[F[_]](
      application: String,
      subtopology: Subtopology[F],
      partition: Int,
      held: Map[String, StoreContents],
      open: TaskStore.Open[F],
      number: Int = 0
  )(using F: MonadThrow[F], refs: Ref.Make[F]): F[Task[F]] =
    subtopology.stores.traverse(store =>
      open(store.value, WindowStore.timesOf(List(subtopology)).get(store.value), held.getOrElse(store.value, StoreContents.empty))
        .map(store.value -> _)
    ).map(stores => new Task(application, subtopology, partition, stores.toMap, number))

  /** A task over stores in memory. */
  def inMemory[F[_]: Concurrent](
      application: String,
      subtopology: Subtopology[F],
      partition: Int,
      held: Map[String, StoreContents] = Map.empty
  ): F[Task[F]] = over(application, subtopology, partition, held, TaskStore.memory[F])

  /** The name a step's stream time goes by: its store's, or for a side of a cogroup, which has one of its own, a name no store can have. */
  def timer(store: Store, side: Option[Int]): String = side.fold(store.value)(number => s"${store.value}#$number")

  /** A look through a stream-stream join's held records: the earliest time it has reached, the sides, by whether they are the left, found with a
    * record whose window is open, whether it has ended, and the held records it sends on, with their keys, in order.
    */
  private final case class Look(minTime: Long, open: Set[Boolean], ended: Boolean, sent: Vector[(Chunk[Byte], Chunk[Byte])])

  /** The topic `source` names, as the application names its repartition topics. */
  def topicOf(application: String, source: Source): String =
    source match
      case Source.Input(topic)         => topic.value
      case Source.Repartitioned(named) => s"$application-${named.value}-repartition"
      case Source.Exchanged(topic)     => s"$application-$topic"

  /** The serde a table view's values are written with, which the result of joining tables kept in no store does not have. */
  def valuesOf[F[_], K, V](view: TableView[F, K, V]): Option[xkafka.Serde[F, V]] =
    view match
      case TableView.Stored(_, _, value)               => Some(value)
      case TableView.Held(_, parent)                   => valuesOf(parent)
      case TableView.Joined(_, _, _)                   => None
      case TableView.Filtered(parent, _)               => valuesOf(parent)
      case TableView.Mapped(_, _)                      => None
      case TableView.WindowedStored(_, _, _, value, _) => Some(value)

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
