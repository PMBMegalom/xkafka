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
import xkafka.streams.{Chain, End, Origin, Segment, Step, Window, Windowed}

/** A record as a topic holds it, read by a task. */
private[xkafka] final case class TaskInput(key: Option[Chunk[Byte]], value: Option[Chunk[Byte]], timestamp: Long, headers: Headers)

/** A record a task writes: to `partition` where one is given, as a changelog is, and otherwise wherever the producer's partitioner puts its key. */
private[xkafka] final case class TaskOutput(
    topic: Topic,
    partition: Option[Int],
    key: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]],
    timestamp: Long,
    headers: Headers
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
    partitionTime: Long,
    streamTimes: Map[String, Long] = Map.empty,
    clocks: Map[String, Long] = Map.empty,
    lastEmitted: Map[String, Long] = Map.empty,
    nextEmit: Map[String, Long] = Map.empty
):
  def store(name: String): Map[Chunk[Byte], Stored] = stores.getOrElse(name, Map.empty)

private[xkafka] object TaskState:
  /** No store holds anything yet, and no partition time is known. */
  val empty: TaskState = TaskState(Map.empty, -1L)

/** One sub-topology on one partition: what it writes for each record it reads, as Java Streams writes it, without Java's record cache, so every
  * change to a store goes to its changelog and on downstream.
  */
private[xkafka] final class Task[F[_]](application: String, segment: Segment[F, ?, ?, ?, ?], partition: Int)(using F: MonadThrow[F]):
  /** A record as it travels through the steps. */
  private final case class Travelling[K, V](key: K, value: V, timestamp: Long, headers: Headers)

  /** Runs `input` through the segment at wall-clock time `now`, in epoch milliseconds. */
  def process(state: TaskState, input: TaskInput, now: Long): F[(TaskState, Vector[TaskOutput])] =
    val read = state.copy(partitionTime = math.max(state.partitionTime, input.timestamp))
    segmentOf(segment, read, input, now)

  private def segmentOf[K, V, K2, V2](
      segment: Segment[F, K, V, K2, V2],
      state: TaskState,
      input: TaskInput,
      now: Long
  ): F[(TaskState, Vector[TaskOutput])] =
    val (topic, key, value) =
      segment.origin match
        case Origin.Input(topic, key, value)               => (F.pure(topic), key, value)
        case Origin.Repartitioned(repartition, key, value) => (topicNamed(s"$application-${repartition.name}-repartition"), key, value)
    for
      source <- topic
      k      <- key.deserializer.deserialize(source, input.headers, input.key)
      v      <- value.deserializer.deserialize(source, input.headers, input.value)
      ran    <- chain(segment.chain, Travelling(k, v, input.timestamp, input.headers), state, now)
      (after, records, written) = ran
      ended <- records.flatTraverse(leaving(segment.end, _))
    yield (after, written ++ ended)

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
      case Step.MapValues(change) => passing(Vector(record.copy(value = change(record.value))))
      case Step.FlatMap(change)   =>
        passing(change(record.key, record.value).toVector.map((k, v) => Travelling(k, v, record.timestamp, record.headers)))
      case Step.Peek(effect)          => effect(record.key, record.value) *> passing(Vector(record))
      case Step.EvalMapValues(change) => change(record.value).flatMap(v => passing(Vector(record.copy(value = v))))
      case Step.EvalFilter(keep)      => keep(record.key, record.value).flatMap(kept => passing(if kept then Vector(record) else Vector.empty))
      case Step.EvalMap(change)       => change(record.key, record.value)
          .flatMap((k, v) => passing(Vector(Travelling(k, v, record.timestamp, record.headers))))
      case Step.ToStream()                                          => passing(Vector(record))
      case folding @ Step.WindowedAggregate(_, _, _, _, _, _, _, _) => windowed(folding, record, state, now)
          .asInstanceOf[F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])]]
      case Step.Aggregate(store, key, value, start, add) => topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
          key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
            // Java skips a record whose key is null, which here is a key its serializer writes as none.
            case None        => F.pure((state, Vector.empty, Vector.empty))
            case Some(bytes) =>
              val held    = state.store(store.name).get(bytes)
              val updated =
                held match
                  case None      => F.pure((start(record.key, record.value), record.timestamp))
                  case Some(old) => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value)).map: previous =>
                      (add(record.key, record.value, previous), math.max(old.timestamp, record.timestamp))
              updated.flatMap: (aggregate, timestamp) =>
                value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
                  val contents = written.fold(state.store(store.name) - bytes)(v => state.store(store.name).updated(bytes, Stored(v, timestamp)))
                  val after    = state.copy(stores = state.stores.updated(store.name, contents))
                  val logged   = TaskOutput(changelog, Some(partition), Some(bytes), written, timestamp, Headers.empty)
                  (after, Vector(Travelling(record.key, aggregate, timestamp, record.headers)), Vector(logged))

  /** The windows `step` updates for `record`, as Java's windowed aggregation does: each window the record falls in that has not closed, by the step's
    * stream time less its grace, is folded from its value, or started where the store has none, then kept unless expired by the store's clock, and
    * written to the changelog either way. The store never holds an expired window, since it drops them as its clock moves and once restored, so one
    * reads as absent, as Java's store reads it. A record whose key its serializer writes as none is skipped, as Java skips a null key.
    */
  private def windowed[K, V, A](
      step: Step.WindowedAggregate[F, K, V, A],
      record: Travelling[K, V],
      state: TaskState,
      now: Long
  ): F[(TaskState, Vector[Travelling[Windowed[K], A]], Vector[TaskOutput])] =
    import step.{add, emitOnClose, key, retention, start, store, value, windows}
    topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
      key.serializer.serialize(changelog, Headers.empty, record.key).flatMap:
        case None        => F.pure((state, Vector.empty, Vector.empty))
        case Some(bytes) =>
          val streamTime = math.max(state.streamTimes.getOrElse(store.name, WindowStore.NoTime), record.timestamp)
          val timed      = state.copy(streamTimes = state.streamTimes.updated(store.name, streamTime))
          val open       = windows.windowsFor(record.timestamp).filter(_.end > streamTime - windows.graceMillis)
          val updated    =
            open.foldLeftM((timed, Vector.empty[Travelling[Windowed[K], A]], Vector.empty[TaskOutput])):
              case ((current, forwarded, logged), window) =>
                val entries  = current.store(store.name)
                val clock    = current.clocks.getOrElse(store.name, WindowStore.clockOf(entries))
                val windowed = WindowStore.key(bytes, window.start)
                val updated  =
                  entries.get(windowed) match
                    case None      => F.pure((start(record.key, record.value), record.timestamp))
                    case Some(old) => value.deserializer.deserialize(changelog, Headers.empty, Some(old.value)).map: previous =>
                        (add(record.key, record.value, previous), math.max(old.timestamp, record.timestamp))
                updated.flatMap: (aggregate, timestamp) =>
                  value.serializer.serialize(changelog, Headers.empty, aggregate).map: written =>
                    val ticked = math.max(clock, window.start)
                    val kept   =
                      if WindowStore.expired(window.start, ticked, retention) then entries
                      else written.fold(entries - windowed)(v => entries.updated(windowed, Stored(v, timestamp)))
                    val contents = if ticked > clock then WindowStore.pruned(kept, ticked, retention) else kept
                    val after    =
                      current.copy(stores = current.stores.updated(store.name, contents), clocks = current.clocks.updated(store.name, ticked))
                    val log = TaskOutput(changelog, Some(partition), Some(windowed), written, timestamp, Headers.empty)
                    (after, forwarded :+ Travelling(Windowed(record.key, window), aggregate, timestamp, record.headers), logged :+ log)
          if !emitOnClose then updated
          else
            updated.flatMap: (after, _, logged) =>
              closed(step, after, streamTime - windows.graceMillis, record, changelog, now).map((emitted, state) => (state, emitted, logged))

  /** The windows `step` emits once they close, as Java's windowed aggregation emits final results: at most once per emit interval of wall-clock time,
    * and only when the close time has moved past the one last emitted for, every window starting from after the last one emitted up to the last one
    * closed by `closeTime`, by window start and then by key, each with its value, its timestamp and the headers of the record that closed it.
    */
  private def closed[K, V, A](
      step: Step.WindowedAggregate[F, K, V, A],
      state: TaskState,
      closeTime: Long,
      record: Travelling[K, V],
      changelog: Topic,
      now: Long
  ): F[(Vector[Travelling[Windowed[K], A]], TaskState)] =
    import step.{key, store, value, windows}
    val size = windows.sizeMillis
    val last = state.lastEmitted.getOrElse(store.name, WindowStore.NoTime)
    if now < state.nextEmit.getOrElse(store.name, 0L) then F.pure((Vector.empty, state))
    else
      // The next look waits an interval from now, so a clock that jumps ahead does not make every record look.
      val throttled = state.copy(nextEmit = state.nextEmit.updated(store.name, now + Task.EmitIntervalMillis))
      val upper     = closeTime - size
      val lower     = if last == WindowStore.NoTime then 0L else math.max(0L, last - size) + 1
      // Java fetches nothing where the windows it would end at are those it ended at last time.
      val moved = last == WindowStore.NoTime || windows.windowsFor(upper) != windows.windowsFor(lower - 1)
      // A close time that has not moved since the last emission ends where that one did, so nothing has closed since.
      if upper < 0 || !moved then F.pure((Vector.empty, throttled))
      else
        val due =
          throttled.store(store.name).toVector.map((stored, held) => (WindowStore.startOf(stored), stored.take(stored.size - 12), held))
            .filter((start, _, _) => start >= lower && start <= upper).sortWith: (a, b) =>
              a._1 < b._1 || (a._1 == b._1 && Task.unsigned.compare(a._2, b._2) < 0)
        due.traverse: (start, serialized, held) =>
          (
            key.deserializer.deserialize(changelog, Headers.empty, Some(serialized)),
            value.deserializer.deserialize(changelog, Headers.empty, Some(held.value))
          ).mapN((k, v) => Travelling(Windowed(k, Window(start, start + size)), v, held.timestamp, record.headers))
        .map(emitted => (emitted, throttled.copy(lastEmitted = throttled.lastEmitted.updated(store.name, closeTime))))

  private def leaving[K, V](exit: End[F, K, V], record: Travelling[K, V]): F[Vector[TaskOutput]] =
    val (topic, key, value, repartitioning) =
      exit match
        case End.Repartitioning(repartition, key, value) => (topicNamed(s"$application-${repartition.name}-repartition"), key, value, true)
        case End.Output(topic, key, value)               => (F.pure(topic), key, value, false)
    for
      target <- topic
      k      <- key.serializer.serialize(target, record.headers, record.key)
      v      <- value.serializer.serialize(target, record.headers, record.value)
    // Java drops a record with a null key before its repartition topic, which here is a key its serializer writes as none.
    yield if repartitioning && k.isEmpty then Vector.empty else Vector(TaskOutput(target, None, k, v, record.timestamp, record.headers))

  private def topicNamed(name: String): F[Topic] = Topic.from(name).leftMap(error => new KafkaException.InvalidValue(error)).liftTo[F]

private[xkafka] object Task:
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
