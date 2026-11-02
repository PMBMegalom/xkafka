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
import xkafka.streams.{BaseKeyed, Chain, End, Origin, Segment, Step, Suppressed, TableKey, TableValue, Window, Windowed}

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
    partitionTime: Long,
    streamTimes: Map[String, Long] = Map.empty,
    clocks: Map[String, Long] = Map.empty,
    lastEmitted: Map[String, Long] = Map.empty,
    nextEmit: Map[String, Long] = Map.empty,
    dirty: Map[String, Dirty] = Map.empty,
    buffers: Map[String, Buffered] = Map.empty,
    reading: Option[TaskInput] = None
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

  /** Runs `input` through the segment at wall-clock time `now`, in epoch milliseconds. */
  def process(state: TaskState, input: TaskInput, now: Long): F[(TaskState, Vector[TaskOutput])] =
    val read = state.copy(partitionTime = math.max(state.partitionTime, input.timestamp), reading = Some(input))
    segmentOf(segment, read, input, now)

  /** What the task's suppression buffers write to their changelogs at a commit, as Java's buffer logs on commit: each key changed since the last
    * commit, with what it holds and the header `v=3`, or a tombstone where it no longer holds the key. The changelog's broker gives each its time.
    */
  def flush(state: TaskState): F[(TaskState, Vector[TaskOutput])] =
    segment.chain.buffers.toList.sorted.flatTraverse: store =>
      topicNamed(s"$application-$store-changelog").map: changelog =>
        state.dirty.getOrElse(store, Dirty.empty).ordered.toList.map: key =>
          state.store(store).get(key) match
            case Some(held) => TaskOutput(changelog, Some(partition), Some(key), Some(held.value), Task.NoTimestamp, Held.Version)
            case None       => TaskOutput(changelog, Some(partition), Some(key), None, Task.NoTimestamp, Headers.empty)
    .map(written => (state.copy(dirty = state.dirty.view.mapValues(_.cleared).toMap), written.toVector))

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
          .asInstanceOf[F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])]]
      case Step.TableSource(store, key, value) => topicNamed(s"$application-${store.name}-changelog").flatMap: changelog =>
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
    import step.{grace, suppressed}
    val store                    = s"${suppressed.name}-store"
    val keys                     = bufferKey(step.key)
    val values                   = bufferValue(step.value)
    val (until, wait, dropEmpty) =
      suppressed match
        case Suppressed.UntilWindowCloses(_, _)      => (record.key.asInstanceOf[Windowed[?]].window.end, grace.getOrElse(0L), true)
        case Suppressed.UntilTimeLimit(_, waited, _) => (record.timestamp, waited.toMillis, false)
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

  private def leaving[K, V](exit: End[F, K, V], record: Travelling[K, V]): F[Vector[TaskOutput]] =
    val (topic, key, value, repartitioning) =
      exit match
        case End.Repartitioning(repartition, key, value) => (topicNamed(s"$application-${repartition.name}-repartition"), key, value, true)
        case End.Output(topic, key, value)               => (F.pure(topic), key, value, false)
    // Java partitions a windowed key by the key alone, whichever topic it goes to.
    def base(target: Topic): F[Option[Chunk[Byte]]] =
      key.serializer match
        case windowed: BaseKeyed[?, ?] => windowed.asInstanceOf[BaseKeyed[F, Any]]
            .base(target, record.headers, record.key.asInstanceOf[xkafka.streams.Windowed[Any]])
        case _ => F.pure(None)
    for
      target <- topic
      k      <- key.serializer.serialize(target, record.headers, record.key)
      v      <- value.serializer.serialize(target, record.headers, record.value)
      by     <- base(target)
    // Java drops a record with a null key before its repartition topic, which here is a key its serializer writes as none.
    yield if repartitioning && k.isEmpty then Vector.empty else Vector(TaskOutput(target, None, k, v, record.timestamp, record.headers, by))

  private def topicNamed(name: String): F[Topic] = Topic.from(name).leftMap(error => new KafkaException.InvalidValue(error)).liftTo[F]

private[xkafka] object Task:
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
