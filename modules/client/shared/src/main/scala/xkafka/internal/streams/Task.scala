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
import xkafka.streams.{Chain, End, Origin, Segment, Step}

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

/** What a task keeps between records: each store's contents by serialized key, and the partition time, the largest timestamp it has read. */
private[xkafka] final case class TaskState(stores: Map[String, Map[Chunk[Byte], Stored]], partitionTime: Long):
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

  def process(state: TaskState, input: TaskInput): F[(TaskState, Vector[TaskOutput])] =
    val read = state.copy(partitionTime = math.max(state.partitionTime, input.timestamp))
    segmentOf(segment, read, input)

  private def segmentOf[K, V, K2, V2](segment: Segment[F, K, V, K2, V2], state: TaskState, input: TaskInput): F[(TaskState, Vector[TaskOutput])] =
    val (topic, key, value) =
      segment.origin match
        case Origin.Input(topic, key, value)               => (F.pure(topic), key, value)
        case Origin.Repartitioned(repartition, key, value) => (topicNamed(s"$application-${repartition.name}-repartition"), key, value)
    for
      source <- topic
      k      <- key.deserializer.deserialize(source, input.headers, input.key)
      v      <- value.deserializer.deserialize(source, input.headers, input.value)
      ran    <- chain(segment.chain, Travelling(k, v, input.timestamp, input.headers), state)
      (after, records, written) = ran
      ended <- records.flatTraverse(leaving(segment.end, _))
    yield (after, written ++ ended)

  private def chain[K, V, K2, V2](
      steps: Chain[F, K, V, K2, V2],
      record: Travelling[K, V],
      state: TaskState
  ): F[(TaskState, Vector[Travelling[K2, V2]], Vector[TaskOutput])] =
    steps match
      case Chain.Start()          => F.pure((state, Vector(record), Vector.empty))
      case Chain.Then(init, last) => chain(init, record, state).flatMap: (before, records, written) =>
          records.foldLeftM((before, Vector.empty[Travelling[K2, V2]], written)):
            case ((now, done, out), next) => step(last, next, now).map((after, more, logged) => (after, done ++ more, out ++ logged))

  private def step[K, V, K2, V2](
      step: Step[F, K, V, K2, V2],
      record: Travelling[K, V],
      state: TaskState
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
      case Step.Peek(effect)                             => effect(record.key, record.value) *> passing(Vector(record))
      case Step.EvalMapValues(change)                    => change(record.value).flatMap(v => passing(Vector(record.copy(value = v))))
      case Step.ToStream()                               => passing(Vector(record))
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
