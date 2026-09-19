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

import scala.concurrent.duration.{Duration, FiniteDuration}

import cats.arrow.FunctionK
import cats.effect.{Async, Deferred, Ref}
import cats.effect.std.Mutex
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.Stream
import fs2.concurrent.Channel

/** Records from one observed topic-partition assignment. `records` drains its queue and terminates after the partition is revoked. */
final case class PartitionRecords[F[_], K, V](topicPartition: TopicPartition, records: Stream[F, CommittableConsumerRecord[F, K, V]]):
  def mapK[G[_]](fk: FunctionK[F, G]): PartitionRecords[G, K, V] = PartitionRecords(topicPartition, records.map(_.mapK(fk)).translate(fk))

object PartitionRecords:
  given [K, V]: FunctorK[[F[_]] =>> PartitionRecords[F, K, V]] with
    override def mapK[F[_], G[_]](partition: PartitionRecords[F, K, V])(fk: FunctionK[F, G]): PartitionRecords[G, K, V] = partition.mapK(fk)

  private[xkafka] def fromConsumer[F[_]: Async, K, V](
      consumer: KafkaConsumer[F, K, V],
      pollInterval: FiniteDuration,
      maxQueuedRecords: Int
  ): Stream[F, PartitionRecords[F, K, V]] =
    if pollInterval <= Duration.Zero then Stream.raiseError(new IllegalArgumentException("pollInterval must be positive"))
    else if maxQueuedRecords <= 0 then Stream.raiseError(new IllegalArgumentException("maxQueuedRecords must be positive"))
    else
      Stream.eval(Runtime.create(consumer, pollInterval, maxQueuedRecords)).flatMap: runtime =>
        runtime.stream.onFinalize(runtime.close)

  private final case class PartitionState[F[_], K, V](topicPartition: TopicPartition, channel: Channel[F, CommittableConsumerRecord[F, K, V]]):
    def public: PartitionRecords[F, K, V] = PartitionRecords(topicPartition, channel.stream)

  private final case class Registry[F[_], K, V](states: Map[TopicPartition, PartitionState[F, K, V]], nextPoll: Deferred[F, Unit])

  private final class Runtime[F[_], K, V](
      consumer: KafkaConsumer[F, K, V],
      pollInterval: FiniteDuration,
      maxQueuedRecords: Int,
      output: Channel[F, PartitionRecords[F, K, V]],
      registry: Ref[F, Registry[F, K, V]],
      mutex: Mutex[F]
  )(using F: Async[F]):
    def stream: Stream[F, PartitionRecords[F, K, V]] = output.stream.concurrently(pollAssignments).concurrently(consumer.records.evalMap(route).drain)

    def initialize: F[Unit] = consumer.assignment.flatMap(reconcile)

    def close: F[Unit] =
      mutex.lock.surround:
        F.uncancelable: _ =>
          registry.get.flatMap: current =>
            current.states.values.toList.traverse_(_.channel.close.void) >> current.nextPoll.complete(()).void >> output.close.void

    private def pollAssignments: Stream[F, Nothing] = Stream.awakeEvery[F](pollInterval).evalMap(_ => consumer.assignment.flatMap(reconcile)).drain

    private def route(record: CommittableConsumerRecord[F, K, V]): F[Unit] =
      registry.get.flatMap: current =>
        current.states.get(record.record.topicPartition) match
          case Some(state) => state.channel.send(record).void
          case None => current.nextPoll.get >> registry.get.flatMap(_.states.get(record.record.topicPartition).traverse_(_.channel.send(record).void))

    private def reconcile(assignment: Set[TopicPartition]): F[Unit] =
      mutex.lock.surround:
        F.uncancelable: _ =>
          for
            current  <- registry.get
            nextPoll <- Deferred[F, Unit]
            added    <- (assignment -- current.states.keySet).toList.traverse(createState).map(_.map(state => state.topicPartition -> state).toMap)
            removed  = current.states.removedAll(assignment)
            retained = current.states.removedAll(removed.keySet)
            _ <- registry.set(Registry(retained ++ added, nextPoll))
            _ <- removed.values.toList.traverse_(_.channel.close.void)
            _ <- added.values.toList.traverse_(state => output.send(state.public).void)
            _ <- current.nextPoll.complete(()).void
          yield ()

    private def createState(topicPartition: TopicPartition): F[PartitionState[F, K, V]] =
      Channel.bounded[F, CommittableConsumerRecord[F, K, V]](maxQueuedRecords).map(PartitionState(topicPartition, _))

  private object Runtime:
    def create[F[_]: Async, K, V](consumer: KafkaConsumer[F, K, V], pollInterval: FiniteDuration, maxQueuedRecords: Int): F[Runtime[F, K, V]] =
      for
        output   <- Channel.unbounded[F, PartitionRecords[F, K, V]]
        nextPoll <- Deferred[F, Unit]
        registry <- Ref.of[F, Registry[F, K, V]](Registry(Map.empty, nextPoll))
        mutex    <- Mutex[F]
        runtime = Runtime(consumer, pollInterval, maxQueuedRecords, output, registry, mutex)
        _ <- runtime.initialize
      yield runtime
