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

import cats.arrow.FunctionK
import cats.effect.{Async, Ref}
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
      assignments: Stream[F, Set[TopicPartition]],
      maxQueuedRecords: Int,
      pausing: PartitionPausing[F]
  ): Stream[F, PartitionRecords[F, K, V]] =
    if maxQueuedRecords <= 0 then Stream.raiseError(new IllegalArgumentException("maxQueuedRecords must be positive"))
    else
      Stream.eval(Runtime.create(consumer, assignments, maxQueuedRecords, pausing)).flatMap: runtime =>
        runtime.stream.onFinalize(runtime.close)

  /** `queued` counts what has been routed and not yet read, which is what decides whether the partition is paused. */
  private final case class PartitionState[F[_], K, V](
      topicPartition: TopicPartition,
      channel: Channel[F, CommittableConsumerRecord[F, K, V]],
      queued: Ref[F, Int],
      paused: Ref[F, Boolean]
  )

  private final class Runtime[F[_], K, V](
      consumer: KafkaConsumer[F, K, V],
      assignments: Stream[F, Set[TopicPartition]],
      maxQueuedRecords: Int,
      pausing: PartitionPausing[F],
      output: Channel[F, PartitionRecords[F, K, V]],
      states: Ref[F, Map[TopicPartition, PartitionState[F, K, V]]],
      mutex: Mutex[F]
  )(using F: Async[F]):
    def stream: Stream[F, PartitionRecords[F, K, V]] =
      output.stream.concurrently(followAssignments).concurrently(consumer.records.evalMap(route).drain)

    def close: F[Unit] =
      mutex.lock.surround:
        F.uncancelable: _ =>
          states.get.flatMap(current => current.values.toList.traverse_(_.channel.close.void)) >> output.close.void

    private def followAssignments: Stream[F, Nothing] = assignments.evalMap(reconcile).drain

    /** Opens a stream for each assigned partition, so one appears as soon as the partition is owned, and ends the streams of revoked ones. */
    private def reconcile(assignment: Set[TopicPartition]): F[Unit] =
      assignment.toList.traverse_(stateFor.andThen(_.void)) >> revokeMissing(assignment)

    /** Routes a record, opening the partition's stream first if the assignment has not reported it yet.
      *
      * A record is itself proof that its partition is assigned, so routing never waits for confirmation and never discards one for want of it.
      */
    private def route(record: CommittableConsumerRecord[F, K, V]): F[Unit] =
      stateFor(record.record.topicPartition).flatMap: state =>
        state.channel.send(record) >> state.queued.updateAndGet(_ + 1).flatMap(queued => pauseWhenFull(state, queued))

    /** A partition nobody is reading stops being fetched, so the records beside it keep arriving.
      *
      * Records already in flight when the pause is issued still arrive, which is why the channel is unbounded. Kafka holds the paused partition's
      * position, so resuming continues from where the reader got to.
      */
    private def pauseWhenFull(state: PartitionState[F, K, V], queued: Int): F[Unit] =
      F.whenA(queued >= maxQueuedRecords):
        state.paused.getAndSet(true).flatMap(wasPaused => F.unlessA(wasPaused)(pausing.pause(Set(state.topicPartition))))

    private def resumeWhenDrained(state: PartitionState[F, K, V], queued: Int): F[Unit] =
      F.whenA(queued < maxQueuedRecords):
        state.paused.getAndSet(false).flatMap(wasPaused => F.whenA(wasPaused)(pausing.resume(Set(state.topicPartition))))

    private def readable(state: PartitionState[F, K, V]): PartitionRecords[F, K, V] =
      PartitionRecords(
        state.topicPartition,
        state.channel.stream.evalTap(_ => state.queued.updateAndGet(_ - 1).flatMap(queued => resumeWhenDrained(state, queued)))
      )

    private def stateFor(topicPartition: TopicPartition): F[PartitionState[F, K, V]] =
      mutex.lock.surround:
        states.get.flatMap: current =>
          current.get(topicPartition) match
            case Some(state) => F.pure(state)
            case None        =>
              for
                // Pausing is what bounds this, so a record already in flight when the pause was issued still lands.
                channel <- Channel.unbounded[F, CommittableConsumerRecord[F, K, V]]
                queued  <- Ref.of[F, Int](0)
                paused  <- Ref.of[F, Boolean](false)
                state = PartitionState(topicPartition, channel, queued, paused)
                _ <- states.set(current.updated(topicPartition, state))
                _ <- output.send(readable(state)).void
              yield state

    private def revokeMissing(assignment: Set[TopicPartition]): F[Unit] =
      mutex.lock.surround:
        F.uncancelable: _ =>
          states.get.flatMap: current =>
            val revoked = current.removedAll(assignment)
            states.set(current.removedAll(revoked.keySet)) >> revoked.values.toList.traverse_(_.channel.close.void)

  private object Runtime:
    def create[F[_]: Async, K, V](
        consumer: KafkaConsumer[F, K, V],
        assignments: Stream[F, Set[TopicPartition]],
        maxQueuedRecords: Int,
        pausing: PartitionPausing[F]
    ): F[Runtime[F, K, V]] =
      for
        output <- Channel.unbounded[F, PartitionRecords[F, K, V]]
        states <- Ref.of[F, Map[TopicPartition, PartitionState[F, K, V]]](Map.empty)
        mutex  <- Mutex[F]
      yield Runtime(consumer, assignments, maxQueuedRecords, pausing, output, states, mutex)
