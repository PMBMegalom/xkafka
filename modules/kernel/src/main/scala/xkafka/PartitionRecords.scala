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

import cats.Eq
import cats.arrow.FunctionK
import cats.effect.{Async, Outcome, Ref}
import cats.effect.std.Mutex
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.{Chunk, Stream}
import fs2.concurrent.Channel

/** A consumer's assignment, and how many times the group had assigned each partition, assigned now or not, when it was taken. A consumer that does
  * not count them knows none, and reads as having seen every partition at its first.
  */
private[xkafka] final case class AssignmentReport(assigned: Set[TopicPartition], leases: Map[TopicPartition, Long]):
  def lease(partition: TopicPartition): Long = leases.getOrElse(partition, 0L)

private[xkafka] object AssignmentReport:
  given Eq[AssignmentReport] = Eq.fromUniversalEquals

/** The records of one partition for as long as this consumer holds it. `records` ends once the partition is revoked. */
final case class PartitionRecords[F[_], K, V](topicPartition: TopicPartition, records: Stream[F, CommittableConsumerRecord[F, K, V]]):
  def mapK[G[_]](fk: FunctionK[F, G]): PartitionRecords[G, K, V] = PartitionRecords(topicPartition, records.map(_.mapK(fk)).translate(fk))

object PartitionRecords:
  given [K, V]: FunctorK[[F[_]] =>> PartitionRecords[F, K, V]] with
    override def mapK[F[_], G[_]](partition: PartitionRecords[F, K, V])(fk: FunctionK[F, G]): PartitionRecords[G, K, V] = partition.mapK(fk)

  private[xkafka] def fromConsumer[F[_]: Async, K, V](
      consumer: KafkaConsumer[F, K, V],
      assignments: Stream[F, AssignmentReport],
      maxQueuedRecords: Int,
      pausing: PartitionPausing[F]
  ): Stream[F, PartitionRecords[F, K, V]] =
    validateMaxQueuedRecords(maxQueuedRecords) match
      case Left(error)  => Stream.raiseError(error)
      case Right(valid) => Stream.eval(Runtime.create(consumer, assignments, valid, pausing)).flatMap: runtime =>
          runtime.stream.onFinalize(runtime.close)

  private[xkafka] def validateMaxQueuedRecords(value: Int): Either[IllegalArgumentException, Int] =
    Either.cond(value > 0, value, new IllegalArgumentException("maxQueuedRecords must be positive"))

  /** `queued` counts what has been reserved for routing and not yet read, which is what decides whether the partition is paused. `assignment` is
    * which assignment of the partition it holds, by how many times the group had assigned the partition.
    */
  private final case class PartitionState[F[_], K, V](
      topicPartition: TopicPartition,
      assignment: Long,
      channel: Channel[F, CommittableConsumerRecord[F, K, V]],
      queued: Ref[F, Int],
      paused: Ref[F, Boolean],
      mutex: Mutex[F],
      // Set when the partition is revoked, after which its records are no longer handed out: the group has given them to another member.
      revoked: Ref[F, Boolean]
  )

  private final class Runtime[F[_], K, V](
      consumer: KafkaConsumer[F, K, V],
      assignments: Stream[F, AssignmentReport],
      maxQueuedRecords: Int,
      pausing: PartitionPausing.Backend[F],
      output: Channel[F, PartitionRecords[F, K, V]],
      states: Ref[F, Map[TopicPartition, PartitionState[F, K, V]]],
      // The assignment each partition's last stream ended at, so a record read under it, routed after, opens no stream of a partition revoked.
      retired: Ref[F, Map[TopicPartition, Long]],
      mutex: Mutex[F]
  )(using F: Async[F]):
    /** A record source that ends carries the partition streams with it, so a graceful stop reaches whoever is reading partitions. */
    def stream: Stream[F, PartitionRecords[F, K, V]] =
      output.stream.concurrently(followAssignments).concurrently(consumer.routedRecords.evalMap(route).onFinalize(close).drain)

    /** Ends every partition's stream, and resumes the partitions it paused, which the next reader of the consumer would otherwise never get records
      * of. A consumer being released may refuse the resume, which then matters to no one.
      */
    def close: F[Unit] =
      mutex.lock.surround:
        F.uncancelable: _ =>
          states.get.flatMap(current => current.values.toList.traverse_(state => state.channel.close.void >> resumed(state))) >> output.close.void

    private def resumed(state: PartitionState[F, K, V]): F[Unit] =
      state.mutex.lock.surround:
        state.paused.getAndSet(false).flatMap(wasPaused => F.whenA(wasPaused)(pausing.resume(Set(state.topicPartition)).attempt.void))

    private def followAssignments: Stream[F, Nothing] = assignments.evalMap(reconcile).drain

    /** Opens a stream for each assigned partition, so one appears as soon as the partition is owned, and ends the streams of revoked ones, and of
      * those assigned again since their stream was opened, which may have been revoked between two looks at the assignment.
      */
    private def reconcile(report: AssignmentReport): F[Unit] =
      report.assigned.toList.traverse_(partition => stateFor(partition, Some(report.lease(partition)), F.pure(true), assigned = true).void) >>
        revokeMissing(report)

    /** Routes a record, opening the partition's stream first if the assignment has not reported it yet.
      *
      * A record read under its partition's current lease is proof that the partition is assigned, so routing never waits for confirmation. One whose
      * lease has ended, decoded after its partition was revoked, opens no stream, as the group has given it to another member: the lease is asked
      * under the lock the revocation takes, which it takes once the lease has ended.
      */
    private def route(record: CommittableConsumerRecord[F, K, V]): F[Unit] =
      val number = record.offset.assignment.filter(_ => consumer.numbersAssignments)
      stateFor(record.record.topicPartition, number, held(record), assigned = false).flatMap(_.traverse_(routedTo(_, record)))

    private def held(record: CommittableConsumerRecord[F, K, V]): F[Boolean] = record.offset.stillHeld.getOrElse(F.pure(true))

    /** Whether the record is handed out, as it is only here that the application gets it. */
    private def handing(record: CommittableConsumerRecord[F, K, V]): F[Boolean] = record.offset.handingOut.getOrElse(held(record))

    private def routedTo(state: PartitionState[F, K, V], record: CommittableConsumerRecord[F, K, V]): F[Unit] =
      state.mutex.lock.surround:
        // One revoked while the record waited for the lock is not paused for it, which librdkafka would keep through the revocation.
        state.revoked.get.ifM(F.unit, delivered(state, record))

    private def delivered(state: PartitionState[F, K, V], record: CommittableConsumerRecord[F, K, V]): F[Unit] =
      F.uncancelable: poll =>
        state.queued.updateAndGet(_ + 1).flatMap: queued =>
          F.guaranteeCase(poll(pauseWhenFull(state, queued))):
            case Outcome.Succeeded(_) => F.unit
            case _                    => rollback(state)
          .flatMap: _ =>
            state.channel.send(record).onError(_ => rollback(state)).flatMap:
              case Right(()) => F.unit
              case Left(_)   => rollback(state)

    private def rollback(state: PartitionState[F, K, V]): F[Unit] =
      state.queued.updateAndGet(_ - 1).flatMap(queued => resumeWhenDrained(state, queued))

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
        // The records waiting are handed out together, as a chunk. Those queued when the partition was revoked are not: the group gave them to another
        // member, and a commit of them is refused.
        state.channel.stream.chunks.evalMap: chunk =>
          state.mutex.lock.surround:
            state.queued.updateAndGet(_ - chunk.size).flatMap(queued => resumeWhenDrained(state, queued)) *>
              state.revoked.get.ifM(F.pure(Chunk.empty), chunk.filterA(handing))
        .unchunks
      )

    /** The state of the partition's `assignment`, where it is not one the stream has moved past. A later one ends the stream of an earlier, as the
      * partition was revoked in between, and a record of an earlier one is not handed out, its partition having been revoked since.
      */
    private def stateFor(
        topicPartition: TopicPartition,
        assignment: Option[Long],
        held: F[Boolean],
        assigned: Boolean
    ): F[Option[PartitionState[F, K, V]]] =
      mutex.lock.surround:
        held.ifM(
          states.get.map(_.get(topicPartition)).flatMap:
            case Some(state) => assignment match
                case Some(number) if number > state.assignment =>
                  F.uncancelable(_ => ended(List(state))) *> stateOf(topicPartition, number).map(Some(_))
                case Some(number) if number < state.assignment => F.pure(None)
                case _                                         => F.pure(Some(state))
            // A record of an assignment whose stream has ended opens none: its partition was revoked. The assignment itself opens one.
            case None => retired.get.flatMap: ended =>
                val past = !assigned && assignment.exists(number => ended.get(topicPartition).exists(_ >= number))
                if past then F.pure(None) else stateOf(topicPartition, assignment.getOrElse(0L)).map(Some(_))
          ,
          F.pure(None)
        )

    private def stateOf(topicPartition: TopicPartition, assignment: Long): F[PartitionState[F, K, V]] =
      states.get.flatMap: current =>
        current.get(topicPartition) match
          case Some(state) => F.pure(state)
          case None        =>
            for
              // Pausing is what bounds this, so a record already in flight when the pause was issued still lands.
              channel <- Channel.unbounded[F, CommittableConsumerRecord[F, K, V]]
              queued  <- Ref.of[F, Int](0)
              paused  <- Ref.of[F, Boolean](false)
              mutex   <- Mutex[F]
              revoked <- Ref.of[F, Boolean](false)
              state = PartitionState(topicPartition, assignment, channel, queued, paused, mutex, revoked)
              _ <- states.set(current.updated(topicPartition, state))
              _ <- output.send(readable(state)).void
            yield state

    /** Ends the streams of the partitions the assignment no longer has. A stream opened by a record of a later assignment than the report knew of is
      * not one it can speak for: the report was taken before that assignment, and is reconciled late.
      */
    private def revokeMissing(report: AssignmentReport): F[Unit] =
      mutex.lock.surround:
        F.uncancelable: _ =>
          states.get.flatMap: current =>
            ended(current.removedAll(report.assigned).values.toList.filter(state => state.assignment <= report.lease(state.topicPartition)))

    /** Ends the streams of partitions revoked, under the runtime's lock. librdkafka keeps a partition paused through its revocation, so one paused
      * for its reader is resumed, to be fetched once assigned again. Each is resumed under its own lock, after a pause being made for a record, and
      * all are, though one fails, before the first failure is raised.
      */
    private def ended(revoked: List[PartitionState[F, K, V]]): F[Unit] =
      val revokedAt = revoked.map(state => state.topicPartition -> state.assignment)
      retired.update(_ ++ revokedAt) >> states.update(_.removedAll(revoked.map(_.topicPartition))) >> revoked.traverse: state =>
        state.mutex.lock.surround:
          state.revoked.set(true) >> state.channel.close.void >> state.paused.getAndSet(false)
            .flatMap(wasPaused => F.whenA(wasPaused)(pausing.resume(Set(state.topicPartition))))
        .attempt
      .flatMap: outcomes =>
        outcomes.collect { case Left(error) => error } match
          case first :: rest => F.raiseError(rest.foldLeft(first) { (all, more) => all.addSuppressed(more); all })
          case Nil           => F.unit

  private object Runtime:
    def create[F[_]: Async, K, V](
        consumer: KafkaConsumer[F, K, V],
        assignments: Stream[F, AssignmentReport],
        maxQueuedRecords: Int,
        pausing: PartitionPausing[F]
    ): F[Runtime[F, K, V]] =
      for
        output  <- Channel.unbounded[F, PartitionRecords[F, K, V]]
        states  <- Ref.of[F, Map[TopicPartition, PartitionState[F, K, V]]](Map.empty)
        retired <- Ref.of[F, Map[TopicPartition, Long]](Map.empty)
        mutex   <- Mutex[F]
      yield Runtime(consumer, assignments, maxQueuedRecords, pausing.orNoop, output, states, retired, mutex)
