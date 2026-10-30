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
package internal.streams

import scala.concurrent.duration.*

import cats.{Functor, Monad}
import cats.effect.{Async, Deferred, Ref, Resource, Temporal}
import cats.effect.std.{NonEmptyHotswap, Queue, Random, Supervisor, UUIDGen}
import cats.syntax.all.*
import fs2.Stream
import scodec.bits.ByteVector
import internal.group.Connection
import internal.protocol.{OffsetCommitRequest, OffsetFetchRequest, Request}

/** A member of a streams group, heartbeating to the group's coordinator on fibers of its own. The engine carries out the changes it is told of and
  * confirms each by its round.
  */
private[xkafka] trait GroupMember[F[_]]:
  /** The changes to the tasks the engine runs, in order: `Revoke`, `Assign` and `LoseAll` actions only. It ends once the member has left, and fails
    * with the error that stopped the member.
    */
  def changes: Stream[F, MemberAction]

  /** The engine gave back the tasks of the `Revoke` of this round. */
  def revoked(round: Long): F[Unit]

  /** The engine runs the assignment of the `Assign` of this round. */
  def assigned(round: Long): F[Unit]

  /** Gives back the active tasks through `changes`, tells the coordinator the member leaves and waits until it has answered. */
  def leave: F[Unit]

  /** The member's epoch: 0 until the coordinator has taken it in, and again after it was fenced, until it is taken in again. */
  def epoch: F[Int]

  /** Commits offsets for the group under the member's id and current epoch, through its connection to the coordinator, answering each partition's
    * error code, or 0. The coordinator checks the epoch, not which tasks the member runs, so the engine commits only for its own.
    */
  def commit(topics: List[OffsetCommitRequest.Topic]): F[List[OffsetCommitRequest.Answer]]

  /** The offsets the group stored for these partitions, read under the member's id and current epoch. A partition whose offsets a transaction has yet
    * to settle answers an error to ask again with.
    */
  def committed(topics: List[OffsetFetchRequest.Topic]): F[OffsetFetchRequest.Answer]

private[xkafka] object GroupMember:
  /** The first wait before a heartbeat that failed goes again, the Java client's `retry.backoff.ms`. */
  val Backoff: FiniteDuration = 100.millis

  /** The longest wait before a heartbeat that failed goes again, the Java client's `retry.backoff.max.ms`. */
  val MaxBackoff: FiniteDuration = 1.second

  /** A member that joins at once, through connections to the coordinator that `coordinator` makes, finding the coordinator again whenever it must.
    * Released before it has left, it leaves, waiting at most `closeTimeout` for the engine to give back its tasks and for the coordinator to answer.
    */
  def apply[F[_]](coordinator: Resource[F, Connection[F]], identity: MemberIdentity, closeTimeout: FiniteDuration)(using
      F: Async[F]
  ): Resource[F, GroupMember[F]] =
    for
      // The connection is closed only once every fiber that might use it has stopped.
      held       <- NonEmptyHotswap.empty[F, Connection[F]]
      supervisor <- Supervisor[F](await = false)
      random     <- Resource.eval(Random.scalaUtilRandom[F])
      events     <- Resource.eval(Queue.unbounded[F, MemberEvent])
      out        <- Resource.eval(Queue.unbounded[F, Option[Either[Throwable, MemberAction]]])
      ended      <- Resource.eval(Deferred[F, Either[Throwable, Unit]])
      timer      <- Resource.eval(F.ref(0L))
      current    <- Resource.eval(F.ref(Member(identity)))
      loop = Loop(coordinator, supervisor, random, events, out, ended, timer, held, current)
      _ <- Resource.make(supervisor.supervise(loop.run(Member(identity)).void))(_ => leaving(events, ended, closeTimeout))
      _ <- Resource.eval(events.offer(MemberEvent.Start))
    yield new GroupMember[F]:
      def changes: Stream[F, MemberAction] = Stream.fromQueueNoneTerminated(out).rethrow
      def revoked(round: Long): F[Unit]    = events.offer(MemberEvent.Revoked(round))
      def assigned(round: Long): F[Unit]   = events.offer(MemberEvent.Assigned(round))
      def leave: F[Unit]                   = events.offer(MemberEvent.Leave) *> ended.get.rethrow
      def epoch: F[Int]                    = current.get.map(_.epoch)

      def commit(topics: List[OffsetCommitRequest.Topic]): F[List[OffsetCommitRequest.Answer]] =
        current.get.flatMap(member => loop.send(OffsetCommitRequest(identity.groupId, identity.memberId, member.epoch, None, topics)))

      def committed(topics: List[OffsetFetchRequest.Topic]): F[OffsetFetchRequest.Answer] =
        current.get
          .flatMap(member => loop.send(OffsetFetchRequest(identity.groupId, Some(identity.memberId), member.epoch, topics, requireStable = true)))

  /** A new member id as the Java client makes one: a random UUID in URL-safe base64 without padding, never starting with a dash. */
  def memberId[F[_]](using F: Monad[F], uuids: UUIDGen[F]): F[String] =
    uuids.randomUUID.flatMap: uuid =>
      val id = (ByteVector.fromLong(uuid.getMostSignificantBits) ++ ByteVector.fromLong(uuid.getLeastSignificantBits)).toBase64UrlNoPad
      if id.startsWith("-") then memberId else F.pure(id)

  /** The wait before the heartbeat that failed for the `attempt`th time in a row goes again: doubling from `Backoff` up to `MaxBackoff`, then spread
    * by a fifth either way, never over `MaxBackoff`, as the Java client's `ExponentialBackoff` does.
    */
  def backoff[F[_]](attempt: Int, random: Random[F])(using F: Functor[F]): F[FiniteDuration] =
    val exponent = math.min(attempt - 1, math.log(MaxBackoff / Backoff) / math.log(2))
    random.betweenDouble(0.8, 1.2).map(factor => (Backoff.toMillis * math.pow(2, exponent) * factor).toLong.millis.min(MaxBackoff))

  /** A failure the connection's contract does not name, which stops the member. */
  private def unexpected(error: Throwable): KafkaException =
    new KafkaException.BackendFailure(s"the heartbeat failed: $error", retriable = Some(false), cause = error)

  private def leaving[F[_]](events: Queue[F, MemberEvent], ended: Deferred[F, Either[Throwable, Unit]], timeout: FiniteDuration)(using
      F: Temporal[F]
  ): F[Unit] = events.offer(MemberEvent.Leave) *> F.timeoutTo(ended.get.void, timeout, F.unit)

  private final class Loop[F[_]](
      coordinator: Resource[F, Connection[F]],
      supervisor: Supervisor[F],
      random: Random[F],
      events: Queue[F, MemberEvent],
      out: Queue[F, Option[Either[Throwable, MemberAction]]],
      ended: Deferred[F, Either[Throwable, Unit]],
      timer: Ref[F, Long],
      held: NonEmptyHotswap[F, Option[Connection[F]]],
      current: Ref[F, Member]
  )(using F: Temporal[F]):
    import MemberAction.*

    def run(member: Member): F[Unit] =
      events.take.flatMap: event =>
        val (next, actions) = member.next(event)
        current.set(next) *> actions.traverse_(perform) *> run(next)

    /** Sends a request of the engine's over the member's connection, which it gives up, to be made again, if the request fails. */
    def send[R](request: Request[R]): F[R] = connection.flatMap(_.send(request)).onError(_ => drop)

    private def perform(action: MemberAction): F[Unit] =
      action match
        case Send(request) =>
          val answer =
            connection.flatMap(_.send(request)).attempt.flatMap:
              case Right(response)             => events.offer(MemberEvent.Answered(response))
              case Left(error: KafkaException) => drop *> events.offer(MemberEvent.RequestFailed(error))
              case Left(other)                 => drop *> events.offer(MemberEvent.RequestFailed(unexpected(other)))
          supervisor.supervise(answer).void
        case Wait(after)                         => due(after)
        case Retry(attempt, rediscover, delayed) => drop.whenA(rediscover) *>
            (if delayed then backoff(attempt, random).flatMap(due) else due(Duration.Zero))
        case change @ (Revoke(_, _) | Assign(_, _) | LoseAll) => out.offer(Some(Right(change)))
        case Fail(error)                                      => out.offer(Some(Left(error))) *> ended.complete(Left(error)).void
        case Done                                             => out.offer(None) *> ended.complete(Right(())).void

    /** Sends `Due` after `after`, unless another heartbeat was planned meanwhile. */
    private def due(after: FiniteDuration): F[Unit] =
      timer.updateAndGet(_ + 1).flatMap: planned =>
        supervisor.supervise(F.sleep(after) *> timer.get.flatMap(now => events.offer(MemberEvent.Due).whenA(now == planned))).void

    private def connection: F[Connection[F]] = held.get.use(F.pure).flatMap(_.fold(held.swap(coordinator.map(Some(_))) *> connection)(F.pure))

    private def drop: F[Unit] = held.swap(Resource.pure(None))
