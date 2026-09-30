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

import scala.concurrent.duration.*

import cats.effect.{Deferred, IO, Ref}
import cats.effect.std.Random
import cats.syntax.all.*
import munit.CatsEffectSuite

final class CommitRecoverySuite extends CatsEffectSuite:
  private val topicPartition = TopicPartition(Topic.from("events").toOption.get, Partition.from(0).toOption.get)
  private val offsets        = Map(topicPartition -> Offset.from(1L).toOption.get)

  test("a policy rejects the values it cannot honour"):
    assertEquals(CommitRecovery.exponential(-1), Left(ValidationError.NegativeAttemptCount(-1)))
    assertEquals(CommitRecovery.exponential(1, initialDelay = Duration.Zero), Left(ValidationError.NonPositiveDelay(Duration.Zero)))
    assertEquals(CommitRecovery.exponential(1, maxDelay = -1.second), Left(ValidationError.NonPositiveDelay(-1.second)))
    assertEquals(CommitRecovery.exponential(1, jitter = 1.5), Left(ValidationError.JitterOutOfRange(1.5)))
    assertEquals(CommitRecovery.exponential(1, jitter = -0.1), Left(ValidationError.JitterOutOfRange(-0.1)))
    assert(CommitRecovery.exponential(1).isRight)

  test("delays double from the first, and stop growing at the maximum"):
    val policy = CommitRecovery.exponential(10, initialDelay = 10.millis, maxDelay = 100.millis, jitter = 0d).toOption.get

    // A sample of 0.5 is the midpoint, which with no jitter is the delay itself.
    assertEquals(
      List.range(1, 7).map(attempt => policy.delayFor(attempt, 0.5)),
      List(10.millis, 20.millis, 40.millis, 80.millis, 100.millis, 100.millis)
    )

  test("jitter spreads a delay by its fraction, in both directions"):
    val policy = CommitRecovery.exponential(10, initialDelay = 100.millis, maxDelay = 1.second, jitter = 0.25).toOption.get

    assertEquals(policy.delayFor(1, 0d), 75.millis)
    assertEquals(policy.delayFor(1, 0.5), 100.millis)
    // The spread is applied after the growth is capped, so it reaches above the maximum by the same fraction.
    assertEquals(policy.delayFor(1, 1d), 125.millis)
    assertEquals(policy.delayFor(20, 1d), 1250.millis)

  test("a delay never reaches zero, however small the policy and the sample"):
    val policy = CommitRecovery.exponential(1, initialDelay = 1.nanos, maxDelay = 1.nanos, jitter = 1d).toOption.get

    assert(policy.delayFor(1, 0d) > Duration.Zero, s"got ${policy.delayFor(1, 0d)}")

  test("doubling does not overflow at an attempt no policy would reach"):
    val policy = CommitRecovery.exponential(Int.MaxValue, initialDelay = 1.second, maxDelay = 30.seconds, jitter = 0d).toOption.get

    assertEquals(policy.delayFor(Int.MaxValue, 0.5), 30.seconds)

  test("a retriable failure is tried again until it succeeds"):
    val policy = CommitRecovery.exponential(5, initialDelay = 1.millis, maxDelay = 2.millis, jitter = 0d).toOption.get

    for
      attempts <- Ref[IO].of(0)
      random   <- Random.scalaUtilRandom[IO]
      committer  = failing(attempts, failures = 3, failure = retriableFailure)
      recovering = CommitRecovery.recovering(committer, policy, random)
      _        <- recovering.commit(offsets)
      observed <- attempts.get
    yield assertEquals(observed, 4, "three failures and the attempt that succeeded")

  test("a failure that is not retriable fails on the first attempt"):
    val policy = CommitRecovery.exponential(5, initialDelay = 1.millis, maxDelay = 2.millis, jitter = 0d).toOption.get

    for
      attempts <- Ref[IO].of(0)
      random   <- Random.scalaUtilRandom[IO]
      committer  = failing(attempts, failures = Int.MaxValue, failure = permanentFailure)
      recovering = CommitRecovery.recovering(committer, policy, random)
      outcome  <- recovering.commit(offsets).attempt
      observed <- attempts.get
    yield
      assertEquals(observed, 1, "a permanent failure should not be tried again")
      assert(outcome.left.exists(_.eq(permanentFailure)), s"the original failure should reach the caller, got $outcome")

  test("a retriable failure that never clears reports how many attempts were made"):
    val policy = CommitRecovery.exponential(3, initialDelay = 1.millis, maxDelay = 2.millis, jitter = 0d).toOption.get

    for
      attempts <- Ref[IO].of(0)
      random   <- Random.scalaUtilRandom[IO]
      committer  = failing(attempts, failures = Int.MaxValue, failure = retriableFailure)
      recovering = CommitRecovery.recovering(committer, policy, random)
      outcome  <- recovering.commit(offsets).attempt
      observed <- attempts.get
    yield
      assertEquals(observed, 4, "the first attempt and the three the policy allowed")
      outcome match
        case Left(failure: KafkaException.CommitFailed) =>
          assertEquals(failure.attempts, 4)
          assertEquals(failure.offsets, offsets)
          assertEquals(failure.getCause, retriableFailure)
        case other => fail(s"expected a CommitFailed, got $other")

  test("a policy that never retries leaves the committer alone"):
    for
      attempts <- Ref[IO].of(0)
      random   <- Random.scalaUtilRandom[IO]
      committer  = failing(attempts, failures = Int.MaxValue, failure = retriableFailure)
      recovering = CommitRecovery.recovering(committer, CommitRecovery.none, random)
      outcome  <- recovering.commit(offsets).attempt
      observed <- attempts.get
    yield
      assertEquals(observed, 1)
      assert(outcome.isLeft)

  test("recovery keeps the group a committer's offsets belong to"):
    val policy = CommitRecovery.exponential(3).toOption.get

    for
      attempts <- Ref[IO].of(0)
      random   <- Random.scalaUtilRandom[IO]
      committer = failing(attempts, failures = 0, failure = retriableFailure)
      handle <- CommitRecovery.recovering(committer, policy, random).membership.handle.traverse(_.acquire)
    yield assertEquals(handle, Some(TestGroupHandle))

  test("recovery sends every attempt at a leased commit through the committer's own check"):
    val policy = CommitRecovery.exponential(3, initialDelay = 1.milli, maxDelay = 1.milli).toOption.get

    for
      checks <- Ref[IO].of(0)
      random <- Random.scalaUtilRandom[IO]
      checking =
        new OffsetCommitter[IO]:
          override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = IO.raiseError(retriableFailure)

          override private[xkafka] def commitLeased(offsets: Map[TopicPartition, (Offset, Option[Lease])]): IO[Unit] =
            checks.update(_ + 1) *> commit(offsets.view.mapValues(_._1).toMap)
      outcome  <- CommitRecovery.recovering(checking, policy, random).commitLeased(offsets.view.mapValues(_ -> None).toMap).attempt
      observed <- checks.get
    yield
      // One first attempt and three retries, each checked again rather than checked once around all of them.
      assertEquals(observed, 4)
      assert(outcome.isLeft)

  test("resolving several group memberships releases earlier handles when a later acquisition fails"):
    for
      acquired <- Ref[IO].of(0)
      released <- Ref[IO].of(0)
      acquire =
        acquired.getAndUpdate(_ + 1).flatMap:
          case 0 => IO.pure(TestGroupHandle)
          case _ => IO.raiseError[GroupHandle](new RuntimeException("second acquisition failed"))
      batch = membershipBatch(managedCommitter(acquire, released.update(_ + 1)), managedCommitter(acquire, released.update(_ + 1)))
      result       <- GroupMembership.resolve(batch).use_.attempt
      acquisitions <- acquired.get
      releases     <- released.get
    yield
      assert(result.isLeft)
      assertEquals(acquisitions, 2)
      assertEquals(releases, 1)

  test("cancelling group membership acquisition releases earlier handles"):
    for
      acquired <- Ref[IO].of(0)
      released <- Ref[IO].of(0)
      waiting  <- Deferred[IO, Unit]
      acquire =
        acquired.getAndUpdate(_ + 1).flatMap:
          case 0 => IO.pure(TestGroupHandle)
          case _ => waiting.complete(()).void *> IO.never[GroupHandle]
      batch = membershipBatch(managedCommitter(acquire, released.update(_ + 1)), managedCommitter(acquire, released.update(_ + 1)))
      fiber    <- GroupMembership.resolve(batch).use_.start
      _        <- waiting.get
      _        <- fiber.cancel
      releases <- released.get
    yield assertEquals(releases, 1)

  private val retriableFailure =
    new KafkaException.BackendFailure("rebalance in progress", code = Some(ErrorCode.RebalanceInProgress), retriable = Some(true))

  private val permanentFailure = new KafkaException.BackendFailure("unknown member", code = Some(ErrorCode.UnknownMemberId), retriable = Some(false))

  private def managedCommitter(acquire: IO[GroupHandle], release: IO[Unit]): OffsetCommitter[IO] =
    new OffsetCommitter[IO]:
      override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = IO.unit

      override private[xkafka] val membership: GroupMembership[IO] = GroupMembership.Backend(acquire, _ => release)

  private def membershipBatch(first: OffsetCommitter[IO], second: OffsetCommitter[IO]): CommittableOffsetBatch[IO] =
    CommittableOffsetBatch.empty[IO].updated(committable(first)).updated(committable(second))

  private def committable(value: OffsetCommitter[IO]): CommittableOffset[IO] =
    new CommittableOffset[IO]:
      override def topicPartition: TopicPartition = CommitRecoverySuite.this.topicPartition

      override def nextOffset: Offset = Offset.from(1L).toOption.get

      override def committer: OffsetCommitter[IO] = value

  /** Fails its first `failures` commits and succeeds afterwards, counting every attempt. */
  private def failing(attempts: Ref[IO, Int], failures: Int, failure: Throwable): OffsetCommitter[IO] =
    new OffsetCommitter[IO]:
      override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] =
        attempts.updateAndGet(_ + 1).flatMap(attempt => IO.raiseError(failure).whenA(attempt <= failures))

      override private[xkafka] val membership: GroupMembership[IO] = GroupMembership.Backend(IO.pure(TestGroupHandle), _ => IO.unit)

  private case object TestGroupHandle extends GroupHandle
