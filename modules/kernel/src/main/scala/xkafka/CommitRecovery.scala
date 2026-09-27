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

import cats.Show
import cats.effect.Temporal
import cats.effect.std.Random
import cats.syntax.all.*

/** How a failed offset commit is retried.
  *
  * A commit fails for two kinds of reason. Some say the broker is moving, such as a coordinator that is loading or a group that is rebalancing, and
  * those are worth trying again. The rest say the commit will never be accepted, and retrying one only delays the failure. Only the first kind is
  * retried, and `ErrorCode.retriable` is what decides which is which, so every backend retries the same conditions.
  *
  * The delay before attempt `n` grows from `initialDelay` by doubling, stops growing at `maxDelay`, and is then spread by `jitter` so that consumers
  * hitting one condition together do not return together. Spreading happens after the cap, so a delay may exceed `maxDelay` by that fraction.
  */
sealed abstract case class CommitRecovery private (maxAttempts: Int, initialDelay: FiniteDuration, maxDelay: FiniteDuration, jitter: Double):
  /** How long to wait before `attempt`, counting the first retry as 1, given a `sample` from `[0, 1)`. */
  private[xkafka] def delayFor(attempt: Int, sample: Double): FiniteDuration =
    // Doubling is bounded before it is applied, because an unbounded exponent overflows long before the cap would matter.
    val exponent = math.min(attempt - 1, 62).toDouble
    val capped   = math.min(maxDelay.toNanos.toDouble, initialDelay.toNanos.toDouble * math.pow(2d, exponent))
    val spread   = capped * (1d - jitter + sample * 2d * jitter)

    FiniteDuration(math.max(1d, spread).toLong, NANOSECONDS)

  override def toString: String =
    if maxAttempts == 0 then "none" else s"exponential(maxAttempts=$maxAttempts, initialDelay=$initialDelay, maxDelay=$maxDelay, jitter=$jitter)"

object CommitRecovery:
  /** Ten attempts, doubling from 10ms, held at 10s, spread by a fifth. */
  val Default: CommitRecovery = new CommitRecovery(10, 10.millis, 10.seconds, 0.2) {}

  /** A failed commit fails. */
  val none: CommitRecovery = new CommitRecovery(0, 10.millis, 10.seconds, 0d) {}

  /** @param maxAttempts
    *   how many times to try again after the first failure, where zero never does
    * @param initialDelay
    *   how long to wait before the first retry
    * @param maxDelay
    *   how far the doubling is allowed to grow
    * @param jitter
    *   the fraction of a delay to spread it by, from 0 for none to 1 for the whole delay
    */
  def exponential(
      maxAttempts: Int,
      initialDelay: FiniteDuration = 10.millis,
      maxDelay: FiniteDuration = 10.seconds,
      jitter: Double = 0.2
  ): Either[ValidationError, CommitRecovery] =
    if maxAttempts < 0 then Left(ValidationError.NegativeAttemptCount(maxAttempts))
    else if initialDelay <= Duration.Zero then Left(ValidationError.NonPositiveDelay(initialDelay))
    else if maxDelay <= Duration.Zero then Left(ValidationError.NonPositiveDelay(maxDelay))
    else if jitter < 0d || jitter > 1d then Left(ValidationError.JitterOutOfRange(jitter))
    else Right(new CommitRecovery(maxAttempts, initialDelay, maxDelay, jitter) {})

  given Show[CommitRecovery] = Show.fromToString

  /** Wraps `committer` so that a retriable failure is tried again, and everything else fails as it did.
    *
    * The backends build their own committers and share this, so what counts as retriable and how long a retry waits are the same on all of them.
    */
  private[xkafka] def recovering[F[_]](committer: OffsetCommitter[F], policy: CommitRecovery, random: Random[F])(using
      F: Temporal[F]
  ): OffsetCommitter[F] =
    if policy.maxAttempts == 0 then committer
    else
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] = attempt(offsets, 1)

        /** The membership travels with the committer, so a transaction still reaches the group these offsets came from. */
        override private[xkafka] def membership: GroupMembership[F] = committer.membership

        private def attempt(offsets: Map[TopicPartition, Offset], number: Int): F[Unit] =
          committer.commit(offsets).handleErrorWith:
            case failure: KafkaException.BackendFailure if failure.code.exists(_.retriable) =>
              if number > policy.maxAttempts then F.raiseError(new KafkaException.CommitFailed(number, offsets, failure))
              else random.nextDouble.flatMap(sample => F.sleep(policy.delayFor(number, sample))) >> attempt(offsets, number + 1)
            case other => F.raiseError(other)
