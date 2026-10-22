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
package kafkaclients

import org.apache.kafka.clients.consumer.{CommitFailedException, InvalidOffsetException}
import org.apache.kafka.common.KafkaException as JavaKafkaException
import org.apache.kafka.common.errors.{
  AuthenticationException, InvalidConfigurationException, InvalidPidMappingException, ProducerFencedException, SaslAuthenticationException,
  SslAuthenticationException, TransactionAbortedException
}
import org.apache.kafka.common.protocol.Errors

/** The portable failure for each Java client exception, classified the way librdkafka classifies the same condition. */
private[kafkaclients] object JavaFailures:
  val portable: PartialFunction[Throwable, Throwable] = { case error: JavaKafkaException =>
    val code = protocolCode(error)
    new KafkaException.BackendFailure(
      Option(error.getMessage).getOrElse(error.getClass.getName),
      code = code,
      // Derived from the portable code, so the same condition is retriable on every backend.
      retriable = code.map(_.retriable),
      fatal = Some(error.isInstanceOf[InvalidPidMappingException] || error.isInstanceOf[ProducerFencedException]),
      cause = error
    )
  }

  /** Why one record was not delivered. The Java client reports its own exceptions there, and anything else is kept as the cause. */
  def delivery(error: Throwable): KafkaException =
    portable.lift(error).collect { case failure: KafkaException => failure }
      .getOrElse(new KafkaException.BackendFailure(Option(error.getMessage).getOrElse(error.getClass.getName), cause = error))

  /** Kafka maps its own exceptions back to the protocol error table, which is the same table librdkafka reports.
    *
    * Authentication never reached the broker, so it has no entry in that table. `Errors.forException` answers `INVALID_CONFIG` for the whole family,
    * which is a code the broker never sent, so those are named here.
    *
    * A transaction's offsets that the group rejects as no longer its member's come back as one `CommitFailedException` for either of two answers, and
    * only its message, which ends with that answer's own description, says which. Reading it back gives the code librdkafka reports for the same
    * rejection. A consumer that has left its group refuses a commit itself, with a message of its own, where librdkafka sends it without a member id
    * and the broker answers that the member is unknown.
    */
  private def protocolCode(error: JavaKafkaException): Option[ErrorCode] =
    error match
      case _: SslAuthenticationException  => Some(ErrorCode.SslAuthenticationFailed)
      case _: SaslAuthenticationException => Some(ErrorCode.SaslAuthenticationFailed)
      case _: AuthenticationException     => None
      case failed: CommitFailedException  => rejectedMembership(failed).orElse(tabled(error))
      // The Java client fails a record its aborted transaction never sent with this, and Kafka's table has no entry for it.
      case _: TransactionAbortedException => Some(ErrorCode.Purged)
      // The consumer raises these itself, for a partition its reset policy leaves without a position.
      case _: InvalidOffsetException => Some(ErrorCode.OffsetResetRequired)
      // A client that cannot be built wraps the setting it could not use in failures of its own.
      case _ if causes(error).exists(_.isInstanceOf[InvalidConfigurationException]) => Some(ErrorCode.InvalidConfiguration)
      case _                                                                        => tabled(error)

  private def causes(error: Throwable): Iterator[Throwable] = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).take(16)

  private def tabled(error: JavaKafkaException): Option[ErrorCode] =
    Option(Errors.forException(error)).filterNot(_ == Errors.NONE).map(value => ErrorCode.fromProtocol(value.code.toInt))

  /** The two answers the Java client folds into a `CommitFailedException` when a transaction records offsets, and its refusal to commit outside a
    * group.
    */
  private def rejectedMembership(failed: CommitFailedException): Option[ErrorCode] =
    Option(failed.getMessage).flatMap: message =>
      if message.contains(NotInGroup) then Some(ErrorCode.UnknownMemberId)
      else
        List(Errors.ILLEGAL_GENERATION, Errors.UNKNOWN_MEMBER_ID).find(value => message.endsWith(value.exception.getMessage))
          .map(value => ErrorCode.fromProtocol(value.code.toInt))

  /** How the Java client words a commit it refuses because the consumer is no longer a member of its group. */
  private val NotInGroup = "since the consumer is not part of an active group"
