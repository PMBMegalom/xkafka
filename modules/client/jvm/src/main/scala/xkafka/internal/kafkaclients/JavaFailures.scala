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
import org.apache.kafka.clients.producer.KafkaProducer as JavaProducer
import org.apache.kafka.clients.producer.internals.TransactionManager
import org.apache.kafka.common.KafkaException as JavaKafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{
  AuthenticationException, AuthorizationException, ClusterAuthorizationException, InvalidConfigurationException, InvalidPidMappingException,
  ProducerFencedException, RetriableException, SaslAuthenticationException, SslAuthenticationException, TransactionAbortedException,
  TransactionalIdAuthorizationException, UnsupportedVersionException
}
import org.apache.kafka.common.protocol.Errors

/** The portable failure for each Java client exception, classified the way librdkafka classifies the same condition. */
private[kafkaclients] object JavaFailures:
  val portable: PartialFunction[Throwable, Throwable] = { case error: JavaKafkaException => failure(error, producer = false) }

  /** A producer's failure, of which the Java client holds some fatal, and whose error state only an abort leaves. */
  val produced: PartialFunction[Throwable, Throwable] = { case error: JavaKafkaException => failure(error, producer = true) }

  /** A failure of `producer`, fatal exactly where the producer itself holds a fatal error, as the Java client knows best whether one is: a failure it
    * is not held fatal for, as an error state, is left by an abort. Where the producer cannot say, as `produced` classifies it.
    */
  def producedBy(producer: AnyRef): PartialFunction[Throwable, Throwable] = { case error: JavaKafkaException =>
    held(failure(error, producer = true), producer)
  }

  private def held(classified: KafkaException.BackendFailure, producer: AnyRef): KafkaException.BackendFailure =
    fatalHeld(producer) match
      case Some(true) =>
        new KafkaException.BackendFailure(classified.detail, classified.code, Some(false), Some(true), Some(false), classified.getCause)
      case Some(false) =>
        val abortable = classified.transactionAbortRequired.isDefined
        new KafkaException.BackendFailure(
          classified.detail,
          classified.code,
          if abortable then Some(false) else classified.retriable,
          Some(false),
          if abortable then Some(true) else classified.transactionAbortRequired,
          classified.getCause
        )
      case None => classified

  /** The producer's transaction manager and whether it holds a fatal error, which the Java client keeps to itself; none where it cannot be asked. */
  private lazy val asking: Option[(java.lang.reflect.Field, java.lang.reflect.Method)] =
    scala.util.Try {
      val manager = classOf[JavaProducer[?, ?]].getDeclaredField("transactionManager")
      val fatal   = classOf[TransactionManager].getDeclaredMethod("hasFatalError")
      manager.setAccessible(true)
      fatal.setAccessible(true)
      (manager, fatal)
    }.toOption

  private[kafkaclients] def fatalHeld(producer: AnyRef): Option[Boolean] =
    producer match
      case client: JavaProducer[?, ?] => asking
          .flatMap((manager, fatal) => scala.util.Try(Option(manager.get(client)).map(fatal.invoke(_).asInstanceOf[Boolean])).toOption.flatten)
      case _ => None

  private def failure(error: JavaKafkaException, producer: Boolean): KafkaException.BackendFailure =
    val message = Option(error.getMessage).getOrElse(error.getClass.getName)
    error.getCause match
      // The error state a failure leaves the producer in, which the Java client reports by that failure as its cause. Retrying a call cannot leave
      // that state: only aborting the transaction does, unless the failure is fatal. Java's abortable failures are each of a class of their own, so
      // one that is a plain client exception, as an answer the client did not expect, is fatal; so is an abort that failed.
      case cause: JavaKafkaException if producer && error.getClass == classOf[JavaKafkaException] && message == ErrorState =>
        val inner = failure(cause, producer)
        val fatal = inner.fatal.contains(true) || cause.getClass == classOf[JavaKafkaException]
        terminal(message, inner, fatal, error)
      case cause: JavaKafkaException if producer && error.getClass == classOf[JavaKafkaException] && message == AbortFailed =>
        terminal(message, failure(cause, producer), fatal = true, error)
      case cause: JavaKafkaException if error.getClass == classOf[JavaKafkaException] =>
        val inner = failure(cause, producer)
        new KafkaException.BackendFailure(message, inner.code, inner.retriable, inner.fatal, cause = error)
      case _ =>
        val code = protocolCode(error)
        new KafkaException.BackendFailure(
          message,
          code = code,
          // Derived from the portable code, so the same condition is retriable on every backend.
          retriable = code.map(_.retriable),
          fatal = fatality(error, producer),
          cause = error
        )

  /** A transactional producer's failure that retrying cannot leave: fatal, or else left only by an abort. */
  private def terminal(message: String, inner: KafkaException.BackendFailure, fatal: Boolean, error: JavaKafkaException) =
    new KafkaException.BackendFailure(
      message,
      inner.code,
      retriable = Some(false),
      fatal = Some(fatal).filter(identity).orElse(inner.fatal),
      transactionAbortRequired = Some(!fatal),
      cause = error
    )

  /** How the Java client words the failure of a transactional call made in an error state, and of an abort that failed, which it holds fatal. */
  private val ErrorState  = "Cannot execute transactional method because we are in an error state"
  private val AbortFailed = "Failed to abort transaction"

  /** Whether the Java client cannot recover: true for the failures its producer holds fatal, false for those it retries, and not known for others. An
    * administration client and a consumer recover from every failure, so none of theirs is known fatal.
    */
  private def fatality(error: JavaKafkaException, producer: Boolean): Option[Boolean] =
    error match
      case _: RetriableException => Some(false)
      case _: ClusterAuthorizationException | _: TransactionalIdAuthorizationException | _: ProducerFencedException | _: UnsupportedVersionException |
          _: InvalidPidMappingException if producer => Some(true)
      case _ => None

  /** Why one record was not delivered. The Java client reports its own exceptions there, and anything else is kept as the cause. */
  def delivery(error: Throwable, producer: AnyRef): KafkaException =
    producedBy(producer).lift(error).collect { case failure: KafkaException => failure }
      .getOrElse(new KafkaException.BackendFailure(Option(error.getMessage).getOrElse(error.getClass.getName), cause = error))

  /** Kafka maps its own exceptions back to the protocol error table, which is the same table librdkafka reports.
    *
    * Authentication never reached the broker, so it has no entry in that table. `Errors.forException` answers `INVALID_CONFIG` for the whole family,
    * which is a code the broker never sent, so those are named here.
    *
    * A transaction's offsets that the group rejects as no longer its member's come back as one `CommitFailedException` for any of three answers, and
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
      // Kafka's authorization failures extend its configuration failure, though each has its own code.
      case _: AuthorizationException => tabled(error)
      // A client that cannot be built wraps the setting it could not use in failures of its own, or refuses a combination of settings outright.
      case _ if causes(error).exists(cause => cause.isInstanceOf[InvalidConfigurationException] || cause.isInstanceOf[ConfigException]) =>
        Some(ErrorCode.InvalidConfiguration)
      case _ => tabled(error)

  private def causes(error: Throwable): Iterator[Throwable] = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).take(16)

  private def tabled(error: JavaKafkaException): Option[ErrorCode] =
    Option(Errors.forException(error)).filterNot(_ == Errors.NONE).map(value => ErrorCode.fromProtocol(value.code.toInt))

  /** The three answers the Java client folds into a `CommitFailedException` when a transaction records offsets, its refusal to commit outside a
    * group, and its own wording for a plain commit the group refuses as ILLEGAL_GENERATION or UNKNOWN_MEMBER_ID, which does not say which, taken as
    * ILLEGAL_GENERATION.
    */
  private def rejectedMembership(failed: CommitFailedException): Option[ErrorCode] =
    Option(failed.getMessage).flatMap: message =>
      if message.contains(NotInGroup) then Some(ErrorCode.UnknownMemberId)
      else if message == Rebalanced then Some(ErrorCode.IllegalGeneration)
      else
        List(Errors.ILLEGAL_GENERATION, Errors.UNKNOWN_MEMBER_ID, Errors.STALE_MEMBER_EPOCH)
          .find(value => message.endsWith(value.exception.getMessage)).map(value => ErrorCode.fromProtocol(value.code.toInt))

  /** How the Java client words a plain commit the group refused. */
  private val Rebalanced = new CommitFailedException().getMessage

  /** How the Java client words a commit it refuses because the consumer is no longer a member of its group. */
  private val NotInGroup = "since the consumer is not part of an active group"
