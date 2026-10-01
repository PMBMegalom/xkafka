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

import cats.ApplicativeThrow
import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all.*

/** A portable classification of a backend failure.
  *
  * The named cases are Kafka protocol errors, which every broker reports with the same code, plus the client-side conditions all three backends can
  * raise on their own. `Other` carries a code that has no portable meaning.
  */
enum ErrorCode derives CanEqual:
  case OffsetOutOfRange           // the requested offset is outside the partition's log
  case UnknownTopicOrPartition    // the topic or partition does not exist
  case LeaderNotAvailable         // the partition has no leader right now, such as during an election
  case NotLeaderOrFollower        // the broker no longer leads or follows the partition
  case RequestTimedOut            // a request got no answer in time
  case BrokerNotAvailable         // the broker cannot be reached
  case MessageTooLarge            // a record is larger than the broker or topic accepts
  case NetworkException           // the connection to the broker failed
  case CoordinatorLoadInProgress  // the group or transaction coordinator is still loading its state
  case CoordinatorNotAvailable    // the group or transaction coordinator is not available
  case NotCoordinator             // the broker is not the coordinator for this group or transaction
  case IllegalGeneration          // the group has moved on to a newer generation
  case UnknownMemberId            // the group does not know this member
  case RebalanceInProgress        // the group is rebalancing
  case InvalidGroupId             // the group id is not valid
  case InvalidTopic               // the topic name is not valid
  case TopicAuthorizationFailed   // the client is not authorized for the topic
  case GroupAuthorizationFailed   // the client is not authorized for the group
  case ClusterAuthorizationFailed // the client is not authorized for the cluster operation
  case UnsupportedVersion         // the broker does not support the request
  case SaslAuthenticationFailed   // SASL authentication failed
  case SslAuthenticationFailed    // the TLS handshake failed
  case InvalidProducerEpoch       // the broker refuses the producer's epoch as out of date
  case Fenced                     // a newer client with the same identity, such as the same transactional id, has taken over
  case Purged                     // the client discarded a record before sending it, such as one waiting when its transaction aborted
  case Other(value: Int)          // a code with no portable meaning

object ErrorCode:
  /** The conditions worth trying again, which are the ones a broker reports while it is moving rather than refusing.
    *
    * This is the single answer every backend reports through `KafkaException.BackendFailure.retriable`, so the same condition is retriable on all of
    * them. The backends' own notions disagree, and one of them does not always have one.
    */
  extension (code: ErrorCode)
    def retriable: Boolean =
      code match
        case NetworkException | RequestTimedOut | LeaderNotAvailable | NotLeaderOrFollower | BrokerNotAvailable | CoordinatorNotAvailable |
            NotCoordinator | CoordinatorLoadInProgress | RebalanceInProgress => true
        case _ => false

  private val protocol: Map[Int, ErrorCode] =
    Map(
      1  -> OffsetOutOfRange,
      3  -> UnknownTopicOrPartition,
      5  -> LeaderNotAvailable,
      6  -> NotLeaderOrFollower,
      7  -> RequestTimedOut,
      8  -> BrokerNotAvailable,
      10 -> MessageTooLarge,
      13 -> NetworkException,
      14 -> CoordinatorLoadInProgress,
      15 -> CoordinatorNotAvailable,
      16 -> NotCoordinator,
      17 -> InvalidTopic,
      22 -> IllegalGeneration,
      24 -> InvalidGroupId,
      25 -> UnknownMemberId,
      27 -> RebalanceInProgress,
      29 -> TopicAuthorizationFailed,
      30 -> GroupAuthorizationFailed,
      31 -> ClusterAuthorizationFailed,
      35 -> UnsupportedVersion,
      47 -> InvalidProducerEpoch,
      58 -> SaslAuthenticationFailed,
      82 -> Fenced, // FENCED_INSTANCE_ID
      90 -> Fenced  // PRODUCER_FENCED
    )

  /** Classifies a Kafka protocol error code, which the JVM and librdkafka both report from the same table. */
  def fromProtocol(value: Int): ErrorCode = protocol.getOrElse(value, Other(value))

  /** Classifies a librdkafka code. Its own client-side errors are negative; the rest are protocol codes. */
  def fromLibrdkafka(value: Int): ErrorCode =
    value match
      case -195 | -187 | -193 => NetworkException         // _TRANSPORT, _ALL_BROKERS_DOWN, _RESOLVE
      case -185 | -192        => RequestTimedOut          // _TIMED_OUT, _MSG_TIMED_OUT
      case -169               => SaslAuthenticationFailed // _AUTHENTICATION
      case -181               => SslAuthenticationFailed  // _SSL
      case -144               => Fenced                   // _FENCED
      case -152 | -151        => Purged                   // _PURGE_QUEUE, _PURGE_INFLIGHT
      // librdkafka normalizes _UNKNOWN_TOPIC to the protocol code before a consumer sees it, and these carry the same
      // meaning wherever it does not.
      case -188 | -190 => UnknownTopicOrPartition // _UNKNOWN_TOPIC, _UNKNOWN_PARTITION
      case other       => fromProtocol(other)

sealed abstract class KafkaException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object KafkaException:
  /** A failure reported by a Kafka backend.
    *
    * `None` means that the backend did not make the corresponding classification available.
    */
  final class BackendFailure(
      val detail: String,
      val code: Option[ErrorCode] = None,
      val retriable: Option[Boolean] = None,
      val fatal: Option[Boolean] = None,
      val transactionAbortRequired: Option[Boolean] = None,
      cause: Throwable = null
  ) extends KafkaException(BackendFailure.message(detail, code), cause)

  object BackendFailure:
    private def message(detail: String, code: Option[ErrorCode]): String = s"Kafka backend failure${code.fold("")(value => s" [$value]")}: $detail"

  /** A value that does not meet the rules of the type it was to become. */
  final class InvalidValue(val error: ValidationError) extends KafkaException(error.message)

  /** Settings that could not be constructed, carrying every reason they could not. */
  final class InvalidSettings(val errors: NonEmptyList[SettingsError]) extends KafkaException(errors.toList.map(_.message).mkString("; "))

  /** Offsets that could not be committed, after every attempt the recovery policy allowed. */
  final class CommitFailed(val attempts: Int, val offsets: Map[TopicPartition, Offset], cause: Throwable)
      extends KafkaException(s"committing ${offsets.size} offsets failed after $attempts attempts", cause)

  /** An operation the portable API cannot carry out with what it was given. */
  final class Unsupported(val detail: String) extends KafkaException(detail)

  /** A backend response that cannot be represented by the portable API. */
  final class InvalidBackendResponse(val detail: String, cause: Throwable = null)
      extends KafkaException(s"Kafka backend returned an invalid response: $detail", cause)

/** Carries the reason a value was rejected into `F`, so a validated value composes with the effects around it. */
extension [A](result: Either[ValidationError, A])
  def liftTo[F[_]](using F: ApplicativeThrow[F]): F[A] = F.fromEither(result.leftMap(KafkaException.InvalidValue(_)))

/** Carries every reason settings were rejected into `F`. */
extension [A](result: ValidatedNel[SettingsError, A])
  def liftTo[F[_]](using F: ApplicativeThrow[F]): F[A] = F.fromEither(result.toEither.leftMap(KafkaException.InvalidSettings(_)))
