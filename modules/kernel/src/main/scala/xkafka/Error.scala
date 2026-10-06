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
  /** The requested offset is outside the partition's log. */
  case OffsetOutOfRange

  /** The topic or partition does not exist. */
  case UnknownTopicOrPartition

  /** The partition has no leader right now, such as during an election. */
  case LeaderNotAvailable

  /** The broker no longer leads or follows the partition. */
  case NotLeaderOrFollower

  /** A request got no answer in time. */
  case RequestTimedOut

  /** The broker cannot be reached. */
  case BrokerNotAvailable

  /** A record is larger than the broker or topic accepts. */
  case MessageTooLarge

  /** The connection to the broker failed. */
  case NetworkException

  /** The group or transaction coordinator is still loading its state. */
  case CoordinatorLoadInProgress

  /** The group or transaction coordinator is not available. */
  case CoordinatorNotAvailable

  /** The broker is not the coordinator for this group or transaction. */
  case NotCoordinator

  /** The group has moved on to a newer generation. */
  case IllegalGeneration

  /** The group does not know this member. */
  case UnknownMemberId

  /** The group is rebalancing. */
  case RebalanceInProgress

  /** The group id is not valid. */
  case InvalidGroupId

  /** The topic name is not valid. */
  case InvalidTopic

  /** The client is not authorized for the topic. */
  case TopicAuthorizationFailed

  /** The client is not authorized for the group. */
  case GroupAuthorizationFailed

  /** The client is not authorized for the cluster operation. */
  case ClusterAuthorizationFailed

  /** The broker does not support the request. */
  case UnsupportedVersion

  /** SASL authentication failed. */
  case SaslAuthenticationFailed

  /** The TLS handshake failed. */
  case SslAuthenticationFailed

  /** The broker refuses the producer's epoch as out of date. */
  case InvalidProducerEpoch

  /** A newer client with the same identity, such as the same transactional id, has taken over. */
  case Fenced

  /** The client discarded a record before sending it, such as one waiting when its transaction aborted. */
  case Purged

  /** A consumer has no committed offset for a partition, or one outside its log, and its `AutoOffsetReset` is `Fail`. */
  case OffsetResetRequired

  /** A code with no portable meaning. */
  case Other(value: Int)

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
      1  -> OffsetOutOfRange,           // OFFSET_OUT_OF_RANGE
      3  -> UnknownTopicOrPartition,    // UNKNOWN_TOPIC_OR_PARTITION
      5  -> LeaderNotAvailable,         // LEADER_NOT_AVAILABLE
      6  -> NotLeaderOrFollower,        // NOT_LEADER_OR_FOLLOWER
      7  -> RequestTimedOut,            // REQUEST_TIMED_OUT
      8  -> BrokerNotAvailable,         // BROKER_NOT_AVAILABLE
      10 -> MessageTooLarge,            // MESSAGE_TOO_LARGE
      13 -> NetworkException,           // NETWORK_EXCEPTION
      14 -> CoordinatorLoadInProgress,  // COORDINATOR_LOAD_IN_PROGRESS
      15 -> CoordinatorNotAvailable,    // COORDINATOR_NOT_AVAILABLE
      16 -> NotCoordinator,             // NOT_COORDINATOR
      17 -> InvalidTopic,               // INVALID_TOPIC_EXCEPTION
      22 -> IllegalGeneration,          // ILLEGAL_GENERATION
      24 -> InvalidGroupId,             // INVALID_GROUP_ID
      25 -> UnknownMemberId,            // UNKNOWN_MEMBER_ID
      27 -> RebalanceInProgress,        // REBALANCE_IN_PROGRESS
      29 -> TopicAuthorizationFailed,   // TOPIC_AUTHORIZATION_FAILED
      30 -> GroupAuthorizationFailed,   // GROUP_AUTHORIZATION_FAILED
      31 -> ClusterAuthorizationFailed, // CLUSTER_AUTHORIZATION_FAILED
      35 -> UnsupportedVersion,         // UNSUPPORTED_VERSION
      47 -> InvalidProducerEpoch,       // INVALID_PRODUCER_EPOCH
      58 -> SaslAuthenticationFailed,   // SASL_AUTHENTICATION_FAILED
      82 -> Fenced,                     // FENCED_INSTANCE_ID
      90 -> Fenced                      // PRODUCER_FENCED
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
      case -140               => OffsetResetRequired      // _AUTO_OFFSET_RESET
      case -188 | -190        => UnknownTopicOrPartition  // _UNKNOWN_TOPIC, _UNKNOWN_PARTITION
      case other              => fromProtocol(other)

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
