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

/** A portable classification of a backend failure.
  *
  * The named cases are Kafka protocol errors, which every broker reports with the same code, plus the client-side conditions all three backends can
  * raise on their own. `Other` carries a code that has no portable meaning.
  */
enum ErrorCode derives CanEqual:
  case OffsetOutOfRange
  case UnknownTopicOrPartition
  case LeaderNotAvailable
  case NotLeaderOrFollower
  case RequestTimedOut
  case BrokerNotAvailable
  case MessageTooLarge
  case NetworkException
  case CoordinatorLoadInProgress
  case CoordinatorNotAvailable
  case NotCoordinator
  case IllegalGeneration
  case UnknownMemberId
  case RebalanceInProgress
  case InvalidGroupId
  case InvalidTopic
  case TopicAuthorizationFailed
  case GroupAuthorizationFailed
  case ClusterAuthorizationFailed
  case UnsupportedVersion
  case SaslAuthenticationFailed
  case SslAuthenticationFailed
  case Other(value: Int)

object ErrorCode:
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
      58 -> SaslAuthenticationFailed
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
      cause: Throwable = null
  ) extends KafkaException(BackendFailure.message(detail, code), cause)

  object BackendFailure:
    private def message(detail: String, code: Option[ErrorCode]): String = s"Kafka backend failure${code.fold("")(value => s" [$value]")}: $detail"

  /** A backend response that cannot be represented by the portable API. */
  final class InvalidBackendResponse(val detail: String, cause: Throwable = null)
      extends KafkaException(s"Kafka backend returned an invalid response: $detail", cause)
