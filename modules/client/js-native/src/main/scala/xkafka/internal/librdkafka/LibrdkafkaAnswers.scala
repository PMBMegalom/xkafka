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
package librdkafka

import cats.syntax.all.*

/** What deleting records answered. */
private[xkafka] enum DeletedRecords:
  /** One failure for the whole call, with librdkafka's code for it. */
  case Refused(failure: KafkaException.BackendFailure, code: Int)

  /** A code and low watermark for each requested partition, in request order; `Failures.NoAnswer` where the result left one out. */
  case Answered(results: Vector[(Int, Long)])

/** One described topic: its error code and text, and its entries when the code is 0. */
private[xkafka] final case class ConfigResource(name: String, error: Int, errorString: String, entries: Vector[ConfigEntry])

private[xkafka] final case class ConfigEntry(name: String, value: Option[String], source: Int, sensitive: Boolean, readOnly: Boolean)

/** How the shim's failures become exceptions, the same on every platform. */
private[xkafka] object Failures:
  /** What the shim leaves in place of a code for a partition its result does not mention. */
  val NoAnswer: Int = Int.MinValue

  /** A failure the shim reported with a librdkafka code. */
  def coded(message: String, code: Int): KafkaException.BackendFailure =
    val classified = ErrorCode.fromLibrdkafka(code)
    new KafkaException.BackendFailure(message, Some(classified), retriable = Some(classified.retriable))

  /** A transactional failure, whose flags are -1 when librdkafka did not say. */
  def classified(message: String, code: Int, fatal: Int, retriable: Int, transactionAbortRequired: Int): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(message, Some(ErrorCode.fromLibrdkafka(code)), flag(retriable), flag(fatal), flag(transactionAbortRequired))

  def plain(detail: String): KafkaException.BackendFailure = new KafkaException.BackendFailure(detail)

  private def flag(value: Int): Option[Boolean] = Option.when(value >= 0)(value != 0)

/** The portable answers for what the shim reports about deleting records and describing configurations. */
private[librdkafka] object LibrdkafkaAnswers:
  // _NOENT, which librdkafka reports when deleting records finds no leader for any requested partition.
  private val NoLeadersFound = -156

  /** librdkafka fails the whole call where no requested partition exists, and answers each one where only some do. */
  def deleted(cuts: Vector[(TopicPartition, Offset)], answer: DeletedRecords, errorString: Int => String): Vector[Either[KafkaException, Offset]] =
    answer match
      case DeletedRecords.Refused(failure, NoLeadersFound) =>
        val unknown =
          new KafkaException.BackendFailure(
            failure.getMessage,
            Some(ErrorCode.UnknownTopicOrPartition),
            Some(ErrorCode.UnknownTopicOrPartition.retriable)
          )
        cuts.map(_ => Left(unknown))
      case DeletedRecords.Refused(failure, _) => throw failure
      case DeletedRecords.Answered(results)   => cuts.zip(results).map:
          case ((topicPartition, _), (code, low)) => code match
              case Failures.NoAnswer => throw new KafkaException.InvalidBackendResponse(s"no answer for partition ${topicPartition.show}")
              case 0                 => Right(Offset.from(low).fold(error => throw invalid("low watermark", low, error), identity))
              case code              =>
                val classified = ErrorCode.fromLibrdkafka(code)
                Left(new KafkaException.BackendFailure(errorString(code), Some(classified), Some(classified.retriable)))

  /** Each requested topic's own answer, which librdkafka reports by name. */
  def configurations(requested: Vector[Topic], resources: Vector[ConfigResource]): Vector[Either[KafkaException, TopicConfiguration]] =
    val answers = resources.map(resource => resource.name -> resource).toMap
    requested.map: topic =>
      answers.get(topic.value) match
        case None           => throw new KafkaException.InvalidBackendResponse(s"no answer for topic ${topic.value}")
        case Some(resource) => configuration(resource)

  private def configuration(resource: ConfigResource): Either[KafkaException, TopicConfiguration] =
    resource.error match
      case 0 =>
        val entries =
          resource.entries.map: entry =>
            entry.name -> ConfigurationEntry.reported(entry.value, entry.sensitive, ConfigurationSource.fromLibrdkafka(entry.source), entry.readOnly)
        Right(TopicConfiguration(entries.toMap))
      case code =>
        val classified = ErrorCode.fromLibrdkafka(code)
        Left(new KafkaException.BackendFailure(resource.errorString, Some(classified), Some(classified.retriable)))

  private def invalid(field: String, value: Any, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error")
