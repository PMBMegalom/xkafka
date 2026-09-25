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

import cats.{Order, Show}
import cats.data.NonEmptyList
import fs2.Chunk

enum ValidationError derives CanEqual:
  case EmptyTopic
  case TopicTooLong(length: Int)
  case InvalidTopicCharacters(value: String)
  case ReservedTopicName(value: String)
  case EmptyTopicPattern
  case NegativePartition(value: Int)
  case NegativeOffset(value: Long)
  case OffsetOverflow
  case EmptyConsumerGroup
  case NonPositivePartitionCount(value: Int)
  case NonPositiveReplicationFactor(value: Short)

  def message: String =
    this match
      case EmptyTopic                          => "topic must not be empty"
      case TopicTooLong(length)                => s"topic must be at most ${Topic.MaxLength} characters, was $length"
      case InvalidTopicCharacters(value)       => s"topic '$value' must contain only [a-zA-Z0-9._-]"
      case ReservedTopicName(value)            => s"topic must not be '$value'"
      case EmptyTopicPattern                   => "topic pattern must not be empty"
      case NegativePartition(value)            => s"partition must not be negative, was $value"
      case NegativeOffset(value)               => s"offset must not be negative, was $value"
      case OffsetOverflow                      => "offset cannot be advanced past Long.MaxValue"
      case EmptyConsumerGroup                  => "consumer group must not be empty"
      case NonPositivePartitionCount(value)    => s"partition count must be positive, was $value"
      case NonPositiveReplicationFactor(value) => s"replication factor must be positive, was $value"

object ValidationError:
  given Show[ValidationError] = Show.show(_.message)

opaque type Topic = String

object Topic:
  /** The broker's own limit on topic name length. */
  val MaxLength = 249

  private val Reserved = Set(".", "..")

  private def legal(character: Char): Boolean =
    (character >= 'a' && character <= 'z') ||
      (character >= 'A' && character <= 'Z') ||
      (character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-'

  /** Applies the broker's topic naming rules, so an unusable name is caught at construction. */
  def from(value: String): Either[ValidationError, Topic] =
    if value.isEmpty then Left(ValidationError.EmptyTopic)
    else if Reserved.contains(value) then Left(ValidationError.ReservedTopicName(value))
    else if value.length > MaxLength then Left(ValidationError.TopicTooLong(value.length))
    else if !value.forall(legal) then Left(ValidationError.InvalidTopicCharacters(value))
    else Right(value)

  extension (topic: Topic) def value: String = topic

  given Order[Topic] = Order.from((x, y) => x.value.compareTo(y.value))
  given Show[Topic]  = Show.show(_.value)

/** A non-empty regular expression matched against complete topic names.
  *
  * Portable patterns should use syntax supported by Java, ECMAScript, and POSIX extended regular expressions.
  */
opaque type TopicPattern = String

object TopicPattern:
  def from(value: String): Either[ValidationError, TopicPattern] = Either.cond(value.nonEmpty, value, ValidationError.EmptyTopicPattern)

  extension (pattern: TopicPattern)
    def value: String = pattern

    private[xkafka] def anchored: String = s"^($pattern)$$"

  given Order[TopicPattern] = Order.from((x, y) => x.value.compareTo(y.value))
  given Show[TopicPattern]  = Show.show(_.value)

opaque type Partition = Int

object Partition:
  def from(value: Int): Either[ValidationError, Partition] = Either.cond(value >= 0, value, ValidationError.NegativePartition(value))

  extension (partition: Partition) def value: Int = partition

  given Order[Partition] = Order.from((x, y) => Integer.compare(x.value, y.value))
  given Show[Partition]  = Show.show(_.value.toString)

opaque type Offset = Long

object Offset:
  def from(value: Long): Either[ValidationError, Offset] = Either.cond(value >= 0L, value, ValidationError.NegativeOffset(value))

  extension (offset: Offset)
    def value: Long = offset

    def next: Either[ValidationError, Offset] = Either.cond(offset < Long.MaxValue, offset + 1L, ValidationError.OffsetOverflow)

  given Order[Offset] = Order.from((x, y) => java.lang.Long.compare(x.value, y.value))
  given Show[Offset]  = Show.show(_.value.toString)

opaque type ConsumerGroup = String

object ConsumerGroup:
  /** Rejects blank names. Brokers accept them, but they make group ownership impossible to attribute. */
  def from(value: String): Either[ValidationError, ConsumerGroup] = Either.cond(value.trim.nonEmpty, value, ValidationError.EmptyConsumerGroup)

  extension (group: ConsumerGroup) def value: String = group

  given Order[ConsumerGroup] = Order.from((x, y) => x.value.compareTo(y.value))
  given Show[ConsumerGroup]  = Show.show(_.value)

opaque type Timestamp = Long

object Timestamp:
  def fromEpochMillis(value: Long): Timestamp = value

  extension (timestamp: Timestamp) def epochMillis: Long = timestamp

  given Order[Timestamp] = Order.from((x, y) => java.lang.Long.compare(x.epochMillis, y.epochMillis))
  given Show[Timestamp]  = Show.show(_.epochMillis.toString)

final case class TopicPartition(topic: Topic, partition: Partition)

object TopicPartition:
  given Order[TopicPartition] = Order.whenEqual(Order.by(_.topic), Order.by(_.partition))
  given Show[TopicPartition]  = Show.show(value => s"${value.topic.value}-${value.partition.value}")

final case class Header(key: String, value: Option[Chunk[Byte]])

opaque type Headers = Vector[Header]

object Headers:
  val empty: Headers = Vector.empty

  def apply(headers: Header*): Headers = headers.toVector

  def fromVector(headers: Vector[Header]): Headers = headers

  extension (headers: Headers)
    def values: Vector[Header] = headers

    def append(header: Header): Headers = headers :+ header

    def getAll(key: String): Vector[Option[Chunk[Byte]]] =
      headers.collect:
        case Header(`key`, value) => value

final case class ProducerRecord[K, V](
    topic: Topic,
    key: K,
    value: V,
    partition: Option[Partition] = None,
    timestamp: Option[Timestamp] = None,
    headers: Headers = Headers.empty
)

final case class RecordMetadata(topicPartition: TopicPartition, offset: Option[Offset], timestamp: Option[Timestamp])

/** The acknowledged result of producing a non-empty collection of records, pairing each record with the metadata its backend reported for it.
  *
  * A `None` means the backend acknowledged the record but did not attribute metadata to it individually, so the absence is visible to the caller
  * instead of being inferred from a shorter list.
  */
final case class ProducerResult[K, V](records: NonEmptyList[(ProducerRecord[K, V], Option[RecordMetadata])]):
  /** Every record that the backend attributed metadata to, in the order the records were produced. */
  def metadata: List[RecordMetadata] = records.toList.flatMap((_, value) => value)

final case class ConsumerRecord[K, V](
    topicPartition: TopicPartition,
    offset: Offset,
    timestamp: Option[Timestamp],
    key: K,
    value: V,
    headers: Headers
)
