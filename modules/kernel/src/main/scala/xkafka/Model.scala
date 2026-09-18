package xkafka

import cats.data.NonEmptyList
import fs2.Chunk

enum ValidationError derives CanEqual:
  case EmptyTopic
  case NegativePartition(value: Int)
  case NegativeOffset(value: Long)
  case OffsetOverflow
  case EmptyConsumerGroup

opaque type Topic = String

object Topic:
  def from(value: String): Either[ValidationError, Topic] =
    Either.cond(value.nonEmpty, value, ValidationError.EmptyTopic)

  extension (topic: Topic) def value: String = topic

opaque type Partition = Int

object Partition:
  def from(value: Int): Either[ValidationError, Partition] =
    Either.cond(value >= 0, value, ValidationError.NegativePartition(value))

  extension (partition: Partition) def value: Int = partition

opaque type Offset = Long

object Offset:
  def from(value: Long): Either[ValidationError, Offset] =
    Either.cond(value >= 0L, value, ValidationError.NegativeOffset(value))

  extension (offset: Offset)
    def value: Long = offset

    def next: Either[ValidationError, Offset] =
      Either.cond(
        offset < Long.MaxValue,
        offset + 1L,
        ValidationError.OffsetOverflow
      )

opaque type ConsumerGroup = String

object ConsumerGroup:
  def from(value: String): Either[ValidationError, ConsumerGroup] =
    Either.cond(value.nonEmpty, value, ValidationError.EmptyConsumerGroup)

  extension (group: ConsumerGroup) def value: String = group

opaque type Timestamp = Long

object Timestamp:
  def fromEpochMillis(value: Long): Timestamp = value

  extension (timestamp: Timestamp) def epochMillis: Long = timestamp

final case class TopicPartition(topic: Topic, partition: Partition)

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
      headers.collect { case Header(`key`, value) => value }

final case class ProducerRecord[K, V](
    topic: Topic,
    key: K,
    value: V,
    partition: Option[Partition] = None,
    timestamp: Option[Timestamp] = None,
    headers: Headers = Headers.empty
)

final case class RecordMetadata(
    topicPartition: TopicPartition,
    offset: Option[Offset],
    timestamp: Option[Timestamp]
)

/** The acknowledged result of producing a non-empty collection of records.
  *
  * Metadata cardinality is backend-defined. The JVM and Native drivers can report metadata per record, while the Confluent JavaScript driver reports
  * it per topic-partition batch.
  */
final case class ProducerResult[K, V](
    records: NonEmptyList[ProducerRecord[K, V]],
    metadata: List[RecordMetadata]
)

final case class ConsumerRecord[K, V](
    topicPartition: TopicPartition,
    offset: Offset,
    timestamp: Option[Timestamp],
    key: K,
    value: V,
    headers: Headers
)
