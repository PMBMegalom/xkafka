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

import scala.concurrent.duration.FiniteDuration

import cats.{Order, Show}
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
  case NegativeLeaderEpoch(value: Int)
  case EmptyConsumerGroup
  case EmptyTransactionalId
  case MissingHeaderValue
  case InvalidHeaderLength(expected: Int, actual: Int)
  case NegativeAttemptCount(value: Int)
  case NonPositiveDelay(value: FiniteDuration)
  case JitterOutOfRange(value: Double)
  case NonPositivePartitionCount(value: Int)
  case NonPositiveReplicationFactor(value: Short)
  case NegativeBrokerId(value: Int)

  def message: String =
    this match
      case EmptyTopic                            => "topic must not be empty"
      case TopicTooLong(length)                  => s"topic must be at most ${Topic.MaxLength} characters, was $length"
      case InvalidTopicCharacters(value)         => s"topic '$value' must contain only [a-zA-Z0-9._-]"
      case ReservedTopicName(value)              => s"topic must not be '$value'"
      case EmptyTopicPattern                     => "topic pattern must not be empty"
      case NegativePartition(value)              => s"partition must not be negative, was $value"
      case NegativeOffset(value)                 => s"offset must not be negative, was $value"
      case OffsetOverflow                        => "offset cannot be advanced past Long.MaxValue"
      case NegativeLeaderEpoch(value)            => s"leader epoch must not be negative, was $value"
      case EmptyConsumerGroup                    => "consumer group must not be empty"
      case EmptyTransactionalId                  => "transactional id must not be empty"
      case MissingHeaderValue                    => "header has no value"
      case InvalidHeaderLength(expected, actual) => s"header value must be $expected bytes, was $actual"
      case NegativeAttemptCount(value)           => s"attempt count must not be negative, was $value"
      case NonPositiveDelay(value)               => s"delay must be positive, was $value"
      case JitterOutOfRange(value)               => s"jitter must be between 0 and 1, was $value"
      case NonPositivePartitionCount(value)      => s"partition count must be positive, was $value"
      case NonPositiveReplicationFactor(value)   => s"replication factor must be positive, was $value"
      case NegativeBrokerId(value)               => s"broker id must not be negative, was $value"

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
  * The JVM matches it as a Java regular expression, and Scala.js and Native through librdkafka, so a pattern meant for every platform should keep to
  * simple syntax, such as `events-.*`.
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

/** A broker's id within its cluster. */
opaque type BrokerId = Int

object BrokerId:
  def from(value: Int): Either[ValidationError, BrokerId] = Either.cond(value >= 0, value, ValidationError.NegativeBrokerId(value))

  extension (id: BrokerId) def value: Int = id

  given Order[BrokerId] = Order.from((x, y) => Integer.compare(x.value, y.value))
  given Show[BrokerId]  = Show.show(_.value.toString)

/** The id the cluster gives a topic when it creates it, in the base64 form Kafka prints it in. A topic deleted and created again under the same name
  * gets a new one.
  */
opaque type TopicId = String

object TopicId:
  private[xkafka] def apply(value: String): TopicId = value

  extension (id: TopicId) def value: String = id

  given Order[TopicId] = Order.from((x, y) => x.value.compareTo(y.value))
  given Show[TopicId]  = Show.show(_.value)

opaque type Offset = Long

object Offset:
  def from(value: Long): Either[ValidationError, Offset] = Either.cond(value >= 0L, value, ValidationError.NegativeOffset(value))

  extension (offset: Offset)
    def value: Long = offset

    def next: Either[ValidationError, Offset] = Either.cond(offset < Long.MaxValue, offset + 1L, ValidationError.OffsetOverflow)

  given Order[Offset] = Order.from((x, y) => java.lang.Long.compare(x.value, y.value))
  given Show[Offset]  = Show.show(_.value.toString)

/** The generation of a partition's leadership, which the broker raises each time a new leader takes the partition over. */
opaque type LeaderEpoch = Int

object LeaderEpoch:
  def from(value: Int): Either[ValidationError, LeaderEpoch] = Either.cond(value >= 0, value, ValidationError.NegativeLeaderEpoch(value))

  extension (epoch: LeaderEpoch) def value: Int = epoch

  given Order[LeaderEpoch] = Order.from((x, y) => Integer.compare(x.value, y.value))
  given Show[LeaderEpoch]  = Show.show(_.value.toString)

opaque type ConsumerGroup = String

object ConsumerGroup:
  /** Rejects a blank name. */
  def from(value: String): Either[ValidationError, ConsumerGroup] = Either.cond(value.trim.nonEmpty, value, ValidationError.EmptyConsumerGroup)

  extension (group: ConsumerGroup) def value: String = group

  given Order[ConsumerGroup] = Order.from((x, y) => x.value.compareTo(y.value))
  given Show[ConsumerGroup]  = Show.show(_.value)

/** Names a transactional producer, so Kafka can fence an earlier instance of it when a new one starts. */
opaque type TransactionalId = String

object TransactionalId:
  /** Rejects blank ids. Kafka fences a producer by this name, so a blank one leaves nothing to fence. */
  def from(value: String): Either[ValidationError, TransactionalId] = Either.cond(value.trim.nonEmpty, value, ValidationError.EmptyTransactionalId)

  extension (id: TransactionalId) def value: String = id

  given Order[TransactionalId] = Order.from((x, y) => x.value.compareTo(y.value))
  given Show[TransactionalId]  = Show.show(_.value)

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

final case class Header(key: String, value: Option[Chunk[Byte]]):
  /** Reads this header's value as `A`, failing where the bytes do not carry one. */
  def as[A](using deserializer: HeaderDeserializer[A]): Either[ValidationError, A] = deserializer.deserialize(value)

opaque type Headers = Vector[Header]

object Headers:
  val empty: Headers = Vector.empty

  def apply(headers: Header*): Headers = headers.toVector

  def fromVector(headers: Vector[Header]): Headers = headers

  extension (headers: Headers)
    def values: Vector[Header] = headers

    def append(header: Header): Headers = headers :+ header

    /** Appends a header whose value is encoded by `A`'s serializer. */
    def append[A](key: String, value: A)(using serializer: HeaderSerializer[A]): Headers = headers :+ Header(key, Some(serializer.serialize(value)))

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

/** A record that was delivered, with the metadata its backend reported for it. */
opaque type Produced[K, V] = (ProducerRecord[K, V], RecordMetadata)

object Produced:
  def apply[K, V](record: ProducerRecord[K, V], metadata: RecordMetadata): Produced[K, V] = (record, metadata)

  def unapply[K, V](produced: Produced[K, V]): (ProducerRecord[K, V], RecordMetadata) = produced

  extension [K, V](produced: Produced[K, V])
    def record: ProducerRecord[K, V] = produced._1

    def metadata: RecordMetadata = produced._2

/** A record that was attempted, with the metadata its backend reported for it where it was delivered, or why it was not. */
opaque type Attempted[K, V] = (ProducerRecord[K, V], Either[KafkaException, RecordMetadata])

object Attempted:
  def apply[K, V](record: ProducerRecord[K, V], outcome: Either[KafkaException, RecordMetadata]): Attempted[K, V] = (record, outcome)

  def unapply[K, V](attempted: Attempted[K, V]): (ProducerRecord[K, V], Either[KafkaException, RecordMetadata]) = attempted

  extension [K, V](attempted: Attempted[K, V])
    def record: ProducerRecord[K, V] = attempted._1

    def outcome: Either[KafkaException, RecordMetadata] = attempted._2

    /** The record as delivered, or why it was not. */
    def produced: Either[KafkaException, Produced[K, V]] = attempted._2.map(Produced(attempted._1, _))

/** A consumed record's timestamp, and which of the two kinds it is. */
enum RecordTimestamp:
  /** Set by the producer, or by its client when the producer gave none. */
  case CreateTime(value: Timestamp)

  /** Set by the broker as it appended the record, for a topic whose `message.timestamp.type` is `LogAppendTime`. */
  case LogAppendTime(value: Timestamp)

  def value: Timestamp

object RecordTimestamp:
  given Show[RecordTimestamp] = Show.fromToString

/** A consumed record.
  *
  * @param leaderEpoch
  *   the leader epoch of the partition when the record was written, where the backend reported one
  * @param serializedKeySize
  *   the size in bytes of the key as it was read, or `None` for a record without a key
  * @param serializedValueSize
  *   the size in bytes of the value as it was read, or `None` for a record without a value
  */
final case class ConsumerRecord[K, V](
    topicPartition: TopicPartition,
    offset: Offset,
    timestamp: Option[RecordTimestamp],
    key: K,
    value: V,
    headers: Headers,
    leaderEpoch: Option[LeaderEpoch] = None,
    serializedKeySize: Option[Int] = None,
    serializedValueSize: Option[Int] = None
)
