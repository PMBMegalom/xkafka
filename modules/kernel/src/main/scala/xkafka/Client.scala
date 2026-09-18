package xkafka

import cats.data.NonEmptyList
import fs2.Stream

final case class ClientSettings(
    bootstrapServers: NonEmptyList[String],
    clientId: Option[String] = None
)

final case class ProducerSettings[F[_], K, V](
    client: ClientSettings,
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
)

enum AutoOffsetReset:
  case Earliest, Latest

final case class ConsumerSettings[F[_], K, V](
    client: ClientSettings,
    groupId: ConsumerGroup,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V],
    autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Latest
)

enum Subscription:
  case Topics(topics: NonEmptyList[Topic])

trait CommittableOffset[F[_]]:
  def topicPartition: TopicPartition

  /** The next offset to consume after this commit succeeds. */
  def nextOffset: Offset

  def commit: F[Unit]

final case class CommittableConsumerRecord[F[_], K, V](
    record: ConsumerRecord[K, V],
    offset: CommittableOffset[F]
)

trait KafkaProducer[F[_], K, V]:
  def produce(
      records: NonEmptyList[ProducerRecord[K, V]]
  ): F[ProducerResult[K, V]]

trait KafkaConsumer[F[_], K, V]:
  def records: Stream[F, CommittableConsumerRecord[F, K, V]]
