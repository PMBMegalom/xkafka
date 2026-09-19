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

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.{Async, Resource}
import cats.effect.implicits.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.kafka.{
  AutoOffsetReset as Fs2AutoOffsetReset, CommittableConsumerRecord as Fs2CommittableConsumerRecord, ConsumerSettings as Fs2ConsumerSettings,
  Deserializer as Fs2Deserializer, Header as Fs2Header, Headers as Fs2Headers, KafkaConsumer as Fs2KafkaConsumer, KafkaProducer as Fs2KafkaProducer,
  ProducerRecord as Fs2ProducerRecord, ProducerSettings as Fs2ProducerSettings, Serializer as Fs2Serializer
}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.producer.MkProducer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata as JavaRecordMetadata
import org.apache.kafka.common.TopicPartition as JavaTopicPartition

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new Fs2KafkaClient[F]

  private[xkafka] def fromFs2[F[_]](using Async[F], Parallel[F], MkProducer[F], MkConsumer[F]): KafkaClient[F] = new Fs2KafkaClient[F]

private final class Fs2KafkaClient[F[_]](using F: Async[F], P: Parallel[F], mkProducer: MkProducer[F], mkConsumer: MkConsumer[F])
    extends KafkaClient[F]:
  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    Fs2KafkaProducer.resource(producerSettings(settings)).map(new Fs2KafkaProducerAdapter(_))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      consumer <- Fs2KafkaConsumer.resource(consumerSettings(settings))
      _        <- Resource.eval(subscribe(consumer, subscription))
    yield new Fs2KafkaConsumerAdapter(consumer)

  private def subscribe[K, V](consumer: Fs2KafkaConsumer[F, K, V], subscription: Subscription): F[Unit] =
    subscription match
      case Subscription.Topics(topics)   => consumer.subscribe(topics.map(_.value))
      case Subscription.Pattern(pattern) => F.delay(pattern.anchored.r).flatMap(consumer.subscribe)

  private def producerSettings[K, V](settings: ProducerSettings[F, K, V]): Fs2ProducerSettings[F, K, V] =
    val base =
      Fs2ProducerSettings(serializer(settings.keySerializer), serializer(settings.valueSerializer))
        .withProperties((settings.client.properties ++ settings.properties).removedAll(ManagedProperties))
        .withBootstrapServers(settings.client.bootstrapServers.toList.mkString(","))

    settings.client.clientId.fold(base)(base.withClientId)

  private def consumerSettings[K, V](settings: ConsumerSettings[F, K, V]): Fs2ConsumerSettings[F, K, V] =
    val base =
      Fs2ConsumerSettings(deserializer(settings.keyDeserializer), deserializer(settings.valueDeserializer))
        .withProperties((settings.client.properties ++ settings.properties).removedAll(ManagedProperties))
        .withBootstrapServers(settings.client.bootstrapServers.toList.mkString(",")).withGroupId(settings.groupId.value)
        .withProperty("enable.auto.commit", "false").withAutoOffsetReset(
          settings.autoOffsetReset match
            case AutoOffsetReset.Earliest => Fs2AutoOffsetReset.Earliest
            case AutoOffsetReset.Latest   => Fs2AutoOffsetReset.Latest
        )

    settings.client.clientId.fold(base)(base.withClientId)

  private def serializer[A](value: Serializer[F, A]): Fs2Serializer[F, A] =
    Fs2Serializer.instance: (topic, headers, input) =>
      for
        portableTopic <- F.fromEither(validTopic(topic))
        bytes         <- value.serialize(portableTopic, portableHeaders(headers), input)
      yield bytes.fold[Array[Byte]](null)(_.toArray)

  private def deserializer[A](value: Deserializer[F, A]): Fs2Deserializer[F, A] =
    Fs2Deserializer.instance: (topic, headers, bytes) =>
      F.fromEither(validTopic(topic)).flatMap: portableTopic =>
        value.deserialize(portableTopic, portableHeaders(headers), Option(bytes).map(Chunk.array))

  private def validTopic(value: String): Either[Throwable, Topic] = Topic.from(value).leftMap(error => invalidBackendValue("topic", error))

  private def portableHeaders(headers: Fs2Headers): Headers =
    Headers.fromVector(headers.toChain.iterator.map: header =>
      Header(header.key, Option(header.value).map(Chunk.array))
    .toVector)

  private def invalidBackendValue(field: String, error: ValidationError): IllegalStateException =
    new IllegalStateException(s"fs2-kafka returned an invalid $field: $error")

  private final class Fs2KafkaProducerAdapter[K, V](underlying: Fs2KafkaProducer[F, K, V]) extends KafkaProducer[F, K, V]:

    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]] =
      underlying.produce(Chunk.from(records.toList.map(producerRecord))).flatten.flatMap: result =>
        result.toList.traverse:
          case (_, metadata) => recordMetadata(metadata)
        .map(metadata => ProducerResult(records, metadata))

    private def producerRecord(record: ProducerRecord[K, V]): Fs2ProducerRecord[K, V] =
      val base        = Fs2ProducerRecord(record.topic.value, record.key, record.value).withHeaders(fs2Headers(record.headers))
      val partitioned = record.partition.fold(base)(partition => base.withPartition(partition.value))

      record.timestamp.fold(partitioned)(timestamp => partitioned.withTimestamp(timestamp.epochMillis))

    private def fs2Headers(headers: Headers): Fs2Headers =
      Fs2Headers.fromSeq(
        headers.values.map: header =>
          val bytes = header.value.fold[Array[Byte]](null)(_.toArray)
          Fs2Header(header.key, bytes)
      )

    private def recordMetadata(metadata: JavaRecordMetadata): F[RecordMetadata] =
      val validated =
        for
          topic     <- Topic.from(metadata.topic).leftMap(error => invalidBackendValue("topic", error))
          partition <- Partition.from(metadata.partition).leftMap(error => invalidBackendValue("partition", error))
          offset    <-
            if metadata.hasOffset then Offset.from(metadata.offset).map(Some(_)).leftMap(error => invalidBackendValue("offset", error))
            else Right(None)
        yield RecordMetadata(
          topicPartition = TopicPartition(topic, partition),
          offset = offset,
          timestamp = Option.when(metadata.hasTimestamp)(Timestamp.fromEpochMillis(metadata.timestamp))
        )

      F.fromEither(validated)

  private final class Fs2KafkaConsumerAdapter[K, V](underlying: Fs2KafkaConsumer[F, K, V]) extends KafkaConsumer[F, K, V]:

    private val offsetCommitter: OffsetCommitter[F] =
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
          underlying.commitSync(
            offsets.map:
              case (topicPartition, offset) => new JavaTopicPartition(topicPartition.topic.value, topicPartition.partition.value) ->
                  new OffsetAndMetadata(offset.value)
          )

    override val records: fs2.Stream[F, CommittableConsumerRecord[F, K, V]] = underlying.records.evalMap(consumerRecord)

    override def assignment: F[Set[TopicPartition]] = underlying.assignment.flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        underlying.committed(topicPartitions.map(javaTopicPartition)).flatMap: values =>
          topicPartitions.toList.traverse: topicPartition =>
            Option(values.getOrElse(javaTopicPartition(topicPartition), null)).traverse: metadata =>
              F.fromEither(Offset.from(metadata.offset).leftMap(error => invalidBackendValue("committed offset", error)))
            .map(topicPartition -> _)
          .map(_.toMap)

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] = underlying.seek(javaTopicPartition(topicPartition), offset.value)

    private def consumerRecord(committable: Fs2CommittableConsumerRecord[F, K, V]): F[CommittableConsumerRecord[F, K, V]] =
      val source    = committable.record
      val validated =
        for
          topic              <- Topic.from(source.topic).leftMap(error => invalidBackendValue("topic", error))
          partition          <- Partition.from(source.partition).leftMap(error => invalidBackendValue("partition", error))
          offset             <- Offset.from(source.offset).leftMap(error => invalidBackendValue("offset", error))
          portableNextOffset <- Offset.from(committable.offset.offsetAndMetadata.offset).leftMap(error => invalidBackendValue("next offset", error))
        yield
          val portableTopicPartition = TopicPartition(topic, partition)
          val portableRecord         =
            ConsumerRecord(
              topicPartition = portableTopicPartition,
              offset = offset,
              timestamp = source.timestamp.toOption.map(Timestamp.fromEpochMillis),
              key = source.key,
              value = source.value,
              headers = portableHeaders(source.headers)
            )
          val portableOffset =
            new CommittableOffset[F]:
              override val topicPartition: TopicPartition = portableTopicPartition

              override val nextOffset: Offset = portableNextOffset

              override val committer: OffsetCommitter[F] = offsetCommitter

          CommittableConsumerRecord(portableRecord, portableOffset)

      F.fromEither(validated)

    private def javaTopicPartition(topicPartition: TopicPartition): JavaTopicPartition =
      new JavaTopicPartition(topicPartition.topic.value, topicPartition.partition.value)

    private def portableTopicPartition(source: JavaTopicPartition): F[TopicPartition] =
      F.fromEither:
        for
          topic     <- Topic.from(source.topic).leftMap(error => invalidBackendValue("assigned topic", error))
          partition <- Partition.from(source.partition).leftMap(error => invalidBackendValue("assigned partition", error))
        yield TopicPartition(topic, partition)
