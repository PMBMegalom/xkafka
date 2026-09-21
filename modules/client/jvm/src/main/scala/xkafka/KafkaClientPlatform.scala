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
import cats.arrow.FunctionK
import cats.data.NonEmptyList
import cats.effect.{Async, Resource}
import cats.effect.implicits.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.{
  AutoOffsetReset as Fs2AutoOffsetReset, CommittableConsumerRecord as Fs2CommittableConsumerRecord, ConsumerSettings as Fs2ConsumerSettings,
  Deserializer as Fs2Deserializer, Header as Fs2Header, Headers as Fs2Headers, KafkaConsumer as Fs2KafkaConsumer, KafkaProducer as Fs2KafkaProducer,
  ProducerRecord as Fs2ProducerRecord, ProducerSettings as Fs2ProducerSettings, Serializer as Fs2Serializer
}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.producer.MkProducer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata as JavaRecordMetadata
import org.apache.kafka.common.{KafkaException as JavaKafkaException, TopicPartition as JavaTopicPartition}
import org.apache.kafka.common.errors.{
  AuthenticationException, InvalidPidMappingException, ProducerFencedException, RetriableException, SaslAuthenticationException,
  SslAuthenticationException
}
import org.apache.kafka.common.protocol.Errors

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new Fs2KafkaClient[F]

  private[xkafka] def fromFs2[F[_]](using Async[F], Parallel[F], MkProducer[F], MkConsumer[F]): KafkaClient[F] = new Fs2KafkaClient[F]

private final class Fs2KafkaClient[F[_]](using F: Async[F], P: Parallel[F], mkProducer: MkProducer[F], mkConsumer: MkConsumer[F])
    extends KafkaClient[F]:
  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    Fs2KafkaProducer.resource(producerSettings(settings)).mapK(handleBackendErrors).map(new Fs2KafkaProducerAdapter(_))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      consumer <- Fs2KafkaConsumer.resource(consumerSettings(settings)).mapK(handleBackendErrors)
      _        <- Resource.eval(subscribe(consumer, subscription))
    yield new Fs2KafkaConsumerAdapter(consumer)

  private def subscribe[K, V](consumer: Fs2KafkaConsumer[F, K, V], subscription: Subscription): F[Unit] =
    subscription match
      case Subscription.Topics(topics)   => backend(consumer.subscribe(topics.map(_.value)))
      case Subscription.Pattern(pattern) => F.delay(pattern.anchored.r).flatMap(value => backend(consumer.subscribe(value)))

  private val handleBackendErrors: FunctionK[F, F] =
    new FunctionK[F, F]:
      override def apply[A](value: F[A]): F[A] = backend(value)

  private def backend[A](value: F[A]): F[A] =
    value.adaptError:
      case error: JavaKafkaException => new KafkaException.BackendFailure(
          Option(error.getMessage).getOrElse(error.getClass.getName),
          code = protocolCode(error),
          retriable = Some(error.isInstanceOf[RetriableException]),
          fatal = Some(error.isInstanceOf[InvalidPidMappingException] || error.isInstanceOf[ProducerFencedException]),
          cause = error
        )

  /** Kafka maps its own exceptions back to the protocol error table, which is the same table librdkafka reports.
    *
    * Authentication never reached the broker, so it has no entry in that table. `Errors.forException` answers `INVALID_CONFIG` for the whole family,
    * which is a code the broker never sent, so those are named here.
    */
  private def protocolCode(error: JavaKafkaException): Option[ErrorCode] =
    error match
      case _: SslAuthenticationException  => Some(ErrorCode.SslAuthenticationFailed)
      case _: SaslAuthenticationException => Some(ErrorCode.SaslAuthenticationFailed)
      case _: AuthenticationException     => None
      case _ => Option(Errors.forException(error)).filterNot(_ == Errors.NONE).map(value => ErrorCode.fromProtocol(value.code.toInt))

  private def producerSettings[K, V](settings: ProducerSettings[F, K, V]): Fs2ProducerSettings[F, K, V] =
    val base =
      Fs2ProducerSettings(serializer(settings.keySerializer), serializer(settings.valueSerializer))
        .withProperties(settings.client.properties ++ settings.properties).withBootstrapServers(settings.client.bootstrapServers.toList.mkString(","))

    settings.client.clientId.fold(base)(base.withClientId)

  private def consumerSettings[K, V](settings: ConsumerSettings[F, K, V]): Fs2ConsumerSettings[F, K, V] =
    val base =
      Fs2ConsumerSettings(deserializer(settings.keyDeserializer), deserializer(settings.valueDeserializer))
        .withProperties(settings.client.properties ++ settings.properties).withBootstrapServers(settings.client.bootstrapServers.toList.mkString(","))
        .withGroupId(settings.groupId.value).withProperty("enable.auto.commit", "false").withAutoOffsetReset(
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
      yield bytes.map(_.toArray).orNull

  private def deserializer[A](value: Deserializer[F, A]): Fs2Deserializer[F, A] =
    Fs2Deserializer.instance: (topic, headers, bytes) =>
      F.fromEither(validTopic(topic)).flatMap: portableTopic =>
        value.deserialize(portableTopic, portableHeaders(headers), Option(bytes).map(Chunk.array))

  private def validTopic(value: String): Either[Throwable, Topic] = Topic.from(value).leftMap(error => invalidBackendValue("topic", error))

  private def portableHeaders(headers: Fs2Headers): Headers =
    Headers.fromVector(headers.toChain.iterator.map: header =>
      Header(header.key, Option(header.value).map(Chunk.array))
    .toVector)

  private def invalidBackendValue(field: String, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field: $error")

  private final class Fs2KafkaProducerAdapter[K, V](underlying: Fs2KafkaProducer[F, K, V]) extends KafkaProducer[F, K, V]:

    // fs2-kafka is already two-stage, and pairs each record with its own metadata, so both survive unflattened.
    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[ProducerResult[K, V]]] =
      backend(underlying.produce(Chunk.from(records.toList.map(producerRecord)))).map: acknowledgement =>
        backend(acknowledgement).flatMap: result =>
          result.toList.traverse((_, metadata) => recordMetadata(metadata)).flatMap: reported =>
            NonEmptyList.fromList(reported).filter(_.size == records.size) match
              case Some(values) => F.pure(ProducerResult(records.zipWith(values)((record, value) => record -> Some(value))))
              case None => F.raiseError(new KafkaException.InvalidBackendResponse(s"expected ${records.size} metadata entries, got ${reported.size}"))

    private def producerRecord(record: ProducerRecord[K, V]): Fs2ProducerRecord[K, V] =
      val base        = Fs2ProducerRecord(record.topic.value, record.key, record.value).withHeaders(fs2Headers(record.headers))
      val partitioned = record.partition.fold(base)(partition => base.withPartition(partition.value))

      record.timestamp.fold(partitioned)(timestamp => partitioned.withTimestamp(timestamp.epochMillis))

    private def fs2Headers(headers: Headers): Fs2Headers =
      Fs2Headers.fromSeq(
        headers.values.map: header =>
          val bytes = header.value.map(_.toArray).orNull
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
          backend(underlying.commitSync(
            offsets.map:
              case (topicPartition, offset) => new JavaTopicPartition(topicPartition.topic.value, topicPartition.partition.value) ->
                  new OffsetAndMetadata(offset.value)
          ))

    override val records: fs2.Stream[F, CommittableConsumerRecord[F, K, V]] =
      underlying.records.translate(handleBackendErrors).evalMap(consumerRecord)

    override def assignment: F[Set[TopicPartition]] = backend(underlying.assignment).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    /** fs2-kafka reports assignments as the group rebalances, so nothing is polled. */
    override val assignmentChanges: Stream[F, Set[TopicPartition]] =
      underlying.assignmentStream.translate(handleBackendErrors).evalMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    /** Delegates to fs2-kafka, whose partition streams are fed independently, so `maxQueuedRecords` does not apply and one slow partition cannot
      * stall another.
      */
    override def partitionedRecords(maxQueuedRecords: Int)(using Async[F]): Stream[F, PartitionRecords[F, K, V]] =
      underlying.partitionsMapStream.translate(handleBackendErrors).flatMap: partitions =>
        Stream.emits(partitions.toList).evalMap: (javaTopicPartition, records) =>
          portableTopicPartition(javaTopicPartition)
            .map(topicPartition => PartitionRecords(topicPartition, records.translate(handleBackendErrors).evalMap(consumerRecord)))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        backend(underlying.committed(topicPartitions.map(javaTopicPartition))).flatMap: values =>
          topicPartitions.toList.traverse: topicPartition =>
            Option(values.getOrElse(javaTopicPartition(topicPartition), null)).traverse: metadata =>
              F.fromEither(Offset.from(metadata.offset).leftMap(error => invalidBackendValue("committed offset", error)))
            .map(topicPartition -> _)
          .map(_.toMap)

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        backend(underlying.beginningOffsets(topicPartitions.map(javaTopicPartition))).flatMap(portableOffsets("beginning offset", topicPartitions, _))

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else backend(underlying.endOffsets(topicPartitions.map(javaTopicPartition))).flatMap(portableOffsets("end offset", topicPartitions, _))

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty)
      else
        val requested =
          timestampsToSearch.map: (topicPartition, timestamp) =>
            javaTopicPartition(topicPartition) -> timestamp.epochMillis
        backend(underlying.offsetsForTimes(requested)).flatMap: values =>
          timestampsToSearch.keys.toList.traverse: topicPartition =>
            values.get(javaTopicPartition(topicPartition)).flatten.traverse: value =>
              F.fromEither(Offset.from(value.offset).leftMap(error => invalidBackendValue("timestamp offset", error)))
            .map(topicPartition -> _)
          .map(_.toMap)

    override def partitionsFor(topic: Topic): F[Set[Partition]] = backend(underlying.partitionsFor(topic.value)).flatMap(portablePartitions)

    override def listTopics: F[Map[Topic, Set[Partition]]] =
      backend(underlying.listTopics).flatMap: values =>
        values.toList.traverse: (topicValue, partitions) =>
          (F.fromEither(validTopic(topicValue)), portablePartitions(partitions)).mapN(_ -> _)
        .map(_.toMap)

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] =
      backend(underlying.seek(javaTopicPartition(topicPartition), offset.value))

    private def portableOffsets(
        field: String,
        requested: Set[TopicPartition],
        values: Map[JavaTopicPartition, Long]
    ): F[Map[TopicPartition, Offset]] =
      requested.toList.traverse: topicPartition =>
        values.get(javaTopicPartition(topicPartition)) match
          case Some(value) => F.fromEither(Offset.from(value).leftMap(error => invalidBackendValue(field, error))).map(topicPartition -> _)
          case None        => F.raiseError(new KafkaException.InvalidBackendResponse(s"missing $field for $topicPartition"))
      .map(_.toMap)

    private def portablePartitions(values: Iterable[org.apache.kafka.common.PartitionInfo]): F[Set[Partition]] =
      values.toList.traverse(value => F.fromEither(Partition.from(value.partition).leftMap(error => invalidBackendValue("partition", error))))
        .map(_.toSet)

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
