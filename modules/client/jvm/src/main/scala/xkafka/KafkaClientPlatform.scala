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

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters.*

import cats.Parallel
import cats.arrow.FunctionK
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Resource}
import cats.effect.implicits.*
import cats.effect.std.Random
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.{
  AdminClientSettings as Fs2AdminClientSettings, AutoOffsetReset as Fs2AutoOffsetReset, CommittableConsumerRecord as Fs2CommittableConsumerRecord,
  CommitTimeoutException, ConsumerSettings as Fs2ConsumerSettings, Deserializer as Fs2Deserializer, Header as Fs2Header, Headers as Fs2Headers,
  KafkaAdminClient as Fs2KafkaAdminClient, KafkaByteConsumer, KafkaConsumer as Fs2KafkaConsumer, KafkaProducer as Fs2KafkaProducer,
  ProducerRecord as Fs2ProducerRecord, ProducerSettings as Fs2ProducerSettings, Serializer as Fs2Serializer
}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.instances.*
import fs2.kafka.producer.MkProducer
import org.apache.kafka.clients.admin.{NewPartitions as JavaNewPartitions, NewTopic as JavaNewTopic}
import org.apache.kafka.clients.consumer.{
  CommitFailedException, Consumer as JavaConsumer, ConsumerGroupMetadata as JavaConsumerGroupMetadata, OffsetAndMetadata
}
import org.apache.kafka.clients.producer.RecordMetadata as JavaRecordMetadata
import org.apache.kafka.common.{KafkaException as JavaKafkaException, TopicPartition as JavaTopicPartition}
import org.apache.kafka.common.errors.{
  AuthenticationException, InvalidPidMappingException, ProducerFencedException, SaslAuthenticationException, SslAuthenticationException
}
import org.apache.kafka.common.protocol.Errors
import internal.ClientProperties
import internal.security.SecurityProperties

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new Fs2KafkaClient[F]

  private[xkafka] def fromFs2[F[_]](using Async[F], Parallel[F], MkProducer[F], MkConsumer[F]): KafkaClient[F] = new Fs2KafkaClient[F]

/** What a transaction on this backend needs to record a consumer's offsets. The Java client names a group by the metadata its consumer carries, which
  * also fences a member the group has already replaced.
  */
private[xkafka] final case class Fs2GroupHandle(metadata: JavaConsumerGroupMetadata) extends GroupHandle

/** A value as it reaches the consumer adapter, with the group membership the consumer held when the value was polled. */
private final case class Polled[V](value: V, membership: JavaConsumerGroupMetadata)

private final class Fs2KafkaClient[F[_]](using F: Async[F], P: Parallel[F], mkProducer: MkProducer[F], mkConsumer: MkConsumer[F])
    extends KafkaClient[F]:
  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    Fs2KafkaProducer.resource(producerSettings(settings)).mapK(handleBackendErrors).map(new Fs2KafkaProducerAdapter(_))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], selection: Selection): Resource[F, KafkaConsumer[F, K, V]] =
    for
      membership <- Resource.eval(F.delay(new AtomicReference[JavaConsumerGroupMetadata]))
      consumer   <- Fs2KafkaConsumer.resource(consumerSettings(settings, membership))(using F, recordingPolls(membership)).mapK(handleBackendErrors)
      _          <- Resource.eval(select(consumer, selection))
      // The commit recovery spreads its retries, so the consumer carries the randomness that does the spreading.
      random <- Resource.eval(Random.scalaUtilRandom[F])
    yield new Fs2KafkaConsumerAdapter(consumer, settings.commitRecovery, random)

  override def transactionalProducer[K, V](settings: TransactionalProducerSettings[F, K, V]): Resource[F, KafkaTransactionalProducer[F, K, V]] =
    Fs2KafkaProducer.transactional(
      producerSettings(settings.producer).withTransactionalId(settings.transactionalId.value).withTransactionTimeout(settings.transactionTimeout)
    ).mapK(handleBackendErrors).map(new Fs2TransactionalProducerAdapter(_))

  private final class Fs2TransactionalProducerAdapter[K, V](underlying: Fs2KafkaProducer[F, K, V]) extends KafkaTransactionalProducer[F, K, V]:
    /** fs2-kafka begins the transaction on acquire, commits it where the body succeeds, and aborts it where the body fails or is cancelled. */
    override def transactionally[A](use: Transaction[F, K, V] => F[A]): F[A] = backend(underlying.transaction.surround(use(transaction)))

    private val records: KafkaProducer[F, K, V] = new Fs2KafkaProducerAdapter(underlying)

    private val transaction: Transaction[F, K, V] =
      new Transaction[F, K, V]:
        /** A transaction cannot commit records the broker has not acknowledged, so this waits for them. */
        override def produce(values: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]] = records.produceAndAwait(values)

        override def commitOffsets(batch: CommittableOffsetBatch[F]): F[Unit] = GroupMembership.resolve(batch).use(commitMemberships)

        private def commitMemberships(memberships: List[(GroupHandle, Map[TopicPartition, Offset])]): F[Unit] =
          memberships.traverse_ : membership =>
            membership match
              case (Fs2GroupHandle(metadata), offsets) =>
                val committed =
                  offsets.map: (topicPartition, offset) =>
                    new JavaTopicPartition(topicPartition.topic.value, topicPartition.partition.value) -> new OffsetAndMetadata(offset.value)
                backend(underlying.sendOffsetsToTransaction(committed, metadata))
              case (other, _) => F.raiseError(GroupMembership.unrecognised(other))

  override def admin(settings: ClientSettings): Resource[F, KafkaAdminClient[F]] =
    Fs2KafkaAdminClient.resource(adminSettings(settings)).mapK(handleBackendErrors).map(new Fs2KafkaAdminClientAdapter(_))

  private def adminSettings(settings: ClientSettings): Fs2AdminClientSettings =
    val base =
      Fs2AdminClientSettings(settings.bootstrapServers.toList.mkString(","))
        .withProperties(settings.properties ++ SecurityProperties.javaClient(settings.security) ++ ClientProperties(settings))

    settings.clientId.fold(base)(base.withClientId)

  private final class Fs2KafkaAdminClientAdapter(underlying: Fs2KafkaAdminClient[F]) extends KafkaAdminClient[F]:
    override def createTopics(topics: NonEmptySet[NewTopic]): F[Unit] = backend(underlying.createTopics(topics.toSortedSet.toList.map(javaNewTopic)))

    override def deleteTopics(topics: NonEmptySet[Topic]): F[Unit] = backend(underlying.deleteTopics(topics.map(_.value)))

    override def createPartitions(topic: Topic, count: Int): F[Unit] =
      F.fromEither(KafkaAdminClient.validatePartitionCount(count))
        .flatMap(valid => backend(underlying.createPartitions(Map(topic.value -> JavaNewPartitions.increaseTo(valid)))))

    override def describeTopics(topics: NonEmptySet[Topic]): F[Map[Topic, Set[Partition]]] =
      backend(underlying.describeTopics(topics.map(_.value))).flatMap: described =>
        described.toList.traverse: (name, description) =>
          for
            topic      <- F.fromEither(validTopic(name))
            partitions <-
              description.partitions.asScala.toList
                .traverse(value => F.fromEither(Partition.from(value.partition).leftMap(error => invalidBackendValue("partition", error))))
          yield topic -> partitions.toSet
        .map(_.toMap)

    private def javaNewTopic(value: NewTopic): JavaNewTopic =
      new JavaNewTopic(value.topic.value, value.partitions, value.replicationFactor).configs(value.configuration.asJava)

  private def select[K, V](consumer: Fs2KafkaConsumer[F, K, V], selection: Selection): F[Unit] =
    selection match
      case Selection.Topics(topics)              => backend(consumer.subscribe(topics.toNonEmptyList.map(_.value)))
      case Selection.Pattern(pattern)            => F.delay(pattern.anchored.r).flatMap(value => backend(consumer.subscribe(value)))
      case Selection.Partitions(topicPartitions) =>
        backend(consumer.assign(topicPartitions.map(value => new JavaTopicPartition(value.topic.value, value.partition.value))))

  private val handleBackendErrors: FunctionK[F, F] =
    new FunctionK[F, F]:
      override def apply[A](value: F[A]): F[A] = backend(value)

  private def backend[A](value: F[A]): F[A] =
    value.adaptError:
      // Classified the way the broker would classify a request that ran out of time, so every backend agrees and the
      // recovery policy retries it.
      case error: CommitTimeoutException => new KafkaException.BackendFailure(
          "the commit did not complete within its timeout",
          code = Some(ErrorCode.RequestTimedOut),
          retriable = Some(true),
          fatal = Some(false),
          cause = error
        )
      case error: JavaKafkaException =>
        val code = protocolCode(error)
        new KafkaException.BackendFailure(
          Option(error.getMessage).getOrElse(error.getClass.getName),
          code = code,
          // Derived from the portable code, so the same condition is retriable on every backend.
          retriable = code.map(_.retriable),
          fatal = Some(error.isInstanceOf[InvalidPidMappingException] || error.isInstanceOf[ProducerFencedException]),
          cause = error
        )

  /** Kafka maps its own exceptions back to the protocol error table, which is the same table librdkafka reports.
    *
    * Authentication never reached the broker, so it has no entry in that table. `Errors.forException` answers `INVALID_CONFIG` for the whole family,
    * which is a code the broker never sent, so those are named here.
    *
    * A transaction's offsets that the group rejects as no longer its member's come back as one `CommitFailedException` for either of two answers, and
    * only its message, which ends with that answer's own description, says which. Reading it back gives the code librdkafka reports for the same
    * rejection.
    */
  private def protocolCode(error: JavaKafkaException): Option[ErrorCode] =
    error match
      case _: SslAuthenticationException  => Some(ErrorCode.SslAuthenticationFailed)
      case _: SaslAuthenticationException => Some(ErrorCode.SaslAuthenticationFailed)
      case _: AuthenticationException     => None
      case failed: CommitFailedException  => rejectedMembership(failed).orElse(tabled(error))
      case _                              => tabled(error)

  private def tabled(error: JavaKafkaException): Option[ErrorCode] =
    Option(Errors.forException(error)).filterNot(_ == Errors.NONE).map(value => ErrorCode.fromProtocol(value.code.toInt))

  /** The two answers the Java client folds into a `CommitFailedException` when a transaction records offsets. */
  private def rejectedMembership(failed: CommitFailedException): Option[ErrorCode] =
    Option(failed.getMessage)
      .flatMap(message => List(Errors.ILLEGAL_GENERATION, Errors.UNKNOWN_MEMBER_ID).find(value => message.endsWith(value.exception.getMessage)))
      .map(value => ErrorCode.fromProtocol(value.code.toInt))

  private def producerSettings[K, V](settings: ProducerSettings[F, K, V]): Fs2ProducerSettings[F, K, V] =
    val base =
      Fs2ProducerSettings(serializer(settings.keySerializer), serializer(settings.valueSerializer)).withProperties(
        settings.client.properties ++ settings.properties ++ SecurityProperties.javaClient(settings.client.security) ++
          ClientProperties(settings.client)
      ).withBootstrapServers(settings.client.bootstrapServers.toList.mkString(",")).withCloseTimeout(settings.closeTimeout)
        .withProperty("acks", settings.acks.property)

    settings.client.clientId.fold(base)(base.withClientId)

  /** Builds the consumer `mkConsumer` would, and has each poll record the membership the consumer then holds.
    *
    * The Java consumer changes membership only inside a poll, and fs2-kafka deserializes a poll's records before it polls again, so the membership a
    * record's deserializer reads is the one its poll ended under. The generation fs2-kafka's own committer reports is read when a transaction runs,
    * which can be after a rebalance has moved the record's partition elsewhere.
    */
  private def recordingPolls(membership: AtomicReference[JavaConsumerGroupMetadata]): MkConsumer[F] =
    new MkConsumer[F]:
      override def apply[G[_]](settings: Fs2ConsumerSettings[G, ?, ?]): F[KafkaByteConsumer] =
        mkConsumer(settings).map: consumer =>
          val recording: InvocationHandler =
            (_: AnyRef, method: Method, arguments: Array[AnyRef]) =>
              val result =
                try method.invoke(consumer, Option(arguments).getOrElse(Array.empty[AnyRef])*)
                catch case error: InvocationTargetException => throw error.getCause
              if method.getName == "poll" then membership.set(consumer.groupMetadata())
              result
          Proxy.newProxyInstance(classOf[JavaConsumer[?, ?]].getClassLoader, Array(classOf[JavaConsumer[?, ?]]), recording)
            .asInstanceOf[KafkaByteConsumer]

  private def consumerSettings[K, V](
      settings: ConsumerSettings[F, K, V],
      membership: AtomicReference[JavaConsumerGroupMetadata]
  ): Fs2ConsumerSettings[F, K, Polled[V]] =
    val polled =
      Fs2Deserializer.instance[F, Polled[V]]: (topic, headers, bytes) =>
        for
          value   <- deserializer(settings.valueDeserializer).deserialize(topic, headers, bytes)
          current <-
            F.delay(Option(membership.get)).flatMap(_.liftTo[F](new KafkaException.InvalidBackendResponse("a record arrived before any poll")))
        yield Polled(value, current)
    val base =
      Fs2ConsumerSettings(deserializer(settings.keyDeserializer), polled).withProperties(
        settings.client.properties ++ settings.properties ++ SecurityProperties.javaClient(settings.client.security) ++
          ClientProperties(settings.client)
      ).withBootstrapServers(settings.client.bootstrapServers.toList.mkString(",")).withGroupId(settings.groupId.value)
        .withPollTimeout(settings.pollTimeout).withDefaultApiTimeout(settings.requestTimeout).withProperty("enable.auto.commit", "false")
        .withProperty("isolation.level", isolationLevel(settings.isolationLevel)).withCommitTimeout(settings.commitTimeout).withAutoOffsetReset(
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

  private def portablePartitions(values: Iterable[org.apache.kafka.common.PartitionInfo]): F[Set[Partition]] =
    values.toList.traverse(value => F.fromEither(Partition.from(value.partition).leftMap(error => invalidBackendValue("partition", error))))
      .map(_.toSet)

  private def isolationLevel(value: IsolationLevel): String =
    value match
      case IsolationLevel.ReadUncommitted => "read_uncommitted"
      case IsolationLevel.ReadCommitted   => "read_committed"

  private final class Fs2KafkaProducerAdapter[K, V](underlying: Fs2KafkaProducer[F, K, V]) extends KafkaProducer[F, K, V]:

    override def partitionsFor(topic: Topic): F[Set[Partition]] = backend(underlying.partitionsFor(topic.value)).flatMap(portablePartitions)

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

  private final class Fs2KafkaConsumerAdapter[K, V](underlying: Fs2KafkaConsumer[F, K, Polled[V]], recovery: CommitRecovery, random: Random[F])
      extends KafkaConsumer[F, K, V]:

    private val offsetCommitter: OffsetCommitter[F] =
      CommitRecovery.recovering(
        new OffsetCommitter[F]:
          override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
            backend(underlying.commitSync(
              offsets.map:
                case (topicPartition, offset) => new JavaTopicPartition(topicPartition.topic.value, topicPartition.partition.value) ->
                    new OffsetAndMetadata(offset.value)
            ))

          override private[xkafka] val membership: GroupMembership[F] =
            GroupMembership.Backend(backend(underlying.groupMetadata).map(Fs2GroupHandle(_)), _ => F.unit)
        ,
        recovery,
        random
      )

    override val records: fs2.Stream[F, CommittableConsumerRecord[F, K, V]] =
      underlying.records.translate(handleBackendErrors).evalMap(consumerRecord)

    override def assignment: F[Set[TopicPartition]] = backend(underlying.assignment).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    override def stopConsuming: F[Unit] = backend(underlying.stopConsuming)

    /** fs2-kafka reports assignments as the group rebalances, so nothing is polled. */
    override val assignmentChanges: Stream[F, Set[TopicPartition]] =
      underlying.assignmentStream.translate(handleBackendErrors).evalMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    /** Delegates to fs2-kafka, whose partition streams are fed independently, so `maxQueuedRecords` does not apply and one slow partition cannot
      * stall another.
      */
    override def partitionedRecords(maxQueuedRecords: Int)(using Async[F]): Stream[F, PartitionRecords[F, K, V]] =
      PartitionRecords.validateMaxQueuedRecords(maxQueuedRecords) match
        case Left(error) => Stream.raiseError(error)
        case Right(_)    => underlying.partitionsMapStream.translate(handleBackendErrors).flatMap: partitions =>
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

    override def seekToBeginning(topicPartitions: Set[TopicPartition]): F[Unit] =
      backend(underlying.seekToBeginning(topicPartitions.toList.map(javaTopicPartition)))

    override def seekToEnd(topicPartitions: Set[TopicPartition]): F[Unit] =
      backend(underlying.seekToEnd(topicPartitions.toList.map(javaTopicPartition)))

    /** The Java client settles on a position before answering, so it has one whenever the partition is assigned. */
    override def position(topicPartition: TopicPartition): F[Option[Offset]] =
      backend(underlying.position(javaTopicPartition(topicPartition))).flatMap: value =>
        F.fromEither(Offset.from(value).map(_.some).leftMap(error => invalidBackendValue("position", error)))

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

    private def consumerRecord(committable: Fs2CommittableConsumerRecord[F, K, Polled[V]]): F[CommittableConsumerRecord[F, K, V]] =
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
              value = source.value.value,
              headers = portableHeaders(source.headers)
            )
          val portableOffset =
            new CommittableOffset[F]:
              override val topicPartition: TopicPartition = portableTopicPartition

              override val nextOffset: Offset = portableNextOffset

              override val committer: OffsetCommitter[F] = offsetCommitter

              override private[xkafka] val membership: GroupMembership[F] = membershipOf(source.value.membership)

          CommittableConsumerRecord(portableRecord, portableOffset)

      F.fromEither(validated)

    private def javaTopicPartition(topicPartition: TopicPartition): JavaTopicPartition =
      new JavaTopicPartition(topicPartition.topic.value, topicPartition.partition.value)

    /** The Java metadata compares by value, so every record polled under one membership names the same one. */
    private def membershipOf(metadata: JavaConsumerGroupMetadata): GroupMembership[F] =
      new GroupMembership.Backend(metadata, F.pure(Fs2GroupHandle(metadata)), _ => F.unit)

    private def portableTopicPartition(source: JavaTopicPartition): F[TopicPartition] =
      F.fromEither:
        for
          topic     <- Topic.from(source.topic).leftMap(error => invalidBackendValue("assigned topic", error))
          partition <- Partition.from(source.partition).leftMap(error => invalidBackendValue("assigned partition", error))
        yield TopicPartition(topic, partition)
