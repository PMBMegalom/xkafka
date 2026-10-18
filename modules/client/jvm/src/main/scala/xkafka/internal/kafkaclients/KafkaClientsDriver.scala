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
package kafkaclients

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}
import java.util.regex.Pattern

import scala.jdk.CollectionConverters.*

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import org.apache.kafka.clients.admin.{
  Admin, ConfigEntry as JavaConfigEntry, CreatePartitionsOptions, CreateTopicsOptions, DeleteRecordsOptions, DeleteTopicsOptions,
  DescribeConfigsOptions, DescribeTopicsOptions, ListTopicsOptions, NewPartitions as JavaNewPartitions, NewTopic as JavaNewTopic, RecordsToDelete
}
import org.apache.kafka.clients.consumer.{
  Consumer as JavaConsumer, ConsumerGroupMetadata as JavaConsumerGroupMetadata, KafkaConsumer as JavaKafkaConsumer, OffsetAndMetadata
}
import org.apache.kafka.clients.producer.{
  Callback, KafkaProducer as JavaKafkaProducer, Producer as JavaProducer, ProducerRecord as JavaProducerRecord, RecordMetadata as JavaRecordMetadata
}
import org.apache.kafka.common.{KafkaFuture, TopicPartition as JavaTopicPartition}
import org.apache.kafka.common.config.ConfigResource as JavaConfigResource
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.utils.AppInfoParser
import driver.{Delivery, EncodedRecord, KafkaDriver, PolledRecord, Rebalance}

/** The Java client a driver makes each of its clients with, which a test replaces with the Java client's mocks. */
private[xkafka] final case class JavaClients(
    producer: Map[String, AnyRef] => JavaProducer[Array[Byte], Array[Byte]],
    consumer: Map[String, AnyRef] => JavaConsumer[Array[Byte], Array[Byte]],
    admin: Map[String, AnyRef] => Admin
)

private[xkafka] object JavaClients:
  val default: JavaClients =
    JavaClients(
      properties => new JavaKafkaProducer(properties.asJava, new ByteArraySerializer, new ByteArraySerializer),
      properties => new JavaKafkaConsumer(properties.asJava, new ByteArrayDeserializer, new ByteArrayDeserializer),
      properties => Admin.create(properties.asJava)
    )

/** What a transaction on this backend needs to record a consumer's offsets. The Java client names a group by the metadata its consumer carries, which
  * also fences a member the group has already replaced.
  */
private[xkafka] final case class JavaGroupMetadata(metadata: JavaConsumerGroupMetadata) extends GroupHandle

/** A Java client of either kind, or an administration client. */
private[kafkaclients] enum JavaClient:
  case Producer(producer: JavaProducer[Array[Byte], Array[Byte]])
  case Consumer(thread: ConsumerThread)
  case Administration(admin: Admin)

/** One produced batch's delivery reports, which the producer's own thread writes as the broker acknowledges each record. */
private[kafkaclients] final class DeliveryBatch(capacity: Int):
  val reports     = new AtomicReferenceArray[Either[Throwable, JavaRecordMetadata]](capacity)
  val enqueued    = new AtomicInteger(0)
  val outstanding = new AtomicInteger(0)
  // Completed once every enqueued record is reported and no more will be enqueued.
  val delivered           = new CompletableFuture[Unit]
  private val sealedBatch = new java.util.concurrent.atomic.AtomicBoolean(false)

  def report(index: Int, outcome: Either[Throwable, JavaRecordMetadata]): Unit =
    reports.set(index, outcome)
    if outstanding.decrementAndGet() == 0 && sealedBatch.get then delivered.complete(()): Unit

  /** No further record joins the batch, so it is delivered once the ones it has are. */
  def seal(): Unit =
    sealedBatch.set(true)
    if outstanding.get == 0 then delivered.complete(()): Unit

/** The Java client as the shared client drives it.
  *
  * Consumers make every call on a thread of their own (see [[ConsumerThread]]). Producers and administration clients are safe to call from any
  * thread, so their calls are made where they are asked for, blocking where the Java client blocks.
  */
private[xkafka] final class KafkaClientsDriver[F[_]](clients: JavaClients)(using F: Async[F]) extends KafkaDriver[F]:
  type Client        = JavaClient
  type Batch         = DeliveryBatch
  type Event         = ConsumerThread.Rebalancing
  type GroupMetadata = JavaGroupMetadata

  override def version: String = AppInfoParser.getVersion

  override val deliveriesShareQueue: Boolean = false

  override def producer(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client] =
    javaCall(F.blocking(JavaClient.Producer(clients.producer(connection(bootstrapServers, clientId, properties)))))

  override def destroyProducer(producer: Client, timeoutMillis: Int): F[Unit] =
    producing(producer)(value => F.blocking(value.close(Duration.ofMillis(timeoutMillis.toLong))))

  override def consumer(
      bootstrapServers: String,
      clientId: Option[String],
      groupId: String,
      autoOffsetReset: String,
      properties: Map[String, String]
  ): F[Client] =
    val configured =
      connection(bootstrapServers, clientId, properties) ++
        Map("group.id" -> groupId, "auto.offset.reset" -> autoOffsetReset, "enable.auto.commit" -> "false")
    javaCall(
      F.blocking:
        val thread = new ConsumerThread(clients.consumer(configured), s"xkafka-consumer-${clientId.getOrElse(groupId)}")
        thread.start()
        JavaClient.Consumer(thread)
    )

  override def destroyConsumer(consumer: Client): F[Unit] = onThread(consumer)(thread => F.async_[Unit](callback => thread.close(callback)))

  override def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client] =
    javaCall(F.blocking(JavaClient.Administration(clients.admin(connection(bootstrapServers, clientId, properties)))))

  override def destroyAdmin(admin: Client): F[Unit] = administering(admin)(value => F.blocking(value.close()))

  override def batch(capacity: Int): F[Batch] = F.delay(new DeliveryBatch(capacity))

  override def freeBatch(batch: Batch): F[Unit] = F.unit

  override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): F[Unit] =
    producing(producer): value =>
      F.blocking:
        try records.toList.foreach: record =>
            val index = batch.enqueued.get
            batch.outstanding.incrementAndGet()
            val callback: Callback = (metadata, error) => batch.report(index, Option(error).toLeft(metadata))
            try value.send(javaRecord(record), callback): Unit
            catch
              case error: Throwable =>
                batch.outstanding.decrementAndGet()
                throw error
            batch.enqueued.incrementAndGet()
        finally batch.seal()

  override def serveDeliveries(producer: Client, batch: Batch, timeoutMillis: Int): F[Int] =
    // The batch's own future is shared by every wait, so a wait that times out gives up on a copy of it.
    F.fromCompletableFuture(F.delay(batch.delivered.thenApply(identity)))
      .timeoutTo(scala.concurrent.duration.FiniteDuration(timeoutMillis.toLong, "ms"), F.unit) *> pending(batch)

  override def pending(batch: Batch): F[Int] = F.delay(batch.outstanding.get)

  override def deliveries(producer: Client, batch: Batch): F[Vector[Delivery]] =
    javaCall:
      F.delay:
        Vector.tabulate(batch.enqueued.get)(batch.reports.get).map:
          case Right(metadata) => Delivery(
              metadata.partition,
              if metadata.hasOffset then metadata.offset else -1L,
              if metadata.hasTimestamp then metadata.timestamp else -1L
            )
          case Left(error) => throw error

  override def initTransactions(producer: Client, timeoutMillis: Int): F[Unit] = producing(producer)(value => F.blocking(value.initTransactions()))

  override def beginTransaction(producer: Client): F[Unit] = producing(producer)(value => F.blocking(value.beginTransaction()))

  override def commitTransaction(producer: Client, timeoutMillis: Int): F[Unit] = producing(producer)(value => F.blocking(value.commitTransaction()))

  override def abortTransaction(producer: Client, timeoutMillis: Int): F[Unit] = producing(producer)(value => F.blocking(value.abortTransaction()))

  override def sendOffsets(producer: Client, metadata: GroupMetadata, offsets: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Unit] =
    producing(producer)(value => F.blocking(value.sendOffsetsToTransaction(committedOffsets(offsets), metadata.metadata)))

  override def groupMetadata(consumer: Client): F[GroupMetadata] = onConsumer(consumer)(value => JavaGroupMetadata(value.groupMetadata()))

  override def destroyGroupMetadata(metadata: GroupMetadata): F[Unit] = F.unit

  override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
    handle match
      case metadata: JavaGroupMetadata => Some(metadata)
      case _                           => None

  override def assign(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    onConsumer(consumer)(_.assign(partitions.map(javaTopicPartition).asJava))

  /** A subscription the shared client spells as a pattern starts with `^`, which no topic name can. */
  override def subscribe(consumer: Client, topics: NonEmptyList[String]): F[Unit] =
    onThread(consumer): thread =>
      onConsumer(consumer): value =>
        if topics.tail.isEmpty && topics.head.startsWith("^") then value.subscribe(Pattern.compile(topics.head), thread.listener)
        else value.subscribe(topics.toList.asJava, thread.listener)

  override def poll(consumer: Client, timeoutMillis: Int): F[List[PolledRecord]] =
    onThread(consumer)(thread =>
      javaCall(F.async_[List[ConsumerThread.Record]](thread.poll(Duration.ofMillis(timeoutMillis.toLong)))).map(_.map(polled))
    )

  override def takeRebalances(consumer: Client): F[List[Rebalance[Event]]] =
    onThread(consumer): thread =>
      F.async_[List[ConsumerThread.Rebalancing]](thread.takeRebalances).map(_.map(taken => Rebalance(taken, taken.kind, taken.partitions)))

  override def applyRebalance(consumer: Client, event: Event): F[Unit] = onThread(consumer)(thread => F.async_[Unit](thread.apply(event)))

  /** The listener hears a revocation before every assignment, whichever protocol the group uses, so an assignment only ever adds partitions. */
  override def cooperative(consumer: Client): F[Boolean] = F.pure(true)

  override def assignment(consumer: Client): F[Vector[(String, Int)]] =
    onConsumer(consumer)(_.assignment.asScala.toVector.map(value => value.topic -> value.partition))

  override def commit(consumer: Client, offsets: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Unit] =
    onConsumer(consumer)(_.commitSync(committedOffsets(offsets), Duration.ofMillis(timeoutMillis.toLong)))

  override def pause(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    onConsumer(consumer)(_.pause(partitions.map(javaTopicPartition).asJava))

  override def resume(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    onConsumer(consumer)(_.resume(partitions.map(javaTopicPartition).asJava))

  override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[Long]] =
    onConsumer(consumer): value =>
      val answers = value.committed(partitions.map(javaTopicPartition).toSet.asJava, Duration.ofMillis(timeoutMillis.toLong))
      partitions.map(partition => Option(answers.get(javaTopicPartition(partition))).fold(-1L)(_.offset))

  override def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): F[(Long, Long)] =
    onConsumer(consumer): value =>
      val requested = List(javaTopicPartition(partition)).asJava
      val timeout   = Duration.ofMillis(timeoutMillis.toLong)
      (
        value.beginningOffsets(requested, timeout).get(requested.get(0)).longValue,
        value.endOffsets(requested, timeout).get(requested.get(0)).longValue
      )

  override def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): F[Vector[Long]] =
    onConsumer(consumer): value =>
      val requested = searches.map((partition, timestamp) => javaTopicPartition(partition) -> Long.box(timestamp.epochMillis)).toMap
      val answers   = value.offsetsForTimes(requested.asJava, Duration.ofMillis(timeoutMillis.toLong))
      searches.map((partition, _) => Option(answers.get(javaTopicPartition(partition))).fold(-1L)(_.offset))

  /** The Java client settles on a position before answering, so it has one whenever the partition is assigned. */
  override def positions(consumer: Client, partitions: Vector[TopicPartition]): F[Vector[Long]] =
    onConsumer(consumer)(value => partitions.map(partition => value.position(javaTopicPartition(partition))))

  override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): F[Unit] =
    onConsumer(consumer): value =>
      val target = javaTopicPartition(partition)
      offset match
        case KafkaDriver.BeginningOffset => value.seekToBeginning(List(target).asJava)
        case KafkaDriver.EndOffset       => value.seekToEnd(List(target).asJava)
        case _                           => value.seek(target, offset)

  override def metadata(client: Client, topic: Option[String], timeoutMillis: Int): F[Vector[(String, Vector[Int])]] =
    val timeout                                                                   = Duration.ofMillis(timeoutMillis.toLong)
    def partitions(values: java.util.List[org.apache.kafka.common.PartitionInfo]) =
      Option(values).fold(Vector.empty)(_.asScala.toVector.map(_.partition))
    client match
      case JavaClient.Consumer(_) => onConsumer(client): value =>
          topic.fold(value.listTopics(timeout).asScala.toVector.map((name, infos) => name -> partitions(infos)))(name =>
            Vector(name -> partitions(value.partitionsFor(name, timeout)))
          )
      case JavaClient.Producer(value) => topic match
          case Some(name) => javaCall(F.blocking(Vector(name -> partitions(value.partitionsFor(name)))))
          case None       => F.raiseError(new IllegalArgumentException("a producer looks up one topic at a time"))
      case JavaClient.Administration(value) =>
        val names =
          topic.fold(answered(value.listTopics(new ListTopicsOptions().timeoutMs(timeoutMillis)).names).map(_.asScala.toList))(name =>
            F.pure(List(name))
          )
        names.flatMap: requested =>
          answered(value.describeTopics(requested.asJava, new DescribeTopicsOptions().timeoutMs(timeoutMillis)).allTopicNames).map:
            _.asScala.toVector.map((name, description) => name -> description.partitions.asScala.toVector.map(_.partition))

  override def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): F[Unit] =
    administering(admin): value =>
      val requested =
        topics.map(topic => new JavaNewTopic(topic.topic.value, topic.partitions, topic.replicationFactor).configs(topic.configuration.asJava))
      answered(value.createTopics(requested.asJava, new CreateTopicsOptions().timeoutMs(timeoutMillis)).all).void

  override def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Unit] =
    administering(admin)(value =>
      answered(value.deleteTopics(topics.map(_.value).asJava, new DeleteTopicsOptions().timeoutMs(timeoutMillis)).all).void
    )

  override def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): F[Unit] =
    administering(admin): value =>
      answered(
        value.createPartitions(Map(topic.value -> JavaNewPartitions.increaseTo(count)).asJava, new CreatePartitionsOptions().timeoutMs(timeoutMillis))
          .all
      ).void

  /** The Java client retries a topic or partition the cluster does not have until the request times out, so the partitions are looked up first and
    * only those the cluster has are submitted, which is how librdkafka answers the same request.
    */
  override def deleteRecords(admin: Client, cuts: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Vector[Either[KafkaException, Offset]]] =
    administering(admin): value =>
      for
        held <- partitionsHeld(value, cuts.map(_._1.topic).distinct, timeoutMillis)
        submitted = cuts.filter((topicPartition, _) => held.get(topicPartition.topic).exists(_.exists(_.contains(topicPartition.partition))))
        answers <- if submitted.isEmpty then F.pure(Vector.empty) else delete(value, submitted, timeoutMillis)
      yield
        val deleted = answers.toMap
        cuts.map: (topicPartition, _) =>
          deleted
            .getOrElse(topicPartition, held.get(topicPartition.topic).flatMap(_.left.toOption).fold(Left(missingPartition(topicPartition)))(Left(_)))

  private def delete(
      admin: Admin,
      cuts: Vector[(TopicPartition, Offset)],
      timeoutMillis: Int
  ): F[Vector[(TopicPartition, Either[KafkaException, Offset])]] =
    val requested = cuts.map((topicPartition, offset) => javaTopicPartition(topicPartition) -> RecordsToDelete.beforeOffset(offset.value)).toMap
    F.delay(admin.deleteRecords(requested.asJava, new DeleteRecordsOptions().timeoutMs(timeoutMillis)).lowWatermarks.asScala.toMap).flatMap:
      answers =>
        eachAnswered(cuts.map((topicPartition, _) => topicPartition -> answers.get(javaTopicPartition(topicPartition))), _.show).flatMap:
          _.traverse:
            case (topicPartition, Right(deleted)) => F
                .fromEither(Offset.from(deleted.lowWatermark).leftMap(error => invalidBackendValue("low watermark", error)))
                .map(topicPartition -> Right(_))
            case (topicPartition, Left(failure)) => F.pure(topicPartition -> Left(failure))

  /** The partitions of each topic, or the reason the cluster would not describe the topic. */
  private def partitionsHeld(admin: Admin, topics: Vector[Topic], timeoutMillis: Int): F[Map[Topic, Either[KafkaException, Set[Partition]]]] =
    F.delay(admin.describeTopics(topics.map(_.value).asJava, new DescribeTopicsOptions().timeoutMs(timeoutMillis)).topicNameValues.asScala.toMap)
      .flatMap: answers =>
        eachAnswered(topics.map(topic => topic -> answers.get(topic.value)), _.value).map:
          _.map((topic, answer) => topic -> answer.map(_.partitions.asScala.flatMap(value => Partition.from(value.partition).toOption).toSet)).toMap

  private def missingPartition(topicPartition: TopicPartition): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(
      s"the cluster has no partition ${topicPartition.show}",
      Some(ErrorCode.UnknownTopicOrPartition),
      retriable = Some(ErrorCode.UnknownTopicOrPartition.retriable)
    )

  override def describeTopicConfigs(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicConfiguration]]] =
    def resource(topic: Topic) = new JavaConfigResource(JavaConfigResource.Type.TOPIC, topic.value)
    administering(admin): value =>
      F.delay(value.describeConfigs(topics.map(resource).asJava, new DescribeConfigsOptions().timeoutMs(timeoutMillis)).values.asScala.toMap).flatMap:
        answers =>
          eachAnswered(topics.map(topic => topic -> answers.get(resource(topic))), _.value)
            .map(_.map((_, answer) => answer.map(config => portableConfiguration(config.entries.asScala))))

  /** Waits for each entry's answer, keeping a refusal as that entry's own.
    *
    * The Java client fails every entry's answer where the request as a whole failed, such as when the cluster cannot be reached in time. Nothing is
    * then known about any entry, so the call fails with that failure, as librdkafka's does.
    */
  private def eachAnswered[K, A](entries: Vector[(K, Option[KafkaFuture[A]])], name: K => String): F[Vector[(K, Either[KafkaException, A])]] =
    entries.traverse:
      case (key, None)         => F.raiseError(new KafkaException.InvalidBackendResponse(s"no answer for ${name(key)}"))
      case (key, Some(answer)) => answered(answer).attempt.flatMap:
          case Right(value)                  => F.pure(key -> Right(value))
          case Left(failure: KafkaException) => F.pure(key -> Left(failure))
          case Left(other)                   => F.raiseError(other)
    .flatMap: answers =>
      val failures = answers.collect { case (_, Left(failure)) => failure }
      failures.headOption.filter(_ => failures.size == answers.size && failures.forall(unreached)).fold(F.pure(answers))(F.raiseError)

  /** A failure that says the request did not get an answer at all, which a refusal by the broker never is. */
  private def unreached(failure: KafkaException): Boolean =
    failure match
      case backendFailure: KafkaException.BackendFailure => backendFailure.code
          .exists(code => code == ErrorCode.RequestTimedOut || code == ErrorCode.NetworkException || code == ErrorCode.BrokerNotAvailable)
      case _ => false

  private def portableConfiguration(entries: Iterable[JavaConfigEntry]): TopicConfiguration =
    TopicConfiguration(entries.map(entry => entry.name -> JavaConfiguration.portableEntry(entry)).toMap)

  private def connection(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): Map[String, AnyRef] =
    properties ++ Map("bootstrap.servers" -> bootstrapServers) ++ clientId.map("client.id" -> _)

  private def javaCall[A](call: F[A]): F[A] = call.adaptError(JavaFailures.portable)

  private def answered[A](future: KafkaFuture[A]): F[A] = javaCall(F.fromCompletionStage(F.delay(future.toCompletionStage)))

  private def producing[A](client: Client)(use: JavaProducer[Array[Byte], Array[Byte]] => F[A]): F[A] =
    client match
      case JavaClient.Producer(value) => javaCall(use(value))
      case other                      => F.raiseError(new IllegalArgumentException(s"not a producer: $other"))

  private def administering[A](client: Client)(use: Admin => F[A]): F[A] =
    client match
      case JavaClient.Administration(value) => javaCall(use(value))
      case other                            => F.raiseError(new IllegalArgumentException(s"not an administration client: $other"))

  private def onThread[A](client: Client)(use: ConsumerThread => F[A]): F[A] =
    client match
      case JavaClient.Consumer(thread) => use(thread)
      case other                       => F.raiseError(new IllegalArgumentException(s"not a consumer: $other"))

  /** Makes a call on the consumer's own thread. */
  private def onConsumer[A](client: Client)(task: JavaConsumer[Array[Byte], Array[Byte]] => A): F[A] =
    onThread(client)(thread => javaCall(F.async_[A](thread.call(task))))

  private def javaTopicPartition(value: TopicPartition): JavaTopicPartition = new JavaTopicPartition(value.topic.value, value.partition.value)

  private def committedOffsets(offsets: Vector[(TopicPartition, Offset)]): java.util.Map[JavaTopicPartition, OffsetAndMetadata] =
    offsets.map((partition, offset) => javaTopicPartition(partition) -> new OffsetAndMetadata(offset.value)).toMap.asJava

  private def javaRecord(record: EncodedRecord): JavaProducerRecord[Array[Byte], Array[Byte]] =
    val headers: java.lang.Iterable[JavaHeader] =
      record.headers.values.map(header => new RecordHeader(header.key, header.value.map(_.toArray).orNull): JavaHeader).asJava
    new JavaProducerRecord(
      record.topic.value,
      record.partition.map(value => Int.box(value.value)).orNull,
      record.timestamp.map(value => Long.box(value.epochMillis)).orNull,
      record.key.map(_.toArray).orNull,
      record.value.map(_.toArray).orNull,
      headers
    )

  private def polled(record: ConsumerThread.Record): PolledRecord =
    PolledRecord(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      timestamp = Option.when(record.timestamp >= 0L)(record.timestamp),
      key = Option(record.key).map(Chunk.array),
      value = Option(record.value).map(Chunk.array),
      headers = Headers.fromVector(record.headers.toArray.toVector.map(header => Header(header.key, Option(header.value).map(Chunk.array))))
    )

  private def invalidBackendValue(field: String, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field: $error")
