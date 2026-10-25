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
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}
import java.util.regex.Pattern

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import org.apache.kafka.clients.admin.{
  Admin, AlterConfigOp, AlterConfigsOptions, AlterConsumerGroupOffsetsOptions, ConfigEntry as JavaConfigEntry,
  ConsumerGroupDescription as JavaConsumerGroupDescription, CreatePartitionsOptions, CreateTopicsOptions, DeleteConsumerGroupOffsetsOptions,
  DeleteConsumerGroupsOptions, DeleteRecordsOptions, DeleteTopicsOptions, DescribeClusterOptions, DescribeConfigsOptions,
  DescribeConsumerGroupsOptions, DescribeTopicsOptions, ElectLeadersOptions, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec,
  ListGroupsOptions, ListTopicsOptions, NewPartitions as JavaNewPartitions, NewTopic as JavaNewTopic, RecordsToDelete,
  TopicDescription as JavaTopicDescription
}
import org.apache.kafka.clients.consumer.{
  Consumer as JavaConsumer, ConsumerGroupMetadata as JavaConsumerGroupMetadata, KafkaConsumer as JavaKafkaConsumer,
  OffsetAndMetadata as JavaOffsetAndMetadata
}
import org.apache.kafka.clients.producer.{
  Callback, KafkaProducer as JavaKafkaProducer, Producer as JavaProducer, ProducerRecord as JavaProducerRecord, RecordMetadata as JavaRecordMetadata
}
import org.apache.kafka.common.{
  ElectionType, GroupState, KafkaException as JavaKafkaException, KafkaFuture, Node, TopicPartition as JavaTopicPartition
}
import org.apache.kafka.common.config.ConfigResource as JavaConfigResource
import org.apache.kafka.common.errors.{ElectionNotNeededException, TimeoutException as JavaTimeoutException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import driver.{Delivery, EncodedRecord, Enqueued, KafkaDriver, PolledRecord, Rebalance, StartAt, StoredOffset}

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

  override val deliveriesShareQueue: Boolean = false

  /** The Java client waits for room, and for a topic's metadata, up to its `max.block.ms`, which is the enqueue timeout. */
  override def producer(bootstrapServers: String, clientId: Option[String], properties: Map[String, String], enqueueTimeoutMillis: Int): F[Client] =
    val configured = connection(bootstrapServers, clientId, properties) + ("max.block.ms" -> enqueueTimeoutMillis.toString)
    javaCall(F.blocking(JavaClient.Producer(clients.producer(configured))))

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

  /** The Java client waits for room itself, so this takes every record unless the client gives up on one. A record it gives up on is reported as
    * timed out before its send returns, and the batch ends there. Interrupting the wait leaves the records before it enqueued.
    *
    * A record for a partition its topic does not have is answered at once from the client's metadata, as librdkafka answers it, where the Java client
    * would wait for the partition to appear.
    */
  override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): F[Enqueued] =
    producing(producer): value =>
      F.interruptible:
        def sent(record: EncodedRecord, partitions: Map[String, Int]): (Boolean, Map[String, Int]) =
          val index = batch.enqueued.get
          batch.outstanding.incrementAndGet()
          def answered(failure: Throwable) =
            batch.report(index, Left(failure))
            batch.enqueued.incrementAndGet(): Unit
          try
            val topic   = record.topic.value
            val counted = record.partition.fold(partitions)(_ => partitions.updatedWith(topic)(_.orElse(Some(value.partitionsFor(topic).size))))
            record.partition.filter(partition => counted.get(topic).exists(partition.value >= _)) match
              case Some(partition) =>
                answered(new UnknownTopicOrPartitionException(s"partition ${partition.value} of topic $topic is not present in metadata"))
                (false, counted)
              case None =>
                val callback: Callback = (metadata, error) => batch.report(index, Option(error).toLeft(metadata))
                value.send(javaRecord(record), callback): Unit
                batch.enqueued.incrementAndGet()
                // A report this early is the client giving up before the record was sent: its delivery cannot have timed out yet.
                (Option(batch.reports.get(index)).exists(_.left.exists(_.isInstanceOf[JavaTimeoutException])), counted)
          catch
            // Looking up the partitions waits as a send does, and gives up the same way.
            case timedOut: JavaTimeoutException =>
              answered(timedOut)
              (true, partitions)
            case error: Throwable =>
              batch.outstanding.decrementAndGet()
              throw error
        def from(rest: List[EncodedRecord], partitions: Map[String, Int], taken: Int): Enqueued =
          rest match
            case Nil            => Enqueued(taken, gaveUp = false)
            case record :: next =>
              val (gaveUp, counted) = sent(record, partitions)
              if gaveUp then Enqueued(taken + 1, gaveUp = true) else from(next, counted, taken + 1)
        try from(records.toList, Map.empty, 0)
        finally batch.seal()

  /** The Java client reports each record on its own thread, so there is no queue to serve. */
  override def serveDeliveries(producer: Client, timeoutMillis: Int): F[Unit] = F.unit

  /** Java waits only for the records sent before the call, so this returns once they are done, and cancelling it interrupts the wait. */
  override def flush(producer: Client, timeoutMillis: Int): F[Unit] = producing(producer)(value => F.interruptible(value.flush()))

  override def awaitDeliveries(batch: Batch, timeoutMillis: Int): F[Int] =
    // The batch's own future is shared by every wait, so a wait that times out gives up on a copy of it.
    F.fromCompletableFuture(F.delay(batch.delivered.thenApply(identity)))
      .timeoutTo(scala.concurrent.duration.FiniteDuration(timeoutMillis.toLong, "ms"), F.unit) *> pending(batch)

  override def pending(batch: Batch): F[Int] = F.delay(batch.outstanding.get)

  override def deliveries(producer: Client, batch: Batch): F[Vector[Either[KafkaException, Delivery]]] =
    F.delay:
      Vector.tabulate(batch.enqueued.get)(batch.reports.get).map:
        case Right(metadata) => Right(Delivery(
            metadata.partition,
            if metadata.hasOffset then metadata.offset else -1L,
            if metadata.hasTimestamp then metadata.timestamp else -1L
          ))
        case Left(error) => Left(JavaFailures.delivery(error))

  override def initTransactions(producer: Client, timeoutMillis: Int): F[Unit] =
    producing(producer)(value => withinTimeout(timeoutMillis)(value.initTransactions()))

  override def beginTransaction(producer: Client): F[Unit] = producing(producer)(value => F.blocking(value.beginTransaction()))

  override def commitTransaction(producer: Client, timeoutMillis: Int): F[Unit] =
    producing(producer)(value => withinTimeout(timeoutMillis)(value.commitTransaction()))

  override def abortTransaction(producer: Client, timeoutMillis: Int): F[Unit] =
    producing(producer)(value => withinTimeout(timeoutMillis)(value.abortTransaction()))

  /** The Java client bounds a transaction call by its `max.block.ms`, the enqueue timeout, and a call that timed out or was interrupted is safe to
    * make again. So it is made again until `timeoutMillis` has passed, when the one still waiting is interrupted, as librdkafka's call gives up then.
    */
  private def withinTimeout(timeoutMillis: Int)(call: => Unit): F[Unit] =
    F.monotonic.flatMap: started =>
      val deadline         = started + timeoutMillis.millis
      def attempt: F[Unit] =
        F.monotonic.flatMap: now =>
          val timedOut = new JavaTimeoutException(s"the transaction call did not complete within $timeoutMillis ms")
          F.interruptible(call).timeoutTo(deadline - now, F.raiseError(timedOut)).recoverWith:
            case _: JavaTimeoutException => F.monotonic.flatMap(now => if now < deadline then attempt else F.raiseError(timedOut))
      attempt

  override def sendOffsets(
      producer: Client,
      metadata: GroupMetadata,
      offsets: Vector[(TopicPartition, OffsetAndMetadata)],
      timeoutMillis: Int
  ): F[Unit] = producing(producer)(value => F.blocking(value.sendOffsetsToTransaction(committedOffsets(offsets), metadata.metadata)))

  override def groupMetadata(consumer: Client): F[GroupMetadata] = onConsumer(consumer)(value => JavaGroupMetadata(value.groupMetadata()))

  /** The constructor is deprecated for removal in Kafka 5.0, which makes the class an interface (KIP-1136); the streams engine then implements it. */
  @annotation.nowarn("cat=deprecation")
  override def groupMetadataFor(group: ConsumerGroup, generation: Int, memberId: String): F[GroupMetadata] =
    F.delay(JavaGroupMetadata(new JavaConsumerGroupMetadata(group.value, generation, memberId, Optional.empty)))

  override def destroyGroupMetadata(metadata: GroupMetadata): F[Unit] = F.unit

  override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
    handle match
      case metadata: JavaGroupMetadata => Some(metadata)
      case _                           => None

  override def assign(consumer: Client, partitions: Vector[TopicPartition], starts: Vector[StartAt]): F[Unit] =
    onConsumer(consumer): value =>
      value.assign(partitions.map(javaTopicPartition).asJava)
      startAt(value, starts)

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

  /** Inside the listener, which an assignment is still waiting in, a seek decides where the partition starts. */
  override def applyRebalance(consumer: Client, event: Event, starts: Vector[StartAt]): F[Unit] =
    onThread(consumer)(thread => F.async_[Unit](thread.apply(event, startAt(_, starts))))

  private def startAt(consumer: JavaConsumer[Array[Byte], Array[Byte]], starts: Vector[StartAt]): Unit =
    starts.foreach: start =>
      val partition = javaTopicPartition(start.partition)
      start.offset match
        case KafkaDriver.BeginningOffset => consumer.seekToBeginning(List(partition).asJava)
        case KafkaDriver.EndOffset       => consumer.seekToEnd(List(partition).asJava)
        case offset                      =>
          val epoch = if start.leaderEpoch < 0 then Optional.empty[Integer] else Optional.of(Integer.valueOf(start.leaderEpoch))
          consumer.seek(partition, new JavaOffsetAndMetadata(offset, epoch, ""))

  /** The listener hears a revocation before every assignment, whichever protocol the group uses, so an assignment only ever adds partitions. */
  override def cooperative(consumer: Client): F[Boolean] = F.pure(true)

  override def assignment(consumer: Client): F[Vector[(String, Int)]] =
    onConsumer(consumer)(_.assignment.asScala.toVector.map(value => value.topic -> value.partition))

  override def commit(consumer: Client, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): F[Unit] =
    onConsumer(consumer)(_.commitSync(committedOffsets(offsets), Duration.ofMillis(timeoutMillis.toLong)))

  override def pause(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    onConsumer(consumer)(_.pause(partitions.map(javaTopicPartition).asJava))

  override def resume(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    onConsumer(consumer)(_.resume(partitions.map(javaTopicPartition).asJava))

  override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[StoredOffset]] =
    onConsumer(consumer): value =>
      val answers = value.committed(partitions.map(javaTopicPartition).toSet.asJava, Duration.ofMillis(timeoutMillis.toLong))
      partitions.map: partition =>
        Option(answers.get(javaTopicPartition(partition))).fold(StoredOffset(-1L, -1, "")): stored =>
          StoredOffset(stored.offset, stored.leaderEpoch.map(_.intValue).orElse(-1), Option(stored.metadata).getOrElse(""))

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

  /** The Java consumer refuses a partition it is not fetching with an `IllegalStateException`, where librdkafka answers that the partition is
    * unknown, so the same answer is given here.
    */
  override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): F[Unit] =
    val target = javaTopicPartition(partition)
    onThread(consumer): thread =>
      javaCall(F.async_[Unit](thread.seek(target)(value =>
        if !value.assignment.contains(target) then throw new UnknownTopicOrPartitionException(s"No current assignment for partition $target")
        offset match
          case KafkaDriver.BeginningOffset => value.seekToBeginning(List(target).asJava)
          case KafkaDriver.EndOffset       => value.seekToEnd(List(target).asJava)
          case _                           => value.seek(target, offset)
      )))

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

  override def describeCluster(admin: Client, timeoutMillis: Int): F[ClusterDescription] =
    administering(admin): value =>
      F.delay(value.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMillis))).flatMap: result =>
        (answered(result.clusterId), answered(result.controller), answered(result.nodes)).tupled.flatMap: (id, controller, nodes) =>
          F.delay:
            ClusterDescription(
              Option(id).getOrElse(throw new KafkaException.InvalidBackendResponse("the cluster reported no id")),
              Option(controller).filterNot(_.isEmpty).map(portableBroker),
              NonEmptyList.fromList(nodes.asScala.toList.map(portableBroker)).map(_.toNes)
                .getOrElse(throw new KafkaException.InvalidBackendResponse("the cluster reported no brokers"))
            )

  override def describeTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicDescription]]] =
    administering(admin): value =>
      F.delay(value.describeTopics(topics.map(_.value).asJava, new DescribeTopicsOptions().timeoutMs(timeoutMillis)).topicNameValues.asScala.toMap)
        .flatMap(answers => eachAnswered(topics.map(topic => topic -> answers.get(topic.value)), _.value))
        .flatMap(_.traverse((_, answer) => answer.traverse(description => F.delay(portableTopic(description)))))

  private def portableTopic(description: JavaTopicDescription): TopicDescription =
    val partitions =
      description.partitions.asScala.toList.map: info =>
        val partition = Partition.from(info.partition).fold(error => throw invalidBackendValue("partition", error), identity)
        val replicas  =
          NonEmptyList.fromList(info.replicas.asScala.toList.map(brokerId))
            .getOrElse(throw new KafkaException.InvalidBackendResponse(s"partition ${info.partition} of ${description.name} has no replicas"))
        partition -> PartitionDescription(Option(info.leader).filterNot(_.isEmpty).map(brokerId), replicas, info.isr.asScala.toList.map(brokerId))
    TopicDescription(
      TopicId(description.topicId.toString),
      description.isInternal,
      NonEmptyList.fromList(partitions).map(_.toNem)
        .getOrElse(throw new KafkaException.InvalidBackendResponse(s"topic ${description.name} has no partitions"))
    )

  private def brokerId(node: Node): BrokerId = BrokerId.from(node.id).fold(error => throw invalidBackendValue("broker id", error), identity)

  private def portableBroker(node: Node): Broker = Broker(brokerId(node), node.host, node.port, Option.when(node.hasRack)(node.rack))

  override def describeTopicConfigs(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicConfiguration]]] =
    def resource(topic: Topic) = new JavaConfigResource(JavaConfigResource.Type.TOPIC, topic.value)
    administering(admin): value =>
      F.delay(value.describeConfigs(topics.map(resource).asJava, new DescribeConfigsOptions().timeoutMs(timeoutMillis)).values.asScala.toMap).flatMap:
        answers =>
          eachAnswered(topics.map(topic => topic -> answers.get(resource(topic))), _.value)
            .map(_.map((_, answer) => answer.map(config => portableConfiguration(config.entries.asScala))))

  override def alterTopicConfigs(
      admin: Client,
      changes: Vector[(Topic, NonEmptyList[(String, ConfigurationChange)])],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, Unit]]] =
    def resource(topic: Topic)                               = new JavaConfigResource(JavaConfigResource.Type.TOPIC, topic.value)
    def operation(name: String, change: ConfigurationChange) =
      change match
        case ConfigurationChange.Set(value)      => new AlterConfigOp(new JavaConfigEntry(name, value), AlterConfigOp.OpType.SET)
        case ConfigurationChange.Unset           => new AlterConfigOp(new JavaConfigEntry(name, null), AlterConfigOp.OpType.DELETE)
        case ConfigurationChange.Append(value)   => new AlterConfigOp(new JavaConfigEntry(name, value), AlterConfigOp.OpType.APPEND)
        case ConfigurationChange.Subtract(value) => new AlterConfigOp(new JavaConfigEntry(name, value), AlterConfigOp.OpType.SUBTRACT)
    val requested = changes.map((topic, properties) => resource(topic) -> properties.toList.map(operation).asJavaCollection).toMap.asJava
    administering(admin): value =>
      F.delay(value.incrementalAlterConfigs(requested, new AlterConfigsOptions().timeoutMs(timeoutMillis)).values.asScala.toMap).flatMap: answers =>
        eachAnswered(changes.map((topic, _) => topic -> answers.get(resource(topic))), _.value).map(_.map((_, answer) => answer.void))

  /** A partition already led by its preferred replica is answered with `ElectionNotNeededException`, which says what was asked for holds. */
  override def electPreferredLeaders(admin: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[Either[KafkaException, Unit]]] =
    administering(admin): value =>
      val requested = partitions.map(javaTopicPartition)
      answered(value.electLeaders(ElectionType.PREFERRED, requested.toSet.asJava, new ElectLeadersOptions().timeoutMs(timeoutMillis)).partitions)
        .flatMap: results =>
          partitions.zip(requested).traverse: (partition, java) =>
            Option(results.get(java)).map(_.toScala) match
              case None => F.raiseError(new KafkaException.InvalidBackendResponse(s"no answer for partition ${partition.show}"))
              case Some(None | Some(_: ElectionNotNeededException)) => F.pure(Right(()))
              case Some(Some(failure))                              => F.pure(Left(portableFailure(failure)))
          .flatMap(wholeUnlessAllUnreached)

  /** Lists the groups of the two kinds a consumer joins, which is what librdkafka lists. */
  override def listGroups(admin: Client, timeoutMillis: Int): F[Vector[(ConsumerGroup, ConsumerGroupState)]] =
    administering(admin): value =>
      javaCall(
        F.fromCompletionStage(F.delay(value.listGroups(ListGroupsOptions.forConsumerGroups().timeoutMs(timeoutMillis)).all.toCompletionStage))
          .adaptError(withoutWrapper)
      ).map:
        _.asScala.toVector.flatMap: listing =>
          ConsumerGroup.from(listing.groupId).toOption.map(_ -> listing.groupState.map(portableState).orElse(ConsumerGroupState.Unknown))

  override def describeGroups(
      admin: Client,
      groups: Vector[ConsumerGroup],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, ConsumerGroupDescription]]] =
    administering(admin): value =>
      F.delay(value.describeConsumerGroups(groups.map(_.value).asJava, new DescribeConsumerGroupsOptions().timeoutMs(timeoutMillis)).describedGroups)
        .flatMap(answers => eachAnswered(groups.map(group => group -> Option(answers.get(group.value))), _.value))
        .flatMap(_.traverse((_, answer) => answer.traverse(description => F.delay(portableDescription(description)))))

  override def groupOffsets(
      admin: Client,
      group: ConsumerGroup,
      partitions: Option[Vector[TopicPartition]],
      timeoutMillis: Int
  ): F[Vector[(TopicPartition, StoredOffset)]] =
    val spec =
      partitions
        .fold(new ListConsumerGroupOffsetsSpec)(named => new ListConsumerGroupOffsetsSpec().topicPartitions(named.map(javaTopicPartition).asJava))
    administering(admin): value =>
      answered(
        value.listConsumerGroupOffsets(Map(group.value -> spec).asJava, new ListConsumerGroupOffsetsOptions().timeoutMs(timeoutMillis))
          .partitionsToOffsetAndMetadata(group.value)
      ).flatMap: answers =>
        F.delay:
          answers.asScala.toVector.map: (partition, stored) =>
            portableTopicPartition(partition) -> Option(stored).fold(StoredOffset(-1L, -1, "")): value =>
              StoredOffset(value.offset, value.leaderEpoch.map(_.intValue).orElse(-1), Option(value.metadata).getOrElse(""))

  override def alterGroupOffsets(
      admin: Client,
      group: ConsumerGroup,
      offsets: Vector[(TopicPartition, OffsetAndMetadata)],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, Unit]]] =
    administering(admin): value =>
      F.delay(
        value.alterConsumerGroupOffsets(group.value, committedOffsets(offsets), new AlterConsumerGroupOffsetsOptions().timeoutMs(timeoutMillis))
      ).flatMap(result =>
        eachAnswered(offsets.map((partition, _) => partition -> Some(result.partitionResult(javaTopicPartition(partition)))), _.show)
      ).map(_.map((_, answer) => answer.void))

  override def deleteGroupOffsets(
      admin: Client,
      group: ConsumerGroup,
      partitions: Vector[TopicPartition],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, Unit]]] =
    administering(admin): value =>
      F.delay(value.deleteConsumerGroupOffsets(
        group.value,
        partitions.map(javaTopicPartition).toSet.asJava,
        new DeleteConsumerGroupOffsetsOptions().timeoutMs(timeoutMillis)
      ))
      .flatMap(result => eachAnswered(partitions.map(partition => partition -> Some(result.partitionResult(javaTopicPartition(partition)))), _.show))
      .map(_.map((_, answer) => answer.void))

  override def deleteGroups(admin: Client, groups: Vector[ConsumerGroup], timeoutMillis: Int): F[Vector[Either[KafkaException, Unit]]] =
    administering(admin): value =>
      F.delay(value.deleteConsumerGroups(groups.map(_.value).asJava, new DeleteConsumerGroupsOptions().timeoutMs(timeoutMillis)).deletedGroups)
        .flatMap(answers => eachAnswered(groups.map(group => group -> Option(answers.get(group.value))), _.value))
        .map(_.map((_, answer) => answer.void))

  /** Java reports three further states for groups on the consumer rebalance protocol, which librdkafka does not, so they have no portable meaning. */
  private def portableState(state: GroupState): ConsumerGroupState =
    state match
      case GroupState.STABLE               => ConsumerGroupState.Stable
      case GroupState.PREPARING_REBALANCE  => ConsumerGroupState.PreparingRebalance
      case GroupState.COMPLETING_REBALANCE => ConsumerGroupState.CompletingRebalance
      case GroupState.EMPTY                => ConsumerGroupState.Empty
      case _                               => ConsumerGroupState.Unknown

  /** Throws where the cluster reports a topic or partition that cannot be named, so call it inside an effect. */
  private def portableDescription(description: JavaConsumerGroupDescription): ConsumerGroupDescription =
    ConsumerGroupDescription(
      Option(description.groupState).fold(ConsumerGroupState.Unknown)(portableState),
      description.isSimpleConsumerGroup,
      Option(description.partitionAssignor).filter(_.nonEmpty),
      Option(description.coordinator).filterNot(_.isEmpty).map(portableBroker),
      description.members.asScala.toList.map: member =>
        GroupMember(
          member.consumerId,
          member.groupInstanceId.toScala,
          member.clientId,
          member.host,
          member.assignment.topicPartitions.asScala.map(portableTopicPartition).toSet
        )
    )

  /** Throws where the cluster reports a topic or partition that cannot be named, so call it inside an effect. */
  private def portableTopicPartition(value: JavaTopicPartition): TopicPartition =
    TopicPartition(
      Topic.from(value.topic).fold(error => throw invalidBackendValue("topic", error), identity),
      Partition.from(value.partition).fold(error => throw invalidBackendValue("partition", error), identity)
    )

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
    .flatMap(answers => wholeUnlessAllUnreached(answers.map(_._2)).as(answers))

  /** The answers, unless every one says the request got none, which fails the call as a whole. */
  private def wholeUnlessAllUnreached[A](answers: Vector[Either[KafkaException, A]]): F[Vector[Either[KafkaException, A]]] =
    val failures = answers.collect { case Left(failure) => failure }
    failures.headOption.filter(_ => failures.size == answers.size && failures.forall(unreached)).fold(F.pure(answers))(F.raiseError)

  private def portableFailure(failure: Throwable): KafkaException =
    JavaFailures.portable.applyOrElse(failure, other => new KafkaException.BackendFailure(other.toString, cause = other)) match
      case portable: KafkaException => portable
      case other                    => new KafkaException.BackendFailure(other.toString, cause = other)

  /** A failure that says the request did not get an answer at all, which a refusal by the broker never is. */
  /** Listing groups wraps the reason it found no broker in a plain Java client exception, which classifies as nothing. */
  private val withoutWrapper: PartialFunction[Throwable, Throwable] = {
    case error: JavaKafkaException if error.getClass == classOf[JavaKafkaException] && error.getCause.isInstanceOf[JavaKafkaException] =>
      error.getCause
  }

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

  /** Takes the request by name and makes it only when the effect runs, so each run sends a request of its own. */
  private def answered[A](future: => KafkaFuture[A]): F[A] = javaCall(F.fromCompletionStage(F.delay(future.toCompletionStage)))

  private def producing[A](client: Client)(use: JavaProducer[Array[Byte], Array[Byte]] => F[A]): F[A] =
    client match
      case JavaClient.Producer(value) => javaCall(F.defer(use(value)))
      case other                      => F.raiseError(new IllegalArgumentException(s"not a producer: $other"))

  private def administering[A](client: Client)(use: Admin => F[A]): F[A] =
    client match
      case JavaClient.Administration(value) => javaCall(F.defer(use(value)))
      case other                            => F.raiseError(new IllegalArgumentException(s"not an administration client: $other"))

  private def onThread[A](client: Client)(use: ConsumerThread => F[A]): F[A] =
    client match
      case JavaClient.Consumer(thread) => F.defer(use(thread))
      case other                       => F.raiseError(new IllegalArgumentException(s"not a consumer: $other"))

  /** Makes a call on the consumer's own thread. */
  private def onConsumer[A](client: Client)(task: JavaConsumer[Array[Byte], Array[Byte]] => A): F[A] =
    onThread(client)(thread => javaCall(F.async_[A](thread.call(task))))

  private def javaTopicPartition(value: TopicPartition): JavaTopicPartition = new JavaTopicPartition(value.topic.value, value.partition.value)

  private def committedOffsets(offsets: Vector[(TopicPartition, OffsetAndMetadata)]): java.util.Map[JavaTopicPartition, JavaOffsetAndMetadata] =
    offsets.map: (partition, offset) =>
      val epoch = offset.leaderEpoch.fold(java.util.Optional.empty[Integer])(value => java.util.Optional.of(Int.box(value.value)))
      javaTopicPartition(partition) -> new JavaOffsetAndMetadata(offset.offset.value, epoch, offset.metadata)
    .toMap.asJava

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
      timestampType = timestampKind(record.timestampType),
      leaderEpoch = record.leaderEpoch.map(_.intValue).orElse(-1),
      key = Option(record.key).map(Chunk.array),
      value = Option(record.value).map(Chunk.array),
      headers = Headers.fromVector(record.headers.toArray.toVector.map(header => Header(header.key, Option(header.value).map(Chunk.array))))
    )

  /** Numbered as librdkafka numbers them, which is what the shared client reads. */
  private def timestampKind(value: TimestampType): Int =
    value match
      case TimestampType.CREATE_TIME     => 1
      case TimestampType.LOG_APPEND_TIME => 2
      case _                             => 0

  private def invalidBackendValue(field: String, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field: $error")
