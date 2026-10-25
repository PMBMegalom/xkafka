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

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.IO
import munit.CatsEffectSuite
import internal.driver.{Delivery, EncodedRecord, Enqueued, KafkaDriver, PolledRecord, Rebalance, StartAt, StoredOffset}

/** Every effect the client hands out is a description: building one asks nothing of the driver, so nothing reaches a broker until it runs. */
final class EffectLazinessSuite extends CatsEffectSuite:
  private val topic     = Topic.from("events").toOption.get
  private val partition = TopicPartition(topic, Partition.from(0).toOption.get)
  private val offset    = Offset.from(1L).toOption.get
  private val group     = ConsumerGroup.from("group").toOption.get
  private val utf8      = Serializer.utf8[IO]
  private val text      = Deserializer.utf8[IO]

  test("building any client effect makes no call to the driver"):
    val driver = new RecordingDriver
    val client = new DriverClient[IO](driver)
    val record = NonEmptyList.one(ProducerRecord(topic = topic, key = "key", value = "value", partition = Some(partition.partition)))

    val clients =
      for
        settings      <- IO(ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get).toResource
        producer      <- client.producer(ProducerSettings.from(settings, utf8, utf8).toOption.get)
        transactional <-
          client
            .transactionalProducer(TransactionalProducerSettings.from(settings, TransactionalId.from("writer").toOption.get, utf8, utf8).toOption.get)
        consumer <-
          client.consumer(
            ConsumerSettings.from(settings, ConsumerGroup.from("group").toOption.get, text, text).toOption.get,
            Selection.Partitions(NonEmptySet.one(partition))
          )
        admin <- client.admin(settings)
      yield (producer, transactional, consumer, admin)

    clients.use: (producer, transactional, consumer, admin) =>
      driver.whileBuilding:
        List(
          producer.produce(record),
          producer.produceAndAwait(record),
          producer.flush,
          producer.partitionsFor(topic),
          transactional.transactionally(_.produce(record)),
          consumer.assignment,
          consumer.stopConsuming,
          consumer.committed(Set(partition)),
          consumer.beginningOffsets(Set(partition)),
          consumer.endOffsets(Set(partition)),
          consumer.offsetsForTimes(Map(partition -> Timestamp.fromEpochMillis(0L))),
          consumer.partitionsFor(topic),
          consumer.listTopics,
          consumer.seek(partition, offset),
          consumer.seekToBeginning(Set(partition)),
          consumer.seekToEnd(Set(partition)),
          consumer.position(partition),
          consumer.records.compile.drain,
          consumer.partitionedRecords().compile.drain,
          consumer.assignmentChanges.compile.drain,
          admin.createTopics(NonEmptySet.one(NewTopic.from(topic, partitions = 1, replicationFactor = 1).toOption.get)),
          admin.deleteTopics(NonEmptySet.one(topic)),
          admin.createPartitions(topic, 2),
          admin.describeCluster,
          admin.describeTopics(NonEmptySet.one(topic)),
          admin.deleteRecords(NonEmptyMap.one(partition, offset)),
          admin.describeTopicConfigurations(NonEmptySet.one(topic)),
          admin.alterTopicConfigurations(NonEmptyMap.one(topic, NonEmptyMap.one("retention.ms", ConfigurationChange.Set("1000")))),
          admin.electPreferredLeaders(NonEmptySet.one(partition)),
          admin.listConsumerGroups,
          admin.describeConsumerGroups(NonEmptySet.one(group)),
          admin.listConsumerGroupOffsets(group),
          admin.listConsumerGroupOffsets(group, NonEmptySet.one(partition)),
          admin.alterConsumerGroupOffsets(group, NonEmptyMap.one(partition, OffsetAndMetadata(offset))),
          admin.deleteConsumerGroupOffsets(group, NonEmptySet.one(partition)),
          admin.deleteConsumerGroups(NonEmptySet.one(group))
        )
      .map(called => assertEquals(called, Vector.empty, "building the effects called the driver"))

  private final case class RecordedGroup(consumer: String) extends GroupHandle

  /** Answers every call with an effect that does nothing, and records the calls made while effects are being built.
    *
    * The building happens in one synchronous step on one thread, so the background work of the clients, which runs in fibers of its own, is not
    * mistaken for it.
    */
  private final class RecordingDriver extends KafkaDriver[IO]:
    type Client        = String
    type Batch         = Int
    type Event         = Int
    type GroupMetadata = RecordedGroup

    private val building =
      new ThreadLocal[Option[Vector[String]]]:
        override def initialValue(): Option[Vector[String]] = None

    def whileBuilding(build: => List[Any]): IO[Vector[String]] =
      IO.delay:
        building.set(Some(Vector.empty))
        try
          build: Unit
          building.get.getOrElse(Vector.empty)
        finally building.set(None)

    private def called[A](name: String)(result: IO[A]): IO[A] =
      building.get.foreach(calls => building.set(Some(calls :+ name)))
      result

    // Never answers, so whatever waits on the broker waits until it is cancelled.
    private def waiting[A](name: String): IO[A] = called(name)(IO.never)

    override val deliveriesShareQueue: Boolean = true

    override def producer(
        bootstrapServers: String,
        clientId: Option[String],
        properties: Map[String, String],
        enqueueTimeoutMillis: Int
    ): IO[Client]                                                                = called("producer")(IO.pure("producer"))
    override def destroyProducer(producer: Client, timeoutMillis: Int): IO[Unit] = called("destroyProducer")(IO.unit)
    override def consumer(
        bootstrapServers: String,
        clientId: Option[String],
        groupId: String,
        autoOffsetReset: String,
        properties: Map[String, String]
    ): IO[Client]                                            = called("consumer")(IO.pure("consumer"))
    override def destroyConsumer(consumer: Client): IO[Unit] = called("destroyConsumer")(IO.unit)
    override def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): IO[Client] =
      called("admin")(IO.pure("admin"))
    override def destroyAdmin(admin: Client): IO[Unit]                                                       = called("destroyAdmin")(IO.unit)
    override def batch(capacity: Int): IO[Batch]                                                             = called("batch")(IO.pure(capacity))
    override def freeBatch(batch: Batch): IO[Unit]                                                           = called("freeBatch")(IO.unit)
    override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): IO[Enqueued] =
      called("enqueue")(IO.pure(Enqueued(records.size, gaveUp = false)))
    override def serveDeliveries(producer: Client, timeoutMillis: Int): IO[Unit] = called("serveDeliveries")(IO.sleep(timeoutMillis.millis))
    override def flush(producer: Client, timeoutMillis: Int): IO[Unit]           = called("flush")(IO.sleep(timeoutMillis.millis))
    override def awaitDeliveries(batch: Batch, timeoutMillis: Int): IO[Int]      = called("awaitDeliveries")(IO.sleep(timeoutMillis.millis).as(1))
    override def pending(batch: Batch): IO[Int]                                  = called("pending")(IO.pure(1))
    override def deliveries(producer: Client, batch: Batch): IO[Vector[Either[KafkaException, Delivery]]] = waiting("deliveries")
    override def initTransactions(producer: Client, timeoutMillis: Int): IO[Unit]                         = called("initTransactions")(IO.unit)
    override def beginTransaction(producer: Client): IO[Unit]                                             = called("beginTransaction")(IO.unit)
    override def commitTransaction(producer: Client, timeoutMillis: Int): IO[Unit]                        = called("commitTransaction")(IO.unit)
    override def abortTransaction(producer: Client, timeoutMillis: Int): IO[Unit]                         = called("abortTransaction")(IO.unit)
    override def sendOffsets(
        producer: Client,
        metadata: GroupMetadata,
        offsets: Vector[(TopicPartition, OffsetAndMetadata)],
        timeoutMillis: Int
    ): IO[Unit]                                                     = called("sendOffsets")(IO.unit)
    override def groupMetadata(consumer: Client): IO[GroupMetadata] = called("groupMetadata")(IO.pure(RecordedGroup(consumer)))
    override def groupMetadataFor(group: ConsumerGroup, generation: Int, memberId: String): IO[GroupMetadata] =
      called("groupMetadataFor")(IO.pure(RecordedGroup(memberId)))
    override def destroyGroupMetadata(metadata: GroupMetadata): IO[Unit]     = called("destroyGroupMetadata")(IO.unit)
    override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
      handle match
        case group: RecordedGroup => Some(group)
        case _                    => None
    override def assign(consumer: Client, partitions: Vector[TopicPartition], starts: Vector[StartAt]): IO[Unit] = called("assign")(IO.unit)
    override def subscribe(consumer: Client, topics: NonEmptyList[String]): IO[Unit]                             = called("subscribe")(IO.unit)
    override def poll(consumer: Client, timeoutMillis: Int): IO[List[PolledRecord]] = called("poll")(IO.sleep(timeoutMillis.millis).as(Nil))
    override def takeRebalances(consumer: Client): IO[List[Rebalance[Event]]]       = called("takeRebalances")(IO.pure(Nil))
    override def applyRebalance(consumer: Client, event: Event, starts: Vector[StartAt]): IO[Unit] = called("applyRebalance")(IO.unit)
    override def cooperative(consumer: Client): IO[Boolean]                                        = called("cooperative")(IO.pure(true))
    override def assignment(consumer: Client): IO[Vector[(String, Int)]]                           = called("assignment")(IO.pure(Vector.empty))
    override def commit(consumer: Client, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): IO[Unit] =
      called("commit")(IO.unit)
    override def pause(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit]  = called("pause")(IO.unit)
    override def resume(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit] = called("resume")(IO.unit)
    // A consumer naming its partitions reads their committed offsets as it is acquired, so this answers that none are committed.
    override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): IO[Vector[StoredOffset]] =
      called("committed")(IO.pure(partitions.map(_ => StoredOffset(-1L, -1, ""))))
    override def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): IO[(Long, Long)] = waiting("watermarks")
    override def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): IO[Vector[Long]] =
      waiting("offsetsForTimes")
    override def positions(consumer: Client, partitions: Vector[TopicPartition]): IO[Vector[Long]]                      = waiting("positions")
    override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): IO[Unit]          = waiting("seek")
    override def metadata(client: Client, topic: Option[String], timeoutMillis: Int): IO[Vector[(String, Vector[Int])]] = waiting("metadata")
    override def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): IO[Unit]                    = waiting("createTopics")
    override def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): IO[Unit]                       = waiting("deleteTopics")
    override def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): IO[Unit]                = waiting("createPartitions")
    override def deleteRecords(
        admin: Client,
        cuts: Vector[(TopicPartition, Offset)],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Offset]]]                                           = waiting("deleteRecords")
    override def describeCluster(admin: Client, timeoutMillis: Int): IO[ClusterDescription] = waiting("describeCluster")

    override def describeTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): IO[Vector[Either[KafkaException, TopicDescription]]] =
      waiting("describeTopics")

    override def describeTopicConfigs(
        admin: Client,
        topics: Vector[Topic],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, TopicConfiguration]]] = waiting("describeTopicConfigs")

    override def alterTopicConfigs(
        admin: Client,
        changes: Vector[(Topic, NonEmptyList[(String, ConfigurationChange)])],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Unit]]] = waiting("alterTopicConfigs")

    override def electPreferredLeaders(
        admin: Client,
        partitions: Vector[TopicPartition],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Unit]]] = waiting("electPreferredLeaders")

    override def listGroups(admin: Client, timeoutMillis: Int): IO[Vector[(ConsumerGroup, ConsumerGroupState)]] = waiting("listGroups")

    override def describeGroups(
        admin: Client,
        groups: Vector[ConsumerGroup],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, ConsumerGroupDescription]]] = waiting("describeGroups")

    override def groupOffsets(
        admin: Client,
        group: ConsumerGroup,
        partitions: Option[Vector[TopicPartition]],
        timeoutMillis: Int
    ): IO[Vector[(TopicPartition, StoredOffset)]] = waiting("groupOffsets")

    override def alterGroupOffsets(
        admin: Client,
        group: ConsumerGroup,
        offsets: Vector[(TopicPartition, OffsetAndMetadata)],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Unit]]] = waiting("alterGroupOffsets")

    override def deleteGroupOffsets(
        admin: Client,
        group: ConsumerGroup,
        partitions: Vector[TopicPartition],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Unit]]] = waiting("deleteGroupOffsets")

    override def deleteGroups(admin: Client, groups: Vector[ConsumerGroup], timeoutMillis: Int): IO[Vector[Either[KafkaException, Unit]]] =
      waiting("deleteGroups")
