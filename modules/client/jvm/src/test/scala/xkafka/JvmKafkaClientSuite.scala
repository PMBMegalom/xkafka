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

import java.nio.charset.StandardCharsets
import java.util.{List as JavaList, Map as JavaMap}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import internal.kafkaclients.{JavaClients, JavaGroupMetadata}
import munit.CatsEffectSuite
import org.apache.kafka.clients.admin.{Admin, ConfigEntry as JavaConfigEntry}
import org.apache.kafka.clients.consumer.{
  CommitFailedException, ConsumerGroupMetadata, ConsumerRecord as JavaConsumerRecord, MockConsumer, NoOffsetForPartitionException, OffsetAndMetadata,
  OffsetAndTimestamp
}
import org.apache.kafka.clients.producer.{MockProducer, Partitioner}
import org.apache.kafka.common.{PartitionInfo, TopicPartition as JavaTopicPartition}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.serialization.ByteArraySerializer

final class JvmKafkaClientSuite extends CatsEffectSuite:
  // Checking that no failed poll is lost takes 80,000 consumers, which a CI runner needs more than munit's default 30 seconds for.
  override val munitIOTimeout: Duration = 3.minutes

  test("reads Java configuration entries, withholding sensitive values and naming their sources"):
    def entry(value: String | Null, source: JavaConfigEntry.ConfigSource, sensitive: Boolean = false, readOnly: Boolean = false) =
      JavaConfiguration
        .portableEntry(new JavaConfigEntry("name", value, source, sensitive, readOnly, JavaList.of(), JavaConfigEntry.ConfigType.STRING, null))
    assertEquals(
      entry("delete", JavaConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG),
      ConfigurationEntry(ConfigurationValue.Present("delete"), ConfigurationSource.TopicOverride, readOnly = false)
    )
    assertEquals(entry(null, JavaConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, sensitive = true).value, ConfigurationValue.Redacted)
    assertEquals(entry(null, JavaConfigEntry.ConfigSource.DEFAULT_CONFIG, readOnly = true).value, ConfigurationValue.Absent)
    assert(entry("v", JavaConfigEntry.ConfigSource.DEFAULT_CONFIG, readOnly = true).readOnly)
    assertEquals(
      List(
        JavaConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
        JavaConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
        JavaConfigEntry.ConfigSource.STATIC_BROKER_CONFIG,
        JavaConfigEntry.ConfigSource.DEFAULT_CONFIG,
        JavaConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG,
        JavaConfigEntry.ConfigSource.UNKNOWN
      ).map(entry("v", _).source),
      List(
        ConfigurationSource.DynamicBroker,
        ConfigurationSource.DynamicClusterDefault,
        ConfigurationSource.StaticBroker,
        ConfigurationSource.Default,
        ConfigurationSource.Unknown,
        ConfigurationSource.Unknown
      )
    )

  test("wraps Kafka client failures"):
    val cause = new TimeoutException("timed out")

    val serializer = Serializer.const[IO, String](None)
    val client     = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val settings   = ProducerSettings.from(client, serializer, serializer).toOption.get

    interceptIO[KafkaException.BackendFailure](mocked(producer = _ => throw cause).producer(settings).use(_ => IO.unit)).map: error =>
      assertEquals(error.retriable, Some(true))
      assertEquals(error.getCause, cause)

  test("producer delegates serialization and production to the Java producer"):
    val mock    = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)
    val backend =
      mocked(producer =
        properties =>
          assertEquals(properties.get("bootstrap.servers"), Some("unused:9092"))
          assertEquals(properties.get("client.id"), Some("client"))
          // The producer's own properties override the client's.
          assertEquals(properties.get("retry.backoff.ms"), Some("2"))
          assertEquals(properties.get("compression.type"), Some("lz4"))
          // A value other than the default, so this proves the mapping rather than agreeing with it by chance.
          assertEquals(properties.get("acks"), Some("1"))
          assertEquals(properties.get("enable.idempotence"), Some("false"))
          mock
      )

    val topic         = Topic.from("events").toOption.get
    val partition     = Partition.from(0).toOption.get
    val keySerializer =
      Serializer.instance[IO, String]: (_, _, value) =>
        IO.pure(Some(Chunk.array(value.getBytes("UTF-8"))))
    val valueSerializer =
      Serializer.instance[IO, String]: (_, _, value) =>
        IO.pure(Some(Chunk.array(value.getBytes("UTF-8"))))
    val client   = ClientSettings.from(NonEmptyList.one("unused:9092"), Some("client"), Map("retry.backoff.ms" -> "1")).toOption.get
    val settings =
      ProducerSettings.from(client, keySerializer, valueSerializer, Map("retry.backoff.ms" -> "2"), compression = Compression.Lz4).toOption.get
        .withoutIdempotence.withAcks(Acks.Leader).toOption.get
    val record =
      ProducerRecord(
        topic = topic,
        key = "key",
        value = "value",
        partition = Some(partition),
        headers = Headers(Header("trace", Some(Chunk.array(Array[Byte](1)))), Header("trace", None))
      )

    backend.producer(settings).use(_.produceAndAwait(NonEmptyList.one(record))).map: result =>
      val produced = mock.history().get(0)

      assertEquals(new String(produced.key(), StandardCharsets.UTF_8), "key")
      assertEquals(new String(produced.value(), StandardCharsets.UTF_8), "value")
      val headerKeys: List[String] = produced.headers().toArray.toList.map(_.key())
      assertEquals(headerKeys, List("trace", "trace"))
      assertEquals(result.metadata.head.topicPartition, TopicPartition(topic, partition))
      assertEquals(result.metadata.head.offset.map(_.value), Some(0L))
      assertEquals(result.records.size, 1)
      assert(result.records.forall((_, metadata) => metadata.isDefined))

  test("consumer delegates streaming and offset commits to the Java consumer"):
    val topic              = Topic.from("events").toOption.get
    val group              = ConsumerGroup.from("workers").toOption.get
    val javaTopicPartition = new JavaTopicPartition(topic.value, 0)
    val pollTimeouts       = new java.util.concurrent.ConcurrentLinkedQueue[java.time.Duration]
    val mock               =
      new MockConsumer[Array[Byte], Array[Byte]]("earliest"):
        override def offsetsForTimes(
            timestampsToSearch: java.util.Map[JavaTopicPartition, java.lang.Long],
            timeout: java.time.Duration
        ): java.util.Map[JavaTopicPartition, OffsetAndTimestamp] = JavaMap.of(javaTopicPartition, new OffsetAndTimestamp(0L, 1234L))

        override def poll(timeout: java.time.Duration) =
          pollTimeouts.add(timeout)
          super.poll(timeout)
    mock.updatePartitions(topic.value, JavaList.of(new PartitionInfo(topic.value, 0, null, Array.empty, Array.empty)))
    mock.schedulePollTask(() =>
      mock.rebalance(JavaList.of(javaTopicPartition))
      mock.updateBeginningOffsets(JavaMap.of(javaTopicPartition, Long.box(0L)))
      mock.updateEndOffsets(JavaMap.of(javaTopicPartition, Long.box(1L)))
      mock.addRecord(new JavaConsumerRecord(topic.value, 0, 0L, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)))
    )
    val backend =
      mocked(consumer =
        properties =>
          assertEquals(properties.get("bootstrap.servers"), Some("unused:9092"))
          assertEquals(properties.get("group.id"), Some("workers"))
          assertEquals(properties.get("fetch.min.bytes"), Some("2"))
          assertEquals(properties.get("enable.auto.commit"), Some("false"))
          assertEquals(properties.get("auto.offset.reset"), Some("earliest"))
          // The request timeout bounds the calls that take no timeout of their own, which the Java consumer reads as default.api.timeout.ms.
          assertEquals(properties.get("default.api.timeout.ms"), Some(ConsumerSettings.DefaultRequestTimeout.toMillis.toString))
          mock
      )

    val keyDeserializer =
      Deserializer.instance[IO, String]: (_, _, bytes) =>
        IO.pure(new String(bytes.get.toArray, StandardCharsets.UTF_8))
    val valueDeserializer =
      Deserializer.instance[IO, String]: (_, _, bytes) =>
        IO.pure(new String(bytes.get.toArray, StandardCharsets.UTF_8))
    val client   = ClientSettings.from(NonEmptyList.one("unused:9092"), properties = Map("fetch.min.bytes" -> "1")).toOption.get
    val settings =
      ConsumerSettings.from(client, group, keyDeserializer, valueDeserializer, AutoOffsetReset.Earliest, properties = Map("fetch.min.bytes" -> "2"))
        .toOption.get

    backend.consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
      for
        consumed   <- consumer.records.take(1).compile.lastOrError
        assignment <- consumer.assignment
        _          <- consumed.offset.commit
        committed  <- consumer.committed(Set(consumed.record.topicPartition)).map(_.offsetsOnly)
        beginning  <- consumer.beginningOffsets(Set(consumed.record.topicPartition))
        end        <- consumer.endOffsets(Set(consumed.record.topicPartition))
        timed      <- consumer.offsetsForTimes(Map(consumed.record.topicPartition -> Timestamp.fromEpochMillis(1234L)))
        partitions <- consumer.partitionsFor(topic)
        topics     <- consumer.listTopics
        _          <- consumer.seek(consumed.record.topicPartition, consumed.record.offset)
        position = mock.position(javaTopicPartition)
      yield
        assertEquals(consumed.record.key, "key")
        assertEquals(consumed.record.value, "value")
        assertEquals(consumed.record.offset.value, 0L)
        assertEquals(consumed.offset.nextOffset.value, 1L)
        assertEquals(assignment, Set(consumed.record.topicPartition))
        assertEquals(committed, Map(consumed.record.topicPartition -> Some(consumed.offset.nextOffset)))
        assertEquals(beginning, Map(consumed.record.topicPartition -> consumed.record.offset))
        assertEquals(end, Map(consumed.record.topicPartition -> consumed.offset.nextOffset))
        assertEquals(timed, Map(consumed.record.topicPartition -> Some(consumed.record.offset)))
        assertEquals(partitions, Set(consumed.record.topicPartition.partition))
        assertEquals(topics.get(topic), Some(partitions))
        assertEquals(position, 0L)
        // The consumer polls in slices within the poll timeout.
        assert(
          pollTimeouts.asScala.nonEmpty && pollTimeouts.asScala.forall(_.toMillis <= ConsumerSettings.DefaultPollTimeout.toMillis),
          pollTimeouts.toString
        )
    .timeout(5.seconds)

  test("a failed poll fails the record stream and the partitioned one on every run"):
    // fs2-kafka, which this backend used to delegate to, lost the failure about once in two thousand runs.
    def outcomes(read: KafkaConsumer[IO, String, String] => Stream[IO, Any]): IO[List[Either[Throwable, Unit]]] =
      val failing =
        IO:
          val (consumer, partition) = subscribedMock()
          // As with a broker, the poll that assigns the partition is the one that finds it has no position, so its record stream already exists.
          consumer.schedulePollTask(() => consumer.rebalance(JavaList.of(partition)))
          consumer.setPollException(new NoOffsetForPartitionException(partition))
          consumer
      failing.flatMap(consumer => consuming(consumer)(read(_).compile.drain).attempt).parReplicateA(500).replicateA(40).map(_.flatten)
    (outcomes(_.records), outcomes(_.partitionedRecords(1))).tupled.map: (records, partitioned) =>
      List("records" -> records, "partitionedRecords" -> partitioned).foreach: (name, results) =>
        val missed = results.filterNot(resetRequired)
        assert(missed.isEmpty, s"$name ended ${missed.size} of ${results.size} runs without the poll's failure, first ${missed.head}")

  test("an offset read under a revoked assignment cannot be recorded in a transaction, and one read after the partition returns can"):
    val (consumer, partition) = subscribedMock()
    val reassigned            = new CountDownLatch(1)
    // MockConsumer runs one task at the start of each poll, so the first poll returns two records and the second revokes the partition and
    // assigns it again before its own record.
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of(partition))
      consumer.updateBeginningOffsets(JavaMap.of(partition, Long.box(0L)))
      addRecord(consumer, 0L)
      addRecord(consumer, 1L)
    )
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of())
      consumer.rebalance(JavaList.of(partition))
      addRecord(consumer, 2L)
      reassigned.countDown()
    )
    val producer = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)

    withMocks(consumer, producer): (transactional, reading) =>
      // Nothing after the first record is taken until the partition has been assigned again, so a record that took the lease current when it
      // was taken would carry the new one.
      reading.records.evalTap(_ => IO.blocking(reassigned.await(5, TimeUnit.SECONDS))).take(3).compile.toList.flatMap: records =>
        records.traverse(record => transactional.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(record.offset))).attempt)
          .map: outcomes =>
            assertEquals(records.map(_.record.offset.value), List(0L, 1L, 2L))
            assert(outcomes.take(2).forall(illegalGeneration), s"offsets read before the revocation should be rejected, got $outcomes")
            assertEquals(outcomes.lift(2), Some(Right(())))

  test("a transactional producer reaches the Java client with its close timeout, every replica acknowledging, and idempotence"):
    val properties = new java.util.concurrent.atomic.AtomicReference[Option[String]](None)
    val closedWith = new java.util.concurrent.atomic.AtomicReference[Option[java.time.Duration]](None)
    val backend    =
      mocked(producer =
        received =>
          properties.set(received.get("acks").zip(received.get("enable.idempotence")).map(_.toString + "," + _.toString))
          new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer):
            override def close(timeout: java.time.Duration): Unit =
              closedWith.set(Some(timeout))
              super.close(timeout)
      )
    val serializer = Serializer.utf8[IO]
    val client     = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val settings   =
      TransactionalProducerSettings.from(client, TransactionalId.from("writer").toOption.get, serializer, serializer, closeTimeout = 5.seconds)
        .toOption.get

    backend.transactionalProducer(settings).use_.map: _ =>
      assertEquals(properties.get, Some("all,true"))
      assertEquals(closedWith.get, Some(java.time.Duration.ofSeconds(5)))

  test("a plain commit of an offset read under a revoked assignment is refused before it reaches the consumer"):
    val (consumer, partition) = subscribedMock()
    val reassigned            = new CountDownLatch(1)
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of(partition))
      consumer.updateBeginningOffsets(JavaMap.of(partition, Long.box(0L)))
      addRecord(consumer, 0L)
      addRecord(consumer, 1L)
    )
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of())
      consumer.rebalance(JavaList.of(partition))
      addRecord(consumer, 2L)
      reassigned.countDown()
    )
    val producer = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)

    withMocks(consumer, producer): (_, reading) =>
      reading.records.evalTap(_ => IO.blocking(reassigned.await(5, TimeUnit.SECONDS))).take(3).compile.toList.flatMap: records =>
        for
          stale     <- records(1).offset.commit.attempt
          untouched <- IO(Option(consumer.committed(java.util.Set.of(partition)).get(partition)))
          current   <- records(2).offset.commit.attempt
          moved     <- IO(Option(consumer.committed(java.util.Set.of(partition)).get(partition)).map(_.offset))
        yield
          assert(illegalGeneration(stale), s"a plain commit from a revoked assignment should be refused, got $stale")
          assertEquals(untouched, None, "the refused commit should not have reached the consumer")
          assertEquals(current, Right(()))
          assertEquals(moved, Some(3L))

  test("a revocation waits for a transaction recording one of its offsets before the group hears of it"):
    val (consumer, partition) = subscribedMock()
    val sending               = new CountDownLatch(1)
    val release               = new CountDownLatch(1)
    val revoked               = new CountDownLatch(1)
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of(partition))
      consumer.updateBeginningOffsets(JavaMap.of(partition, Long.box(0L)))
      addRecord(consumer, 0L)
    )
    // The first poll after the transaction starts sending revokes the partition, and says when the revocation has gone through. Polls before it
    // only look, since a poll that waited would hold the thread the transaction reads the group's membership on.
    def revokeOnceSending(): Unit =
      if sending.getCount == 0L then
        consumer.rebalance(JavaList.of())
        revoked.countDown()
      else consumer.schedulePollTask(() => revokeOnceSending())
    consumer.schedulePollTask(() => revokeOnceSending())
    val producer =
      new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer):
        override def sendOffsetsToTransaction(offsets: JavaMap[JavaTopicPartition, OffsetAndMetadata], metadata: ConsumerGroupMetadata): Unit =
          sending.countDown()
          release.await()
          super.sendOffsetsToTransaction(offsets, metadata)

    withMocks(consumer, producer): (transactional, reading) =>
      for
        held      <- reading.records.take(1).compile.lastOrError
        recording <- transactional.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(held.offset))).attempt.start
        _         <- IO.blocking(sending.await())
        _         <- IO.sleep(200.millis)
        early     <- IO(revoked.getCount == 0L)
        _         <- IO(release.countDown())
        recorded  <- recording.joinWithNever
        _         <- IO.blocking(revoked.await(5, TimeUnit.SECONDS))
      yield
        assert(!early, "the revocation went through while a transaction was still recording its offset")
        assertEquals(recorded, Right(()))

  test("a transaction's offsets the group rejects as another generation's are classified the way librdkafka reports them"):
    // Worded as the Java client words it, which appends the broker's own description of its answer.
    def rejected(error: Errors) =
      new CommitFailedException(s"Transaction offset Commit failed due to consumer group metadata mismatch: ${error.exception.getMessage}")
    val cases =
      List(
        rejected(Errors.ILLEGAL_GENERATION) -> Some(ErrorCode.IllegalGeneration),
        rejected(Errors.UNKNOWN_MEMBER_ID)  -> Some(ErrorCode.UnknownMemberId),
        // Any other wording is left as before: Kafka's table has no entry for the exception, so it stays an unknown error.
        new CommitFailedException("the group has already rebalanced") -> Some(ErrorCode.fromProtocol(Errors.UNKNOWN_SERVER_ERROR.code.toInt))
      )
    val transactionalId = TransactionalId.from("writer").toOption.get
    val serializer      = Serializer.const[IO, String](None)
    val settings        =
      TransactionalProducerSettings.from(ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get, transactionalId, serializer, serializer)
        .toOption.get
    val topic = Topic.from("events").toOption.get

    cases.traverse: (failure, expected) =>
      val mock = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)
      mock.sendOffsetsToTransactionException = failure
      val readUnder =
        new GroupMembership.Backend[IO](
          "group",
          IO(JavaGroupMetadata(new MockConsumer[Array[Byte], Array[Byte]]("earliest").groupMetadata())),
          _ => IO.unit
        )
      val offset =
        new CommittableOffset[IO]:
          override def topicPartition: TopicPartition = TopicPartition(topic, Partition.from(0).toOption.get)
          override def nextOffset: Offset             = Offset.from(1L).toOption.get
          override def committer: OffsetCommitter[IO] =
            new OffsetCommitter[IO]:
              override def commitWithMetadata(offsets: Map[TopicPartition, xkafka.OffsetAndMetadata]): IO[Unit] = IO.unit
          override private[xkafka] def membership: GroupMembership[IO] = readUnder
      mocked(producer = _ => mock).transactionalProducer(settings)
        .use(_.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(offset)))).attempt.map:
          case Left(error: KafkaException.BackendFailure) => assertEquals(error.code, expected, failure.getMessage)
          case other                                      => fail(s"expected a backend failure, got $other")
    .void

  private val topic = Topic.from("events").toOption.get
  private val group = ConsumerGroup.from("workers").toOption.get

  /** A consumer the client will subscribe, which MockConsumer requires to know the topic's partitions. */
  private def subscribedMock(): (MockConsumer[Array[Byte], Array[Byte]], JavaTopicPartition) =
    val consumer = new MockConsumer[Array[Byte], Array[Byte]]("earliest")
    consumer.updatePartitions(topic.value, JavaList.of(new PartitionInfo(topic.value, 0, null, Array.empty, Array.empty)))
    (consumer, new JavaTopicPartition(topic.value, 0))

  private def addRecord(consumer: MockConsumer[Array[Byte], Array[Byte]], offset: Long): Unit =
    consumer
      .addRecord(new JavaConsumerRecord(topic.value, 0, offset, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)))

  /** A transactional producer and a subscribed consumer, both built by the client over the given mocks. */
  private def withMocks[A](consumer: MockConsumer[Array[Byte], Array[Byte]], producer: MockProducer[Array[Byte], Array[Byte]])(
      use: (KafkaTransactionalProducer[IO, String, String], KafkaConsumer[IO, String, String]) => IO[A]
  ): IO[A] =
    val utf8       = Deserializer.utf8[IO]
    val serializer = Serializer.utf8[IO]
    val client     = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val reading    = ConsumerSettings.from(client, group, utf8, utf8, AutoOffsetReset.Earliest).toOption.get
    val writing    = TransactionalProducerSettings.from(client, TransactionalId.from("writer").toOption.get, serializer, serializer).toOption.get
    val backend    = mocked(producer = _ => producer, consumer = _ => consumer)
    (backend.transactionalProducer(writing), backend.consumer(reading, Selection.Topics(NonEmptySet.one(topic)))).tupled.use(use.tupled)
      .timeout(10.seconds)

  /** A consumer the client builds over the given mock, reading UTF-8 from the shared topic. */
  private def consuming[A](consumer: MockConsumer[Array[Byte], Array[Byte]])(use: KafkaConsumer[IO, String, String] => IO[A]): IO[A] =
    val client  = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val utf8    = Deserializer.utf8[IO]
    val reading = ConsumerSettings.from(client, group, utf8, utf8, AutoOffsetReset.Fail).toOption.get
    mocked(consumer = _ => consumer).consumer(reading, Selection.Topics(NonEmptySet.one(topic))).use(use)

  /** The client over the given mocks; a client of a kind the test does not expect fails the test. */
  test("an administration effect calls the Java client only when it runs, and again on every run"):
    val calls   = new java.util.concurrent.ConcurrentLinkedQueue[String]
    val handler =
      new java.lang.reflect.InvocationHandler:
        override def invoke(proxy: AnyRef, method: java.lang.reflect.Method, arguments: Array[AnyRef]): AnyRef =
          if method.getName == "close" then null
          else
            calls.add(method.getName): Unit
            throw new IllegalStateException(s"${method.getName} reached the Java client")
    val admin    = java.lang.reflect.Proxy.newProxyInstance(classOf[Admin].getClassLoader, Array(classOf[Admin]), handler).asInstanceOf[Admin]
    val client   = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val topic    = Topic.from("events").toOption.get
    val newTopic = NewTopic.from(topic, partitions = 1, replicationFactor = 1).toOption.get
    val cut      = cats.data.NonEmptyMap.one(TopicPartition(topic, Partition.from(0).toOption.get), Offset.from(1L).toOption.get)

    mocked(admin = _ => admin).admin(client).use: client =>
      for
        built <-
          IO.delay(List(
            client.createTopics(NonEmptySet.one(newTopic)),
            client.deleteTopics(NonEmptySet.one(topic)),
            client.createPartitions(topic, 2),
            client.describeTopics(NonEmptySet.one(topic)),
            client.deleteRecords(cut),
            client.describeTopicConfigurations(NonEmptySet.one(topic))
          ))
        before <- IO.delay(calls.asScala.toList)
        _      <- built.traverse_(_.attempt)
        _      <- built.traverse_(_.attempt)
      yield
        assertEquals(before, Nil, "building the effects should not call the Java client")
        assertEquals(
          calls.asScala.toList,
          // Describing topics lists them first, and deleting records stops at looking up its topic, which the proxy refuses.
          List.fill(2)(List("createTopics", "deleteTopics", "createPartitions", "listTopics", "describeTopics", "describeConfigs")).flatten,
          "each run should make its own call"
        )

  test("a producer effect calls the Java client only when it runs"):
    val mock     = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)
    val utf8     = Serializer.utf8[IO]
    val client   = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val settings = ProducerSettings.from(client, utf8, utf8).toOption.get
    val topic    = Topic.from("events").toOption.get
    val records  = NonEmptyList.one(ProducerRecord(topic = topic, key = "key", value = "value", partition = Partition.from(0).toOption))

    mocked(producer = _ => mock).producer(settings).use: producer =>
      for
        built  <- IO.delay(List(producer.produce(records).void, producer.produceAndAwait(records).void, producer.partitionsFor(topic).void))
        before <- IO.delay(mock.history.size)
        _      <- built.traverse_(_.attempt)
      yield
        assertEquals(before, 0, "building the effects should not send anything")
        assertEquals(mock.history.size, 2, "running them should send each record")

  private def mocked(
      producer: Map[String, AnyRef] => org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]] = _ => fail("no producer expected"),
      consumer: Map[String, AnyRef] => org.apache.kafka.clients.consumer.Consumer[Array[Byte], Array[Byte]] = _ => fail("no consumer expected"),
      admin: Map[String, AnyRef] => Admin = _ => fail("no administration client expected")
  ): KafkaClient[IO] = KafkaClientPlatform.fromJava[IO](JavaClients(producer, consumer, admin))

  private def resetRequired(outcome: Either[Throwable, Unit]): Boolean =
    outcome.left.exists:
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.OffsetResetRequired)
      case _                                      => false

  private def illegalGeneration(outcome: Either[Throwable, Unit]): Boolean =
    outcome.left.exists:
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.IllegalGeneration)
      case _                                      => false
