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

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import fs2.kafka.{ConsumerSettings as Fs2ConsumerSettings, KafkaByteConsumer, KafkaByteProducer, ProducerSettings as Fs2ProducerSettings}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.producer.MkProducer
import munit.CatsEffectSuite
import org.apache.kafka.clients.consumer.{
  CommitFailedException, ConsumerGroupMetadata, ConsumerRecord as JavaConsumerRecord, MockConsumer, OffsetAndMetadata, OffsetAndTimestamp
}
import org.apache.kafka.clients.producer.{MockProducer, Partitioner}
import org.apache.kafka.common.{PartitionInfo, TopicPartition as JavaTopicPartition}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.serialization.ByteArraySerializer

final class JvmKafkaClientSuite extends CatsEffectSuite:
  test("wraps Kafka client failures"):
    val cause = new TimeoutException("timed out")
    given MkProducer[IO] with
      override def apply[G[_]](settings: Fs2ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO.raiseError(cause)

    val serializer = Serializer.const[IO, String](None)
    val client     = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val settings   = ProducerSettings.from(client, serializer, serializer).toOption.get

    interceptIO[KafkaException.BackendFailure](KafkaClientPlatform.fromFs2[IO].producer(settings).use(_ => IO.unit)).map: error =>
      assertEquals(error.retriable, Some(true))
      assertEquals(error.getCause, cause)

  test("producer delegates serialization and production to fs2-kafka"):
    val mock = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)
    given MkProducer[IO] with
      override def apply[G[_]](settings: Fs2ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
        IO:
          assertEquals(settings.properties.get("bootstrap.servers"), Some("unused:9092"))
          assertEquals(settings.properties.get("client.id"), Some("client"))
          assertEquals(settings.properties.get("compression.type"), Some("lz4"))
          // A value other than the default, so this proves the mapping rather than agreeing with it by chance.
          assertEquals(settings.properties.get("acks"), Some("1"))
          mock

    val topic         = Topic.from("events").toOption.get
    val partition     = Partition.from(0).toOption.get
    val keySerializer =
      Serializer.instance[IO, String]: (_, _, value) =>
        IO.pure(Some(Chunk.array(value.getBytes("UTF-8"))))
    val valueSerializer =
      Serializer.instance[IO, String]: (_, _, value) =>
        IO.pure(Some(Chunk.array(value.getBytes("UTF-8"))))
    val client   = ClientSettings.from(NonEmptyList.one("unused:9092"), Some("client"), Map("compression.type" -> "gzip")).toOption.get
    val settings = ProducerSettings.from(client, keySerializer, valueSerializer, Map("compression.type" -> "lz4")).toOption.get.withAcks(Acks.Leader)
    val record   =
      ProducerRecord(
        topic = topic,
        key = "key",
        value = "value",
        partition = Some(partition),
        headers = Headers(Header("trace", Some(Chunk.array(Array[Byte](1)))), Header("trace", None))
      )

    KafkaClientPlatform.fromFs2[IO].producer(settings).use(_.produceAndAwait(NonEmptyList.one(record))).map: result =>
      val produced = mock.history().get(0)

      assertEquals(new String(produced.key(), StandardCharsets.UTF_8), "key")
      assertEquals(new String(produced.value(), StandardCharsets.UTF_8), "value")
      val headerKeys: List[String] = produced.headers().toArray.toList.map(_.key())
      assertEquals(headerKeys, List("trace", "trace"))
      assertEquals(result.metadata.head.topicPartition, TopicPartition(topic, partition))
      assertEquals(result.metadata.head.offset.map(_.value), Some(0L))
      assertEquals(result.records.size, 1)
      assert(result.records.forall((_, metadata) => metadata.isDefined))

  test("consumer delegates streaming and offset commits to fs2-kafka"):
    val topic              = Topic.from("events").toOption.get
    val group              = ConsumerGroup.from("workers").toOption.get
    val javaTopicPartition = new JavaTopicPartition(topic.value, 0)
    val mock               =
      new MockConsumer[Array[Byte], Array[Byte]]("earliest"):
        override def offsetsForTimes(
            timestampsToSearch: java.util.Map[JavaTopicPartition, java.lang.Long]
        ): java.util.Map[JavaTopicPartition, OffsetAndTimestamp] = JavaMap.of(javaTopicPartition, new OffsetAndTimestamp(0L, 1234L))
    mock.updatePartitions(topic.value, JavaList.of(new PartitionInfo(topic.value, 0, null, Array.empty, Array.empty)))
    mock.schedulePollTask(() =>
      mock.rebalance(JavaList.of(javaTopicPartition))
      mock.updateBeginningOffsets(JavaMap.of(javaTopicPartition, Long.box(0L)))
      mock.updateEndOffsets(JavaMap.of(javaTopicPartition, Long.box(1L)))
      mock.addRecord(new JavaConsumerRecord(topic.value, 0, 0L, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)))
    )
    given MkConsumer[IO] with
      override def apply[G[_]](settings: Fs2ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] =
        IO:
          assertEquals(settings.properties.get("bootstrap.servers"), Some("unused:9092"))
          assertEquals(settings.properties.get("group.id"), Some("workers"))
          assertEquals(settings.properties.get("fetch.min.bytes"), Some("2"))
          assertEquals(settings.properties.get("enable.auto.commit"), Some("false"))
          assertEquals(settings.properties.get("auto.offset.reset"), Some("earliest"))
          // The consumer poll timeout has to reach fs2-kafka, which is the only place it takes effect.
          assertEquals(settings.pollTimeout, ConsumerSettings.DefaultPollTimeout)
          // The request timeout has to reach fs2-kafka, which reads it as default.api.timeout.ms.
          assertEquals(settings.properties.get("default.api.timeout.ms"), Some(ConsumerSettings.DefaultRequestTimeout.toMillis.toString))
          mock

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

    KafkaClientPlatform.fromFs2[IO].consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
      for
        consumed   <- consumer.records.take(1).compile.lastOrError
        assignment <- consumer.assignment
        _          <- consumed.offset.commit
        committed  <- consumer.committed(Set(consumed.record.topicPartition))
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
    .timeout(5.seconds)

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

  test("a transactional producer reaches fs2-kafka with its close timeout and every replica acknowledging"):
    val captured = new java.util.concurrent.atomic.AtomicReference[Option[(scala.concurrent.duration.FiniteDuration, Option[String])]](None)
    given MkProducer[IO] with
      override def apply[G[_]](settings: Fs2ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
        IO(captured.set(Some(settings.closeTimeout -> settings.properties.get("acks")))) *>
          IO.pure(new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer))
    val serializer = Serializer.utf8[IO]
    val client     = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val settings   =
      TransactionalProducerSettings.from(client, TransactionalId.from("writer").toOption.get, serializer, serializer, closeTimeout = 5.seconds)
        .toOption.get

    KafkaClientPlatform.fromFs2[IO].transactionalProducer(settings).use_.map(_ => assertEquals(captured.get, Some(5.seconds -> Some("all"))))

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

  test("a revocation waits for a transaction recording one of its offsets before fs2-kafka hears of it"):
    val (consumer, partition) = subscribedMock()
    val sending               = new CountDownLatch(1)
    val release               = new CountDownLatch(1)
    val revoked               = new CountDownLatch(1)
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of(partition))
      consumer.updateBeginningOffsets(JavaMap.of(partition, Long.box(0L)))
      addRecord(consumer, 0L)
    )
    // The next poll revokes the partition once the transaction is sending, and says when the revocation has gone through.
    consumer.schedulePollTask(() =>
      sending.await(): Unit
      consumer.rebalance(JavaList.of())
      revoked.countDown()
    )
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
      given MkProducer[IO] with
        override def apply[G[_]](settings: Fs2ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO.pure(mock)
      val readUnder =
        new GroupMembership.Backend[IO](
          "group",
          IO(Fs2GroupHandle(new MockConsumer[Array[Byte], Array[Byte]]("earliest").groupMetadata())),
          _ => IO.unit
        )
      val offset =
        new CommittableOffset[IO]:
          override def topicPartition: TopicPartition = TopicPartition(topic, Partition.from(0).toOption.get)
          override def nextOffset: Offset             = Offset.from(1L).toOption.get
          override def committer: OffsetCommitter[IO] =
            new OffsetCommitter[IO]:
              override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = IO.unit
          override private[xkafka] def membership: GroupMembership[IO] = readUnder
      KafkaClientPlatform.fromFs2[IO].transactionalProducer(settings)
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
    given MkConsumer[IO] with
      override def apply[G[_]](settings: Fs2ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] = IO.pure(consumer)
    given MkProducer[IO] with
      override def apply[G[_]](settings: Fs2ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO.pure(producer)
    val utf8       = Deserializer.utf8[IO]
    val serializer = Serializer.utf8[IO]
    val client     = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val reading    = ConsumerSettings.from(client, group, utf8, utf8, AutoOffsetReset.Earliest).toOption.get
    val writing    = TransactionalProducerSettings.from(client, TransactionalId.from("writer").toOption.get, serializer, serializer).toOption.get
    val backend    = KafkaClientPlatform.fromFs2[IO]
    (backend.transactionalProducer(writing), backend.consumer(reading, Selection.Topics(NonEmptySet.one(topic)))).tupled.use(use.tupled)
      .timeout(10.seconds)

  private def illegalGeneration(outcome: Either[Throwable, Unit]): Boolean =
    outcome.left.exists:
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.IllegalGeneration)
      case _                                      => false
