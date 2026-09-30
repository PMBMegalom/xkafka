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
import java.util.{List as JavaList, Map as JavaMap, Optional}
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn
import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import fs2.Chunk
import fs2.kafka.{ConsumerSettings as Fs2ConsumerSettings, KafkaByteConsumer, KafkaByteProducer, ProducerSettings as Fs2ProducerSettings}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.producer.MkProducer
import munit.CatsEffectSuite
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, ConsumerRecord as JavaConsumerRecord, MockConsumer, OffsetAndTimestamp}
import org.apache.kafka.clients.producer.{MockProducer, Partitioner}
import org.apache.kafka.common.{PartitionInfo, TopicPartition as JavaTopicPartition}
import org.apache.kafka.common.errors.TimeoutException
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

  test("a record keeps the membership it was polled under after the consumer moves to a later one"):
    val topic              = Topic.from("events").toOption.get
    val group              = ConsumerGroup.from("workers").toOption.get
    val javaTopicPartition = new JavaTopicPartition(topic.value, 0)
    val generation         = new AtomicInteger(1)
    val mock               =
      new MockConsumer[Array[Byte], Array[Byte]]("earliest"):
        // Only the deprecated constructor sets a generation, which is the one thing this test needs the consumer to change.
        @nowarn("cat=deprecation")
        override def groupMetadata(): ConsumerGroupMetadata = new ConsumerGroupMetadata(group.value, generation.get, "member", Optional.empty())
    def add(offset: Long): Unit =
      mock.addRecord(new JavaConsumerRecord(topic.value, 0, offset, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)))
    mock.updatePartitions(topic.value, JavaList.of(new PartitionInfo(topic.value, 0, null, Array.empty, Array.empty)))
    // MockConsumer runs one task at the start of each poll, so the first poll returns two records and the second one moves the generation.
    mock.schedulePollTask(() =>
      mock.rebalance(JavaList.of(javaTopicPartition))
      mock.updateBeginningOffsets(JavaMap.of(javaTopicPartition, Long.box(0L)))
      add(0L)
      add(1L)
    )
    mock.schedulePollTask(() =>
      generation.set(2)
      add(2L)
    )
    given MkConsumer[IO] with
      override def apply[G[_]](settings: Fs2ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] = IO.pure(mock)

    val utf8     = Deserializer.utf8[IO]
    val client   = ClientSettings.from(NonEmptyList.one("unused:9092")).toOption.get
    val settings = ConsumerSettings.from(client, group, utf8, utf8, AutoOffsetReset.Earliest).toOption.get
    // Nothing is read until the second poll has moved the generation, so a record that took the generation current when it was read would show 2.
    val moved = (IO.sleep(10.millis) *> IO(generation.get)).iterateUntil(_ == 2)

    KafkaClientPlatform.fromFs2[IO].consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
      consumer.records.evalTap(_ => moved).take(3).compile.toList.map: records =>
        val generations =
          records.map(record =>
            record.record.offset.value -> record.offset.membership.handle.map(_.key).collect { case metadata: ConsumerGroupMetadata =>
              metadata.generationId
            }
          )
        assertEquals(generations, List(0L -> Some(1), 1L -> Some(1), 2L -> Some(2)))
    .timeout(10.seconds)
