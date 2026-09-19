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
import java.util.{List as JavaList, Map as JavaMap, Set as JavaSet}

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import fs2.kafka.{ConsumerSettings as Fs2ConsumerSettings, KafkaByteConsumer, KafkaByteProducer, ProducerSettings as Fs2ProducerSettings}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.producer.MkProducer
import munit.CatsEffectSuite
import org.apache.kafka.clients.consumer.{ConsumerRecord as JavaConsumerRecord, MockConsumer}
import org.apache.kafka.clients.producer.{MockProducer, Partitioner}
import org.apache.kafka.common.TopicPartition as JavaTopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer

final class JvmKafkaClientSuite extends CatsEffectSuite:
  test("producer delegates serialization and production to fs2-kafka"):
    val mock = new MockProducer[Array[Byte], Array[Byte]](true, null: Partitioner, new ByteArraySerializer, new ByteArraySerializer)
    given MkProducer[IO] with
      override def apply[G[_]](settings: Fs2ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
        IO:
          assertEquals(settings.properties.get("bootstrap.servers"), Some("unused:9092"))
          assertEquals(settings.properties.get("client.id"), Some("client"))
          assertEquals(settings.properties.get("compression.type"), Some("lz4"))
          assertEquals(settings.properties.get("acks"), Some("all"))
          mock

    val topic         = Topic.from("events").toOption.get
    val partition     = Partition.from(0).toOption.get
    val keySerializer =
      Serializer.instance[IO, String]: (_, _, value) =>
        IO.pure(Some(Chunk.array(value.getBytes("UTF-8"))))
    val valueSerializer =
      Serializer.instance[IO, String]: (_, _, value) =>
        IO.pure(Some(Chunk.array(value.getBytes("UTF-8"))))
    val settings =
      ProducerSettings(
        ClientSettings(NonEmptyList.one("unused:9092"), Some("client"), Map("compression.type" -> "gzip", "client.id" -> "ignored")),
        keySerializer,
        valueSerializer,
        Map("compression.type" -> "lz4", "acks" -> "all", "bootstrap.servers" -> "ignored:9092")
      )
    val record =
      ProducerRecord(
        topic = topic,
        key = "key",
        value = "value",
        partition = Some(partition),
        headers = Headers(Header("trace", Some(Chunk.array(Array[Byte](1)))), Header("trace", None))
      )

    KafkaClientPlatform.fromFs2[IO].producer(settings).use(_.produce(NonEmptyList.one(record))).map: metadata =>
      val produced = mock.history().get(0)

      assertEquals(new String(produced.key(), StandardCharsets.UTF_8), "key")
      assertEquals(new String(produced.value(), StandardCharsets.UTF_8), "value")
      val headerKeys: List[String] = produced.headers().toArray.toList.map(_.key())
      assertEquals(headerKeys, List("trace", "trace"))
      assertEquals(metadata.metadata.head.topicPartition, TopicPartition(topic, partition))
      assertEquals(metadata.metadata.head.offset.map(_.value), Some(0L))

  test("consumer delegates streaming and offset commits to fs2-kafka"):
    val topic              = Topic.from("events").toOption.get
    val group              = ConsumerGroup.from("workers").toOption.get
    val javaTopicPartition = new JavaTopicPartition(topic.value, 0)
    val mock               = new MockConsumer[Array[Byte], Array[Byte]]("earliest")
    mock.schedulePollTask(() =>
      mock.rebalance(JavaList.of(javaTopicPartition))
      mock.updateBeginningOffsets(JavaMap.of(javaTopicPartition, Long.box(0L)))
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
          mock

    val keyDeserializer =
      Deserializer.instance[IO, String]: (_, _, bytes) =>
        IO.pure(new String(bytes.get.toArray, StandardCharsets.UTF_8))
    val valueDeserializer =
      Deserializer.instance[IO, String]: (_, _, bytes) =>
        IO.pure(new String(bytes.get.toArray, StandardCharsets.UTF_8))
    val settings =
      ConsumerSettings(
        ClientSettings(NonEmptyList.one("unused:9092"), properties = Map("fetch.min.bytes" -> "1", "group.id" -> "ignored")),
        group,
        keyDeserializer,
        valueDeserializer,
        AutoOffsetReset.Earliest,
        Map("fetch.min.bytes" -> "2", "enable.auto.commit" -> "true", "auto.offset.reset" -> "none")
      )

    KafkaClientPlatform.fromFs2[IO].consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use: consumer =>
      for
        consumed <- consumer.records.take(1).compile.lastOrError
        _        <- consumed.offset.commit
        committed = mock.committed(JavaSet.of(javaTopicPartition))
      yield
        assertEquals(consumed.record.key, "key")
        assertEquals(consumed.record.value, "value")
        assertEquals(consumed.record.offset.value, 0L)
        assertEquals(consumed.offset.nextOffset.value, 1L)
        assertEquals(committed.get(javaTopicPartition).offset(), 1L)
    .timeout(5.seconds)
