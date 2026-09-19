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

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite

final class KafkaIntegrationSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val bootstrapServers = PlatformKafkaClient.integrationBootstrapServers

  bootstrapServers match
    case None                  => test("resume from a committed offset against Kafka".ignore)(IO.unit)
    case Some(bootstrapServer) => test("resume from a committed offset against Kafka"):
        roundTrip(bootstrapServer)

  private def roundTrip(bootstrapServer: String): IO[Unit] =
    val suffix         = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topic          = validTopic(s"xkafka-integration-$suffix")
    val group          = validConsumerGroup(s"xkafka-integration-$suffix")
    val clientSettings = ClientSettings(NonEmptyList.one(bootstrapServer), properties = Map("metadata.max.age.ms" -> "30000"))
    val headers        = Headers(Header("x-xkafka-integration", Some(Chunk.array(Array[Byte](1, 2, 3)))))
    val partition      = Partition.from(0).fold(error => fail(s"invalid test partition: $error"), identity)
    val first          =
      ProducerRecord(
        topic = topic,
        key = "first-key",
        value = s"first-value-${PlatformKafkaClient.name}",
        partition = Some(partition),
        headers = headers
      )
    val second =
      ProducerRecord(
        topic = topic,
        key = "second-key",
        value = s"second-value-${PlatformKafkaClient.name}",
        partition = Some(partition),
        headers = headers
      )
    val producerSettings = ProducerSettings(clientSettings, utf8Serializer, utf8Serializer, Map("linger.ms" -> "0"))
    val consumerSettings =
      ConsumerSettings(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest, Map("fetch.min.bytes" -> "1"))

    for
      produced <-
        PlatformKafkaClient().producer(producerSettings).use(_.produce(NonEmptyList.of(first, second)))
          .timeoutTo(45.seconds, IO.raiseError(new RuntimeException("Kafka producer timed out")))
      consumed <- consumeTwoAndCommitFirst(consumerSettings, topic)
      (consumedFirst, observedSecond) = consumed
      consumedSecond <- consumeOne(consumerSettings, topic)
    yield
      assert(produced.metadata.nonEmpty)
      assertEquals(consumedFirst.record.topicPartition, TopicPartition(topic, partition))
      assertEquals(consumedFirst.record.key, first.key)
      assertEquals(consumedFirst.record.value, first.value)
      assertEquals(consumedFirst.record.headers.getAll("x-xkafka-integration").map(_.map(_.toVector)), Vector(Some(Vector[Byte](1, 2, 3))))
      assertEquals(consumedFirst.offset.nextOffset.value, consumedFirst.record.offset.value + 1L)
      assertEquals(consumedSecond.record.topicPartition, TopicPartition(topic, partition))
      assertEquals(consumedSecond.record.key, second.key)
      assertEquals(consumedSecond.record.value, second.value)
      assertEquals(consumedSecond.record.offset, consumedFirst.offset.nextOffset)
      assertEquals(consumedSecond.record, observedSecond.record)

  private def consumeTwoAndCommitFirst(
      settings: ConsumerSettings[IO, String, String],
      topic: Topic
  ): IO[(CommittableConsumerRecord[IO, String, String], CommittableConsumerRecord[IO, String, String])] =
    PlatformKafkaClient().consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use: consumer =>
      consumer.records.take(2).compile.toList.flatMap:
        case first :: second :: Nil => first.offset.commit.as((first, second))
        case records                => IO.raiseError(new AssertionError(s"expected two records, got ${records.size}"))
    .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer timed out")))

  private def consumeOne(settings: ConsumerSettings[IO, String, String], topic: Topic): IO[CommittableConsumerRecord[IO, String, String]] =
    PlatformKafkaClient().consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use(_.records.take(1).compile.lastOrError)
      .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer timed out")))

  private val utf8Serializer: Serializer[IO, String] = Serializer.instance((_, _, value) => IO.pure(Some(Chunk.array(value.getBytes("UTF-8")))))

  private val utf8Deserializer: Deserializer[IO, String] =
    Deserializer.instance((_, _, bytes) =>
      IO.fromOption(bytes)(new IllegalStateException("expected non-null bytes")).map(chunk => new String(chunk.toArray, "UTF-8"))
    )

  private def validTopic(value: String): Topic = Topic.from(value).fold(error => fail(s"invalid test topic: $error"), identity)

  private def validConsumerGroup(value: String): ConsumerGroup =
    ConsumerGroup.from(value).fold(error => fail(s"invalid test consumer group: $error"), identity)
