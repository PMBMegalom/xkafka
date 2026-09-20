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
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite

final class KafkaIntegrationSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val bootstrapServers = PlatformKafkaClient.integrationBootstrapServers

  bootstrapServers match
    case None =>
      test("resume from a committed offset against Kafka".ignore)(IO.unit)
      test("resume from batched offsets across topic-partitions".ignore)(IO.unit)
      test("inspect consumer offsets and seek".ignore)(IO.unit)
      test("consume partition-scoped streams".ignore)(IO.unit)
    case Some(bootstrapServer) =>
      test("resume from a committed offset against Kafka"):
        roundTrip(bootstrapServer)
      test("resume from batched offsets across topic-partitions"):
        batchRoundTrip(bootstrapServer)
      test("inspect consumer offsets and seek"):
        controlRoundTrip(bootstrapServer)
      test("consume partition-scoped streams"):
        partitionedRoundTrip(bootstrapServer)

  private def roundTrip(bootstrapServer: String): IO[Unit] =
    val suffix         = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topicPrefix    = s"xkafka-integration-$suffix"
    val topic          = validTopic(s"$topicPrefix-events")
    val topicPattern   = validTopicPattern(s"$topicPrefix-.*")
    val group          = validConsumerGroup(s"xkafka-integration-$suffix")
    val clientSettings = ClientSettings.from(NonEmptyList.one(bootstrapServer), properties = Map("metadata.max.age.ms" -> "30000")).toOption.get
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
    val producerSettings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer, Map("linger.ms" -> "0")).toOption.get
    val consumerSettings =
      ConsumerSettings
        .from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest, properties = Map("fetch.min.bytes" -> "1"))
        .toOption.get

    for
      produced <-
        PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(NonEmptyList.of(first, second)))
          .timeoutTo(45.seconds, IO.raiseError(new RuntimeException("Kafka producer timed out")))
      consumed <- consumeTwoAndCommitFirst(consumerSettings, topic)
      (consumedFirst, observedSecond) = consumed
      consumedSecond <- consumeOne(consumerSettings, Subscription.Pattern(topicPattern))
    yield
      assertEquals(produced.records.size, 2)
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

  private def batchRoundTrip(bootstrapServer: String): IO[Unit] =
    val suffix           = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topicPrefix      = s"xkafka-batch-integration-$suffix"
    val firstTopic       = validTopic(s"$topicPrefix-first")
    val secondTopic      = validTopic(s"$topicPrefix-second")
    val subscription     = Subscription.Pattern(validTopicPattern(s"$topicPrefix-.*"))
    val group            = validConsumerGroup(s"xkafka-batch-integration-$suffix")
    val clientSettings   = ClientSettings.from(NonEmptyList.one(bootstrapServer)).toOption.get
    val producerSettings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer).toOption.get
    val consumerSettings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest).toOption.get
    val firstRecords     = NonEmptyList.of(ProducerRecord(firstTopic, "first-key", "first-1"), ProducerRecord(secondTopic, "second-key", "second-1"))
    val secondRecords    = NonEmptyList.of(ProducerRecord(firstTopic, "first-key", "first-2"), ProducerRecord(secondTopic, "second-key", "second-2"))

    for
      _        <- PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(firstRecords)).timeout(45.seconds)
      consumed <- consumeAndCommitBatch(consumerSettings, subscription, 2)
      _        <- PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(secondRecords)).timeout(45.seconds)
      resumed  <- consume(consumerSettings, subscription, 2)
    yield
      assertEquals(consumed.map(record => record.record.topicPartition.topic).toSet, Set(firstTopic, secondTopic))
      assertEquals(resumed.map(record => record.record.value).toSet, Set("first-2", "second-2"))
      assert(resumed.forall(record => record.record.offset.value == 1L))

  private def controlRoundTrip(bootstrapServer: String): IO[Unit] =
    val suffix           = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topic            = validTopic(s"xkafka-control-integration-$suffix")
    val partition        = Partition.from(0).fold(error => fail(s"invalid test partition: $error"), identity)
    val topicPartition   = TopicPartition(topic, partition)
    val group            = validConsumerGroup(s"xkafka-control-integration-$suffix")
    val clientSettings   = ClientSettings.from(NonEmptyList.one(bootstrapServer)).toOption.get
    val producerSettings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer).toOption.get
    val consumerSettings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest).toOption.get

    for
      timestamp <- IO.realTime.map(value => Timestamp.fromEpochMillis(value.toMillis))
      record = ProducerRecord(topic, "control-key", "control-value", partition = Some(partition), timestamp = Some(timestamp))
      _        <- PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(NonEmptyList.one(record))).timeout(45.seconds)
      consumed <-
        PlatformKafkaClient().consumer(consumerSettings, Subscription.Topics(NonEmptyList.one(topic))).use: consumer =>
          consumer.records.zipWithIndex.evalMap:
            case (value, 0L) =>
              for
                assignment <- consumer.assignment
                before     <- consumer.committed(Set(topicPartition))
                beginning  <- consumer.beginningOffsets(Set(topicPartition))
                end        <- consumer.endOffsets(Set(topicPartition))
                timed      <- consumer.offsetsForTimes(Map(topicPartition -> timestamp))
                missing    <- consumer.offsetsForTimes(Map(topicPartition -> Timestamp.fromEpochMillis(timestamp.epochMillis + 1L)))
                partitions <- consumer.partitionsFor(topic)
                topics     <- consumer.listTopics
                _          <- value.offset.commit
                stored     <- consumer.committed(Set(topicPartition))
                _          <- consumer.seek(topicPartition, value.record.offset)
              yield (value, Some((assignment, before, stored, beginning, end, timed, missing, partitions, topics)))
            case (value, _) => IO.pure((value, None))
          .take(2).compile.toList.timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer control test timed out")))
    yield
      val first                                                                            = consumed.head
      val replayed                                                                         = consumed.last
      val (assignment, before, stored, beginning, end, timed, missing, partitions, topics) =
        first._2.getOrElse(fail("missing consumer control results"))
      assertEquals(assignment, Set(topicPartition))
      assertEquals(before, Map(topicPartition -> None))
      assertEquals(stored, Map(topicPartition -> Some(first._1.offset.nextOffset)))
      assertEquals(beginning, Map(topicPartition -> first._1.record.offset))
      assertEquals(end, Map(topicPartition -> first._1.offset.nextOffset))
      assertEquals(timed, Map(topicPartition -> Some(first._1.record.offset)))
      assertEquals(missing, Map(topicPartition -> None))
      assertEquals(partitions, Set(partition))
      assertEquals(topics.get(topic), Some(partitions))
      assertEquals(replayed._1.record.offset, first._1.record.offset)
      assertEquals(replayed._1.record.value, "control-value")

  private def partitionedRoundTrip(bootstrapServer: String): IO[Unit] =
    val suffix           = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topicPrefix      = s"xkafka-partitioned-integration-$suffix"
    val firstTopic       = validTopic(s"$topicPrefix-first")
    val secondTopic      = validTopic(s"$topicPrefix-second")
    val subscription     = Subscription.Pattern(validTopicPattern(s"$topicPrefix-.*"))
    val group            = validConsumerGroup(s"xkafka-partitioned-integration-$suffix")
    val clientSettings   = ClientSettings.from(NonEmptyList.one(bootstrapServer)).toOption.get
    val producerSettings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer).toOption.get
    val consumerSettings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest).toOption.get
    val records          = NonEmptyList.of(ProducerRecord(firstTopic, "first-key", "first"), ProducerRecord(secondTopic, "second-key", "second"))

    for
      _        <- PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(records)).timeout(45.seconds)
      consumed <-
        PlatformKafkaClient().consumer(consumerSettings, subscription).use: consumer =>
          consumer.partitionedRecords(16).map: partition =>
            partition.records.take(1).map(partition.topicPartition -> _)
          .parJoinUnbounded.take(2).compile.toList
        .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka partitioned consumer timed out")))
    yield
      assertEquals(consumed.map(_._1.topic).toSet, Set(firstTopic, secondTopic))
      assert(consumed.forall((topicPartition, record) => topicPartition == record.record.topicPartition))

  private def consumeTwoAndCommitFirst(
      settings: ConsumerSettings[IO, String, String],
      topic: Topic
  ): IO[(CommittableConsumerRecord[IO, String, String], CommittableConsumerRecord[IO, String, String])] =
    PlatformKafkaClient().consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use: consumer =>
      consumer.records.take(2).compile.toList.flatMap:
        case first :: second :: Nil => first.offset.commit.as((first, second))
        case records                => IO.raiseError(new AssertionError(s"expected two records, got ${records.size}"))
    .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer timed out")))

  private def consumeOne(
      settings: ConsumerSettings[IO, String, String],
      subscription: Subscription
  ): IO[CommittableConsumerRecord[IO, String, String]] =
    PlatformKafkaClient().consumer(settings, subscription).use(_.records.take(1).compile.lastOrError)
      .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer timed out")))

  private def consume(
      settings: ConsumerSettings[IO, String, String],
      subscription: Subscription,
      count: Long
  ): IO[List[CommittableConsumerRecord[IO, String, String]]] =
    PlatformKafkaClient().consumer(settings, subscription).use(_.records.take(count).compile.toList)
      .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer timed out")))

  private def consumeAndCommitBatch(
      settings: ConsumerSettings[IO, String, String],
      subscription: Subscription,
      count: Int
  ): IO[List[CommittableConsumerRecord[IO, String, String]]] =
    PlatformKafkaClient().consumer(settings, subscription).use: consumer =>
      consumer.records.take(count.toLong).compile.toList.flatMap: records =>
        Stream.emits(records.map(_.offset)).covary[IO].through(commitBatchWithin(count, 1.minute)).compile.drain.as(records)
    .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("Kafka consumer timed out")))

  private val utf8Serializer   = Serializer.utf8[IO]
  private val utf8Deserializer = Deserializer.utf8[IO]

  private def validTopic(value: String): Topic = Topic.from(value).fold(error => fail(s"invalid test topic: $error"), identity)

  private def validTopicPattern(value: String): TopicPattern =
    TopicPattern.from(value).fold(error => fail(s"invalid test topic pattern: $error"), identity)

  private def validConsumerGroup(value: String): ConsumerGroup =
    ConsumerGroup.from(value).fold(error => fail(s"invalid test consumer group: $error"), identity)
