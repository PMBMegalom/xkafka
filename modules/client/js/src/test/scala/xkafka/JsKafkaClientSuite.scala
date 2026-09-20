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

import scala.scalajs.js
import scala.scalajs.js.JSConverters.*
import scala.scalajs.js.typedarray.Uint8Array

import cats.data.NonEmptyList
import cats.effect.{Deferred, IO, Ref}
import cats.effect.std.Dispatcher
import fs2.Chunk
import internal.confluent
import munit.CatsEffectSuite

final class JsKafkaClientSuite extends CatsEffectSuite:
  test("wraps rejected backend promises"):
    Dispatcher.sequential[IO].use: dispatcher =>
      val cause    = new RuntimeException("connection failed")
      val producer =
        js.Dynamic.literal(
          connect = (() => promise[Unit](dispatcher)(IO.raiseError(cause))): js.Function0[js.Promise[Unit]],
          disconnect = (() => js.Promise.resolve(())): js.Function0[js.Promise[Unit]]
        ).asInstanceOf[confluent.Producer]
      val serializer = Serializer.const[IO, String](None)
      val settings   = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get

      interceptIO[KafkaException.BackendFailure](
        KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer)).producer(settings).use(_ => IO.unit)
      ).map: error =>
        assert(error.getCause ne null)

  test("Confluent facade uses direct librdkafka configuration"):
    val common       = dynamic(confluent.Values.kafkaConfig(js.Array("broker-1:9092", "broker-2:9092"), "client", Map("socket.timeout.ms" -> "123")))
    val producer     = dynamic(confluent.Values.producerConfig(Map("linger.ms" -> "5")))
    val consumer     = dynamic(confluent.Values.consumerConfig("group", AutoOffsetReset.Earliest, Map("fetch.wait.max.ms" -> "10")))
    val subscription = dynamic(confluent.Values.subscription(js.Array[confluent.SubscriptionTopic]("events")))
    val run          = dynamic(confluent.Values.consumerRun(_ => js.Promise.resolve(())))

    assertEquals(common.selectDynamic("bootstrap.servers").asInstanceOf[String], "broker-1:9092,broker-2:9092")
    assertEquals(common.selectDynamic("client.id").asInstanceOf[String], "client")
    assertEquals(common.selectDynamic("socket.timeout.ms").asInstanceOf[String], "123")
    assertEquals(producer.selectDynamic("linger.ms").asInstanceOf[String], "5")
    assertEquals(consumer.selectDynamic("group.id").asInstanceOf[String], "group")
    assertEquals(consumer.selectDynamic("fetch.wait.max.ms").asInstanceOf[String], "10")
    assertEquals(consumer.selectDynamic("enable.auto.commit").asInstanceOf[Boolean], false)
    assertEquals(consumer.selectDynamic("auto.offset.reset").asInstanceOf[String], "earliest")
    assert(js.isUndefined(subscription.selectDynamic("fromBeginning")))
    assert(js.isUndefined(run.selectDynamic("autoCommit")))
    assertEquals(run.selectDynamic("eachBatchAutoResolve").asInstanceOf[Boolean], false)

  test("producer delegates serialization and preserves large offsets"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        connected    <- Ref.of[IO, Int](0)
        disconnected <- Ref.of[IO, Int](0)
        sent         <- Deferred[IO, confluent.ProducerBatch]
        metadata =
          js.Dynamic.literal(topicName = "events", partition = 2, errorCode = 0, offset = "9007199254740993", timestamp = "1234")
            .asInstanceOf[confluent.RecordMetadata]
        producer =
          js.Dynamic.literal(
            connect = (() => promise(dispatcher)(connected.update(_ + 1))): js.Function0[js.Promise[Unit]],
            disconnect = (() => promise(dispatcher)(disconnected.update(_ + 1))): js.Function0[js.Promise[Unit]],
            sendBatch =
              ((batch: confluent.ProducerBatch) => promise(dispatcher)(sent.complete(batch).as(js.Array(metadata)))): js.Function1[
                confluent.ProducerBatch,
                js.Promise[js.Array[confluent.RecordMetadata]]
              ]
          ).asInstanceOf[confluent.Producer]
        record =
          ProducerRecord(
            topic = topic("events"),
            key = "key",
            value = "value",
            partition = Some(partition(2)),
            timestamp = Some(Timestamp.fromEpochMillis(1234L)),
            headers =
              Headers(
                Header("trace", Some(Chunk.array(Array[Byte](1)))),
                Header("trace", Some(Chunk.array(Array[Byte](2)))),
                Header("nullable", None)
              )
          )
        settings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer, Map("linger.ms" -> "5")).toOption.get
        result <-
          KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer, expectedProducerProperties = Map("linger.ms" -> "5")))
            .producer(settings).use(_.produce(NonEmptyList.one(record)))
        connectedCount    <- connected.get
        disconnectedCount <- disconnected.get
        batch             <- sent.get
        _                 <-
          IO:
            assertEquals(connectedCount, 1)
            assertEquals(disconnectedCount, 1)
            assertEquals(result.records, NonEmptyList.one(record))
            assertEquals(result.metadata.map(_.offset.map(_.value)), List(Some(9007199254740993L)))

            val topicMessages = dynamic(batch).topicMessages.asInstanceOf[js.Array[js.Dynamic]]
            assertEquals(topicMessages.length, 1)
            assertEquals(topicMessages(0).topic.asInstanceOf[String], "events")

            val messages = topicMessages(0).messages.asInstanceOf[js.Array[js.Dynamic]]
            assertEquals(messages.length, 1)
            assertEquals(messages(0).partition.asInstanceOf[Int], 2)
            assertEquals(messages(0).timestamp.asInstanceOf[String], "1234")
            assertEquals(byteVector(messages(0).key.asInstanceOf[Uint8Array]), "key".getBytes("UTF-8").toVector)

            val headers = messages(0).headers.asInstanceOf[js.Dictionary[js.Any]]
            val traces  = headers("trace").asInstanceOf[js.Array[Uint8Array]]
            assertEquals(traces.map(byteVector).toVector, Vector(Vector(1.toByte), Vector(2.toByte)))
            assertEquals(headers("nullable"), null)
      yield ()

  test("consumer decodes a batch and commits the exact next offset"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        connected          <- Ref.of[IO, Int](0)
        disconnected       <- Ref.of[IO, Int](0)
        runConfig          <- Deferred[IO, confluent.ConsumerRunConfig]
        committed          <- Ref.of[IO, Vector[(String, Int, String)]](Vector.empty)
        resolved           <- Deferred[IO, String]
        seeked             <- Deferred[IO, (String, Int, String)]
        requestedTimestamp <- Deferred[IO, Double]
        adminConnected     <- Ref.of[IO, Int](0)
        adminDisconnected  <- Ref.of[IO, Int](0)
        admin =
          js.Dynamic.literal(
            connect = (() => promise(dispatcher)(adminConnected.update(_ + 1))): js.Function0[js.Promise[Unit]],
            disconnect = (() => promise(dispatcher)(adminDisconnected.update(_ + 1))): js.Function0[js.Promise[Unit]],
            fetchTopicMetadata =
              (
                  (_: confluent.TopicMetadataOptions) =>
                    js.Promise.resolve(js.Array(
                      js.Dynamic
                        .literal(name = "events", partitions = js.Array(js.Dynamic.literal(partitionId = 4), js.Dynamic.literal(partitionId = 5)))
                        .asInstanceOf[confluent.TopicMetadata],
                      js.Dynamic.literal(name = "other", partitions = js.Array(js.Dynamic.literal(partitionId = 0)))
                        .asInstanceOf[confluent.TopicMetadata]
                    ))
              ): js.Function1[confluent.TopicMetadataOptions, js.Promise[js.Array[confluent.TopicMetadata]]],
            fetchTopicOffsets =
              (
                  (_: String) =>
                    js.Promise.resolve(js.Array(
                      js.Dynamic.literal(partition = 4, offset = "9007199254740995", high = "9007199254740995", low = "9007199254740992")
                        .asInstanceOf[confluent.TopicOffsets],
                      js.Dynamic.literal(partition = 5, offset = "10", high = "10", low = "0").asInstanceOf[confluent.TopicOffsets]
                    ))
              ): js.Function1[String, js.Promise[js.Array[confluent.TopicOffsets]]],
            fetchTopicOffsetsByTimestamp =
              (
                  (_: String, timestamp: Double) =>
                    promise(dispatcher)(requestedTimestamp.complete(timestamp).void.as(js.Array(
                      confluent.Values.topicPartitionOffset("events", 4, "9007199254740994"),
                      confluent.Values.topicPartitionOffset("events", 5, "10")
                    )))
              ): js.Function2[String, Double, js.Promise[js.Array[confluent.TopicPartitionOffset]]]
          ).asInstanceOf[confluent.Admin]
        consumer =
          js.Dynamic.literal(
            connect = (() => promise(dispatcher)(connected.update(_ + 1))): js.Function0[js.Promise[Unit]],
            disconnect = (() => promise(dispatcher)(disconnected.update(_ + 1))): js.Function0[js.Promise[Unit]],
            subscribe = ((_: confluent.ConsumerSubscribe) => js.Promise.resolve(())): js.Function1[confluent.ConsumerSubscribe, js.Promise[Unit]],
            run =
              ((config: confluent.ConsumerRunConfig) => promise(dispatcher)(runConfig.complete(config).void)): js.Function1[
                confluent.ConsumerRunConfig,
                js.Promise[Unit]
              ],
            commitOffsets =
              (
                  (offsets: js.Array[confluent.TopicPartitionOffset]) =>
                    promise(dispatcher)(committed.set(offsets.toVector.map(topicPartitionOffset)))
              ): js.Function1[js.Array[confluent.TopicPartitionOffset], js.Promise[Unit]],
            assignment = (() => js.Array(confluent.Values.topicPartition("events", 4))): js.Function0[js.Array[confluent.TopicPartition]],
            committed =
              (
                  (_: js.Array[confluent.TopicPartition]) =>
                    js.Promise.resolve(js.Array(confluent.Values.topicPartitionOffset("events", 4, "9007199254740994")))
              ): js.Function1[js.Array[confluent.TopicPartition], js.Promise[js.Array[confluent.TopicPartitionOffset]]],
            dependentAdmin = (() => admin): js.Function0[confluent.Admin],
            seek =
              (
                  (offset: confluent.TopicPartitionOffset) => dispatcher.unsafeRunAndForget(seeked.complete(topicPartitionOffset(offset)).void)
              ): js.Function1[confluent.TopicPartitionOffset, Unit]
          ).asInstanceOf[confluent.Consumer]
        settings =
          ConsumerSettings
            .from(clientSettings, consumerGroup("tests"), utf8Deserializer, utf8Deserializer, properties = Map("fetch.min.bytes" -> "2")).toOption.get
        result <-
          KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer, expectedConsumerProperties = Map("fetch.min.bytes" -> "2")))
            .consumer(settings, Subscription.Topics(NonEmptyList.one(topic("events")))).use: portable =>
              for
                config <- runConfig.get
                callback = dynamic(config).eachBatch.asInstanceOf[js.Function1[confluent.EachBatchPayload, js.Promise[Unit]]]
                payload  =
                  consumerPayload(
                    topic = "events",
                    partition = 4,
                    offset = "9007199254740993",
                    timestamp = "5678",
                    key = "key",
                    value = "value",
                    resolveOffset = offset => dispatcher.unsafeRunAndForget(resolved.complete(offset).void)
                  )
                _          <- IO.fromFuture(IO(callback(payload).toFuture))
                record     <- portable.records.take(1).compile.lastOrError
                _          <- record.offset.commit
                assignment <- portable.assignment
                stored     <- portable.committed(Set(record.record.topicPartition))
                beginning  <- portable.beginningOffsets(Set(record.record.topicPartition))
                end        <- portable.endOffsets(Set(record.record.topicPartition))
                otherTopicPartition = TopicPartition(record.record.topicPartition.topic, partition(5))
                timed <-
                  portable.offsetsForTimes(
                    Map(record.record.topicPartition -> Timestamp.fromEpochMillis(5678L), otherTopicPartition -> Timestamp.fromEpochMillis(5678L))
                  )
                partitions     <- portable.partitionsFor(record.record.topicPartition.topic)
                topics         <- portable.listTopics
                _              <- portable.seek(record.record.topicPartition, record.record.offset)
                seekedOffset   <- seeked.get
                resolvedOffset <- resolved.get
                timestampValue <- requestedTimestamp.get
              yield (record, assignment, stored, beginning, end, timed, partitions, topics, seekedOffset, resolvedOffset, timestampValue)
        connectedCount         <- connected.get
        disconnectedCount      <- disconnected.get
        adminConnectedCount    <- adminConnected.get
        adminDisconnectedCount <- adminDisconnected.get
        committedOffsets       <- committed.get
        (record, assignment, stored, beginning, end, timed, partitions, topics, seekedOffset, resolvedOffset, timestampValue) = result
        _ <-
          IO:
            assertEquals(connectedCount, 1)
            assertEquals(disconnectedCount, 1)
            assertEquals(adminConnectedCount, 5)
            assertEquals(adminDisconnectedCount, 5)
            assertEquals(record.record.topicPartition.topic, topic("events"))
            assertEquals(record.record.topicPartition.partition, partition(4))
            assertEquals(record.record.offset.value, 9007199254740993L)
            assertEquals(record.record.key, "key")
            assertEquals(record.record.value, "value")
            assertEquals(record.offset.nextOffset.value, 9007199254740994L)
            assertEquals(assignment, Set(record.record.topicPartition))
            assertEquals(stored, Map(record.record.topicPartition -> Some(record.offset.nextOffset)))
            assertEquals(beginning, Map(record.record.topicPartition -> Offset.from(9007199254740992L).toOption.get))
            assertEquals(end, Map(record.record.topicPartition -> Offset.from(9007199254740995L).toOption.get))
            assertEquals(
              timed,
              Map(
                record.record.topicPartition                                     -> Some(record.offset.nextOffset),
                TopicPartition(record.record.topicPartition.topic, partition(5)) -> None
              )
            )
            assertEquals(partitions, Set(partition(4), partition(5)))
            assertEquals(topics, Map(topic("events") -> partitions, topic("other") -> Set(partition(0))))
            assertEquals(seekedOffset, ("events", 4, "9007199254740993"))
            assertEquals(resolvedOffset, "9007199254740993")
            assertEquals(timestampValue, 5678d)
            assertEquals(committedOffsets, Vector(("events", 4, "9007199254740994")))
      yield ()

  private val clientSettings = ClientSettings.from(NonEmptyList.one("localhost:9092"), Some("tests")).toOption.get

  private val utf8Serializer: Serializer[IO, String] = Serializer.instance((_, _, value) => IO.pure(Some(Chunk.array(value.getBytes("UTF-8")))))

  private val utf8Deserializer: Deserializer[IO, String] =
    Deserializer.instance((_, _, value) => IO.pure(value.fold("")(bytes => new String(bytes.toArray, "UTF-8"))))

  private def driver(
      producerValue: confluent.Producer = null,
      consumerValue: confluent.Consumer = null,
      expectedProducerProperties: Map[String, String] = Map.empty,
      expectedConsumerProperties: Map[String, String] = Map.empty
  ): ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.Producer =
        assertEquals(properties, expectedProducerProperties)
        producerValue

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String]
      ): confluent.Consumer =
        assertEquals(properties, expectedConsumerProperties)
        consumerValue

  private def consumerPayload(
      topic: String,
      partition: Int,
      offset: String,
      timestamp: String,
      key: String,
      value: String,
      resolveOffset: String => Unit
  ): confluent.EachBatchPayload =
    val message =
      js.Dynamic.literal(
        key = uint8(key),
        value = uint8(value),
        timestamp = timestamp,
        attributes = 0,
        offset = offset,
        size = key.length + value.length,
        headers = js.Dictionary[js.Any]("trace" -> uint8("header"))
      )
    val batch = js.Dynamic.literal(topic = topic, partition = partition, messages = js.Array(message))
    js.Dynamic.literal(
      batch = batch,
      isRunning = (() => true): js.Function0[Boolean],
      isStale = (() => false): js.Function0[Boolean],
      resolveOffset = ((offset: String) => resolveOffset(offset)): js.Function1[String, Unit]
    ).asInstanceOf[confluent.EachBatchPayload]

  private def promise[A](dispatcher: Dispatcher[IO])(value: IO[A]): js.Promise[A] = dispatcher.unsafeToFuture(value).toJSPromise

  private def topicPartitionOffset(raw: confluent.TopicPartitionOffset): (String, Int, String) =
    val value = dynamic(raw)
    (value.topic.asInstanceOf[String], value.partition.asInstanceOf[Int], value.offset.asInstanceOf[String])

  private def topic(value: String): Topic = Topic.from(value).fold(error => fail(error.toString), identity)

  private def partition(value: Int): Partition = Partition.from(value).fold(error => fail(error.toString), identity)

  private def consumerGroup(value: String): ConsumerGroup = ConsumerGroup.from(value).fold(error => fail(error.toString), identity)

  private def uint8(value: String): Uint8Array =
    val bytes  = value.getBytes("UTF-8")
    val result = new Uint8Array(bytes.length)
    bytes.iterator.zipWithIndex.foreach:
      case (byte, index) => result(index) = byte.toShort
    result

  private def byteVector(value: Uint8Array): Vector[Byte] = Vector.tabulate(value.length)(index => value(index).toByte)

  private def dynamic(value: Any): js.Dynamic = value.asInstanceOf[js.Dynamic]
