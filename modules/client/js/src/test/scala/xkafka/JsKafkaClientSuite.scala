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
  test("wraps librdkafka failures with their code and classification"):
    val failure  = js.Dynamic.literal(message = "connection failed", code = -195, isRetriable = true, isFatal = false).asInstanceOf[confluent.RdError]
    val producer =
      js.Dynamic.literal(
        connect =
          ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(failure, ())): js.Function2[
            js.Any,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        disconnect =
          ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function1[
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
        on =
          ((_: String, _: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) => ()): js.Function2[
            String,
            js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit],
            Unit
          ]
      ).asInstanceOf[confluent.RdProducer]
    val serializer = Serializer.const[IO, String](None)
    val settings   = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get

    interceptIO[KafkaException.BackendFailure](
      KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer)).producer(settings).use(_ => IO.unit)
    ).map: error =>
      assertEquals(error.detail, "connection failed")
      assertEquals(error.code, Some("-195"))
      assertEquals(error.retriable, Some(true))
      assertEquals(error.fatal, Some(false))

  test("librdkafka configuration is built directly, with the managed keys derived from typed settings"):
    val producer = dynamic(confluent.Values.rdProducerConfig(js.Array("broker-1:9092", "broker-2:9092"), "client", Map("linger.ms" -> "5")))
    val consumer =
      dynamic(
        confluent.Values.rdConsumerConfig(js.Array("broker-1:9092"), "client", "group", AutoOffsetReset.Earliest, Map("fetch.wait.max.ms" -> "10"))
      )

    assertEquals(producer.selectDynamic("bootstrap.servers").asInstanceOf[String], "broker-1:9092,broker-2:9092")
    assertEquals(producer.selectDynamic("client.id").asInstanceOf[String], "client")
    assertEquals(producer.selectDynamic("linger.ms").asInstanceOf[String], "5")
    // Delivery reports are what make per-record metadata possible.
    assertEquals(producer.selectDynamic("dr_cb").asInstanceOf[Boolean], true)
    assertEquals(consumer.selectDynamic("group.id").asInstanceOf[String], "group")
    assertEquals(consumer.selectDynamic("fetch.wait.max.ms").asInstanceOf[String], "10")
    assertEquals(consumer.selectDynamic("enable.auto.commit").asInstanceOf[Boolean], false)
    assertEquals(consumer.selectDynamic("auto.offset.reset").asInstanceOf[String], "earliest")

  test("producer enqueues each record with its own opaque and reports metadata per record"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        connected    <- Ref.of[IO, Int](0)
        disconnected <- Ref.of[IO, Int](0)
        produced     <- IO(js.Array[js.Dynamic]())
        reporter     <- Deferred[IO, js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]]
        producer =
          js.Dynamic.literal(
            connect =
              (
                  (_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
                    dispatcher.unsafeRunAndForget(connected.update(_ + 1) >> IO(done(null, ())))
              ): js.Function2[js.Any, js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
            disconnect =
              (
                  (done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
                    dispatcher.unsafeRunAndForget(disconnected.update(_ + 1) >> IO(done(null, ())))
              ): js.Function1[js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
            setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
            on =
              (
                  (_: String, listener: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) =>
                    dispatcher.unsafeRunAndForget(reporter.complete(listener).void)
              ): js.Function2[String, js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit], Unit],
            produce =
              (
                  (topic: String, partition: js.Any, value: js.Any, key: js.Any, timestamp: js.Any, opaque: js.Any, headers: js.Any) =>
                    produced.push(js.Dynamic.literal(
                      topic = topic,
                      partition = partition,
                      value = value,
                      key = key,
                      timestamp = timestamp,
                      opaque = opaque,
                      headers = headers
                    )): Unit
              ): js.Function7[String, js.Any, js.Any, js.Any, js.Any, js.Any, js.Any, Unit]
          ).asInstanceOf[confluent.RdProducer]
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
                Header("other", Some(Chunk.array(Array[Byte](9)))),
                Header("trace", Some(Chunk.array(Array[Byte](2))))
              )
          )
        settings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer, Map("linger.ms" -> "5")).toOption.get
        result <-
          KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer, expectedProducerProperties = Map("linger.ms" -> "5")))
            .producer(settings).use: underlying =>
              for
                acknowledgement <- underlying.produce(NonEmptyList.one(record))
                listener        <- reporter.get
                opaque = produced(0).opaque
                _ <-
                  IO(listener(
                    null,
                    js.Dynamic.literal(topic = "events", partition = 2, offset = 41d, timestamp = 1234d, opaque = opaque)
                      .asInstanceOf[confluent.RdDeliveryReport]
                  ))
                value <- acknowledgement
              yield value
        connectedCount    <- connected.get
        disconnectedCount <- disconnected.get
        _                 <-
          IO:
            assertEquals(connectedCount, 1)
            assertEquals(disconnectedCount, 1)
            assertEquals(produced.length, 1)
            assertEquals(produced(0).topic.asInstanceOf[String], "events")
            assertEquals(produced(0).partition.asInstanceOf[Int], 2)
            assertEquals(produced(0).timestamp.asInstanceOf[Double], 1234d)
            assertEquals(byteVector(produced(0).key.asInstanceOf[Uint8Array]), "key".getBytes("UTF-8").toVector)

            // librdkafka takes headers as an ordered array, so duplicate names keep their produced order.
            val headers = produced(0).headers.asInstanceOf[js.Array[js.Dictionary[Uint8Array]]]
            assertEquals(headers.length, 3)
            assertEquals(headers.toList.flatMap(_.keys.toList), List("trace", "other", "trace"))
            assertEquals(headers.toList.flatMap(_.values.toList).map(byteVector), List(Vector(1.toByte), Vector(9.toByte), Vector(2.toByte)))

            assertEquals(result.records.map((value, _) => value), NonEmptyList.one(record))
            assertEquals(result.records.toList.map((_, metadata) => metadata.flatMap(_.offset.map(_.value))), List(Some(41L)))
      yield ()

  test("consumer decodes a pulled batch, keeps header order, and commits the exact next offset"):
    for
      committed <- IO(js.Array[js.Dynamic]())
      delivered <- IO(js.Array[confluent.RdMessage]())
      message =
        js.Dynamic.literal(
          topic = "events",
          partition = 2,
          offset = 41d,
          key = uint8("key"),
          value = uint8("value"),
          timestamp = 1234d,
          headers = js.Array(rdHeader("trace", Array[Byte](1)), rdHeader("other", Array[Byte](9)), rdHeader("trace", Array[Byte](2)))
        ).asInstanceOf[confluent.RdMessage]
      _ <- IO(delivered.push(message): Unit)
      consumer =
        js.Dynamic.literal(
          connect =
            ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
              js.Any,
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          disconnect =
            ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function1[
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          setDefaultConsumeTimeout = ((_: Int) => ()): js.Function1[Int, Unit],
          subscribe = ((_: js.Array[confluent.SubscriptionTopic]) => ()): js.Function1[js.Array[confluent.SubscriptionTopic], Unit],
          consume =
            (
                (_: Int, done: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) =>
                  done(null, delivered.splice(0, delivered.length).toJSArray)
            ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit], Unit],
          commit =
            ((offsets: js.Array[confluent.RdTopicPartitionOffset]) => offsets.foreach(value => committed.push(dynamic(value)): Unit)): js.Function1[
              js.Array[confluent.RdTopicPartitionOffset],
              Unit
            ]
        ).asInstanceOf[confluent.RdConsumer]
      settings =
        ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest, Map("fetch.wait.max.ms" -> "10"))
          .toOption.get
      record <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer, expectedConsumerProperties = Map("fetch.wait.max.ms" -> "10")))
          .consumer(settings, Subscription.Topics(NonEmptyList.one(topic("events")))).use(_.records.take(1).compile.lastOrError)
      _ <- record.offset.commit
    yield
      assertEquals(record.record.topicPartition, TopicPartition(topic("events"), partition(2)))
      assertEquals(record.record.key, "key")
      assertEquals(record.record.value, "value")
      assertEquals(record.record.offset.value, 41L)
      assertEquals(record.record.timestamp.map(_.epochMillis), Some(1234L))

      // librdkafka returns one object per header, so duplicate names keep their order.
      assertEquals(record.record.headers.values.map(_.key), Vector("trace", "other", "trace"))
      assertEquals(record.record.headers.values.map(_.value.map(_.toList)), Vector(Some(List[Byte](1)), Some(List[Byte](9)), Some(List[Byte](2))))

      assertEquals(record.offset.nextOffset.value, 42L)
      assertEquals(committed.length, 1)
      assertEquals(committed(0).topic.asInstanceOf[String], "events")
      assertEquals(committed(0).partition.asInstanceOf[Int], 2)
      assertEquals(committed(0).offset.asInstanceOf[Double], 42d)

  private val clientSettings = ClientSettings.from(NonEmptyList.one("localhost:9092"), Some("tests")).toOption.get

  private val utf8Serializer: Serializer[IO, String] = Serializer.instance((_, _, value) => IO.pure(Some(Chunk.array(value.getBytes("UTF-8")))))

  private val utf8Deserializer: Deserializer[IO, String] =
    Deserializer.instance((_, _, value) => IO.pure(value.fold("")(bytes => new String(bytes.toArray, "UTF-8"))))

  private def driver(
      producerValue: confluent.RdProducer = null,
      consumerValue: confluent.RdConsumer = null,
      expectedProducerProperties: Map[String, String] = Map.empty,
      expectedConsumerProperties: Map[String, String] = Map.empty
  ): ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer =
        assertEquals(properties, expectedProducerProperties)
        producerValue

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String]
      ): confluent.RdConsumer =
        assertEquals(properties, expectedConsumerProperties)
        consumerValue

  private def rdHeader(name: String, value: Array[Byte]): confluent.RdHeader =
    val bytes = new Uint8Array(value.length)
    value.zipWithIndex.foreach((byte, index) => bytes(index) = byte.toShort)
    val result = js.Dictionary.empty[Uint8Array | String]
    result(name) = bytes
    result

  private def topic(value: String): Topic = Topic.from(value).fold(error => fail(error.toString), identity)

  private def partition(value: Int): Partition = Partition.from(value).fold(error => fail(error.toString), identity)

  private val group = ConsumerGroup.from("workers").fold(error => fail(error.toString), identity)

  private def uint8(value: String): Uint8Array =
    val bytes  = value.getBytes("UTF-8")
    val result = new Uint8Array(bytes.length)
    bytes.iterator.zipWithIndex.foreach:
      case (byte, index) => result(index) = byte.toShort
    result

  private def byteVector(value: Uint8Array): Vector[Byte] = Vector.tabulate(value.length)(index => value(index).toByte)

  private def dynamic(value: Any): js.Dynamic = value.asInstanceOf[js.Dynamic]
