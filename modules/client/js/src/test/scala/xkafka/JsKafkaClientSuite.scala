package xkafka

import scala.concurrent.duration.*
import scala.scalajs.js
import scala.scalajs.js.typedarray.Uint8Array

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.internal.confluent

final class JsKafkaClientSuite extends CatsEffectSuite:
  test("Confluent facade uses direct librdkafka configuration") {
    val common = dynamic(
      confluent.Values.kafkaConfig(
        js.Array("broker-1:9092", "broker-2:9092"),
        "client"
      )
    )
    val consumer = dynamic(
      confluent.Values.consumerConfig("group", AutoOffsetReset.Earliest)
    )
    val subscription = dynamic(
      confluent.Values.subscription(js.Array("events"))
    )
    val run = dynamic(
      confluent.Values.consumerRun(_ => js.Promise.resolve(()))
    )

    assertEquals(
      common.selectDynamic("bootstrap.servers").asInstanceOf[String],
      "broker-1:9092,broker-2:9092"
    )
    assertEquals(common.selectDynamic("client.id").asInstanceOf[String], "client")
    assertEquals(consumer.selectDynamic("group.id").asInstanceOf[String], "group")
    assertEquals(
      consumer.selectDynamic("enable.auto.commit").asInstanceOf[Boolean],
      false
    )
    assertEquals(
      consumer.selectDynamic("auto.offset.reset").asInstanceOf[String],
      "earliest"
    )
    assert(js.isUndefined(subscription.selectDynamic("fromBeginning")))
    assert(js.isUndefined(run.selectDynamic("autoCommit")))
    assertEquals(
      run.selectDynamic("eachBatchAutoResolve").asInstanceOf[Boolean],
      false
    )
  }

  test("producer delegates serialization and preserves large offsets") {
    var connected                             = 0
    var disconnected                          = 0
    var sent: Option[confluent.ProducerBatch] = None

    val metadata = js.Dynamic
      .literal(
        topicName = "events",
        partition = 2,
        errorCode = 0,
        offset = "9007199254740993",
        timestamp = "1234"
      )
      .asInstanceOf[confluent.RecordMetadata]

    val producer = js.Dynamic
      .literal(
        connect = (() =>
          connected += 1
          js.Promise.resolve(())
        ): js.Function0[js.Promise[Unit]],
        disconnect = (() =>
          disconnected += 1
          js.Promise.resolve(())
        ): js.Function0[js.Promise[Unit]],
        sendBatch = ((batch: confluent.ProducerBatch) =>
          sent = Some(batch)
          js.Promise.resolve(js.Array(metadata))
        ): js.Function1[
          confluent.ProducerBatch,
          js.Promise[js.Array[confluent.RecordMetadata]]
        ]
      )
      .asInstanceOf[confluent.Producer]

    val record = ProducerRecord(
      topic = topic("events"),
      key = "key",
      value = "value",
      partition = Some(partition(2)),
      timestamp = Some(Timestamp.fromEpochMillis(1234L)),
      headers = Headers(
        Header("trace", Some(Chunk.array(Array[Byte](1)))),
        Header("trace", Some(Chunk.array(Array[Byte](2)))),
        Header("nullable", None)
      )
    )
    val settings = ProducerSettings(
      clientSettings,
      utf8Serializer,
      utf8Serializer
    )

    KafkaClientPlatform
      .fromDriver[IO](driver(producerValue = producer))
      .producer(settings)
      .use(_.produce(NonEmptyList.one(record)))
      .map { result =>
        assertEquals(connected, 1)
        assertEquals(result.records, NonEmptyList.one(record))
        assertEquals(result.metadata.map(_.offset.map(_.value)), List(Some(9007199254740993L)))

        val batch         = sent.getOrElse(fail("sendBatch was not called"))
        val topicMessages = dynamic(batch).topicMessages
          .asInstanceOf[js.Array[js.Dynamic]]
        assertEquals(topicMessages.length, 1)
        assertEquals(topicMessages(0).topic.asInstanceOf[String], "events")

        val messages = topicMessages(0).messages
          .asInstanceOf[js.Array[js.Dynamic]]
        assertEquals(messages.length, 1)
        assertEquals(messages(0).partition.asInstanceOf[Int], 2)
        assertEquals(messages(0).timestamp.asInstanceOf[String], "1234")
        assertEquals(
          byteVector(messages(0).key.asInstanceOf[Uint8Array]),
          "key".getBytes("UTF-8").toVector
        )

        val headers = messages(0).headers
          .asInstanceOf[js.Dictionary[js.Any]]
        val traces = headers("trace").asInstanceOf[js.Array[Uint8Array]]
        assertEquals(
          traces.map(byteVector).toVector,
          Vector(Vector(1.toByte), Vector(2.toByte))
        )
        assertEquals(headers("nullable"), null)
      }
      .flatMap(_ => IO(assertEquals(disconnected, 1)))
  }

  test("consumer decodes a batch and commits the exact next offset") {
    var connected                                      = 0
    var disconnected                                   = 0
    var runConfig: Option[confluent.ConsumerRunConfig] = None
    var committed: Vector[(String, Int, String)]       = Vector.empty

    val consumer = js.Dynamic
      .literal(
        connect = (() =>
          connected += 1
          js.Promise.resolve(())
        ): js.Function0[js.Promise[Unit]],
        disconnect = (() =>
          disconnected += 1
          js.Promise.resolve(())
        ): js.Function0[js.Promise[Unit]],
        subscribe = ((_: confluent.ConsumerSubscribe) => js.Promise.resolve(())): js.Function1[confluent.ConsumerSubscribe, js.Promise[Unit]],
        run = ((config: confluent.ConsumerRunConfig) =>
          runConfig = Some(config)
          js.Promise.resolve(())
        ): js.Function1[confluent.ConsumerRunConfig, js.Promise[Unit]],
        commitOffsets = ((offsets: js.Array[confluent.TopicPartitionOffset]) =>
          committed = offsets.toVector.map { raw =>
            val value = dynamic(raw)
            (
              value.topic.asInstanceOf[String],
              value.partition.asInstanceOf[Int],
              value.offset.asInstanceOf[String]
            )
          }
          js.Promise.resolve(())
        ): js.Function1[
          js.Array[confluent.TopicPartitionOffset],
          js.Promise[Unit]
        ]
      )
      .asInstanceOf[confluent.Consumer]

    val settings = ConsumerSettings(
      clientSettings,
      consumerGroup("tests"),
      utf8Deserializer,
      utf8Deserializer
    )
    val resource = KafkaClientPlatform
      .fromDriver[IO](driver(consumerValue = consumer))
      .consumer(settings, Subscription.Topics(NonEmptyList.one(topic("events"))))

    resource
      .use { portable =>
        waitForRunConfig(runConfig).flatMap { config =>
          val callback = dynamic(config).eachBatch.asInstanceOf[
            js.Function1[confluent.EachBatchPayload, js.Promise[Unit]]
          ]
          val payload = consumerPayload(
            topic = "events",
            partition = 4,
            offset = "9007199254740993",
            timestamp = "5678",
            key = "key",
            value = "value"
          )

          IO.fromFuture(IO(callback(payload).toFuture)) >>
            portable.records.take(1).compile.lastOrError.flatMap { record =>
              IO {
                assertEquals(record.record.topicPartition.topic, topic("events"))
                assertEquals(record.record.topicPartition.partition, partition(4))
                assertEquals(record.record.offset.value, 9007199254740993L)
                assertEquals(record.record.key, "key")
                assertEquals(record.record.value, "value")
                assertEquals(record.offset.nextOffset.value, 9007199254740994L)
              } >> record.offset.commit
            }
        }
      }
      .flatMap { _ =>
        IO {
          assertEquals(connected, 1)
          assertEquals(disconnected, 1)
          assertEquals(
            committed,
            Vector(("events", 4, "9007199254740994"))
          )
        }
      }
  }

  private val clientSettings =
    ClientSettings(NonEmptyList.one("localhost:9092"), Some("tests"))

  private val utf8Serializer: Serializer[IO, String] =
    Serializer.instance((_, _, value) => IO.pure(Some(Chunk.array(value.getBytes("UTF-8")))))

  private val utf8Deserializer: Deserializer[IO, String] =
    Deserializer.instance((_, _, value) => IO.pure(value.fold("")(bytes => new String(bytes.toArray, "UTF-8"))))

  private def driver(
      producerValue: confluent.Producer = null,
      consumerValue: confluent.Consumer = null
  ): ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings): confluent.Producer =
        producerValue

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset
      ): confluent.Consumer =
        consumerValue

  private def consumerPayload(
      topic: String,
      partition: Int,
      offset: String,
      timestamp: String,
      key: String,
      value: String
  ): confluent.EachBatchPayload =
    val message = js.Dynamic.literal(
      key = uint8(key),
      value = uint8(value),
      timestamp = timestamp,
      attributes = 0,
      offset = offset,
      size = key.length + value.length,
      headers = js.Dictionary[js.Any]("trace" -> uint8("header"))
    )
    val batch = js.Dynamic.literal(
      topic = topic,
      partition = partition,
      messages = js.Array(message)
    )
    js.Dynamic
      .literal(
        batch = batch,
        isRunning = (() => true): js.Function0[Boolean],
        isStale = (() => false): js.Function0[Boolean]
      )
      .asInstanceOf[confluent.EachBatchPayload]

  private def waitForRunConfig(
      current: => Option[confluent.ConsumerRunConfig]
  ): IO[confluent.ConsumerRunConfig] =
    current.fold(
      IO.sleep(10.millis) >> waitForRunConfig(current)
    )(IO.pure)

  private def topic(value: String): Topic =
    Topic.from(value).fold(error => fail(error.toString), identity)

  private def partition(value: Int): Partition =
    Partition.from(value).fold(error => fail(error.toString), identity)

  private def consumerGroup(value: String): ConsumerGroup =
    ConsumerGroup.from(value).fold(error => fail(error.toString), identity)

  private def uint8(value: String): Uint8Array =
    val bytes  = value.getBytes("UTF-8")
    val result = new Uint8Array(bytes.length)
    bytes.iterator.zipWithIndex.foreach { case (byte, index) =>
      result(index) = byte.toShort
    }
    result

  private def byteVector(value: Uint8Array): Vector[Byte] =
    Vector.tabulate(value.length)(index => value(index).toByte)

  private def dynamic(value: Any): js.Dynamic =
    value.asInstanceOf[js.Dynamic]
