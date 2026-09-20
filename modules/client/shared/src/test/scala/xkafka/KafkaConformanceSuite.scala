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
import cats.syntax.all.*
import fs2.Chunk
import munit.{CatsEffectSuite, TestOptions}

/** Behaviour every backend is expected to share, asserted identically on each one.
  *
  * A case listed in `divergent` is marked as expected to fail on the named backends. Fixing the backend makes the marked case fail, so a marker
  * cannot outlive the divergence it records.
  */
final class KafkaConformanceSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val backend          = PlatformKafkaClient.name
  private val bootstrapServers = PlatformKafkaClient.integrationBootstrapServers

  private def conformance(name: String, divergent: Set[String] = Set.empty): TestOptions =
    val base = if divergent.contains(backend) then TestOptions(name).fail else TestOptions(name)
    if bootstrapServers.isEmpty then base.ignore else base

  private def withBroker(run: String => IO[Unit]): IO[Unit] = bootstrapServers.fold(IO.unit)(run)

  test(conformance("records keep their produced order within a partition")):
    withBroker: server =>
      val topic     = uniqueTopic("order")
      val partition = validPartition(0)
      val values    = List("one", "two", "three", "four", "five")
      val records   = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), partition)))

      for
        _        <- produce(server, records)
        consumed <- consume(server, topic, values.size)
      yield
        assertEquals(consumed.map(_.record.value), values.map(Some(_)))
        assertEquals(consumed.map(_.record.offset.value), consumed.map(_.record.offset.value).sorted)
        assertEquals(consumed.map(_.record.offset.value), List.range(0L, values.size.toLong))

  test(conformance("a null value round-trips as a tombstone")):
    withBroker: server =>
      val topic     = uniqueTopic("tombstone")
      val tombstone = record(topic, Some("key"), None, validPartition(0))

      for
        _        <- produce(server, NonEmptyList.one(tombstone))
        consumed <- consume(server, topic, 1)
      yield
        assertEquals(consumed.map(_.record.key), List(Some("key")))
        assertEquals(consumed.map(_.record.value), List(None))

  test(conformance("a null key round-trips as an absent key")):
    withBroker: server =>
      val topic   = uniqueTopic("nullkey")
      val keyless = record(topic, None, Some("value"), validPartition(0))

      for
        _        <- produce(server, NonEmptyList.one(keyless))
        consumed <- consume(server, topic, 1)
      yield
        assertEquals(consumed.map(_.record.key), List(None))
        assertEquals(consumed.map(_.record.value), List(Some("value")))

  // JavaScript encodes headers through a js.Dictionary keyed by header name, which cannot represent
  // duplicate keys interleaved with other keys in their produced order.
  test(conformance("headers keep their produced order, including duplicate names", divergent = Set("js"))):
    withBroker: server =>
      val topic   = uniqueTopic("headers")
      val headers = Headers(Header("a", byteHeader(1)), Header("b", byteHeader(2)), Header("a", byteHeader(3)))
      val headed  = record(topic, Some("key"), Some("value"), validPartition(0), headers)

      for
        _        <- produce(server, NonEmptyList.one(headed))
        consumed <- consume(server, topic, 1)
      yield
        val observed = consumed.head.record.headers.values.map(header => header.key -> header.value.map(_.toList)).toList
        val expected = List[(String, Option[List[Byte]])](("a", Some(List[Byte](1))), ("b", Some(List[Byte](2))), ("a", Some(List[Byte](3))))
        assertEquals(observed, expected)

  test(conformance("producing reports metadata for every record")):
    withBroker: server =>
      val topic     = uniqueTopic("metadata")
      val partition = validPartition(0)
      val records   = NonEmptyList.of("a", "b", "c").map(value => record(topic, Some("k"), Some(value), partition))

      produce(server, records).map: result =>
        assertEquals(result.records.size, records.size)
        assert(result.records.forall((_, metadata) => metadata.isDefined), "every record should carry its own metadata")
        assertEquals(result.metadata.flatMap(_.offset.map(_.value)), List(0L, 1L, 2L))

  test(conformance("acknowledgements can be held while further batches are enqueued")):
    withBroker: server =>
      val topic     = uniqueTopic("pipeline")
      val partition = validPartition(0)
      val first     = NonEmptyList.of("a", "b").map(value => record(topic, Some("k"), Some(value), partition))
      val second    = NonEmptyList.of("c", "d").map(value => record(topic, Some("k"), Some(value), partition))

      producerSettings(server).flatMap: settings =>
        PlatformKafkaClient().producer(settings).use: producer =>
          for
            firstAck     <- producer.produce(first)
            secondAck    <- producer.produce(second)
            firstResult  <- firstAck
            secondResult <- secondAck
            consumed     <- consume(server, topic, 4)
          yield
            assertEquals(firstResult.records.size, 2)
            assertEquals(secondResult.records.size, 2)
            assertEquals(consumed.map(_.record.value), List("a", "b", "c", "d").map(Some(_)))
        .timeout(60.seconds)

  test(conformance("a consumer resource can be allocated and released repeatedly")):
    withBroker: server =>
      val topic = uniqueTopic("lifecycle")

      for
        _ <- produce(server, NonEmptyList.one(record(topic, Some("key"), Some("value"), validPartition(0))))
        _ <-
          List.range(0, 3).traverse_ { _ =>
            consumerSettings(server, uniqueGroup("lifecycle")).flatMap: settings =>
              PlatformKafkaClient().consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use(_.assignment.void).timeout(60.seconds)
          }
      yield ()

  private def produce(
      server: String,
      records: NonEmptyList[ProducerRecord[Option[String], Option[String]]]
  ): IO[ProducerResult[Option[String], Option[String]]] =
    producerSettings(server).flatMap: settings =>
      PlatformKafkaClient().producer(settings).use(_.produceAndAwait(records)).timeout(60.seconds)

  private def consume(server: String, topic: Topic, count: Int): IO[List[CommittableConsumerRecord[IO, Option[String], Option[String]]]] =
    consumerSettings(server, uniqueGroup("conformance")).flatMap: settings =>
      PlatformKafkaClient().consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use(_.records.take(count.toLong).compile.toList)
        .timeout(60.seconds)

  private def producerSettings(server: String): IO[ProducerSettings[IO, Option[String], Option[String]]] =
    validated(ClientSettings.from(NonEmptyList.one(server))).flatMap: client =>
      validated(ProducerSettings.from(client, optionalSerializer, optionalSerializer))

  private def consumerSettings(server: String, group: ConsumerGroup): IO[ConsumerSettings[IO, Option[String], Option[String]]] =
    validated(ClientSettings.from(NonEmptyList.one(server))).flatMap: client =>
      validated(ConsumerSettings.from(client, group, optionalDeserializer, optionalDeserializer, AutoOffsetReset.Earliest))

  private val optionalSerializer   = Serializer.utf8[IO].option
  private val optionalDeserializer = Deserializer.utf8[IO].option

  private def validated[A](result: cats.data.ValidatedNel[SettingsError, A]): IO[A] =
    IO.fromEither(result.toEither.leftMap(errors => new IllegalArgumentException(errors.toList.map(_.message).mkString("; "))))

  private def record(
      topic: Topic,
      key: Option[String],
      value: Option[String],
      partition: Partition,
      headers: Headers = Headers.empty
  ): ProducerRecord[Option[String], Option[String]] = ProducerRecord(topic, key, value, partition = Some(partition), headers = headers)

  private def byteHeader(value: Byte): Option[Chunk[Byte]] = Some(Chunk.array(Array(value)))

  private def uniqueTopic(label: String): Topic = validTopic(s"xkafka-conformance-$label-$backend-${System.nanoTime()}")

  private def uniqueGroup(label: String): ConsumerGroup = validGroup(s"xkafka-conformance-$label-$backend-${System.nanoTime()}")

  private def validTopic(value: String): Topic = Topic.from(value).fold(error => fail(s"invalid test topic: ${error.message}"), identity)

  private def validGroup(value: String): ConsumerGroup =
    ConsumerGroup.from(value).fold(error => fail(s"invalid test consumer group: ${error.message}"), identity)

  private def validPartition(value: Int): Partition = Partition.from(value).fold(error => fail(s"invalid test partition: ${error.message}"), identity)
