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

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Deferred, IO}
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

  /** Made by the fixture with more than one partition, because topics created on demand get exactly one. */
  private val partitionedTopic = PlatformKafkaClient.environment("XKAFKA_INTEGRATION_PARTITIONED_TOPIC").getOrElse("xkafka-partitioned")

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

  test(conformance("headers keep their produced order, including duplicate names")):
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

  test(conformance("enqueueing does not wait for an outstanding acknowledgement")):
    withBroker: server =>
      val topic     = uniqueTopic("inflight")
      val partition = validPartition(0)
      val first     = NonEmptyList.one(record(topic, Some("k"), Some("first"), partition))
      val second    = NonEmptyList.one(record(topic, Some("k"), Some("second"), partition))

      // Lingering holds the first batch long enough that its acknowledgement is still outstanding when the
      // second is enqueued, which is the whole point of separating the two stages.
      ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
        ProducerSettings.from(client, optionalSerializer, optionalSerializer, Map("linger.ms" -> "5000")).liftTo[IO]
      .flatMap: settings =>
        PlatformKafkaClient().producer(settings).use: producer =>
          for
            firstAck  <- producer.produce(first)
            _         <- IO.sleep(250.millis)
            secondAck <- producer.produce(second).timeout(2.seconds)
            _         <- firstAck
            _         <- secondAck
          yield ()
        .timeout(60.seconds)

  test(conformance("an unreachable broker fails with the same portable classification")):
    val unreachable = "127.0.0.1:1"
    val settings    =
      ClientSettings.from(NonEmptyList.one(unreachable), properties = Map("socket.timeout.ms" -> "1000", "message.timeout.ms" -> "2000"))
        .andThen(client => ProducerSettings.from(client, optionalSerializer, optionalSerializer))

    settings.liftTo[IO].flatMap: producerSettings =>
      PlatformKafkaClient().producer(producerSettings)
        .use(_.produceAndAwait(NonEmptyList.one(record(uniqueTopic("unreachable"), Some("k"), Some("v"), validPartition(0))))).attempt.map:
          case Left(failure: KafkaException.BackendFailure) =>
            assert(failure.code.isDefined, s"expected a portable code, got ${failure.code}")
            assert(!failure.code.contains(ErrorCode.Other(-1)), s"expected a classified code, got ${failure.code}")
          case Left(other) => fail(s"expected a BackendFailure, got $other")
          case Right(_)    => fail("expected producing to an unreachable broker to fail")
    .timeout(90.seconds)

  test(conformance("a consumer resource can be allocated and released repeatedly")):
    withBroker: server =>
      val topic = uniqueTopic("lifecycle")

      for
        _ <- produce(server, NonEmptyList.one(record(topic, Some("key"), Some("value"), validPartition(0))))
        _ <-
          List.range(0, 3).traverse_ { _ =>
            consumerSettings(server, uniqueGroup("lifecycle")).flatMap: settings =>
              PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.assignment.void).timeout(60.seconds)
          }
      yield ()

  test(conformance("a second consumer in the group takes a share of the partitions")):
    withBroker: server =>
      val topic     = validTopic(partitionedTopic)
      val group     = uniqueGroup("rebalance")
      val selection = Selection.Topics(NonEmptySet.one(topic))
      val both      = Set(TopicPartition(topic, validPartition(0)), TopicPartition(topic, validPartition(1)))

      for
        settings <- consumerSettings(server, group)
        shares   <-
          PlatformKafkaClient().consumer(settings, selection).use: alone =>
            assignmentOf(alone, 2) *> PlatformKafkaClient().consumer(settings, selection).use: joined =>
              (assignmentOf(alone, 1), assignmentOf(joined, 1)).parTupled
          .timeout(45.seconds)
      yield
        val (left, right) = shares
        assertEquals(left.size, 1)
        assertEquals(right.size, 1)
        assertEquals(left ++ right, both)

  test(conformance("a consumer answers queries while its records are being consumed")):
    withBroker: server =>
      val topic     = uniqueTopic("concurrent")
      val partition = validPartition(0)
      val values    = List("one", "two", "three")
      val records   = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), partition)))

      for
        _        <- produce(server, records)
        settings <- consumerSettings(server, uniqueGroup("concurrent"))
        result   <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            (consumer.records.take(values.size.toLong).compile.toList, List.range(0, 20).traverse(_ => consumer.assignment)).parTupled
          .timeout(60.seconds)
      yield
        val (consumed, assignments) = result
        assertEquals(consumed.map(_.record.value), values.map(Some(_)))
        assertEquals(assignments.size, 20)

  test(conformance("cancelling a record stream releases the consumer")):
    withBroker: server =>
      val topic = uniqueTopic("cancel")

      for
        _        <- produce(server, NonEmptyList.one(record(topic, Some("key"), Some("value"), validPartition(0))))
        settings <- consumerSettings(server, uniqueGroup("cancel"))
        started  <- Deferred[IO, Unit]
        fiber    <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
            .use(_.records.evalTap(_ => started.complete(()).void).compile.drain).start
        // Cancelling once a record has arrived leaves a poll in flight, which is what has to unwind.
        _        <- started.get.timeout(90.seconds)
        _        <- fiber.cancel.timeout(90.seconds)
        replayed <- consume(server, topic, 1)
      yield assertEquals(replayed.map(_.record.value), List(Some("value")))

  test(conformance("a topic created after a consumer subscribed arrives within the metadata refresh interval")):
    withBroker: server =>
      val topic   = uniqueTopic("late")
      val refresh = 5.seconds
      val bound   = 60.seconds
      // No partition is named, because librdkafka cannot place a record on a partition of a topic it has never seen.
      val late = ProducerRecord[Option[String], Option[String]](topic, Some("key"), Some("value"))

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].map(_.withMetadataRefreshInterval(refresh))
        settings <-
          ConsumerSettings.from(client, uniqueGroup("late"), optionalDeserializer, optionalDeserializer, AutoOffsetReset.Earliest).liftTo[IO]
        outcome <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            // Nothing has created the topic at selection time, so the record can only arrive once metadata is refreshed. A
            // backend left on the five minute default outlasts the bound below.
            IO.both(consumer.records.head.compile.lastOrError, IO.sleep(2.seconds) *> produce(server, NonEmptyList.one(late))).timed
              .timeout(bound + 30.seconds)
        (elapsed, arrivals) = outcome
      yield
        assertEquals(arrivals._1.record.value, Some("value"))
        assert(elapsed < bound, s"$backend took $elapsed to see a topic created after subscribing, with a $refresh refresh interval")

  test(conformance("seeking to the beginning replays a partition a consumer would otherwise skip")):
    withBroker: server =>
      val topic     = uniqueTopic("ends")
      val partition = validPartition(0)
      val values    = List("one", "two", "three")
      val records   = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), partition)))

      for
        _      <- produce(server, records)
        client <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        // Starting at the latest offset leaves nothing waiting to be read, so whatever arrives arrives because of the seek.
        settings <- ConsumerSettings.from(client, uniqueGroup("ends"), optionalDeserializer, optionalDeserializer, AutoOffsetReset.Latest).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            val topicPartition = TopicPartition(topic, partition)
            for
              // A seek can only name a partition this consumer already owns.
              _        <- assignmentOf(consumer, 1)
              _        <- whenSeekable(consumer.seekToEnd(Set(topicPartition)))
              _        <- whenSeekable(consumer.seekToBeginning(Set(topicPartition)))
              consumed <- consumer.records.take(values.size.toLong).compile.toList
              settled  <- consumer.position(topicPartition)
            yield (consumed.map(_.record.value), consumed.map(_.record.offset.value), settled)
          .timeout(90.seconds)
      yield
        val (consumed, offsets, settled) = outcome
        assertEquals(consumed, values.map(Some(_)), "seeking to the beginning should replay every record")
        assertEquals(offsets, List.range(0L, values.size.toLong))
        assertEquals(settled.map(_.value), Some(values.size.toLong), "the position should follow what this consumer consumed")

  /** The Java client joins a group from its poll, and librdkafka joins from its subscribe, so a consumer nobody reads holds partitions on one and not
    * the other. Neither protocol version changes this, and librdkafka never starts its own idle timer for such a consumer, so the partitions it holds
    * are not handed back.
    */
  test(conformance("a consumer that is never read leaves the whole topic to one that is", divergent = Set("js", "native"))):
    withBroker: server =>
      val topic     = validTopic(partitionedTopic)
      val group     = uniqueGroup("idle")
      val selection = Selection.Topics(NonEmptySet.one(topic))

      for
        settings <- consumerSettings(server, group)
        settled  <-
          PlatformKafkaClient().consumer(settings, selection).use: _ =>
            PlatformKafkaClient().consumer(settings, selection).use: reading =>
              for
                _ <- reading.assignmentChanges.filter(_.nonEmpty).head.compile.lastOrError
                // A member that joined without being read would take its share through a rebalance, so this settles first.
                _      <- IO.sleep(8.seconds)
                latest <- reading.assignment
              yield latest
          .timeout(90.seconds)
      yield assertEquals(settled.size, 2, s"$backend gave the consumer that is read ${settled.size} of 2 partitions")

  test(conformance("naming partitions directly keeps the assignment whole and fixed")):
    withBroker: server =>
      val topic     = validTopic(partitionedTopic)
      val partition = validPartition(0)
      val named     = TopicPartition(topic, partition)
      val value     = s"assigned-${System.nanoTime()}"

      for
        _        <- produce(server, NonEmptyList.one(record(topic, Some("k"), Some(value), partition)))
        settings <- consumerSettings(server, uniqueGroup("assigned"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(named))).use: first =>
            // A second consumer naming the same partition takes no share of it, because neither joined a group.
            PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(named))).use: second =>
              for
                held       <- first.assignment
                also       <- second.assignment
                fromFirst  <- first.records.head.compile.lastOrError
                fromSecond <- second.records.head.compile.lastOrError
              yield (held, also, fromFirst.record, fromSecond.record)
          .timeout(90.seconds)
      yield
        val (held, also, fromFirst, fromSecond) = outcome
        assertEquals(held, Set(named), "a named partition should be assigned outright")
        assertEquals(also, Set(named), "a second consumer naming it should hold it too, since no group divides it")
        assertEquals(fromFirst.topicPartition, named)
        // Both share a consumer group, and both still read the same record, because naming partitions joins no group.
        assertEquals(fromSecond.topicPartition, named)
        assertEquals(fromSecond.offset, fromFirst.offset, "both consumers should read the same record, not a share of the partition")

  /** librdkafka refuses a seek until the partition it names is being fetched, which holding the assignment does not yet mean. */
  private def whenSeekable(seek: IO[Unit]): IO[Unit] =
    def attempt: IO[Unit] = seek.handleErrorWith(_ => IO.sleep(250.millis) *> attempt)
    attempt.timeout(30.seconds)

  test(conformance("a later commit replaces an earlier one, even where its offset is lower")):
    withBroker: server =>
      val topic     = validTopic(partitionedTopic)
      val partition = validPartition(0)
      val named     = TopicPartition(topic, partition)
      val values    = List("a", "b", "c")
      val records   = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), partition)))

      for
        _        <- produce(server, records)
        settings <- consumerSettings(server, uniqueGroup("race"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(named))).use: ahead =>
            PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(named))).use: behind =>
              for
                // Both read before either commits, so neither starts from the other's committed offset.
                third       <- ahead.records.take(3).compile.toList
                firstOnly   <- behind.records.take(1).compile.toList
                _           <- third.last.offset.commit
                afterAhead  <- ahead.committed(Set(named))
                _           <- firstOnly.head.offset.commit
                afterBehind <- behind.committed(Set(named))
              yield (third.last.offset.nextOffset, firstOnly.head.offset.nextOffset, afterAhead.get(named).flatten, afterBehind.get(named).flatten)
          .timeout(90.seconds)
      yield
        val (higher, lower, afterAhead, afterBehind) = outcome
        assertEquals(afterAhead, Some(higher), "committing the third record should store the offset after it")
        // Kafka stores whatever was committed last, so a commit can move a group's position backwards and replay records.
        assertEquals(afterBehind, Some(lower), "a later commit should replace the stored offset even where it is lower")

  private def assignmentOf(consumer: KafkaConsumer[IO, Option[String], Option[String]], size: Int): IO[Set[TopicPartition]] =
    consumer.assignmentChanges.filter(_.size == size).head.compile.lastOrError

  private def produce(
      server: String,
      records: NonEmptyList[ProducerRecord[Option[String], Option[String]]]
  ): IO[ProducerResult[Option[String], Option[String]]] =
    producerSettings(server).flatMap: settings =>
      PlatformKafkaClient().producer(settings).use(_.produceAndAwait(records)).timeout(60.seconds)

  private def consume(server: String, topic: Topic, count: Int): IO[List[CommittableConsumerRecord[IO, Option[String], Option[String]]]] =
    consumerSettings(server, uniqueGroup("conformance")).flatMap: settings =>
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(count.toLong).compile.toList)
        .timeout(60.seconds)

  private def producerSettings(server: String): IO[ProducerSettings[IO, Option[String], Option[String]]] =
    ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
      ProducerSettings.from(client, optionalSerializer, optionalSerializer).liftTo[IO]

  private def consumerSettings(server: String, group: ConsumerGroup): IO[ConsumerSettings[IO, Option[String], Option[String]]] =
    ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
      ConsumerSettings.from(client, group, optionalDeserializer, optionalDeserializer, AutoOffsetReset.Earliest).liftTo[IO]

  private val optionalSerializer   = Serializer.utf8[IO].option
  private val optionalDeserializer = Deserializer.utf8[IO].option

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
