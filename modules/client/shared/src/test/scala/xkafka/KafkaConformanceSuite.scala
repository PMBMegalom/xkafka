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

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{Deferred, FiberIO, IO, Ref}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
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

  test(conformance("a record carries the timestamp its producer set, as a creation time")):
    withBroker: server =>
      val topic = uniqueTopic("create-time")
      val stamp = Timestamp.fromEpochMillis(1700000000000L)

      for
        _        <- produce(server, NonEmptyList.one(record(topic, Some("k"), Some("v"), validPartition(0)).copy(timestamp = Some(stamp))))
        consumed <- consume(server, topic, 1)
      yield assertEquals(consumed.map(_.record.timestamp), List(Some(RecordTimestamp.CreateTime(stamp))))

  test(conformance("a topic that stamps records as it appends them reports the broker's time")):
    withBroker: server =>
      val topic = uniqueTopic("append-time")
      val stamp = Timestamp.fromEpochMillis(1700000000000L)

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, 1, 1, Map("message.timestamp.type" -> "LogAppendTime")).liftTo[IO]
        _        <- PlatformKafkaClient().admin(client).use(admin => admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 1))
        _        <- produce(server, NonEmptyList.one(record(topic, Some("k"), Some("v"), validPartition(0)).copy(timestamp = Some(stamp))))
        now      <- IO.realTime
        consumed <- consume(server, topic, 1)
      yield consumed.map(_.record.timestamp) match
        case List(Some(RecordTimestamp.LogAppendTime(appended))) =>
          // The broker's clock is not this one, so this only asks that the time is recent and is not the one produced.
          assert((appended.epochMillis - now.toMillis).abs < 60000L, s"the append time $appended should be close to $now")
        case other => fail(s"expected a log append time, got $other")

  test(conformance("a record reports its partition's leader epoch and the sizes of its key and value as read")):
    withBroker: server =>
      val topic     = uniqueTopic("record-fields")
      val partition = validPartition(0)

      for
        _        <- produce(server, NonEmptyList.of(record(topic, Some("key"), Some("välue"), partition), record(topic, None, None, partition)))
        consumed <- consume(server, topic, 2)
      yield assertEquals(
        consumed.map(read => (read.record.leaderEpoch.map(_.value), read.record.serializedKeySize, read.record.serializedValueSize)),
        // A topic's first leader starts the epoch at zero, and the value is six bytes in UTF-8.
        List((Some(0), Some(3), Some(6)), (Some(0), None, None))
      )

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

  test(conformance("a header without a value stays distinct from one with an empty value")):
    withBroker: server =>
      val topic   = uniqueTopic("null-header")
      val headers = Headers(Header("missing", None), Header("empty", Some(Chunk.empty)))
      val headed  = record(topic, Some("key"), Some("value"), validPartition(0), headers)

      for
        _        <- produce(server, NonEmptyList.one(headed))
        consumed <- consume(server, topic, 1)
      yield assertEquals(consumed.head.record.headers.values, headers.values)

  Compression.values.foreach: compression =>
    test(conformance(s"records compressed with $compression come back as they were produced")):
      withBroker: server =>
        val topic       = uniqueTopic(compression.toString.toLowerCase)
        val values      = List.tabulate(200)(index => s"record $index " + "compressible " * 50)
        val records     = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), validPartition(0))))
        val compressing =
          ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
            ProducerSettings.from(client, optionalSerializer, optionalSerializer, compression = compression).liftTo[IO]

        for
          settings <- compressing
          _        <- PlatformKafkaClient().producer(settings).use(_.produceAndAwait(records)).timeout(60.seconds)
          consumed <- consume(server, topic, values.size)
        yield assertEquals(consumed.map(_.record.value), values.map(Some(_)))

  test(conformance("a record waits out the linger before it is sent")):
    withBroker: server =>
      val topic                                                    = uniqueTopic("linger")
      val one                                                      = NonEmptyList.one(record(topic, Some("k"), Some("v"), validPartition(0)))
      def acknowledged(linger: FiniteDuration): IO[FiniteDuration] =
        ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
          ProducerSettings.from(client, optionalSerializer, optionalSerializer, linger = linger).liftTo[IO]
        .flatMap: settings =>
          PlatformKafkaClient().producer(settings).use: producer =>
            // The first record also fetches the topic's metadata, so only the second one is timed.
            producer.produceAndAwait(one) *> producer.produceAndAwait(one).timed.map(_._1)

      for
        immediate <- acknowledged(Duration.Zero)
        lingering <- acknowledged(2.seconds)
      yield
        assert(immediate < 1.second, s"acknowledged after $immediate without lingering")
        assert(lingering >= 1900.millis, s"acknowledged after $lingering while lingering for 2 seconds")

  test(conformance("a full batch is sent without waiting out the linger")):
    withBroker: server =>
      val topic   = uniqueTopic("batch")
      val records = List.tabulate(20)(index => record(topic, Some("k"), Some(s"record $index " + "x" * 100), validPartition(0)))
      def firstAcknowledged(batchSize: Option[Int]): IO[FiniteDuration] =
        ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
          ProducerSettings.from(client, optionalSerializer, optionalSerializer, linger = 10.seconds, batchSize = batchSize).liftTo[IO]
        .flatMap: settings =>
          PlatformKafkaClient().producer(settings).use: producer =>
            // The first record also fetches the topic's metadata, so it is sent and acknowledged before anything is timed.
            producer.produceAndAwait(NonEmptyList.one(records.head)).timeout(30.seconds) *>
              // Each record is enqueued on its own, and only the first one's acknowledgement is timed: its batch fills up as the
              // later records arrive, while the last batch, which nothing fills, waits out the linger on every backend.
              records.traverse(value => producer.produce(NonEmptyList.one(value))).flatMap(_.head.timeout(30.seconds)).timed.map(_._1)

      for
        full    <- firstAcknowledged(Some(1024))
        waiting <- firstAcknowledged(None)
      yield
        assert(full < 5.seconds, s"acknowledged after $full although its batch was full")
        assert(waiting >= 9.seconds, s"acknowledged after $waiting although its batch was not full")

  test(conformance("producing reports metadata for every record")):
    withBroker: server =>
      val topic     = uniqueTopic("metadata")
      val partition = validPartition(0)
      val records   = NonEmptyList.of("a", "b", "c").map(value => record(topic, Some("k"), Some(value), partition))

      produce(server, records).map: result =>
        assertEquals(result.map(_.record), records)
        assertEquals(result.toList.flatMap(_.metadata.offset.map(_.value)), List(0L, 1L, 2L))

  test(conformance("attempting each record answers every record on its own, so one the broker rejects leaves the others delivered")):
    withBroker: server =>
      val topic = uniqueTopic("attempt-each")
      // The topic takes no record larger than a kilobyte. The large record goes to a partition of its own, so the broker rejects it alone, and its
      // value does not compress below the limit.
      val small   = record(topic, Some("k"), Some("small"), validPartition(0))
      val large   = record(topic, Some("k"), Some(scala.util.Random(7).alphanumeric.take(8192).mkString), validPartition(1))
      val records = NonEmptyList.of(small, large, small)

      for
        _        <- createPartitioned(server, topic, 2, Map("max.message.bytes" -> "1024"))
        settings <- producerSettings(server)
        outcomes <-
          PlatformKafkaClient().producer(settings)
            .use(producer => (producer.produceAttemptEachAndAwait(records), producer.produceAndAwait(records).attempt).tupled).timeout(60.seconds)
      yield
        val (attempted, produced)                = outcomes
        val code: Throwable => Option[ErrorCode] =
          case failure: KafkaException.BackendFailure => failure.code
          case _                                      => None
        assertEquals(attempted.map(_.record), records)
        assertEquals(
          attempted.toList.map(_.outcome.leftMap(code).map(_.offset.map(_.value))),
          List(Right(Some(0L)), Left(Some(ErrorCode.MessageTooLarge)), Right(Some(1L)))
        )
        assertEquals(produced.leftMap(code), Left(Some(ErrorCode.MessageTooLarge)))

  test(conformance("flushing sends records without waiting out the linger and returns once they are delivered")):
    withBroker: server =>
      val topic   = uniqueTopic("flush")
      val records = NonEmptyList.of("a", "b").map(value => record(topic, Some("k"), Some(value), validPartition(0)))

      for
        _        <- createPartitioned(server, topic, 1)
        base     <- producerSettings(server)
        settings <- base.withLinger(60.seconds).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().producer(settings).use: producer =>
            for
              produced <- producer.produce(records)
              lingered <- produced.timeout(1.second).attempt
              _        <- producer.flush.timeout(30.seconds)
              // Already delivered, so this answers without waiting on the broker.
              sent <- produced.timeout(1.second)
            yield (lingered, sent)
      yield
        val (lingered, sent) = outcome
        assert(lingered.isLeft, s"the records were delivered before the linger passed: $lingered")
        assertEquals(sent.toList.flatMap(_.metadata.offset.map(_.value)), List(0L, 1L))

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
            assertEquals(firstResult.size, 2)
            assertEquals(secondResult.size, 2)
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
        ProducerSettings.from(client, optionalSerializer, optionalSerializer, linger = 5.seconds).liftTo[IO]
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

  test(conformance("enqueueing does not slow down as acknowledgements pile up")):
    withBroker: server =>
      val topic   = uniqueTopic("piling")
      val records = List.tabulate(20)(index => NonEmptyList.one(record(topic, Some("k"), Some(s"record $index"), validPartition(0))))

      // A long linger keeps every batch waiting for its acknowledgement while the next ones are enqueued.
      ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
        ProducerSettings.from(client, optionalSerializer, optionalSerializer, linger = 10.seconds).liftTo[IO]
      .flatMap: settings =>
        PlatformKafkaClient().producer(settings).use: producer =>
          for
            // The first record also fetches the topic's metadata, so it is acknowledged before anything is timed.
            _        <- producer.produceAndAwait(records.head).timeout(30.seconds)
            enqueued <- records.traverse(producer.produce).timed
            (took, acknowledgements) = enqueued
            _ <- acknowledgements.sequence.timeout(30.seconds)
          yield assert(took < 1.second, s"enqueueing 20 batches took $took while their acknowledgements were outstanding")

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
            // A broker that cannot be reached is worth trying again, and every backend has to say so identically,
            // because the commit recovery decides what to retry from this alone.
            assertEquals(failure.retriable, Some(true), s"a broker that is unreachable should be retriable, code was ${failure.code}")
            assertEquals(failure.code.map(_.retriable), failure.retriable, "retriable should follow the portable code")
          case Left(other) => fail(s"expected a BackendFailure, got $other")
          case Right(_)    => fail("expected producing to an unreachable broker to fail")
    .timeout(90.seconds)

  test(conformance("releasing a producer delivers the records it had not acknowledged yet")):
    withBroker: server =>
      val topic     = uniqueTopic("close-flush")
      val partition = validPartition(0)
      val values    = List("a", "b", "c")
      val records   = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), partition)))

      for
        settings <- producerSettings(server)
        // The acknowledgement is deliberately dropped, so only the release can get these to the broker.
        _        <- PlatformKafkaClient().producer(settings).use(_.produce(records).void).timeout(60.seconds)
        consumed <- consume(server, topic, values.size)
      yield assertEquals(consumed.flatMap(_.record.value), values, "a released producer should deliver what it still held")

  test(conformance("a producer reports the partitions of a topic")):
    withBroker: server =>
      // Made with more than one partition by the fixture, so the answer is not the one a topic gets by default.
      val topic = validTopic(partitionedTopic)

      for
        settings <- producerSettings(server)
        observed <- PlatformKafkaClient().producer(settings).use(_.partitionsFor(topic)).timeout(60.seconds)
      yield assertEquals(observed.toList.map(_.value).sorted, List(0, 1))

  test(conformance("a producer asked for the partitions of a topic the cluster does not have creates it, where the broker creates topics")):
    withBroker: server =>
      val topic = uniqueTopic("partitions-unknown-producer")

      for
        settings   <- producerSettings(server)
        client     <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        partitions <- PlatformKafkaClient().producer(settings).use(_.partitionsFor(topic)).timeout(60.seconds)
        created    <- PlatformKafkaClient().admin(client).use(described(_, topic, 1))
      yield
        assertEquals(partitions, NonEmptySet.one(validPartition(0)))
        assertEquals(created, 1)

  test(conformance("a consumer asked for the partitions of a topic the cluster does not have fails as unknown, and does not create it")):
    withBroker: server =>
      val topic = uniqueTopic("partitions-unknown-consumer")

      for
        settings <- consumerSettings(server, uniqueGroup("partitions-unknown-consumer"))
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(uniqueTopic("partitions-unknown-subscribed")))).use:
            _.partitionsFor(topic).attempt
          .timeout(60.seconds)
        // Long enough for a creation the lookup asked for to show.
        _      <- IO.sleep(3.seconds)
        exists <- PlatformKafkaClient().admin(client).use(_.describeTopics(NonEmptySet.one(topic)).map(_(topic)))
      yield
        refusedWith(Some(outcome), ErrorCode.UnknownTopicOrPartition, "the partitions of a topic the cluster does not have")
        refusedWith(exists, ErrorCode.UnknownTopicOrPartition, "the topic after the lookup")

  test(conformance("the producer pipe delivers every batch, in order")):
    withBroker: server =>
      val topic     = uniqueTopic("pipe")
      val partition = validPartition(0)
      val values    = List("a", "b", "c", "d")
      val batches   = values.map(value => NonEmptyList.one(record(topic, Some("k"), Some(value), partition)))

      for
        settings <- producerSettings(server)
        results  <-
          PlatformKafkaClient().producer(settings).use(producer => Stream.emits(batches).through(producer.pipe()).compile.toList).timeout(60.seconds)
        consumed <- consume(server, topic, values.size)
      yield
        assertEquals(results.flatMap(_.toList.flatMap(_.record.value)), values, "the pipe should report batches in the order it took them")
        assertEquals(consumed.flatMap(_.record.value), values, "and the broker should hold them in that order")

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

  test(conformance("a static member that comes back within its session timeout gets its partitions back, and the others keep theirs")):
    withBroker: server =>
      val topic     = validTopic(partitionedTopic)
      val group     = uniqueGroup("static")
      val selection = Selection.Topics(NonEmptySet.one(topic))

      for
        dynamic <- consumerSettings(server, group)
        static  <- dynamic.withGroupInstanceId(s"static-$backend").andThen(_.withSessionTimeout(30.seconds)).liftTo[IO]
        outcome <-
          PlatformKafkaClient().consumer(dynamic, selection).use: other =>
            for
              before <- PlatformKafkaClient().consumer(static, selection).use(member => (assignmentOf(member, 1), assignmentOf(other, 1)).parTupled)
              // A dynamic member would leave the group here and the other one would take both partitions.
              _     <- IO.sleep(5.seconds)
              kept  <- other.assignment
              back  <- PlatformKafkaClient().consumer(static, selection).use(member => assignmentOf(member, 1))
              still <- other.assignment
            yield (before, kept, back, still)
          .timeout(90.seconds)
      yield
        val ((held, others), kept, back, still) = outcome
        assertEquals(kept, others, "the other member should keep only its own partition while the static member is away")
        assertEquals(back, held, "the static member should get its own partition back")
        assertEquals(still, others)

  test(conformance("a static member that stays away loses its partitions once its session times out")):
    withBroker: server =>
      val topic     = validTopic(partitionedTopic)
      val group     = uniqueGroup("expiring")
      val selection = Selection.Topics(NonEmptySet.one(topic))

      for
        dynamic <- consumerSettings(server, group)
        // The broker accepts no session timeout shorter than six seconds by default.
        static  <- dynamic.withGroupInstanceId(s"expiring-$backend").andThen(_.withSessionTimeout(6.seconds)).liftTo[IO]
        outcome <-
          PlatformKafkaClient().consumer(dynamic, selection).use: other =>
            for
              _       <- PlatformKafkaClient().consumer(static, selection).use(member => (assignmentOf(member, 1), assignmentOf(other, 1)).parTupled)
              started <- IO.monotonic
              _       <- assignmentOf(other, 2).timeout(30.seconds)
              taken   <- IO.monotonic
            yield taken - started
          .timeout(90.seconds)
      yield
        assert(outcome >= 4.seconds, s"the other member took the partitions after $outcome, before the session could time out")
        assert(outcome < 20.seconds, s"the other member took the partitions after $outcome, long after a six second session")

  test(conformance("a consumer whose records go unread leaves its group once its poll interval passes")):
    withBroker: server =>
      val topic     = uniqueTopic("unread")
      val group     = uniqueGroup("unread")
      val selection = Selection.Topics(NonEmptySet.one(topic))
      // More records than a consumer holds unread, in each partition, so fetching stops for the one nobody reads.
      val records = NonEmptyList.fromListUnsafe(List.tabulate(2000)(index => record(topic, None, Some(s"record $index"), validPartition(index % 2))))

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 2, replicationFactor = 1).liftTo[IO]
        _        <- PlatformKafkaClient().admin(client).use(admin => admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 2))
        _        <- produce(server, records)
        reader   <- consumerSettings(server, group)
        idle <- reader.withSessionTimeout(6.seconds).andThen(_.withHeartbeatInterval(2.seconds)).andThen(_.withMaxPollInterval(6.seconds)).liftTo[IO]
        history <- Ref.of[IO, Vector[(FiniteDuration, Int)]](Vector.empty)
        // The reading member's assignments as they change, so a handover that happens quickly is not missed. It holds both
        // partitions before the idle member joins, so the handover runs from its first change to fewer than both.
        holdsBoth  = (IO.sleep(250.millis) *> history.get).iterateWhile(!_.exists(_._2 == 2)).void
        handedOver =
          history.get.map: changes =>
            changes.dropWhile(_._2 < 2).collectFirst { case (shared, held) if held < 2 => shared }
              .flatMap(shared => changes.collectFirst { case (taken, 2) if taken > shared => taken - shared })
        outcome <-
          PlatformKafkaClient().consumer(reader, selection).use: other =>
            val watching = other.assignmentChanges.evalMap(changed => IO.monotonic.flatMap(at => history.update(_ :+ (at -> changed.size))))
            (other.records.compile.drain, watching.compile.drain).parTupled.background.surround:
              holdsBoth.timeout(60.seconds) *> PlatformKafkaClient().consumer(idle, selection).use: _ =>
                (IO.sleep(250.millis) *> handedOver).iterateWhile(_.isEmpty).map(_.get).timeout(90.seconds)
                  .onError(_ => history.get.flatMap(changes => IO.println(s"[$backend] the reading member's assignments: $changes")))
          .timeout(120.seconds)
      yield assert(outcome >= 4.seconds, s"the reading member took both partitions after $outcome, before the poll interval could pass")

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
        client   <- ClientSettings.from(NonEmptyList.one(server)).andThen(_.withMetadataRefreshInterval(refresh)).liftTo[IO]
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
  test(conformance("a consumer that is never read still takes its share of the topic")):
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
                // The member that joined without being read takes its share through a rebalance, so this settles first.
                _      <- IO.sleep(8.seconds)
                latest <- reading.assignment
              yield latest
          .timeout(90.seconds)
      // Every backend polls for a consumer from the moment it is made, so the one never read is still a member and keeps a partition.
      yield assertEquals(settled.size, 1, s"$backend gave the consumer that is read ${settled.size} of 2 partitions")

  test(conformance("a consumer that commits every chunk it reads goes on reading")):
    withBroker: server =>
      val partition = TopicPartition(uniqueTopic("chunk-commits"), validPartition(0))
      val count     = 5000

      for
        _        <- produceValues(server, partition, count)
        settings <- consumerSettings(server, uniqueGroup("chunk-commits"))
        read     <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(partition.topic))).use: consumer =>
            consumer.records.take(count.toLong).chunks.evalMap(chunk => chunk.last.traverse_(_.offset.commit).as(chunk.size)).compile.foldMonoid
          .timeout(60.seconds)
      yield assertEquals(read, count)

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
                held        <- first.assignment
                also        <- second.assignment
                firstChange <- first.assignmentChanges.head.compile.lastOrError
                laterChange <- first.assignmentChanges.drop(1).head.compile.last.timeoutTo(250.millis, IO.pure(None))
                fromFirst   <- first.records.head.compile.lastOrError
                fromSecond  <- second.records.head.compile.lastOrError
              yield (held, also, firstChange, laterChange, fromFirst.record, fromSecond.record)
          .timeout(90.seconds)
      yield
        val (held, also, firstChange, laterChange, fromFirst, fromSecond) = outcome
        assertEquals(held, Set(named), "a named partition should be assigned outright")
        assertEquals(also, Set(named), "a second consumer naming it should hold it too, since no group divides it")
        assertEquals(firstChange, Set(named), "the direct assignment should be the first assignment change")
        assertEquals(laterChange, None, "a direct assignment should not report a later change")
        assertEquals(fromFirst.topicPartition, named)
        // Both share a consumer group, and both still read the same record, because naming partitions joins no group.
        assertEquals(fromSecond.topicPartition, named)
        assertEquals(fromSecond.offset, fromFirst.offset, "both consumers should read the same record, not a share of the partition")

  test(conformance("partition streams reject a non-positive queue threshold")):
    withBroker: server =>
      for
        settings <- consumerSettings(server, uniqueGroup("queue-threshold"))
        outcome  <-
          PlatformKafkaClient()
            .consumer(settings, Selection.Partitions(NonEmptySet.one(TopicPartition(validTopic(partitionedTopic), validPartition(0)))))
            .use(_.partitionedRecords(0).compile.drain).attempt
      yield outcome match
        case Left(error: IllegalArgumentException) => assertEquals(error.getMessage, "maxQueuedRecords must be positive")
        case Left(error)                           => fail(s"unexpected error: $error")
        case Right(())                             => fail("expected a non-positive queue threshold to fail")

  /** librdkafka refuses a seek until the partition it names is being fetched, which holding the assignment does not yet mean. */
  test(conformance("a seek of a partition not assigned is refused as unknown, and one of a partition assigned succeeds once it is fetched")):
    withBroker: server =>
      val topic      = uniqueTopic("early-seek")
      val partition  = TopicPartition(topic, validPartition(0))
      val unassigned = TopicPartition(uniqueTopic("early-seek-unassigned"), validPartition(0))

      for
        _        <- produceValues(server, partition, 3)
        settings <- consumerSettings(server, uniqueGroup("early-seek"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            (assignmentOf(consumer, 1) *> consumer.seek(unassigned, validOffset(1L)).attempt) <*
              whenSeekable(consumer.seek(partition, validOffset(1L)))
          .timeout(90.seconds)
      yield refusedWith(Some(outcome), ErrorCode.UnknownTopicOrPartition, "a seek of a partition not assigned")

  test(conformance("after a seek, the next record handed out of the partition is the one sought, and none fetched before it")):
    withBroker: server =>
      val topic     = uniqueTopic("seek-buffered")
      val partition = TopicPartition(topic, validPartition(0))
      // Each seek is made once the record before it is handed out, when the records fetched after that one are already waiting.
      def seekAfter(consumer: KafkaConsumer[IO, ?, ?], index: Long): IO[Unit] =
        index match
          case 0 => consumer.seek(partition, validOffset(6L))
          case 2 => consumer.seek(partition, validOffset(2L))
          case 4 => consumer.seekToBeginning(Set(partition))
          case 6 => consumer.seekToEnd(Set(partition))
          case _ => IO.unit

      for
        _        <- produceValues(server, partition, 10)
        settings <- consumerSettings(server, uniqueGroup("seek-buffered"))
        offsets  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            // Past the end there is nothing more to read, so the stream is given a while to hand out a record it should not.
            consumer.records.map(_.record.offset.value).zipWithIndex.evalTap((_, index) => seekAfter(consumer, index)).map(_._1).take(8)
              .interruptAfter(20.seconds).compile.toList
          .timeout(90.seconds)
      yield assertEquals(offsets, List(0L, 6L, 7L, 2L, 3L, 0L, 1L))

  test(conformance("the position can be unset before consuming, and after consuming is at or past the offset after the record consumed")):
    withBroker: server =>
      val topic     = uniqueTopic("position-ahead")
      val partition = TopicPartition(topic, validPartition(0))

      for
        _        <- produceValues(server, partition, 5)
        settings <- consumerSettings(server, uniqueGroup("position-ahead"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            (assignmentOf(consumer, 1) *> consumer.position(partition), consumer.records.take(1).compile.lastOrError, consumer.position(partition))
              .tupled
          .timeout(90.seconds)
      yield
        val (before, taken, position) = outcome
        val next                      = taken.offset.nextOffset.value
        assert(position.exists(at => at.value >= next && at.value <= 5L), s"the position should be between $next and 5, got $position")
        // Only the JVM settles on a position before anything is consumed.
        if PlatformKafkaClient.javaRelease.isEmpty then assertEquals(before, None, "librdkafka has no position before consuming")

  private def whenSeekable(seek: IO[Unit]): IO[Unit] =
    def attempt: IO[Unit] = seek.handleErrorWith(_ => IO.sleep(250.millis) *> attempt)
    attempt.timeout(30.seconds)

  test(conformance("a commit that cannot complete in time fails as a timed out request")):
    withBroker: server =>
      val topic     = uniqueTopic("commit-timeout")
      val partition = validPartition(0)

      for
        _        <- produce(server, NonEmptyList.one(record(topic, Some("k"), Some("v"), partition)))
        settings <- consumerSettings(server, uniqueGroup("commit-timeout"))
        // A nanosecond cannot cover a round trip, so the outcome does not depend on how fast the broker is. Recovery is
        // off, because the policy would otherwise retry this and report its own exhaustion instead.
        bounded <- settings.withCommitTimeout(1.nanos).map(_.withCommitRecovery(CommitRecovery.none)).liftTo[IO]
        outcome <-
          PlatformKafkaClient().consumer(bounded, Selection.Topics(NonEmptySet.one(topic)))
            .use(_.records.take(1).evalMap(_.offset.commit).compile.drain).attempt.timeout(60.seconds)
      yield outcome match
        case Left(failure: KafkaException.BackendFailure) =>
          assertEquals(failure.code, Some(ErrorCode.RequestTimedOut), s"got ${failure.getMessage}")
          assert(failure.code.exists(_.retriable), "a commit that ran out of time should be worth retrying")
        case other => fail(s"expected a timed out commit to fail, got $other")

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
                afterAhead  <- ahead.committed(Set(named)).map(_.offsetsOnly)
                _           <- firstOnly.head.offset.commit
                afterBehind <- behind.committed(Set(named)).map(_.offsetsOnly)
              yield (third.last.offset.nextOffset, firstOnly.head.offset.nextOffset, afterAhead.get(named).flatten, afterBehind.get(named).flatten)
          .timeout(90.seconds)
      yield
        val (higher, lower, afterAhead, afterBehind) = outcome
        assertEquals(afterAhead, Some(higher), "committing the third record should store the offset after it")
        // Kafka stores whatever was committed last, so a commit can move a group's position backwards and replay records.
        assertEquals(afterBehind, Some(lower), "a later commit should replace the stored offset even where it is lower")

  test(conformance("a commit stores its offset's leader epoch and metadata, and a bare offset stores neither")):
    withBroker: server =>
      val topic = uniqueTopic("commit-metadata")
      val named = TopicPartition(topic, validPartition(0))

      for
        _        <- produceValues(server, named, 2)
        settings <- consumerSettings(server, uniqueGroup("commit-metadata"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            for
              taken     <- consumer.records.take(2).compile.toList
              _         <- taken.head.offset.withMetadata("première note").commit
              annotated <- consumer.committed(Set(named))
              _         <- taken.last.offset.committer.commit(Map(named -> taken.last.offset.nextOffset))
              bare      <- consumer.committed(Set(named))
            yield (annotated.get(named).flatten, bare.get(named).flatten)
          .timeout(60.seconds)
      yield
        val (annotated, bare) = outcome
        assertEquals(annotated, Some(OffsetAndMetadata(validOffset(1), Some(validEpoch(0)), "première note")))
        assertEquals(bare, Some(OffsetAndMetadata(validOffset(2))))

  test(conformance("a commit whose metadata is longer than the broker keeps fails as too large")):
    withBroker: server =>
      val topic = uniqueTopic("commit-metadata-size")

      for
        _        <- produceValues(server, TopicPartition(topic, validPartition(0)), 1)
        settings <- consumerSettings(server, uniqueGroup("commit-metadata-size"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
            // The broker keeps 4096 bytes by default.
            .use(_.records.take(1).evalMap(_.offset.withMetadata("x" * 4097).commit).compile.drain).attempt.timeout(60.seconds)
      yield outcome match
        case Left(failure: KafkaException.BackendFailure) =>
          assertEquals(failure.code, Some(ErrorCode.OffsetMetadataTooLarge), s"got ${failure.getMessage}")
        case other => fail(s"expected the commit to fail, got $other")

  test(conformance("stopping a consumer ends its streams and leaves the rest of the topic behind")):
    withBroker: server =>
      val topic     = uniqueTopic("stop")
      val partition = validPartition(0)
      val group     = uniqueGroup("stop")
      val values    = List.range(0, 10).map(_.toString)
      val produced  = NonEmptyList.fromListUnsafe(values.map(value => record(topic, Some("k"), Some(value), partition)))

      for
        _        <- produce(server, produced)
        settings <- consumerSettings(server, group)
        taken    <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            // Stopping from inside the stream is the graceful shutdown this is for. Committing after it proves offsets
            // still reach the broker, and the stream ending on its own proves nothing is left waiting.
            consumer.records.evalTap(_ => consumer.stopConsuming).evalTap(_.offset.commit).compile.toList
          .timeout(60.seconds)
        stored <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
            .use(_.committed(Set(TopicPartition(topic, partition))).map(_.offsetsOnly)).timeout(60.seconds)
        rest <-
          if taken.sizeIs >= values.size then IO.pure(List.empty[String])
          else
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
              .use(_.records.take((values.size - taken.size).toLong).compile.toList).timeout(60.seconds).map(_.flatMap(_.record.value))
      yield
        val read = taken.flatMap(_.record.value)
        assert(read.nonEmpty, "a consumer stopped after its first record should still deliver that record")
        assertEquals(read, values.take(read.size), "the records delivered should be the ones from the start of the partition")
        assertEquals(
          stored.get(TopicPartition(topic, partition)).flatten.map(_.value),
          Some(read.size.toLong),
          "committing after the stop should still reach the broker"
        )
        assertEquals(read ++ rest, values, "a consumer resuming the group should read exactly what the stopped one left")

  test(conformance("topics can be created, grown, described, and deleted")):
    withBroker: server =>
      val topic = uniqueTopic("admin")

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              _ <- admin.createTopics(NonEmptySet.one(newTopic))
              // Creating and growing a topic propagates through the cluster, so each is polled for.
              created <- described(admin, topic, 1)
              _       <- admin.createPartitions(topic, 2)
              grown   <- described(admin, topic, 2)
              _       <- admin.deleteTopics(NonEmptySet.one(topic))
              // Deletion is asynchronous on the broker, so the topic is polled until it has gone.
              gone <- absent(admin, topic)

            yield (created, grown, gone)
          .timeout(90.seconds)
      yield
        val (created, grown, gone) = outcome
        assertEquals(created, 1, "a created topic should report the partitions it was made with")
        assertEquals(grown, 2, "growing a topic should report the added partition")
        assert(gone, "a deleted topic should stop being described")

  test(conformance("an administration effect changes nothing until it runs")):
    withBroker: server =>
      val topic     = uniqueTopic("admin-unrun")
      val other     = uniqueTopic("admin-unrun-other")
      val partition = TopicPartition(topic, validPartition(0))

      for
        client     <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic   <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        otherTopic <- NewTopic.from(other, partitions = 1, replicationFactor = 1).liftTo[IO]
        settings   <- consumerSettings(server, uniqueGroup("admin-unrun"))
        outcome    <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              _ <- admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 1)
              _ <-
                produce(
                  server,
                  NonEmptyList.of(record(topic, None, Some("one"), partition.partition), record(topic, None, Some("two"), partition.partition))
                )
              // Each effect is built and thrown away, so none of them may reach the broker.
              _ <-
                IO.delay(List(
                  admin.deleteTopics(NonEmptySet.one(topic)),
                  admin.createPartitions(topic, 2),
                  admin.createTopics(NonEmptySet.one(otherTopic)),
                  admin.deleteRecords(NonEmptyMap.one(partition, validOffset(2)))
                ))
              _          <- IO.sleep(3.seconds)
              partitions <- admin.describeTopics(NonEmptySet.one(topic)).map(_(topic).flatMap(_.toOption).map(_.partitions.length))
              unmade     <- admin.describeTopics(NonEmptySet.one(other)).map(_(other))
              beginning  <-
                PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(partition))).use(_.beginningOffsets(Set(partition)))
              _ <- admin.deleteTopics(NonEmptySet.one(topic)).attempt
            yield (partitions, unmade, beginning)
          .timeout(90.seconds)
      yield
        val (partitions, unmade, beginning) = outcome
        assertEquals(partitions, Some(1), "the topic should be neither deleted nor grown")
        refusedWith(unmade, ErrorCode.UnknownTopicOrPartition, "the other topic, which should not have been created")
        assertEquals(beginning.get(partition).map(_.value), Some(0L), "no records should have been deleted")

  test(conformance("an administration effect does its work again each time it runs")):
    withBroker: server =>
      val topic = uniqueTopic("admin-rerun")

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().admin(client).use: admin =>
            val create = admin.createTopics(NonEmptySet.one(newTopic))
            val delete = admin.deleteTopics(NonEmptySet.one(topic))
            for
              _         <- create *> described(admin, topic, 1)
              recreated <- create.attempt
              _         <- delete *> absent(admin, topic)
              redeleted <- delete.attempt
            yield (recreated, redeleted)
          .timeout(90.seconds)
      yield
        val (recreated, redeleted) = outcome
        assert(recreated.isLeft, s"creating the topic again should fail as it already exists, got $recreated")
        assert(redeleted.isLeft, s"deleting the topic again should fail as it is gone, got $redeleted")

  test(conformance("a producer effect sends nothing until it runs")):
    withBroker: server =>
      val topic     = uniqueTopic("produce-unrun")
      val partition = TopicPartition(topic, validPartition(0))
      val records   = NonEmptyList.one(record(topic, None, Some("unsent"), partition.partition))

      for
        // The topic exists before anything is built, so only the records themselves are at stake.
        _        <- produce(server, NonEmptyList.one(record(topic, None, Some("first"), partition.partition)))
        settings <- producerSettings(server)
        consumer <- consumerSettings(server, uniqueGroup("produce-unrun"))
        ends     <-
        PlatformKafkaClient().producer(settings).use: producer =>
          IO.delay(List(producer.produce(records), producer.produceAndAwait(records))) *> IO.sleep(3.seconds)
        *> PlatformKafkaClient().consumer(consumer, Selection.Partitions(NonEmptySet.one(partition))).use(_.endOffsets(Set(partition)))
      yield assertEquals(ends.get(partition).map(_.value), Some(1L), "only the first record should have been sent")

  test(conformance("creating a topic that already exists fails")):
    withBroker: server =>
      val topic = uniqueTopic("admin-twice")

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().admin(client).use: admin =>
            admin.createTopics(NonEmptySet.one(newTopic)) *> admin.createTopics(NonEmptySet.one(newTopic)).attempt <*
              admin.deleteTopics(NonEmptySet.one(topic)).attempt
          .timeout(90.seconds)
      yield assert(outcome.isLeft, s"creating an existing topic should fail, got $outcome")

  test(conformance("creating a topic with an invalid configuration fails")):
    withBroker: server =>
      val topic = uniqueTopic("admin-config")

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1, configuration = Map("xkafka.invalid.config" -> "value")).liftTo[IO]
        outcome  <-
          PlatformKafkaClient().admin(client).use: admin =>
            admin.createTopics(NonEmptySet.one(newTopic)).attempt.flatTap(_ => admin.deleteTopics(NonEmptySet.one(topic)).attempt).timeout(90.seconds)
      yield assert(outcome.isLeft, s"creating a topic with an invalid configuration should fail, got $outcome")

  test(conformance("partition growth fails unless its count is positive and larger")):
    withBroker: server =>
      val topic = uniqueTopic("admin-grow")

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        outcomes <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              invalid <- admin.createPartitions(topic, 0).attempt
              _       <- admin.createTopics(NonEmptySet.one(newTopic))
              same    <- admin.createPartitions(topic, 1).attempt
              _       <- admin.deleteTopics(NonEmptySet.one(topic)).attempt
            yield (invalid, same)
          .timeout(90.seconds)
      yield
        outcomes._1 match
          case Left(failure: KafkaException.InvalidValue) => assertEquals(failure.error, ValidationError.NonPositivePartitionCount(0))
          case other                                      => fail(s"a non-positive partition count should fail validation, got $other")
        assert(outcomes._2.isLeft, s"growing a topic to its existing size should fail, got ${outcomes._2}")

  test(conformance("stopping a consumer returns before its streams drain, and stays stopped")):
    withBroker: server =>
      val topic     = uniqueTopic("stop-return")
      val partition = validPartition(0)
      val produced  = NonEmptyList.of("a", "b", "c").map(value => record(topic, Some("k"), Some(value), partition))

      for
        _        <- produce(server, produced)
        settings <- consumerSettings(server, uniqueGroup("stop-return"))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            for
              reached <- Deferred[IO, Unit]
              release <- Deferred[IO, Unit]
              // Every record waits, so the streams cannot drain until this test lets them.
              reading <- consumer.records.evalTap(_ => reached.complete(()).attempt.void).evalTap(_ => release.get).compile.toList.start
              _       <- reached.get
              // If this waited for the draining it could not return, because the draining waits on `release`.
              _ <- consumer.stopConsuming.timeout(15.seconds)
              // A second call has nothing left to do.
              _     <- consumer.stopConsuming.timeout(15.seconds)
              _     <- release.complete(())
              taken <- reading.joinWithNever.timeout(30.seconds)
              // A stream read after the stop has nothing to give.
              afterwards <- consumer.records.compile.toList.timeout(30.seconds)
            yield (taken, afterwards)
          .timeout(90.seconds)
      yield
        val (taken, afterwards) = outcome
        assert(taken.nonEmpty, "the records already fetched should still be delivered")
        assertEquals(afterwards, List.empty, "a stream read after the stop should be empty")

  test(conformance("records of a committed transaction are delivered")):
    withBroker: server =>
      val topic     = uniqueTopic("transaction-commit")
      val partition = validPartition(0)
      val records   = NonEmptyList.of("a", "b").map(value => record(topic, Some("k"), Some(value), partition))

      for
        settings <- transactionalSettings(server, uniqueTransactionalId("commit"))
        _        <- PlatformKafkaClient().transactionalProducer(settings).use(_.transactionally(_.produce(records))).timeout(90.seconds)
        consumed <- consumeCommitted(server, topic, records.size)
      yield assertEquals(consumed.map(_.record.value), List(Some("a"), Some("b")))

  test(conformance("a transaction whose body fails delivers none of its records")):
    withBroker: server =>
      val topic     = uniqueTopic("transaction-abort")
      val partition = validPartition(0)

      for
        settings <- transactionalSettings(server, uniqueTransactionalId("abort"))
        _        <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            producer.transactionally(transaction =>
              transaction.produce(NonEmptyList.one(record(topic, Some("k"), Some("aborted"), partition))) *>
                IO.raiseError[Unit](new RuntimeException("rolled back"))
            ).attempt *> producer.transactionally(_.produce(NonEmptyList.one(record(topic, Some("k"), Some("committed"), partition))))
          .timeout(90.seconds)
        // The aborted record sits before the committed one in the log, so reading one record proves it was skipped.
        consumed <- consumeCommitted(server, topic, 1)
      yield assertEquals(consumed.map(_.record.value), List(Some("committed")))

  test(conformance("a consumer reading uncommitted records sees the records of a transaction that aborted")):
    withBroker: server =>
      val topic     = uniqueTopic("transaction-uncommitted")
      val partition = validPartition(0)

      for
        settings <- transactionalSettings(server, uniqueTransactionalId("uncommitted"))
        _        <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            producer.transactionally(transaction =>
              transaction.produce(NonEmptyList.one(record(topic, Some("k"), Some("aborted"), partition))) *>
                IO.raiseError[Unit](new RuntimeException("rolled back"))
            ).attempt.void
          .timeout(90.seconds)
        // The default isolation level, which is Kafka's own, holds nothing back.
        consumed <- consume(server, topic, 1)
      yield assertEquals(consumed.map(_.record.value), List(Some("aborted")))

  test(conformance("transactions on one producer run one at a time")):
    withBroker: server =>
      val topic     = uniqueTopic("transaction-serial")
      val partition = validPartition(0)
      val values    = List("first", "second")

      for
        settings <- transactionalSettings(server, uniqueTransactionalId("serial"))
        _        <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            // Kafka refuses a second transaction while one is open, so both succeeding is what shows they were serialised.
            values.parTraverse_(value => producer.transactionally(_.produce(NonEmptyList.one(record(topic, Some("k"), Some(value), partition)))))
          .timeout(90.seconds)
        consumed <- consumeCommitted(server, topic, values.size)
      yield assertEquals(consumed.flatMap(_.record.value).sorted, values.sorted)

  test(conformance("a second producer under one transactional id fences the first")):
    withBroker: server =>
      val topic           = uniqueTopic("transaction-fenced")
      val partition       = validPartition(0)
      val transactionalId = uniqueTransactionalId("fenced")
      val produced        = NonEmptyList.one(record(topic, Some("k"), Some("fenced"), partition))

      for
        settings <- transactionalSettings(server, transactionalId)
        outcome  <-
          PlatformKafkaClient().transactionalProducer(settings).use: first =>
            for
              opened  <- Deferred[IO, Unit]
              release <- Deferred[IO, Unit]
              // Held open so the second producer arrives while this transaction still has work to commit.
              running <-
                first.transactionally(transaction => transaction.produce(produced) *> opened.complete(()).attempt *> release.get).attempt.start
              _ <- opened.get
              // Allocating under the same id is what takes the id over.
              _      <- PlatformKafkaClient().transactionalProducer(settings).use_.timeout(60.seconds)
              _      <- release.complete(())
              result <- running.joinWithNever.timeout(60.seconds)
              // A fenced producer stays fenced, so its next transaction fails the same way.
              next <- first.transactionally(transaction => transaction.produce(produced)).attempt.timeout(60.seconds)
            yield (result, next)
          .timeout(120.seconds)
      yield
        val (fenced, next) = outcome
        fenced match
          case Left(failure: KafkaException.BackendFailure) =>
            assertEquals(failure.code, Some(ErrorCode.Fenced), failure.getMessage)
            assertEquals(failure.retriable, Some(false), failure.getMessage)
            assertEquals(failure.fatal, Some(true), failure.getMessage)
            if backend == "jvm" then assertEquals(failure.transactionAbortRequired, None, failure.getMessage)
            else assertEquals(failure.transactionAbortRequired, Some(false), failure.getMessage)
          case other => fail(s"a fenced producer should report a backend failure, got $other")
        next match
          case Left(failure: KafkaException.BackendFailure) =>
            assertEquals(failure.code, Some(ErrorCode.Fenced), failure.getMessage)
            assertEquals(failure.fatal, Some(true), failure.getMessage)
          case other => fail(s"a fenced producer's next transaction should report a backend failure, got $other")

  test(conformance("a transaction opened inside another one cannot proceed")):
    withBroker: server =>
      val topic     = uniqueTopic("transaction-nested")
      val partition = validPartition(0)
      val produced  = NonEmptyList.one(record(topic, Some("k"), Some("nested"), partition))

      for
        settings <- transactionalSettings(server, uniqueTransactionalId("nested"))
        outcome  <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            producer.transactionally(_ => producer.transactionally(_.produce(produced)).as("inner")).timeoutTo(10.seconds, IO.pure("blocked"))
          .timeout(90.seconds)
      yield assertEquals(outcome, "blocked", "the inner transaction should wait for the outer one, which cannot finish")

  test(conformance("offsets recorded in a transaction move the group only where it commits")):
    withBroker: server =>
      val input          = uniqueTopic("transaction-input")
      val output         = uniqueTopic("transaction-output")
      val partition      = validPartition(0)
      val topicPartition = TopicPartition(input, partition)
      val produced       = record(output, Some("k"), Some("out"), partition)

      for
        _        <- produce(server, NonEmptyList.one(record(input, Some("k"), Some("value"), partition)))
        settings <- transactionalSettings(server, uniqueTransactionalId("offsets"))
        reading  <- committedConsumerSettings(server, uniqueGroup("transaction"))
        outcome  <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: consumer =>
              for
                taken <- consumer.records.take(1).compile.toList
                batch = CommittableOffsetBatch.fromFoldable(taken.map(_.offset))
                _ <-
                  producer.transactionally(transaction =>
                    transaction.produce(NonEmptyList.one(produced)) *> transaction.commitOffsets(batch) *>
                      IO.raiseError[Unit](new RuntimeException("rolled back"))
                  ).attempt
                afterAbort <- consumer.committed(Set(topicPartition)).map(_.offsetsOnly)
                _ <- producer.transactionally(transaction => transaction.produce(NonEmptyList.one(produced)) *> transaction.commitOffsets(batch))
                afterCommit <- consumer.committed(Set(topicPartition)).map(_.offsetsOnly)
              yield (afterAbort.get(topicPartition).flatten, afterCommit.get(topicPartition).flatten)
          .timeout(90.seconds)
      yield
        val (afterAbort, afterCommit) = outcome
        assertEquals(afterAbort, None, "an aborted transaction should leave the group where it was")
        assertEquals(afterCommit.map(_.value), Some(1L), "a committed transaction should store the offset it recorded")

  test(conformance("a transaction stores the leader epoch and metadata of the offsets it records")):
    withBroker: server =>
      val input = uniqueTopic("transaction-metadata")
      val named = TopicPartition(input, validPartition(0))

      for
        _        <- produceValues(server, named, 1)
        settings <- transactionalSettings(server, uniqueTransactionalId("metadata"))
        reading  <- committedConsumerSettings(server, uniqueGroup("transaction-metadata"))
        stored   <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: consumer =>
              for
                taken <- consumer.records.take(1).compile.toList
                _     <-
                  producer.transactionally(_.commitOffsets(CommittableOffsetBatch.fromFoldable(taken.map(_.offset.withMetadata("in a transaction")))))
                after <- consumer.committed(Set(named))
              yield after.get(named).flatten
          .timeout(90.seconds)
      yield assertEquals(stored, Some(OffsetAndMetadata(validOffset(1), Some(validEpoch(0)), "in a transaction")))

  test(conformance("a transaction cannot record an offset from a partition its consumer has since lost")):
    withBroker: server =>
      val input  = validTopic(partitionedTopic)
      val output = uniqueTopic("revoked-output")
      val group  = uniqueGroup("revoked")
      val seeded = NonEmptyList.of(0, 1).map(value => record(input, Some("k"), Some("seed"), validPartition(value)))
      val marker = s"after-rebalance-${System.nanoTime()}"

      def writeWith(producer: KafkaTransactionalProducer[IO, Option[String], Option[String]], offset: CommittableOffset[IO]): IO[Unit] =
        producer.transactionally(transaction =>
          transaction.produce(NonEmptyList.one(record(output, Some("k"), Some("out"), validPartition(0)))) *>
            transaction.commitOffsets(CommittableOffsetBatch.empty[IO].updated(offset))
        )

      for
        _        <- produce(server, seeded)
        settings <- transactionalSettings(server, uniqueTransactionalId("revoked"))
        // Reading committed data also makes a committed offset wait until the transaction that recorded it has finished.
        reading <- committedConsumerSettings(server, group)
        read    <- Ref[IO].of(Vector.empty[CommittableConsumerRecord[IO, Option[String], Option[String]]])
        outcome <-
          PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: first =>
            // The first consumer keeps reading, so it follows the rebalance while the test holds what it read before.
            first.records.evalMap(next => read.update(_ :+ next)).compile.drain.background.surround:
              for
                before  <- read.get.map(_.groupMapReduce(_.record.topicPartition)(_.offset)((earlier, _) => earlier)).iterateUntil(_.size == 2)
                outcome <-
                  PlatformKafkaClient().transactionalProducer(settings).use: producer =>
                    PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: second =>
                      second.records.compile.drain.background.surround:
                        for
                          kept    <- (assignmentOf(first, 1), assignmentOf(second, 1)).parTupled.map(_._1)
                          revoked <- IO.fromOption(before.keySet.find(!kept.contains(_)))(new AssertionError(s"nothing was revoked from $kept"))
                          stale   <- writeWith(producer, before(revoked)).attempt
                          lost    <- first.committed(Set(revoked)).map(_.offsetsOnly)
                          // The default protocol revokes every partition on a rebalance, so one the first consumer kept was read under an assignment it no longer holds either.
                          earlier <- writeWith(producer, before(kept.head)).attempt
                          // A record read after the rebalance, from the partition the first consumer kept, is still its to commit.
                          _       <- produce(server, NonEmptyList.one(record(input, Some("k"), Some(marker), kept.head.partition)))
                          current <- read.get.map(_.find(_.record.value.contains(marker)).map(_.offset)).iterateUntil(_.isDefined).map(_.get)
                          _       <- writeWith(producer, current)
                          moved   <- first.committed(Set(current.topicPartition)).map(_.offsetsOnly)
                        yield (revoked, stale, lost.get(revoked).flatten, earlier, current, moved.get(current.topicPartition).flatten)
              yield outcome
          .timeout(90.seconds)
      yield
        val (revoked, stale, lost, earlier, current, moved) = outcome
        // Every backend classifies both the way the broker answers an offset from a generation the group has left.
        def rejected(outcome: Either[Throwable, Unit], clue: String): Unit =
          outcome match
            case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.IllegalGeneration), failure.getMessage)
            case other                                        => fail(s"$clue, got $other")
        rejected(stale, s"the offset of $revoked was recorded after the partition moved to another consumer")
        rejected(earlier, "an offset read before the rebalance was recorded for a partition the consumer kept")
        assertEquals(lost, None, "a rejected transaction should leave the revoked partition where it was")
        assertEquals(moved, Some(current.nextOffset), "an offset read after the rebalance should still commit")

  test(conformance("with assignment fencing off, a transaction records an offset from a partition its consumer has lost")):
    withBroker: server =>
      val input  = validTopic(partitionedTopic)
      val output = uniqueTopic("unfenced-output")
      val group  = uniqueGroup("unfenced")
      val seeded = NonEmptyList.of(0, 1).map(value => record(input, Some("k"), Some("seed"), validPartition(value)))

      for
        _        <- produce(server, seeded)
        settings <- transactionalSettings(server, uniqueTransactionalId("unfenced"))
        reading  <- committedConsumerSettings(server, group).map(_.withoutAssignmentFencing)
        read     <- Ref[IO].of(Map.empty[TopicPartition, CommittableOffset[IO]])
        outcome  <-
          PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: first =>
            first.records.evalMap(next =>
              read.update(held => if held.contains(next.record.topicPartition) then held else held.updated(next.record.topicPartition, next.offset))
            ).compile.drain.background.surround:
              for
                before  <- read.get.iterateUntil(_.size == 2)
                outcome <-
                  PlatformKafkaClient().transactionalProducer(settings).use: producer =>
                    PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: second =>
                      second.records.compile.drain.background.surround:
                        for
                          kept    <- (assignmentOf(first, 1), assignmentOf(second, 1)).parTupled.map(_._1)
                          revoked <- IO.fromOption(before.keySet.find(!kept.contains(_)))(new AssertionError(s"nothing was revoked from $kept"))
                          _       <-
                            producer.transactionally(transaction =>
                              transaction.produce(NonEmptyList.one(record(output, Some("k"), Some("out"), validPartition(0)))) *>
                                transaction.commitOffsets(CommittableOffsetBatch.empty[IO].updated(before(revoked)))
                            )
                          lost <- first.committed(Set(revoked)).map(_.offsetsOnly)
                        yield (before(revoked).nextOffset, lost.get(revoked).flatten)
              yield outcome
          .timeout(90.seconds)
      yield
        val (recorded, lost) = outcome
        // Without fencing the offset goes out under the consumer's current membership, which the broker accepts, as it did before fencing existed.
        assertEquals(lost, Some(recorded))

  test(conformance("a plain commit cannot move a partition back to an offset its consumer read before losing it")):
    withBroker: server =>
      rolledBack(server, fencing = true).map: outcome =>
        rejected(outcome.stale, "the first consumer's offset for the partition it lost was committed")
        rejected(outcome.earlier, "an offset read before the rebalance was committed for a partition the consumer kept")
        rejected(outcome.direct, "the first consumer's committer committed a partition it no longer holds")
        // A committer's own commit names no lease, so it is held to the partitions the consumer holds now, which includes the one it kept.
        assertEquals(outcome.owned, Right(()))
        assertEquals(outcome.after, Some(outcome.ahead), "the partition should stay where its new owner committed it")

  test(conformance("with assignment fencing off, a plain commit moves a partition back to an offset its consumer read before losing it")):
    withBroker: server =>
      rolledBack(server, fencing = false).map: outcome =>
        assertEquals(outcome.stale, Right(()))
        assertEquals(outcome.direct, Right(()))
        // Without fencing the old offset goes out under the consumer's current membership, which the broker accepts, as it did before fencing.
        assert(
          outcome.after.exists(_.value < outcome.ahead.value),
          s"the stale commit should have moved the partition back from ${outcome.ahead}, got ${outcome.after}"
        )

  /** What each commit came to, where the second consumer committed, and where the partition it took ended up. */
  private final case class RolledBack(
      stale: Either[Throwable, Unit],
      earlier: Either[Throwable, Unit],
      direct: Either[Throwable, Unit],
      owned: Either[Throwable, Unit],
      ahead: Offset,
      after: Option[Offset]
  )

  private def rejected(outcome: Either[Throwable, Unit], clue: String): Unit =
    outcome match
      case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.IllegalGeneration), failure.getMessage)
      case other                                        => fail(s"$clue, got $other")

  /** The first consumer reads one record from each partition and keeps it. Once a second consumer has taken one partition and committed past that
    * record, the first plain-commits both records it kept, then commits each partition directly through its committer.
    */
  private def rolledBack(server: String, fencing: Boolean): IO[RolledBack] =
    val input  = validTopic(partitionedTopic)
    val group  = uniqueGroup(if fencing then "rollback-fenced" else "rollback-unfenced")
    val seeded = NonEmptyList.of(0, 0, 1, 1).map(value => record(input, Some("k"), Some("seed"), validPartition(value)))

    for
      _       <- produce(server, seeded)
      reading <-
        committedConsumerSettings(server, group)
          .map(settings => if fencing then settings.withAssignmentFencing else settings.withoutAssignmentFencing)
      held    <- Ref[IO].of(Map.empty[TopicPartition, CommittableOffset[IO]])
      taken   <- Ref[IO].of(Vector.empty[CommittableConsumerRecord[IO, Option[String], Option[String]]])
      outcome <-
        PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: first =>
          first.records.evalMap(next =>
            held.update(kept => if kept.contains(next.record.topicPartition) then kept else kept.updated(next.record.topicPartition, next.offset))
          ).compile.drain.background.surround:
            for
              before  <- held.get.iterateUntil(_.size == 2)
              outcome <-
                PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: second =>
                  second.records.evalMap(next => taken.update(_ :+ next)).compile.drain.background.surround:
                    for
                      kept    <- (assignmentOf(first, 1), assignmentOf(second, 1)).parTupled.map(_._1)
                      revoked <- IO.fromOption(before.keySet.find(!kept.contains(_)))(new AssertionError(s"nothing was revoked from $kept"))
                      // The second consumer reads past the first one's record and commits there.
                      latest <-
                        taken.get.map(
                          _.filter(_.record.topicPartition == revoked).lastOption.filter(_.offset.nextOffset.value > before(revoked).nextOffset.value)
                        ).iterateUntil(_.isDefined).map(_.get)
                      _       <- latest.offset.commit
                      stale   <- before(revoked).commit.attempt
                      earlier <- before(kept.head).commit.attempt
                      direct  <- before(revoked).committer.commit(Map(revoked -> before(revoked).nextOffset)).attempt
                      owned   <- before(kept.head).committer.commit(Map(kept.head -> before(kept.head).nextOffset)).attempt
                      after   <- first.committed(Set(revoked)).map(_.offsetsOnly)
                    yield RolledBack(stale, earlier, direct, owned, latest.offset.nextOffset, after.get(revoked).flatten)
            yield outcome
        .timeout(90.seconds)
    yield outcome

  test(conformance("under cooperative rebalancing, offsets read before a rebalance stay committable for the partitions a consumer keeps")):
    withBroker: server =>
      val input  = validTopic(partitionedTopic)
      val output = uniqueTopic("cooperative-output")
      val group  = uniqueGroup("cooperative")
      val seeded = NonEmptyList.of(0, 1).map(value => record(input, Some("k"), Some("seed"), validPartition(value)))
      // The Java client names its assignors by class, and librdkafka by name.
      val strategy = if backend == "jvm" then "org.apache.kafka.clients.consumer.CooperativeStickyAssignor" else "cooperative-sticky"

      for
        _        <- produce(server, seeded)
        settings <- transactionalSettings(server, uniqueTransactionalId("cooperative"))
        reading  <-
          committedConsumerSettings(server, group).flatMap(value =>
            IO.fromOption(value.withProperty("partition.assignment.strategy", strategy).toOption)(new AssertionError("strategy rejected"))
          )
        held    <- Ref[IO].of(Map.empty[TopicPartition, CommittableOffset[IO]])
        outcome <-
          PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: first =>
            first.records.evalMap(next =>
              held.update(kept => if kept.contains(next.record.topicPartition) then kept else kept.updated(next.record.topicPartition, next.offset))
            ).compile.drain.background.surround:
              for
                before  <- held.get.iterateUntil(_.size == 2)
                outcome <-
                  PlatformKafkaClient().transactionalProducer(settings).use: producer =>
                    PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(input))).use: second =>
                      second.records.compile.drain.background.surround:
                        for
                          kept     <- (assignmentOf(first, 1), assignmentOf(second, 1)).parTupled.map(_._1)
                          revoked  <- IO.fromOption(before.keySet.find(!kept.contains(_)))(new AssertionError(s"nothing was revoked from $kept"))
                          lost     <- before(revoked).commit.attempt
                          recorded <-
                            producer.transactionally(transaction =>
                              transaction.produce(NonEmptyList.one(record(output, Some("k"), Some("out"), validPartition(0)))) *>
                                transaction.commitOffsets(CommittableOffsetBatch.empty[IO].updated(before(kept.head)))
                            ).attempt
                          plain <- before(kept.head).commit.attempt
                          moved <- first.committed(Set(kept.head)).map(_.offsetsOnly)
                        yield (lost, recorded, plain, before(kept.head).nextOffset, moved.get(kept.head).flatten)
              yield outcome
          .timeout(90.seconds)
      yield
        val (lost, recorded, plain, expected, moved) = outcome
        rejected(lost, "the offset of the partition the consumer lost was committed")
        // The cooperative protocol revokes only the partition that moves, so the one the consumer kept keeps its lease.
        assertEquals(recorded, Right(()))
        assertEquals(plain, Right(()))
        assertEquals(moved, Some(expected))

  test(conformance("a record still waiting to be sent when its transaction aborts fails as purged")):
    withBroker: server =>
      val topic = uniqueTopic("purged")
      for
        base <- transactionalSettings(server, uniqueTransactionalId("purged"))
        // Held back long enough that the transaction aborts before the record is sent.
        settings <- base.withLinger(5.seconds).liftTo[IO]
        waiting  <- Deferred[IO, Either[Throwable, NonEmptyList[Produced[Option[String], Option[String]]]]]
        outcome  <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            producer.transactionally(transaction =>
              transaction.produce(NonEmptyList.one(record(topic, Some("k"), Some("held"), validPartition(0)))).attempt.flatMap(waiting.complete)
                .start *> IO.sleep(500.millis) *> IO.raiseError[Unit](new RuntimeException("the body failed"))
            ).attempt *> waiting.get.timeout(30.seconds)
      yield outcome match
        case Left(failure: KafkaException.BackendFailure) =>
          assertEquals(failure.code, Some(ErrorCode.Purged), failure.getMessage)
          assertEquals(failure.retriable, Some(false), failure.getMessage)
        case other => fail(s"the held record should have been purged, got $other")

  test(conformance("with the default assignment strategy, a rebalance refuses every offset read before it, including those of partitions kept")):
    withBroker: server =>
      val topic      = uniqueTopic("eager")
      val group      = uniqueGroup("eager")
      val partitions = (0 until 4).toList.map(index => TopicPartition(topic, validPartition(index)))

      for
        _        <- createPartitioned(server, topic, 4)
        _        <- partitions.traverse_(produceValues(server, _, 3))
        settings <- consumerSettings(server, group)
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: first =>
            for
              _       <- assignmentOf(first, 4)
              records <- first.records.take(12).compile.toList
              held = records.groupBy(_.offset.topicPartition).view.mapValues(_.head.offset).toMap
              outcome <-
                PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: second =>
                  for
                    _ <-
                      (first.assignmentChanges.filter(held => held.nonEmpty && held.size < 4).head.compile.lastOrError, assignmentOf(second, 2))
                        .parTupled
                    kept    <- first.assignment
                    commits <- held.toList.traverse((partition, offset) => offset.commit.attempt.map(partition -> _))
                  yield (held.keySet, kept, commits.toMap)
            yield outcome
          .timeout(120.seconds)
      yield
        val (held, kept, commits) = outcome
        assertEquals(held, partitions.toSet, "the first consumer should hold an offset of every partition")
        assert(kept.nonEmpty, "the first consumer should keep some partitions")
        commits.foreach((partition, commit) => refusedWith(Some(commit), ErrorCode.IllegalGeneration, s"the offset of $partition"))

  test(conformance("under the consumer group protocol, a consumer with a session timeout or heartbeat interval fails as invalid configuration")):
    withBroker: server =>
      val topic                                                                    = uniqueTopic("group-protocol")
      val protocol                                                                 = Map("group.protocol" -> "consumer")
      def consumed(settings: ConsumerSettings[IO, Option[String], Option[String]]) =
        PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(1).compile.lastOrError)
          .timeout(90.seconds)

      for
        _             <- createPartitioned(server, topic, 1)
        _             <- produceValues(server, TopicPartition(topic, validPartition(0)), 1)
        plain         <- consumerSettings(server, uniqueGroup("group-protocol")).flatMap(_.withProperties(protocol).liftTo[IO])
        timed         <- plain.withSessionTimeout(10.seconds).liftTo[IO]
        beating       <- plain.withHeartbeatInterval(1.second).liftTo[IO]
        unset         <- consumed(plain).attempt
        withTimeout   <- consumed(timed).void.attempt
        withHeartbeat <- consumed(beating).void.attempt
      yield
        assert(unset.isRight, s"a consumer leaving both unset should consume, got $unset")
        refusedWith(Some(withTimeout), ErrorCode.InvalidConfiguration, "a session timeout")
        refusedWith(Some(withHeartbeat), ErrorCode.InvalidConfiguration, "a heartbeat interval")

  test(conformance("a group is described and listed with its state, its members and their assignments while it has members")):
    withBroker: server =>
      val topic = uniqueTopic("groups-active")
      val group = uniqueGroup("groups-active")

      for
        _        <- createPartitioned(server, topic, 2)
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        settings <-
          ConsumerSettings.from(client.withClientId("group-member"), group, optionalDeserializer, optionalDeserializer, AutoOffsetReset.Earliest)
            .liftTo[IO]
        seen <-
          PlatformKafkaClient().admin(client).use: admin =>
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
              assignmentOf(consumer, 2) *> (admin.describeConsumerGroups(NonEmptySet.one(group)), admin.listConsumerGroups).tupled
          .timeout(90.seconds)
      yield
        val (described, listed) = seen
        described(group) match
          case Some(Right(description)) =>
            assertEquals(description.state, ConsumerGroupState.Stable)
            assert(!description.simple, "a group whose members joined it is not simple")
            assert(description.assignor.isDefined, "a group with members has an assignor")
            assert(description.coordinator.isDefined, "a group has a coordinator")
            assertEquals(description.members.map(_.clientId), List("group-member"))
            assertEquals(description.members.map(_.instanceId), List(None))
            assertEquals(
              description.members.flatMap(_.assignment).toSet,
              Set(TopicPartition(topic, validPartition(0)), TopicPartition(topic, validPartition(1)))
            )
          case other => fail(s"the group should be described, got $other")
        assertEquals(listed.get(group), Some(ConsumerGroupState.Stable))

  test(conformance("a group whose members left is empty, and one that never existed is not found")):
    withBroker: server =>
      val topic   = uniqueTopic("groups-empty")
      val group   = uniqueGroup("groups-empty")
      val missing = uniqueGroup("groups-missing")

      for
        _        <- createPartitioned(server, topic, 1)
        settings <- consumerSettings(server, group)
        _        <- PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(assignmentOf(_, 1)).timeout(90.seconds)
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        seen     <-
          PlatformKafkaClient().admin(client).use: admin =>
            (admin.describeConsumerGroups(NonEmptySet.of(group, missing)), admin.listConsumerGroups).tupled
          .timeout(90.seconds)
      yield
        val (described, listed) = seen
        described(group) match
          case Some(Right(description)) =>
            assertEquals(description.state, ConsumerGroupState.Empty)
            assertEquals(description.members, Nil)
            assert(!description.simple, "a group whose members joined it stays not simple once they leave")
            assertEquals(description.assignor, None)
          case other => fail(s"the group should be described, got $other")
        refusedWith(described(missing), ErrorCode.GroupIdNotFound, "a group that never existed")
        assertEquals(listed.get(group), Some(ConsumerGroupState.Empty))
        assertEquals(listed.get(missing), None)

  test(conformance("a group with members refuses changes to its offsets and its deletion")):
    withBroker: server =>
      val topic     = uniqueTopic("groups-busy")
      val group     = uniqueGroup("groups-busy")
      val partition = TopicPartition(topic, validPartition(0))

      for
        _        <- createPartitioned(server, topic, 1)
        settings <- consumerSettings(server, group)
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers  <-
          PlatformKafkaClient().admin(client).use: admin =>
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
              assignmentOf(consumer, 1) *>
                (
                  admin.alterConsumerGroupOffsets(group, NonEmptyMap.one(partition, OffsetAndMetadata(validOffset(0L)))),
                  admin.deleteConsumerGroupOffsets(group, NonEmptySet.one(partition)),
                  admin.deleteConsumerGroups(NonEmptySet.one(group))
                ).tupled
          .timeout(90.seconds)
      yield
        val (altered, deletedOffsets, deleted) = answers
        refusedWith(altered(partition), ErrorCode.UnknownMemberId, "altering a group with members")
        refusedWith(deletedOffsets(partition), ErrorCode.GroupSubscribedToTopic, "deleting offsets of a topic the group subscribes to")
        refusedWith(deleted(group), ErrorCode.NonEmptyGroup, "deleting a group with members")

  test(conformance("altering offsets answers each partition, and creates a group that only holds offsets")):
    withBroker: server =>
      val topic   = uniqueTopic("groups-alter")
      val group   = uniqueGroup("groups-alter")
      val first   = TopicPartition(topic, validPartition(0))
      val second  = TopicPartition(topic, validPartition(1))
      val unknown = TopicPartition(uniqueTopic("groups-nowhere"), validPartition(0))

      for
        _      <- createPartitioned(server, topic, 2)
        client <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        seen   <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              altered <-
                admin.alterConsumerGroupOffsets(
                  group,
                  NonEmptyMap.of(first -> OffsetAndMetadata(validOffset(3L), None, "kept"), unknown -> OffsetAndMetadata(validOffset(0L)))
                )
              all       <- admin.listConsumerGroupOffsets(group)
              named     <- admin.listConsumerGroupOffsets(group, NonEmptySet.of(first, second))
              described <- admin.describeConsumerGroups(NonEmptySet.one(group))
              listed    <- admin.listConsumerGroups
            yield (altered, all, named, described, listed)
          .timeout(90.seconds)
      yield
        val (altered, all, named, described, listed) = seen
        assertEquals(altered(first).map(_.leftMap(_.getMessage)), Some(Right(())))
        refusedWith(altered(unknown), ErrorCode.UnknownTopicOrPartition, "altering the offset of an unknown topic")
        assertEquals(all, Map(first -> OffsetAndMetadata(validOffset(3L), None, "kept")))
        assertEquals(named.toSortedMap.toMap, Map(first -> Some(OffsetAndMetadata(validOffset(3L), None, "kept")), second -> None))
        described(group) match
          case Some(Right(description)) =>
            assertEquals(description.state, ConsumerGroupState.Empty)
            assert(description.simple, "a group that only holds offsets is simple")
            assertEquals(description.members, Nil)
          case other => fail(s"the group should be described, got $other")
        assertEquals(listed.get(group), Some(ConsumerGroupState.Empty))

  test(conformance("deleting offsets and groups answers each one, and a group that never existed is not found")):
    withBroker: server =>
      val topic  = uniqueTopic("groups-delete")
      val group  = uniqueGroup("groups-delete")
      val never  = uniqueGroup("groups-never")
      val first  = TopicPartition(topic, validPartition(0))
      val second = TopicPartition(topic, validPartition(1))

      for
        _      <- createPartitioned(server, topic, 2)
        client <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        seen   <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              _ <-
                admin.alterConsumerGroupOffsets(
                  group,
                  NonEmptyMap.of(first -> OffsetAndMetadata(validOffset(1L)), second -> OffsetAndMetadata(validOffset(2L)))
                )
              deletedOffsets <- admin.deleteConsumerGroupOffsets(group, NonEmptySet.one(second))
              remaining      <- admin.listConsumerGroupOffsets(group)
              neverOffsets   <- admin.deleteConsumerGroupOffsets(never, NonEmptySet.one(second))
              deleted        <- admin.deleteConsumerGroups(NonEmptySet.of(group, never))
              listed         <- admin.listConsumerGroups
              gone           <- admin.listConsumerGroupOffsets(group)
            yield (deletedOffsets, remaining, neverOffsets, deleted, listed, gone)
          .timeout(90.seconds)
      yield
        val (deletedOffsets, remaining, neverOffsets, deleted, listed, gone) = seen
        assertEquals(deletedOffsets(second).map(_.leftMap(_.getMessage)), Some(Right(())))
        assertEquals(remaining, Map(first -> OffsetAndMetadata(validOffset(1L))))
        refusedWith(neverOffsets(second), ErrorCode.GroupIdNotFound, "deleting offsets of a group that never existed")
        assertEquals(deleted(group).map(_.leftMap(_.getMessage)), Some(Right(())))
        refusedWith(deleted(never), ErrorCode.GroupIdNotFound, "deleting a group that never existed")
        assertEquals(listed.get(group), None)
        assertEquals(gone, Map.empty)

  test(conformance("deleting records removes those below each offset and answers each partition on its own")):
    withBroker: server =>
      val topic = uniqueTopic("delete-records")
      val first = TopicPartition(topic, validPartition(0))
      val other = TopicPartition(topic, validPartition(1))

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 2, replicationFactor = 1).liftTo[IO]
        answers  <-
          PlatformKafkaClient().admin(client).use: admin =>
            admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 2) *> produceValues(server, first, 5) *>
              produceValues(server, other, 2) *> admin.deleteRecords(NonEmptyMap.of(first -> validOffset(3), other -> validOffset(100)))
          .timeout(90.seconds)
        beginnings <- beginningsOf(server, Set(first, other))
        remaining  <- consumePartition(server, first, 2)
      yield
        assertEquals(answers(first).map(_.leftMap(_.getMessage)), Some(Right(validOffset(3))))
        answers(other) match
          case Some(Left(failure: KafkaException.BackendFailure)) => assertEquals(failure.code, Some(ErrorCode.OffsetOutOfRange), failure.getMessage)
          case refused                                            => fail(s"a cut past the end of a partition should be refused, got $refused")
        assertEquals(beginnings, Map(first -> validOffset(3), other -> validOffset(0)), "only the accepted cut should move a beginning")
        assertEquals(remaining, List(3L, 4L), "the records at and above the cut should remain")

  test(conformance("a cut at or below the start of a partition answers with its current low watermark")):
    withBroker: server =>
      val partition = TopicPartition(uniqueTopic("delete-again"), validPartition(0))

      for
        _       <- produceValues(server, partition, 4)
        client  <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers <-
          PlatformKafkaClient().admin(client).use: admin =>
            List(3L, 3L, 1L, 4L).traverse(offset => admin.deleteRecords(NonEmptyMap.one(partition, validOffset(offset))))
          .timeout(90.seconds)
        beginnings <- beginningsOf(server, Set(partition))
      yield
        assertEquals(answers.map(_(partition).map(_.leftMap(_.getMessage))), List(3L, 3L, 3L, 4L).map(value => Some(Right(validOffset(value)))))
        assertEquals(beginnings, Map(partition -> validOffset(4)), "a cut at the end should leave the partition empty")

  test(conformance("deleting records moves no consumer group's offsets")):
    withBroker: server =>
      val topic     = uniqueTopic("delete-committed")
      val partition = TopicPartition(topic, validPartition(0))
      val group     = uniqueGroup("delete-committed")

      for
        _      <- produceValues(server, partition, 3)
        _      <- consumerSettings(server, group).flatMap(settings => commitFirst(settings, topic))
        client <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        _      <- PlatformKafkaClient().admin(client).use(_.deleteRecords(NonEmptyMap.one(partition, validOffset(3)))).timeout(90.seconds)
        stays  <-
          consumerSettings(server, group).flatMap: settings =>
            PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(partition)))
              .use(_.committed(Set(partition)).map(_.offsetsOnly)).timeout(60.seconds)
      yield assertEquals(stays, Map(partition -> Some(validOffset(1))))

  test(conformance("deleting records against a cluster that cannot be reached fails as a timed out request")):
    val partitions =
      NonEmptyMap.of(
        TopicPartition(uniqueTopic("delete-unreachable"), validPartition(0)) -> validOffset(1),
        TopicPartition(uniqueTopic("delete-unreachable"), validPartition(1)) -> validOffset(1)
      )
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use(_.deleteRecords(partitions)).attempt.timeout(150.seconds).map(unreachable)

  test(conformance("deleting records answers a topic or partition the cluster does not have as unknown, beside the ones it has")):
    withBroker: server =>
      val known           = TopicPartition(uniqueTopic("delete-known"), validPartition(0))
      val absentPartition = TopicPartition(known.topic, validPartition(5))
      val absentTopic     = TopicPartition(uniqueTopic("delete-unknown"), validPartition(0))
      for
        _       <- produceValues(server, known, 2)
        client  <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers <-
          PlatformKafkaClient().admin(client).use: admin =>
            // The Java client would otherwise retry an unknown topic for a whole minute, which this bound rules out.
            admin.deleteRecords(NonEmptyMap.of(known -> validOffset(1), absentPartition -> validOffset(1), absentTopic -> validOffset(1)))
              .timeout(30.seconds)
      yield
        assertEquals(answers(known).map(_.leftMap(_.getMessage)), Some(Right(validOffset(1))))
        List(absentPartition, absentTopic).foreach(partition => unknownAnswer(answers(partition), partition.show))

  test(conformance("deleting records of topics the cluster has none of answers each partition as unknown")):
    withBroker: server =>
      val partitions =
        NonEmptyMap.of(
          TopicPartition(uniqueTopic("delete-none"), validPartition(0)) -> validOffset(1),
          TopicPartition(uniqueTopic("delete-none"), validPartition(0)) -> validOffset(1)
        )
      ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
        PlatformKafkaClient().admin(client).use(_.deleteRecords(partitions)).timeout(30.seconds).map: answers =>
          assertEquals(answers.keys.toSortedSet.toSet, partitions.keys.toSortedSet.toSet)
          answers.toSortedMap.foreach((partition, answer) => unknownAnswer(Some(answer), partition.show))

  /** A cut is bounded by what has been written, not by what has committed, so it reaches into a transaction still open. */
  test(conformance("deleting records can remove those of a transaction that has not committed yet")):
    withBroker: server =>
      val topic     = uniqueTopic("delete-transaction")
      val partition = TopicPartition(topic, validPartition(0))

      for
        _        <- produceValues(server, partition, 2)
        settings <- transactionalSettings(server, uniqueTransactionalId("delete-transaction"))
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers  <-
          PlatformKafkaClient().transactionalProducer(settings).use: producer =>
            producer.transactionally: transaction =>
              val pending = NonEmptyList.of("pending-0", "pending-1").map(value => record(topic, Some("k"), Some(value), partition.partition))
              // Records two and three belong to the open transaction, so a cut at four covers all of it.
              transaction.produce(pending) *> PlatformKafkaClient().admin(client).use(_.deleteRecords(NonEmptyMap.one(partition, validOffset(4))))
          .timeout(90.seconds)
        _        <- produce(server, NonEmptyList.one(record(topic, Some("k"), Some("after"), partition.partition)))
        consumed <- consumeCommitted(server, topic, 1)
      yield
        assertEquals(answers(partition).map(_.leftMap(_.getMessage)), Some(Right(validOffset(4))))
        assertEquals(consumed.map(_.record.value), List(Some("after")), "the committed transaction should have lost its deleted records")

  test(conformance("with rebalance-safe commits, a member commits what it is handling as its partitions are revoked, so no record is read twice")):
    withBroker: server =>
      val topic                             = uniqueTopic("safe-handover")
      val group                             = uniqueGroup("safe-handover")
      val selection                         = Selection.Topics(NonEmptySet.one(topic))
      val partitions                        = List(validPartition(0), validPartition(1))
      def values(label: String, count: Int) =
        NonEmptyList.fromListUnsafe(
          partitions.flatMap(partition => List.tabulate(count)(index => record(topic, None, Some(s"$label-${partition.value}-$index"), partition)))
        )
      // Slow enough that the first member is still handling a record when the second one joins.
      val early = values("early", 40)
      // Produced once each member holds one partition, so each has to read the one it was given.
      val late       = values("late", 5)
      val everything = (early ++ late.toList).toList.flatMap(_.value).toSet

      for
        _       <- createPartitioned(server, topic, 2)
        _       <- produce(server, early)
        plain   <- consumerSettings(server, group)
        safe    <- plain.withRebalanceSafeCommits(20.seconds).liftTo[IO]
        reads   <- Ref.of[IO, Vector[(String, String)]](Vector.empty)
        commits <- Ref.of[IO, Vector[Either[Throwable, Unit]]](Vector.empty)
        reading =
          (member: String, consumer: KafkaConsumer[IO, Option[String], Option[String]], handling: FiniteDuration) =>
            consumer.records.evalMap: read =>
              reads.update(_ :+ (member -> read.record.value.getOrElse(""))) *> IO.sleep(handling) *>
                read.offset.commit.attempt.flatMap(outcome => commits.update(_ :+ outcome))
        readAll = (IO.sleep(250.millis) *> reads.get).iterateUntil(_.map(_._2).toSet == everything)
        _ <-
          PlatformKafkaClient().consumer(safe, selection).use: first =>
            reading("first", first, 250.millis).compile.drain.background.surround:
              (IO.sleep(250.millis) *> reads.get).iterateUntil(_.size >= 3) *> PlatformKafkaClient().consumer(plain, selection).use: second =>
                reading("second", second, Duration.Zero).compile.drain.background.surround:
                  (assignmentOf(first, 1), assignmentOf(second, 1)).parTupled *> produce(server, late) *> readAll
          .timeout(120.seconds)
        read      <- reads.get
        committed <- commits.get
      yield
        val repeated = read.groupBy(_._2).collect { case (value, readers) if readers.size > 1 => value -> readers.map(_._1) }
        assertEquals(repeated, Map.empty[String, Vector[String]], "records were read more than once")
        assertEquals(committed.collect { case Left(error) => error.getMessage }, Vector.empty[String], "commits failed")

  test(conformance("with rebalance-safe commits, a revocation goes ahead once its limit passes, and the records are read again")):
    withBroker: server =>
      heldPastLimit(server, "safe-proceed", RebalanceSafeCommits.AtLimit.Proceed): (first, firstReads, secondReads, waited) =>
        val readAgain = (IO.sleep(250.millis) *> firstReads.get).iterateUntil(_.size >= 3).timeout(60.seconds)
        IO.race(first.joinWithNever, readAgain).map:
          case Left(ended) => fail(s"the first member's record stream ended: $ended")
          case Right(read) =>
            assert(waited >= 4.seconds, s"the second member was given a partition after $waited, before the limit passed")
            assertEquals(secondReads.size, 1)
            assert(read.take(2).contains(secondReads.head), s"the second member read ${secondReads.head}, which the first had not handed out")
            assertEquals((secondReads ++ read.drop(2)).toSet, read.take(2).toSet, "between them, the members should read both records again")

  test(conformance("with rebalance-safe commits set to fail at the limit, the record stream fails naming the uncommitted partitions")):
    withBroker: server =>
      heldPastLimit(server, "safe-fail", RebalanceSafeCommits.AtLimit.Fail): (first, _, _, waited) =>
        first.joinWithNever.timeout(60.seconds).map:
          case Left(failure: KafkaException.UncommittedRevocation) =>
            assert(waited >= 4.seconds, s"the second member was given a partition after $waited, before the limit passed")
            assertEquals(failure.topicPartitions.map(_.partition), Set(validPartition(0), validPartition(1)))
          case other => fail(s"the record stream should have failed as an uncommitted revocation, got $other")

  test(
    conformance("with rebalance-safe commits, a member that has lost its partitions lets them go without waiting, and cannot commit what it held")
  ):
    withBroker: server =>
      val topic     = uniqueTopic("safe-lost")
      val selection = Selection.Topics(NonEmptySet.one(topic))
      // More records than a consumer holds unread, so fetching stops while the first one is in hand.
      val records = NonEmptyList.fromListUnsafe(List.tabulate(2000)(index => record(topic, None, Some(s"record $index"), validPartition(index % 2))))

      // A member of the same group reading another topic keeps the group from being empty, so the broker does not remove it while
      // the member under test is out of it. It joins first, so its joining does not revoke the record held by the member under test.
      val elsewhere = uniqueTopic("safe-lost-elsewhere")

      for
        _      <- createPartitioned(server, topic, 2)
        _      <- createPartitioned(server, elsewhere, 1)
        _      <- produce(server, records)
        plain  <- consumerSettings(server, uniqueGroup("safe-lost"))
        member <-
          plain.withSessionTimeout(6.seconds).andThen(_.withHeartbeatInterval(2.seconds)).andThen(_.withMaxPollInterval(6.seconds))
            .andThen(_.withRebalanceSafeCommits(4.seconds, RebalanceSafeCommits.AtLimit.Fail)).liftTo[IO]
        inHand <- Deferred[IO, (TopicPartition, Either[Throwable, Unit])]
        read   <- Ref.of[IO, Vector[(TopicPartition, Offset)]](Vector.empty)
        // The record held through the loss was never committed, so it comes again once the member is given its partitions back.
        rejoined  = (IO.sleep(250.millis) *> read.get).iterateUntil(seen => seen.headOption.exists(first => seen.count(_ == first) > 1))
        bystander = PlatformKafkaClient().consumer(plain, Selection.Topics(NonEmptySet.one(elsewhere))).evalTap(assignmentOf(_, 1))
        outcome <- (bystander *> PlatformKafkaClient().consumer(member, selection)).use: consumer =>
          // The first record stays in hand, uncommitted, for longer than the poll interval, which takes the member out of its group.
          // It is then committed, as an application finishing with it would.
          val reading =
            consumer.records.zipWithIndex.evalMap: (handed, index) =>
              read.update(_ :+ (handed.offset.topicPartition -> handed.record.offset)) *>
                (IO.sleep(15.seconds) *>
                  handed.offset.commit.attempt.flatMap(committed => inHand.complete(handed.offset.topicPartition -> committed))).whenA(index == 0)
          // Waiting past the limit after the member is back gives a failure every chance to arrive.
          IO.race(reading.compile.drain.attempt, rejoined.timeout(90.seconds) *> IO.sleep(6.seconds)).flatMap: raced =>
            inHand.get.flatMap((partition, _) => consumer.committed(Set(partition))).map(raced -> _)
        .timeout(120.seconds)
        commit <- inHand.get
      yield
        val (raced, stored)        = outcome
        val (partition, committed) = commit
        raced.left.foreach(ended => fail(s"the record stream ended while the member was out of its group: $ended"))
        committed match
          case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.UnknownMemberId), failure.getMessage)
          case other => fail(s"the commit of the record held through the loss should have failed as an unknown member, got $other")
        assertEquals(stored.get(partition).flatten, None, "the refused commit stored an offset")

  /** A first member with a limit of four seconds hands out one record of each partition and commits neither. A second member then joins, and `check`
    * gets the first member's record stream, what each member has read, and how long the second one waited for its partition.
    */
  private def heldPastLimit(server: String, label: String, atLimit: RebalanceSafeCommits.AtLimit)(
      check: (FiberIO[Either[Throwable, Unit]], Ref[IO, Vector[String]], List[String], FiniteDuration) => IO[Unit]
  ): IO[Unit] =
    val topic     = uniqueTopic(label)
    val group     = uniqueGroup(label)
    val selection = Selection.Topics(NonEmptySet.one(topic))
    val records   = NonEmptyList.of(record(topic, None, Some("held-0"), validPartition(0)), record(topic, None, Some("held-1"), validPartition(1)))

    for
      _          <- createPartitioned(server, topic, 2)
      _          <- produce(server, records)
      plain      <- consumerSettings(server, group)
      safe       <- plain.withRebalanceSafeCommits(4.seconds, atLimit).liftTo[IO]
      firstReads <- Ref.of[IO, Vector[String]](Vector.empty)
      _          <-
        PlatformKafkaClient().consumer(safe, selection).use: first =>
          first.records.evalMap(read => firstReads.update(_ :+ read.record.value.getOrElse(""))).compile.drain.attempt.start.flatMap: reading =>
            for
              _           <- (IO.sleep(250.millis) *> firstReads.get).iterateUntil(_.size >= 2).timeout(60.seconds)
              started     <- IO.monotonic
              secondReads <-
                PlatformKafkaClient().consumer(plain, selection)
                  .use(_.records.take(1).evalTap(_.offset.commit).compile.toList.map(_.map(_.record.value.getOrElse(""))))
              joined <- IO.monotonic
              _      <- check(reading, firstReads, secondReads, joined - started)
            yield ()
        .timeout(120.seconds)
    yield ()

  test(conformance("with starting offsets, a subscribed consumer starts its partition at the offset supplied, with its leader epoch")):
    withBroker: server =>
      val partition = TopicPartition(uniqueTopic("starting"), validPartition(0))

      for
        _        <- produceValues(server, partition, 10)
        asked    <- Ref.of[IO, List[Set[TopicPartition]]](Nil)
        plain    <- consumerSettings(server, uniqueGroup("starting"))
        settings <-
          plain.withStartingOffsets(10.seconds)(partitions =>
            asked.update(_ :+ partitions).as(partitions.map(_ -> StartingOffset(validOffset(6), Some(validEpoch(0)))).toMap)
          ).liftTo[IO]
        read      <- firstOffsets(settings, Selection.Topics(NonEmptySet.one(partition.topic)), 4)
        questions <- asked.get
      yield
        assertEquals(questions, List(Set(partition)))
        assertEquals(read, List(6L, 7L, 8L, 9L))

  test(conformance("with starting offsets, a partition they answer nothing for starts at its committed offset, or where the offset reset says")):
    withBroker: server =>
      val partition = TopicPartition(uniqueTopic("starting-committed"), validPartition(0))
      val selection = Selection.Topics(NonEmptySet.one(partition.topic))

      for
        _        <- produceValues(server, partition, 3)
        plain    <- consumerSettings(server, uniqueGroup("starting-committed"))
        settings <- plain.withStartingOffsets(10.seconds)(_ => IO.pure(Map.empty)).liftTo[IO]
        reset    <- firstOffsets(settings, selection, 2)
        _        <- commitFirst(plain, partition.topic)
        resumed  <- firstOffsets(settings, selection, 2)
      yield
        assertEquals(reset, List(0L, 1L), "without a committed offset, the earliest reset should start the partition at its first record")
        assertEquals(resumed, List(1L, 2L), "the partition should start at its committed offset")

  test(conformance("with starting offsets past the end of the partition, the offset reset decides where it starts")):
    withBroker: server =>
      val partition = TopicPartition(uniqueTopic("starting-beyond"), validPartition(0))

      for
        _        <- produceValues(server, partition, 3)
        plain    <- consumerSettings(server, uniqueGroup("starting-beyond"))
        settings <-
          plain.withStartingOffsets(10.seconds)(partitions => IO.pure(partitions.map(_ -> StartingOffset(validOffset(1000))).toMap)).liftTo[IO]
        read <- firstOffsets(settings, Selection.Topics(NonEmptySet.one(partition.topic)), 3)
      yield assertEquals(read, List(0L, 1L, 2L))

  test(conformance("with starting offsets, partitions named directly start at the offset supplied")):
    withBroker: server =>
      val partition = TopicPartition(uniqueTopic("starting-named"), validPartition(0))

      for
        _        <- produceValues(server, partition, 6)
        plain    <- consumerSettings(server, uniqueGroup("starting-named"))
        settings <- plain.withStartingOffsets(10.seconds)(partitions => IO.pure(partitions.map(_ -> StartingOffset(validOffset(4))).toMap)).liftTo[IO]
        read     <- firstOffsets(settings, Selection.Partitions(NonEmptySet.one(partition)), 2)
      yield assertEquals(read, List(4L, 5L))

  test(conformance("with offset reset set to fail, a group without a committed offset fails as needing a reset")):
    withBroker: server =>
      val topic = uniqueTopic("reset-fail-none")
      for
        _        <- produceValues(server, TopicPartition(topic, validPartition(0)), 1)
        settings <- consumerSettings(server, uniqueGroup("reset-fail-none")).map(_.withAutoOffsetReset(AutoOffsetReset.Fail))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
            .use(_.records.take(1).map(_.record.offset.value).compile.toList).attempt.timeout(60.seconds)
      yield resetRequired(outcome)

  test(conformance("a group whose committed offset was deleted fails with offset reset set to fail and starts at the first record with earliest")):
    withBroker: server =>
      val topic                            = uniqueTopic("reset-deleted")
      val partition                        = TopicPartition(topic, validPartition(0))
      val group                            = uniqueGroup("reset-deleted")
      def starting(reset: AutoOffsetReset) = consumerSettings(server, group).map(_.withAutoOffsetReset(reset))
      for
        _ <- produceValues(server, partition, 3)
        _ <- consumerSettings(server, group).flatMap(settings => commitFirst(settings, topic))
        // The group's committed offset is one, which this cut leaves outside the partition.
        client <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        _      <- PlatformKafkaClient().admin(client).use(_.deleteRecords(NonEmptyMap.one(partition, validOffset(2)))).timeout(90.seconds)
        failed <-
          starting(AutoOffsetReset.Fail).flatMap: settings =>
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
              .use(_.records.take(1).map(_.record.offset.value).compile.toList).attempt.timeout(60.seconds)
        earliest <-
          starting(AutoOffsetReset.Earliest).flatMap: settings =>
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
              .use(_.records.take(1).map(_.record.offset.value).compile.toList).timeout(60.seconds)
      yield
        resetRequired(failed)
        assertEquals(earliest, List(2L), "Earliest should start at the first record still in the partition")

  test(conformance("a topic's configuration reports its own settings and the values it inherits")):
    withBroker: server =>
      val topic = uniqueTopic("configuration")
      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1, configuration = Map("retention.ms" -> "123456789")).liftTo[IO]
        answers  <-
          PlatformKafkaClient().admin(client).use: admin =>
            admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 1) *> admin.describeTopicConfigurations(NonEmptySet.one(topic))
          .timeout(90.seconds)
        configuration <- IO.fromEither(answers(topic).toRight(new AssertionError(s"no answer for the topic: $answers")).flatten)
      yield
        assertEquals(
          configuration.get("retention.ms"),
          Some(ConfigurationEntry(ConfigurationValue.Present("123456789"), ConfigurationSource.TopicOverride, readOnly = false))
        )
        assertEquals(
          configuration.get("cleanup.policy").map(entry => (entry.value, entry.source)),
          Some((ConfigurationValue.Present("delete"), ConfigurationSource.Default))
        )
        assertEquals(
          configuration.get("message.timestamp.type").map(entry => (entry.value, entry.source)),
          Some((ConfigurationValue.Present("CreateTime"), ConfigurationSource.Default))
        )
        List("retention.bytes", "delete.retention.ms").foreach(name => assert(configuration.get(name).isDefined, s"$name should be reported"))
        assertEquals(configuration.get("no.such.property"), None)

  test(conformance("a topic the cluster does not have answers with its own failure")):
    withBroker: server =>
      val present = uniqueTopic("configuration-present")
      val missing = uniqueTopic("configuration-missing")
      for
        _       <- produceValues(server, TopicPartition(present, validPartition(0)), 1)
        client  <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers <- PlatformKafkaClient().admin(client).use(_.describeTopicConfigurations(NonEmptySet.of(present, missing))).timeout(90.seconds)
      yield
        assert(answers(present).exists(_.isRight), s"an existing topic should be described, got ${answers(present)}")
        answers(missing) match
          case Some(Left(failure: KafkaException.BackendFailure)) =>
            assertEquals(failure.code, Some(ErrorCode.UnknownTopicOrPartition), failure.getMessage)
          case other => fail(s"a topic the cluster does not have should fail, got $other")

  test(conformance("describing topic configurations against a cluster that cannot be reached fails as a timed out request")):
    val topics = NonEmptySet.of(uniqueTopic("configuration-unreachable"), uniqueTopic("configuration-unreachable"))
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use(_.describeTopicConfigurations(topics)).attempt.timeout(150.seconds).map(unreachable)

  test(conformance("changing a topic's configuration sets, appends to, removes from, and unsets the named properties, and leaves the others")):
    withBroker: server =>
      val topic                                                                          = uniqueTopic("configuration-change")
      def changing(admin: KafkaAdminClient[IO], changes: (String, ConfigurationChange)*) =
        admin.alterTopicConfigurations(NonEmptyMap.one(topic, NonEmptyMap.of(changes.head, changes.tail*)))
      def entries(admin: KafkaAdminClient[IO]) =
        admin.describeTopicConfigurations(NonEmptySet.one(topic))
          .flatMap(answers => IO.fromEither(answers(topic).toRight(new AssertionError("no answer")).flatten)).map(configuration =>
            List("retention.ms", "cleanup.policy", "segment.ms")
              .map(name => name -> configuration.get(name).map(entry => (entry.value, entry.source))).toMap
          )

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        observed <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              _      <- admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 1)
              before <- entries(admin)
              first  <-
                changing(admin, "retention.ms" -> ConfigurationChange.Set("123456789"), "cleanup.policy" -> ConfigurationChange.Append("compact"))
              changed  <- entries(admin)
              second   <- changing(admin, "retention.ms" -> ConfigurationChange.Unset, "cleanup.policy" -> ConfigurationChange.Subtract("delete"))
              restored <- entries(admin)
            yield (before, first, changed, second, restored)
          .timeout(90.seconds)
      yield
        val (before, first, changed, second, restored) = observed
        assertEquals(first.map(_.leftMap(_.getMessage)), NonEmptyMap.one(topic, Right(())))
        assertEquals(second.map(_.leftMap(_.getMessage)), NonEmptyMap.one(topic, Right(())))
        assertEquals(changed("retention.ms"), Some((ConfigurationValue.Present("123456789"), ConfigurationSource.TopicOverride)))
        assertEquals(changed("cleanup.policy"), Some((ConfigurationValue.Present("delete,compact"), ConfigurationSource.TopicOverride)))
        assertEquals(changed("segment.ms"), before("segment.ms"))
        assertEquals(restored("retention.ms"), before("retention.ms"))
        assertEquals(restored("retention.ms").map(_._2), Some(ConfigurationSource.Default))
        assertEquals(restored("cleanup.policy"), Some((ConfigurationValue.Present("compact"), ConfigurationSource.TopicOverride)))
        assertEquals(restored("segment.ms"), before("segment.ms"))

  test(conformance("a configuration change the broker refuses answers for its own topic, and the other topics are still changed")):
    withBroker: server =>
      val changed  = uniqueTopic("configuration-changed")
      val listless = uniqueTopic("configuration-listless")
      val unknown  = uniqueTopic("configuration-unknown-property")
      val missing  = uniqueTopic("configuration-change-missing")
      for
        client    <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopics <- List(changed, listless, unknown).traverse(NewTopic.from(_, partitions = 1, replicationFactor = 1).liftTo[IO])
        answers   <-
          PlatformKafkaClient().admin(client).use: admin =>
            admin.createTopics(NonEmptySet.of(newTopics.head, newTopics.tail*)) *> List(changed, listless, unknown)
              .traverse_(described(admin, _, 1)) *> admin.alterTopicConfigurations(NonEmptyMap.of(
              changed  -> NonEmptyMap.one("retention.ms", ConfigurationChange.Set("123456789")),
              listless -> NonEmptyMap.one("retention.ms", ConfigurationChange.Append("123456789")),
              unknown  -> NonEmptyMap.one("no.such.property", ConfigurationChange.Set("1")),
              missing  -> NonEmptyMap.one("retention.ms", ConfigurationChange.Set("123456789"))
            ))
          .timeout(90.seconds)
      yield
        assertEquals(answers(changed).map(_.leftMap(_.getMessage)), Some(Right(())))
        refusedWith(answers(listless), ErrorCode.InvalidConfiguration, "appending to a property that holds no list")
        refusedWith(answers(unknown), ErrorCode.InvalidConfiguration, "a property Kafka does not have")
        refusedWith(answers(missing), ErrorCode.UnknownTopicOrPartition, "a topic the cluster does not have")

  test(conformance("changing topic configurations against a cluster that cannot be reached fails as a timed out request")):
    val changes =
      NonEmptyMap.of(
        uniqueTopic("configuration-change-unreachable") -> NonEmptyMap.one("retention.ms", ConfigurationChange.Set("1")),
        uniqueTopic("configuration-change-unreachable") -> NonEmptyMap.one("retention.ms", ConfigurationChange.Set("1"))
      )
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use(_.alterTopicConfigurations(changes)).attempt.timeout(150.seconds).map(unreachable)

  test(
    conformance("electing the preferred leader of a partition it already leads answers as done, and a partition the cluster does not have as unknown")
  ):
    withBroker: server =>
      val led     = TopicPartition(uniqueTopic("election-led"), validPartition(0))
      val missing = TopicPartition(uniqueTopic("election-missing"), validPartition(0))
      for
        _       <- produceValues(server, led, 1)
        client  <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers <- PlatformKafkaClient().admin(client).use(_.electPreferredLeaders(NonEmptySet.of(led, missing))).timeout(90.seconds)
      yield
        assertEquals(answers(led).map(_.leftMap(_.getMessage)), Some(Right(())))
        refusedWith(answers(missing), ErrorCode.UnknownTopicOrPartition, "a partition the cluster does not have")

  test(conformance("electing preferred leaders against a cluster that cannot be reached fails as a timed out request")):
    val partitions =
      NonEmptySet.of(
        TopicPartition(uniqueTopic("election-unreachable"), validPartition(0)),
        TopicPartition(uniqueTopic("election-unreachable"), validPartition(1))
      )
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use(_.electPreferredLeaders(partitions)).attempt.timeout(150.seconds).map(unreachable)

  test(conformance("describing the cluster reports its id, and its broker as clients reach it, which is also its controller")):
    withBroker: server =>
      val (host, port) = server.splitAt(server.lastIndexOf(':'))
      val broker       = Broker(BrokerId.from(1).toOption.get, host, port.drop(1).toInt, None)

      for
        client      <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        description <- PlatformKafkaClient().admin(client).use(_.describeCluster).timeout(60.seconds)
      yield
        assert(description.id.nonEmpty, s"the cluster should report an id, got $description")
        assertEquals(description.brokers, NonEmptySet.one(broker))
        assertEquals(description.controller, Some(broker))

  test(conformance("describing topics answers each one: its partitions and replicas, whether Kafka keeps it, or that it is unknown")):
    withBroker: server =>
      val topic   = uniqueTopic("describe-topics")
      val missing = uniqueTopic("describe-topics-missing")
      val offsets = validTopic("__consumer_offsets")
      val leader  = BrokerId.from(1).toOption.get

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, 2, 1, Map.empty).liftTo[IO]
        _        <- PlatformKafkaClient().admin(client).use(admin => admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 2))
        // A fresh cluster creates the offsets topic for the first commit.
        _       <- produceValues(server, TopicPartition(topic, validPartition(0)), 1)
        _       <- consumerSettings(server, uniqueGroup("describe-topics")).flatMap(commitFirst(_, topic))
        answers <- PlatformKafkaClient().admin(client).use(_.describeTopics(NonEmptySet.of(topic, offsets, missing))).timeout(60.seconds)
      yield
        val placed = PartitionDescription(Some(leader), NonEmptyList.one(leader), List(leader))
        answers(topic) match
          case Some(Right(description)) =>
            assert(!description.internal, s"a topic of the application should not be internal, got $description")
            assertEquals(description.partitions, NonEmptyMap.of(validPartition(0) -> placed, validPartition(1) -> placed))
          case other => fail(s"the topic should be described, got $other")
        assert(answers(offsets).exists(_.exists(_.internal)), s"the offsets topic should be internal, got ${answers(offsets)}")
        refusedWith(answers(missing), ErrorCode.UnknownTopicOrPartition, "a topic the cluster does not have")

  test(conformance("a topic keeps its id, and one deleted and created again under the same name gets a new id")):
    withBroker: server =>
      val topic                                                  = uniqueTopic("topic-id")
      def idOf(admin: KafkaAdminClient[IO]): IO[Option[TopicId]] =
        admin.describeTopics(NonEmptySet.one(topic)).map(_(topic).flatMap(_.toOption).map(_.id))
      // Deleting a topic finishes in the background, so it is described until it is gone and then until it is back.
      def until(admin: KafkaAdminClient[IO], done: Option[TopicId] => Boolean): IO[Option[TopicId]] =
        idOf(admin).flatMap(id => if done(id) then IO.pure(id) else IO.sleep(250.millis) *> until(admin, done)).timeout(60.seconds)

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, 1, 1, Map.empty).liftTo[IO]
        ids      <-
          PlatformKafkaClient().admin(client).use: admin =>
            for
              _      <- admin.createTopics(NonEmptySet.one(newTopic))
              first  <- until(admin, _.nonEmpty)
              again  <- idOf(admin)
              _      <- admin.deleteTopics(NonEmptySet.one(topic))
              _      <- until(admin, _.isEmpty)
              _      <- admin.createTopics(NonEmptySet.one(newTopic))
              second <- until(admin, _.nonEmpty)
            yield (first, again, second)
      yield
        val (first, again, second) = ids
        assertEquals(again, first)
        assertNotEquals(second, first)

  test(conformance("describing the cluster or topics against a cluster that cannot be reached fails as a timed out request")):
    val topics = NonEmptySet.of(uniqueTopic("describe-unreachable"), uniqueTopic("describe-unreachable"))
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use: admin =>
        admin.describeCluster.attempt.timeout(150.seconds).map(unreachable) *> admin.describeTopics(topics).attempt.timeout(150.seconds)
          .map(unreachable)

  test(conformance("consumer group administration against a cluster that cannot be reached fails as a timed out request")):
    val group     = uniqueGroup("groups-unreachable")
    val partition = TopicPartition(uniqueTopic("groups-unreachable"), validPartition(0))
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use: admin =>
        List[(String, IO[Any])](
          "listing groups"        -> admin.listConsumerGroups,
          "describing groups"     -> admin.describeConsumerGroups(NonEmptySet.one(group)),
          "listing all offsets"   -> admin.listConsumerGroupOffsets(group),
          "listing named offsets" -> admin.listConsumerGroupOffsets(group, NonEmptySet.one(partition)),
          "altering offsets"      -> admin.alterConsumerGroupOffsets(group, NonEmptyMap.one(partition, OffsetAndMetadata(validOffset(0L)))),
          "deleting offsets"      -> admin.deleteConsumerGroupOffsets(group, NonEmptySet.one(partition)),
          "deleting groups"       -> admin.deleteConsumerGroups(NonEmptySet.one(group))
        ).parTraverse((call, run) => run.attempt.map(call -> _))
      .timeout(150.seconds).map: outcomes =>
        val wrong =
          outcomes.collect:
            case (call, Left(failure: KafkaException.BackendFailure))
                if failure.code.contains(ErrorCode.RequestTimedOut) && failure.retriable.contains(true) => None
            case (call, outcome) => Some(s"$call: $outcome")
        assertEquals(wrong.flatten, Nil, "each call should fail as a retriable timed out request")

  private def unreachable(outcome: Either[Throwable, ?]): Unit =
    outcome match
      case Left(failure: KafkaException.BackendFailure) =>
        assertEquals(failure.code, Some(ErrorCode.RequestTimedOut), failure.getMessage)
        assertEquals(failure.retriable, Some(true), failure.getMessage)
      case other => fail(s"a cluster that cannot be reached should fail the call, got $other")

  private def refusedWith(answer: Option[Either[Throwable, ?]], code: ErrorCode, clue: String): Unit =
    answer match
      case Some(Left(failure: KafkaException.BackendFailure)) => assertEquals(failure.code, Some(code), s"$clue: ${failure.getMessage}")
      case other                                              => fail(s"$clue should be refused as $code, got $other")

  private def unknownAnswer(answer: Option[Either[KafkaException, ?]], clue: String): Unit =
    answer match
      case Some(Left(failure: KafkaException.BackendFailure)) =>
        assertEquals(failure.code, Some(ErrorCode.UnknownTopicOrPartition), s"$clue: ${failure.getMessage}")
      case other => fail(s"$clue should answer as unknown, got $other")

  /** The offsets read show whether the stream ended without its failure or ignored the reset policy. */
  private def resetRequired(outcome: Either[Throwable, List[Long]]): Unit =
    outcome match
      case Left(failure: KafkaException.BackendFailure) =>
        assertEquals(failure.code, Some(ErrorCode.OffsetResetRequired), failure.getMessage)
        assertEquals(failure.retriable, Some(false), failure.getMessage)
      case Right(Nil)     => fail("the record stream should have failed as needing a reset, but it ended without records")
      case Right(offsets) => fail(s"the record stream should have failed as needing a reset, but it read offsets $offsets")
      case other          => fail(s"the record stream should have failed as needing a reset, got $other")

  private def produceValues(server: String, partition: TopicPartition, count: Int): IO[Unit] =
    val values =
      NonEmptyList.fromListUnsafe(List.tabulate(count)(index => record(partition.topic, Some("k"), Some(s"value-$index"), partition.partition)))
    produce(server, values).void

  /** Commits the offset after the first record of the topic, which is one. */
  private def commitFirst(settings: ConsumerSettings[IO, Option[String], Option[String]], topic: Topic): IO[Unit] =
    PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(1).evalMap(_.offset.commit).compile.drain)
      .timeout(60.seconds)

  private def beginningsOf(server: String, partitions: Set[TopicPartition]): IO[Map[TopicPartition, Offset]] =
    consumerSettings(server, uniqueGroup("beginnings")).flatMap: settings =>
      PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptyList.fromListUnsafe(partitions.toList).toNes))
        .use(_.beginningOffsets(partitions)).timeout(60.seconds)

  private def consumePartition(server: String, partition: TopicPartition, count: Int): IO[List[Long]] =
    consumerSettings(server, uniqueGroup("partition")).flatMap: settings =>
      PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(partition))).use(_.records.take(count.toLong).compile.toList)
        .map(_.map(_.record.offset.value)).timeout(60.seconds)

  /** Topic changes reach the cluster in their own time, so this waits for the partition count to settle. */
  private def described(admin: KafkaAdminClient[IO], topic: Topic, partitions: Int): IO[Int] =
    admin.describeTopics(NonEmptySet.one(topic)).attempt.map(_.toOption.flatMap(_(topic)).flatMap(_.toOption).map(_.partitions.length).getOrElse(0))
      .iterateUntil(_ == partitions).timeout(60.seconds)

  /** The offsets of the first `count` records a consumer hands out. */
  private def firstOffsets(settings: ConsumerSettings[IO, Option[String], Option[String]], selection: Selection, count: Int): IO[List[Long]] =
    PlatformKafkaClient().consumer(settings, selection).use(_.records.take(count.toLong).map(_.record.offset.value).compile.toList)
      .timeout(60.seconds)

  /** Topics created on demand get one partition, so a test that needs more makes its own. */
  private def createPartitioned(server: String, topic: Topic, partitions: Int, configuration: Map[String, String] = Map.empty): IO[Unit] =
    for
      client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
      newTopic <- NewTopic.from(topic, partitions = partitions, replicationFactor = 1, configuration).liftTo[IO]
      _ <- PlatformKafkaClient().admin(client).use(admin => admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, partitions))
    yield ()

  /** Deletion reaches the cluster in its own time, and describing a topic it no longer has is a failure. */
  private def absent(admin: KafkaAdminClient[IO], topic: Topic): IO[Boolean] =
    admin.describeTopics(NonEmptySet.one(topic)).map(_(topic).exists(_.left.exists(unknownTopic))).iterateUntil(identity).timeout(60.seconds)

  private def unknownTopic(failure: KafkaException): Boolean =
    failure match
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.UnknownTopicOrPartition)
      case _                                      => false

  private def assignmentOf(consumer: KafkaConsumer[IO, Option[String], Option[String]], size: Int): IO[Set[TopicPartition]] =
    consumer.assignmentChanges.filter(_.size == size).head.compile.lastOrError

  private def produce(
      server: String,
      records: NonEmptyList[ProducerRecord[Option[String], Option[String]]]
  ): IO[NonEmptyList[Produced[Option[String], Option[String]]]] =
    producerSettings(server).flatMap: settings =>
      PlatformKafkaClient().producer(settings).use(_.produceAndAwait(records)).timeout(60.seconds)

  private def consume(server: String, topic: Topic, count: Int): IO[List[CommittableConsumerRecord[IO, Option[String], Option[String]]]] =
    consumerSettings(server, uniqueGroup("conformance")).flatMap: settings =>
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(count.toLong).compile.toList)
        .timeout(60.seconds)

  private def consumeCommitted(server: String, topic: Topic, count: Int): IO[List[CommittableConsumerRecord[IO, Option[String], Option[String]]]] =
    committedConsumerSettings(server, uniqueGroup("conformance")).flatMap: settings =>
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(count.toLong).compile.toList)
        .timeout(60.seconds)

  private def committedConsumerSettings(server: String, group: ConsumerGroup): IO[ConsumerSettings[IO, Option[String], Option[String]]] =
    consumerSettings(server, group).map(_.withIsolationLevel(IsolationLevel.ReadCommitted))

  private def transactionalSettings(
      server: String,
      transactionalId: TransactionalId
  ): IO[TransactionalProducerSettings[IO, Option[String], Option[String]]] =
    ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
      TransactionalProducerSettings.from(client, transactionalId, optionalSerializer, optionalSerializer).liftTo[IO]

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

  private def uniqueTransactionalId(label: String): TransactionalId =
    TransactionalId.from(s"xkafka-conformance-$label-$backend-${System.nanoTime()}")
      .fold(error => fail(s"invalid test transactional id: ${error.message}"), identity)

  private def uniqueGroup(label: String): ConsumerGroup = validGroup(s"xkafka-conformance-$label-$backend-${System.nanoTime()}")

  private def validTopic(value: String): Topic = Topic.from(value).fold(error => fail(s"invalid test topic: ${error.message}"), identity)

  private def validGroup(value: String): ConsumerGroup =
    ConsumerGroup.from(value).fold(error => fail(s"invalid test consumer group: ${error.message}"), identity)

  private def validPartition(value: Int): Partition = Partition.from(value).fold(error => fail(s"invalid test partition: ${error.message}"), identity)

  private def validOffset(value: Long): Offset = Offset.from(value).fold(error => fail(s"invalid test offset: ${error.message}"), identity)

  private def validEpoch(value: Int): LeaderEpoch =
    LeaderEpoch.from(value).fold(error => fail(s"invalid test leader epoch: ${error.message}"), identity)
