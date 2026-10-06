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
import cats.effect.{Deferred, IO, Ref}
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

  test(conformance("a header without a value is preserved or rejected explicitly")):
    withBroker: server =>
      val topic   = uniqueTopic("null-header")
      val headers = Headers(Header("missing", None), Header("empty", Some(Chunk.empty)))
      val headed  = record(topic, Some("key"), Some("value"), validPartition(0), headers)

      produce(server, NonEmptyList.one(headed)).attempt.flatMap:
        case Left(_: KafkaException.Unsupported) => IO(assertEquals(backend, "js"))
        case Left(error)                         => IO(fail(s"unexpected error: $error"))
        case Right(_)                            => consume(server, topic, 1).map: consumed =>
            assert(backend != "js", "the JavaScript backend must reject the value its wrapper cannot represent")
            assertEquals(consumed.head.record.headers.values, headers.values)

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
        assertEquals(results.flatMap(_.records.toList.flatMap(_._1.value)), values, "the pipe should report batches in the order it took them")
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
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.committed(Set(TopicPartition(topic, partition))))
            .timeout(60.seconds)
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
                afterAbort <- consumer.committed(Set(topicPartition))
                _ <- producer.transactionally(transaction => transaction.produce(NonEmptyList.one(produced)) *> transaction.commitOffsets(batch))
                afterCommit <- consumer.committed(Set(topicPartition))
              yield (afterAbort.get(topicPartition).flatten, afterCommit.get(topicPartition).flatten)
          .timeout(90.seconds)
      yield
        val (afterAbort, afterCommit) = outcome
        assertEquals(afterAbort, None, "an aborted transaction should leave the group where it was")
        assertEquals(afterCommit.map(_.value), Some(1L), "a committed transaction should store the offset it recorded")

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
                          lost    <- first.committed(Set(revoked))
                          // The default protocol revokes every partition on a rebalance, so one the first consumer kept was read under an assignment it no longer holds either.
                          earlier <- writeWith(producer, before(kept.head)).attempt
                          // A record read after the rebalance, from the partition the first consumer kept, is still its to commit.
                          _       <- produce(server, NonEmptyList.one(record(input, Some("k"), Some(marker), kept.head.partition)))
                          current <- read.get.map(_.find(_.record.value.contains(marker)).map(_.offset)).iterateUntil(_.isDefined).map(_.get)
                          _       <- writeWith(producer, current)
                          moved   <- first.committed(Set(current.topicPartition))
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
                          lost <- first.committed(Set(revoked))
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
                      after   <- first.committed(Set(revoked))
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
                          moved <- first.committed(Set(kept.head))
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
        settings <- IO.fromOption(base.withProperty("linger.ms", "5000").toOption)(new AssertionError("linger.ms was rejected"))
        waiting  <- Deferred[IO, Either[Throwable, ProducerResult[Option[String], Option[String]]]]
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
        assertEquals(answers.get(first).map(_.leftMap(_.getMessage)), Some(Right(validOffset(3))))
        answers.get(other) match
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
        assertEquals(answers.map(_.get(partition).map(_.leftMap(_.getMessage))), List(3L, 3L, 3L, 4L).map(value => Some(Right(validOffset(value)))))
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
            PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptySet.one(partition))).use(_.committed(Set(partition)))
              .timeout(60.seconds)
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
        assertEquals(answers.get(known).map(_.leftMap(_.getMessage)), Some(Right(validOffset(1))))
        List(absentPartition, absentTopic).foreach(partition => unknownAnswer(answers.get(partition), partition.show))

  test(conformance("deleting records of topics the cluster has none of answers each partition as unknown")):
    withBroker: server =>
      val partitions =
        NonEmptyMap.of(
          TopicPartition(uniqueTopic("delete-none"), validPartition(0)) -> validOffset(1),
          TopicPartition(uniqueTopic("delete-none"), validPartition(0)) -> validOffset(1)
        )
      ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
        PlatformKafkaClient().admin(client).use(_.deleteRecords(partitions)).timeout(30.seconds).map: answers =>
          assertEquals(answers.keySet, partitions.keys.toSortedSet.toSet)
          answers.foreach((partition, answer) => unknownAnswer(Some(answer), partition.show))

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
        assertEquals(answers.get(partition).map(_.leftMap(_.getMessage)), Some(Right(validOffset(4))))
        assertEquals(consumed.map(_.record.value), List(Some("after")), "the committed transaction should have lost its deleted records")

  test(conformance("with offset reset set to fail, a group without a committed offset fails as needing a reset")):
    withBroker: server =>
      val topic = uniqueTopic("reset-fail-none")
      for
        _        <- produceValues(server, TopicPartition(topic, validPartition(0)), 1)
        settings <- consumerSettings(server, uniqueGroup("reset-fail-none")).map(_.withAutoOffsetReset(AutoOffsetReset.Fail))
        outcome  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(1).compile.drain).attempt
            .timeout(60.seconds)
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
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(1).compile.drain).attempt
              .timeout(60.seconds)
        earliest <-
          starting(AutoOffsetReset.Earliest).flatMap: settings =>
            PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic)))
              .use(_.records.take(1).map(_.record.offset.value).compile.toList).timeout(60.seconds)
      yield
        resetRequired(failed)
        assertEquals(earliest, List(2L), "Earliest should start at the first record still in the partition")

  test(conformance("a topic's configuration reports its own settings and the values it inherits", divergent = Set("js"))):
    withBroker: server =>
      val topic = uniqueTopic("configuration")
      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1, configuration = Map("retention.ms" -> "123456789")).liftTo[IO]
        answers  <-
          PlatformKafkaClient().admin(client).use: admin =>
            admin.createTopics(NonEmptySet.one(newTopic)) *> described(admin, topic, 1) *> admin.describeTopicConfigurations(NonEmptySet.one(topic))
          .timeout(90.seconds)
        configuration <- IO.fromEither(answers.get(topic).toRight(new AssertionError(s"no answer for the topic: $answers")).flatten)
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

  test(conformance("a topic the cluster does not have answers with its own failure", divergent = Set("js"))):
    withBroker: server =>
      val present = uniqueTopic("configuration-present")
      val missing = uniqueTopic("configuration-missing")
      for
        _       <- produceValues(server, TopicPartition(present, validPartition(0)), 1)
        client  <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        answers <- PlatformKafkaClient().admin(client).use(_.describeTopicConfigurations(NonEmptySet.of(present, missing))).timeout(90.seconds)
      yield
        assert(answers.get(present).exists(_.isRight), s"an existing topic should be described, got ${answers.get(present)}")
        answers.get(missing) match
          case Some(Left(failure: KafkaException.BackendFailure)) =>
            assertEquals(failure.code, Some(ErrorCode.UnknownTopicOrPartition), failure.getMessage)
          case other => fail(s"a topic the cluster does not have should fail, got $other")

  test(conformance("describing topic configurations against a cluster that cannot be reached fails as a timed out request", divergent = Set("js"))):
    val topics = NonEmptySet.of(uniqueTopic("configuration-unreachable"), uniqueTopic("configuration-unreachable"))
    ClientSettings.from(NonEmptyList.one("127.0.0.1:1")).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use(_.describeTopicConfigurations(topics)).attempt.timeout(150.seconds).map(unreachable)

  test(conformance("only the JavaScript backend refuses to describe topic configurations")):
    withBroker: server =>
      ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
        PlatformKafkaClient().admin(client).use(_.describeTopicConfigurations(NonEmptySet.one(uniqueTopic("configuration-support")))).attempt
          .timeout(90.seconds).map:
            case Left(_: KafkaException.Unsupported) => assertEquals(backend, "js")
            case Left(other)                         => fail(s"expected an answer or Unsupported, got $other")
            case Right(_)                            => assertNotEquals(backend, "js")

  private def unreachable(outcome: Either[Throwable, ?]): Unit =
    outcome match
      case Left(failure: KafkaException.BackendFailure) =>
        assertEquals(failure.code, Some(ErrorCode.RequestTimedOut), failure.getMessage)
        assertEquals(failure.retriable, Some(true), failure.getMessage)
      case other => fail(s"a cluster that cannot be reached should fail the call, got $other")

  private def unknownAnswer(answer: Option[Either[KafkaException, ?]], clue: String): Unit =
    answer match
      case Some(Left(failure: KafkaException.BackendFailure)) =>
        assertEquals(failure.code, Some(ErrorCode.UnknownTopicOrPartition), s"$clue: ${failure.getMessage}")
      case other => fail(s"$clue should answer as unknown, got $other")

  private def resetRequired(outcome: Either[Throwable, Unit]): Unit =
    outcome match
      case Left(failure: KafkaException.BackendFailure) =>
        assertEquals(failure.code, Some(ErrorCode.OffsetResetRequired), failure.getMessage)
        assertEquals(failure.retriable, Some(false), failure.getMessage)
      case other => fail(s"the record stream should have failed as needing a reset, got $other")

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
    admin.describeTopics(NonEmptySet.one(topic)).attempt.map(_.toOption.flatMap(_.get(topic)).map(_.size).getOrElse(0)).iterateUntil(_ == partitions)
      .timeout(60.seconds)

  /** Deletion reaches the cluster in its own time, and describing a topic it no longer has is a failure. */
  private def absent(admin: KafkaAdminClient[IO], topic: Topic): IO[Boolean] =
    admin.describeTopics(NonEmptySet.one(topic)).attempt.map(_.isLeft).iterateUntil(identity).timeout(60.seconds)

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
