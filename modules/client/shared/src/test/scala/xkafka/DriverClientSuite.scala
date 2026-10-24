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
import fs2.Chunk
import internal.driver.*
import munit.CatsEffectSuite

/** The client every backend shares, against a driver that stands in for the platform's Kafka client, so each ordering it promises can be forced. */
final class DriverClientSuite extends CatsEffectSuite:
  private val events    = Topic.from("events").toOption.get
  private val partition = LeaseKey("events", 0)

  private val client = ClientSettings.from(NonEmptyList.one("fake:9092")).toOption.get

  private val consumerSettings =
    ConsumerSettings.from(client, ConsumerGroup.from("fake").toOption.get, Deserializer.utf8[IO], Deserializer.utf8[IO]).toOption.get

  private val transactionalSettings =
    TransactionalProducerSettings.from(client, TransactionalId.from("fake").toOption.get, Serializer.utf8[IO], Serializer.utf8[IO]).toOption.get

  private val producerSettings = ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).toOption.get

  // The fake refuses to enqueue a record whose value is "refused", and fails to deliver one whose value is "lost". One whose value is "slow" is
  // delivered once flushed, and one whose value is "stuck" never is.
  private val refused = new KafkaException.BackendFailure("queue full", Some(ErrorCode.Other(-184)))
  private val lost    = new KafkaException.BackendFailure("too large", Some(ErrorCode.MessageTooLarge))

  test("attempting each record answers every record on its own, and producing fails with the first record that was not delivered"):
    val records = values("a", "lost", "b", "lost")
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(producerSettings).use: producer =>
        (producer.produceAttemptEachAndAwait(records), producer.produceAndAwait(records).attempt).tupled.timeout(10.seconds)
    .map: (attempted, produced) =>
      assertEquals(
        attempted.toList.map(each => each.record.value -> each.outcome.map(_.offset.map(_.value))),
        List("a" -> Right(Some(0L)), "lost" -> Left(lost), "b" -> Right(Some(2L)), "lost" -> Left(lost))
      )
      assertEquals(produced.leftMap(identity[Throwable]), Left(lost))

  test("a record the backend refuses fails with its refusal when attempting each, as do the ones after it, and fails producing as it is enqueued"):
    val records = values("a", "refused", "b")
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(producerSettings).use: producer =>
        (producer.produceAttemptEachAndAwait(records), producer.produce(records).attempt).tupled.timeout(10.seconds)
    .map: (attempted, enqueued) =>
      assertEquals(
        attempted.toList.map(each => each.record.value -> each.outcome.map(_.offset.map(_.value))),
        List("a" -> Right(Some(0L)), "refused" -> Left(refused), "b" -> Left(refused))
      )
      assertEquals(enqueued.void.leftMap(identity[Throwable]), Left(refused))

  test("attempting each record still fails as a whole where a record cannot be serialized"):
    val failing =
      Serializer.instance[IO, String]((_, _, value) => if value == "bad" then IO.raiseError(new IllegalArgumentException("bad")) else IO.pure(None))
    val settings = ProducerSettings.from(client, Serializer.utf8[IO], failing).toOption.get
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(settings).use(_.produceAttemptEach(values("a", "bad")).attempt).map: outcome =>
        assertEquals(outcome.void.leftMap(_.getMessage), Left("bad"))

  test("flushing sends the records enqueued before it and waits for them, and not for the records enqueued while it waits"):
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(producerSettings).use: producer =>
        for
          before   <- producer.produce(values("slow"))
          unsent   <- before.timeout(100.millis).attempt
          flushing <- producer.flush.start
          // The first round reaches the driver once the flush has taken its count, so the next record is enqueued while it waits.
          _       <- fake.until(_.contains("flush"))
          during  <- producer.produce(values("stuck"))
          _       <- flushing.joinWithNever.timeout(10.seconds)
          sent    <- before.timeout(10.seconds)
          waiting <- during.timeout(100.millis).attempt
        yield
          assert(unsent.isLeft, unsent)
          assertEquals(sent.toList.map(_.record.value), List("slow"))
          assert(waiting.isLeft, waiting)

  test("flushing completes where a record fails, which only that record's result reports"):
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(producerSettings).use: producer =>
        for
          produced <- producer.produce(values("slow", "lost"))
          flushed  <- producer.flush.timeout(10.seconds).attempt
          outcome  <- produced.timeout(10.seconds).attempt
        yield
          assertEquals(flushed, Right(()))
          assertEquals(outcome.void.leftMap(identity[Throwable]), Left(lost))

  test("flushing with nothing enqueued returns at once, and a flush that waits can be cancelled, leaving its records enqueued"):
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(producerSettings).use: producer =>
        for
          _         <- producer.flush.timeout(10.seconds)
          idle      <- fake.calls
          stuck     <- producer.produce(values("stuck"))
          cancelled <- producer.flush.timeout(200.millis).attempt
          held      <- stuck.timeout(100.millis).attempt
          rounds    <- fake.calls.map(_.count(_ == "flush"))
          _         <- IO.sleep(100.millis)
          later     <- fake.calls.map(_.count(_ == "flush"))
        yield
          assert(!idle.contains("flush"), idle)
          assert(cancelled.isLeft, cancelled)
          assert(held.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]), held)
          assert(rounds > 0, rounds)
          assertEquals(later, rounds)

  private def values(first: String, rest: String*): NonEmptyList[ProducerRecord[String, String]] =
    NonEmptyList(first, rest.toList).map(ProducerRecord(events, "k", _))

  test("an offset read under a revoked assignment cannot be recorded in a transaction, and one read after reassignment can"):
    FakeDriver().flatMap: fake =>
      transactional(fake, consumerSettings): (producer, consumer) =>
        for
          _            <- fake.assign *> fake.until(_.count(_ == "assign") == 1)
          earlier      <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _            <- fake.revoke *> fake.until(_.contains("unassign"))
          revoked      <- recorded(producer, earlier)
          _            <- fake.assign *> fake.until(_.count(_ == "assign") == 2)
          later        <- fake.deliver(1L) *> consumer.records.take(1).compile.lastOrError
          stillRevoked <- recorded(producer, earlier)
          current      <- recorded(producer, later)
          sent         <- fake.count("send")
        yield
          assert(illegalGeneration(revoked), s"an offset from a revoked assignment should be rejected, got $revoked")
          assert(illegalGeneration(stillRevoked), s"reassigning the partition should not revive an earlier offset, got $stillRevoked")
          assertEquals(current, Right(()))
          assertEquals(sent, 1, "only the offset read under the current assignment should reach the client")

  test("with assignment fencing off, an offset read under a revoked assignment is still recorded, as it was before fencing"):
    FakeDriver().flatMap: fake =>
      transactional(fake, consumerSettings.withoutAssignmentFencing): (producer, consumer) =>
        for
          _       <- fake.assign *> fake.until(_.contains("assign"))
          earlier <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _       <- fake.revoke *> fake.until(_.contains("unassign"))
          stale   <- recorded(producer, earlier)
          sent    <- fake.count("send")
        yield
          assertEquals(stale, Right(()))
          assertEquals(sent, 1)

  test("a plain commit of an offset read under a revoked assignment is refused before it reaches the client"):
    FakeDriver().flatMap: fake =>
      consuming(fake): consumer =>
        for
          _         <- fake.assign *> fake.until(_.count(_ == "assign") == 1)
          earlier   <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _         <- fake.revoke *> fake.until(_.contains("unassign"))
          revoked   <- earlier.offset.commit.attempt
          _         <- fake.assign *> fake.until(_.count(_ == "assign") == 2)
          later     <- fake.deliver(1L) *> consumer.records.take(1).compile.lastOrError
          current   <- later.offset.commit.attempt
          committed <- fake.count("commit")
        yield
          assert(illegalGeneration(revoked), s"a plain commit from a revoked assignment should be refused, got $revoked")
          assertEquals(current, Right(()))
          assertEquals(committed, 1, "only the offset read under the current assignment should reach the client")

  test("a revocation waits for a plain commit the client has not reported yet"):
    FakeDriver(holdCommits = true).flatMap: fake =>
      consuming(fake): consumer =>
        for
          _          <- fake.assign *> fake.until(_.contains("assign"))
          record     <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          committing <- record.offset.commit.attempt.start
          _          <- fake.until(_.contains("commit"))
          _          <- fake.revoke *> IO.sleep(200.millis)
          early      <- fake.calls.map(_.contains("unassign"))
          _          <- fake.release
          reported   <- committing.joinWithNever
          _          <- fake.until(_.contains("unassign"))
        yield
          assert(!early, "the partition was unassigned while its commit had not been reported")
          assertEquals(reported, Right(()))

  test("a revocation waits for a transaction recording one of its offsets before it unassigns"):
    FakeDriver(holdOffsets = true).flatMap: fake =>
      transactional(fake, consumerSettings): (producer, consumer) =>
        for
          _         <- fake.assign *> fake.until(_.contains("assign"))
          held      <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          recording <- recorded(producer, held).start
          _         <- fake.until(_.contains("send"))
          _         <- fake.revoke *> IO.sleep(200.millis)
          early     <- fake.calls.map(_.contains("unassign"))
          _         <- fake.release
          outcome   <- recording.joinWithNever
          _         <- fake.until(_.contains("unassign"))
        yield
          assert(!early, "the partition was unassigned while a transaction was still recording its offset")
          assertEquals(outcome, Right(()))

  test("a revocation during a poll leaves the records it returns unable to be recorded in a transaction"):
    FakeDriver().flatMap: fake =>
      transactional(fake, consumerSettings): (producer, consumer) =>
        for
          _       <- fake.assign *> fake.until(_.contains("assign"))
          spanned <-
          fake.duringNextPoll(fake.revoke *> fake.assign *> IO.sleep(100.millis) *> fake.deliver(0L)) *> consumer.records.take(1).compile.lastOrError
          _      <- fake.until(_.count(_ == "assign") == 2)
          after  <- fake.deliver(1L) *> consumer.records.take(1).compile.lastOrError
          first  <- recorded(producer, spanned)
          second <- recorded(producer, after)
        yield
          assert(illegalGeneration(first), s"a record from a poll the revocation interrupted should be rejected, got $first")
          assertEquals(second, Right(()))

  test("a group handle from one client is recorded by a transactional producer from another"):
    FakeDriver().flatMap: fake =>
      (new DriverClient[IO](fake).transactionalProducer(transactionalSettings), new DriverClient[IO](fake).consumer(consumerSettings, topics)).tupled
        .use: (producer, consumer) =>
          for
            _       <- fake.assign *> fake.until(_.contains("assign"))
            record  <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
            outcome <- recorded(producer, record)
          yield assertEquals(outcome, Right(()))
        .timeout(10.seconds)

  test("a rebalance taken from the driver is applied even when the consumer is released while taking it"):
    for
      fake  <- FakeDriver()
      taken <- Deferred[IO, Unit]
      _     <-
        new DriverClient[IO](fake).consumer(consumerSettings, topics).use: _ =>
          // The fake pauses once it has taken a rebalance, and the consumer is released during that pause.
          fake.assign *> fake.until(_.contains("assign")) *> fake.pauseNextTake(taken) *> fake.revoke *> taken.get
        .timeout(10.seconds)
      calls <- fake.calls
    yield assert(calls.contains("unassign"), s"the revocation was taken from the driver but never applied: $calls")

  test("with rebalance-safe commits, a revocation waits for the records handed out to be committed, with their partition paused meanwhile"):
    FakeDriver().flatMap: fake =>
      consuming(fake, safe(10.seconds)): consumer =>
        for
          _      <- fake.assign *> fake.until(_.contains("assign"))
          record <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _      <- fake.revoke *> fake.until(_.contains("pause")) *> IO.sleep(200.millis)
          early  <- fake.calls.map(_.contains("unassign"))
          _      <- record.offset.commit *> fake.until(_.contains("unassign"))
          calls  <- fake.calls
        yield
          assert(!early, "the partition was unassigned while a record handed out of it was uncommitted")
          assertEquals(handOverCalls(calls), Vector("pause", "commit", "resume", "unassign"))

  test("with rebalance-safe commits, a record of a partition being revoked that was not handed out yet is dropped"):
    FakeDriver().flatMap: fake =>
      consuming(fake, safe(10.seconds)): consumer =>
        for
          _       <- fake.assign *> fake.until(_.contains("assign"))
          record  <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _       <- fake.deliver(1L) *> fake.drained
          _       <- fake.revoke *> fake.until(_.contains("pause"))
          dropped <- consumer.records.take(1).compile.lastOrError.timeout(300.millis).attempt
          _       <- record.offset.commit *> fake.until(_.contains("unassign"))
        yield assert(dropped.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]), s"a record was handed out: $dropped")

  test("with rebalance-safe commits, a record read before a revocation is not handed out once the group assigns its partition again"):
    FakeDriver().flatMap: fake =>
      consuming(fake, safe(10.seconds)): consumer =>
        for
          _      <- fake.assign *> fake.until(_.contains("assign"))
          record <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _      <- fake.deliver(1L) *> fake.drained
          _      <- fake.revoke *> fake.until(_.contains("pause")) *> record.offset.commit *> fake.until(_.contains("unassign"))
          // The partition starts again at its committed offset, so the new assignment reads the record again.
          _     <- fake.assign *> fake.until(_.count(_ == "assign") == 2)
          after <- fake.deliver(1L) *> fake.deliver(2L) *> consumer.records.take(2).compile.toList
        yield assertEquals(after.map(_.record.offset.value), List(1L, 2L))

  test("with rebalance-safe commits, a revocation goes ahead at its limit and the consumer carries on"):
    FakeDriver().flatMap: fake =>
      consuming(fake, safe(300.millis)): consumer =>
        for
          _     <- fake.assign *> fake.until(_.contains("assign"))
          _     <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _     <- fake.revoke *> fake.until(_.contains("unassign"))
          _     <- fake.assign *> fake.until(_.count(_ == "assign") == 2)
          after <- fake.deliver(1L) *> consumer.records.take(1).compile.lastOrError.attempt
          calls <- fake.calls
        yield
          assertEquals(after.map(_.record.offset.value), Right(1L))
          assertEquals(handOverCalls(calls), Vector("pause", "resume", "unassign"))

  test("with rebalance-safe commits set to fail, a revocation at its limit fails the record stream"):
    FakeDriver().flatMap: fake =>
      consuming(fake, safe(300.millis, RebalanceSafeCommits.AtLimit.Fail)): consumer =>
        for
          _       <- fake.assign *> fake.until(_.contains("assign"))
          _       <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _       <- fake.revoke *> fake.until(_.contains("unassign"))
          outcome <- consumer.records.compile.drain.attempt
        yield assertEquals(uncommittedRevocation(outcome), Some(Set(TopicPartition(events, Partition.from(0).toOption.get))))

  test("with rebalance-safe commits, a lost partition is let go at once"):
    FakeDriver().flatMap: fake =>
      consuming(fake, safe(5.seconds, RebalanceSafeCommits.AtLimit.Fail)): consumer =>
        for
          _     <- fake.assign *> fake.until(_.contains("assign"))
          _     <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          let   <- (fake.lose *> fake.until(_.contains("unassign"))).timed
          _     <- fake.assign *> fake.until(_.count(_ == "assign") == 2)
          after <- fake.deliver(1L) *> consumer.records.take(1).compile.lastOrError.attempt
          calls <- fake.calls
        yield
          assert(let._1 < 1.second, s"the lost partition was let go after ${let._1}")
          assertEquals(after.map(_.record.offset.value), Right(1L))
          assertEquals(handOverCalls(calls), Vector("unassign"))

  test("with rebalance-safe commits, a revocation waits for a transaction that records the offsets handed out to commit"):
    FakeDriver().flatMap: fake =>
      transactional(fake, safe(10.seconds)): (producer, consumer) =>
        for
          _       <- fake.assign *> fake.until(_.contains("assign"))
          record  <- fake.deliver(0L) *> consumer.records.take(1).compile.lastOrError
          _       <- fake.revoke *> fake.until(_.contains("pause"))
          aborted <-
            producer.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(record.offset)) *> IO.raiseError(new Exception)).attempt
          _       <- IO.sleep(200.millis)
          early   <- fake.calls.map(_.contains("unassign"))
          outcome <- recorded(producer, record)
          _       <- fake.until(_.contains("unassign"))
        yield
          assert(aborted.isLeft)
          assert(!early, "an aborted transaction let the revocation go ahead")
          assertEquals(outcome, Right(()))

  test("starting offsets start each assigned partition where the application says, before it is assigned"):
    FakeDriver().flatMap: fake =>
      Ref.of[IO, List[Set[TopicPartition]]](Nil).flatMap: asked =>
        val settings = starting(1.second)(partitions => asked.update(_ :+ partitions).as(partitions.map(_ -> startAt(7L, Some(3))).toMap))
        consuming(fake, settings)(_ => fake.assign *> fake.until(_.contains("assign")) *> (fake.calls, asked.get).tupled).map: (calls, questions) =>
          assertEquals(questions, List(Set(eventsZero)))
          assertEquals(calls.filter(call => call.startsWith("start") || call == "assign"), Vector("start events-0 at 7 in epoch 3", "assign"))

  test("a partition the starting offsets answer nothing for starts where it would, and one they were not asked for is left alone"):
    FakeDriver().flatMap: fake =>
      val elsewhere = TopicPartition(Topic.from("elsewhere").toOption.get, Partition.from(0).toOption.get)
      consuming(fake, starting(1.second)(_ => IO.pure(Map(elsewhere -> startAt(7L))))): _ =>
        fake.assign *> fake.until(_.contains("assign")) *> fake.calls.map(calls => assertEquals(calls.filter(_.startsWith("start")), Vector.empty))

  test("starting offsets that fail fail the record stream, and the partition is still assigned"):
    FakeDriver().flatMap: fake =>
      val failure = new Exception("no offsets")
      consuming(fake, starting(1.second)(_ => IO.raiseError(failure))): consumer =>
        fake.assign *> fake.until(_.contains("assign")) *> consumer.records.compile.drain.attempt.map(outcome => assertEquals(outcome, Left(failure)))

  test("starting offsets that pass their limit fail the record stream as timed out"):
    FakeDriver().flatMap: fake =>
      consuming(fake, starting(200.millis)(_ => IO.never)): consumer =>
        fake.assign *> fake.until(_.contains("assign")) *> consumer.records.compile.drain.attempt.map:
          case Left(timedOut: KafkaException.StartingOffsetsTimedOut) =>
            assertEquals(timedOut.topicPartitions, Set(eventsZero))
            assertEquals(timedOut.limit, 200.millis)
          case other => fail(s"expected the starting offsets to time out, got $other")

  test("partitions named directly start where the starting offsets say, and their failure fails creating the consumer"):
    FakeDriver().flatMap: fake =>
      val named   = Selection.Partitions(NonEmptySet.one(eventsZero))
      val failure = new Exception("no offsets")
      val client  = new DriverClient[IO](fake)
      for
        _      <- client.consumer(starting(1.second)(partitions => IO.pure(partitions.map(_ -> startAt(4L)).toMap)), named).use_
        calls  <- fake.calls
        failed <- client.consumer(starting(1.second)(_ => IO.raiseError(failure)), named).use_.attempt
      yield
        assertEquals(calls.filter(call => call.startsWith("start") || call == "assign"), Vector("start events-0 at 4 in epoch -1", "assign"))
        assertEquals(failed, Left(failure))

  test(
    "a group's offsets leave out partitions without one, and named partitions answer None for them, whether the backend says so or leaves them out"
  ):
    val committed = OffsetAndMetadata(Offset.from(5L).toOption.get, None, "kept")
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).admin(client).use: admin =>
        (admin.listConsumerGroupOffsets(group), admin.listConsumerGroupOffsets(group, NonEmptySet.of(eventsAt(0), eventsAt(1), eventsAt(2)))).tupled
    .map: (all, named) =>
      assertEquals(all, Map(eventsAt(0) -> committed))
      assertEquals(named, NonEmptyMap.of(eventsAt(0) -> Some(committed), eventsAt(1) -> None, eventsAt(2) -> None))

  test("an administration call the backend answers for a different number of items than asked fails as an invalid response"):
    val offset = OffsetAndMetadata(Offset.from(1L).toOption.get)
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).admin(client).use: admin =>
        (
          admin.alterConsumerGroupOffsets(group, NonEmptyMap.one(eventsAt(0), offset)),
          admin.alterConsumerGroupOffsets(group, NonEmptyMap.of(eventsAt(0) -> offset, eventsAt(1) -> offset)).attempt
        ).tupled
    .map: (one, two) =>
      assertEquals(one, NonEmptyMap.one(eventsAt(0), Right(())))
      assert(two.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]), s"a short answer should be refused, got $two")

  test("a topic's partitions are never empty: one the backend knows none for is unknown, and a listing with such a topic is an invalid response"):
    FakeDriver().flatMap: fake =>
      consuming(fake): consumer =>
        (consumer.partitionsFor(events), consumer.partitionsFor(Topic.from("missing").toOption.get).attempt, consumer.listTopics.attempt).tupled
    .map: (known, missing, listed) =>
      assertEquals(known, NonEmptySet.one(Partition.from(0).toOption.get))
      assert(
        missing.left.exists:
          case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.UnknownTopicOrPartition)
          case _                                      => false
        ,
        s"a topic without partitions should be unknown, got $missing"
      )
      assert(
        listed.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]),
        s"a listed topic without partitions should be refused, got $listed"
      )

  test("a producer asked about a topic the broker is still creating waits for it"):
    FakeDriver().flatMap: fake =>
      new DriverClient[IO](fake).producer(producerSettings).use(_.partitionsFor(Topic.from("creating").toOption.get)).timeout(10.seconds)
        .product(fake.count("metadata"))
    .map: (partitions, lookups) =>
      assertEquals(partitions, NonEmptySet.one(Partition.from(0).toOption.get))
      assertEquals(lookups, 3)

  private val group = ConsumerGroup.from("fake").toOption.get

  private val eventsZero = TopicPartition(events, Partition.from(0).toOption.get)

  private def eventsAt(partition: Int): TopicPartition = TopicPartition(events, Partition.from(partition).toOption.get)

  private def startAt(offset: Long, epoch: Option[Int] = None): StartingOffset =
    StartingOffset(Offset.from(offset).toOption.get, epoch.map(LeaderEpoch.from(_).toOption.get))

  private def starting(limit: FiniteDuration)(offsets: Set[TopicPartition] => IO[Map[TopicPartition, StartingOffset]]) =
    consumerSettings.withStartingOffsets(limit)(offsets).toOption.get

  private val topics = Selection.Topics(NonEmptySet.one(events))

  private def consuming[A](fake: FakeDriver, settings: ConsumerSettings[IO, String, String] = consumerSettings)(
      use: KafkaConsumer[IO, String, String] => IO[A]
  ): IO[A] = new DriverClient[IO](fake).consumer(settings, topics).use(use).timeout(10.seconds)

  private def safe(limit: FiniteDuration, atLimit: RebalanceSafeCommits.AtLimit = RebalanceSafeCommits.AtLimit.Proceed) =
    consumerSettings.withRebalanceSafeCommits(limit, atLimit).toOption.get

  private def handOverCalls(calls: Vector[String]): Vector[String] = calls.filter(Set("pause", "commit", "resume", "unassign"))

  private def uncommittedRevocation(outcome: Either[Throwable, ?]): Option[Set[TopicPartition]] =
    outcome.left.toOption.collect { case failure: KafkaException.UncommittedRevocation => failure.topicPartitions }

  private def transactional[A](fake: FakeDriver, settings: ConsumerSettings[IO, String, String])(
      use: (KafkaTransactionalProducer[IO, String, String], KafkaConsumer[IO, String, String]) => IO[A]
  ): IO[A] =
    val client = new DriverClient[IO](fake)
    (client.transactionalProducer(transactionalSettings), client.consumer(settings, topics)).tupled.use(use.tupled).timeout(10.seconds)

  private def recorded(producer: KafkaTransactionalProducer[IO, String, String], value: CommittableConsumerRecord[IO, String, String]) =
    producer.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(value.offset))).attempt

  private def illegalGeneration(outcome: Either[Throwable, Unit]): Boolean =
    outcome.left.exists:
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.IllegalGeneration)
      case _                                      => false

  /** A driver for one eager-protocol consumer and one transactional producer, recording each call that reaches it. */
  private final case class FakeGroupMetadata(consumer: String) extends GroupHandle

  private final class FakeDriver(
      batches: Ref[IO, Map[Int, Vector[EncodedRecord]]],
      flushed: Ref[IO, Set[Int]],
      log: Ref[IO, Vector[String]],
      queued: Ref[IO, List[PolledRecord]],
      pendingRebalances: Ref[IO, List[Rebalance[Int]]],
      inPoll: Ref[IO, Option[IO[Unit]]],
      holds: Ref[IO, List[Deferred[IO, Unit]]],
      pauseTake: Ref[IO, Option[Deferred[IO, Unit]]],
      holdCommits: Boolean,
      holdOffsets: Boolean
  ) extends KafkaDriver[IO]:
    type Client        = String
    type Batch         = Int
    type Event         = Int
    type GroupMetadata = FakeGroupMetadata

    val calls: IO[Vector[String]] = log.get

    def count(call: String): IO[Int] = calls.map(_.count(_ == call))

    def until(condition: Vector[String] => Boolean): IO[Unit] =
      calls.map(condition).flatMap(done => if done then IO.unit else IO.sleep(5.millis) *> until(condition))

    def assign: IO[Unit] = pendingRebalances.update(_ :+ Rebalance(1, 1, List(partition)))

    def revoke: IO[Unit] = pendingRebalances.update(_ :+ Rebalance(0, 0, List(partition)))

    def lose: IO[Unit] = pendingRebalances.update(_ :+ Rebalance(2, 2, List(partition)))

    /** Completes once every delivered record has been polled. */
    def drained: IO[Unit] = queued.get.map(_.isEmpty).flatMap(done => if done then IO.unit else IO.sleep(5.millis) *> drained)

    def deliver(offset: Long): IO[Unit] =
      queued.update(
        _ :+ PolledRecord(
          "events",
          0,
          offset,
          Some(0L),
          1,
          0,
          Some(Chunk.array("k".getBytes)),
          Some(Chunk.array("v".getBytes)),
          Headers.fromVector(Vector.empty)
        )
      )

    /** Runs `action` inside the next poll, before that poll returns. */
    def duringNextPoll(action: IO[Unit]): IO[Unit] = inPoll.set(Some(action))

    /** Lets every held commit or offset recording finish. */
    def release: IO[Unit] = holds.getAndSet(Nil).flatMap(_.traverse_(_.complete(())))

    private def record(call: String): IO[Unit] = log.update(_ :+ call)

    private def held(hold: Boolean): IO[Unit] = if hold then Deferred[IO, Unit].flatTap(gate => holds.update(_ :+ gate)).flatMap(_.get) else IO.unit

    private def unused[A](call: String): IO[A] = IO.raiseError(new UnsupportedOperationException(s"$call is not part of the fake"))

    override val deliveriesShareQueue: Boolean = true

    override def producer(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): IO[Client] = IO.pure("producer")

    override def destroyProducer(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def consumer(
        bootstrapServers: String,
        clientId: Option[String],
        groupId: String,
        autoOffsetReset: String,
        properties: Map[String, String]
    ) = IO.pure("consumer")

    override def destroyConsumer(consumer: Client): IO[Unit] = IO.unit

    override def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): IO[Client] = IO.pure("admin")

    override def destroyAdmin(admin: Client): IO[Unit] = IO.unit

    override def batch(capacity: Int): IO[Batch] = batches.modify(held => (held.updated(held.size, Vector.empty), held.size))

    override def freeBatch(batch: Batch): IO[Unit] = IO.unit

    override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): IO[Unit] =
      records.traverse_ : record =>
        if valueOf(record) == "refused" then IO.raiseError(refused) else batches.update(held => held.updated(batch, held(batch) :+ record))

    override def serveDeliveries(producer: Client, timeoutMillis: Int): IO[Unit] = IO.sleep(5.millis)

    /** Takes a moment, then sends every batch enqueued so far. */
    override def flush(producer: Client, timeoutMillis: Int): IO[Unit] =
      log.update(_ :+ "flush") *> IO.sleep(20.millis) *> batches.get.flatMap(held => flushed.update(_ ++ held.keySet))

    override def awaitDeliveries(batch: Batch, timeoutMillis: Int): IO[Int] = unused("awaitDeliveries")

    override def pending(batch: Batch): IO[Int] =
      (batches.get, flushed.get)
        .mapN((held, sent) => held(batch).count(record => valueOf(record) == "stuck" || valueOf(record) == "slow" && !sent(batch)))

    override def deliveries(producer: Client, batch: Batch): IO[Vector[Either[KafkaException, Delivery]]] =
      batches.get
        .map(_(batch).zipWithIndex.map((record, index) => if valueOf(record) == "lost" then Left(lost) else Right(Delivery(0, index.toLong, -1L))))

    private def valueOf(record: EncodedRecord): String = record.value.fold("")(value => new String(value.toArray))

    override def initTransactions(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def beginTransaction(producer: Client): IO[Unit] = IO.unit

    override def commitTransaction(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def abortTransaction(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def sendOffsets(
        producer: Client,
        metadata: GroupMetadata,
        offsets: Vector[(TopicPartition, OffsetAndMetadata)],
        timeoutMillis: Int
    ): IO[Unit] = record("send") *> held(holdOffsets)

    override def groupMetadata(consumer: Client): IO[GroupMetadata] = IO.pure(FakeGroupMetadata(consumer))

    override def destroyGroupMetadata(metadata: GroupMetadata): IO[Unit] = IO.unit

    override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
      handle match
        case metadata: FakeGroupMetadata => Some(metadata)
        case _                           => None

    override def assign(consumer: Client, partitions: Vector[TopicPartition], starts: Vector[StartAt]): IO[Unit] =
      starts.traverse_(started) *> record("assign")

    override def subscribe(consumer: Client, topics: NonEmptyList[String]): IO[Unit] = IO.unit

    override def poll(consumer: Client, timeoutMillis: Int): IO[List[PolledRecord]] =
      inPoll.getAndSet(None).flatMap(_.sequence_) *> queued.modify {
        case head :: tail => (tail, List(head))
        case Nil          => (Nil, Nil)
      }.flatMap(polled => if polled.isEmpty then IO.sleep(5.millis).as(Nil) else IO.pure(polled))

    override def takeRebalances(consumer: Client): IO[List[Rebalance[Event]]] =
      pendingRebalances.getAndSet(Nil).flatTap: rebalances =>
        if rebalances.isEmpty then IO.unit else pauseTake.getAndSet(None).flatMap(_.traverse_(signal => signal.complete(()) *> IO.sleep(200.millis)))

    /** Makes the next take that finds a rebalance signal `taken`, then pause before it returns. */
    def pauseNextTake(taken: Deferred[IO, Unit]): IO[Unit] = pauseTake.set(Some(taken))

    override def applyRebalance(consumer: Client, event: Event, starts: Vector[StartAt]): IO[Unit] =
      starts.traverse_(started) *> record(if event == 1 then "assign" else "unassign")

    private def started(start: StartAt): IO[Unit] =
      record(s"start ${start.partition.topic.value}-${start.partition.partition.value} at ${start.offset} in epoch ${start.leaderEpoch}")

    override def cooperative(consumer: Client): IO[Boolean] = IO.pure(false)

    override def assignment(consumer: Client): IO[Vector[(String, Int)]] =
      calls.map(log => if log.lastIndexOf("assign") > log.lastIndexOf("unassign") then Vector("events" -> 0) else Vector.empty)

    override def commit(consumer: Client, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): IO[Unit] =
      record("commit") *> held(holdCommits)

    override def pause(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit] = record("pause")

    override def resume(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit] = record("resume")

    override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): IO[Vector[StoredOffset]] = unused("committed")

    override def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): IO[(Long, Long)] = unused("watermarks")

    override def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): IO[Vector[Long]] =
      unused("offsetsForTimes")

    override def positions(consumer: Client, partitions: Vector[TopicPartition]): IO[Vector[Long]] = unused("positions")

    override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): IO[Unit] = unused("seek")

    /** Knows `events` with one partition, and lists `bare` without any, which no Kafka cluster answers. `creating` has its partition from the third
      * lookup on, as a topic the broker creates when asked about does.
      */
    override def metadata(client: Client, topic: Option[String], timeoutMillis: Int): IO[Vector[(String, Vector[Int])]] =
      record("metadata") *> count("metadata").map: lookups =>
        topic.fold(Vector("events" -> Vector(0), "bare" -> Vector.empty)): name =>
          Vector(name -> (if name == "events" || (name == "creating" && lookups >= 3) then Vector(0) else Vector.empty))

    override def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): IO[Unit] = unused("createTopics")

    override def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): IO[Unit] = unused("deleteTopics")

    override def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): IO[Unit] = unused("createPartitions")

    override def deleteRecords(
        admin: Client,
        cuts: Vector[(TopicPartition, Offset)],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Offset]]] = unused("deleteRecords")

    override def describeCluster(admin: Client, timeoutMillis: Int): IO[ClusterDescription] = unused("describeCluster")

    override def describeTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): IO[Vector[Either[KafkaException, TopicDescription]]] =
      unused("describeTopics")

    override def describeTopicConfigs(
        admin: Client,
        topics: Vector[Topic],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, TopicConfiguration]]] = unused("describeTopicConfigs")

    override def listGroups(admin: Client, timeoutMillis: Int): IO[Vector[(ConsumerGroup, ConsumerGroupState)]] = unused("listGroups")

    override def describeGroups(
        admin: Client,
        groups: Vector[ConsumerGroup],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, ConsumerGroupDescription]]] = unused("describeGroups")

    override def groupOffsets(
        admin: Client,
        group: ConsumerGroup,
        partitions: Option[Vector[TopicPartition]],
        timeoutMillis: Int
    ): IO[Vector[(TopicPartition, StoredOffset)]] =
      // Partition 0 holds offset 5, partition 1 has none, and any other partition is left out of the answer.
      val answers = Vector(eventsAt(0) -> StoredOffset(5L, -1, "kept"), eventsAt(1) -> StoredOffset(-1L, -1, ""))
      IO.pure(partitions.fold(answers)(requested => answers.filter((partition, _) => requested.contains(partition))))

    override def alterGroupOffsets(
        admin: Client,
        group: ConsumerGroup,
        offsets: Vector[(TopicPartition, OffsetAndMetadata)],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Unit]]] = IO.pure(Vector(Right(()))) // one answer, however many offsets it is given

    override def deleteGroupOffsets(
        admin: Client,
        group: ConsumerGroup,
        partitions: Vector[TopicPartition],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Unit]]] = unused("deleteGroupOffsets")

    override def deleteGroups(admin: Client, groups: Vector[ConsumerGroup], timeoutMillis: Int): IO[Vector[Either[KafkaException, Unit]]] =
      unused("deleteGroups")

  private object FakeDriver:
    def apply(holdCommits: Boolean = false, holdOffsets: Boolean = false): IO[FakeDriver] =
      (
        Ref.of[IO, Map[Int, Vector[EncodedRecord]]](Map.empty),
        Ref.of[IO, Set[Int]](Set.empty),
        Ref.of[IO, Vector[String]](Vector.empty),
        Ref.of[IO, List[PolledRecord]](Nil),
        Ref.of[IO, List[Rebalance[Int]]](Nil),
        Ref.of[IO, Option[IO[Unit]]](None),
        Ref.of[IO, List[Deferred[IO, Unit]]](Nil),
        Ref.of[IO, Option[Deferred[IO, Unit]]](None)
      ).mapN((batches, flushed, log, queued, rebalances, inPoll, holds, pauseTake) =>
        new FakeDriver(batches, flushed, log, queued, rebalances, inPoll, holds, pauseTake, holdCommits, holdOffsets)
      )
