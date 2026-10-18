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

  private val topics = Selection.Topics(NonEmptySet.one(events))

  private def consuming[A](fake: FakeDriver)(use: KafkaConsumer[IO, String, String] => IO[A]): IO[A] =
    new DriverClient[IO](fake).consumer(consumerSettings, topics).use(use).timeout(10.seconds)

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

    def deliver(offset: Long): IO[Unit] =
      queued.update(
        _ :+ PolledRecord(
          "events",
          0,
          offset,
          Some(0L),
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

    override def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): IO[Client] = unused("admin")

    override def destroyAdmin(admin: Client): IO[Unit] = IO.unit

    override def batch(capacity: Int): IO[Batch] = unused("batch")

    override def freeBatch(batch: Batch): IO[Unit] = IO.unit

    override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): IO[Unit] = unused("enqueue")

    override def serveDeliveries(producer: Client, batch: Batch, timeoutMillis: Int): IO[Int] = unused("serveDeliveries")

    override def pending(batch: Batch): IO[Int] = IO.pure(0)

    override def deliveries(producer: Client, batch: Batch): IO[Vector[Delivery]] = unused("deliveries")

    override def initTransactions(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def beginTransaction(producer: Client): IO[Unit] = IO.unit

    override def commitTransaction(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def abortTransaction(producer: Client, timeoutMillis: Int): IO[Unit] = IO.unit

    override def sendOffsets(producer: Client, metadata: GroupMetadata, offsets: Vector[(TopicPartition, Offset)], timeoutMillis: Int): IO[Unit] =
      record("send") *> held(holdOffsets)

    override def groupMetadata(consumer: Client): IO[GroupMetadata] = IO.pure(FakeGroupMetadata(consumer))

    override def destroyGroupMetadata(metadata: GroupMetadata): IO[Unit] = IO.unit

    override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
      handle match
        case metadata: FakeGroupMetadata => Some(metadata)
        case _                           => None

    override def assign(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit] = unused("assign")

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

    override def applyRebalance(consumer: Client, event: Event): IO[Unit] = record(if event == 1 then "assign" else "unassign")

    override def cooperative(consumer: Client): IO[Boolean] = IO.pure(false)

    override def assignment(consumer: Client): IO[Vector[(String, Int)]] =
      calls.map(log => if log.lastIndexOf("assign") > log.lastIndexOf("unassign") then Vector("events" -> 0) else Vector.empty)

    override def commit(consumer: Client, offsets: Vector[(TopicPartition, Offset)], timeoutMillis: Int): IO[Unit] =
      record("commit") *> held(holdCommits)

    override def pause(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit] = IO.unit

    override def resume(consumer: Client, partitions: Vector[TopicPartition]): IO[Unit] = IO.unit

    override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): IO[Vector[Long]] = unused("committed")

    override def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): IO[(Long, Long)] = unused("watermarks")

    override def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): IO[Vector[Long]] =
      unused("offsetsForTimes")

    override def positions(consumer: Client, partitions: Vector[TopicPartition]): IO[Vector[Long]] = unused("positions")

    override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): IO[Unit] = unused("seek")

    override def metadata(client: Client, topic: Option[String], timeoutMillis: Int): IO[Vector[(String, Vector[Int])]] = unused("metadata")

    override def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): IO[Unit] = unused("createTopics")

    override def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): IO[Unit] = unused("deleteTopics")

    override def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): IO[Unit] = unused("createPartitions")

    override def deleteRecords(
        admin: Client,
        cuts: Vector[(TopicPartition, Offset)],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, Offset]]] = unused("deleteRecords")

    override def describeTopicConfigs(
        admin: Client,
        topics: Vector[Topic],
        timeoutMillis: Int
    ): IO[Vector[Either[KafkaException, TopicConfiguration]]] = unused("describeTopicConfigs")

  private object FakeDriver:
    def apply(holdCommits: Boolean = false, holdOffsets: Boolean = false): IO[FakeDriver] =
      (
        Ref.of[IO, Vector[String]](Vector.empty),
        Ref.of[IO, List[PolledRecord]](Nil),
        Ref.of[IO, List[Rebalance[Int]]](Nil),
        Ref.of[IO, Option[IO[Unit]]](None),
        Ref.of[IO, List[Deferred[IO, Unit]]](Nil),
        Ref.of[IO, Option[Deferred[IO, Unit]]](None)
      ).mapN((log, queued, rebalances, inPoll, holds, pauseTake) =>
        new FakeDriver(log, queued, rebalances, inPoll, holds, pauseTake, holdCommits, holdOffsets)
      )
