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
package internal.streams

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import internal.driver.{KafkaDriver, PolledRecord, StartAt}
import internal.protocol.{OffsetCommitRequest, OffsetFetchRequest}
import internal.protocol.OffsetFetchRequest.Stored as Fetched
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{KStream, Repartition, Serde, Store, Topology}

/** How the engine carries out its member's changes, starts, commits and gives back tasks, over a member and a data plane the tests script, in
  * simulated time, on every platform.
  */
final class EngineSuite extends CatsEffectSuite:
  import EngineSuite.*

  test("assigned tasks start where the group committed them, or at the start of their partitions, with their stores restored"):
    val fetched =
      OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, 3, Some(PartitionTime.encode(1234L)), 0), Fetched("in", 1, -1L, -1, None, 0)), 0)
    run(committed = _ => IO.pure(fetched)): world =>
      for
        _ <- world.tell(MemberAction.Assign(1, Assignment(Set(TaskId("0", 0), TaskId("0", 1)), Set.empty, Set.empty)))
        _ <- IO.sleep(100.millis)
        // Older than the partition time stored with the offset, which the task keeps.
        _       <- world.records(record(0, 5L, "k", 1000L))
        _       <- IO.sleep(1.second)
        log     <- world.log.get
        fetches <- world.fetches.get
      yield
        assertEquals(fetches, List(List(OffsetFetchRequest.Topic("in", List(0, 1)))))
        assertEquals(
          log.collect { case Event.Restored(changelogs) => changelogs }.toSet,
          Set(Map(partition("app-counts-changelog", 0) -> "counts"), Map(partition("app-counts-changelog", 1) -> "counts"))
        )
        val starts = Set(StartAt(partition("in", 0), 5L, -1), StartAt(partition("in", 1), KafkaDriver.BeginningOffset, -1))
        assertEquals(log.collect { case Event.Read(read) => read.toSet }, List(starts))
        assertEquals(log.dropWhile(!_.isInstanceOf[Event.Read]).take(2).last, Event.Assigned(1))
        assertEquals(
          log.collect { case Event.Committed(topics) => topics.flatMap(_.partitions).map(_.metadata) },
          List(List(Some(PartitionTime.encode(1234L))))
        )

  test("a fetch the coordinator cannot answer yet, or that meets offsets a transaction has yet to settle, goes again after waits that double"):
    val unsettled = OffsetFetchRequest.Answer(List(Fetched("in", 0, -1L, -1, None, 88)), 0)
    val answers   = List(OffsetFetchRequest.Answer(Nil, 14), unsettled, OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0))
    run(committed = n => IO.pure(answers(n))): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(1.second) *> (world.fetchedAt.get, world.log.get).tupled.map: (times, log) =>
        assertEquals(times.map(_ - times.head), List(0L, 100L, 300L))
        assertEquals(log.collect { case Event.Read(starts) => starts }, List(Vector(StartAt(partition("in", 0), 5L, -1))))

  test(
    "every interval, each task that read something commits the offset after its last record, with that record's leader epoch and its partition time"
  ):
    run(): world =>
      for
        _       <- world.tell(assign(TaskId("0", 0), TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(0, 5L, "k", 2000L, leaderEpoch = 3), record(0, 6L, "k", 1000L, leaderEpoch = 4))
        _       <- IO.sleep(1.second)
        first   <- world.commits.get
        _       <- IO.sleep(2.seconds)
        later   <- world.commits.get
        written <- world.written.get
      yield
        val partitionTime = Some(PartitionTime.encode(2000L))
        assertEquals(first, List(List(OffsetCommitRequest.Topic("in", List(OffsetCommitRequest.Partition(0, 7L, 4, partitionTime))))))
        assertEquals(later, first, "nothing was read since, so nothing more is committed")
        assertEquals(
          written.map(r => (r.topic.value, r.partition.map(_.value))),
          List(("app-counts-changelog", Some(0)), ("out", None), ("app-counts-changelog", Some(0)), ("out", None))
        )

  test("tasks given back are committed before the member hears they are, and their partitions are no longer read"):
    run(): world =>
      for
        _   <- world.tell(assign(TaskId("0", 0), TaskId("0", 1)))
        _   <- IO.sleep(100.millis)
        _   <- world.records(record(0, 5L, "k", 1000L), record(1, 8L, "k", 1000L))
        _   <- IO.sleep(200.millis)
        _   <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1))))
        _   <- IO.sleep(200.millis)
        log <- world.log.get
      yield
        val after = log.dropWhile(_ != Event.Assigned(1)).tail
        assertEquals(
          after,
          List(
            Event.Committed(List(OffsetCommitRequest.Topic("in", List(OffsetCommitRequest.Partition(1, 9L, -1, Some(PartitionTime.encode(1000L))))))),
            Event.Read(Vector(StartAt(partition("in", 0), 6L, -1))),
            Event.Revoked(2)
          )
        )

  test("lost tasks are dropped without a commit"):
    run(): world =>
      for
        _   <- world.tell(assign(TaskId("0", 0)))
        _   <- IO.sleep(100.millis)
        _   <- world.records(record(0, 5L, "k", 1000L))
        _   <- IO.sleep(200.millis)
        _   <- world.tell(MemberAction.LoseAll)
        _   <- IO.sleep(2.seconds)
        log <- world.log.get
      yield
        assertEquals(log.collect { case committed: Event.Committed => committed }, Nil)
        assertEquals(log.last, Event.Read(Vector.empty))

  test("a commit the coordinator cannot take yet goes again after a wait"):
    val answers = List(113, 16, 0).map(code => List(OffsetCommitRequest.Answer("in", 0, code.toShort)))
    run(commit = (n, _) => IO.pure(answers(n))): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis) *>
        world.committedAt.get.map(times => assertEquals(times.map(_ - times.head), List(0L, 100L, 300L)))

  test("a commit from a member the group no longer counts is dropped, and the engine goes on"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 25)))): world =>
      for
        _         <- world.tell(assign(TaskId("0", 0)))
        _         <- IO.sleep(100.millis)
        _         <- world.records(record(0, 5L, "k", 1000L))
        _         <- IO.sleep(3.seconds)
        _         <- world.records(record(0, 6L, "k", 1000L))
        _         <- IO.sleep(300.millis)
        committed <- world.committedAt.get
        written   <- world.written.get
        ended     <- world.ended
      yield
        assertEquals(committed.size, 1)
        assertEquals(written.size, 4)
        assertEquals(ended, None)

  test("any other refusal of a commit stops the engine with it"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 29)))): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis) *>
        world.ended.map: ended =>
          assertEquals(
            ended.flatMap(_.left.toOption).collect { case error: KafkaException.BackendFailure => error.code },
            Some(Some(ErrorCode.TopicAuthorizationFailed))
          )

  test("records of partitions the engine does not run are skipped, and nothing is polled while it runs no task"):
    run(): world =>
      for
        _       <- IO.sleep(1.second)
        early   <- world.polls.get
        _       <- world.tell(assign(TaskId("0", 0)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(3, 5L, "k", 1000L))
        _       <- IO.sleep(200.millis)
        written <- world.written.get
      yield
        assertEquals(early, 0)
        assertEquals(written, Nil)

  private val exactlyOnce: Option[Int => Option[Throwable]] = Some(_ => None)

  test(
    "exactly once, a transaction opens only once records arrive, and writes what the tasks wrote with the offsets they read, not through the member"
  ):
    run(failing = exactlyOnce): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0), TaskId("0", 1)))
        _     <- IO.sleep(500.millis)
        quiet <- world.log.get
        _     <- world.records(record(0, 5L, "k", 2000L, leaderEpoch = 3), record(0, 6L, "k", 1000L, leaderEpoch = 4))
        _     <- IO.sleep(2.seconds)
        log   <- world.log.get
        sent  <- world.commits.get
        _     <- world.records(record(1, 8L, "k", 3000L))
        _     <- IO.sleep(2.seconds)
        later <- world.log.get
      yield
        assertEquals(quiet.collect { case transaction: Event.Transaction => transaction }, Nil)
        val offset = OffsetAndMetadata(Offset.from(7L).orFail, Some(LeaderEpoch.from(4).orFail), PartitionTime.encode(2000L))
        assertEquals(
          log.collect { case transaction: Event.Transaction => transaction },
          List(Event.Transaction(4, Map(partition("in", 0) -> offset)))
        )
        assertEquals(sent, Nil)
        // A later transaction carries only what was read since the last.
        val next = OffsetAndMetadata(Offset.from(9L).orFail, None, PartitionTime.encode(3000L))
        assertEquals(
          later.collect { case transaction: Event.Transaction => transaction }.drop(1),
          List(Event.Transaction(2, Map(partition("in", 1) -> next)))
        )

  test("exactly once, the member's news ends the transaction under way, which commits before the news is carried out"):
    run(failing = exactlyOnce): world =>
      for
        _   <- world.tell(assign(TaskId("0", 0), TaskId("0", 1)))
        _   <- IO.sleep(100.millis)
        _   <- world.records(record(1, 8L, "k", 1000L))
        _   <- IO.sleep(200.millis)
        _   <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1))))
        _   <- IO.sleep(500.millis)
        log <- world.log.get
      yield
        val offset = OffsetAndMetadata(Offset.from(9L).orFail, None, PartitionTime.encode(1000L))
        assertEquals(
          log.dropWhile(_ != Event.Assigned(1)).tail,
          List(
            Event.Transaction(2, Map(partition("in", 1) -> offset)),
            Event.Read(Vector(StartAt(partition("in", 0), KafkaDriver.BeginningOffset, -1))),
            Event.Revoked(2)
          )
        )

  // ILLEGAL_GENERATION or STALE_MEMBER_EPOCH for an epoch the member is not at, and UNKNOWN_MEMBER_ID.
  List(ErrorCode.IllegalGeneration, ErrorCode.fromProtocol(113), ErrorCode.UnknownMemberId).foreach: code =>
    test(s"exactly once, a transaction refused for the member's identity with $code starts the tasks again from what the group committed"):
      refusedWith(code)

  private def refusedWith(code: ErrorCode): IO[Unit] =
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    val refused   = new KafkaException.BackendFailure("refused", Some(code), Some(false))
    run(committed = _ => IO.pure(committed), failing = Some(n => Option.when(n == 0)(refused))): world =>
      for
        _       <- world.tell(assign(TaskId("0", 0)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(0, 5L, "k", 1000L), record(0, 6L, "k", 1000L))
        _       <- IO.sleep(1500.millis)
        _       <- world.records(record(0, 5L, "k", 1000L))
        _       <- IO.sleep(1500.millis)
        log     <- world.log.get
        fetches <- world.fetches.get
        ended   <- world.ended
      yield
        assertEquals(fetches.size, 2)
        assertEquals(log.collect { case restored: Event.Restored => restored }.size, 2)
        assertEquals(log.collect { case Event.Read(starts) => starts }, List.fill(2)(Vector(StartAt(partition("in", 0), 5L, -1))))
        // One record read again after the restart makes one count, written to the changelog and the output.
        assertEquals(log.collect { case transaction: Event.Transaction => transaction.written }, List(2))
        assertEquals(ended, None)

  test("exactly once, any other failure of a transaction stops the engine with it"):
    val fenced = new KafkaException.BackendFailure("fenced", Some(ErrorCode.Other(90)), Some(false))
    run(failing = Some(_ => Some(fenced))): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis) *>
        world.ended.map(ended => assertEquals(ended, Some(Left(fenced))))

  /** Feeds a task of each sub-topology of the repartitioning program, waits `seconds` and gives the purges made. */
  private def purges(world: World, seconds: Int): IO[List[Map[TopicPartition, Offset]]] =
    for
      _   <- world.tell(assign(TaskId("0", 0), TaskId("1", 0)))
      _   <- IO.sleep(100.millis)
      _   <- world.records(record(0, 5L, "k", 1000L, value = "x1"))
      _   <- world.records((3L to 4L).map(n => record(0, n, "x", 1000L, topic = "app-by-letter-repartition"))*)
      _   <- IO.sleep(seconds.seconds)
      log <- world.log.get
    yield log.collect { case Event.Purged(before) => before }

  private val purged = Map(partition("app-by-letter-repartition", 0) -> Offset.from(5L).orFail)

  test("at least once, once the purge interval has passed, each repartition partition loses the records before where its task last committed"):
    run(topology = byLetter)(world => purges(world, 5).map(made => assertEquals(made, List(purged))))

  test("exactly once, once the purge interval has passed, each repartition partition loses the records before where its task last committed"):
    run(topology = byLetter, failing = exactlyOnce)(world => purges(world, 5).map(made => assertEquals(made, List(purged))))

  test("a purge that fails leaves the engine running, and the next goes after another interval"):
    run(topology = byLetter): world =>
      (world.failPurges *> purges(world, 5), world.ended).tupled.map: (made, ended) =>
        assertEquals(made, List(purged, purged))
        assertEquals(ended, None)

  test("tasks run at once up to the concurrency given, each reading its records in order"):
    run(topology = slow, concurrency = 2): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0), TaskId("0", 1)))
        _     <- IO.sleep(100.millis)
        start <- IO.monotonic.map(_.toMillis)
        _     <- world.records(record(0, 5L, "k", 1000L, value = "a"), record(1, 7L, "k", 1000L, value = "c"), record(0, 6L, "k", 1000L, value = "b"))
        _     <- IO.sleep(4.seconds)
        wrote <- world.wroteAt.get
      yield
        assertEquals(wrote.map(_._2.filter(_ != "c")), List(List("a", "b")))
        // Both tasks at once: as long as the task with two records takes, and less than the three seconds of one after the other.
        val took = wrote.map(_._1 - start)
        assert(took.forall(t => t >= 2000L && t < 2200L), s"written after $took ms")

  test("with a concurrency of one, tasks run one after another"):
    run(topology = slow, concurrency = 1): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0), TaskId("0", 1)))
        _     <- IO.sleep(100.millis)
        start <- IO.monotonic.map(_.toMillis)
        _     <- world.records(record(0, 5L, "k", 1000L, value = "a"), record(1, 7L, "k", 1000L, value = "c"), record(0, 6L, "k", 1000L, value = "b"))
        _     <- IO.sleep(4.seconds)
        wrote <- world.wroteAt.get
      yield
        val took = wrote.map(_._1 - start)
        assert(took.forall(t => t >= 3000L && t < 3200L), s"written after $took ms")

  test("released, the engine asks its member to leave and waits for it no longer than the close timeout"):
    val run =
      World().flatMap: world =>
        Engine.running("app", counting, world.member(leave = IO.never), world.plane, None, 1.second, 1, PurgeInterval, CloseTimeout)
          .use(_ => IO.unit) *> (IO.monotonic, world.log.get).tupled
    TestControl.executeEmbed(run.timeout(1.minute)).map: (released, log) =>
      assertEquals(released, CloseTimeout)
      assertEquals(log, List(Event.Leave))

object EngineSuite:
  val CloseTimeout: FiniteDuration = 3.seconds

  private val text = Serde.utf8[IO]

  val PurgeInterval: FiniteDuration = 2.seconds

  /** Counts by the value's first letter, through the repartition topic `app-by-letter-repartition`, read by the tasks of sub-topology 1. */
  private val byLetter: Topology[IO] =
    Topology.of(
      KStream(Topic.from("in").orFail, text, text).groupBy(Repartition("by-letter"), text, text)((_, value) => value.take(1)).count(Store("counts"))
        .toStream.to(Topic.from("out").orFail, text, Serde.long[IO])
    ).orFail

  /** Waits a second over each value, then writes it unchanged. */
  private val slow: Topology[IO] =
    Topology
      .of(KStream(Topic.from("in").orFail, text, text).evalMapValues(value => IO.sleep(1.second).as(value)).to(Topic.from("out").orFail, text, text))
      .orFail

  private val counting: Topology[IO] =
    Topology.of(
      KStream(Topic.from("in").orFail, text, text).groupByKey(text, text).count(Store("counts")).toStream
        .to(Topic.from("out").orFail, text, Serde.long[IO])
    ).orFail

  def partition(topic: String, n: Int): TopicPartition = TopicPartition(Topic.from(topic).orFail, Partition.from(n).orFail)

  def assign(tasks: TaskId*): MemberAction = MemberAction.Assign(1, Assignment(tasks.toSet, Set.empty, Set.empty))

  def record(
      partition: Int,
      offset: Long,
      key: String,
      timestamp: Long,
      leaderEpoch: Int = -1,
      value: String = "v",
      topic: String = "in"
  ): PolledRecord =
    PolledRecord(
      topic,
      partition,
      offset,
      Some(timestamp),
      1,
      leaderEpoch,
      Some(Chunk.array(key.getBytes)),
      Some(Chunk.array(value.getBytes)),
      Headers.empty
    )

  /** What the engine did, in order. */
  enum Event derives CanEqual:
    case Read(starts: Vector[StartAt])
    case Restored(changelogs: Map[TopicPartition, String])
    case Committed(topics: List[OffsetCommitRequest.Topic])
    case Assigned(round: Long)
    case Revoked(round: Long)
    case Leave
    case Transaction(written: Int, offsets: Map[TopicPartition, OffsetAndMetadata])
    case Purged(before: Map[TopicPartition, Offset])

  /** A member and a data plane the tests script, recording what the engine does with them. */
  final class World(
      changes: Queue[IO, Option[MemberAction]],
      incoming: Queue[IO, PolledRecord],
      val log: Ref[IO, List[Event]],
      val fetches: Ref[IO, List[List[OffsetFetchRequest.Topic]]],
      val fetchedAt: Ref[IO, List[Long]],
      val commits: Ref[IO, List[List[OffsetCommitRequest.Topic]]],
      val committedAt: Ref[IO, List[Long]],
      val written: Ref[IO, List[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]],
      val polls: Ref[IO, Int],
      transacted: Ref[IO, Int],
      outcome: Ref[IO, Option[Either[Throwable, Unit]]],
      val wroteAt: Ref[IO, List[(Long, List[String])]],
      purging: Ref[IO, IO[Unit]]
  ):
    /** Makes each purge fail after it is logged. */
    def failPurges: IO[Unit] = purging.set(IO.raiseError(new RuntimeException("purge refused")))

    def tell(change: MemberAction): IO[Unit]                = changes.offer(Some(change))
    def records(polled: PolledRecord*): IO[Unit]            = polled.toList.traverse_(incoming.offer)
    def ended: IO[Option[Either[Throwable, Unit]]]          = outcome.get
    def finished(result: Either[Throwable, Unit]): IO[Unit] = outcome.set(Some(result))

    private def logged(event: Event): IO[Unit] = log.update(_ :+ event)
    private def now: IO[Long]                  = IO.monotonic.map(_.toMillis)

    def member(
        committed: Int => IO[OffsetFetchRequest.Answer] = _ => IO.pure(OffsetFetchRequest.Answer(Nil, 0)),
        commit: (Int, List[OffsetCommitRequest.Topic]) => IO[List[OffsetCommitRequest.Answer]] =
          (_, topics) => IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, 0)))),
        leave: IO[Unit] = IO.unit
    ): GroupMember[IO] =
      val (fetch, send) = (committed, commit)
      val leaving       = leave
      new GroupMember[IO]:
        def changes: Stream[IO, MemberAction]                                                     = Stream.fromQueueNoneTerminated(World.this.changes)
        def revoked(round: Long): IO[Unit]                                                        = logged(Event.Revoked(round))
        def assigned(round: Long): IO[Unit]                                                       = logged(Event.Assigned(round))
        def leave: IO[Unit]                                                                       = logged(Event.Leave) *> leaving
        def epoch: IO[Int]                                                                        = IO.pure(1)
        def commit(topics: List[OffsetCommitRequest.Topic]): IO[List[OffsetCommitRequest.Answer]] =
          (now.flatMap(at => committedAt.update(_ :+ at)) *> commits.modify(before => (before :+ topics, before.size))).flatMap: n =>
            send(n, topics).flatTap(answers => logged(Event.Committed(topics)).whenA(answers.forall(_.errorCode == 0)))
        def committed(topics: List[OffsetFetchRequest.Topic]): IO[OffsetFetchRequest.Answer] =
          (now.flatMap(at => fetchedAt.update(_ :+ at)) *> fetches.modify(before => (before :+ topics, before.size))).flatMap(fetch)

    /** Transactions that record what they wrote and the offsets they took, failing as `failing` says for the `n`th. */
    def transactions(failing: Int => Option[Throwable]): Engine.Transactions[IO] =
      new Engine.Transactions[IO]:
        def apply[A](body: Engine.Writer[IO] => IO[(A, Map[TopicPartition, OffsetAndMetadata])]): IO[A] =
          IO.ref(0).flatMap: count =>
            body(records => count.update(_ + records.size)).flatMap: (result, offsets) =>
              transacted.modify(n => (n + 1, n)).flatMap: n =>
                failing(n).fold(count.get.flatMap(written => logged(Event.Transaction(written, offsets))).as(result))(IO.raiseError)

    val plane: Engine.Plane[IO] =
      new Engine.Plane[IO]:
        def read(starts: Vector[StartAt]): IO[Unit] = logged(Event.Read(starts))
        def poll: IO[List[PolledRecord]]            =
          polls.update(_ + 1) *> incoming.tryTakeN(None)
            .flatMap(taken => if taken.isEmpty then IO.sleep(Engine.PollMillis.millis).as(Nil) else IO.pure(taken))
        def restore(changelogs: Map[TopicPartition, String]): IO[Map[String, Map[Chunk[Byte], Stored]]] =
          logged(Event.Restored(changelogs)).as(Map.empty)
        def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): IO[Unit] =
          val values = records.toList.flatMap(_.value).map(value => new String(value.toArray))
          written.update(_ ++ records.toList) *> now.flatMap(at => wroteAt.update(_ :+ (at, values)))
        def purge(before: Map[TopicPartition, Offset]): IO[Unit] = logged(Event.Purged(before)) *> purging.get.flatten

  object World:
    def apply(): IO[World] =
      (
        Queue.unbounded[IO, Option[MemberAction]],
        Queue.unbounded[IO, PolledRecord],
        IO.ref(List.empty[Event]),
        IO.ref(List.empty[List[OffsetFetchRequest.Topic]]),
        IO.ref(List.empty[Long]),
        IO.ref(List.empty[List[OffsetCommitRequest.Topic]]),
        IO.ref(List.empty[Long]),
        IO.ref(List.empty[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]),
        IO.ref(0),
        IO.ref(0),
        IO.ref(Option.empty[Either[Throwable, Unit]]),
        IO.ref(List.empty[(Long, List[String])]),
        IO.ref(IO.unit)
      ).mapN(new World(_, _, _, _, _, _, _, _, _, _, _, _, _))

  /** Runs a program's engine, the counting one unless another is given, with a commit interval of a second over a member answering as given, while
    * `script` runs: at least once, or exactly once with transactions that fail as `failing` says, running up to `concurrency` tasks at once.
    */
  def run(
      committed: Int => IO[OffsetFetchRequest.Answer] = _ => IO.pure(OffsetFetchRequest.Answer(Nil, 0)),
      commit: (Int, List[OffsetCommitRequest.Topic]) => IO[List[OffsetCommitRequest.Answer]] =
        (_, topics) => IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, 0)))),
      failing: Option[Int => Option[Throwable]] = None,
      topology: Topology[IO] = counting,
      concurrency: Int = 1
  )(script: World => IO[Unit]): IO[Unit] =
    val program =
      World().flatMap: world =>
        val transactions = failing.map(world.transactions)
        val member       = world.member(committed, commit)
        Engine.running("app", topology, member, world.plane, transactions, 1.second, concurrency, PurgeInterval, CloseTimeout).use: running =>
          running.attempt.flatMap(world.finished).start *> script(world)
    TestControl.executeEmbed(program.timeout(1.minute))
