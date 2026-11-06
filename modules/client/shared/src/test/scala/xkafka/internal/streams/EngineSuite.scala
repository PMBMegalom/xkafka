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
import fs2.concurrent.{Signal, SignallingRef}
import munit.CatsEffectSuite
import internal.driver.{KafkaDriver, PolledRecord, StartAt}
import internal.protocol.{OffsetCommitRequest, OffsetFetchRequest, StreamsGroup}
import internal.protocol.OffsetFetchRequest.Stored as Fetched
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{
  BufferLimit, GlobalKTable, KafkaStreams, KStream, KTable, Named, Repartition, Serde, SessionWindows, Store, StreamsPhase, StreamsProblem,
  StreamsState, Suppressed, TimeWindows, Topology
}

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

  /** Stores in memory that record each batch written to them. */
  private def recording(batches: Ref[IO, List[StoreChanges]]): TaskStore.Open[IO] =
    (_, held) =>
      TaskStore.inMemory[IO](held).map: store =>
        new TaskStore[IO]:
          def get(key: Chunk[Byte]): IO[Option[Stored]]                        = store.get(key)
          def prefixed(prefix: Chunk[Byte]): IO[Vector[(Chunk[Byte], Stored)]] = store.prefixed(prefix)
          def all: IO[StoreContents]                                           = store.all
          def write(changes: StoreChanges): IO[Unit]                           = batches.update(_ :+ changes) *> store.write(changes)

  private def storing(
      commit: (Int, List[OffsetCommitRequest.Topic]) => IO[List[OffsetCommitRequest.Answer]],
      failing: Option[Int => Option[Throwable]]
  )(check: (IO[List[StoreChanges]], World) => IO[Unit]): IO[Unit] =
    IO.ref(List.empty[StoreChanges]).flatMap(batches => run(commit = commit, failing = failing, open = recording(batches))(check(batches.get, _)))

  private val answered: (Int, List[OffsetCommitRequest.Topic]) => IO[List[OffsetCommitRequest.Answer]] =
    (_, topics) => IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, 0))))

  test("at least once, what a task wrote reaches its store as one batch once the commit is answered, not before"):
    storing(answered, None): (batches, world) =>
      for
        _      <- world.tell(assign(TaskId("0", 0)))
        _      <- IO.sleep(100.millis)
        _      <- world.records(record(0, 5L, "k", 1000L), record(0, 6L, "other", 1000L))
        _      <- IO.sleep(500.millis)
        before <- batches
        _      <- IO.sleep(2.seconds)
        after  <- batches
        sent   <- world.commits.get
      yield
        assertEquals(before, Nil)
        assert(sent.nonEmpty)
        assertEquals(after.map(_.keySet.map(key => new String(key.toArray))), List(Set("k", "other")))

  test("at least once, what a task wrote does not reach its store where the commit fails"):
    val failed: (Int, List[OffsetCommitRequest.Topic]) => IO[List[OffsetCommitRequest.Answer]] =
      (_, topics) => IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, 29))))
    storing(failed, None): (batches, world) =>
      for
        _     <- world.tell(assign(TaskId("0", 0)))
        _     <- IO.sleep(100.millis)
        _     <- world.records(record(0, 5L, "k", 1000L))
        _     <- IO.sleep(3.seconds)
        ended <- world.ended
        after <- batches
      yield
        assert(ended.exists(_.isLeft), ended)
        assertEquals(after, Nil)

  test("exactly once, what a task wrote reaches its store as one batch once its transaction commits"):
    storing(answered, exactlyOnce): (batches, world) =>
      for
        _     <- world.tell(assign(TaskId("0", 0)))
        _     <- IO.sleep(100.millis)
        _     <- world.records(record(0, 5L, "k", 1000L))
        _     <- IO.sleep(3.seconds)
        after <- batches
        log   <- world.log.get
      yield
        assertEquals(log.collect { case transaction: Event.Transaction => transaction }.size, 1)
        assertEquals(after.map(_.keySet.map(key => new String(key.toArray))), List(Set("k")))

  test("exactly once, what a task wrote does not reach its store where its transaction is refused"):
    val refused = new KafkaException.BackendFailure("refused", Some(ErrorCode.UnknownMemberId), Some(false))
    storing(answered, Some(n => Option.when(n == 0)(refused))): (batches, world) =>
      for
        _     <- world.tell(assign(TaskId("0", 0)))
        _     <- IO.sleep(100.millis)
        _     <- world.records(record(0, 5L, "k", 1000L))
        _     <- IO.sleep(3.seconds)
        after <- batches
      yield assertEquals(after, Nil)

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
  List(ErrorCode.IllegalGeneration, ErrorCode.StaleMemberEpoch, ErrorCode.UnknownMemberId).foreach: code =>
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

  /** Runs the counting program's engine while `script` runs with it, recording the states it reports. */
  private def watched(script: (World, KafkaStreams[IO]) => IO[Unit]): IO[Unit] = watchedOf(counting)(script)

  private def watchedOf(topology: Topology[IO])(script: (World, KafkaStreams[IO]) => IO[Unit]): IO[Unit] =
    val program =
      World().flatMap: world =>
        Engine.running(
          "app",
          topology,
          world.member(),
          world.plane,
          None,
          1.second,
          1,
          PurgeInterval,
          CloseTimeout,
          _ => IO.pure(4),
          TaskStore.memory[IO]
        ).use: streams =>
          streams.state.evalMap(state => world.states.update(_ :+ state)).compile.drain.start *> script(world, streams)
    TestControl.executeEmbed(program.timeout(1.minute))

  test("the state goes from joining, through restoring the stores of tasks given, to running, and to lost when tasks are lost"):
    watched: (world, _) =>
      for
        _      <- world.restoreFor(500.millis)
        _      <- IO.sleep(100.millis)
        _      <- world.tell(assign(TaskId("0", 0)))
        _      <- IO.sleep(300.millis)
        during <- world.states.get
        _      <- IO.sleep(500.millis)
        _      <- world.tell(MemberAction.LoseAll)
        _      <- IO.sleep(300.millis)
        states <- world.states.get
      yield
        assertEquals(during.map(_.phase), List(StreamsPhase.Joining, StreamsPhase.Restoring))
        assertEquals(states.map(_.phase), List(StreamsPhase.Joining, StreamsPhase.Restoring, StreamsPhase.Running, StreamsPhase.Lost))

  test("what the coordinator reports about the group, and its refusal of the member's topics, show as the state's problems"):
    watched: (world, _) =>
      val statuses = List(StreamsGroup.Status(1, "missing"), StreamsGroup.Status(5, "delayed"), StreamsGroup.Status(9, "new"))
      for
        _      <- IO.sleep(100.millis)
        _      <- world.report(_.copy(statuses = statuses, unauthorized = Some("denied")))
        _      <- IO.sleep(100.millis)
        _      <- world.report(_.copy(statuses = Nil, unauthorized = None))
        _      <- IO.sleep(100.millis)
        states <- world.states.get
      yield assertEquals(
        states.map(_.problems),
        List(
          Nil,
          List(
            StreamsProblem.MissingSourceTopics("missing"),
            StreamsProblem.AssignmentDelayed("delayed"),
            StreamsProblem.Other(9, "new"),
            StreamsProblem.TopicAuthorizationFailed("denied")
          ),
          Nil
        )
      )

  // Java's set of "orders" and "owners" puts them in one bucket, whose order it takes from names xkafka does not know; "orders" and "users" it does not.
  test("an application joining topics it cannot list in Java's order says so in its state, and one joining topics it can does not"):
    def joined(table: String) =
      Topology.of(
        KStream(Named("orders"), Topic.from("orders").orFail, text, text)
          .join(Named("join-t"), KTable(Named("t"), Topic.from(table).orFail, text, text, Store("t")))(_ + _)
          .to(Named("write-1"), Topic.from("out").orFail, text, text)
      ).orFail
    assertEquals(Declaration.unordered("app", joined("owners")), List(List("orders", "owners")))
    assertEquals(Declaration.unordered("app", joined("users")), Nil)
    watchedOf(joined("owners")): (world, _) =>
      IO.sleep(100.millis) *> world.states.get.map: states =>
        assertEquals(
          states.map(_.problems),
          List(List(StreamsProblem.JavaMembersMayBeRefused("Java Streams may list the joined topics orders, owners in another order")))
        )

  test("an application joining an input that has sub-topologies of its own says in its state that Java numbers them by its program's build order"):
    val counted =
      KStream(Named("visits"), Topic.from("visits").orFail, text, text).groupBy(Repartition("by-page"), text, text)((_, v) => v)
        .count(Named("count-1"), Store("visits"))
    val joined =
      Topology.of(
        KStream(Named("orders"), Topic.from("orders").orFail, text, text).join(Named("join-1"), counted)((o, n) => s"$o$n")
          .to(Named("write-2"), Topic.from("out").orFail, text, text)
      ).orFail
    watchedOf(joined): (world, _) =>
      IO.sleep(100.millis) *> world.states.get.map: states =>
        assertEquals(
          states.map(_.problems),
          List(List(StreamsProblem.JavaMembersMayBeRefused(
            "Java Streams numbers the sub-topologies of the join of orders, app-by-page-repartition otherwise unless it builds the receiver first"
          )))
        )

  test("an application cogrouping through a repartition topic says in its state that Java cannot start it"):
    def group(name: String) = KStream(Named(name), Topic.from(name).orFail, text, text)
    val cogrouped           =
      Topology.of(
        group("a").groupBy(Repartition("r"), text, text)((_, v) => v).cogroup[String]((_, v, t) => t + v)
          .cogroup(group("b").groupByKey(text, text))((_, v, t) => t + v).aggregate(Named("co"), Store("kept"), text)("").toStream(Named("c"))
          .to(Named("w"), Topic.from("out").orFail, text, text)
      ).orFail
    watchedOf(cogrouped): (world, _) =>
      IO.sleep(100.millis) *> world.states.get.map: states =>
        assertEquals(
          states.map(_.problems),
          List(List(
            StreamsProblem
              .JavaMembersMayBeRefused("Java Streams cannot start a cogroup of app-r-repartition, b, since a group goes through a repartition topic")
          ))
        )

  test("an application joining a global table says in its state that Java numbers sub-topologies by where its program builds it"):
    val joined =
      Topology.of(
        KStream(Named("in"), Topic.from("in").orFail, text, text)
          .join(Named("j"), GlobalKTable(Named("g"), Topic.from("g").orFail, text, text, Store("gs")))((_, v) => v)(_ + _)
          .to(Named("w"), Topic.from("out").orFail, text, text)
      ).orFail
    watchedOf(joined): (world, _) =>
      IO.sleep(100.millis) *> world.states.get.map: states =>
        assertEquals(
          states.map(_.problems),
          List(List(StreamsProblem.JavaMembersMayBeRefused(
            "Java Streams numbers the sub-topologies around the global table of g otherwise unless it builds it after the stream that first joins it"
          )))
        )

  test("an application can stop itself on a state it sees, giving back its tasks as it leaves"):
    val run =
      World().flatMap: world =>
        val refused = world.report(_.copy(unauthorized = Some("denied"))).delayBy(2.seconds).start
        val stopped =
          Engine.running(
            "app",
            counting,
            world.member(),
            world.plane,
            None,
            1.second,
            1,
            PurgeInterval,
            CloseTimeout,
            _ => IO.pure(4),
            TaskStore.memory[IO]
          ).use: streams =>
            val unauthorized = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.TopicAuthorizationFailed])).compile.drain
            refused *> IO.race(streams.run, unauthorized)
        stopped *> (IO.monotonic, world.log.get).tupled
    TestControl.executeEmbed(run.timeout(1.minute)).map: (at, log) =>
      // Leaving waits for the standby reader to finish its poll.
      assert(at >= 2.seconds && at <= 2.seconds + Engine.PollMillis.millis, s"stopped at $at")
      assertEquals(log, List(Event.Leave))

  private def changelog(n: Int) = partition("app-counts-changelog", n)

  private def logged(key: String, count: Long, offset: Long): PolledRecord =
    PolledRecord(
      "app-counts-changelog",
      1,
      offset,
      Some(1000L),
      1,
      -1,
      Some(Chunk.array(key.getBytes)),
      Some(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(count).array)),
      Headers.empty
    )

  test("a standby task follows its changelog from the start, and where it is and where the changelog ends are its task offsets"):
    run(): world =>
      for
        _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
        _       <- IO.sleep(500.millis)
        _       <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
        _       <- world.endsAt(Map(changelog(1) -> 5L))
        _       <- IO.sleep(500.millis)
        log     <- world.log.get
        offsets <- world.taskOffsets
      yield
        assertEquals(log.collect { case followed: Event.Followed => followed }, List(Event.Followed(Vector(StartAt(changelog(1), 0L, -1)))))
        assertEquals(log.last, Event.Assigned(1))
        assertEquals(offsets, TaskOffsets(List(StreamsGroup.TaskOffset("0", 1, 2L)), List(StreamsGroup.TaskOffset("0", 1, 5L))))

  test("a standby task made active restores only what it had not read, onto what it holds, and is no longer followed"):
    run(): world =>
      for
        _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
        _       <- IO.sleep(500.millis)
        _       <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
        _       <- IO.sleep(500.millis)
        _       <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty)))
        _       <- IO.sleep(500.millis)
        log     <- world.log.get
        from    <- world.restoredFrom.get
        offsets <- world.taskOffsets
      yield
        assertEquals(from.map(_._1), List(Map(changelog(1) -> 2L)))
        assertEquals(from.map(_._2("counts").keySet.map(key => new String(key.toArray))), List(Set("k")))
        assertEquals(log.collect { case followed: Event.Followed => followed }.last, Event.Followed(Vector.empty))
        assertEquals(offsets, TaskOffsets(Nil, Nil))

  test("a changelog record read again from before where a standby task has read to is not applied again"):
    run(): world =>
      for
        _    <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(logged("k", 1L, 0L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty)))
        _    <- IO.sleep(500.millis)
        from <- world.restoredFrom.get
      yield
        val held = from.map(_._2("counts").values.map(stored => java.nio.ByteBuffer.wrap(stored.value.toArray).getLong).toList)
        assertEquals((from.map(_._1), held), (List(Map(changelog(1) -> 2L)), List(List(2L))))

  test("a task that emits windows once they close commits the close time it last emitted for, as Java does"):
    run(topology = emitting): world =>
      for
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 0L, "k", 1000L))
        _       <- IO.sleep(2.seconds)
        _       <- world.records(record(1, 1L, "k", 20000L))
        _       <- IO.sleep(2.seconds)
        log     <- world.log.get
        written <- world.written.get
      yield
        assertEquals(written.count(_.topic.value == "out"), 1)
        assertEquals(
          log.collect { case Event.Committed(topics) => topics.flatMap(_.partitions).flatMap(_.metadata) }.flatten.last,
          PartitionTime.encode(20000L, Map("windows" -> 15000L))
        )

  test("a task started where the group committed emitted windows does not emit them again"):
    val fetched = OffsetFetchRequest.Answer(List(Fetched("in", 1, 1L, -1, Some(PartitionTime.encode(20000L, Map("windows" -> 15000L))), 0)), 0)
    run(committed = _ => IO.pure(fetched), topology = emitting): world =>
      for
        _       <- world.restoredAlso.set(Map("windows" -> StoreContents.of(Map(window("k", 0L) -> counted(5L)))))
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 1L, "k", 20000L))
        _       <- IO.sleep(2.seconds)
        written <- world.written.get
      yield assertEquals(written.count(_.topic.value == "out"), 0)

  // Java partitions a windowed key by the key alone (WindowedStreamPartitioner), so every window of a key goes to one partition.
  test("each window's result goes to the partition the Java producer picks for its key alone"):
    run(topology = windowing): world =>
      for
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 0L, "k", 2000L), record(1, 1L, "k", 12000L), record(1, 2L, "other", 2000L))
        _       <- IO.sleep(1.second)
        written <- world.written.get
      yield
        val out = written.filter(_.topic.value == "out")
        assertEquals(
          out.map(record => record.key.map(key => new String(key.take(key.size - 8).toArray)) -> record.partition.map(_.value)),
          List("k", "k", "other").map(key => Some(key) -> Some(Murmur2.partition(Chunk.array(key.getBytes), 4)))
        )

  test("at a commit, a suppression's buffer logs each key it changed since the last, what it holds or a tombstone once it holds it no more"):
    run(topology = suppressing): world =>
      for
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 0L, "k", 1000L), record(1, 1L, "k", 2000L))
        _       <- IO.sleep(1500.millis)
        first   <- world.written.get
        _       <- world.records(record(1, 2L, "k", 20000L))
        _       <- IO.sleep(1500.millis)
        written <- world.written.get
      yield
        val logged = written.filter(_.topic.value == "app-final-store-changelog")
        assertEquals(first.count(_.topic.value == "app-final-store-changelog"), 1)
        assertEquals(logged.map(_.partition.map(_.value)).distinct, List(Some(1)))
        assertEquals(logged.map(_.timestamp), List(None, None, None))
        val held = logged.head.value.flatMap(Held.decode)
        assertEquals(held.map(h => (h.time, h.context.offset, h.context.topic, h.context.partition)), Some((10000L, 1L, "in", 1)))
        assertEquals(logged.head.headers, Held.Version)
        // The window [0, 10000) has gone on, and [20000, 30000) is held.
        assertEquals(
          logged.drop(1).map(r => (r.key.map(k => java.nio.ByteBuffer.wrap(k.toArray).getLong(k.size - 8)), r.value.isEmpty)).toSet,
          Set((Some(0L), true), (Some(20000L), false))
        )
        assertEquals(written.filter(_.topic.value == "out").flatMap(_.value).map(v => java.nio.ByteBuffer.wrap(v.toArray).getLong), List(2L))

  test("a suppression's buffer logs what it changed before the offsets are committed"):
    run(topology = suppressing, commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 1, 29)))): world =>
      world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis) *> world.records(record(1, 0L, "k", 1000L)) *> IO.sleep(1500.millis) *>
        (world.ended, world.written.get).mapN: (ended, written) =>
          assert(ended.exists(_.isLeft), ended)
          assertEquals(written.count(_.topic.value == "app-final-store-changelog"), 1)

  test("exactly once, what a suppression's buffer logs goes in the transaction with the offsets"):
    run(topology = suppressing, failing = exactlyOnce): world =>
      for
        _   <- world.tell(assign(TaskId("0", 1)))
        _   <- IO.sleep(100.millis)
        _   <- world.records(record(1, 0L, "k", 1000L), record(1, 1L, "other", 1000L))
        _   <- IO.sleep(2.seconds)
        log <- world.log.get
      // Two records to the window store's changelog, and two to the buffer's.
      yield assertEquals(log.collect { case Event.Transaction(written, _) => written }, List(4))

  test("a suppression's buffer set to shut down when full stops the engine with an error once it holds more"):
    val full =
      Topology.of(
        KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text)
          .windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds)).count(Named("count-2"), Store("windows"))
          .suppress(Suppressed.untilWindowCloses("final", BufferLimit.records(1).shutDownWhenFull)).toStream(Named("to-stream-1"))
          .to(Named("write-3"), Topic.from("out").orFail, Serde.timeWindowed(text, 10.seconds), Serde.long[IO])
      ).orFail
    run(topology = full): world =>
      world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis) *> world.records(record(1, 0L, "k", 1000L)) *> IO.sleep(500.millis) *>
        world.ended.flatMap: before =>
          world.records(record(1, 1L, "other", 1000L)) *> IO.sleep(500.millis) *> world.ended.map: after =>
            assertEquals(before, None)
            assertEquals(
              after.flatMap(_.left.toOption.map(_.getMessage)),
              Some("the suppression 'final' buffer exceeded its limit: 2/1 records and 114/9223372036854775807 bytes")
            )

  test("a restored suppression's buffer sends on what it held once its window closes, with the time and headers it was held with"):
    val context = HeldContext(4000L, 7L, "in", 1, Headers(Header("trace", Some(Chunk.array("held".getBytes)))))
    val held    = Held(10000L, context, None, Some(counted(4L).value), Some(counted(5L).value))
    run(topology = suppressing): world =>
      for
        _       <- world.restoredAlso.set(Map("final-store" -> StoreContents.of(Map(window("k", 0L) -> Stored(held.encode, 0L)))))
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 8L, "other", 15000L))
        _       <- IO.sleep(1500.millis)
        written <- world.written.get
      yield
        val out = written.filter(_.topic.value == "out")
        assertEquals(
          out.map(r => (r.key, r.value, r.timestamp.map(_.epochMillis), r.headers)),
          List((Some(window("k", 0L)), Some(counted(5L).value), Some(4000L), context.headers))
        )

  test("a task of a stream joined with a table reads its partition of each topic from where the group committed it, and commits each with its time"):
    val fetched =
      OffsetFetchRequest.Answer(
        List(Fetched("orders", 1, 4L, -1, Some(PartitionTime.encode(1500L)), 0), Fetched("users", 1, 7L, -1, Some(PartitionTime.encode(900L)), 0)),
        0
      )
    run(committed = _ => IO.pure(fetched), topology = joining): world =>
      for
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 7L, "u", 300L, value = "ann", topic = "users"))
        _       <- IO.sleep(1500.millis)
        log     <- world.log.get
        fetches <- world.fetches.get
      yield
        assertEquals(fetches, List(List(OffsetFetchRequest.Topic("orders", List(1)), OffsetFetchRequest.Topic("users", List(1)))))
        assertEquals(
          log.collect { case Event.Read(read) => read.toSet }.head,
          Set(StartAt(partition("orders", 1), 4L, -1), StartAt(partition("users", 1), 7L, -1))
        )
        // Only the partition read since commits, keeping its own partition time, which an older record leaves as it was.
        assertEquals(
          log.collect { case Event.Committed(topics) => topics.map(t => t.name -> t.partitions.map(p => (p.index, p.offset, p.metadata))) },
          List(List("users" -> List((1, 8L, Some(PartitionTime.encode(900L))))))
        )

  test("a task takes the records of its partitions earliest first, so an order joins the user record read with it but timestamped before it"):
    run(topology = joining): world =>
      for
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 0L, "u", 2000L, value = "o1", topic = "orders"), record(1, 0L, "u", 1000L, value = "ann", topic = "users"))
        _       <- IO.sleep(1500.millis)
        written <- world.written.get
      yield assertEquals(written.filter(_.topic.value == "out").flatMap(_.value).map(v => new String(v.toArray)), List("o1:ann"))

  test("records of a task's partitions go earliest first, the earliest read of those at one time, each partition in the order read"):
    def polled(topic: String, offset: Long, at: Long) = record(0, offset, "k", at, topic = topic)
    val records                                       =
      List("a" -> polled("a", 0, 5L), "a" -> polled("a", 1, 1L), "b" -> polled("b", 0, 3L), "b" -> polled("b", 1, 9L), "c" -> polled("c", 0, 5L))
    assertEquals(Engine.byTime(records).map((p, r) => (p, r.offset)), List(("b", 0L), ("a", 0L), ("a", 1L), ("c", 0L), ("b", 1L)))

  test("a restored session store keeps the sessions its clock has not expired, which a record then joins"):
    run(topology = sessioning): world =>
      for
        _ <-
          world.restoredAlso
            .set(Map("sessions" -> StoreContents.of(Map(session("k", 1000L, 9000L) -> counted(5L), session("other", 12000L, 12000L) -> counted(1L)))))
        _       <- world.tell(assign(TaskId("0", 1)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(1, 0L, "k", 10000L))
        _       <- IO.sleep(1.second)
        written <- world.written.get
      yield
        val counts = written.filter(_.topic.value == "out").flatMap(_.value).map(value => java.nio.ByteBuffer.wrap(value.toArray).getLong)
        assertEquals(counts, List(6L))

  test("a standby session store loses the sessions its clock has expired as it follows its changelog"):
    def sessionLogged(key: String, start: Long, end: Long, offset: Long) =
      PolledRecord("app-sessions-changelog", 1, offset, Some(end), 1, -1, Some(session(key, start, end)), Some(counted(1L).value), Headers.empty)
    run(topology = sessioning): world =>
      for
        _ <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
        _ <- IO.sleep(500.millis)
        // By its end, which a session store expires it by, [0, 25000] is not expired; read as a window's start, it would be.
        _    <- world.changelogs(sessionLogged("k", 0L, 25000L, 0L), sessionLogged("gone", 0L, 5000L, 1L), sessionLogged("other", 30000L, 30000L, 2L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty)))
        _    <- IO.sleep(500.millis)
        from <- world.restoredFrom.get
      yield assertEquals(from.map(_._2("sessions").keySet), List(Set(session("k", 0L, 25000L), session("other", 30000L, 30000L))))

  test("a restored window store loses the windows its clock has expired, so a record for one starts it again"):
    run(topology = windowing): world =>
      for
        _ <- world.restoredAlso.set(Map("windows" -> StoreContents.of(Map(window("k", 0L) -> counted(5L), window("other", 30000L) -> counted(1L)))))
        _ <- world.tell(assign(TaskId("0", 1)))
        _ <- IO.sleep(100.millis)
        _ <- world.records(record(1, 0L, "k", 2000L))
        _ <- IO.sleep(1.second)
        written <- world.written.get
      yield
        val counts = written.filter(_.topic.value == "out").flatMap(_.value).map(value => java.nio.ByteBuffer.wrap(value.toArray).getLong)
        assertEquals(counts, List(1L))

  test("a standby window store loses the windows its clock has expired as it follows its changelog"):
    def windowLogged(key: String, start: Long, offset: Long) =
      PolledRecord("app-windows-changelog", 1, offset, Some(start), 1, -1, Some(window(key, start)), Some(counted(1L).value), Headers.empty)
    run(topology = windowing): world =>
      for
        _    <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(windowLogged("k", 0L, 0L), windowLogged("other", 30000L, 1L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty)))
        _    <- IO.sleep(500.millis)
        from <- world.restoredFrom.get
      yield assertEquals(from.map(_._2("windows").keySet), List(Set(window("other", 30000L))))

  /** Stands by for task 0_1, reads two of its changelog records, takes the standby away, then after `later` makes the task active. */
  private def droppedThenActive(world: World, later: FiniteDuration): IO[(List[Map[TopicPartition, Long]], TaskOffsets)] =
    for
      _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
      _       <- IO.sleep(500.millis)
      _       <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
      _       <- IO.sleep(500.millis)
      _       <- world.tell(MemberAction.Assign(2, Assignment(Set.empty, Set.empty, Set.empty)))
      _       <- IO.sleep(500.millis)
      offsets <- world.taskOffsets
      _       <- IO.sleep(later)
      _       <- world.tell(MemberAction.Assign(3, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty)))
      _       <- IO.sleep(500.millis)
      from    <- world.restoredFrom.get
    yield (from.map(_._1), offsets)

  test("a standby taken away keeps its store, reported in task offsets, and its task made active soon after restores only what it had not read"):
    run(): world =>
      droppedThenActive(world, 1.minute).map: (from, offsets) =>
        assertEquals(from, List(Map(changelog(1) -> 2L)))
        assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 2L)))

  test("a standby taken away longer ago than Java's cleanup delay is forgotten, and its task restores from the start"):
    run(): world =>
      droppedThenActive(world, Engine.StateCleanupDelay).map((from, _) => assertEquals(from, List(Map.empty[TopicPartition, Long])))

  test("lost tasks are no longer stood by, but their stores are kept and reported, as Java keeps a lost task's state directory"):
    run(): world =>
      for
        _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty)))
        _       <- IO.sleep(500.millis)
        _       <- world.tell(MemberAction.LoseAll)
        _       <- IO.sleep(500.millis)
        log     <- world.log.get
        offsets <- world.taskOffsets
      yield
        assertEquals(log.collect { case followed: Event.Followed => followed }.last, Event.Followed(Vector.empty))
        assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 0L)))

  test("released, the engine asks its member to leave and waits for it no longer than the close timeout and one poll of its standby reader"):
    val run =
      World().flatMap: world =>
        Engine.running(
          "app",
          counting,
          world.member(leave = IO.never),
          world.plane,
          None,
          1.second,
          1,
          PurgeInterval,
          CloseTimeout,
          _ => IO.pure(4),
          TaskStore.memory[IO]
        ).use(_ => IO.unit) *> (IO.monotonic, world.log.get).tupled
    TestControl.executeEmbed(run.timeout(1.minute)).map: (released, log) =>
      assert(released >= CloseTimeout && released <= CloseTimeout + Engine.PollMillis.millis, s"released at $released")
      assertEquals(log, List(Event.Leave))

object EngineSuite:
  val CloseTimeout: FiniteDuration = 3.seconds

  private val text = Serde.utf8[IO]

  val PurgeInterval: FiniteDuration = 2.seconds

  /** Counts by the value's first letter, through the repartition topic `app-by-letter-repartition`, read by the tasks of sub-topology 1. */
  private val byLetter: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupBy(Repartition("by-letter"), text, text)((_, value) => value.take(1))
        .count(Named("count-3"), Store("counts")).toStream(Named("to-stream-2")).to(Named("write-4"), Topic.from("out").orFail, text, Serde.long[IO])
    ).orFail

  /** Waits a second over each value, then writes it unchanged. */
  private val slow: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).evalMapValues(Named("eval-map-values-1"))(value => IO.sleep(1.second).as(value))
        .to(Named("write-5"), Topic.from("out").orFail, text, text)
    ).orFail

  /** Counts each key's records in tumbling windows of 10 s with 5 s of grace, in the window store `windows`. */
  private val windowing: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .count(Named("count-4"), Store("windows")).toStream(Named("to-stream-3"))
        .to(Named("write-6"), Topic.from("out").orFail, Serde.timeWindowed(text, 10.seconds), Serde.long[IO])
    ).orFail

  /** Counts as `windowing` does, keeping windows a minute and emitting each once it closes. */
  private val emitting: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .withRetention(1.minute).emitOnWindowClose.count(Named("count-5"), Store("windows")).toStream(Named("to-stream-4"))
        .to(Named("write-7"), Topic.from("out").orFail, Serde.timeWindowed(text, 10.seconds), Serde.long[IO])
    ).orFail

  /** Counts as `windowing` does, passing on each window's final result once it closes. */
  private val suppressing: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .count(Named("count-6"), Store("windows")).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-5"))
        .to(Named("write-8"), Topic.from("out").orFail, Serde.timeWindowed(text, 10.seconds), Serde.long[IO])
    ).orFail

  /** Joins each order with its user. */
  private val joining: Topology[IO] =
    Topology.of(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text)
        .join(Named("join-users"), KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users")))((o, u) => s"$o:$u")
        .to(Named("write-9"), Topic.from("out").orFail, text, text)
    ).orFail

  /** Counts each key's records in sessions with a gap of 5 s and 5 s of grace, in the session store `sessions`. */
  private val sessioning: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).windowedBy(SessionWindows(5.seconds, grace = 5.seconds))
        .count(Named("count-7"), Store("sessions")).toStream(Named("to-stream-6"))
        .to(Named("write-10"), Topic.from("out").orFail, Serde.sessionWindowed(text), Serde.long[IO].option)
    ).orFail

  private def session(key: String, start: Long, end: Long): Chunk[Byte] = SessionStore.key(Chunk.array(key.getBytes), start, end)

  private def counted(n: Long): Stored = Stored(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(n).array), 0L)

  private def window(key: String, start: Long): Chunk[Byte] = WindowStore.key(Chunk.array(key.getBytes), start)

  private val counting: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).count(Named("count-8"), Store("counts"))
        .toStream(Named("to-stream-7")).to(Named("write-11"), Topic.from("out").orFail, text, Serde.long[IO])
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
    case Followed(starts: Vector[StartAt])

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
      purging: Ref[IO, IO[Unit]],
      reported: SignallingRef[IO, Member],
      restoring: Ref[IO, FiniteDuration],
      val states: Ref[IO, List[StreamsState]],
      standingBy: Queue[IO, PolledRecord],
      ending: Ref[IO, Map[TopicPartition, Long]],
      val restoredFrom: Ref[IO, List[(Map[TopicPartition, Long], Map[String, StoreContents])]],
      offsetsRead: Ref[IO, IO[TaskOffsets]],
      val restoredAlso: Ref[IO, Map[String, StoreContents]]
  ):
    /** Changelog records for the standby tasks to follow. */
    def changelogs(polled: PolledRecord*): IO[Unit] = polled.toList.traverse_(standingBy.offer)

    /** Where changelog partitions end. */
    def endsAt(ends: Map[TopicPartition, Long]): IO[Unit] = ending.set(ends)

    /** The task offsets the engine gives its member to send. */
    def taskOffsets: IO[TaskOffsets] = offsetsRead.get.flatten

    /** Changes what the member reports about the group. */
    def report(change: Member => Member): IO[Unit] = reported.update(change)

    /** Makes each restore take this long. */
    def restoreFor(time: FiniteDuration): IO[Unit] = restoring.set(time)

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
        def reports: Signal[IO, Member]                                                           = reported
        def offsetsFrom(read: IO[TaskOffsets]): IO[Unit]                                          = offsetsRead.set(read)
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
        def restore(
            changelogs: Map[TopicPartition, String],
            from: Map[TopicPartition, Long],
            held: Map[String, StoreContents]
        ): IO[Map[String, StoreContents]] =
          restoring.get.flatMap(IO.sleep) *> logged(Event.Restored(changelogs)) *> restoredFrom.update(_ :+ (from, held)) *>
            restoredAlso.get.map(held ++ _)
        def follow(starts: Vector[StartAt]): IO[Unit] = logged(Event.Followed(starts))
        def followed: IO[List[PolledRecord]]          =
          standingBy.tryTakeN(None).flatMap(taken => if taken.isEmpty then IO.sleep(Engine.PollMillis.millis).as(Nil) else IO.pure(taken))
        def ends(partitions: Vector[TopicPartition]): IO[Map[TopicPartition, Long]] = ending.get.map(_.view.filterKeys(partitions.toSet).toMap)
        def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): IO[Unit] =
          val values = records.toList.flatMap(_.value).map(value => new String(value.toArray))
          written.update(_ ++ records.toList) *> now.flatMap(at => wroteAt.update(_ :+ (at, values)))
        def purge(before: Map[TopicPartition, Offset]): IO[Unit] = logged(Event.Purged(before)) *> purging.get.flatten

  private val identity = MemberIdentity("app", "member", "process", StreamsGroup.Topology(0, Nil), 300000, None, Nil, None)

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
        IO.ref(IO.unit),
        SignallingRef[IO].of(Member(identity)),
        IO.ref(Duration.Zero: FiniteDuration),
        IO.ref(List.empty[StreamsState]),
        Queue.unbounded[IO, PolledRecord],
        IO.ref(Map.empty[TopicPartition, Long]),
        IO.ref(List.empty[(Map[TopicPartition, Long], Map[String, StoreContents])]),
        IO.ref(IO.raiseError[TaskOffsets](new AssertionError("the engine gave no task offsets"))),
        IO.ref(Map.empty[String, StoreContents])
      ).mapN(new World(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _))

  /** Runs a program's engine, the counting one unless another is given, with a commit interval of a second over a member answering as given, while
    * `script` runs: at least once, or exactly once with transactions that fail as `failing` says, running up to `concurrency` tasks at once.
    */
  def run(
      committed: Int => IO[OffsetFetchRequest.Answer] = _ => IO.pure(OffsetFetchRequest.Answer(Nil, 0)),
      commit: (Int, List[OffsetCommitRequest.Topic]) => IO[List[OffsetCommitRequest.Answer]] =
        (_, topics) => IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, 0)))),
      failing: Option[Int => Option[Throwable]] = None,
      topology: Topology[IO] = counting,
      concurrency: Int = 1,
      open: TaskStore.Open[IO] = TaskStore.memory[IO]
  )(script: World => IO[Unit]): IO[Unit] =
    val program =
      World().flatMap: world =>
        val transactions = failing.map(world.transactions)
        val member       = world.member(committed, commit)
        Engine.running("app", topology, member, world.plane, transactions, 1.second, concurrency, PurgeInterval, CloseTimeout, _ => IO.pure(4), open)
          .use: running =>
            running.run.attempt.flatMap(world.finished).start *> script(world)
    TestControl.executeEmbed(program.timeout(15.minutes))
