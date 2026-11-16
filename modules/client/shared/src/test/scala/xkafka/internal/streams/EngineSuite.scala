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
import xkafka.Serde
import xkafka.streams.{
  BufferLimit, GlobalKTable, KafkaStreams, KStream, KTable, Named, QueryableStore, QueryProblem, Repartition, SessionWindows, Store, StreamsPhase,
  StreamsProblem, StreamsSettings, StreamsState, Suppressed, TimeWindows, Topology, Windowed
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
        _ <- world.tell(MemberAction.Assign(1, Assignment(Set(TaskId("0", 0), TaskId("0", 1)), Set.empty, Set.empty), 1))
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
    val answers = List(14, 16, 0).map(code => List(OffsetCommitRequest.Answer("in", 0, code.toShort)))
    run(commit = (n, _) => IO.pure(answers(n))): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis) *>
        world.committedAt.get.map(times => assertEquals(times.map(_ - times.head), List(0L, 100L, 300L)))

  test("commits and transactions carry the epoch the member gave the tasks at, not the member's epoch when they are sent"):
    val at =
      (world: World) =>
        // The fake member's own epoch is 1.
        world.tell(MemberAction.Assign(1, Assignment(Set(TaskId("0", 0)), Set.empty, Set.empty), 3)) *> IO.sleep(100.millis) *>
          world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis) *> world.epochs.get.map(epochs => assertEquals(epochs.distinct, List(3)))
    run()(at) *> run(failing = Some(_ => None))(at)

  test("a commit refused as STALE_MEMBER_EPOCH, for a task the member no longer has at that epoch, is dropped, and the engine goes on"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 113)))): world =>
      for
        _         <- world.tell(assign(TaskId("0", 0)))
        _         <- IO.sleep(100.millis)
        _         <- world.records(record(0, 5L, "k", 1000L))
        _         <- IO.sleep(3.seconds)
        committed <- world.committedAt.get
        ended     <- world.ended
      yield
        assertEquals(committed.size, 1)
        assertEquals(ended, None)

  test("a commit of tasks handed back that the coordinator is never ready for gives the tasks up, and the engine answers the revocation and goes on"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 14))), taskTimeout = 1.second): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis)
        _     <- world.records(record(0, 0L, "k", 1000L)) *> IO.sleep(100.millis)
        _     <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 0)))) *> IO.sleep(3.seconds)
        log   <- world.log.get
        ended <- world.ended
      yield
        assertEquals(ended, None)
        assert(log.contains(Event.Revoked(2)), log)

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

  test("a commit the coordinator is never ready for stops the engine once the task timeout has passed, with the last refusal"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 14))), taskTimeout = 10.seconds): world =>
      for
        _      <- world.tell(assign(TaskId("0", 0)))
        _      <- IO.sleep(100.millis)
        _      <- world.records(record(0, 5L, "k", 1000L))
        _      <- IO.sleep(10.seconds)
        before <- world.ended
        _      <- IO.sleep(2.seconds)
        after  <- world.ended
      yield
        assertEquals(before, None, "the commit started a second after the record, so its timeout has not passed")
        assertEquals(
          after.flatMap(_.left.toOption).collect { case error: KafkaException.BackendFailure => (error.code, error.retriable) },
          Some((Some(ErrorCode.CoordinatorLoadInProgress), Some(true)))
        )

  test("a fetch of the group's offsets that keeps failing in a way worth retrying stops the engine with that failure after the task timeout"):
    val unavailable = new KafkaException.BackendFailure("coordinator unreachable", Some(ErrorCode.CoordinatorNotAvailable), Some(true))
    run(committed = _ => IO.raiseError(unavailable), taskTimeout = 5.seconds): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(6.seconds) *> world.ended.map(ended => assertEquals(ended, Some(Left(unavailable))))

  test("with a task timeout of zero, a commit the coordinator is not ready for stops the engine at once"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 14))), taskTimeout = Duration.Zero): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1050.millis) *>
        world.committedAt.get.map(times => assertEquals(times.size, 1)) *> world.ended.map(ended => assert(ended.exists(_.isLeft), ended))

  test("a refusal that stops the engine says it is worth retrying exactly when its code does"):
    def ended(code: Short) =
      run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, code))), taskTimeout = 2.seconds): world =>
        world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(5.seconds) *>
          world.ended.map: ended =>
            assertEquals(
              ended.flatMap(_.left.toOption).collect { case error: KafkaException.BackendFailure => (error.code, error.retriable) },
              Some((Some(ErrorCode.fromProtocol(code.toInt)), Some(ErrorCode.fromProtocol(code.toInt).retriable))),
              s"code $code"
            )
    // REBALANCE_IN_PROGRESS stops a commit at once though its code is worth retrying; UNSTABLE_OFFSET_COMMIT is asked again until the timeout
    // though its code is not.
    ended(27) *> ended(88)

  test("any other refusal of a commit stops the engine with it"):
    run(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 29)))): world =>
      world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis) *>
        world.ended.map: ended =>
          assertEquals(
            ended.flatMap(_.left.toOption).collect { case error: KafkaException.BackendFailure => error.code },
            Some(Some(ErrorCode.TopicAuthorizationFailed))
          )

  test("an engine stopped by a failure abandons its tasks as it is released, without waiting for them to be given back"):
    val program =
      World().flatMap: world =>
        // A member that, once asked to leave, would wait for the stopped engine to give back its tasks.
        val member = world.member(commit = (_, _) => IO.pure(List(OffsetCommitRequest.Answer("in", 0, 29))), leave = IO.never)
        val run    =
          Engine.running("app", counting, member, world.plane, None, 1.second, 1, PurgeInterval, CloseTimeout, _ => IO.pure(4), TaskStore.memory[IO])
            .use: running =>
              world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis) *> world.records(record(0, 5L, "k", 1000L)) *> running.run.attempt
        run.flatMap(stopped => (IO.monotonic, world.log.get).tupled.map((at, log) => (stopped, at, log)))
    TestControl.executeEmbed(program.timeout(1.minute)).map: (stopped, at, log) =>
      assert(stopped.isLeft, stopped)
      assert(log.contains(Event.Abandon), log)
      assert(at < CloseTimeout, s"released at $at")

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
    (_, time, held) =>
      TaskStore.inMemory[IO](held, time).map: store =>
        new TaskStore[IO]:
          def get(key: Chunk[Byte]): IO[Option[Stored]]                                                    = store.get(key)
          def range(from: Option[Chunk[Byte]], to: Option[Chunk[Byte]]): IO[Vector[(Chunk[Byte], Stored)]] = store.range(from, to)
          def timed(from: Long, to: Long): IO[Vector[(Chunk[Byte], Stored)]]                               = store.timed(from, to)
          def clock: IO[Long]                                                                              = store.clock
          def close: IO[Unit]                                                                              = store.close
          def clear: IO[Unit]                                                                              = store.clear
          def write(changes: StoreChanges, positions: Map[Int, Long]): IO[Unit] = batches.update(_ :+ changes) *> store.write(changes, positions)
          def positions: IO[Map[Int, Long]]                                     = store.positions

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

  test("exactly once, after a refused transaction, offsets the group refuses to read until the member has joined again are asked for again"):
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    // The coordinator answers UNKNOWN_MEMBER_ID, then FENCED_MEMBER_EPOCH, for a member it has not taken back yet.
    val answers = Map(1 -> OffsetFetchRequest.Answer(Nil, 25), 2 -> OffsetFetchRequest.Answer(Nil, 110))
    val refused = new KafkaException.BackendFailure("refused", Some(ErrorCode.UnknownMemberId), Some(false))
    run(committed = n => IO.pure(answers.getOrElse(n, committed)), failing = Some(n => Option.when(n == 0)(refused))): world =>
      for
        _       <- world.tell(assign(TaskId("0", 0)))
        _       <- IO.sleep(100.millis)
        _       <- world.records(record(0, 5L, "k", 1000L))
        _       <- IO.sleep(3.seconds)
        fetches <- world.fetches.get
        ended   <- world.ended
      yield
        assertEquals(fetches.size, 4)
        assertEquals(ended, None)

  /** Exactly once, a transaction failing as `failure` says the first time: the fetches of committed offsets, the producers made anew, and the end. */
  private def failedOnce(failure: Throwable, taskTimeout: FiniteDuration = StreamsSettings.DefaultTaskTimeout, always: Boolean = false) =
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    IO.ref(Option.empty[(Int, Int, Option[Either[Throwable, Unit]])]).flatMap: outcome =>
      run(committed = _ => IO.pure(committed), failing = Some(n => Option.when(always || n == 0)(failure)), taskTimeout = taskTimeout): world =>
        for
          _ <- world.tell(assign(TaskId("0", 0)))
          _ <- IO.sleep(100.millis)
          _ <- world.records(record(0, 5L, "k", 1000L))
          // Where every transaction fails, records keep coming, so the tasks keep trying until the task timeout has passed.
          _       <- if always then (IO.sleep(300.millis) *> world.records(record(0, 5L, "k", 1000L))).replicateA_(8) else IO.sleep(2500.millis)
          fetches <- world.fetches.get
          log     <- world.log.get
          ended   <- world.ended
          _       <- outcome.set(Some((fetches.size, log.count(_ == Event.Renewed), ended)))
        yield ()
      *> outcome.get.map(_.get)

  test("exactly once, a transaction that fails in a way Java recovers from starts the tasks again from what the group committed, and goes on"):
    failedOnce(new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))).map:
      (fetches, renewed, ended) => assertEquals((fetches, renewed, ended), (2, 0, None))

  test("exactly once, a producer fenced by another with its transactional id is made anew, and the tasks start again from what the group committed"):
    failedOnce(new KafkaException.BackendFailure("fenced", Some(ErrorCode.Fenced), Some(false), Some(true))).map: (fetches, renewed, ended) =>
      assertEquals((fetches, renewed, ended), (2, 1, None))

  test("exactly once, a producer refused for its epoch or an expired producer id is made anew, as Java takes either as its tasks having moved"):
    val epoch   = new KafkaException.BackendFailure("epoch", Some(ErrorCode.InvalidProducerEpoch), Some(false), Some(false))
    val expired = new KafkaException.BackendFailure("expired", Some(ErrorCode.Other(49)), Some(false), Some(true))
    failedOnce(epoch).map((fetches, renewed, ended) => assertEquals((fetches, renewed, ended), (2, 1, None))) *> failedOnce(expired)
      .map((fetches, renewed, ended) => assertEquals((fetches, renewed, ended), (2, 1, None)))

  test("exactly once, a failure recovered from does not count against tasks the member is given after it"):
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0), Fetched("in", 1, 5L, -1, None, 0)), 0)
    val timedOut  = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    // The first transaction fails, the one finishing it succeeds, and the next fails too, after the task timeout.
    run(committed = _ => IO.pure(committed), failing = Some(n => Option.when(n == 0 || n == 2)(timedOut)), taskTimeout = 1.second): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0)))
        _     <- IO.sleep(100.millis)
        _     <- world.records(record(0, 5L, "k", 1000L))
        _     <- IO.sleep(500.millis)
        _     <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
        _     <- IO.sleep(2.seconds)
        _     <- world.records(record(1, 5L, "k", 1000L))
        _     <- IO.sleep(500.millis)
        ended <- world.ended
      yield assertEquals(ended, None)

  test("exactly once, a failure that is neither retriable nor fatal stops the engine at once, as Java stops for one"):
    val tooLarge = new KafkaException.BackendFailure("too large", Some(ErrorCode.MessageTooLarge), Some(false), Some(false))
    failedOnce(tooLarge).map((fetches, _, ended) => assertEquals((fetches, ended), (1, Some(Left(tooLarge)))))

  test("at least once, a write that fails in a way worth retrying starts the tasks again from what the group committed, and the engine goes on"):
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    val timedOut  = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(committed = _ => IO.pure(committed)): world =>
      for
        _       <- world.tell(assign(TaskId("0", 0)))
        _       <- IO.sleep(100.millis)
        _       <- world.failWrite(timedOut)
        _       <- world.records(record(0, 5L, "k", 1000L))
        _       <- IO.sleep(1500.millis)
        fetches <- world.fetches.get
        ended   <- world.ended
      yield assertEquals((fetches.size, ended), (2, None))

  test("at least once, records processed between two failures end the first one's time, though no commit came between, as Java clears it"):
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    val timedOut  = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(committed = _ => IO.pure(committed), taskTimeout = 500.millis): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis)
        _     <- world.failWrite(timedOut) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(100.millis)
        _     <- (6L to 12L).toList.traverse_(offset => world.records(record(0, offset, "k", 1000L)) *> IO.sleep(100.millis))
        _     <- world.failWrite(timedOut) *> world.records(record(0, 13L, "k", 1000L)) *> IO.sleep(200.millis)
        ended <- world.ended
      yield assertEquals(ended, None)

  test("at least once, a write that fails for every record stops the engine after the task timeout, though commits of nothing come between"):
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    val timedOut  = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(committed = _ => IO.pure(committed), taskTimeout = 2.seconds): world =>
      for
        _     <- world.tell(assign(TaskId("0", 0))) *> IO.sleep(100.millis)
        _     <- world.failWrites(timedOut)
        _     <- (0 until 5).toList.traverse_(_ => world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(1500.millis))
        ended <- world.ended
      yield assertEquals(ended, Some(Left(timedOut)))

  test("at least once, a failure recovered from counts no longer once the tasks commit again, so a later one starts its own task timeout"):
    val committed = OffsetFetchRequest.Answer(List(Fetched("in", 0, 5L, -1, None, 0)), 0)
    val timedOut  = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(committed = _ => IO.pure(committed), taskTimeout = 1.second): world =>
      for
        _ <- world.tell(assign(TaskId("0", 0)))
        _ <- IO.sleep(100.millis)
        _ <- world.failWrite(timedOut) *> world.records(record(0, 5L, "k", 1000L)) *> IO.sleep(500.millis)
        // Processed well for longer than the task timeout, then a failure of its own.
        _       <- (6L to 9L).toList.traverse_(offset => world.records(record(0, offset, "k", 1000L)) *> IO.sleep(500.millis))
        _       <- world.failWrite(timedOut) *> world.records(record(0, 10L, "k", 1000L)) *> IO.sleep(500.millis)
        fetches <- world.fetches.get
        ended   <- world.ended
      yield assertEquals((fetches.size, ended), (3, None))

  test("exactly once, a fatal failure of a transaction stops the engine with it, and one Java recovers from does after the task timeout"):
    val fatal = new KafkaException.BackendFailure("not authorized", Some(ErrorCode.Other(53)), Some(false), Some(true))
    val again = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    failedOnce(fatal).map((_, _, ended) => assertEquals(ended, Some(Left(fatal)))) *> failedOnce(again, taskTimeout = 1.second, always = true)
      .map((_, _, ended) => assertEquals(ended, Some(Left(again))))

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

  test("at least once, a commit dropped as the member no longer counts or no longer has the task purges nothing, since the group did not take it"):
    List[Short](25, 110, 113).traverse_ : code =>
      val refused =
        (_: Int, topics: List[OffsetCommitRequest.Topic]) =>
          IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, code))))
      run(topology = byLetter, commit = refused)(world => purges(world, 5).map(made => assertEquals(made, Nil, s"code $code")))

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

  test("a standby whose store holds a position outside its changelog, as after the topic was made anew, is emptied and follows it from the start"):
    val stale = StoreContents.unchanged.updated(Chunk.array("gone".getBytes), Some(Stored(Chunk.array(Array[Byte](1)), 1L)))
    MemoryStateDirectory(Map(("0_1", "counts") -> (StoreContents.empty.after(stale), Map(1 -> 50L)))).flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _   <- world.endsAt(Map(changelog(1) -> 10L))
          _   <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
          _   <- IO.sleep(500.millis)
          log <- world.log.get
        yield assertEquals(log.collect { case followed: Event.Followed => followed }, List(Event.Followed(Vector(StartAt(changelog(1), 0L, -1)))))

  test("a standby whose store holds a position waits for its changelog's bounds while the changelog's leader moves, and the engine goes on"):
    MemoryStateDirectory(Map(("0_1", "counts") -> (StoreContents.empty, Map(1 -> 5L)))).flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _     <- world.endsAt(Map(changelog(1) -> 10L)) *> world.failEnds(2)
          _     <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
          _     <- IO.sleep(1.second)
          log   <- world.log.get
          ended <- world.ended
        yield
          assertEquals(ended, None)
          assertEquals(log.collect { case followed: Event.Followed => followed }, List(Event.Followed(Vector(StartAt(changelog(1), 5L, -1)))))

  test("a standby task follows its changelog from the start, and where it is and where the changelog ends are its task offsets"):
    run(): world =>
      for
        _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
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
        _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _       <- IO.sleep(500.millis)
        _       <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
        _       <- IO.sleep(500.millis)
        _       <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
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
        _    <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(logged("k", 1L, 0L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
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

  test(
    "at least once, a write of a suppression's buffer at a commit that fails in a way worth retrying starts the tasks again, and the engine goes on"
  ):
    val timedOut = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(topology = suppressing): world =>
      for
        _ <- world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis)
        _ <- world.records(record(1, 0L, "k", 1000L)) *> IO.sleep(300.millis)
        // Processed, the buffer's change waits for the commit, whose write of it is the next.
        _       <- world.failWrite(timedOut) *> IO.sleep(1500.millis)
        fetches <- world.fetches.get
        ended   <- world.ended
      yield assertEquals((fetches.size, ended), (2, None))

  test(
    "at least once, a write of a suppression's buffer that fails at every commit stops the engine once the task timeout has passed, though records are processed between"
  ):
    val timedOut = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(topology = suppressing, taskTimeout = 1.second): world =>
      for
        _     <- world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis)
        _     <- world.failWrites(timedOut)
        _     <- (0L until 20L).toList.traverse_(offset => world.records(record(1, offset, s"k$offset", 1000L)) *> IO.sleep(150.millis))
        ended <- world.ended
      yield assertEquals(ended, Some(Left(timedOut)))

  test("a write of a suppression's buffer that fails in a way worth retrying as its task is handed back gives the task up, and the engine goes on"):
    val timedOut = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(topology = suppressing): world =>
      for
        _     <- world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis)
        _     <- world.records(record(1, 0L, "k", 1000L)) *> IO.sleep(50.millis)
        _     <- world.failWrite(timedOut) *> world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1)))) *> IO.sleep(500.millis)
        log   <- world.log.get
        ended <- world.ended
      yield
        assertEquals(ended, None)
        assert(log.contains(Event.Revoked(2)), log)

  test(
    "at least once, a suppression's buffer that cannot be written at any commit stops the engine after the task timeout, though records processed between are written"
  ):
    val timedOut = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    run(topology = suppressing, taskTimeout = 2.seconds): world =>
      for
        _     <- world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis)
        _     <- world.failWritesTo(_.endsWith("final-store-changelog"), timedOut)
        _     <- (0L until 60L).toList.traverse_(offset => world.records(record(1, offset, s"k$offset", 1000L)) *> IO.sleep(100.millis))
        ended <- world.ended
      yield assertEquals(ended, Some(Left(timedOut)))

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
          .to(Named("write-3"), Topic.from("out").orFail, Windowed.timeSerde(text, 10.seconds), Serde.long[IO])
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
        _ <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _ <- IO.sleep(500.millis)
        // By its end, which a session store expires it by, [0, 25000] is not expired; read as a window's start, it would be.
        _    <- world.changelogs(sessionLogged("k", 0L, 25000L, 0L), sessionLogged("gone", 0L, 5000L, 1L), sessionLogged("other", 30000L, 30000L, 2L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
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
        _    <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(windowLogged("k", 0L, 0L), windowLogged("other", 30000L, 1L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
        _    <- IO.sleep(500.millis)
        from <- world.restoredFrom.get
      yield assertEquals(from.map(_._2("windows").keySet), List(Set(window("other", 30000L))))

  test("a removal a standby window store reads counts towards its clock, so what it expired is gone though the removed window left nothing"):
    def windowLogged(key: String, start: Long, offset: Long, value: Option[Chunk[Byte]]) =
      PolledRecord("app-windows-changelog", 1, offset, Some(start), 1, -1, Some(window(key, start)), value, Headers.empty)
    run(topology = windowing): world =>
      for
        _    <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(windowLogged("k", 0L, 0L, Some(counted(1L).value)), windowLogged("other", 30000L, 1L, None))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
        _    <- IO.sleep(500.millis)
        from <- world.restoredFrom.get
      // The plane records only the stores holding something.
      yield assertEquals(from.map(_._2.get("windows").fold(Set.empty[Chunk[Byte]])(_.keySet)), List(Set.empty[Chunk[Byte]]))

  /** Stands by for task 0_1, reads two of its changelog records, takes the standby away, then after `later` makes the task active. */
  private def droppedThenActive(world: World, later: FiniteDuration): IO[(List[Map[TopicPartition, Long]], TaskOffsets)] =
    for
      _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
      _       <- IO.sleep(500.millis)
      _       <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
      _       <- IO.sleep(500.millis)
      _       <- world.tell(MemberAction.Assign(2, Assignment(Set.empty, Set.empty, Set.empty), 1))
      _       <- IO.sleep(500.millis)
      offsets <- world.taskOffsets
      _       <- IO.sleep(later)
      _       <- world.tell(MemberAction.Assign(3, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
      _       <- IO.sleep(500.millis)
      from    <- world.restoredFrom.get
    yield (from.map(_._1), offsets)

  test("with a state directory, a task given up after its commit keeps its stores at its changelog's end, and back again restores from there"):
    MemoryStateDirectory().flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _       <- world.endsAt(Map(changelog(1) -> 7L))
          _       <- world.tell(assign(TaskId("0", 1)))
          _       <- IO.sleep(100.millis)
          _       <- world.records(record(1, 0L, "k", 1000L))
          _       <- IO.sleep(200.millis)
          _       <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1))))
          _       <- IO.sleep(200.millis)
          offsets <- world.taskOffsets
          gaveUp  <- directory.held
          _       <- world.tell(MemberAction.Assign(3, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
          _       <- IO.sleep(200.millis)
          from    <- world.restoredFrom.get
        yield
          assertEquals(gaveUp, Map("0_1" -> Map("counts" -> 7L)))
          assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 7L)))
          assertEquals(from.map(_._1), List(Map.empty[TopicPartition, Long], Map(changelog(1) -> 7L)))
          assertEquals(from.map(_._2.get("counts").map(_.keySet)), List(None, Some(Set(Chunk.array("k".getBytes)))))

  test("with a state directory, a task given up while its changelog's leader moves waits for the end, and the engine goes on"):
    MemoryStateDirectory().flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _      <- world.endsAt(Map(changelog(1) -> 7L))
          _      <- world.tell(assign(TaskId("0", 1)))
          _      <- IO.sleep(100.millis)
          _      <- world.records(record(1, 0L, "k", 1000L))
          _      <- IO.sleep(200.millis)
          _      <- world.failEnds(2)
          _      <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1))))
          _      <- IO.sleep(1.second)
          gaveUp <- directory.held
          ended  <- world.ended
        yield
          assertEquals(ended, None)
          assertEquals(gaveUp, Map("0_1" -> Map("counts" -> 7L)))

  test(
    "with a state directory, a task given up after a commit the group refused does not take its changelog's end, which the next owner may write to"
  ):
    val refused =
      (_: Int, topics: List[OffsetCommitRequest.Topic]) =>
        IO.pure(topics.flatMap(topic => topic.partitions.map(p => OffsetCommitRequest.Answer(topic.name, p.index, 25.toShort))))
    MemoryStateDirectory().flatMap: directory =>
      run(state = Some(directory), commit = refused): world =>
        for
          _      <- world.endsAt(Map(changelog(1) -> 7L))
          _      <- world.tell(assign(TaskId("0", 1)))
          _      <- IO.sleep(100.millis)
          _      <- world.records(record(1, 0L, "k", 1000L))
          _      <- IO.sleep(200.millis)
          _      <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1))))
          _      <- IO.sleep(200.millis)
          gaveUp <- directory.held
        yield assert(!gaveUp.get("0_1").exists(_.get("counts").contains(7L)), gaveUp)

  test("with a state directory, a standby records with each batch how far it has read, kept when it is taken away, and restored from there"):
    MemoryStateDirectory().flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _      <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
          _      <- IO.sleep(500.millis)
          _      <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
          _      <- IO.sleep(500.millis)
          _      <- world.tell(MemberAction.Assign(2, Assignment(Set.empty, Set.empty, Set.empty), 1))
          _      <- IO.sleep(500.millis)
          gaveUp <- directory.held
          _      <- world.tell(MemberAction.Assign(3, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
          _      <- IO.sleep(500.millis)
          from   <- world.restoredFrom.get
        yield
          assertEquals(gaveUp, Map("0_1" -> Map("counts" -> 2L)))
          assertEquals(from.map(_._1), List(Map(changelog(1) -> 2L)))

  test("with a state directory, a standby the member loses with all its tasks is closed and kept as given up, and restored from there"):
    MemoryStateDirectory().flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _      <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
          _      <- IO.sleep(500.millis)
          _      <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
          _      <- IO.sleep(500.millis)
          _      <- world.tell(MemberAction.LoseAll)
          _      <- IO.sleep(500.millis)
          gaveUp <- directory.held
          _      <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
          _      <- IO.sleep(500.millis)
          from   <- world.restoredFrom.get
          opened <- directory.opened.get
        yield
          assertEquals(gaveUp, Map("0_1" -> Map("counts" -> 2L)))
          assertEquals(from.map(_._1), List(Map(changelog(1) -> 2L)))
          assertEquals(opened.count(_ == ("0_1", "counts")), 2, "opened as a standby, then again once closed")

  test("tasks an earlier run gave up are reported in task offsets from the start, and restored from where their stores were"):
    val held = Map(("0_1", "counts") -> (StoreContents.of(Map(Chunk.array("k".getBytes) -> counted(3L))), Map(1 -> 4L)))
    MemoryStateDirectory(held, Map("0_1" -> Map("counts" -> 4L), "9_0" -> Map("gone" -> 1L))).flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _       <- IO.sleep(100.millis)
          offsets <- world.taskOffsets
          _       <- world.tell(assign(TaskId("0", 1)))
          _       <- IO.sleep(200.millis)
          from    <- world.restoredFrom.get
        yield
          assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 4L)))
          assertEquals(from.map(_._1), List(Map(changelog(1) -> 4L)))

  test("with a state directory, an active task the member loses with all its tasks is still reported in task offsets, as its stores stay on disk"):
    val held = Map(("0_1", "counts") -> (StoreContents.of(Map(Chunk.array("k".getBytes) -> counted(3L))), Map(1 -> 4L)))
    MemoryStateDirectory(held, Map("0_1" -> Map("counts" -> 4L))).flatMap: directory =>
      run(state = Some(directory)): world =>
        for
          _       <- world.tell(assign(TaskId("0", 1)))
          _       <- IO.sleep(200.millis)
          _       <- world.tell(MemberAction.LoseAll)
          _       <- IO.sleep(200.millis)
          offsets <- world.taskOffsets
        yield assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 4L)))

  test("a store kept in memory, and a store with no position, are not taken from the state directory"):
    val inMemory =
      KStream(Named("read"), Topic.from("in").orFail, text, text).groupByKey(text, text).count(Named("count"), Store("counts").inMemory)
        .toStream(Named("to-stream")).to(Named("write"), Topic.from("out").orFail, text, Serde.long[IO])
    MemoryStateDirectory().flatMap: directory =>
      run(topology = Topology.of(inMemory).orFail, state = Some(directory)): world =>
        (world.tell(assign(TaskId("0", 1))) *> IO.sleep(200.millis) *> directory.opened.get).map(opened => assertEquals(opened, Nil))

  test("with a state directory, a task given up whose stores are all kept in memory is not reported in task offsets, as nothing of it stays"):
    val inMemory =
      KStream(Named("read"), Topic.from("in").orFail, text, text).groupByKey(text, text).count(Named("count"), Store("counts").inMemory)
        .toStream(Named("to-stream")).to(Named("write"), Topic.from("out").orFail, text, Serde.long[IO])
    MemoryStateDirectory().flatMap: directory =>
      run(topology = Topology.of(inMemory).orFail, state = Some(directory)): world =>
        for
          _       <- world.endsAt(Map(changelog(1) -> 7L))
          _       <- world.tell(assign(TaskId("0", 1))) *> IO.sleep(100.millis)
          _       <- world.records(record(1, 0L, "k", 1000L)) *> IO.sleep(200.millis)
          _       <- world.tell(MemberAction.Revoke(2, Set(TaskId("0", 1)))) *> IO.sleep(200.millis)
          offsets <- world.taskOffsets
          held    <- directory.held
        yield
          assertEquals(offsets.positions, Nil)
          assertEquals(held, Map.empty)

  test(
    "with a state directory, a standby taken away whose stores are all kept in memory is not kept, as its stores are closed with nothing of it left"
  ):
    val inMemory =
      KStream(Named("read"), Topic.from("in").orFail, text, text).groupByKey(text, text).count(Named("count"), Store("counts").inMemory)
        .toStream(Named("to-stream")).to(Named("write"), Topic.from("out").orFail, text, Serde.long[IO])
    MemoryStateDirectory().flatMap: directory =>
      run(topology = Topology.of(inMemory).orFail, state = Some(directory)): world =>
        for
          _       <- world.endsAt(Map(changelog(1) -> 7L))
          _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1)) *> IO.sleep(500.millis)
          standby <- world.taskOffsets.map(_.positions.map(_.partition))
          _       <- world.tell(MemberAction.Assign(2, Assignment(Set.empty, Set.empty, Set.empty), 1))
          // Waited for, as the engine handles the change in its own time; a standby kept would be reported for good.
          left <- (IO.sleep(20.millis) *> world.taskOffsets.map(_.positions)).iterateUntil(_.isEmpty).timeout(5.seconds).attempt
        yield
          assertEquals(standby, List(1))
          assert(left.isRight, left)

  test("a standby taken away keeps its store, reported in task offsets, and its task made active soon after restores only what it had not read"):
    run(): world =>
      droppedThenActive(world, 1.minute).map: (from, offsets) =>
        assertEquals(from, List(Map(changelog(1) -> 2L)))
        assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 2L)))

  test("a changelog record read for a standby just before the last one went is not applied when its task stands by again from the start"):
    run(): world =>
      for
        _ <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _ <- IO.sleep(500.millis)
        // While the member restores an active task, and so no longer stands by, a record for the standby is still read.
        _ <- world.restoreFor(2.seconds)
        _ <- world.tell(MemberAction.Assign(2, Assignment(Set(TaskId("0", 0)), Set.empty, Set.empty), 1))
        _ <- IO.sleep(500.millis)
        _ <- world.changelogs(logged("stale", 9L, 500L))
        _ <- IO.sleep(3.seconds)
        _ <- world.restoreFor(Duration.Zero)
        // Past the cleanup delay, the standby comes back from the start, and reads its changelog from there.
        _    <- IO.sleep(Engine.StateCleanupDelay + 1.minute)
        _    <- world.tell(MemberAction.Assign(3, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _    <- IO.sleep(500.millis)
        _    <- world.changelogs(logged("k", 1L, 0L), logged("k", 2L, 1L))
        _    <- IO.sleep(500.millis)
        _    <- world.tell(MemberAction.Assign(4, Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty), 1))
        _    <- IO.sleep(500.millis)
        from <- world.restoredFrom.get
      yield
        val (positions, held) = from.last
        assertEquals(positions, Map(changelog(1) -> 2L))
        assertEquals(held.get("counts").map(_.keySet), Some(Set(Chunk.array("k".getBytes))))

  test("a standby taken away longer ago than Java's cleanup delay is forgotten, and its task restores from the start"):
    run(): world =>
      droppedThenActive(world, Engine.StateCleanupDelay).map((from, _) => assertEquals(from, List(Map.empty[TopicPartition, Long])))

  test("lost tasks are no longer stood by, but their stores are kept and reported, as Java keeps a lost task's state directory"):
    run(): world =>
      for
        _       <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 1)), Set.empty), 1))
        _       <- IO.sleep(500.millis)
        _       <- world.tell(MemberAction.LoseAll)
        _       <- IO.sleep(500.millis)
        log     <- world.log.get
        offsets <- world.taskOffsets
      yield
        assertEquals(log.collect { case followed: Event.Followed => followed }.last, Event.Followed(Vector.empty))
        assertEquals(offsets.positions, List(StreamsGroup.TaskOffset("0", 1, 0L)))

  test("released, the engine closes every store though one fails to close, and fails with that failure"):
    val boom = new RuntimeException("boom")
    val run  =
      (World(), IO.ref(0)).tupled.flatMap: (world, closes) =>
        // The first store to close fails, and each counts its closes.
        val failing: TaskStore.Open[IO] =
          (name, time, held) =>
            TaskStore.memory[IO].apply(name, time, held).map: store =>
              new TaskStore[IO]:
                export store.{clear, clock, get, positions, range, timed}
                def write(changes: StoreChanges, positions: Map[Int, Long]): IO[Unit] = store.write(changes, positions)
                def close: IO[Unit] = closes.getAndUpdate(_ + 1).flatMap(before => IO.raiseWhen(before == 0)(boom))
        Engine.running("app", counting, world.member(), world.plane, None, 1.second, 1, PurgeInterval, CloseTimeout, _ => IO.pure(4), failing)
          .use(_ => world.tell(assign(TaskId("0", 0), TaskId("0", 1))) *> IO.sleep(500.millis)).attempt.product(closes.get)
    TestControl.executeEmbed(run.timeout(1.minute)).map: (released, closes) =>
      assertEquals(released, Left(boom))
      assertEquals(closes, 2)

  test("in memory, a standby no longer wanted has its stores closed once it has been kept past the cleanup delay"):
    val run =
      (World(), IO.ref(0)).tupled.flatMap: (world, closes) =>
        val counted: TaskStore.Open[IO] =
          (name, time, held) =>
            TaskStore.memory[IO].apply(name, time, held).map: store =>
              new TaskStore[IO]:
                export store.{clear, clock, get, positions, range, timed}
                def write(changes: StoreChanges, positions: Map[Int, Long]): IO[Unit] = store.write(changes, positions)
                def close: IO[Unit]                                                   = closes.update(_ + 1) *> store.close
        val none = Assignment(Set.empty, Set.empty, Set.empty)
        Engine.running("app", counting, world.member(), world.plane, None, 1.second, 1, PurgeInterval, CloseTimeout, _ => IO.pure(4), counted).use:
          _ =>
            for
              _      <- world.tell(MemberAction.Assign(1, Assignment(Set.empty, Set(TaskId("0", 0)), Set.empty), 1)) *> IO.sleep(500.millis)
              _      <- world.tell(MemberAction.Assign(2, none, 1)) *> IO.sleep(500.millis)
              kept   <- closes.get
              _      <- IO.sleep(11.minutes) *> world.tell(MemberAction.Assign(3, none, 1)) *> IO.sleep(500.millis)
              closed <- closes.get
            yield (kept, closed)
    TestControl.executeEmbed(run.timeout(1.hour)).map(counts => assertEquals(counts, (0, 1)))

  test("a query reads what a task has counted since it last committed, before the commit"):
    val key       = Chunk.array("k".getBytes)
    val partition = Murmur2.partition(key, 4)
    val task      = TaskId("0", partition)
    val counts    = QueryableStore.keyValue(Store("counts"), text, Serde.long[IO])
    World().flatMap: world =>
      Engine
        .running("app", counting, world.member(), world.plane, None, 1.hour, 1, PurgeInterval, CloseTimeout, _ => IO.pure(4), TaskStore.memory[IO])
        .use: streams =>
          for
            _       <- world.report(_.copy(owned = Assignment(Set(task), Set.empty, Set.empty)))
            _       <- world.tell(assign(task)) *> IO.sleep(200.millis)
            _       <- world.records(record(partition, 0L, "k", 1000L), record(partition, 1L, "k", 1000L)) *> IO.sleep(300.millis)
            counted <- streams.query(counts.get("k"))
            commits <- world.commits.get
          yield
            assertEquals(commits, Nil)
            assertEquals(counted, Right(Some(2L)))

  test("exactly once, while tasks restarted after a failed transaction restore their stores, a query answers that the group is rebalancing"):
    val key       = Chunk.array("k".getBytes)
    val partition = Murmur2.partition(key, 4)
    val task      = TaskId("0", partition)
    val counts    = QueryableStore.keyValue(Store("counts"), text, Serde.long[IO])
    val timedOut  = new KafkaException.BackendFailure("timed out", Some(ErrorCode.RequestTimedOut), Some(true), Some(false))
    World().flatMap: world =>
      val transactions = world.transactions(n => Option.when(n == 0)(timedOut))
      Engine.running(
        "app",
        counting,
        world.member(),
        world.plane,
        Some(transactions),
        100.millis,
        1,
        PurgeInterval,
        CloseTimeout,
        _ => IO.pure(4),
        TaskStore.memory[IO]
      ).use: streams =>
        for
          _      <- world.report(_.copy(owned = Assignment(Set(task), Set.empty, Set.empty)))
          _      <- world.tell(assign(task)) *> IO.sleep(200.millis)
          _      <- world.restoreFor(2.seconds)
          _      <- world.records(record(partition, 0L, "k", 1000L)) *> IO.sleep(500.millis)
          during <- streams.query(counts.get("k"))
        yield assertEquals(during, Left(QueryProblem.Rebalancing))

  test("tasks given up together are each closed though one fails to close"):
    val boom = new RuntimeException("boom")
    val run  =
      (World(), IO.ref(0)).tupled.flatMap: (world, closes) =>
        // The first store to close fails, and each counts its closes.
        val failing: TaskStore.Open[IO] =
          (name, time, held) =>
            TaskStore.memory[IO].apply(name, time, held).map: store =>
              new TaskStore[IO]:
                export store.{clear, clock, get, positions, range, timed}
                def write(changes: StoreChanges, positions: Map[Int, Long]): IO[Unit] = store.write(changes, positions)
                def close: IO[Unit] = closes.getAndUpdate(_ + 1).flatMap(before => IO.raiseWhen(before == 0)(boom))
        Engine.running("app", counting, world.member(), world.plane, None, 1.second, 1, PurgeInterval, CloseTimeout, _ => IO.pure(4), failing).use:
          _ =>
            world.tell(assign(TaskId("0", 0), TaskId("0", 1))) *> IO.sleep(500.millis) *>
              world.tell(MemberAction.Revoke(2, Set(TaskId("0", 0), TaskId("0", 1)))) *> IO.sleep(500.millis) *> closes.get
        .attempt
    TestControl.executeEmbed(run.timeout(1.minute)).map(closed => assertEquals(closed.toOption, Some(2)))

  test("a store still closing when the close timeout runs out is not closed again as the engine is released"):
    val run =
      (World(), IO.ref(0)).tupled.flatMap: (world, closes) =>
        // Each store takes longer to close than the close timeout, and counts its closes.
        val slow: TaskStore.Open[IO] =
          (name, time, held) =>
            TaskStore.memory[IO].apply(name, time, held).map: store =>
              new TaskStore[IO]:
                export store.{clear, clock, get, positions, range, timed}
                def write(changes: StoreChanges, positions: Map[Int, Long]): IO[Unit] = store.write(changes, positions)
                def close: IO[Unit]                                                   = closes.update(_ + 1) *> IO.sleep(CloseTimeout * 2)
        // Leaving takes the task back, which the engine gives back by closing its store.
        val member = world.member(leave = world.tell(MemberAction.Revoke(2, Set(TaskId("0", 0)))) *> IO.never)
        Engine.running("app", counting, member, world.plane, None, 1.second, 1, PurgeInterval, CloseTimeout, _ => IO.pure(4), slow)
          .use(_ => world.tell(assign(TaskId("0", 0))) *> IO.sleep(500.millis)) *> IO.sleep(CloseTimeout * 3) *> closes.get
    TestControl.executeEmbed(run.timeout(1.minute)).map(closes => assertEquals(closes, 1))

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
        .to(Named("write-6"), Topic.from("out").orFail, Windowed.timeSerde(text, 10.seconds), Serde.long[IO])
    ).orFail

  /** Counts as `windowing` does, keeping windows a minute and emitting each once it closes. */
  private val emitting: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .withRetention(1.minute).emitOnWindowClose.count(Named("count-5"), Store("windows")).toStream(Named("to-stream-4"))
        .to(Named("write-7"), Topic.from("out").orFail, Windowed.timeSerde(text, 10.seconds), Serde.long[IO])
    ).orFail

  /** Counts as `windowing` does, passing on each window's final result once it closes. */
  private val suppressing: Topology[IO] =
    Topology.of(
      KStream(Named("in"), Topic.from("in").orFail, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .count(Named("count-6"), Store("windows")).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-5"))
        .to(Named("write-8"), Topic.from("out").orFail, Windowed.timeSerde(text, 10.seconds), Serde.long[IO])
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
        .to(Named("write-10"), Topic.from("out").orFail, Windowed.sessionSerde(text), Serde.long[IO].option)
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

  def assign(tasks: TaskId*): MemberAction = MemberAction.Assign(1, Assignment(tasks.toSet, Set.empty, Set.empty), 1)

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
    case Abandon
    case Transaction(written: Int, offsets: Map[TopicPartition, OffsetAndMetadata])
    case Purged(before: Map[TopicPartition, Offset])
    case Followed(starts: Vector[StartAt])
    case Renewed

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
      // A purge's outcome, the next write's, and a failure of every write to a topic a test names.
      purging: Ref[IO, (IO[Unit], IO[Unit], Option[(String => Boolean, Throwable)])],
      reported: SignallingRef[IO, Member],
      restoring: Ref[IO, FiniteDuration],
      val states: Ref[IO, List[StreamsState]],
      standingBy: Queue[IO, PolledRecord],
      ending: Ref[IO, (Map[TopicPartition, Long], Int)],
      val restoredFrom: Ref[IO, List[(Map[TopicPartition, Long], Map[String, StoreContents])]],
      offsetsRead: Ref[IO, IO[TaskOffsets]],
      val restoredAlso: Ref[IO, Map[String, StoreContents]],
      val epochs: Ref[IO, List[Int]]
  ):
    /** Changelog records for the standby tasks to follow. */
    def changelogs(polled: PolledRecord*): IO[Unit] = polled.toList.traverse_(standingBy.offer)

    /** Where changelog partitions end. */
    def endsAt(ends: Map[TopicPartition, Long]): IO[Unit] = ending.update(_.copy(_1 = ends))

    /** Makes the next `times` asks for where changelogs end fail, as librdkafka's does while a partition's leader moves. */
    def failEnds(times: Int): IO[Unit] = ending.update(_.copy(_2 = times))

    /** The task offsets the engine gives its member to send. */
    def taskOffsets: IO[TaskOffsets] = offsetsRead.get.flatten

    /** Changes what the member reports about the group. */
    def report(change: Member => Member): IO[Unit] = reported.update(change)

    /** Makes each restore take this long. */
    def restoreFor(time: FiniteDuration): IO[Unit] = restoring.set(time)

    /** Makes each purge fail after it is logged. */
    def failPurges: IO[Unit] = purging.update(_.copy(_1 = IO.raiseError(new RuntimeException("purge refused"))))

    /** Makes the next write fail with `failure`, once. */
    def failWrite(failure: Throwable): IO[Unit] = purging.update(_.copy(_2 = IO.raiseError(failure)))

    /** Fails every write to a topic `to` takes from now on, the others written. */
    def failWritesTo(to: String => Boolean, failure: Throwable): IO[Unit] = purging.update(_.copy(_3 = Some(to -> failure)))

    /** Fails every write from now on, each failure arming the next. */
    def failWrites(failure: Throwable): IO[Unit] = purging.update(_.copy(_2 = failWrites(failure) *> IO.raiseError(failure)))

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
        def changes: Stream[IO, MemberAction]            = Stream.fromQueueNoneTerminated(World.this.changes)
        def revoked(round: Long): IO[Unit]               = logged(Event.Revoked(round))
        def assigned(round: Long): IO[Unit]              = logged(Event.Assigned(round))
        def leave: IO[Unit]                              = logged(Event.Leave) *> leaving
        def abandon: IO[Unit]                            = logged(Event.Abandon)
        def epoch: IO[Int]                               = IO.pure(1)
        def reports: Signal[IO, Member]                  = reported
        def offsetsFrom(read: IO[TaskOffsets]): IO[Unit] = offsetsRead.set(read)
        def commit(topics: List[OffsetCommitRequest.Topic], epoch: Int): IO[List[OffsetCommitRequest.Answer]] =
          (now.flatMap(at => committedAt.update(_ :+ at)) *> epochs.update(_ :+ epoch) *> commits.modify(before => (before :+ topics, before.size)))
            .flatMap: n =>
              send(n, topics).flatTap(answers => logged(Event.Committed(topics)).whenA(answers.forall(_.errorCode == 0)))
        def committed(topics: List[OffsetFetchRequest.Topic]): IO[OffsetFetchRequest.Answer] =
          (now.flatMap(at => fetchedAt.update(_ :+ at)) *> fetches.modify(before => (before :+ topics, before.size))).flatMap(fetch)

    /** Transactions that record what they wrote and the offsets they took, failing as `failing` says for the `n`th. */
    def transactions(failing: Int => Option[Throwable]): Engine.Transactions[IO] =
      new Engine.Transactions[IO]:
        def apply[A](epoch: Int)(body: Engine.Writer[IO] => IO[(A, Map[TopicPartition, OffsetAndMetadata])]): IO[A] =
          epochs.update(_ :+ epoch) *> IO.ref(0).flatMap: count =>
            body(records => count.update(_ + records.size)).flatMap: (result, offsets) =>
              transacted.modify(n => (n + 1, n)).flatMap: n =>
                failing(n).fold(count.get.flatMap(written => logged(Event.Transaction(written, offsets))).as(result))(IO.raiseError)

        def renew: IO[Unit] = logged(Event.Renewed)

    val plane: Engine.Plane[IO] =
      new Engine.Plane[IO]:
        def read(starts: Vector[StartAt]): IO[Unit] = logged(Event.Read(starts))
        def poll: IO[List[PolledRecord]]            =
          polls.update(_ + 1) *> incoming.tryTakeN(None)
            .flatMap(taken => if taken.isEmpty then IO.sleep(Engine.PollMillis.millis).as(Nil) else IO.pure(taken))
        def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], stores: Map[String, TaskStore[IO]]): IO[Unit] =
          val held = stores.toList.traverse((name, store) => store.all.map(name -> StoreContents.of(_))).map(_.toMap.filter(!_._2.isEmpty))
          restoring.get.flatMap(IO.sleep) *> logged(Event.Restored(changelogs)) *> held.flatMap(now => restoredFrom.update(_ :+ (from, now))) *>
            restoredAlso.get.flatMap(_.toList.traverse_((name, also) =>
              stores.get(name).traverse_(_.write(StoreContents.unchanged ++ also.toVector.map((k, v) => k -> Some(v))))
            ))
        def follow(starts: Vector[StartAt]): IO[Unit] = logged(Event.Followed(starts))
        def followed: IO[List[PolledRecord]]          =
          standingBy.tryTakeN(None).flatMap(taken => if taken.isEmpty then IO.sleep(Engine.PollMillis.millis).as(Nil) else IO.pure(taken))
        // Each changelog starts at its first offset, 0, and ends where `endsAt` says.
        // Each fails while `failEnds` says, as while a leader moves.
        def bounds(partitions: Vector[TopicPartition]): IO[Map[TopicPartition, (Long, Long)]] = ends(partitions).map(_.view.mapValues(0L -> _).toMap)
        def ends(partitions: Vector[TopicPartition]): IO[Map[TopicPartition, Long]]           =
          ending.modify((ends, failing) => ((ends, (failing - 1).max(0)), failing)).flatMap: failing =>
            if failing > 0 then IO.raiseError(new KafkaException.BackendFailure("no leader", Some(ErrorCode.NotLeaderOrFollower), Some(true)))
            else ending.get.map(_._1.view.filterKeys(partitions.toSet).toMap)
        def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): IO[Unit] =
          val values = records.toList.flatMap(_.value).map(value => new String(value.toArray))
          purging.get
            .flatMap(_._3.filter((to, _) => records.exists(record => to(record.topic.value))).traverse_((_, failure) => IO.raiseError(failure))) *>
            purging.getAndUpdate(_.copy(_2 = IO.unit)).flatMap(_._2) *> written.update(_ ++ records.toList) *>
            now.flatMap(at => wroteAt.update(_ :+ (at, values)))
        def purge(before: Map[TopicPartition, Offset]): IO[Unit] = logged(Event.Purged(before)) *> purging.get.flatMap(_._1)

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
        IO.ref((IO.unit, IO.unit, Option.empty[(String => Boolean, Throwable)])),
        SignallingRef[IO].of(Member(identity)),
        IO.ref(Duration.Zero: FiniteDuration),
        IO.ref(List.empty[StreamsState]),
        Queue.unbounded[IO, PolledRecord],
        IO.ref((Map.empty[TopicPartition, Long], 0)),
        IO.ref(List.empty[(Map[TopicPartition, Long], Map[String, StoreContents])]),
        IO.ref(IO.raiseError[TaskOffsets](new AssertionError("the engine gave no task offsets"))),
        IO.ref(Map.empty[String, StoreContents]),
        IO.ref(List.empty[Int])
      ).mapN(new World(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _))

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
      open: TaskStore.Open[IO] = TaskStore.memory[IO],
      state: Option[StateDirectory[IO]] = None,
      taskTimeout: FiniteDuration = StreamsSettings.DefaultTaskTimeout
  )(script: World => IO[Unit]): IO[Unit] =
    val program =
      World().flatMap: world =>
        val transactions = failing.map(world.transactions)
        val member       = world.member(committed, commit)
        Engine.running(
          "app",
          topology,
          member,
          world.plane,
          transactions,
          1.second,
          concurrency,
          PurgeInterval,
          CloseTimeout,
          _ => IO.pure(4),
          open,
          None,
          state,
          taskTimeout = taskTimeout
        ).use: running =>
          running.run.attempt.flatMap(world.finished).start *> script(world)
    TestControl.executeEmbed(program.timeout(15.minutes))
