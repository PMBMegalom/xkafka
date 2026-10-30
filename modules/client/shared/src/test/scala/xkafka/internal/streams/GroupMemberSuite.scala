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

import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite
import internal.group.Connection
import internal.protocol.{OffsetCommitRequest, OffsetFetchRequest, Request, StreamsGroup, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse}

/** How a streams group member paces its heartbeats, finds its coordinator and tells its engine, over a coordinator the tests script, in simulated
  * time, on every platform.
  */
final class GroupMemberSuite extends CatsEffectSuite:
  import GroupMemberSuite.*

  test("heartbeats go at the interval the coordinator gives"):
    val run =
      Coordinator((_, request) => IO.pure(answer(request, active = Some(Set.empty), interval = 1000))).flatMap: coordinator =>
        member(coordinator).use(_ => IO.sleep(3500.millis)) *> coordinator.sent
    TestControl.executeEmbed(run.timeout(1.minute)).map: sent =>
      assertEquals(sent.map(_._1), List(0L, 1000L, 2000L, 3000L, 3500L))
      assertEquals(sent.map(_._2.memberEpoch), List(0, 1, 1, 1, -1))

  test("tasks the engine has taken on are reported at once, and the next heartbeat goes an interval after that"):
    val run =
      Coordinator((_, request) => IO.pure(answer(request, active = Some(Set(TaskId("0", 0))), interval = 1000))).flatMap: coordinator =>
        member(coordinator).use: member =>
          member.changes.head.compile.lastOrError.flatMap:
            case MemberAction.Assign(round, _) => IO.sleep(500.millis) *> member.assigned(round) *> IO.sleep(1200.millis)
            case other                         => IO.raiseError(new AssertionError(s"expected an assignment, got $other"))
        *> coordinator.sent
    TestControl.executeEmbed(run.timeout(1.minute)).map: sent =>
      assertEquals(sent.map(_._1).take(3), List(0L, 500L, 1500L))
      assertEquals(sent(1)._2.activeTasks, Some(List(StreamsGroup.Tasks("0", List(0)))))

  test("a heartbeat the coordinator is still loading for goes again after waits that double from 100 ms up to a second, a fifth either way"):
    val run =
      Coordinator((n, request) => IO.pure(answer(request, error = if n < 20 then 14 else 0))).flatMap: coordinator =>
        member(coordinator).use(_ => IO.sleep(30.seconds)) *> coordinator.sent
    TestControl.executeEmbed(run.timeout(1.minute)).map: sent =>
      val waits  = sent.map(_._1).sliding(2).map(pair => pair(1) - pair(0)).take(20).toList
      val bounds = List((80L, 120L), (160L, 240L), (320L, 480L), (640L, 960L)) ++ List.fill(16)((800L, 1000L))
      waits.zip(bounds).foreach((wait, bound) => assert(wait >= bound._1 && wait <= bound._2, s"waited $wait ms, not within $bound in $waits"))
      assert(waits.drop(4).distinct.size > 1, s"the waits at the limit never varied: $waits")

  test("a coordinator that is no longer the group's is looked for again, and the heartbeat goes at once"):
    val run =
      Coordinator((n, request) => IO.pure(answer(request, error = if n == 0 then 16 else 0, active = Some(Set.empty)))).flatMap: coordinator =>
        member(coordinator).use(_ => IO.sleep(100.millis)) *> (coordinator.sent, coordinator.opened, coordinator.closed).tupled
    TestControl.executeEmbed(run.timeout(1.minute)).map: (sent, opened, closed) =>
      assertEquals(sent.map(_._1).take(2), List(0L, 0L))
      assertEquals((opened, closed), (2, 2))

  test("a heartbeat that fails closes its connection and goes again on a new one after a wait"):
    val failure = new KafkaException.BackendFailure("gone", Some(ErrorCode.RequestTimedOut), Some(true))
    val run     =
      Coordinator((n, request) => if n == 0 then IO.raiseError(failure) else IO.pure(answer(request, active = Some(Set.empty)))).flatMap:
        coordinator => member(coordinator).use(_ => IO.sleep(1.second)) *> (coordinator.sent, coordinator.opened).tupled
    TestControl.executeEmbed(run.timeout(1.minute)).map: (sent, opened) =>
      assert(sent(1)._1 >= 80L && sent(1)._1 <= 120L, s"the second heartbeat went at ${sent(1)._1} ms")
      assertEquals(opened, 2)

  test("commits and reads of offsets go under the member's id and the epoch it is at, and one that fails gives up its connection for a new one"):
    val failure = new KafkaException.BackendFailure("gone", Some(ErrorCode.RequestTimedOut), Some(true))
    val offsets = List(OffsetCommitRequest.Topic("input", List(OffsetCommitRequest.Partition(0, 5L, -1, Some("meta")))))
    val asked   = List(OffsetFetchRequest.Topic("input", List(0)))
    val run     =
      Coordinator(
        (_, request) => IO.pure(answer(request, active = Some(Set.empty))),
        {
          case (0, _)                      => IO.raiseError(failure)
          case (_, _: OffsetCommitRequest) => IO.pure(List(OffsetCommitRequest.Answer("input", 0, 0)))
          case (_, _: OffsetFetchRequest)  => IO.pure(OffsetFetchRequest.Answer(Nil, 0))
        }
      ).flatMap: coordinator =>
        member(coordinator).use: member =>
          for
            before <- member.epoch
            _      <- IO.sleep(100.millis)
            after  <- member.epoch
            first  <- member.commit(offsets).attempt
            opened <- coordinator.opened
            second <- member.commit(offsets)
            read   <- member.committed(asked)
            counts <- (coordinator.opened, coordinator.closed).tupled
            made   <- coordinator.requests
          yield (before, after, first, opened, second, read, counts, made)
    TestControl.executeEmbed(run.timeout(1.minute)).map: (before, after, first, opened, second, read, counts, made) =>
      assertEquals((before, after), (0, 1))
      assertEquals(first, Left(failure))
      assertEquals(opened, 1)
      assertEquals(second, List(OffsetCommitRequest.Answer("input", 0, 0)))
      assertEquals(read, OffsetFetchRequest.Answer(Nil, 0))
      assertEquals(counts, (2, 1))
      assertEquals(
        made,
        List(
          OffsetCommitRequest("app", "member", 1, None, offsets),
          OffsetCommitRequest("app", "member", 1, None, offsets),
          OffsetFetchRequest("app", Some("member"), 1, asked, requireStable = true)
        )
      )

  test("an error the member cannot recover from ends the changes with it after every task is dropped, and leaving fails with it"):
    val run =
      Coordinator((_, request) => IO.pure(answer(request, error = 42))).flatMap: coordinator =>
        member(coordinator).use(member => (member.changes.attempt.compile.toList, member.leave.attempt).tupled)
    TestControl.executeEmbed(run.timeout(1.minute)).map: (changes, left) =>
      assertEquals(changes.collect { case Right(change) => change }, List(MemberAction.LoseAll))
      assertEquals(changes.collect { case Left(error: KafkaException.BackendFailure) => error.code }, List(Some(ErrorCode.Other(42))))
      assertEquals(left.left.toOption.collect { case error: KafkaException.BackendFailure => error.code }, Some(Some(ErrorCode.Other(42))))

  test("a failure the connection does not name stops the member with a failure that carries it"):
    val boom = new IllegalStateException("boom")
    val run  = Coordinator((_, _) => IO.raiseError(boom)).flatMap(coordinator => member(coordinator).use(_.changes.compile.drain.attempt))
    TestControl.executeEmbed(run.timeout(1.minute)).map: outcome =>
      assertEquals(outcome.left.toOption.map(_.getCause), Some(boom))
      assertEquals(outcome.left.toOption.collect { case error: KafkaException.BackendFailure => error.retriable }, Some(Some(false)))

  test("leaving gives back the active tasks first, then tells the coordinator, and the changes end"):
    val tasks = Set(TaskId("0", 0))
    val run   =
      Coordinator((_, request) => IO.pure(answer(request, active = Some(tasks)))).flatMap: coordinator =>
        member(coordinator).use: member =>
          val engine =
            member.changes.evalTap {
              case MemberAction.Assign(round, _) => member.assigned(round) *> IO.sleep(100.millis) *> member.leave.start.void
              case MemberAction.Revoke(round, _) => member.revoked(round)
              case _                             => IO.unit
            }.compile.toList
          engine.flatMap(changes => coordinator.sent.map((changes, _)))
    TestControl.executeEmbed(run.timeout(1.minute)).map: (changes, sent) =>
      assertEquals(changes, List(MemberAction.Assign(1, Assignment(tasks, Set.empty, Set.empty)), MemberAction.Revoke(2, tasks)))
      assertEquals(sent.last._2.memberEpoch, -1)
      assertEquals(sent.last._2.activeTasks, Some(Nil))

  test("released without leaving, the member leaves, waiting no longer than its close timeout for its tasks to be given back"):
    val run =
      Coordinator((_, request) => IO.pure(answer(request, active = Some(Set(TaskId("0", 0)))))).flatMap: coordinator =>
        val engine =
          (member: GroupMember[IO]) =>
            member.changes.head.compile.lastOrError.flatMap:
              case MemberAction.Assign(round, _) => member.assigned(round) *> IO.sleep(100.millis)
              case other                         => IO.raiseError(new AssertionError(s"expected an assignment, got $other"))
        member(coordinator).use(engine).timed.map(_._1).flatMap(took => coordinator.sent.map((took, _)))
    TestControl.executeEmbed(run.timeout(1.minute)).map: (took, sent) =>
      assertEquals(took, 100.millis + CloseTimeout)
      assert(sent.forall(_._2.memberEpoch != -1), s"the member said it left while it still had tasks: $sent")

  test("released without tasks, the member tells the coordinator it leaves"):
    val run =
      Coordinator((_, request) => IO.pure(answer(request, active = Some(Set.empty)))).flatMap: coordinator =>
        member(coordinator).use(_ => IO.sleep(100.millis)).timed.map(_._1).flatMap(took => coordinator.sent.map((took, _)))
    TestControl.executeEmbed(run.timeout(1.minute)).map: (took, sent) =>
      assertEquals(took, 100.millis)
      assertEquals(sent.map(_._2.memberEpoch), List(0, -1))

  test("a member id is a random UUID in URL-safe base64 without padding, drawn again while it starts with a dash"):
    val dashed = UUID.fromString("fbffffff-ffff-ffff-ffff-ffffffffffff")
    val plain  = UUID.fromString("0123cdef-4567-89ab-cdef-0123456789ab")
    for
      left <- IO.ref(List(dashed, plain))
      id   <- GroupMember.memberId[IO](using summon, uuids(left))
    yield assertEquals(id, "ASPN70VniavN7wEjRWeJqw")

object GroupMemberSuite:
  val CloseTimeout: FiniteDuration = 3.seconds

  private val identity =
    MemberIdentity(
      "app",
      "member",
      "process",
      StreamsGroup.Topology(0, List(StreamsGroup.Subtopology("0", List("input"), Nil, Nil, Nil, Nil, Nil))),
      300000,
      None,
      Nil,
      None
    )

  def member(coordinator: Coordinator): Resource[IO, GroupMember[IO]] = GroupMember(coordinator.connect, identity, CloseTimeout)

  def uuids(left: Ref[IO, List[UUID]]): UUIDGen[IO] =
    new UUIDGen[IO]:
      def randomUUID: IO[UUID] = left.modify(ids => (ids.tail, ids.head))

  /** An answer at the epoch after the request's, or -1 to a member leaving. */
  def answer(
      request: StreamsGroupHeartbeatRequest,
      error: Short = 0,
      active: Option[Set[TaskId]] = None,
      interval: Int = 5000
  ): StreamsGroupHeartbeatResponse =
    def wire(tasks: Set[TaskId]) =
      tasks.toList.sortBy(t => (t.subtopology, t.partition)).map(t => StreamsGroup.Tasks(t.subtopology, List(t.partition)))
    val epoch = if request.memberEpoch == -1 then -1 else 1
    val tasks = active.filter(_ => request.memberEpoch == 0)
    StreamsGroupHeartbeatResponse(
      0,
      error,
      Option.when(error != 0)("refused"),
      request.memberId,
      epoch,
      interval,
      -1L,
      60000,
      None,
      tasks.map(wire),
      tasks.map(_ => Nil),
      tasks.map(_ => Nil),
      false,
      0,
      None
    )

  /** A coordinator that answers the `n`th heartbeat as `script` says, and the `n`th other request as `others` does, counting the connections made to
    * it and closed.
    */
  final class Coordinator(
      script: (Int, StreamsGroupHeartbeatRequest) => IO[StreamsGroupHeartbeatResponse],
      others: PartialFunction[(Int, Request[?]), IO[Any]],
      log: Ref[IO, Vector[(Long, StreamsGroupHeartbeatRequest)]],
      made: Ref[IO, Vector[Request[?]]],
      counts: Ref[IO, (Int, Int)]
  ):
    def sent: IO[List[(Long, StreamsGroupHeartbeatRequest)]] = log.get.map(_.toList)
    def requests: IO[List[Request[?]]]                       = made.get.map(_.toList)
    def opened: IO[Int]                                      = counts.get.map(_._1)
    def closed: IO[Int]                                      = counts.get.map(_._2)

    val connect: Resource[IO, Connection[IO]] =
      Resource.make(counts.update((opened, closed) => (opened + 1, closed)))(_ => counts.update((opened, closed) => (opened, closed + 1))).as:
        new Connection[IO]:
          def send[R](request: Request[R]): IO[R] =
            request match
              case heartbeat: StreamsGroupHeartbeatRequest => IO.monotonic
                  .flatMap(now => log.modify(sent => (sent :+ (now.toMillis, heartbeat), sent.size))).flatMap(n => script(n, heartbeat))
                  .map(_.asInstanceOf[R])
              case other => made.modify(before => (before :+ other, before.size)).flatMap: n =>
                  others.applyOrElse((n, other), _ => IO.raiseError(new AssertionError(s"unexpected request $other"))).map(_.asInstanceOf[R])

  object Coordinator:
    def apply(
        script: (Int, StreamsGroupHeartbeatRequest) => IO[StreamsGroupHeartbeatResponse],
        others: PartialFunction[(Int, Request[?]), IO[Any]] = PartialFunction.empty
    ): IO[Coordinator] =
      (IO.ref(Vector.empty[(Long, StreamsGroupHeartbeatRequest)]), IO.ref(Vector.empty[Request[?]]), IO.ref((0, 0)))
        .mapN(new Coordinator(script, others, _, _, _))
