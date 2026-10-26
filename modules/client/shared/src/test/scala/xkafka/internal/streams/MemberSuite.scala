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

import munit.FunSuite
import internal.protocol.{StreamsGroup, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse}
import MemberAction.*
import MemberEvent.*

/** How a streams group member answers its coordinator and its engine, over scripted answers, on every platform. */
final class MemberSuite extends FunSuite:
  private val topology =
    StreamsGroup.Topology(
      0,
      List(
        StreamsGroup.Subtopology("0", List("input"), Nil, Nil, Nil, Nil, Nil),
        StreamsGroup.Subtopology("1", List("other"), Nil, Nil, Nil, Nil, Nil)
      )
    )

  private val identity =
    MemberIdentity("app", "member", "process", topology, 300000, Some("rack"), List("zone" -> "a"), Some(StreamsGroup.Endpoint("host", 8080)))

  private def task(subtopology: String, partition: Int): TaskId = TaskId(subtopology, partition)

  private def answer(
      epoch: Int,
      active: Option[Set[TaskId]] = None,
      standby: Set[TaskId] = Set.empty,
      error: Short = 0,
      interval: Int = 5000,
      endpointEpoch: Int = 0,
      statuses: Option[List[StreamsGroup.Status]] = None
  ): StreamsGroupHeartbeatResponse =
    def wire(tasks: Set[TaskId]) =
      tasks.toList.sortBy(t => (t.subtopology, t.partition)).map(t => StreamsGroup.Tasks(t.subtopology, List(t.partition)))
    StreamsGroupHeartbeatResponse(
      0,
      error,
      Option.when(error != 0)("refused"),
      "member",
      epoch,
      interval,
      -1L,
      60000,
      statuses,
      active.map(wire),
      active.map(_ => wire(standby)),
      active.map(_ => Nil),
      false,
      endpointEpoch,
      None
    )

  /** Runs the events in order, returning the member after them and the actions of each. */
  private def run(member: Member, events: MemberEvent*): (Member, List[List[MemberAction]]) =
    events.foldLeft((member, List.empty[List[MemberAction]])):
      case ((current, done), event) =>
        val (after, actions) = current.next(event)
        (after, done :+ actions)

  private def sent(actions: List[MemberAction]): List[StreamsGroupHeartbeatRequest] = actions.collect { case Send(request) => request }

  /** A member that has joined and runs `active`. */
  private def joined(active: Set[TaskId]): Member =
    val (member, actions) = run(Member(identity), Start, Answered(answer(1, Some(active))))
    val round             = actions.last.collectFirst { case Assign(round, _) => round }
    round.fold(member)(round => run(member, Assigned(round), Answered(answer(1)))._1)

  test("joining sends the topology and everything else said once, with epoch 0 and no tasks"):
    val (_, actions) = run(Member(identity), Start)
    assertEquals(
      sent(actions.head),
      List(StreamsGroupHeartbeatRequest(
        "app",
        "member",
        0,
        -1,
        None,
        Some("rack"),
        300000,
        Some(topology),
        Some(Nil),
        Some(Nil),
        Some(Nil),
        Some("process"),
        Some(StreamsGroup.Endpoint("host", 8080)),
        Some(List("zone" -> "a")),
        None,
        None,
        false
      ))
    )

  test("a joined member runs what it is given, then reports it at once, then sends only the epochs until something changes"):
    val handed            = Set(task("0", 0), task("1", 2))
    val (member, actions) = run(Member(identity), Start, Answered(answer(3, Some(handed), standby = Set(task("0", 1)), endpointEpoch = 4)))
    val assignment        = Assignment(handed, Set(task("0", 1)), Set.empty)
    assertEquals(actions(1), List(Assign(1, assignment), Wait(5.seconds)))
    val (later, after) = run(member, Assigned(1), Answered(answer(3, interval = 2000)), Due)
    val ack            = sent(after(0)).head
    assertEquals((ack.memberEpoch, ack.endpointInformationEpoch, ack.topology, ack.processId, ack.rebalanceTimeoutMs), (3, 4, None, None, -1))
    assertEquals(
      (ack.activeTasks, ack.standbyTasks, ack.warmupTasks),
      (Some(List(StreamsGroup.Tasks("0", List(0)), StreamsGroup.Tasks("1", List(2)))), Some(List(StreamsGroup.Tasks("0", List(1)))), Some(Nil))
    )
    assertEquals(after(1), List(Wait(2.seconds)))
    val quiet = sent(after(2)).head
    assertEquals((quiet.activeTasks, quiet.standbyTasks, quiet.warmupTasks, quiet.rackId, quiet.clientTags), (None, None, None, None, None))
    assertEquals(later.owned, assignment)

  test("active tasks taken away are given back before the new assignment runs, and the change is reported once it does"):
    val member       = joined(Set(task("0", 0), task("0", 1)))
    val (_, actions) = run(member, Answered(answer(2, Some(Set(task("0", 0), task("0", 2))))), Revoked(2), Assigned(3))
    val target       = Assignment(Set(task("0", 0), task("0", 2)), Set.empty, Set.empty)
    assertEquals(actions(0), List(Revoke(2, Set(task("0", 1))), Wait(5.seconds)))
    assertEquals(actions(1), List(Assign(3, target)))
    assertEquals(sent(actions(2)).flatMap(_.activeTasks), List(List(StreamsGroup.Tasks("0", List(0, 2)))))

  test("nothing is reported while active tasks are being given back"):
    val member       = joined(Set(task("0", 0), task("0", 1)))
    val (_, actions) = run(member, Answered(answer(2, Some(Set(task("0", 0))))), Due)
    assertEquals(sent(actions(1)).map(_.activeTasks), List(None))

  test("standby tasks taken away are dropped with the new assignment, without being given back first"):
    val (member, actions) = run(Member(identity), Start, Answered(answer(1, Some(Set(task("0", 0))), standby = Set(task("0", 1)))), Assigned(1))
    val (_, after)        = run(member, Answered(answer(2, Some(Set(task("0", 0))))))
    assertEquals(actions(2).map(_.getClass.getSimpleName), List("Send"))
    assertEquals(after(0), List(Assign(2, Assignment(Set(task("0", 0)), Set.empty, Set.empty)), Wait(5.seconds)))

  test("an assignment that arrives while one is being carried out is carried out next, and only the last is reported"):
    val member       = joined(Set.empty)
    val (_, actions) =
      run(member, Answered(answer(2, Some(Set(task("0", 0))))), Answered(answer(3, Some(Set(task("0", 1))))), Assigned(1), Revoked(2), Assigned(3))
    assertEquals(actions(1), List(Wait(5.seconds)))
    assertEquals(actions(2), List(Revoke(2, Set(task("0", 0)))))
    assertEquals(actions(3), List(Assign(3, Assignment(Set(task("0", 1)), Set.empty, Set.empty))))
    assertEquals(sent(actions(4)).flatMap(_.activeTasks), List(List(StreamsGroup.Tasks("0", List(1)))))

  test("an answer that leaves the tasks unchanged keeps the assignment"):
    val member           = joined(Set(task("0", 0)))
    val (after, actions) = run(member, Answered(answer(2)))
    assertEquals(actions, List(List(Wait(5.seconds))))
    assertEquals((after.owned.active, after.epoch), (Set(task("0", 0)), 2))

  for (code, name) <- List[(Short, String)](110.toShort -> "FENCED_MEMBER_EPOCH", 25.toShort -> "UNKNOWN_MEMBER_ID") do
    test(s"$name loses every task and joins again at once from epoch 0, and the engine's answers to earlier steps are ignored"):
      val member         = joined(Set(task("0", 0)))
      val (after, steps) = run(member, Answered(answer(2, Some(Set.empty))), Answered(answer(2, error = code)), Revoked(2))
      assertEquals(steps(1).head, LoseAll)
      val rejoin = sent(steps(1)).head
      assertEquals((rejoin.memberEpoch, rejoin.topology, rejoin.activeTasks), (0, Some(topology), Some(Nil)))
      assertEquals(steps(2), Nil)
      assertEquals((after.phase, after.owned), (Phase.Joining, Assignment.empty))

  test(
    "a coordinator that moved or is not there is found again at once, one loading is asked again after a backoff, and the tasks are reported again"
  ):
    val member       = joined(Set(task("0", 0)))
    val (_, actions) = run(member, Answered(answer(2, error = 16)), Due, Answered(answer(2, error = 15)), Due, Answered(answer(2, error = 14)), Due)
    assertEquals(actions(0), List(Retry(1, rediscover = true, backoff = false)))
    assertEquals(actions(2), List(Retry(2, rediscover = true, backoff = false)))
    assertEquals(actions(4), List(Retry(3, rediscover = false, backoff = true)))
    assertEquals(sent(actions(5)).flatMap(_.activeTasks), List(List(StreamsGroup.Tasks("0", List(0)))))

  test("a heartbeat that gets no answer is retried after a backoff, finding the coordinator again only when the connection failed"):
    val member         = joined(Set.empty)
    val network        = new KafkaException.BackendFailure("down", Some(ErrorCode.NetworkException), Some(true))
    val timeout        = new KafkaException.BackendFailure("slow", Some(ErrorCode.RequestTimedOut), Some(true))
    val (after, steps) = run(member, RequestFailed(network), Due, RequestFailed(timeout), Due, Answered(answer(2)))
    assertEquals(steps(0), List(Retry(1, rediscover = true, backoff = true)))
    assertEquals(steps(2), List(Retry(2, rediscover = false, backoff = true)))
    assertEquals(after.failures, 0)

  test("an error it cannot recover from loses every task and stops the member, which then ignores everything"):
    val member         = joined(Set(task("0", 0)))
    val (after, steps) = run(member, Answered(answer(2, error = 30)), Due, Leave)
    steps(0) match
      case List(LoseAll, Fail(error: KafkaException.BackendFailure)) =>
        assertEquals((error.code, error.retriable, error.detail), (Some(ErrorCode.GroupAuthorizationFailed), Some(false), "refused"))
      case other => fail(s"unexpected $other")
    assertEquals((steps(1), steps(2), after.phase), (Nil, Nil, Phase.Failed))

  test("a failure that will not pass by itself stops the member"):
    val error        = new KafkaException.BackendFailure("bad", Some(ErrorCode.SaslAuthenticationFailed), Some(false))
    val (_, actions) = run(joined(Set.empty), RequestFailed(error))
    assertEquals(actions, List(List(LoseAll, Fail(error))))

  test("an answer that changes only some of the task lists, or assigns tasks of a sub-topology it does not have, stops the member"):
    val mixed = answer(2, Some(Set(task("0", 0)))).copy(standbyTasks = None)
    val alien = answer(2, Some(Set(task("7", 0))))
    List(mixed, alien).foreach: response =>
      val (after, actions) = run(joined(Set.empty), Answered(response))
      assert(actions.head.exists { case Fail(_: KafkaException.InvalidBackendResponse) => true; case _ => false }, actions)
      assertEquals(after.phase, Phase.Failed)

  test("leaving gives back the active tasks, then sends epoch -1 once, and is done whatever the answer"):
    List(answer(-1), answer(-1, error = 25)).foreach: last =>
      val member       = joined(Set(task("0", 0), task("1", 1)))
      val (_, actions) = run(member, Leave, Revoked(2), Answered(last))
      assertEquals(actions(0), List(Revoke(2, Set(task("0", 0), task("1", 1)))))
      assertEquals(sent(actions(1)).map(_.memberEpoch), List(-1))
      assertEquals(actions(2), List(Done))

  test("leaving while a heartbeat is in flight sends epoch -1 after its answer, ignoring any new assignment"):
    val member       = joined(Set.empty)
    val (_, actions) = run(member, Due, Leave, Answered(answer(2, Some(Set(task("0", 0))))))
    assertEquals(actions(1), Nil)
    assertEquals(actions(2).collect { case Send(request) => request.memberEpoch; case Assign(_, _) => -99 }, List(-1))

  test("a leave that gets no answer is done all the same"):
    val down         = new KafkaException.BackendFailure("down", Some(ErrorCode.NetworkException), Some(true))
    val (_, actions) = run(joined(Set.empty), Leave, RequestFailed(down))
    assertEquals(actions(1), List(Done))

  test("leaving before joining is done at once, and a member fenced while leaving loses its tasks and is done"):
    assertEquals(Member(identity).next(Leave)._2, List(Done))
    val (_, actions) = run(joined(Set(task("0", 0))), Leave, Due, Answered(answer(2, error = 110)))
    assertEquals(actions(2), List(LoseAll, Done))

  test("at most one heartbeat is in flight; one wanted meanwhile goes as soon as it is answered"):
    val member       = joined(Set.empty)
    val (_, actions) = run(member, Due, Due, Answered(answer(2, Some(Set(task("0", 0))))), Due, Assigned(1), Answered(answer(2)))
    assertEquals(actions(1), Nil)
    assertEquals(sent(actions(3)).map(_.activeTasks), List(None))
    assertEquals(actions(4), Nil)
    assertEquals(sent(actions(5)).flatMap(_.activeTasks), List(List(StreamsGroup.Tasks("0", List(0)))))

  test("the group's statuses are kept until the coordinator reports them again"):
    val missing    = List(StreamsGroup.Status(1, "missing source topics"))
    val (after, _) = run(joined(Set.empty), Answered(answer(2, statuses = Some(missing))), Answered(answer(2)))
    assertEquals(after.statuses, missing)

  test("a member that has not started sends nothing"):
    assertEquals(Member(identity).next(Due), (Member(identity), Nil))

  test("the engine's answer to a step other than the one under way is ignored"):
    val member           = joined(Set(task("0", 0), task("0", 1)))
    val (after, actions) = run(member, Answered(answer(2, Some(Set(task("0", 0))))), Revoked(1), Assigned(2))
    assertEquals((actions(1), actions(2)), (Nil, Nil))
    assertEquals(after.owned.active, Set(task("0", 0), task("0", 1)))

  test("starting again or leaving again changes nothing"):
    val member          = joined(Set(task("0", 0)))
    val (_, started)    = run(member, Start)
    val (leaving, left) = run(member, Leave, Leave)
    assertEquals(started, List(Nil))
    assertEquals(left(1), Nil)
    assertEquals(leaving.step, Some(Step.Revoking(2, Set(task("0", 0)))))
    val (_, sentLeave) = run(joined(Set.empty), Leave, Leave, Answered(answer(-1)))
    assertEquals((sent(sentLeave(0)).map(_.memberEpoch), sentLeave(1), sentLeave(2)), (List(-1), Nil, List(Done)))

  test("the engine's answer to an assignment other than the one under way is ignored"):
    val (member, _)      = run(joined(Set.empty), Answered(answer(2, Some(Set(task("0", 0))))))
    val (after, actions) = run(member, Assigned(0))
    assertEquals((actions, after.owned), (List(Nil), Assignment.empty))

  test("tasks are reported by sub-topology and partition in order"):
    val many         = (for subtopology <- List("1", "0"); partition <- 20 to 0 by -1 yield task(subtopology, partition)).toSet
    val (_, actions) = run(Member(identity), Start, Answered(answer(1, Some(many))), Assigned(1))
    assertEquals(
      sent(actions(2)).flatMap(_.activeTasks),
      List(List(StreamsGroup.Tasks("0", (0 to 20).toList), StreamsGroup.Tasks("1", (0 to 20).toList)))
    )
