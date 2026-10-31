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

import internal.protocol.{StreamsGroup, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse}

/** One partition of a sub-topology, the unit of work the group hands out. */
private[xkafka] final case class TaskId(subtopology: String, partition: Int) derives CanEqual

/** The tasks a member runs, or is told to run. */
private[xkafka] final case class Assignment(active: Set[TaskId], standby: Set[TaskId], warmup: Set[TaskId]) derives CanEqual

private[xkafka] object Assignment:
  val empty: Assignment = Assignment(Set.empty, Set.empty, Set.empty)

/** What a member tells the coordinator about itself when it joins. */
private[xkafka] final case class MemberIdentity(
    groupId: String,
    memberId: String,
    processId: String,
    topology: StreamsGroup.Topology,
    rebalanceTimeoutMs: Int,
    rackId: Option[String],
    clientTags: List[(String, String)],
    userEndpoint: Option[StreamsGroup.Endpoint]
)

private[xkafka] enum Phase derives CanEqual:
  /** Not started. */
  case Idle

  /** Asking to join, which every heartbeat does until the coordinator answers one. */
  case Joining

  /** In the group. */
  case Joined

  /** Giving back its active tasks, then saying it leaves once `sent`. */
  case Leaving(sent: Boolean)

  /** Out of the group; nothing more happens. */
  case Left

  /** Stopped by an error it cannot recover from; nothing more happens. */
  case Failed

/** A change to the tasks the member runs, which the engine carries out and confirms by its round. */
private[xkafka] enum Step derives CanEqual:
  /** Active tasks to finish and give back, once their last transaction has committed. */
  case Revoking(round: Long, tasks: Set[TaskId])

  /** The whole assignment to run, once nothing is left to give back. */
  case Assigning(round: Long, assignment: Assignment)

private[xkafka] enum MemberEvent:
  /** Join the group. */
  case Start

  /** The time for the next heartbeat has come. */
  case Due

  /** The coordinator answered the last heartbeat. */
  case Answered(response: StreamsGroupHeartbeatResponse)

  /** The last heartbeat got no answer. */
  case RequestFailed(error: KafkaException)

  /** The engine gave back the tasks of a `Revoke`. */
  case Revoked(round: Long)

  /** The engine runs the assignment of an `Assign`. */
  case Assigned(round: Long)

  /** Leave the group. */
  case Leave

private[xkafka] enum MemberAction derives CanEqual:
  /** Send this heartbeat now. */
  case Send(request: StreamsGroupHeartbeatRequest)

  /** Send the next heartbeat after this long. */
  case Wait(after: FiniteDuration)

  /** Send the heartbeat again after the failure numbered `attempt`, finding the coordinator again first if `rediscover`, and at once unless
    * `backoff`.
    */
  case Retry(attempt: Int, rediscover: Boolean, backoff: Boolean)

  /** Finish these active tasks, commit them and stop them, then answer `Revoked(round)`. */
  case Revoke(round: Long, tasks: Set[TaskId])

  /** Run exactly this assignment, then answer `Assigned(round)`. */
  case Assign(round: Long, assignment: Assignment)

  /** Stop every task without committing: the group no longer counts them as this member's. */
  case LoseAll

  /** The member stopped for this error. */
  case Fail(error: KafkaException)

  /** The member has left the group. */
  case Done

/** A member of a streams group (KIP-1071), as a pure function from its state and an event to its next state and what to do, following the Java
  * client's member where it applies.
  *
  * Only active tasks are given back before the member reports the change, since only they commit; standby and warm-up tasks are dropped as the new
  * assignment is run. At most one heartbeat is in flight.
  *
  * @param owned
  *   what the engine runs, which every heartbeat reports
  * @param target
  *   the assignment the coordinator gave last
  * @param reported
  *   the tasks the last heartbeat reported, or none when the next one must report them whatever they are
  * @param unauthorized
  *   why the coordinator last refused the member its topics, until it next accepts a heartbeat
  * @param taskOffsetInterval
  *   how often the coordinator asks for the offsets of the member's tasks' changelogs
  */
private[xkafka] final case class Member(
    identity: MemberIdentity,
    phase: Phase,
    epoch: Int,
    endpointEpoch: Int,
    owned: Assignment,
    target: Option[Assignment],
    step: Option[Step],
    round: Long,
    reported: Option[Assignment],
    inFlight: Boolean,
    failures: Int,
    statuses: List[StreamsGroup.Status],
    unauthorized: Option[String] = None,
    taskOffsetInterval: Option[FiniteDuration] = None
):
  import Member.*
  import MemberAction.*
  import MemberEvent.*

  def next(event: MemberEvent): (Member, List[MemberAction]) =
    (phase, event) match
      case (Phase.Left | Phase.Failed, _) => (this, Nil)
      case (Phase.Idle, Start)            => copy(phase = Phase.Joining).heartbeat
      case (Phase.Idle, Leave)            => (copy(phase = Phase.Left), List(Done))
      case (Phase.Idle, _)                => (this, Nil)
      case (_, Start)                     => (this, Nil)
      case (_, Due)                       => if inFlight then (this, Nil) else heartbeat
      case (_, Answered(response))        => copy(inFlight = false).answered(response)
      case (_, RequestFailed(error))      => copy(inFlight = false).failed(error)
      case (_, Revoked(round))            => step match
          case Some(Step.Revoking(`round`, tasks)) => copy(owned = owned.copy(active = owned.active -- tasks), step = None).settle(None)
          case _                                   => (this, Nil)
      case (_, Assigned(round)) => step match
          case Some(Step.Assigning(`round`, assignment)) => copy(owned = assignment, step = None).settle(None)
          case _                                         => (this, Nil)
      case (Phase.Leaving(_), Leave) => (this, Nil)
      case (_, Leave)                => copy(phase = Phase.Leaving(false), target = None).settle(None)

  /** Sends a heartbeat now, unless one is in flight: its answer settles the member again, which sends what is still wanted. */
  private def heartbeat: (Member, List[MemberAction]) =
    if inFlight then (this, Nil)
    else
      val leaving = phase == Phase.Leaving(true)
      val joining = phase == Phase.Joining
      val tasks   = Option.when(!reported.contains(owned))(owned)
      val request =
        StreamsGroupHeartbeatRequest(
          groupId = identity.groupId,
          memberId = identity.memberId,
          memberEpoch = if leaving then LeaveEpoch else epoch,
          endpointInformationEpoch = endpointEpoch,
          instanceId = None,
          rackId = if joining then identity.rackId else None,
          rebalanceTimeoutMs = if joining then identity.rebalanceTimeoutMs else -1,
          topology = Option.when(joining)(identity.topology),
          activeTasks = tasks.map(assigned => wire(assigned.active)),
          standbyTasks = tasks.map(assigned => wire(assigned.standby)),
          warmupTasks = tasks.map(assigned => wire(assigned.warmup)),
          processId = Option.when(joining)(identity.processId),
          userEndpoint = if joining then identity.userEndpoint else None,
          clientTags = Option.when(joining)(identity.clientTags),
          taskOffsets = None,
          taskEndOffsets = None,
          shutdownApplication = false
        )
      (copy(inFlight = true, reported = Some(owned)), List(Send(request)))

  private def answered(response: StreamsGroupHeartbeatResponse): (Member, List[MemberAction]) =
    phase match
      case Phase.Leaving(true) => (copy(phase = Phase.Left), List(Done))
      case _                   => response.errorCode match
          case 0                             => accepted(response)
          case NotCoordinator | NotAvailable => retry(rediscover = true, backoff = false)
          case LoadInProgress                => retry(rediscover = false, backoff = true)
          case FencedEpoch | UnknownMember   => fenced
          // Java Streams reports this and goes on heartbeating, so that the application can see it and decide.
          case TopicAuthorizationFailed =>
            val detail = response.errorMessage.getOrElse("the application may not use some of its topics")
            copy(unauthorized = Some(detail)).retry(rediscover = false, backoff = true)
          case code =>
            val detail = response.errorMessage.getOrElse(s"the coordinator refused the heartbeat with error $code")
            fail(new KafkaException.BackendFailure(detail, Some(ErrorCode.fromProtocol(code)), Some(false)))

  private def accepted(response: StreamsGroupHeartbeatResponse): (Member, List[MemberAction]) =
    val known   = identity.topology.subtopologies.map(_.id).toSet
    val updated =
      copy(
        phase = if phase == Phase.Joining then Phase.Joined else phase,
        epoch = response.memberEpoch,
        endpointEpoch = response.endpointInformationEpoch,
        failures = 0,
        statuses = response.statuses.getOrElse(statuses),
        unauthorized = None,
        taskOffsetInterval = Option.when(response.taskOffsetIntervalMs > 0)(response.taskOffsetIntervalMs.millis).orElse(taskOffsetInterval)
      )
    (response.activeTasks, response.standbyTasks, response.warmupTasks) match
      case (None, None, None)                          => updated.settle(Some(response.heartbeatIntervalMs.millis))
      case (Some(active), Some(standby), Some(warmup)) =>
        val offered = Assignment(tasks(active), tasks(standby), tasks(warmup))
        val alien   = (offered.active ++ offered.standby ++ offered.warmup).map(_.subtopology) -- known
        val names   = alien.toList.sorted.mkString(", ")
        if alien.nonEmpty then fail(new KafkaException.InvalidBackendResponse(s"the coordinator assigned tasks of unknown sub-topologies $names"))
        else updated.copy(target = Some(offered)).settle(Some(response.heartbeatIntervalMs.millis))
      case _ => fail(new KafkaException.InvalidBackendResponse("the coordinator left some but not all of the task lists unchanged"))

  /** Takes the next step towards the target, or towards leaving, then sends the next heartbeat: at once when the coordinator must hear news, and
    * otherwise after `interval` when one is given.
    */
  private def settle(interval: Option[FiniteDuration]): (Member, List[MemberAction]) =
    val (moved, changes) = reconcile
    val ready            = moved.step.isEmpty
    val (sent, timing)   =
      moved.phase match
        case Phase.Leaving(false) if ready && moved.owned.active.isEmpty =>
          if moved.inFlight then (moved, Nil) else moved.copy(phase = Phase.Leaving(true)).heartbeat
        case Phase.Joined if ready && !moved.reported.contains(moved.owned) => moved.heartbeat
        case _                                                              => (moved, if moved.inFlight then Nil else interval.map(Wait(_)).toList)
    (sent, changes ++ timing)

  /** Starts the next step towards the target when none is under way: first giving back the active tasks it no longer has, then running it. */
  private def reconcile: (Member, List[MemberAction]) =
    if step.nonEmpty then (this, Nil)
    else
      val goal =
        phase match
          case Phase.Leaving(_) => Some(Assignment(Set.empty, owned.standby, owned.warmup))
          case _                => target
      goal match
        case None         => (this, Nil)
        case Some(wanted) =>
          val giveBack = owned.active -- wanted.active
          if giveBack.nonEmpty then (copy(step = Some(Step.Revoking(round + 1, giveBack)), round = round + 1), List(Revoke(round + 1, giveBack)))
          else if wanted != owned then (copy(step = Some(Step.Assigning(round + 1, wanted)), round = round + 1), List(Assign(round + 1, wanted)))
          else (this, Nil)

  private def retry(rediscover: Boolean, backoff: Boolean): (Member, List[MemberAction]) =
    (copy(failures = failures + 1, reported = None), List(Retry(failures + 1, rediscover, backoff)))

  private def failed(error: KafkaException): (Member, List[MemberAction]) =
    phase match
      case Phase.Leaving(true) => (copy(phase = Phase.Left), List(Done))
      case _                   => error match
          case failure: KafkaException.BackendFailure if failure.retriable.contains(true) =>
            retry(rediscover = failure.code.contains(ErrorCode.NetworkException), backoff = true)
          case other => fail(other)

  /** The coordinator no longer counts this member in the group: its tasks are lost, and it joins again from the start. */
  private def fenced: (Member, List[MemberAction]) =
    phase match
      case Phase.Leaving(_) => (copy(phase = Phase.Left, owned = Assignment.empty, step = None), List(LoseAll, Done))
      case _                =>
        val (joining, send) = copy(phase = Phase.Joining, epoch = 0, owned = Assignment.empty, target = None, step = None, reported = None).heartbeat
        (joining, LoseAll :: send)

  private def fail(error: KafkaException): (Member, List[MemberAction]) =
    (copy(phase = Phase.Failed, owned = Assignment.empty, step = None), List(LoseAll, Fail(error)))

private[xkafka] object Member:
  def apply(identity: MemberIdentity): Member = Member(identity, Phase.Idle, 0, -1, Assignment.empty, None, None, 0L, None, false, 0, Nil)

  private val LeaveEpoch = -1

  private val LoadInProgress: Short = 14 // COORDINATOR_LOAD_IN_PROGRESS
  private val NotAvailable: Short   = 15 // COORDINATOR_NOT_AVAILABLE
  private val NotCoordinator: Short = 16 // NOT_COORDINATOR
  private val UnknownMember: Short  = 25 // UNKNOWN_MEMBER_ID

  private val TopicAuthorizationFailed: Short = 29  // TOPIC_AUTHORIZATION_FAILED
  private val FencedEpoch: Short              = 110 // FENCED_MEMBER_EPOCH

  private def wire(tasks: Set[TaskId]): List[StreamsGroup.Tasks] =
    tasks.groupBy(_.subtopology).toList.sortBy(_._1).map((id, group) => StreamsGroup.Tasks(id, group.toList.map(_.partition).sorted))

  private def tasks(wire: List[StreamsGroup.Tasks]): Set[TaskId] = wire.flatMap(tasks => tasks.partitions.map(TaskId(tasks.subtopologyId, _))).toSet
