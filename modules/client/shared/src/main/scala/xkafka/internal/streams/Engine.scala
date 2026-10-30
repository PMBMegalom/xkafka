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
package internal
package streams

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.{Async, Resource}
import cats.effect.std.{Queue, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import driver.{KafkaDriver, PolledRecord, StartAt}
import group.{Connections, PlatformCrypto, PlatformSockets}
import protocol.{OffsetCommitRequest, OffsetFetchRequest}
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{Origin, Segment, Topology}

/** Runs a streams program as one member of its application's group: the tasks the coordinator gives the member read their input partitions through
  * one consumer, write through one producer, and commit what they read through the member, at least once.
  */
private[xkafka] object Engine:
  /** How long each poll of the input waits for records. */
  val PollMillis = 100

  /** How often the engine commits what its tasks have read, Java Streams' `commit.interval.ms` under at-least-once. */
  val CommitInterval: FiniteDuration = 30.seconds

  /** How long the coordinator waits for a member to give back its tasks, Java's `max.poll.interval.ms`, which Java Streams sends as its rebalance
    * timeout.
    */
  val RebalanceTimeout: FiniteDuration = 5.minutes

  /** The first wait before asking the coordinator again, and the longest. */
  private val Backoff: FiniteDuration    = 100.millis
  private val MaxBackoff: FiniteDuration = 1.second

  private val LoadInProgress: Short       = 14  // COORDINATOR_LOAD_IN_PROGRESS
  private val NotAvailable: Short         = 15  // COORDINATOR_NOT_AVAILABLE
  private val NotCoordinator: Short       = 16  // NOT_COORDINATOR
  private val UnknownMember: Short        = 25  // UNKNOWN_MEMBER_ID
  private val UnstableOffsetCommit: Short = 88  // UNSTABLE_OFFSET_COMMIT
  private val FencedEpoch: Short          = 110 // FENCED_MEMBER_EPOCH
  private val StaleEpoch: Short           = 113 // STALE_MEMBER_EPOCH

  /** Answers to ask again after: the coordinator is moving or loading, offsets a transaction has yet to settle, or an epoch the member is about to
    * leave behind.
    */
  private val Again: Set[Short] = Set(LoadInProgress, NotAvailable, NotCoordinator, UnstableOffsetCommit, StaleEpoch)

  /** Answers saying the group no longer counts the member, which hears so in its next heartbeat and drops its tasks. */
  private val Gone: Set[Short] = Set(UnknownMember, FencedEpoch)

  /** The engine of `application` running `topology` over the brokers `settings` name. Released, it gives back its tasks, committing them, and leaves
    * the group, waiting at most `closeTimeout`. The effect it gives completes with the error that stops the engine, and otherwise does not complete.
    */
  def apply[F[_]](
      client: KafkaClient[F],
      driver: KafkaDriver[F],
      settings: ClientSettings,
      application: String,
      topology: Topology[F],
      commitInterval: FiniteDuration = CommitInterval,
      closeTimeout: FiniteDuration = 30.seconds
  )(using F: Async[F]): Resource[F, F[Unit]] =
    val connections = Connections(PlatformSockets[F], PlatformCrypto[F], settings, closeTimeout)
    val servers     = settings.bootstrapServers.toList.mkString(",")
    val properties  = settings.properties ++ ClientProperties.security(settings.security) ++ ClientProperties(settings)
    def consumer(isolation: String, reportEnds: Boolean): Resource[F, driver.Client] =
      Resource.make(
        driver.consumer(servers, settings.clientId, application, "earliest", properties.updated("isolation.level", isolation), reportEnds)
      )(driver.destroyConsumer)
    given UUIDGen[F] = GroupMember.uuids(PlatformCrypto[F])
    for
      memberId  <- Resource.eval(GroupMember.memberId[F])
      processId <- Resource.eval(UUIDGen[F].randomUUID.map(_.toString))
      identity =
        MemberIdentity(application, memberId, processId, Declaration(application, topology), RebalanceTimeout.toMillis.toInt, None, Nil, None)
      member   <- GroupMember(connections.coordinator(application), identity, closeTimeout)
      input    <- consumer("read_uncommitted", reportEnds = false)
      producer <-
        ProducerSettings.from(settings, Serializer.bytes[F].option, Serializer.bytes[F].option, ClientProperties.producer).toEither
          .leftMap(new KafkaException.InvalidSettings(_)).liftTo[Resource[F, *]].flatMap(client.producer)
      plane =
        new Plane[F]:
          def read(starts: Vector[StartAt]): F[Unit] = driver.assign(input, starts.map(_.partition), starts)
          def poll: F[List[PolledRecord]]            = driver.poll(input, PollMillis).map(_.records)
          def restore(changelogs: Map[TopicPartition, String]): F[Map[String, Map[Chunk[Byte], Stored]]] =
            consumer("read_committed", reportEnds = true).use(Restore(driver, _, changelogs))
          def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): F[Unit] = producer.produceAndAwait(records).void
      running <- running(application, topology, member, plane, commitInterval, closeTimeout)
    yield running

  /** The engine of `application` running `topology` as `member`, reading and writing through `plane`. Released, it gives back its tasks through the
    * member, committing them, waiting at most `closeTimeout`.
    */
  def running[F[_]](
      application: String,
      topology: Topology[F],
      member: GroupMember[F],
      plane: Plane[F],
      commitInterval: FiniteDuration,
      closeTimeout: FiniteDuration
  )(using F: Async[F]): Resource[F, F[Unit]] =
    val segments = topology.pipelines.toList.flatMap(_.segments.toList).zipWithIndex.map((segment, id) => id.toString -> segment).toMap
    for
      changes <- Resource.eval(Queue.unbounded[F, Either[Throwable, MemberAction]])
      _       <- member.changes.attempt.evalMap(changes.offer).compile.drain.background
      started <- Resource.eval(F.monotonic)
      loop = Loop(application, segments, member, plane, changes, commitInterval)
      running <- Resource.make(loop.run(Running(Map.empty, started)).start)(fiber => member.leave.timeout(closeTimeout).attempt *> fiber.cancel)
    yield running.joinWithNever

  /** How the engine reads its tasks' input and changelogs and writes what they write. */
  trait Plane[F[_]]:
    /** Reads exactly these partitions, each from where it starts. */
    def read(starts: Vector[StartAt]): F[Unit]

    /** The next records of the partitions being read, waiting a little for some. */
    def poll: F[List[PolledRecord]]

    /** Each store's contents, read back from its changelog partition. */
    def restore(changelogs: Map[TopicPartition, String]): F[Map[String, Map[Chunk[Byte], Stored]]]

    /** Writes the records, waiting until each is delivered. */
    def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): F[Unit]

  /** A task the engine runs: its program, what it holds, the partition it reads, and where the next commit puts it, once it has read past its start.
    */
  private final case class Live[F[_]](task: Task[F], state: TaskState, input: TopicPartition, position: Long, read: Option[(Long, Int)])

  private final case class Running[F[_]](tasks: Map[TaskId, Live[F]], committed: FiniteDuration)

  private final class Loop[F[_]](
      application: String,
      segments: Map[String, Segment[F, ?, ?, ?, ?]],
      member: GroupMember[F],
      plane: Plane[F],
      changes: Queue[F, Either[Throwable, MemberAction]],
      commitInterval: FiniteDuration
  )(using F: Async[F]):
    import MemberAction.*

    /** Carries out the changes the member told of since the last batch, then reads, processes and writes the next batch, committing once the interval
      * has passed.
      */
    def run(running: Running[F]): F[Unit] =
      changes.tryTakeN(None).flatMap(_.foldLeftM(running)(change)).flatMap(batch).flatMap: after =>
        F.monotonic.flatMap: now =>
          if now - after.committed >= commitInterval then commit(after, after.tasks.keySet).map(_.copy(committed = now)).flatMap(run) else run(after)

    private def change(running: Running[F], told: Either[Throwable, MemberAction]): F[Running[F]] =
      told.liftTo[F].flatMap:
        case Revoke(round, tasks) => commit(running, tasks).flatMap: committed =>
            val kept = committed.copy(tasks = committed.tasks -- tasks)
            reassign(kept) *> member.revoked(round).as(kept)
        case Assign(round, assignment) =>
          val kept  = running.tasks.view.filterKeys(assignment.active).toMap
          val added = (assignment.active -- kept.keySet).toList
          start(added).flatMap: started =>
            val now = running.copy(tasks = kept ++ started)
            reassign(now) *> member.assigned(round).as(now)
        case LoseAll => reassign(running.copy(tasks = Map.empty)).as(running.copy(tasks = Map.empty))
        case _       => F.pure(running)

    /** Reads each partition its task holds from where that task is. */
    private def reassign(running: Running[F]): F[Unit] = plane.read(running.tasks.values.toVector.map(task => StartAt(task.input, task.position, -1)))

    /** Starts tasks where the group last committed them, or at the start of their partitions, with their stores refilled from their changelogs. */
    private def start(tasks: List[TaskId]): F[Map[TaskId, Live[F]]] =
      tasks.traverse(id => sourceOf(id).map(id -> _)).flatMap: inputs =>
        val asked = inputs.groupMap(_._2.topic.value)(_._2.partition.value).toList.map(OffsetFetchRequest.Topic(_, _))
        if inputs.isEmpty then F.pure(Map.empty)
        else
          (stored(asked), restore(tasks)).tupled.flatMap: (offsets, stores) =>
            inputs.traverse: (id, partition) =>
              val found = offsets.get((partition.topic.value, partition.partition.value))
              val time  = found.flatMap(_.metadata).flatMap(PartitionTime.decode).getOrElse(-1L)
              val state = TaskState(stores.getOrElse(id, Map.empty), time)
              val from  = found.filter(_.offset >= 0).fold(KafkaDriver.BeginningOffset)(_.offset)
              val task  = new Task[F](application, segments(id.subtopology), id.partition)
              F.pure(id -> Live(task, state, partition, from, None))
            .map(_.toMap)

    /** The offsets the group stored, asking again while the coordinator or a transaction is not ready to answer. */
    private def stored(asked: List[OffsetFetchRequest.Topic]): F[Map[(String, Int), OffsetFetchRequest.Stored]] =
      untilAnswered(member.committed(asked)): answer =>
        val codes = answer.errorCode :: answer.partitions.map(_.errorCode)
        codes.find(code => code != 0 && !Again(code)) match
          case Some(code) => F.raiseError(failure("reading the group's offsets", code))
          case None       => F.pure(Option.when(codes.forall(_ == 0))(answer.partitions.map(p => (p.topic, p.partition) -> p).toMap))

    /** Each task's stores, read back from the task's partition of their changelogs, one task after another. */
    private def restore(tasks: List[TaskId]): F[Map[TaskId, Map[String, Map[Chunk[Byte], Stored]]]] =
      val stateful = tasks.filter(id => segments(id.subtopology).chain.stores.nonEmpty)
      if stateful.isEmpty then F.pure(Map.empty)
      else
        stateful.traverse: id =>
          segments(id.subtopology).chain.stores
            .traverse(store => partitionOf(s"$application-${store.name}-changelog", id.partition).map(_ -> store.name))
            .flatMap(changelogs => plane.restore(changelogs.toMap)).map(id -> _)
        .map(_.toMap)

    /** Commits where each of these tasks has read to, with its partition time, asking again while the coordinator is not ready. A member the group no
      * longer counts commits nothing: its tasks go to others, which read again from the group's last commit.
      */
    private def commit(running: Running[F], tasks: Set[TaskId]): F[Running[F]] =
      val due    = running.tasks.view.filterKeys(tasks).toList.collect { case (id, live @ Live(_, _, _, _, Some(_))) => id -> live }
      val topics =
        due.groupMap(_._2.input.topic.value): (_, live) =>
          val (next, leaderEpoch) = live.read.get
          OffsetCommitRequest.Partition(live.input.partition.value, next, leaderEpoch, Some(PartitionTime.encode(live.state.partitionTime)))
        .toList.map(OffsetCommitRequest.Topic(_, _))
      val sent =
        untilAnswered(member.commit(topics)): answers =>
          answers.map(_.errorCode).find(code => code != 0 && !Again(code)) match
            case Some(code) if Gone(code) => F.pure(Some(()))
            case Some(code)               => F.raiseError(failure("committing", code))
            case None                     => F.pure(Option.when(answers.forall(_.errorCode == 0))(()))
      if topics.isEmpty then F.pure(running) else sent.as(running.copy(tasks = running.tasks ++ due.map((id, live) => id -> live.copy(read = None))))

    /** Reads the next records, runs each through its task in order, and writes what the tasks wrote, waiting until every record is delivered. */
    private def batch(running: Running[F]): F[Running[F]] =
      // The Java consumer refuses to poll with nothing assigned.
      val polling = if running.tasks.isEmpty then F.sleep(PollMillis.millis).as(Nil) else plane.poll
      polling.flatMap: polled =>
        val routes = running.tasks.map((id, live) => (live.input.topic.value, live.input.partition.value) -> id)
        polled.foldLeftM((running.tasks, Vector.empty[TaskOutput])):
          case ((tasks, written), record) => routes.get((record.topic, record.partition)).flatMap(id => tasks.get(id).map(id -> _)) match
              case None             => F.pure((tasks, written))
              case Some((id, live)) => live.task.process(live.state, inputOf(record)).map: (state, out) =>
                  val next = live.copy(state = state, position = record.offset + 1, read = Some((record.offset + 1, record.leaderEpoch)))
                  (tasks.updated(id, next), written ++ out)
        .flatMap: (tasks, written) =>
          written.traverse(recordOf).flatMap(records => NonEmptyList.fromList(records.toList).traverse_(plane.write)).as(running.copy(tasks = tasks))

    private def inputOf(record: PolledRecord): TaskInput = TaskInput(record.key, record.value, record.timestamp.getOrElse(-1L), record.headers)

    private def recordOf(output: TaskOutput): F[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]] =
      output.partition.traverse(n => Partition.from(n).leftMap(new KafkaException.InvalidValue(_)).liftTo[F]).map: partition =>
        ProducerRecord(output.topic, output.key, output.value, partition, Some(Timestamp.fromEpochMillis(output.timestamp)), output.headers)

    /** The partition a task reads: its sub-topology's input topic, or its repartition topic, at the task's partition. */
    private def sourceOf(id: TaskId): F[TopicPartition] =
      segments(id.subtopology).origin match
        case Origin.Input(topic, _, _)         => partitionOf(topic.value, id.partition)
        case Origin.Repartitioned(named, _, _) => partitionOf(s"$application-${named.name}-repartition", id.partition)

    private def partitionOf(topic: String, partition: Int): F[TopicPartition] =
      (Topic.from(topic), Partition.from(partition)).mapN(TopicPartition(_, _)).leftMap(new KafkaException.InvalidValue(_)).liftTo[F]

    /** Sends `request` until `answered` gives a result, waiting from 100 ms, doubling up to a second, between tries, and also after a retriable
      * failure of the request itself.
      */
    private def untilAnswered[A, B](request: F[A])(answered: A => F[Option[B]]): F[B] =
      def attempt(wait: FiniteDuration): F[B] =
        request.attempt.flatMap:
          case Right(answer)                                                                => answered(answer).flatMap(_.fold(again(wait))(F.pure))
          case Left(error: KafkaException.BackendFailure) if error.retriable.contains(true) => again(wait)
          case Left(error)                                                                  => F.raiseError(error)
      def again(wait: FiniteDuration): F[B] = F.sleep(wait) *> attempt((wait * 2).min(MaxBackoff))
      attempt(Backoff)

    private def failure(what: String, code: Short): KafkaException =
      new KafkaException.BackendFailure(s"$what failed with error $code", Some(ErrorCode.fromProtocol(code.toInt)), Some(false))
