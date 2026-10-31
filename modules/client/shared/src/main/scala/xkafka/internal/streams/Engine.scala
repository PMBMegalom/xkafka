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
import cats.effect.{Async, Ref, Resource}
import cats.effect.std.{Queue, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.concurrent.SignallingRef
import driver.{KafkaDriver, PolledRecord, StartAt}
import group.{Connections, PlatformCrypto, PlatformSockets}
import protocol.{OffsetCommitRequest, OffsetFetchRequest, StreamsGroup}
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{KafkaStreams, Origin, ProcessingGuarantee, Segment, StreamsPhase, StreamsProblem, StreamsState, Topology}

/** Runs a streams program as one member of its application's group: the tasks the coordinator gives the member read their input partitions through
  * one consumer and write through one producer. At least once, they commit what they read through the member; exactly once, in the transaction that
  * writes what they wrote.
  */
private[xkafka] object Engine:
  /** How long each poll of the input waits for records. */
  val PollMillis = 100

  /** How often the engine commits by default under each guarantee, Java Streams' `commit.interval.ms` for it. */
  def commitInterval(guarantee: ProcessingGuarantee): FiniteDuration =
    guarantee match
      case ProcessingGuarantee.AtLeastOnce => 30.seconds
      case ProcessingGuarantee.ExactlyOnce => 100.millis

  /** How often the engine deletes the records of its repartition topics that it committed past, Java Streams' `repartition.purge.interval.ms`. */
  val PurgeInterval: FiniteDuration = 30.seconds

  /** How long the engine keeps the store of a standby task taken away, for its task to come back from there, Java Streams' `state.cleanup.delay.ms`,
    * for which it keeps a task's state directory.
    */
  val StateCleanupDelay: FiniteDuration = 10.minutes

  /** How long a transaction may stay open before the coordinator aborts it, Java Streams' `transaction.timeout.ms` under exactly-once. */
  val TransactionTimeout: FiniteDuration = 10.seconds

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

  /** How the coordinator refuses offsets a transaction records under an epoch the member is not at, either way, or an id the group does not have.
    */
  private val Refused: Set[ErrorCode] = Set(ErrorCode.IllegalGeneration, ErrorCode.fromProtocol(StaleEpoch.toInt), ErrorCode.UnknownMemberId)

  /** The engine of `application` running `topology` over the brokers `settings` name. Released, it gives back its tasks, committing them, and leaves
    * the group, waiting at most `closeTimeout`. `watch` lets a test see what the engine reads and writes.
    */
  def apply[F[_]](
      client: KafkaClient[F],
      driver: KafkaDriver[F],
      settings: ClientSettings,
      application: String,
      topology: Topology[F],
      guarantee: ProcessingGuarantee = ProcessingGuarantee.AtLeastOnce,
      commitInterval: Option[FiniteDuration] = None,
      concurrency: Int = 1,
      purgeInterval: FiniteDuration = PurgeInterval,
      closeTimeout: FiniteDuration = 30.seconds,
      watch: Plane[F] => Plane[F] = identity[Plane[F]]
  )(using F: Async[F]): Resource[F, KafkaStreams[F]] =
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
      admin    <- client.admin(settings)
      input    <- consumer(if guarantee == ProcessingGuarantee.ExactlyOnce then "read_committed" else "read_uncommitted", reportEnds = false)
      follower <- consumer("read_committed", reportEnds = false)
      bytes = Serializer.bytes[F].option
      producer <-
        ProducerSettings.from(settings, bytes, bytes, ClientProperties.producer).toEither.leftMap(new KafkaException.InvalidSettings(_))
          .liftTo[Resource[F, *]].flatMap(client.producer)
      transactions <-
        guarantee match
          case ProcessingGuarantee.AtLeastOnce => Resource.pure[F, Option[Transactions[F]]](None)
          case ProcessingGuarantee.ExactlyOnce =>
            // Java's form, the application id, the process id and the thread's index, for the one loop here.
            (TransactionalId.from(s"$application-$processId-1"), ConsumerGroup.from(application)).tupled.leftMap(new KafkaException.InvalidValue(_))
              .liftTo[Resource[F, *]].flatMap: (id, group) =>
                TransactionalProducerSettings.from(settings, id, bytes, bytes, TransactionTimeout, ClientProperties.producer).toEither
                  .leftMap(new KafkaException.InvalidSettings(_)).liftTo[Resource[F, *]].flatMap(client.transactionalProducer)
                  .map(producer => Some(transactional(driver, producer, member, group, memberId)))
      plane =
        new Plane[F]:
          def read(starts: Vector[StartAt]): F[Unit] = driver.assign(input, starts.map(_.partition), starts)
          def poll: F[List[PolledRecord]]            = driver.poll(input, PollMillis).map(_.records)
          def restore(
              changelogs: Map[TopicPartition, String],
              from: Map[TopicPartition, Long],
              held: Map[String, Map[Chunk[Byte], Stored]]
          ): F[Map[String, Map[Chunk[Byte], Stored]]]  = consumer("read_committed", reportEnds = true).use(Restore(driver, _, changelogs, from, held))
          def follow(starts: Vector[StartAt]): F[Unit] = driver.assign(follower, starts.map(_.partition), starts)
          def followed: F[List[PolledRecord]]          = driver.poll(follower, PollMillis).map(_.records)
          def ends(partitions: Vector[TopicPartition]): F[Map[TopicPartition, Long]] =
            partitions.traverse(partition => driver.watermarks(follower, partition, 30000).map(marks => partition -> marks._2)).map(_.toMap)
          def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): F[Unit] = producer.produceAndAwait(records).void
          def purge(before: Map[TopicPartition, Offset]): F[Unit]                                             =
            NonEmptyList.fromList(before.toList).traverse_(cuts => admin.deleteRecords(cuts.toNem).void)
      running <-
        val interval = commitInterval.getOrElse(Engine.commitInterval(guarantee))
        running(application, topology, member, watch(plane), transactions, interval, concurrency, purgeInterval, closeTimeout)
    yield running

  /** Transactions of `producer` that record offsets for `group` under the member's id and its epoch as each commits. */
  private def transactional[F[_]](
      driver: KafkaDriver[F],
      producer: KafkaTransactionalProducer[F, Option[Chunk[Byte]], Option[Chunk[Byte]]],
      member: GroupMember[F],
      group: ConsumerGroup,
      memberId: String
  )(using F: Async[F]): Transactions[F] =
    new Transactions[F]:
      def apply[A](body: Writer[F] => F[(A, Map[TopicPartition, OffsetAndMetadata])]): F[A] =
        producer.transactionally: transaction =>
          body(records => transaction.produce(records).void).flatMap: (result, offsets) =>
            if offsets.isEmpty then F.pure(result)
            else
              member.epoch.flatMap: epoch =>
                val named =
                  GroupMembership.Backend[F](
                    driver.groupMetadataFor(group, epoch, memberId).widen[GroupHandle],
                    handle => driver.asGroupMetadata(handle).traverse_(driver.destroyGroupMetadata)
                  )
                val committable =
                  offsets.toList.map: (partition, offset) =>
                    new CommittableOffset[F]:
                      override val topicPartition: TopicPartition                 = partition
                      override val nextOffset: Offset                             = offset.offset
                      override val leaderEpoch: Option[LeaderEpoch]               = offset.leaderEpoch
                      override val metadata: String                               = offset.metadata
                      override val committer: OffsetCommitter[F]                  = InTransaction[F]
                      override private[xkafka] def membership: GroupMembership[F] = named
                transaction.commitOffsets(CommittableOffsetBatch.fromFoldable(committable)).as(result)

  /** Offsets the engine records only in its transactions. */
  private final class InTransaction[F[_]](using F: Async[F]) extends OffsetCommitter[F]:
    override def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      F.raiseError(new IllegalStateException("the engine records these offsets only in its transactions"))

  /** Writes records, waiting until each is delivered. */
  type Writer[F[_]] = NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]] => F[Unit]

  /** Runs a body that writes records inside a transaction, which also records the offsets the body gives, then commits. A body that fails, or a
    * commit that does, aborts the transaction.
    */
  trait Transactions[F[_]]:
    def apply[A](body: Writer[F] => F[(A, Map[TopicPartition, OffsetAndMetadata])]): F[A]

  /** The engine of `application` running `topology` as `member`, reading and writing through `plane`. Released, it gives back its tasks through the
    * member, committing them, waiting at most `closeTimeout`.
    */
  def running[F[_]](
      application: String,
      topology: Topology[F],
      member: GroupMember[F],
      plane: Plane[F],
      transactions: Option[Transactions[F]],
      commitInterval: FiniteDuration,
      concurrency: Int,
      purgeInterval: FiniteDuration,
      closeTimeout: FiniteDuration
  )(using F: Async[F]): Resource[F, KafkaStreams[F]] =
    val segments = topology.pipelines.toList.flatMap(_.segments.toList).zipWithIndex.map((segment, id) => id.toString -> segment).toMap
    for
      changes  <- Resource.eval(Queue.unbounded[F, Either[Throwable, MemberAction]])
      _        <- member.changes.attempt.evalMap(changes.offer).compile.drain.background
      started  <- Resource.eval(F.monotonic)
      phase    <- Resource.eval(SignallingRef[F].of[StreamsPhase](StreamsPhase.Joining))
      standing <- Resource.eval(F.ref(Map.empty[TaskId, Standby]))
      _        <- Resource.eval(member.offsetsFrom(taskOffsets(standing, plane)))
      // Standby tasks read on a fiber of their own, so that waiting for their records holds up no processing; the Java consumer refuses to
      // poll with nothing assigned, and would not read in polls that do not wait.
      read      <- Resource.eval(Queue.unbounded[F, PolledRecord])
      following <- Resource.eval(F.ref(false))
      _         <-
        following.get.ifM(plane.followed.flatMap(_.traverse_(read.offer)), F.sleep(PollMillis.millis))
          // A poll can meet the follower just after its last standby went; the next goes on.
          .handleErrorWith(_ => F.sleep(PollMillis.millis)).foreverM.background
      loop =
        Loop(
          application,
          segments,
          member,
          plane,
          transactions,
          changes,
          commitInterval,
          concurrency,
          purgeInterval,
          phase,
          standing,
          read,
          following
        )
      running <-
        Resource.make(loop.run(Running(Map.empty, started, started)).start)(fiber => member.leave.timeout(closeTimeout).attempt *> fiber.cancel)
    yield new KafkaStreams[F]:
      def run: F[Unit]                   = running.joinWithNever
      def state: Stream[F, StreamsState] = (phase, member.reports).mapN((now, reported) => StreamsState(now, problemsOf(reported))).discrete.changes

  /** Where each standby task is in its changelogs, and where those end, each summed over the task's partitions, as Java Streams reports them. */
  private def taskOffsets[F[_]](standing: Ref[F, Map[TaskId, Standby]], plane: Plane[F])(using F: Async[F]): F[TaskOffsets] =
    standing.get.flatMap: standbys =>
      plane.ends(standbys.values.flatMap(_.changelogs.keys).toVector).map: ends =>
        val ordered                               = standbys.toList.sortBy((id, _) => (id.subtopology, id.partition))
        def summed(of: Standby => Iterable[Long]) =
          ordered.map((id, standby) => StreamsGroup.TaskOffset(id.subtopology, id.partition, of(standby).sum))
        TaskOffsets(summed(_.positions.values), summed(_.changelogs.keys.map(partition => ends.getOrElse(partition, 0L))))

  /** What the coordinator last reported wrong with the group, and why it refuses the member its topics, if it does. */
  private def problemsOf(member: Member): List[StreamsProblem] =
    member.statuses.map: status =>
      status.code.toInt match
        case 0    => StreamsProblem.StaleTopology(status.detail)
        case 1    => StreamsProblem.MissingSourceTopics(status.detail)
        case 2    => StreamsProblem.IncorrectlyPartitionedTopics(status.detail)
        case 3    => StreamsProblem.MissingInternalTopics(status.detail)
        case 4    => StreamsProblem.ShutdownRequested(status.detail)
        case 5    => StreamsProblem.AssignmentDelayed(status.detail)
        case 6    => StreamsProblem.MissingClientTags(status.detail)
        case code => StreamsProblem.Other(code, status.detail)
    ++ member.unauthorized.map(StreamsProblem.TopicAuthorizationFailed(_)).toList

  /** How the engine reads its tasks' input and changelogs and writes what they write. */
  trait Plane[F[_]]:
    /** Reads exactly these partitions, each from where it starts. */
    def read(starts: Vector[StartAt]): F[Unit]

    /** The next records of the partitions being read, waiting a little for some. */
    def poll: F[List[PolledRecord]]

    /** Each store's contents, read back from its changelog partition from where `from` says, or its start, onto what `held` holds. */
    def restore(
        changelogs: Map[TopicPartition, String],
        from: Map[TopicPartition, Long],
        held: Map[String, Map[Chunk[Byte], Stored]]
    ): F[Map[String, Map[Chunk[Byte], Stored]]]

    /** Follows exactly these changelog partitions for standby tasks, each from where it starts. */
    def follow(starts: Vector[StartAt]): F[Unit]

    /** The next changelog records followed, waiting a little for some. */
    def followed: F[List[PolledRecord]]

    /** Where each of these partitions ends. */
    def ends(partitions: Vector[TopicPartition]): F[Map[TopicPartition, Long]]

    /** Writes the records, waiting until each is delivered. */
    def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): F[Unit]

    /** Deletes the records of each partition before its offset, as far as the cluster lets it. */
    def purge(before: Map[TopicPartition, Offset]): F[Unit]

  /** A task the engine runs: its program, what it holds, the partition it reads, and where the next commit puts it, once it has read past its start.
    */
  private final case class Live[F[_]](
      task: Task[F],
      state: TaskState,
      input: TopicPartition,
      position: Long,
      read: Option[(Long, Int)],
      committed: Option[Long] = None,
      purged: Option[Long] = None
  ):
    /** Committed where it read to. */
    def settled: Live[F] = copy(read = None, committed = read.map(_._1).orElse(committed))

  /** A standby task: its stores, kept as their changelogs are read, the changelog partition of each, and where it has read each to. */
  private final case class Standby(
      stores: Map[String, Map[Chunk[Byte], Stored]],
      changelogs: Map[TopicPartition, String],
      positions: Map[TopicPartition, Long]
  )

  /** The tasks the engine runs and keeps standing by, and when it last committed and purged. */
  private final case class Running[F[_]](
      tasks: Map[TaskId, Live[F]],
      committed: FiniteDuration,
      purged: FiniteDuration,
      standbys: Map[TaskId, Standby] = Map.empty,
      kept: Map[TaskId, (Standby, FiniteDuration)] = Map.empty
  )

  private final class Loop[F[_]](
      application: String,
      segments: Map[String, Segment[F, ?, ?, ?, ?]],
      member: GroupMember[F],
      plane: Plane[F],
      transactions: Option[Transactions[F]],
      changes: Queue[F, Either[Throwable, MemberAction]],
      commitInterval: FiniteDuration,
      concurrency: Int,
      purgeInterval: FiniteDuration,
      phase: SignallingRef[F, StreamsPhase],
      standing: Ref[F, Map[TaskId, Standby]],
      read: Queue[F, PolledRecord],
      following: Ref[F, Boolean]
  )(using F: Async[F]):
    import MemberAction.*

    /** Carries out the changes the member told of since the last batch, reads, processes and writes until the next commit, then purges what is due.
      */
    def run(running: Running[F]): F[Unit] =
      changes.tryTakeN(None).flatMap(_.foldLeftM(running)(change)).flatMap(window).flatMap(purge).flatMap(follow).flatMap(run)

    /** Applies the changelog records read since the last batch to the standby tasks' stores. */
    private def follow(running: Running[F]): F[Running[F]] =
      if running.standbys.isEmpty then F.pure(running)
      else
        read.tryTakeN(None).flatMap: records =>
          val owners =
            running.standbys.toList.flatMap((id, standby) =>
              standby.changelogs.map((partition, store) => (partition.topic.value, partition.partition.value) -> (id, partition, store))
            ).toMap
          val after =
            records.foldLeft(running.standbys): (standbys, record) =>
              // A record read before its partition was followed again from further on was applied already.
              owners.get((record.topic, record.partition)).filter((id, partition, _) => record.offset >= standbys(id).positions(partition))
                .fold(standbys): (id, partition, store) =>
                  val standby = standbys(id)
                  standbys.updated(
                    id,
                    standby.copy(
                      stores = Restore.applied(standby.stores, store, record),
                      positions = standby.positions.updated(partition, record.offset + 1)
                    )
                  )
          val now = running.copy(standbys = after)
          report(now).as(now)

    /** At least once: one batch, then a commit through the member once the interval has passed. Exactly once: once a poll brings records, a
      * transaction that writes the batches read until the interval has passed or the member has news, with the offsets read up to there. A
      * transaction the coordinator refuses for the member's identity leaves the tasks to start again from what the group committed.
      */
    private def window(running: Running[F]): F[Running[F]] =
      transactions match
        case None => poll(running).flatMap(process(running, _, plane.write)).flatMap: after =>
            F.monotonic.flatMap: now =>
              if now - after.committed >= commitInterval then commit(after, after.tasks.keySet).map(_.copy(committed = now)) else F.pure(after)
        case Some(transaction) => poll(running).flatMap: polled =>
            if polled.isEmpty then F.pure(running)
            else
              def more(after: Running[F], write: Writer[F]): F[Running[F]] =
                (F.monotonic, changes.size).tupled.flatMap: (now, waiting) =>
                  if now - after.committed >= commitInterval || waiting > 0 then F.pure(after)
                  else poll(after).flatMap(process(after, _, write)).flatMap(more(_, write))
              transaction(write => process(running, polled, write).flatMap(more(_, write)).map(after => (after, offsetsOf(after))))
                .flatMap(after => F.monotonic.map(now => after.copy(tasks = after.tasks.view.mapValues(_.settled).toMap, committed = now)))
                .recoverWith:
                  case refused: KafkaException.BackendFailure if refused.code.exists(Refused) => restart(running)

    /** Once the purge interval has passed since the last purge, deletes the records of each repartition topic partition before where its task last
      * committed, where that moved since. Java Streams only logs a purge that fails, and tries those records again at the next.
      */
    private def purge(running: Running[F]): F[Running[F]] =
      F.monotonic.flatMap: now =>
        val due =
          running.tasks.toList.flatMap: (id, live) =>
            live.committed.filter(next => repartitioned(live) && !live.purged.contains(next)).flatMap(Offset.from(_).toOption).map(id -> _)
        if now - running.purged < purgeInterval || due.isEmpty then F.pure(running)
        else
          plane.purge(due.map((id, offset) => running.tasks(id).input -> offset).toMap).attempt.map:
            case Right(_) => running
                .copy(tasks = running.tasks ++ due.map((id, offset) => id -> running.tasks(id).copy(purged = Some(offset.value))), purged = now)
            case Left(_) => running.copy(purged = now)

    private def repartitioned(live: Live[F]): Boolean =
      segments.values.exists:
        case Segment(Origin.Repartitioned(named, _, _), _, _) => live.input.topic.value == s"$application-${named.name}-repartition"
        case _                                                => false

    /** The offset after the last record each task read since the last commit, with that record's leader epoch and the task's partition time. */
    private def offsetsOf(running: Running[F]): Map[TopicPartition, OffsetAndMetadata] =
      running.tasks.values.toList.flatMap: live =>
        live.read.map: (next, leaderEpoch) =>
          live.input ->
            OffsetAndMetadata(Offset.from(next).toOption.get, LeaderEpoch.from(leaderEpoch).toOption, PartitionTime.encode(live.state.partitionTime))
      .toMap

    /** Starts every task again from what the group committed, dropping what it held and where it read to since. */
    private def restart(running: Running[F]): F[Running[F]] =
      (start(running.tasks.keys.toList, Map.empty), F.monotonic).tupled.flatMap: (started, now) =>
        val again = running.copy(tasks = started, committed = now)
        reassign(again).as(again)

    private def change(running: Running[F], told: Either[Throwable, MemberAction]): F[Running[F]] =
      told.liftTo[F].flatMap:
        case Revoke(round, tasks) => commit(running, tasks).flatMap: committed =>
            val kept = committed.copy(tasks = committed.tasks -- tasks)
            reassign(kept) *> member.revoked(round).as(kept)
        case Assign(round, assignment) =>
          val kept  = running.tasks.view.filterKeys(assignment.active).toMap
          val added = (assignment.active -- kept.keySet).toList
          F.monotonic.flatMap: now =>
            val stillKept = running.kept.filter((_, held) => now - held._2 < StateCleanupDelay)
            val available = running.standbys ++ stillKept.view.mapValues(_._1)
            phase.set(StreamsPhase.Restoring).whenA(added.exists(id => segments(id.subtopology).chain.stores.nonEmpty)) *>
              start(added, available).flatMap: started =>
                standby((assignment.standby ++ assignment.warmup) -- assignment.active, available).flatMap: standbys =>
                  val dropped = running.standbys.view.filterKeys(id => !standbys.contains(id) && !assignment.active(id)).mapValues(_ -> now)
                  val keeping = (stillKept ++ dropped).view.filterKeys(id => !standbys.contains(id) && !assignment.active(id)).toMap
                  val after   = running.copy(tasks = kept ++ started, standbys = standbys, kept = keeping)
                  reassign(after) *> refollow(running, after) *> member.assigned(round) *> phase.set(StreamsPhase.Running).as(after)
        case LoseAll => F.monotonic.flatMap: now =>
            val lost =
              running.copy(tasks = Map.empty[TaskId, Live[F]], standbys = Map.empty, kept = running.kept ++ running.standbys.view.mapValues(_ -> now))
            phase.set(StreamsPhase.Lost) *> reassign(lost) *> refollow(running, lost).as(lost)
        case _ => F.pure(running)

    /** The standby tasks of `wanted` that keep stores, those already standing by as they are and the others from the start of their changelogs. */
    private def standby(wanted: Set[TaskId], standing: Map[TaskId, Standby]): F[Map[TaskId, Standby]] =
      wanted.toList.filter(id => segments(id.subtopology).chain.stores.nonEmpty).traverse: id =>
        standing.get(id).fold(changelogsOf(id).map(changelogs => Standby(Map.empty, changelogs, changelogs.keys.map(_ -> 0L).toMap)))(F.pure)
          .map(id -> _)
      .map(_.toMap)

    /** Follows the standby tasks' changelogs, where the tasks standing by changed. */
    private def refollow(before: Running[F], after: Running[F]): F[Unit] =
      val starts = after.standbys.values.toVector.flatMap(_.positions.map((partition, offset) => StartAt(partition, offset, -1)))
      // The fiber reading for standbys stops before the follower has nothing to read, and starts once it has.
      val refollowing = if after.standbys.isEmpty then following.set(false) *> plane.follow(starts) else plane.follow(starts) *> following.set(true)
      refollowing.whenA(before.standbys.keySet != after.standbys.keySet) *> report(after)

    /** The stores the member reports the task offsets of: those standing by and those kept. */
    private def report(running: Running[F]): F[Unit] = standing.set(running.standbys ++ running.kept.view.mapValues(_._1))

    /** The changelog partitions of a task's stores, with the store each backs. */
    private def changelogsOf(id: TaskId): F[Map[TopicPartition, String]] =
      segments(id.subtopology).chain.stores.traverse(store => partitionOf(s"$application-${store.name}-changelog", id.partition).map(_ -> store.name))
        .map(_.toMap)

    /** Reads each partition its task holds from where that task is. */
    private def reassign(running: Running[F]): F[Unit] = plane.read(running.tasks.values.toVector.map(task => StartAt(task.input, task.position, -1)))

    /** Starts tasks where the group last committed them, or at the start of their partitions, with their stores refilled from their changelogs. */
    private def start(tasks: List[TaskId], standbys: Map[TaskId, Standby]): F[Map[TaskId, Live[F]]] =
      tasks.traverse(id => sourceOf(id).map(id -> _)).flatMap: inputs =>
        val asked = inputs.groupMap(_._2.topic.value)(_._2.partition.value).toList.map(OffsetFetchRequest.Topic(_, _))
        if inputs.isEmpty then F.pure(Map.empty)
        else
          (stored(asked), restore(tasks, standbys)).tupled.flatMap: (offsets, stores) =>
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

    /** Each task's stores, read back from the task's partition of their changelogs, one task after another: a task that stood by from where it read
      * to, onto what it holds, and any other from the start.
      */
    private def restore(tasks: List[TaskId], standbys: Map[TaskId, Standby]): F[Map[TaskId, Map[String, Map[Chunk[Byte], Stored]]]] =
      val stateful = tasks.filter(id => segments(id.subtopology).chain.stores.nonEmpty)
      if stateful.isEmpty then F.pure(Map.empty)
      else
        stateful.traverse: id =>
          val held = standbys.get(id)
          changelogsOf(id).flatMap(changelogs => plane.restore(changelogs, held.fold(Map.empty)(_.positions), held.fold(Map.empty)(_.stores)))
            .map(id -> _)
        .map(_.toMap)

    /** Commits where each of these tasks has read to, with its partition time, asking again while the coordinator is not ready. A member the group no
      * longer counts commits nothing: its tasks go to others, which read again from the group's last commit.
      */
    private def commit(running: Running[F], tasks: Set[TaskId]): F[Running[F]] =
      val due    = running.tasks.view.filterKeys(tasks).toList.filter(_._2.read.nonEmpty)
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
      if topics.isEmpty then F.pure(running) else sent.as(running.copy(tasks = running.tasks ++ due.map((id, live) => id -> live.settled)))

    /** The next records, or none after a wait while no task runs, since the Java consumer refuses to poll with nothing assigned. */
    private def poll(running: Running[F]): F[List[PolledRecord]] = if running.tasks.isEmpty then F.sleep(PollMillis.millis).as(Nil) else plane.poll

    /** Runs each task's records through it in order, up to `concurrency` tasks at once, and writes what the tasks wrote, waiting until every record
      * is delivered.
      */
    private def process(running: Running[F], polled: List[PolledRecord], write: Writer[F]): F[Running[F]] =
      val routes = running.tasks.map((id, live) => (live.input.topic.value, live.input.partition.value) -> id)
      val byTask = polled.flatMap(record => routes.get((record.topic, record.partition)).map(_ -> record)).groupMap(_._1)(_._2)
      byTask.toList.parTraverseN(concurrency): (id, records) =>
        records.foldLeftM((running.tasks(id), Vector.empty[TaskOutput])):
          case ((live, written), record) => live.task.process(live.state, inputOf(record)).map: (state, out) =>
              (live.copy(state = state, position = record.offset + 1, read = Some((record.offset + 1, record.leaderEpoch))), written ++ out)
        .map(id -> _)
      .flatMap: done =>
        val written = done.flatMap(_._2._2)
        written.traverse(recordOf).flatMap(records => NonEmptyList.fromList(records).traverse_(write))
          .as(running.copy(tasks = running.tasks ++ done.map((id, ran) => id -> ran._1)))

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
