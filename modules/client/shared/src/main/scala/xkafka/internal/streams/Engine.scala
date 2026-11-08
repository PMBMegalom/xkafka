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

import cats.Functor
import cats.data.{NonEmptyList, NonEmptySet}
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
import xkafka.streams.{GlobalStore, KafkaStreams, ProcessingGuarantee, Source, StreamsPhase, StreamsProblem, StreamsState, Subtopology, Topology}

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
  private val Refused: Set[ErrorCode] = Set(ErrorCode.IllegalGeneration, ErrorCode.StaleMemberEpoch, ErrorCode.UnknownMemberId)

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
      topologyDescriptionPush: Boolean = true,
      stateDirectory: Option[String] = None,
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
      state     <- stateDirectory.traverse(PlatformStateDirectory[F](_, application))
      memberId  <- Resource.eval(GroupMember.memberId[F])
      processId <- Resource.eval(UUIDGen[F].randomUUID.map(_.toString).flatMap(fresh => state.fold(F.pure(fresh))(_.processId(fresh))))
      identity =
        MemberIdentity(
          application,
          memberId,
          processId,
          Declaration(application, topology),
          RebalanceTimeout.toMillis.toInt,
          None,
          Nil,
          None,
          Option.when(topologyDescriptionPush)(Declaration.described(application, topology))
        )
      admin  <- client.admin(settings)
      counts <- Resource.eval(F.ref(Map.empty[Topic, Int]))
      // Asked once for each topic, which the engine writes to for as long as it runs.
      partitions =
        (topic: Topic) =>
          counts.get.map(_.get(topic)).flatMap:
            case Some(count) => F.pure(count)
            case None        => admin.describeTopics(NonEmptySet.one(topic)).flatMap: described =>
                described(topic)
                  .fold(F.raiseError[TopicDescription](new IllegalStateException(s"topic ${topic.value} was not described")))(_.liftTo[F])
                  .map(_.partitions.length.toInt).flatTap(count => counts.update(_.updated(topic, count)))
      isolation = if guarantee == ProcessingGuarantee.ExactlyOnce then "read_committed" else "read_uncommitted"
      // As Java's, global tables are read to their ends before the member joins, in which time it would be given work it could not do.
      globals <-
        topology.globals.map(_._2) match
          case Nil    => Resource.pure[F, Option[GlobalStores.Held[F]]](None)
          case tables => consumer(isolation, reportEnds = true).flatMap(GlobalStores(driver, _, tables, partitions, globalStores(state, tables)))
              .map(Some(_))
      member   <- GroupMember(connections.coordinator(application), identity, closeTimeout)
      input    <- consumer(isolation, reportEnds = false)
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
          def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], stores: Map[String, TaskStore[F]]): F[Unit] =
            consumer("read_committed", reportEnds = true).use(Restore(driver, _, changelogs, from, stores))
          def follow(starts: Vector[StartAt]): F[Unit]                               = driver.assign(follower, starts.map(_.partition), starts)
          def followed: F[List[PolledRecord]]                                        = driver.poll(follower, PollMillis).map(_.records)
          def ends(partitions: Vector[TopicPartition]): F[Map[TopicPartition, Long]] =
            partitions.traverse(partition => driver.watermarks(follower, partition, 30000).map(marks => partition -> marks._2)).map(_.toMap)
          def write(records: NonEmptyList[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]]): F[Unit] = producer.produceAndAwait(records).void
          def purge(before: Map[TopicPartition, Offset]): F[Unit]                                             =
            NonEmptyList.fromList(before.toList).traverse_(cuts => admin.deleteRecords(cuts.toNem).void)
      running <-
        val interval = commitInterval.getOrElse(Engine.commitInterval(guarantee))
        running(
          application,
          topology,
          member,
          watch(plane),
          transactions,
          interval,
          concurrency,
          purgeInterval,
          closeTimeout,
          partitions,
          TaskStore.memory[F],
          globals,
          state
        )
    yield running

  /** Opens the global tables' stores: on disk where `state` is given, unless a table's store is kept in memory. */
  private def globalStores[F[_]: Async](state: Option[StateDirectory[F]], tables: List[GlobalStore[F]]): TaskStore.Open[F] =
    val inMemory = tables.filter(_.store.keptInMemory).map(_.store.value).toSet
    (name, time, held) =>
      state.filterNot(_ => inMemory(name)).fold(TaskStore.memory[F].apply(name, time, held))(_.open(StateDirectory.Global, name, time))

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
      closeTimeout: FiniteDuration,
      partitions: Topic => F[Int],
      open: TaskStore.Open[F],
      globals: Option[GlobalStores.Held[F]] = None,
      state: Option[StateDirectory[F]] = None
  )(using F: Async[F]): Resource[F, KafkaStreams[F]] =
    val subtopologies = topology.numbered.map((id, subtopology) => id.toString -> subtopology).toMap
    val unordered     =
      Declaration.unordered(application, topology).map: topics =>
        StreamsProblem.JavaMembersMayBeRefused(s"Java Streams may list the joined topics ${topics.mkString(", ")} in another order")
    // Java numbers a join's sub-topologies by which of its inputs the program builds first, which xkafka takes to be the join's receiver.
    val buildOrdered =
      topology.subtopologies.flatMap(_.buildOrdered).distinct.map: sources =>
        val topics = sources.map(Task.topicOf(application, _)).mkString(", ")
        StreamsProblem
          .JavaMembersMayBeRefused(s"Java Streams numbers the sub-topologies of the join of $topics otherwise unless it builds the receiver first")
    val unstartable =
      topology.subtopologies.flatMap(_.unstartable).map: sources =>
        val topics = sources.map(Task.topicOf(application, _)).mkString(", ")
        StreamsProblem.JavaMembersMayBeRefused(s"Java Streams cannot start a cogroup of $topics, since a group goes through a repartition topic")
    // Java numbers a global table by where its program makes it, which xkafka takes to be after the stream that first joins it.
    val globalOrdered =
      topology.globals.map: (_, global) =>
        StreamsProblem.JavaMembersMayBeRefused(s"Java Streams numbers the sub-topologies around the global table of ${global.topic
            .value} otherwise unless it builds it after the stream that first joins it")
    for
      changes  <- Resource.eval(Queue.unbounded[F, Either[Throwable, MemberAction]])
      _        <- member.changes.attempt.evalMap(changes.offer).compile.drain.background
      started  <- Resource.eval(F.monotonic)
      phase    <- Resource.eval(SignallingRef[F].of[StreamsPhase](StreamsPhase.Joining))
      standing <- Resource.eval(F.ref(Map.empty[TaskId, Standby[F]]))
      _        <- Resource.eval(member.offsetsFrom(taskOffsets(standing, plane)))
      // Standby tasks read on a fiber of their own, so that waiting for their records holds up no processing; the Java consumer refuses to
      // poll with nothing assigned, and would not read in polls that do not wait.
      read      <- Resource.eval(Queue.unbounded[F, PolledRecord])
      following <- Resource.eval(F.ref(false))
      stopping  <- Resource.eval(F.ref(false))
      // Released, the fiber is asked to stop and ends after its poll, so that no poll is cut short before the follower is destroyed.
      _ <-
        def reading: F[Unit] =
          stopping.get.ifM(
            F.unit,
            following.get.ifM(plane.followed.flatMap(_.traverse_(read.offer)), F.sleep(PollMillis.millis))
              // A poll can meet the follower just after its last standby went; the next goes on.
              .handleErrorWith(_ => F.sleep(PollMillis.millis)) *> reading
          )
        Resource.make(reading.start)(fiber => stopping.set(true) *> fiber.join.void)
      opened <- Resource.eval(F.ref(Map.empty[TaskId, Task[F]]))
      kept   <- Resource.eval(state.fold(F.pure(Map.empty[TaskId, Map[String, Long]]))(_.held.map(Engine.heldTasks)))
      stores <- Resource.eval(F.ref(Map.empty[TaskId, Map[String, TaskStore[F]]]))
      loop =
        Loop(
          application,
          subtopologies,
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
          following,
          partitions,
          globals.fold(Map.empty[String, TaskStore[F]])(_.stores),
          open,
          opened,
          state,
          stores
        )
      running <-
        // The tasks still open once the loop has stopped close with the engine, and with them their processors' instances.
        Resource.make(loop.begin(Running(Map.empty, started, started), kept).start): fiber =>
          member.leave.timeout(closeTimeout).attempt *> fiber.cancel *> opened.getAndSet(Map.empty).flatMap(_.values.toList.traverse_(_.close)) *>
            stores.getAndSet(Map.empty).flatMap(_.values.toList.flatMap(_.values).traverse_(_.close))
    yield new KafkaStreams[F]:
      def run: F[Unit]                   = globals.fold(running.joinWithNever)(held => F.race(running.joinWithNever, held.failure).void)
      def state: Stream[F, StreamsState] =
        (phase, member.reports)
          .mapN((now, reported) => StreamsState(now, problemsOf(reported) ++ unordered ++ buildOrdered ++ unstartable ++ globalOrdered)).discrete
          .changes

  /** A task's records, taken partition by partition in the order each was read, as Java's partition group takes them: next the record with the
    * smallest timestamp at the head of its partition, the earliest read of those with equal timestamps.
    */
  private[streams] def byTime[P](records: List[(P, PolledRecord)]): List[(P, PolledRecord)] =
    val queues = records.zipWithIndex.groupBy(_._1._1).values.map(_.sortBy(_._2)).toList
    @scala.annotation.tailrec
    def next(queues: List[List[((P, PolledRecord), Int)]], taken: List[(P, PolledRecord)]): List[(P, PolledRecord)] =
      queues.filter(_.nonEmpty) match
        case Nil       => taken.reverse
        case remaining =>
          val head = remaining.minBy(queue => (queue.head._1._2.timestamp.getOrElse(-1L), queue.head._2))
          next(remaining.map(queue => if queue eq head then queue.tail else queue), head.head._1 :: taken)
    next(queues, Nil)

  /** The tasks a state directory holds stores of, by the id each was recorded under, which names a task as `<sub-topology>_<partition>`. */
  private[streams] def heldTasks(held: Map[String, Map[String, Long]]): Map[TaskId, Map[String, Long]] =
    held.flatMap: (owner, positions) =>
      owner.split('_') match
        case Array(subtopology, partition) => partition.toIntOption.map(TaskId(subtopology, _) -> positions)
        case _                             => None

  /** The name a task's stores go under in a state directory. */
  private[streams] def ownerOf(id: TaskId): String = s"${id.subtopology}_${id.partition}"

  /** Where each standby task is in its changelogs, and where those end, each summed over the task's partitions, as Java Streams reports them. */
  private def taskOffsets[F[_]](standing: Ref[F, Map[TaskId, Standby[F]]], plane: Plane[F])(using F: Async[F]): F[TaskOffsets] =
    standing.get.flatMap: standbys =>
      plane.ends(standbys.values.flatMap(_.changelogs.keys).toVector).map: ends =>
        val ordered                                  = standbys.toList.sortBy((id, _) => (id.subtopology, id.partition))
        def summed(of: Standby[F] => Iterable[Long]) =
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

    /** Reads each store's changelog partition from where `from` says, or its start, into the store. */
    def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], stores: Map[String, TaskStore[F]]): F[Unit]

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

  /** A partition a task reads: where it reads next, where the next commit puts it once the task has read past its start, with the leader epoch of the
    * last record read, where the last commit put it, and where its records were last purged before.
    */
  private final case class Input(position: Long, read: Option[(Long, Int)] = None, committed: Option[Long] = None, purged: Option[Long] = None):
    /** Committed where it read to. */
    def settled: Input = copy(read = None, committed = read.map(_._1).orElse(committed))

  /** A task the engine runs: its program, what it holds, and each partition it reads, one of each topic its sub-topology reads. */
  private final case class Live[F[_]](
      task: Task[F],
      state: TaskState,
      inputs: Map[TopicPartition, Input],
      stores: Map[String, TaskStore[F]] = Map.empty[String, TaskStore[F]]
  ):
    /** Committed where it read to. */
    def settled: Live[F] = copy(inputs = inputs.view.mapValues(_.settled).toMap)

    /** With what it wrote since its last commit in its stores. */
    def stored(using F: Functor[F]): F[Live[F]] = task.stored(state).map(after => copy(state = after))

    /** Whether it read anything since its last commit. */
    def read: Boolean = inputs.values.exists(_.read.nonEmpty)

  /** A standby task: its stores, kept as their changelogs are read, the changelog partition of each, and where it has read each to. */
  private final case class Standby[F[_]](
      stores: Map[String, TaskStore[F]],
      changelogs: Map[TopicPartition, String],
      positions: Map[TopicPartition, Long]
  )

  /** The tasks the engine runs and keeps standing by, and when it last committed and purged. */
  private final case class Running[F[_]](
      tasks: Map[TaskId, Live[F]],
      committed: FiniteDuration,
      purged: FiniteDuration,
      standbys: Map[TaskId, Standby[F]] = Map.empty[TaskId, Standby[F]],
      kept: Map[TaskId, (Standby[F], FiniteDuration)] = Map.empty[TaskId, (Standby[F], FiniteDuration)]
  )

  private final class Loop[F[_]](
      application: String,
      subtopologies: Map[String, Subtopology[F]],
      member: GroupMember[F],
      plane: Plane[F],
      transactions: Option[Transactions[F]],
      changes: Queue[F, Either[Throwable, MemberAction]],
      commitInterval: FiniteDuration,
      concurrency: Int,
      purgeInterval: FiniteDuration,
      phase: SignallingRef[F, StreamsPhase],
      standing: Ref[F, Map[TaskId, Standby[F]]],
      read: Queue[F, PolledRecord],
      following: Ref[F, Boolean],
      partitions: Topic => F[Int],
      globals: Map[String, TaskStore[F]],
      open: TaskStore.Open[F],
      opened: Ref[F, Map[TaskId, Task[F]]],
      state: Option[StateDirectory[F]],
      holding: Ref[F, Map[TaskId, Map[String, TaskStore[F]]]]
  )(using F: Async[F]):
    import MemberAction.*

    /** Closes the tasks given, which no longer run here, and with them their processors' instances, as Java closes a task it gives up. */
    private def closed(tasks: Iterable[(TaskId, Live[F])]): F[Unit] =
      tasks.toList.traverse_((id, live) => live.task.close *> released(id, live.stores, settled = false).void) *> opened.update(_ -- tasks.map(_._1))

    /** Closes the stores of a task given up here. `settled`, where the task has just committed, so that its stores hold all it wrote to their
      * changelogs, each store records the end of its changelog partition first. Where stores are kept on disk, the task's are then recorded as given
      * up, with where each holds its changelog to, which is what the task is kept as, for the group to give back.
      */
    private def released(id: TaskId, stores: Map[String, TaskStore[F]], settled: Boolean): F[Option[Standby[F]]] =
      if stores.isEmpty then F.pure(None)
      else
        changelogsOf(id).flatMap: changelogs =>
          val settling =
            if settled then
              plane.ends(changelogs.keys.toVector)
                .flatMap(_.toList.traverse_((partition, end) => stores(changelogs(partition)).write(StoreContents.unchanged, Some(end))))
            else F.unit
          settling *> changelogs.toList.traverse((partition, store) => stores(store).position.map(at => (partition, store, at))).flatMap: positions =>
            stores.values.toList.traverse_(_.close) *> holding.update(_ - id) *> state.traverse: directory =>
              directory.released(ownerOf(id), positions.flatMap((_, store, at) => at.map(store -> _)).toMap)
                .as(Standby(Map.empty, changelogs, positions.map((partition, _, at) => partition -> at.getOrElse(0L)).toMap))

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
          // A record read before its partition was followed again from further on was applied already.
          val (positions, fresh) =
            records.foldLeft((running.standbys.view.mapValues(_.positions).toMap, Vector.empty[(TaskId, String, PolledRecord)])):
              case ((at, taken), record) =>
                owners.get((record.topic, record.partition)).filter((id, partition, _) => record.offset >= at(id)(partition)).fold((at, taken)):
                  (id, partition, store) => (at.updated(id, at(id).updated(partition, record.offset + 1)), taken :+ (id, store, record))
          // Expired windows go once the batch is applied, as Java's restore drops them after each batch it reads.
          fresh.groupBy(_._1).toList.traverse_ { (id, taken) =>
            val standby = running.standbys(id)
            val stores  = standby.stores
            val reached = standby.changelogs.flatMap((partition, store) => positions(id).get(partition).map(store -> _))
            Restore.written(stores, Restore.changes(taken.map((_, store, record) => store -> record)), reached) *>
              WindowStore.swept(stores, subtopologies(id.subtopology).retentions)
          } *> {
            val now = running.copy(standbys = running.standbys.map((id, standby) => id -> standby.copy(positions = positions(id))))
            report(now).as(now)
          }

    /** At least once: one batch, then a commit through the member once the interval has passed. Exactly once: once a poll brings records, a
      * transaction that writes the batches read until the interval has passed or the member has news, with the offsets read up to there. A
      * transaction the coordinator refuses for the member's identity leaves the tasks to start again from what the group committed.
      */
    private def window(running: Running[F]): F[Running[F]] =
      transactions match
        case None => poll(running).flatMap(process(running, _, plane.write)).flatMap: after =>
            F.monotonic.flatMap: now =>
              if now - after.committed >= commitInterval then commit(after, after.tasks.keySet).map(_.copy(committed = now)) else F.pure(after)
        case Some(transaction) => (poll(running), F.realTime).tupled.flatMap: (polled, clock) =>
            // A punctuator that is due runs without records, in a transaction of its own that records no offsets.
            if polled.isEmpty && !running.tasks.values.exists(live => live.task.due(live.state, clock.toMillis)) then F.pure(running)
            else
              def more(after: Running[F], write: Writer[F]): F[Running[F]] =
                (F.monotonic, changes.size).tupled.flatMap: (now, waiting) =>
                  if now - after.committed >= commitInterval || waiting > 0 then F.pure(after)
                  else poll(after).flatMap(process(after, _, write)).flatMap(more(_, write))
              transaction(write =>
                process(running, polled, write).flatMap(more(_, write)).flatMap(flushed(_, write)).map(after => (after, offsetsOf(after)))
              ).flatMap(after =>
                (after.tasks.toList.traverse((id, live) => live.stored.map(id -> _.settled)), F.monotonic)
                  .mapN((tasks, now) => after.copy(tasks = tasks.toMap, committed = now))
              ).recoverWith:
                case refused: KafkaException.BackendFailure if refused.code.exists(Refused) => restart(running)

    /** Once the purge interval has passed since the last purge, deletes the records of each repartition topic partition before where its task last
      * committed, where that moved since. Java Streams only logs a purge that fails, and tries those records again at the next.
      */
    private def purge(running: Running[F]): F[Running[F]] =
      F.monotonic.flatMap: now =>
        val due =
          running.tasks.toList.flatMap: (id, live) =>
            live.inputs.toList.flatMap: (partition, input) =>
              input.committed.filter(next => repartitioned(partition) && !input.purged.contains(next)).flatMap(Offset.from(_).toOption)
                .map((id, partition, _))
        if now - running.purged < purgeInterval || due.isEmpty then F.pure(running)
        else
          plane.purge(due.map((_, partition, offset) => partition -> offset).toMap).attempt.map:
            case Right(_) =>
              val purged =
                due.foldLeft(running.tasks): (tasks, done) =>
                  val (id, partition, offset) = done
                  val live                    = tasks(id)
                  tasks.updated(id, live.copy(inputs = live.inputs.updated(partition, live.inputs(partition).copy(purged = Some(offset.value)))))
              running.copy(tasks = purged, purged = now)
            case Left(_) => running.copy(purged = now)

    private def repartitioned(partition: TopicPartition): Boolean =
      subtopologies.values.flatMap(_.origins).exists:
        case Source.Input(_) => false
        case internal        => partition.topic.value == Task.topicOf(application, internal)

    /** The offset after the last record each task read since the last commit, with that record's leader epoch and the task's partition time. */
    private def offsetsOf(running: Running[F]): Map[TopicPartition, OffsetAndMetadata] =
      running.tasks.values.toList.flatMap: live =>
        live.inputs.toList.flatMap: (partition, input) =>
          input.read.map: (next, leaderEpoch) =>
            partition -> OffsetAndMetadata(
              Offset.from(next).toOption.get,
              LeaderEpoch.from(leaderEpoch).toOption,
              PartitionTime.encode(live.state.partitionTime(partition.topic.value), live.state.lastEmitted)
            )
      .toMap

    /** Starts every task again from what the group committed, dropping what it held and where it read to since. */
    private def restart(running: Running[F]): F[Running[F]] =
      (closed(running.tasks.toList) *> start(running.tasks.keys.toList, Map.empty), F.monotonic).tupled.flatMap: (started, now) =>
        val again = running.copy(tasks = started, committed = now)
        reassign(again).as(again)

    private def change(running: Running[F], told: Either[Throwable, MemberAction]): F[Running[F]] =
      told.liftTo[F].flatMap:
        case Revoke(round, tasks) => commit(running, tasks).flatMap(gaveUp(_, tasks))
            .flatTap(after => reassign(after) *> report(after) *> member.revoked(round))
        case Assign(round, assignment) =>
          val kept  = running.tasks.view.filterKeys(assignment.active).toMap
          val added = (assignment.active -- kept.keySet).toList
          closed(running.tasks.view.filterKeys(id => !assignment.active(id)).toList) *> F.monotonic.flatMap: now =>
            val stillKept = running.kept.filter((_, held) => now - held._2 < StateCleanupDelay)
            val available = running.standbys ++ stillKept.view.mapValues(_._1)
            phase.set(StreamsPhase.Restoring).whenA(added.exists(id => subtopologies(id.subtopology).stores.nonEmpty)) *>
              start(added, available).flatMap: started =>
                standby((assignment.standby ++ assignment.warmup) -- assignment.active, available).flatMap: standbys =>
                  val gone = running.standbys.view.filterKeys(id => !standbys.contains(id) && !assignment.active(id)).toList
                  dropped(gone).flatMap: left =>
                    val keeping = (stillKept ++ left.view.mapValues(_ -> now)).view.filterKeys(id => !standbys.contains(id) && !assignment.active(id))
                      .toMap
                    val after = running.copy(tasks = kept ++ started, standbys = standbys, kept = keeping)
                    val owned = (after.tasks.keySet ++ after.standbys.keySet).map(ownerOf)
                    reassign(after) *> refollow(running, after) *> member.assigned(round) *> phase.set(StreamsPhase.Running) *>
                      state.traverse_(_.cleaned(owned, StateCleanupDelay)).as(after)
        case LoseAll => closed(running.tasks.toList) *> F.monotonic.flatMap: now =>
            val lost =
              running.copy(tasks = Map.empty[TaskId, Live[F]], standbys = Map.empty, kept = running.kept ++ running.standbys.view.mapValues(_ -> now))
            phase.set(StreamsPhase.Lost) *> reassign(lost) *> refollow(running, lost).as(lost)
        case _ => F.pure(running)

    /** `running` without `tasks`, which have just committed and are given up: closed, and where their stores are on disk, kept as they are there. */
    private def gaveUp(running: Running[F], tasks: Set[TaskId]): F[Running[F]] =
      val giving = running.tasks.view.filterKeys(tasks).toList
      (giving.traverse((id, live) => live.task.close *> released(id, live.stores, settled = true).map(_.map(id -> _))), F.monotonic).tupled.flatMap:
        (held, now) =>
          opened.update(_ -- tasks)
            .as(running.copy(tasks = running.tasks -- tasks, kept = running.kept ++ held.flatten.map((id, kept) => id -> (kept, now))))

    /** The standby tasks no longer wanted, as they are kept: those whose stores are in memory as they are, and the others with their stores, on disk,
      * closed and recorded as given up.
      */
    private def dropped(gone: List[(TaskId, Standby[F])]): F[Map[TaskId, Standby[F]]] =
      gone.traverse: (id, standby) =>
        if state.isEmpty then F.pure(id -> standby) else released(id, standby.stores, settled = false).map(held => id -> held.getOrElse(standby))
      .map(_.toMap)

    /** Runs from `running`, keeping the tasks a state directory holds stores of as an earlier run left them, as given up now, and reporting them. */
    def begin(running: Running[F], held: Map[TaskId, Map[String, Long]]): F[Unit] =
      restarted(held).map(kept => running.copy(kept = kept)).flatTap(report).flatMap(run)

    /** The tasks a state directory holds stores of as an earlier run left them, kept as given up then, of the sub-topologies this program has. */
    private def restarted(held: Map[TaskId, Map[String, Long]]): F[Map[TaskId, (Standby[F], FiniteDuration)]] =
      F.monotonic.flatMap: now =>
        held.toList.filter((id, _) => subtopologies.contains(id.subtopology)).traverse: (id, positions) =>
          changelogsOf(id).map(changelogs =>
            id ->
              (
                Standby(
                  Map.empty[String, TaskStore[F]],
                  changelogs,
                  changelogs.map((partition, store) => partition -> positions.getOrElse(store, 0L))
                ),
                now
              )
          )
        .map(_.toMap)

    /** The standby tasks of `wanted` that keep stores, those already standing by as they are and the others from the start of their changelogs. */
    private def standby(wanted: Set[TaskId], standing: Map[TaskId, Standby[F]]): F[Map[TaskId, Standby[F]]] =
      wanted.toList.filter(id => subtopologies(id.subtopology).stores.nonEmpty).traverse: id =>
        standing.get(id).filter(_.stores.nonEmpty).fold(
          (changelogsOf(id), storesOf(id)).tupled.flatMap: (changelogs, stores) =>
            changelogs.toList.traverse((partition, store) => stores(store).position.map(at => partition -> at.getOrElse(0L)))
              .map(positions => Standby(stores, changelogs, positions.toMap))
        )(F.pure).map(id -> _)
      .map(_.toMap)

    /** Follows the standby tasks' changelogs, where the tasks standing by changed. */
    private def refollow(before: Running[F], after: Running[F]): F[Unit] =
      val starts = after.standbys.values.toVector.flatMap(_.positions.map((partition, offset) => StartAt(partition, offset, -1)))
      // The fiber reading for standbys stops before the follower has nothing to read, and starts once it has.
      val refollowing = if after.standbys.isEmpty then following.set(false) *> plane.follow(starts) else plane.follow(starts) *> following.set(true)
      refollowing.whenA(before.standbys.keySet != after.standbys.keySet) *> report(after)

    /** The stores the member reports the task offsets of: those standing by and those kept. */
    private def report(running: Running[F]): F[Unit] = standing.set(running.standbys ++ running.kept.view.mapValues(_._1))

    /** A task's stores, opened empty. */
    private def storesOf(id: TaskId): F[Map[String, TaskStore[F]]] =
      val read  = subtopologies(id.subtopology)
      val times = WindowStore.timesOf(List(read))
      read.kept.traverse: kept =>
        val name = kept.store.value
        // Suppression buffers stay in memory, as Java keeps them.
        state.filterNot(_ => kept.store.keptInMemory || kept.buffer)
          .fold(open(name, times.get(name), StoreContents.empty))(_.open(ownerOf(id), name, times.get(name))).map(name -> _)
      .map(_.toMap).flatTap(stores => holding.update(_.updated(id, stores)))

    /** The changelog partitions of a task's stores, with the store each backs. */
    private def changelogsOf(id: TaskId): F[Map[TopicPartition, String]] =
      subtopologies(id.subtopology).stores
        .traverse(store => partitionOf(s"$application-${store.value}-changelog", id.partition).map(_ -> store.value)).map(_.toMap)

    /** Reads each partition its task holds from where that task is. */
    private def reassign(running: Running[F]): F[Unit] =
      plane.read(running.tasks.values.toVector.flatMap(_.inputs.toVector.map((partition, input) => StartAt(partition, input.position, -1))))

    /** Starts tasks where the group last committed them, or at the start of their partitions, with their stores refilled from their changelogs. */
    private def start(tasks: List[TaskId], standbys: Map[TaskId, Standby[F]]): F[Map[TaskId, Live[F]]] =
      tasks.traverse(id => sourcesOf(id).map(id -> _)).flatMap: inputs =>
        val asked = inputs.flatMap(_._2).groupMap(_.topic.value)(_.partition.value).toList.map(OffsetFetchRequest.Topic(_, _))
        if inputs.isEmpty then F.pure(Map.empty)
        else
          (stored(asked), restore(tasks, standbys), F.realTime).tupled.flatMap: (offsets, stores, clock) =>
            inputs.traverse: (id, partitions) =>
              val found   = partitions.map(partition => partition -> offsets.get((partition.topic.value, partition.partition.value)))
              val times   = found.flatMap((partition, at) => at.flatMap(_.metadata).flatMap(PartitionTime.decode).map(partition.topic.value -> _))
              val emitted = PartitionTime.merged(found.flatMap(_._2.flatMap(_.metadata)))
              val read    = subtopologies(id.subtopology)
              val state   = TaskState(Map.empty, times.toMap, lastEmitted = emitted)
              val reads   = found.map((partition, at) => partition -> Input(at.filter(_.offset >= 0).fold(KafkaDriver.BeginningOffset)(_.offset)))
              val task    =
                new Task[F](application, read, id.partition, stores.getOrElse(id, Map.empty), id.subtopology.toIntOption.getOrElse(0), globals)
              (task.opened(state, clock.toMillis) <* opened.update(_.updated(id, task)))
                .map(ready => id -> Live(task, ready, reads.toMap, stores.getOrElse(id, Map.empty)))
            .map(_.toMap)

    /** The offsets the group stored, asking again while the coordinator or a transaction is not ready to answer. */
    private def stored(asked: List[OffsetFetchRequest.Topic]): F[Map[(String, Int), OffsetFetchRequest.Stored]] =
      untilAnswered(member.committed(asked)): answer =>
        val codes = answer.errorCode :: answer.partitions.map(_.errorCode)
        codes.find(code => code != 0 && !Again(code)) match
          case Some(code) => F.raiseError(failure("reading the group's offsets", code))
          case None       => F.pure(Option.when(codes.forall(_ == 0))(answer.partitions.map(p => (p.topic, p.partition) -> p).toMap))

    /** Each task's stores, read back from the task's partition of their changelogs, one task after another: a task that stood by from where it read
      * to, into the stores it holds, and any other from the start into stores opened empty, each without what has expired once it is read.
      */
    private def restore(tasks: List[TaskId], standbys: Map[TaskId, Standby[F]]): F[Map[TaskId, Map[String, TaskStore[F]]]] =
      tasks.filter(id => subtopologies(id.subtopology).stores.nonEmpty).traverse: id =>
        val held = standbys.get(id).filter(_.stores.nonEmpty)
        (changelogsOf(id), held.fold(storesOf(id))(standby => F.pure(standby.stores))).tupled.flatMap: (changelogs, stores) =>
          val from =
            held.fold(changelogs.toList.traverse((partition, store) => stores(store).position.map(_.map(partition -> _))).map(_.flatten.toMap))(
              standby => F.pure(standby.positions)
            )
          from.flatMap(plane.restore(changelogs, _, stores)) *> WindowStore.swept(stores, subtopologies(id.subtopology).retentions).as(id -> stores)
      .map(_.toMap)

    /** Commits where each of these tasks has read to, with its partition time, asking again while the coordinator is not ready. A member the group no
      * longer counts commits nothing: its tasks go to others, which read again from the group's last commit.
      */
    private def commit(running: Running[F], tasks: Set[TaskId]): F[Running[F]] = flushed(running, tasks, plane.write).flatMap(committed(_, tasks))

    /** Writes what these tasks' suppression buffers log at a commit, before the commit, as Java's buffers log before the producer flushes. */
    private def flushed(running: Running[F], tasks: Set[TaskId], write: Writer[F]): F[Running[F]] =
      running.tasks.view.filterKeys(tasks).toList.filter(_._2.state.dirty.values.exists(_.keys.nonEmpty))
        .traverse((id, live) => live.task.flush(live.state).map(id -> (live, _))).flatMap: done =>
          done.flatMap(_._2._2._2).traverse(recordOf).flatMap(records => NonEmptyList.fromList(records).traverse_(write))
            .as(running.copy(tasks = running.tasks ++ done.map((id, flushed) => id -> flushed._1.copy(state = flushed._2._1))))

    private def flushed(running: Running[F], write: Writer[F]): F[Running[F]] = flushed(running, running.tasks.keySet, write)

    private def committed(running: Running[F], tasks: Set[TaskId]): F[Running[F]] =
      val named = running.tasks.view.filterKeys(tasks).toList
      val due   = named.filter(_._2.read)
      // A task that only punctuated has no offsets to commit, but what it wrote has gone to its changelogs.
      val wrote  = named.filter((_, live) => live.read || live.state.pending.nonEmpty)
      val topics =
        due.flatMap((_, live) => live.inputs.toList.flatMap((partition, input) => input.read.map((live, partition, _)))).groupMap(_._2.topic.value):
          (live, partition, read) =>
            val (next, leaderEpoch) = read
            val time                = PartitionTime.encode(live.state.partitionTime(partition.topic.value), live.state.lastEmitted)
            OffsetCommitRequest.Partition(partition.partition.value, next, leaderEpoch, Some(time))
        .toList.map(OffsetCommitRequest.Topic(_, _))
      val sent =
        untilAnswered(member.commit(topics)): answers =>
          answers.map(_.errorCode).find(code => code != 0 && !Again(code)) match
            case Some(code) if Gone(code) => F.pure(Some(()))
            case Some(code)               => F.raiseError(failure("committing", code))
            case None                     => F.pure(Option.when(answers.forall(_.errorCode == 0))(()))
      sent.whenA(topics.nonEmpty) *> wrote.traverse((id, live) => live.stored.map(id -> _.settled))
        .map(done => running.copy(tasks = running.tasks ++ done))

    /** The next records, or none after a wait while no task runs, since the Java consumer refuses to poll with nothing assigned. */
    private def poll(running: Running[F]): F[List[PolledRecord]] = if running.tasks.isEmpty then F.sleep(PollMillis.millis).as(Nil) else plane.poll

    /** Runs each task's records through it in order, and its punctuators after each record and whether or not it has any, up to `concurrency` tasks
      * at once, and writes what the tasks wrote, waiting until every record is delivered.
      */
    private def process(running: Running[F], polled: List[PolledRecord], write: Writer[F]): F[Running[F]] =
      val routes =
        running.tasks.toList
          .flatMap((id, live) => live.inputs.keys.map(partition => (partition.topic.value, partition.partition.value) -> (id, partition))).toMap
      val byTask =
        polled.flatMap(record => routes.get((record.topic, record.partition)).map(_ -> record)).groupMap(_._1._1)(found => (found._1._2, found._2))
      F.realTime.flatMap: clock =>
        def punctuated(live: Live[F]): F[(Live[F], Vector[TaskOutput])] =
          if !live.task.punctuates then F.pure((live, Vector.empty))
          else live.task.punctuate(live.state, clock.toMillis).map((state, out) => (live.copy(state = state), out))
        running.tasks.toList.filter((id, live) => byTask.contains(id) || live.task.punctuates).parTraverseN(concurrency): (id, live) =>
          Engine.byTime(byTask.getOrElse(id, Nil)).foldLeftM((live, Vector.empty[TaskOutput])):
            case ((live, written), (partition, record)) => live.task.process(live.state, inputOf(record), clock.toMillis).flatMap: (state, out) =>
                val read =
                  Input(
                    record.offset + 1,
                    Some((record.offset + 1, record.leaderEpoch)),
                    live.inputs(partition).committed,
                    live.inputs(partition).purged
                  )
                punctuated(live.copy(state = state, inputs = live.inputs.updated(partition, read)))
                  .map((after, more) => (after, written ++ out ++ more))
          .flatMap((after, written) => punctuated(after).map((done, more) => id -> (done, written ++ more)))
      .flatMap: done =>
        val written = done.flatMap(_._2._2)
        written.traverse(recordOf).flatMap(records => NonEmptyList.fromList(records).traverse_(write))
          .as(running.copy(tasks = running.tasks ++ done.map((id, ran) => id -> ran._1)))

    private def inputOf(record: PolledRecord): TaskInput =
      TaskInput(record.key, record.value, record.timestamp.getOrElse(-1L), record.headers, record.topic, record.partition, record.offset)

    private def recordOf(output: TaskOutput): F[ProducerRecord[Option[Chunk[Byte]], Option[Chunk[Byte]]]] =
      def outside(chosen: Int, count: Int) =
        new IllegalArgumentException(s"a partitioner chose partition $chosen of the topic '${output.topic.value}', which has $count partitions")
      // A partitioner that gives no partition leaves the record to Kafka's default, as Java's does.
      def default = output.partitionBy.fold(F.pure(output.partition))(by => partitions(output.topic).map(count => Some(Murmur2.partition(by, count))))
      val chosen  =
        output.choose.fold(default): choose =>
          partitions(output.topic).flatMap(count => F.catchNonFatal(choose(count)).map(count -> _)).flatMap: (count, answer) =>
            answer match
              case None                                          => default
              case Some(picked) if picked >= 0 && picked < count => F.pure(Some(picked))
              case Some(picked)                                  => F.raiseError(outside(picked, count))
      chosen.flatMap(_.traverse(n => Partition.from(n).leftMap(new KafkaException.InvalidValue(_)).liftTo[F])).map: partition =>
        ProducerRecord(
          output.topic,
          output.key,
          output.value,
          partition,
          Option.when(output.timestamp != Task.NoTimestamp)(Timestamp.fromEpochMillis(output.timestamp)),
          output.headers
        )

    /** The partitions a task reads: those of its sub-topology's topics, input or repartition topics, at the task's partition. */
    private def sourcesOf(id: TaskId): F[List[TopicPartition]] =
      subtopologies(id.subtopology).origins.traverse(origin => partitionOf(Task.topicOf(application, origin), id.partition))

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
