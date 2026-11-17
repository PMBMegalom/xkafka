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
import cats.effect.std.{Mutex, Queue, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.concurrent.SignallingRef
import driver.{KafkaDriver, PolledRecord, StartAt}
import group.{Connections, PlatformCrypto, PlatformSockets}
import protocol.{OffsetCommitRequest, OffsetFetchRequest, StreamsGroup}
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{
  Endpoint, Forward, GlobalStore, KafkaStreams, ProcessingGuarantee, Query, QueryProblem, Source, StreamsPhase, StreamsProblem, StreamsSettings,
  StreamsState, Subtopology, Topology
}

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
  val TransactionTimeout: FiniteDuration = StreamsSettings.TransactionTimeout

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

  /** What a transactional producer is refused for once its tasks have moved: fenced, its epoch, and its producer id expired (49). */
  private val Moved: Set[ErrorCode] = Set(ErrorCode.Fenced, ErrorCode.InvalidProducerEpoch, ErrorCode.Other(49))

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
      queryEndpoint: Option[Endpoint] = None,
      fetchWait: FiniteDuration = ConsumerSettings.DefaultFetchWait,
      taskTimeout: FiniteDuration = StreamsSettings.DefaultTaskTimeout,
      forward: Option[Forward[F]] = None,
      watch: Plane[F] => Plane[F] = identity[Plane[F]]
  )(using F: Async[F]): Resource[F, KafkaStreams[F]] =
    val connections = Connections(PlatformSockets[F], PlatformCrypto[F], settings, closeTimeout)
    val servers     = settings.bootstrapServers.toList.mkString(",")
    val properties  =
      settings.properties ++ ClientProperties.security(settings.security) ++ ClientProperties(settings) ++
        ClientProperties.fetchWait(fetchWait, settings.properties)
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
          queryEndpoint.map(at => StreamsGroup.Endpoint(at.host, at.port)),
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
                  .leftMap(new KafkaException.InvalidSettings(_)).liftTo[Resource[F, *]].flatMap: transactional =>
                    transactions(driver, client.transactionalProducer(transactional), group, memberId).map(Some(_))
      plane =
        new Plane[F]:
          def read(starts: Vector[StartAt]): F[Unit] = driver.assign(input, starts.map(_.partition), starts)
          def poll: F[List[PolledRecord]]            = driver.poll(input, PollMillis).map(_.records)
          def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], stores: Map[String, TaskStore[F]]): F[Unit] =
            consumer("read_committed", reportEnds = true).use(Restore(driver, _, changelogs, from, stores))
          def follow(starts: Vector[StartAt]): F[Unit]                               = driver.assign(follower, starts.map(_.partition), starts)
          def followed: F[List[PolledRecord]]                                        = driver.poll(follower, PollMillis).map(_.records)
          def ends(partitions: Vector[TopicPartition]): F[Map[TopicPartition, Long]] = bounds(partitions).map(_.view.mapValues(_._2).toMap)
          def bounds(partitions: Vector[TopicPartition]): F[Map[TopicPartition, (Long, Long)]] =
            partitions.traverse(partition => driver.watermarks(follower, partition, 30000).map(partition -> _)).map(_.toMap)
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
          state,
          queryEndpoint,
          forward,
          taskTimeout
        )
    yield running

  /** Opens the global tables' stores: on disk where `state` is given, unless a table's store is kept in memory. */
  private def globalStores[F[_]: Async](state: Option[StateDirectory[F]], tables: List[GlobalStore[F]]): TaskStore.Open[F] =
    val inMemory = tables.filter(_.store.keptInMemory).map(_.store.value).toSet
    (name, time, held) =>
      state.filterNot(_ => inMemory(name)).fold(TaskStore.memory[F].apply(name, time, held))(_.open(StateDirectory.Global, name, time))

  /** Transactions of `producer` that record offsets for `group` under the member's id and the epoch each is given, its tasks' epoch. */
  private type Producing[F[_]] = KafkaTransactionalProducer[F, Option[Chunk[Byte]], Option[Chunk[Byte]]]

  /** Transactions through the producer `made` makes, which is made anew once another producer of its transactional id has fenced it: the fenced one
    * is released first, and the new one, made with the same id, starts its transactions where the coordinator fenced the old.
    */
  private def transactions[F[_]](driver: KafkaDriver[F], made: Resource[F, Producing[F]], group: ConsumerGroup, memberId: String)(using
      F: Async[F]
  ): Resource[F, Transactions[F]] =
    Resource.make(made.allocated.flatMap(Ref.of[F, (Producing[F], F[Unit])]))(_.get.flatMap(_._2)).map: current =>
      new Transactions[F]:
        def apply[A](epoch: Int)(body: Writer[F] => F[(A, Map[TopicPartition, OffsetAndMetadata])]): F[A] =
          current.get.flatMap((producer, _) => transactional(driver, producer, group, memberId, epoch, body))

        // The release is taken as it runs, so a renewal that fails to make the new producer, and is tried again, does not release the old one twice.
        def renew: F[Unit] =
          F.uncancelable(_ =>
            current.modify((producer, release) => ((producer, F.unit), release)).flatMap(_.attempt) *> made.allocated.flatMap(current.set)
          )

  private def transactional[F[_], A](
      driver: KafkaDriver[F],
      producer: Producing[F],
      group: ConsumerGroup,
      memberId: String,
      epoch: Int,
      body: Writer[F] => F[(A, Map[TopicPartition, OffsetAndMetadata])]
  )(using F: Async[F]): F[A] =
    producer.transactionally: transaction =>
      body(records => transaction.produce(records).void).flatMap: (result, offsets) =>
        if offsets.isEmpty then F.pure(result)
        else
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
    def apply[A](epoch: Int)(body: Writer[F] => F[(A, Map[TopicPartition, OffsetAndMetadata])]): F[A]

    /** Makes the producer anew, as Java does once another producer of the same transactional id has fenced it. */
    def renew: F[Unit]

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
      state: Option[StateDirectory[F]] = None,
      own: Option[Endpoint] = None,
      forward: Option[Forward[F]] = None,
      taskTimeout: FiniteDuration = StreamsSettings.DefaultTaskTimeout
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
      read      <- Resource.eval(Queue.unbounded[F, (Int, PolledRecord)])
      following <- Resource.eval(F.ref(false))
      // Each poll is of one following, which a change of the standbys followed ends: what a poll of an earlier one read is dropped, as a standby
      // starting again from further back would take it for one of its own. A poll and a change of following do not overlap.
      generation <- Resource.eval(F.ref(0))
      polling    <- Resource.eval(Mutex[F])
      stopping   <- Resource.eval(F.ref(false))
      // Released, the fiber is asked to stop and ends after its poll, so that no poll is cut short before the follower is destroyed.
      _ <-
        def reading: F[Unit] =
          stopping.get.ifM(
            F.unit,
            following.get.ifM(
              polling.lock.surround(generation.get.flatMap(now => plane.followed.map(_.map(now -> _)))).flatMap(_.traverse_(read.offer)),
              F.sleep(PollMillis.millis)
            )
              // A poll can meet the follower just after its last standby went; the next goes on.
              .handleErrorWith(_ => F.sleep(PollMillis.millis)) *> reading
          )
        Resource.make(reading.start)(fiber => stopping.set(true) *> fiber.join.void)
      opened <- Resource.eval(F.ref(Map.empty[TaskId, Task[F]]))
      kept   <- Resource.eval(state.fold(F.pure(Map.empty[TaskId, Map[String, Long]]))(_.held.map(Engine.heldTasks)))
      stores <- Resource.eval(F.ref(Map.empty[TaskId, Map[String, TaskStore[F]]]))
      // What each active task has written since it last committed, which its stores hold only after the commit, for queries to read meanwhile.
      unstored <- Resource.eval(F.ref(Map.empty[TaskId, Map[String, StoreChanges]]))
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
          generation,
          polling,
          partitions,
          globals.fold(Map.empty[String, TaskStore[F]])(_.stores),
          open,
          opened,
          state,
          stores,
          unstored,
          taskTimeout
        )
      running <-
        // The tasks still open once the loop has stopped close with the engine, and with them their processors' instances.
        Resource.make(loop.begin(Running(Map.empty, started, started), kept).start): fiber =>
          // A loop that stopped on a failure gives back no tasks, so the member leaves without waiting for it to.
          // Every task and store is closed though one fails to close.
          F.race(member.leave, fiber.join *> member.abandon).timeout(closeTimeout).attempt *> fiber.cancel *>
            (opened.getAndSet(Map.empty), stores.getAndSet(Map.empty)).tupled.flatMap: (tasks, held) =>
              Task.closingAll(tasks.values.toList.map(_.close) ++ held.values.toList.flatMap(_.values).map(_.close))
      querying =
        Querying(
          application,
          subtopologies,
          globals.fold(Map.empty[String, TaskStore[F]])(_.stores),
          stores,
          unstored.get,
          member.reports,
          phase,
          partitions,
          own,
          forward.getOrElse(Forward.none[F]),
          topology.globals.map((_, global) => global.store.value -> global.topic.value).toMap
        )
    yield new KafkaStreams[F]:
      def run: F[Unit] = globals.fold(running.joinWithNever)(held => F.race(running.joinWithNever, held.failure).void)
      def query[A](query: Query[F, A], allowStandby: Boolean): F[Either[QueryProblem, A]] = querying.query(query, allowStandby)
      def answer(request: Chunk[Byte]): F[Chunk[Byte]]                                    = querying.answer(request)
      def state: Stream[F, StreamsState]                                                  =
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

    /** Where each of these partitions starts and ends. */
    def bounds(partitions: Vector[TopicPartition]): F[Map[TopicPartition, (Long, Long)]]

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

    /** Where its commit was dropped: nothing to commit again, and still committed where the group last took it. */
    def dropped: Input = copy(read = None)

  /** A task the engine runs: its program, what it holds, and each partition it reads, one of each topic its sub-topology reads. */
  private final case class Live[F[_]](
      task: Task[F],
      state: TaskState,
      inputs: Map[TopicPartition, Input],
      stores: Map[String, TaskStore[F]] = Map.empty[String, TaskStore[F]]
  ):
    /** Committed where it read to. */
    def settled: Live[F] = copy(inputs = inputs.view.mapValues(_.settled).toMap)

    def dropped: Live[F] = copy(inputs = inputs.view.mapValues(_.dropped).toMap)

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
      kept: Map[TaskId, (Standby[F], FiniteDuration)] = Map.empty[TaskId, (Standby[F], FiniteDuration)],
      // The member's epoch when it gave the tasks, which their commits carry.
      epoch: Int = 0,
      // Since when transactions have failed in ways Java recovers from, which the task timeout bounds.
      failing: Option[FiniteDuration] = None,
      // Since when writes at the commit have failed one after another, at least once, which records processed between them do not end.
      flushFailing: Option[FiniteDuration] = None
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
      read: Queue[F, (Int, PolledRecord)],
      following: Ref[F, Boolean],
      generation: Ref[F, Int],
      polling: Mutex[F],
      partitions: Topic => F[Int],
      globals: Map[String, TaskStore[F]],
      open: TaskStore.Open[F],
      opened: Ref[F, Map[TaskId, Task[F]]],
      state: Option[StateDirectory[F]],
      holding: Ref[F, Map[TaskId, Map[String, TaskStore[F]]]],
      unstored: Ref[F, Map[TaskId, Map[String, StoreChanges]]],
      taskTimeout: FiniteDuration
  )(using F: Async[F]):
    import MemberAction.*

    /** Closes the tasks given, which no longer run here, and with them their processors' instances, as Java closes a task it gives up, answering
      * those whose stores stay on disk, kept as given up.
      */
    private def closed(tasks: Iterable[(TaskId, Live[F])]): F[Map[TaskId, Standby[F]]] = givingUp(tasks.toList, settled = false).map(_.toMap)

    /** Closes each task given and releases its stores, though one fails to, answering those kept as given up, and then fails with the first failure,
      * the others suppressed in it.
      */
    private def givingUp(tasks: List[(TaskId, Live[F])], settled: Boolean): F[List[(TaskId, Standby[F])]] =
      tasks.traverse: (id, live) =>
        (live.task.close.attempt, released(id, live.stores, settled).attempt).tupled
      .flatMap: outcomes =>
        val failures = outcomes.flatMap((closing, releasing) => closing.left.toOption ++ releasing.left.toOption)
        opened.update(_ -- tasks.map(_._1)) *>
          (failures match
            case first :: rest => F.delay(rest.foreach(first.addSuppressed)) *> F.raiseError(first)
            case Nil           => F.pure(tasks.map(_._1).zip(outcomes).flatMap((id, outcome) => outcome._2.toOption.flatten.map(id -> _))))

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
              // Asked again while a changelog's leader moves, as the restore asks, so a broker restarting does not stop the application.
              Restore.whileMoving(plane.ends(changelogs.keys.toVector)).flatMap(_.toList.traverse_((partition, end) =>
                stores(changelogs(partition)).write(StoreContents.unchanged, Map(partition.partition.value -> end))
              ))
            else F.unit
          settling *> changelogs.toList.traverse((partition, store) => stores(store).position.map(at => (partition, store, at))).flatMap: positions =>
            // Queries stop finding the stores first, and closing them through the guards queries read them by waits for the reads under way. A
            // release that cancels the loop meanwhile waits for the stores to close, so that it does not close them again.
            F.uncancelable(_ =>
              holding.modify(held => (held - id, held.get(id))).flatMap(guarded => guarded.getOrElse(stores).values.toList.traverse_(_.close))
            ) *> {
              // Only stores kept on disk stay, so only they are recorded and reported, and a task with none of them is not kept at all.
              val onDisk = subtopologies(id.subtopology).kept.filterNot(kept => kept.store.keptInMemory || kept.buffer).map(_.store.value).toSet
              val kept   = positions.filter((_, store, _) => onDisk(store))
              state.filter(_ => kept.nonEmpty).traverse: directory =>
                directory.released(ownerOf(id), kept.flatMap((_, store, at) => at.map(store -> _)).toMap).as(Standby(
                  Map.empty,
                  changelogs.filter((_, store) => onDisk(store)),
                  kept.map((partition, _, at) => partition -> at.getOrElse(0L)).toMap
                ))
            }

    /** Carries out the changes the member told of since the last batch, reads, processes and writes until the next commit, then purges what is due.
      */
    def run(running: Running[F]): F[Unit] =
      changes.tryTakeN(None).flatMap(_.foldLeftM(running)(change)).flatMap(window).flatTap(published).flatMap(purge).flatMap(follow).flatMap(run)

    /** Publishes what each active task has written since it last committed, for queries to read over its stores. */
    private def published(running: Running[F]): F[Unit] =
      unstored.set(running.tasks.collect { case (id, live) if live.state.pending.nonEmpty => id -> live.state.pending })

    /** Applies the changelog records read since the last batch to the standby tasks' stores. */
    private def follow(running: Running[F]): F[Running[F]] =
      // With no standbys, what was read is dropped, being of a following that has ended.
      if running.standbys.isEmpty then read.tryTakeN(None).as(running)
      else
        (read.tryTakeN(None), generation.get).tupled.map((taken, now) => taken.collect { case (`now`, record) => record }).flatMap: records =>
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
            val reached =
              standby.changelogs.flatMap((partition, store) => positions(id).get(partition).map(at => store -> Map(partition.partition.value -> at)))
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
        // A write that fails in a way worth retrying starts the tasks again from what the group committed, as Java revives them, whether it was made
        // processing records or writing a buffer at the commit. Records processed end the time a failure in processing counts against, as Java
        // clears a task's timeout once it processes a record. Writes at the commit that fail one after another count from the first, until a commit
        // is made, so that a buffer that can never be written stops the application once the task timeout has passed. The offsets' commit after
        // has its own way of asking again.
        case None =>
          def recovering(from: Running[F])(attempt: F[Running[F]]): F[Running[F]] =
            attempt.recoverWith:
              case failure: KafkaException.BackendFailure if failure.retriable.contains(true) && !failure.fatal.contains(true) =>
                recovered(from, failure, F.unit)
          val processed =
            poll(running)
              .flatMap(polled => process(running, polled, plane.write).map(after => if polled.nonEmpty then after.copy(failing = None) else after))
          recovering(running)(processed).flatMap: after =>
            F.monotonic.flatMap: now =>
              if now - after.committed < commitInterval then F.pure(after)
              else
                flushed(after, after.tasks.keySet, plane.write).map(Right(_)).recoverWith {
                  case failure: KafkaException.BackendFailure if failure.retriable.contains(true) && !failure.fatal.contains(true) =>
                    recovered(after.copy(failing = after.flushFailing), failure, F.unit)
                      .map(restarted => Left(restarted.copy(flushFailing = restarted.failing)))
                }.flatMap:
                  case Right(written)  => committed(written, after.tasks.keySet).map(_._1.copy(committed = now, flushFailing = None))
                  case Left(restarted) => F.pure(restarted)
        case Some(transaction) => (poll(running), F.realTime).tupled.flatMap: (polled, clock) =>
            // A punctuator that is due runs without records, in a transaction of its own that records no offsets.
            if polled.isEmpty && !running.tasks.values.exists(live => live.task.due(live.state, clock.toMillis)) then F.pure(running)
            else
              def more(after: Running[F], write: Writer[F]): F[Running[F]] =
                (F.monotonic, changes.size).tupled.flatMap: (now, waiting) =>
                  if now - after.committed >= commitInterval || waiting > 0 then F.pure(after)
                  else poll(after).flatMap(process(after, _, write)).flatMap(more(_, write))
              transaction(running.epoch)(write =>
                process(running, polled, write).flatMap(more(_, write)).flatMap(flushed(_, write)).map(after => (after, offsetsOf(after)))
              ).flatMap(after =>
                (after.tasks.toList.traverse((id, live) => live.stored.map(id -> _.settled)), F.monotonic)
                  .mapN((tasks, now) => after.copy(tasks = tasks.toMap, committed = now, failing = None))
              ).recoverWith:
                case refused: KafkaException.BackendFailure if refused.code.exists(Refused) => restart(running)
                // Fenced by another producer of its transactional id, refused for its epoch, or its producer id expired: Java takes each as its tasks
                // having moved, and makes the producer anew, the tasks starting again from what the group committed.
                case moved: KafkaException.BackendFailure if moved.code.exists(Moved) => transaction.renew *> restart(running)
                // A failure worth retrying, as a commit that timed out or a write the broker could not take yet, Java takes as its tasks corrupted:
                // what the producer left unfinished is finished, by a transaction of nothing, and the tasks start again from what the group committed,
                // until the task timeout has passed. Any other stops the application, as Java's does.
                case failure: KafkaException.BackendFailure if failure.retriable.contains(true) && !failure.fatal.contains(true) =>
                  recovered(running, failure, transaction(running.epoch)(_ => F.pure(((), Map.empty))).attempt.void)

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

    /** The tasks started again from what the group committed, after `finish`, while the task timeout has not passed since the first of the failures
      * that came one after another, the last of which is `failure`.
      */
    private def recovered(running: Running[F], failure: Throwable, finish: F[Unit]): F[Running[F]] =
      F.monotonic.flatMap: now =>
        val since = running.failing.getOrElse(now)
        if now - since >= taskTimeout then F.raiseError(failure) else finish *> restart(running).map(_.copy(failing = Some(since)))

    /** Starts every task again from what the group committed, dropping what it held and where it read to since. While the tasks restore their stores,
      * queries answer that the group is rebalancing, as while an assignment restores them.
      */
    private def restart(running: Running[F]): F[Running[F]] =
      val restoring = running.tasks.keys.exists(id => subtopologies(id.subtopology).stores.nonEmpty)
      phase.set(StreamsPhase.Restoring).whenA(restoring) *> (closed(running.tasks.toList) *> start(running.tasks.keys.toList, Map.empty), F.monotonic)
        .tupled.flatMap: (started, now) =>
          val again = running.copy(tasks = started, committed = now)
          reassign(again).as(again) <* phase.set(StreamsPhase.Running).whenA(restoring)

    // Recovering from failures is bounded per set of tasks, as Java bounds it per task: tasks that change start their time afresh.
    private def change(running: Running[F], told: Either[Throwable, MemberAction]): F[Running[F]] =
      changed(running, told)
        .map(after => if after.tasks.keySet == running.tasks.keySet then after else after.copy(failing = None, flushFailing = None))

    private def changed(running: Running[F], told: Either[Throwable, MemberAction]): F[Running[F]] =
      told.liftTo[F].flatMap:
        // A task whose commit the group did not take may already be another member's, writing to its changelogs, so its stores keep their own place.
        // A write of its buffers, or a commit, that fails in a way worth retrying gives it up uncommitted, as Java gives up a task it hands back whose
        // commit timed out: its next owner reads again from what the group committed.
        case Revoke(round, tasks) =>
          def worthRetrying(failure: Throwable) =
            failure match
              case failed: KafkaException.BackendFailure => failed.retriable.contains(true) && !failed.fatal.contains(true)
              case _                                     => false
          val handedBack =
            flushed(running, tasks, plane.write).attempt.flatMap:
              case Right(written) => committed(written, tasks).recover { case failure if worthRetrying(failure) => (written, false) }
              case Left(failure) if worthRetrying(failure) => F.pure((running, false))
              case Left(failure)                           => F.raiseError(failure)
          handedBack.flatMap((after, taken) => gaveUp(after, tasks, settled = taken))
            .flatTap(after => reassign(after) *> report(after) *> member.revoked(round))
        case Assign(round, assignment, epoch) =>
          val kept  = running.tasks.view.filterKeys(assignment.active).toMap
          val added = (assignment.active -- kept.keySet).toList
          closed(running.tasks.view.filterKeys(id => !assignment.active(id)).toList).product(F.monotonic).flatMap: (closedHere, now) =>
            val stillKept = running.kept.filter((_, held) => now - held._2 < StateCleanupDelay)
            val available = running.standbys ++ stillKept.view.mapValues(_._1)
            // A task kept past the delay is forgotten, and where its stores are in memory they are closed and no longer held for queries.
            val expired = (running.kept -- stillKept.keySet).toList.map((id, held) => id -> held._1.stores)
            expired.traverse_((id, stores) => released(id, stores, settled = false)) *> phase.set(StreamsPhase.Restoring)
              .whenA(added.exists(id => subtopologies(id.subtopology).stores.nonEmpty)) *> start(added, available).flatMap: started =>
              standby((assignment.standby ++ assignment.warmup) -- assignment.active, available).flatMap: standbys =>
                val gone = running.standbys.view.filterKeys(id => !standbys.contains(id) && !assignment.active(id)).toList
                dropped(gone).flatMap: left =>
                  val keeping = (stillKept ++ (closedHere ++ left).view.mapValues(_ -> now)).view
                    .filterKeys(id => !standbys.contains(id) && !assignment.active(id)).toMap
                  val after = running.copy(tasks = kept ++ started, standbys = standbys, kept = keeping, epoch = epoch)
                  val owned = (after.tasks.keySet ++ after.standbys.keySet).map(ownerOf)
                  reassign(after) *> refollow(running, after) *> member.assigned(round) *> phase.set(StreamsPhase.Running) *>
                    state.traverse_(_.cleaned(owned, StateCleanupDelay)).as(after)
        // Standby tasks are kept as those no longer wanted are, their stores on disk closed, so that a later open of them finds them free.
        // Active tasks are kept too where their stores stay on disk, so that the coordinator hears of them when the member joins again.
        case LoseAll => (closed(running.tasks.toList), dropped(running.standbys.toList), F.monotonic).tupled.flatMap: (gone, left, now) =>
            val lost =
              running.copy(tasks = Map.empty[TaskId, Live[F]], standbys = Map.empty, kept = running.kept ++ (gone ++ left).view.mapValues(_ -> now))
            phase.set(StreamsPhase.Lost) *> reassign(lost) *> refollow(running, lost).as(lost)
        case _ => F.pure(running)

    /** `running` without `tasks`, which have just committed and are given up: closed, and where their stores are on disk, kept as they are there. */
    private def gaveUp(running: Running[F], tasks: Set[TaskId], settled: Boolean): F[Running[F]] =
      val giving = running.tasks.view.filterKeys(tasks).toList
      (givingUp(giving, settled), F.monotonic).tupled.map: (held, now) =>
        running.copy(tasks = running.tasks -- tasks, kept = running.kept ++ held.map((id, kept) => id -> (kept, now)))

    /** The standby tasks no longer wanted, as they are kept: without a state directory, with their stores in memory as they are, and with one, those
      * with stores on disk, closed and recorded as given up. One whose stores are all in memory then keeps nothing, its stores closed.
      */
    private def dropped(gone: List[(TaskId, Standby[F])]): F[Map[TaskId, Standby[F]]] =
      gone.traverse: (id, standby) =>
        if state.isEmpty then F.pure(Some(id -> standby)) else released(id, standby.stores, settled = false).map(_.map(id -> _))
      .map(_.flatten.toMap)

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

    /** The standby tasks of `wanted` that keep stores, those already standing by as they are and the others from where their stores hold their
      * changelogs to, or their start. A store whose position is outside its changelog, as one past its end after the topic was made anew, holds what
      * the changelog no longer does: it is emptied and follows from the start, as the restore of an active task does.
      */
    private def standby(wanted: Set[TaskId], standing: Map[TaskId, Standby[F]]): F[Map[TaskId, Standby[F]]] =
      wanted.toList.filter(id => subtopologies(id.subtopology).stores.nonEmpty).traverse: id =>
        standing.get(id).filter(_.stores.nonEmpty).fold(
          (changelogsOf(id), storesOf(id)).tupled.flatMap: (changelogs, stores) =>
            changelogs.toList.traverse((partition, store) => stores(store).position.map(at => (partition, store, at))).flatMap: held =>
              Restore.whileMoving(plane.bounds(held.collect { case (partition, _, Some(_)) => partition }.toVector)).flatMap: bounds =>
                val outside =
                  held.collect:
                    case (partition, store, Some(at)) if bounds.get(partition).exists((low, high) => at < low || at > high) => store
                outside.distinct.traverse_(stores(_).clear).as:
                  val positions = held.map((partition, store, at) => partition -> at.filterNot(_ => outside.contains(store)).getOrElse(0L))
                  Standby(stores, changelogs, positions.toMap)
        )(F.pure).map(id -> _)
      .map(_.toMap)

    /** Follows the standby tasks' changelogs, where the tasks standing by changed. */
    private def refollow(before: Running[F], after: Running[F]): F[Unit] =
      val starts = after.standbys.values.toVector.flatMap(_.positions.map((partition, offset) => StartAt(partition, offset, -1)))
      // The fiber reading for standbys stops before the follower has nothing to read, and starts once it has.
      val refollowing = if after.standbys.isEmpty then following.set(false) *> plane.follow(starts) else plane.follow(starts) *> following.set(true)
      polling.lock.surround(generation.update(_ + 1) *> refollowing).whenA(before.standbys.keySet != after.standbys.keySet) *> report(after)

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
      .map(_.toMap).flatTap: stores =>
        // Queries read the stores through guards, which their close waits for.
        stores.toList.traverse((name, store) => TaskStore.guarded(store).map(name -> _))
          .flatMap(guarded => holding.update(_.updated(id, guarded.toMap)))

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

    /** The offsets the group stored, asking again while the coordinator or a transaction is not ready to answer, or while the group does not know the
      * member, which joins again on its own after a refused transaction.
      */
    private def stored(asked: List[OffsetFetchRequest.Topic]): F[Map[(String, Int), OffsetFetchRequest.Stored]] =
      untilAnswered("reading the group's offsets", member.committed(asked)): answer =>
        val codes = answer.errorCode :: answer.partitions.map(_.errorCode)
        codes.find(code => code != 0 && !Again(code) && !Gone(code)) match
          case Some(code) => F.raiseError(failure("reading the group's offsets", code))
          case None       => F.pure(codes.find(_ != 0).toLeft(answer.partitions.map(p => (p.topic, p.partition) -> p).toMap))

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

    /** Writes what these tasks' suppression buffers log at a commit, before the commit, as Java's buffers log before the producer flushes. */
    private def flushed(running: Running[F], tasks: Set[TaskId], write: Writer[F]): F[Running[F]] =
      running.tasks.view.filterKeys(tasks).toList.filter(_._2.state.dirty.values.exists(_.keys.nonEmpty))
        .traverse((id, live) => live.task.flush(live.state).map(id -> (live, _))).flatMap: done =>
          done.flatMap(_._2._2._2).traverse(recordOf).flatMap(records => NonEmptyList.fromList(records).traverse_(write))
            .as(running.copy(tasks = running.tasks ++ done.map((id, flushed) => id -> flushed._1.copy(state = flushed._2._1))))

    private def flushed(running: Running[F], write: Writer[F]): F[Running[F]] = flushed(running, running.tasks.keySet, write)

    /** Commits where each of these tasks has read to, with its partition time, asking again while the coordinator is not ready, and answers whether
      * the group took the commit. A member the group no longer counts commits nothing: its tasks go to others, which read again from the group's last
      * commit.
      */
    private def committed(running: Running[F], tasks: Set[TaskId]): F[(Running[F], Boolean)] =
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
        // STALE_MEMBER_EPOCH answers a commit at an older epoch for a task the member no longer has, which goes to another as Gone ones do.
        untilAnswered("committing", member.commit(topics, running.epoch)): answers =>
          answers.map(_.errorCode).find(code => code != 0 && (!Again(code) || code == StaleEpoch)) match
            case Some(code) if Gone(code) || code == StaleEpoch => F.pure(Right(false))
            case Some(code)                                     => F.raiseError(failure("committing", code))
            case None                                           => F.pure(answers.map(_.errorCode).find(_ != 0).toLeft(true))
      // A commit the group did not take leaves the inputs committed where it last took one, so that no purge passes what the task's next owner reads.
      (if topics.nonEmpty then sent else F.pure(true)).flatMap: taken =>
        wrote.traverse((id, live) => live.stored.map(stored => id -> (if taken then stored.settled else stored.dropped)))
          .map(done => (running.copy(tasks = running.tasks ++ done), taken))

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

    /** Sends `request` until `answered` gives a result, not the code of a refusal to ask again after, waiting from 100 ms, doubling up to a second,
      * between tries, and also after a retriable failure of the request itself. Once `taskTimeout` has passed since the first try, it fails with the
      * last refusal.
      */
    private def untilAnswered[A, B](what: String, request: F[A])(answered: A => F[Either[Short, B]]): F[B] =
      F.monotonic.flatMap: start =>
        def attempt(wait: FiniteDuration): F[B] =
          request.attempt.flatMap:
            case Right(answer) => answered(answer).flatMap:
                case Right(result) => F.pure(result)
                case Left(code)    => again(wait, refused(what, code))
            case Left(error: KafkaException.BackendFailure) if error.retriable.contains(true) => again(wait, error)
            case Left(error)                                                                  => F.raiseError(error)
        def again(wait: FiniteDuration, last: KafkaException): F[B] =
          F.monotonic.flatMap: now =>
            val left = taskTimeout - (now - start)
            if left <= Duration.Zero then F.raiseError(last) else F.sleep(wait.min(left)) *> attempt((wait * 2).min(MaxBackoff))
        attempt(Backoff)

    private def refused(what: String, code: Short): KafkaException =
      coded(s"$what was refused with error $code for longer than the task timeout of $taskTimeout", code)

    private def failure(what: String, code: Short): KafkaException = coded(s"$what failed with error $code", code)

    /** A failure with `code`, worth retrying exactly when the code is, as every client's failures are. */
    private def coded(detail: String, code: Short): KafkaException =
      val known = ErrorCode.fromProtocol(code.toInt)
      new KafkaException.BackendFailure(detail, Some(known), Some(known.retriable))
