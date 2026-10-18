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

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{Async, Deferred, Outcome, Ref, Resource}
import cats.effect.implicits.*
import cats.effect.std.{Mutex, Queue, Random, Semaphore, Supervisor}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.{Channel, SignallingRef}
import internal.{ClientProperties, PollQueue}
import internal.driver.{Delivery, EncodedRecord, KafkaDriver, PolledRecord, Rebalance}

private def isolationLevel(value: IsolationLevel): String =
  value match
    case IsolationLevel.ReadUncommitted => "read_uncommitted"
    case IsolationLevel.ReadCommitted   => "read_committed"

/** The client every backend shares, over the driver of the platform it runs on. */
private[xkafka] final class DriverClient[F[_]](val driver: KafkaDriver[F])(using F: Async[F]) extends KafkaClient[F]:
  // Records the poll loop has read but nothing has taken yet. The loop stops polling when this fills, which is
  // also when Kafka would consider the consumer stalled.
  private val RecordQueueSize = 256

  /** Group metadata, from a consumer of any client on this platform, that a transaction records offsets against. */
  private object GroupMetadata:
    def unapply(handle: GroupHandle): Option[driver.GroupMetadata] = driver.asGroupMetadata(handle)
  // How long one poll waits for delivery reports before the batch waiting on them checks whether it is done.
  private val DeliveryPollTimeoutMillis = 100
  // Admin calls are metadata round trips against the controller, so they are bounded on their own.
  private val AdminTimeoutMillis = 30000

  // The offsets `KafkaDriver.seek` reads as the ends of a partition's log.
  private val BeginningOffset = KafkaDriver.BeginningOffset
  private val EndOffset       = KafkaDriver.EndOffset

  /** A driver's client handle, and the gate that keeps calls from reaching it once it has been destroyed.
    *
    * Destroying takes the permit, so whatever call is already running finishes first, and sets the flag, so a call that arrives afterwards is refused
    * while the handle it would have passed to the driver is already gone.
    */
  private sealed trait Gate
  private final case class Open(calls: Int)                                 extends Gate
  private final case class Draining(calls: Int, drained: Deferred[F, Unit]) extends Gate
  private case object Shut                                                  extends Gate

  /** Keeps a handle alive for as long as calls are inside it, and refuses calls once it has been destroyed.
    *
    * Every driver takes calls from any thread, so calls do not exclude one another. Only the destroy waits, which it does by refusing new calls and
    * then letting the ones already inside leave.
    */
  private final class NativeClient(val handle: driver.Client, gate: Ref[F, Gate]):
    def apply[A](operation: F[A]): F[A] =
      F.bracket(enter)(entered => if entered then operation else F.raiseError(new IllegalStateException("the client is closed")))(entered =>
        leave.whenA(entered)
      )

    /** Refuses further calls and waits for the ones already inside to finish. */
    val close: F[Unit] =
      Deferred[F, Unit].flatMap: drained =>
        gate.modify:
          case Open(0)     => (Shut, F.unit)
          case Open(calls) => (Draining(calls, drained), drained.get)
          case other       => (other, F.unit)
        .flatten

    private val enter: F[Boolean] =
      gate.modify:
        case Open(calls) => (Open(calls + 1), true)
        case other       => (other, false)

    private val leave: F[Unit] =
      gate.modify:
        case Open(calls)              => (Open(calls - 1), F.unit)
        case Draining(1, drained)     => (Shut, drained.complete(()).void)
        case Draining(calls, drained) => (Draining(calls - 1, drained), F.unit)
        case Shut                     => (Shut, F.unit)
      .flatten

  private def nativeClient(acquire: F[driver.Client], destroy: driver.Client => F[Unit]): Resource[F, NativeClient] =
    Resource.eval(Ref.of[F, Gate](Open(0))).flatMap: gate =>
      Resource.make(acquire.map(handle => new NativeClient(handle, gate)))(client => client.close >> destroy(client.handle))

  /** Owns producer batches outside their acknowledgement fibers, so resource release can cancel those fibers, flush and destroy the producer within
    * its configured bound, and only then free any batch slots the backend could still have referenced.
    */
  private def producerHandle(acquire: F[driver.Client], destroy: driver.Client => F[Unit]): Resource[F, NativeProducerHandle] =
    for
      owned  <- Resource.make(Ref.of[F, Set[driver.Batch]](Set.empty))(releaseBatches)
      client <- nativeClient(acquire, destroy)
      // Release cancels acknowledgement fibers before closing the client instead of waiting indefinitely for them.
      supervisor <- Supervisor[F](await = false)
      batches    <- Resource.eval(Semaphore[F](1))
    yield NativeProducerHandle(client, supervisor, batches, owned)

  private def releaseBatches(owned: Ref[F, Set[driver.Batch]]): F[Unit] = owned.getAndSet(Set.empty).flatMap(_.toList.traverse_(driver.freeBatch))

  private final case class NativeProducerHandle(
      client: NativeClient,
      supervisor: Supervisor[F],
      batches: Semaphore[F],
      owned: Ref[F, Set[driver.Batch]]
  )

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    producerHandle(
      createProducer(settings.client, settings.properties ++ settings.managedProperties),
      handle => driver.destroyProducer(handle, settings.closeTimeout.toMillis.toInt)
    ).map(handle => new DriverProducer(handle.client, handle.supervisor, handle.batches, handle.owned, settings))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], selection: Selection): Resource[F, KafkaConsumer[F, K, V]] =
    for
      client <- nativeClient(createConsumer(settings), driver.destroyConsumer)
      leases <- Resource.eval(AssignmentLeases[F](settings.assignmentFencing))
      _      <- Resource.eval(client(select(client.handle, selection)))
      // No rebalance ever revokes a partition named directly, so each keeps the lease it starts with.
      _ <-
        Resource.eval(
          initialAssignment(selection).toList
            .traverse_(value => leases.assign(List(LeaseKey(value.topic.value, value.partition.value)), replacing = false))
        )
      polled      <- Resource.eval(Channel.bounded[F, ReadRecord](RecordQueueSize))
      rebalances  <- Resource.eval(Queue.unbounded[F, Rebalance[driver.Event]])
      assignments <- Resource.eval(SignallingRef[F, Set[TopicPartition]](initialAssignment(selection)))
      // Set by `stopConsuming`, and read before each poll so that no records already fetched are dropped.
      stopping <- Resource.eval(Deferred[F, Unit])
      // A poll that fails takes the rebalance callback down with it, so the failure is kept and reported to whoever
      // reads the records instead of leaving a consumer that never receives anything.
      failure <- Resource.eval(Deferred[F, Throwable])
      // The commit recovery spreads its retries, so the consumer carries the randomness that does the spreading.
      random <- Resource.eval(Random.scalaUtilRandom[F])
      consumer = new DriverConsumer(client, settings, leases, polled, rebalances, assignments, stopping, random, failure)
      // Both are started after the client and cancelled before it, so neither is using the handle when it is destroyed.
      _ <- consumer.rebalancing.compile.drain.onError(failure.complete(_).void).background
      _ <- consumer.pollLoop.compile.drain.onError(failure.complete(_).void).background
    yield consumer

  private def bootstrap(client: ClientSettings): String = client.bootstrapServers.toList.mkString(",")

  private def createProducer(client: ClientSettings, properties: Map[String, String]): F[driver.Client] =
    driver.producer(
      bootstrap(client),
      client.clientId,
      client.properties ++ properties ++ ClientProperties.security(client.security) ++ ClientProperties(client)
    )

  private def createConsumer[K, V](settings: ConsumerSettings[F, K, V]): F[driver.Client] =
    driver.consumer(
      bootstrap(settings.client),
      settings.client.clientId,
      settings.groupId.value,
      ClientProperties.autoOffsetReset(settings.autoOffsetReset),
      settings.client.properties ++ settings.properties.updated("isolation.level", isolationLevel(settings.isolationLevel)) ++
        ClientProperties.security(settings.client.security) ++ ClientProperties(settings.client) ++ ClientProperties.consumer(settings)
    )

  override def transactionalProducer[K, V](settings: TransactionalProducerSettings[F, K, V]): Resource[F, KafkaTransactionalProducer[F, K, V]] =
    val timeoutMillis = settings.transactionTimeout.toMillis.toInt
    for
      handle <-
        producerHandle(
          createProducer(
            settings.producer.client,
            settings.producer.properties ++ settings.producer.managedProperties + TransactionalIdempotence ++
              Map("transactional.id" -> settings.transactionalId.value, "transaction.timeout.ms" -> timeoutMillis.toString)
          ),
          handle => driver.destroyProducer(handle, settings.producer.closeTimeout.toMillis.toInt)
        )
      _ <- Resource.eval(handle.client(driver.initTransactions(handle.client.handle, timeoutMillis)))
      // One transactional id carries one transaction at a time, so this keeps concurrent callers out of each other's.
      lock <- Resource.eval(Mutex[F])
    yield new DriverTransactionalProducer(
      handle.client,
      new DriverProducer(handle.client, handle.supervisor, handle.batches, handle.owned, settings.producer),
      lock,
      timeoutMillis
    )

  private final class DriverTransactionalProducer[K, V](client: NativeClient, records: KafkaProducer[F, K, V], lock: Mutex[F], timeoutMillis: Int)
      extends KafkaTransactionalProducer[F, K, V]:

    override def transactionally[A](use: Transaction[F, K, V] => F[A]): F[A] =
      lock.lock.surround(client(driver.beginTransaction(client.handle)) *> use(transaction).guaranteeCase:
        case Outcome.Succeeded(_) => client(driver.commitTransaction(client.handle, timeoutMillis))
        case Outcome.Canceled()   => client(driver.abortTransaction(client.handle, timeoutMillis))
        // The failure that caused the abort is the one worth reporting, so a failed abort is carried along with it.
        case Outcome.Errored(error) => client(driver.abortTransaction(client.handle, timeoutMillis))
            .handleErrorWith(failure => F.delay(error.addSuppressed(failure))))

    private val transaction: Transaction[F, K, V] =
      new Transaction[F, K, V]:
        /** A transaction cannot commit records the broker has not acknowledged, so this waits for them. */
        override def produce(values: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]] = records.produceAndAwait(values)

        override def commitOffsets(batch: CommittableOffsetBatch[F]): F[Unit] = GroupMembership.resolve(batch).use(commitMemberships)

        private def commitMemberships(memberships: List[(GroupHandle, Map[TopicPartition, Offset])]): F[Unit] =
          memberships.traverse_ : membership =>
            membership match
              case (GroupMetadata(metadata), offsets) =>
                if offsets.isEmpty then F.unit else client(driver.sendOffsets(client.handle, metadata, offsets.toVector, timeoutMillis))
              case (other, _) => F.raiseError(GroupMembership.unrecognised(other))

  override def admin(settings: ClientSettings): Resource[F, KafkaAdminClient[F]] =
    nativeClient(
      driver.admin(
        bootstrap(settings),
        settings.clientId,
        settings.properties ++ ClientProperties.security(settings.security) ++ ClientProperties(settings)
      ),
      driver.destroyAdmin
    ).map(new DriverAdminClient(_))

  private final class DriverAdminClient(client: NativeClient) extends KafkaAdminClient[F]:
    override def createTopics(topics: NonEmptySet[NewTopic]): F[Unit] =
      client(driver.createTopics(client.handle, topics.toSortedSet.toVector, requestTimeoutMillis))

    override def deleteTopics(topics: NonEmptySet[Topic]): F[Unit] =
      client(driver.deleteTopics(client.handle, topics.toSortedSet.toVector, requestTimeoutMillis))

    override def createPartitions(topic: Topic, count: Int): F[Unit] =
      F.fromEither(KafkaAdminClient.validatePartitionCount(count))
        .flatMap(valid => client(driver.createPartitions(client.handle, topic, valid, requestTimeoutMillis)))

    /** Topic metadata already reports what the cluster holds, so this asks for that rather than a second admin call. */
    override def describeTopics(topics: NonEmptySet[Topic]): F[Map[Topic, Set[Partition]]] =
      client(readTopicMetadata(client.handle, requestTimeoutMillis, None)).map(_.view.filterKeys(topics.contains).toMap).flatMap: described =>
        topics.find(topic => !described.contains(topic)) match
          case Some(missing) => F.raiseError(unknownTopic(missing))
          case None          => F.pure(described)

    override def deleteRecords(before: NonEmptyMap[TopicPartition, Offset]): F[Map[TopicPartition, Either[KafkaException, Offset]]] =
      val cuts = before.toSortedMap.toVector
      client(driver.deleteRecords(client.handle, cuts, requestTimeoutMillis)).map(answers => cuts.map(_._1).zip(answers).toMap)

    override def describeTopicConfigurations(topics: NonEmptySet[Topic]): F[Map[Topic, Either[KafkaException, TopicConfiguration]]] =
      val requested = topics.toSortedSet.toVector
      client(driver.describeTopicConfigs(client.handle, requested, requestTimeoutMillis)).map(answers => requested.zip(answers).toMap)

    private def unknownTopic(topic: Topic): KafkaException.BackendFailure =
      new KafkaException.BackendFailure(
        s"the cluster has no topic '${topic.value}'",
        Some(ErrorCode.UnknownTopicOrPartition),
        retriable = Some(false)
      )

    private val requestTimeoutMillis = AdminTimeoutMillis

  private def select(consumer: driver.Client, selection: Selection): F[Unit] =
    selection match
      // Names the partitions to read directly, so the consumer joins no group and its assignment never changes.
      case Selection.Partitions(topicPartitions) => driver.assign(consumer, topicPartitions.toSortedSet.toVector)
      case Selection.Topics(values)              => driver.subscribe(consumer, values.toNonEmptyList.map(_.value))
      case Selection.Pattern(pattern)            => driver.subscribe(consumer, NonEmptyList.one(pattern.anchored))

  private def initialAssignment(selection: Selection): Set[TopicPartition] =
    selection match
      case Selection.Partitions(topicPartitions) => topicPartitions.toSortedSet.toSet
      case _: Selection.Subscription             => Set.empty

  private def readTopicMetadata(handle: driver.Client, requestTimeoutMillis: Int, requestedTopic: Option[Topic]): F[Map[Topic, Set[Partition]]] =
    driver.metadata(handle, requestedTopic.map(_.value), requestTimeoutMillis).flatMap: topics =>
      F.delay:
        topics.map: (topicValue, partitionValues) =>
          val topic      = Topic.from(topicValue).fold(error => throw invalidBackendValue("metadata topic", topicValue, error), identity)
          val partitions =
            partitionValues.map(value => Partition.from(value).fold(error => throw invalidBackendValue("metadata partition", value, error), identity))
          topic -> partitions.toSet
        .toMap

  private def readPartitionsFor(handle: driver.Client, requestTimeoutMillis: Int, topic: Topic): F[Set[Partition]] =
    readTopicMetadata(handle, requestTimeoutMillis, Some(topic)).flatMap: topics =>
      F.fromOption(topics.get(topic), new KafkaException.InvalidBackendResponse(s"missing metadata for topic '${topic.value}'"))

  private def invalidBackendValue(field: String, value: Any, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error")

  private def offset(field: String, value: Long): Offset = Offset.from(value).fold(error => throw invalidBackendValue(field, value, error), identity)

  private final class DriverProducer[K, V](
      client: NativeClient,
      supervisor: Supervisor[F],
      batches: Semaphore[F],
      owned: Ref[F, Set[driver.Batch]],
      settings: ProducerSettings[F, K, V]
  ) extends KafkaProducer[F, K, V]:

    /** A metadata lookup is a round trip against the cluster, so it is bounded the way the administration calls are. */
    private val metadataTimeoutMillis = settings.client.metadataRefreshInterval.toMillis.toInt.min(AdminTimeoutMillis)

    override def partitionsFor(topic: Topic): F[Set[Partition]] = client(readPartitionsFor(client.handle, metadataTimeoutMillis, topic))

    /** Enqueues the whole batch in one pass and serves its delivery reports once, so a batch of any size costs a single round trip. */
    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[ProducerResult[K, V]]] =
      for
        encoded <- records.traverse(encodeRecord)
        result  <-
          F.uncancelable: _ =>
            for
              batch <- client(driver.batch(encoded.size))
              _     <- owned.update(_ + batch)
              // Delivery reports are served from whichever batch is being awaited, so the slots they write into are
              // only ever touched by one call at a time.
              enqueued <- batches.permit.use(_ => client(driver.enqueue(client.handle, batch, encoded))).attempt
              result   <-
                enqueued match
                  case Right(_) => superviseBatch(batch, awaitDelivery(batch) *> batches.permit.use(_ => client(awaitBatch(batch, records))))
                  // A prefix can already belong to the producer when a later enqueue fails. It keeps its supervised cleanup,
                  // while the caller receives the failure saying that the complete batch was not accepted.
                  case Left(failure) => superviseBatch(batch, awaitDelivery(batch)).void *> F.raiseError(failure)
            yield result
      yield result

    private def superviseBatch[A](batch: driver.Batch, result: F[A]): F[F[A]] =
      supervisor.supervise(result.guaranteeCase(_ => releaseBatchWhenComplete(batch))).map(_.joinWithNever)

    private def releaseBatchWhenComplete(batch: driver.Batch): F[Unit] =
      batches.permit.use(_ => driver.pending(batch)).flatMap: pending =>
        if pending == 0 then owned.modify(current => (current - batch, current.contains(batch))).flatMap(F.whenA(_)(driver.freeBatch(batch)))
        else F.unit

    /** Serves delivery reports until this batch has all of its own.
      *
      * Where serving reports serves every batch's, the lock is taken for one poll at a time, so the reports a poll delivers still reach their slots
      * one thread at a time, and a batch waiting here does not hold up the next enqueue.
      */
    private def awaitDelivery(batch: driver.Batch): F[Unit] =
      val serve = client(driver.serveDeliveries(client.handle, batch, DeliveryPollTimeoutMillis))
      (if driver.deliveriesShareQueue then batches.permit.use(_ => serve) else serve)
        .flatMap(pending => if pending == 0 then F.unit else awaitDelivery(batch))

    private def encodeRecord(record: ProducerRecord[K, V]): F[EncodedRecord] =
      (
        settings.keySerializer.serialize(record.topic, record.headers, record.key),
        settings.valueSerializer.serialize(record.topic, record.headers, record.value)
      ).mapN((key, value) => EncodedRecord(record.topic, record.partition, record.timestamp, record.headers, key, value))

    private def awaitBatch(batch: driver.Batch, records: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]] =
      driver.deliveries(client.handle, batch).flatMap: reported =>
        F.delay:
          if reported.size != records.size then
            throw new KafkaException.InvalidBackendResponse(s"expected ${records.size} delivery reports, got ${reported.size}")
          ProducerResult(records.zipWithIndex.map((record, index) => record -> Some(metadataOf(reported(index), record.topic))))

    private def metadataOf(delivery: Delivery, topic: Topic): RecordMetadata =
      val partition = Partition.from(delivery.partition).fold(error => throw invalidBackendValue("partition", delivery.partition, error), identity)
      RecordMetadata(
        TopicPartition(topic, partition),
        Option.when(delivery.offset >= 0L)(offset("offset", delivery.offset)),
        Option.when(delivery.timestamp >= 0L)(delivery.timestamp).map(Timestamp.fromEpochMillis)
      )

  private final case class ReadRecord(record: PolledRecord, lease: Option[Lease])

  private final class DriverConsumer[K, V](
      client: NativeClient,
      settings: ConsumerSettings[F, K, V],
      leases: AssignmentLeases[F],
      polled: Channel[F, ReadRecord],
      rebalances: Queue[F, Rebalance[driver.Event]],
      assignments: SignallingRef[F, Set[TopicPartition]],
      stopping: Deferred[F, Unit],
      random: Random[F],
      failure: Deferred[F, Throwable]
  ) extends KafkaConsumer[F, K, V]:

    private val requestTimeoutMillis = settings.requestTimeout.toMillis.toInt
    private val commitTimeoutMillis  = settings.commitTimeout.toMillis.toInt

    private val groupHandle: F[GroupHandle] = client(driver.groupMetadata(client.handle)).widen

    private val offsetCommitter: OffsetCommitter[F] =
      CommitRecovery.recovering(
        new OffsetCommitter[F]:
          /** Offsets with no lease of their own commit only for a partition this consumer holds now. */
          override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] = leases.current(offsets).flatMap(commitLeased)

          /** Each attempt holds the offsets' leases while it runs, so a revocation waits for a commit already sent. */
          override private[xkafka] def commitLeased(offsets: Map[TopicPartition, (Offset, Option[Lease])]): F[Unit] =
            leases.holding(offsets.values.map(_._2).toList)(commitOffsets(offsets.view.mapValues(_._1).toMap))

          /** A group is named by the metadata its consumer hands out, which also fences a member the group has already replaced. */
          override private[xkafka] val membership: GroupMembership[F] = GroupMembership.Backend(groupHandle, releaseGroupMetadata)
        ,
        settings.commitRecovery,
        random
      )

    private def releaseGroupMetadata(handle: GroupHandle): F[Unit] =
      handle match
        case GroupMetadata(metadata) => driver.destroyGroupMetadata(metadata)
        case _                       => F.unit

    /** The consumer's single poll, which both its records and its rebalances come from.
      *
      * Kafka clients advance group membership only from this call, so it belongs to the consumer and not to whoever happens to be reading records.
      * Each record is labelled with the lease it was read under, and each rebalance the poll raised is handed to `rebalancing`.
      */
    val pollLoop: Stream[F, Nothing] =
      // Sending outside the permit keeps a full queue from holding the handle that a close is waiting for.
      PollQueue.feed(
        Stream.repeatEval(stopping.tryGet).takeWhile(_.isEmpty)
          .evalMap(_ => leases.reading(pollOnce)(record => LeaseKey(record.topic, record.partition))).flatMap(Stream.emits)
          .map((record, lease) => ReadRecord(record, lease)),
        polled,
        failure
      )

    /** A rebalance taken from the driver has to be applied before the consumer can close, so taking one and queueing it for `rebalancing` is never
      * cut apart by a cancellation, which costs a release at most one poll.
      */
    private def pollOnce: F[List[PolledRecord]] =
      F.uncancelable(_ =>
        client(poll.product(driver.takeRebalances(client.handle))).flatMap((records, taken) => taken.traverse_(rebalances.offer).as(records))
      )

    /** Applies each rebalance in the order the poll raised it.
      *
      * A revocation ends its leases and waits for any transaction still recording an offset under one of them before it is applied, and until then
      * the group cannot hand those partitions to another member. Polling carries on meanwhile. Whatever is cancelled or left is still applied,
      * because the close cannot finish while a rebalance is waiting.
      */
    val rebalancing: Stream[F, Nothing] =
      Stream.fromQueueUnterminated(rebalances).evalMap(rebalanced).drain
        .onFinalize(rebalances.tryTakeN(None).flatMap(_.traverse_(taken => client(driver.applyRebalance(client.handle, taken.event)))))

    private def rebalanced(taken: Rebalance[driver.Event]): F[Unit] =
      val leasing =
        taken.kind match
          case 1 => client(driver.cooperative(client.handle)).flatMap(cooperative => leases.assign(taken.partitions, replacing = !cooperative))
          case 0 => leases.revoke(taken.partitions)
          // Anything else clears the assignment to resynchronise, which ends every lease with it.
          case _ => leases.assign(Nil, replacing = true)
      F.uncancelable(poll => poll(leasing).guarantee(client(driver.applyRebalance(client.handle, taken.event)))) *>
        assignment.flatMap(assignments.set)

    override def stopConsuming: F[Unit] = stopping.complete(()).attempt.void

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] = PollQueue.read(polled, failure).evalMap(decode)

    override def assignment: F[Set[TopicPartition]] =
      client(driver.assignment(client.handle)).flatMap: entries =>
        F.delay:
          entries.map: (topicValue, partitionValue) =>
            val topic     = Topic.from(topicValue).fold(error => throw invalidBackendValue("assigned topic", topicValue, error), identity)
            val partition =
              Partition.from(partitionValue).fold(error => throw invalidBackendValue("assigned partition", partitionValue, error), identity)
            TopicPartition(topic, partition)
          .toSet

    override val assignmentChanges: Stream[F, Set[TopicPartition]] =
      assignments.discrete.changes.concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        val entries = topicPartitions.toVector
        client(driver.committed(client.handle, entries, requestTimeoutMillis))
          .flatMap(values => F.delay(entries.zip(values).map((tp, value) => tp -> Option.when(value >= 0L)(offset("committed offset", value))).toMap))

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "beginning offset", (low, _) => low)

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "end offset", (_, high) => high)

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty)
      else
        val entries = timestampsToSearch.toVector
        client(driver.offsetsForTimes(client.handle, entries, requestTimeoutMillis)).flatMap: values =>
          F.delay(entries.zip(values).map { case ((tp, _), value) => tp -> Option.when(value >= 0L)(offset("timestamp offset", value)) }.toMap)

    override def partitionsFor(topic: Topic): F[Set[Partition]] = client(readPartitionsFor(client.handle, requestTimeoutMillis, topic))

    override def listTopics: F[Map[Topic, Set[Partition]]] = client(readTopicMetadata(client.handle, requestTimeoutMillis, None))

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] =
      client(driver.seek(client.handle, topicPartition, offset.value, requestTimeoutMillis))

    override def seekToBeginning(topicPartitions: Set[TopicPartition]): F[Unit] =
      client(topicPartitions.toList.traverse_(driver.seek(client.handle, _, BeginningOffset, requestTimeoutMillis)))

    override def seekToEnd(topicPartitions: Set[TopicPartition]): F[Unit] =
      client(topicPartitions.toList.traverse_(driver.seek(client.handle, _, EndOffset, requestTimeoutMillis)))

    override def position(topicPartition: TopicPartition): F[Option[Offset]] =
      client(driver.positions(client.handle, Vector(topicPartition))).flatMap: values =>
        F.delay(values.headOption.filter(_ >= 0L).map(offset("position", _)))

    private def poll: F[List[PolledRecord]] =
      driver.poll(client.handle, settings.pollTimeout.toMillis.toInt).recover:
        // librdkafka reports a subscribed topic that does not exist yet through the poll, and the topic can still be
        // created afterwards, so this keeps polling for it.
        case failure: KafkaException.BackendFailure if failure.code.contains(ErrorCode.UnknownTopicOrPartition) => Nil

    private def decode(read: ReadRecord): F[CommittableConsumerRecord[F, K, V]] =
      val source = read.record
      for
        topic     <- F.fromEither(Topic.from(source.topic).leftMap(error => invalidBackendValue("topic", source.topic, error)))
        partition <- F.fromEither(Partition.from(source.partition).leftMap(error => invalidBackendValue("partition", source.partition, error)))
        offset    <- F.fromEither(Offset.from(source.offset).leftMap(error => invalidBackendValue("offset", source.offset, error)))
        portableNextOffset <- F.fromEither(offset.next.leftMap(error => invalidBackendValue("next offset", source.offset, error)))
        key                <- settings.keyDeserializer.deserialize(topic, source.headers, source.key)
        value              <- settings.valueDeserializer.deserialize(topic, source.headers, source.value)
      yield
        val topicPartition = TopicPartition(topic, partition)
        val record         = ConsumerRecord(topicPartition, offset, source.timestamp.map(Timestamp.fromEpochMillis), key, value, source.headers)
        val committable    =
          new CommittableOffset[F]:
            override val topicPartition: TopicPartition = TopicPartition(topic, partition)

            override val nextOffset: Offset = portableNextOffset

            override val committer: OffsetCommitter[F] = offsetCommitter

            override private[xkafka] val lease: Option[Lease] = read.lease

            override private[xkafka] val membership: GroupMembership[F] =
              leases.membership(DriverConsumer.this, read.lease, groupHandle, releaseGroupMetadata)

        CommittableConsumerRecord(record, committable)

    private def commitOffsets(offsets: Map[TopicPartition, Offset]): F[Unit] =
      if offsets.isEmpty then F.unit else client(driver.commit(client.handle, offsets.toVector, commitTimeoutMillis))

    override val pausing: PartitionPausing[F] =
      new PartitionPausing.Backend[F]:
        override def pause(topicPartitions: Set[TopicPartition]): F[Unit] =
          if topicPartitions.isEmpty then F.unit else client(driver.pause(client.handle, topicPartitions.toVector))

        override def resume(topicPartitions: Set[TopicPartition]): F[Unit] =
          if topicPartitions.isEmpty then F.unit else client(driver.resume(client.handle, topicPartitions.toVector))

    private def boundaryOffsets(topicPartitions: Set[TopicPartition], field: String, select: (Long, Long) => Long): F[Map[TopicPartition, Offset]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        client(topicPartitions.toList.traverse(tp => driver.watermarks(client.handle, tp, requestTimeoutMillis).map(tp -> _))).flatMap: bounds =>
          F.delay(bounds.map { case (tp, (low, high)) => tp -> offset(field, select(low, high)) }.toMap)
