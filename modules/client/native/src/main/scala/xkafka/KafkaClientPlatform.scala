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

import scala.scalanative.libc.string.memcpy
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Deferred, Outcome, Ref, Resource}
import cats.effect.implicits.*
import cats.effect.std.{Mutex, Queue, Random, Semaphore, Supervisor}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.concurrent.{Channel, SignallingRef}
import internal.ClientProperties
import internal.librdkafka.Bindings
import internal.security.SecurityProperties

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new LibrdkafkaClient[F]

/** What a transaction on this backend needs to record a consumer's offsets.
  *
  * librdkafka hands out group metadata that outlives the consumer it came from, so a transaction records offsets against the group without holding
  * that consumer open, and a pointer is not something a pattern can test for on its own, so the handle wraps it.
  */
private final case class LibrdkafkaGroupHandle(metadata: CVoidPtr) extends GroupHandle

private def isolationLevel(value: IsolationLevel): String =
  value match
    case IsolationLevel.ReadUncommitted => "read_uncommitted"
    case IsolationLevel.ReadCommitted   => "read_committed"

private[xkafka] object LibrdkafkaPlatform:
  def version: String = fromCString(Bindings.xkafka_version_str())

private final class LibrdkafkaClient[F[_]](using F: Async[F]) extends KafkaClient[F]:
  // Records the poll loop has read but nothing has taken yet. The loop stops polling when this fills, which is
  // also when Kafka would consider the consumer stalled.
  private val RecordQueueSize = 256
  // How long one poll waits for delivery reports before the batch waiting on them checks whether it is done.
  private val DeliveryPollTimeoutMillis = 100
  private val ErrorBufferSize           = 512
  private val UnassignedPartition       = -1
  // Admin calls are metadata round trips against the controller, so they are bounded on their own.
  private val AdminTimeoutMillis = 30000
  // librdkafka's sentinels for the ends of a partition's log.
  private val BeginningOffset = -2L
  private val EndOffset       = -1L

  /** A librdkafka client.handle, and the gate that keeps calls from reaching it once it has been destroyed.
    *
    * Destroying takes the permit, so whatever call is already running finishes first, and sets the flag, so a call that arrives afterwards is refused
    * while the pointer it would have passed to librdkafka is already freed.
    */
  private sealed trait Gate
  private final case class Open(calls: Int)                                 extends Gate
  private final case class Draining(calls: Int, drained: Deferred[F, Unit]) extends Gate
  private case object Shut                                                  extends Gate

  /** Keeps a handle alive for as long as calls are inside it, and refuses calls once it has been destroyed.
    *
    * librdkafka is thread-safe, so calls do not exclude one another. Only the destroy waits, which it does by refusing new calls and then letting the
    * ones already inside leave.
    */
  private final class NativeClient(val handle: CVoidPtr, gate: Ref[F, Gate]):
    def apply[A](operation: => A): F[A] =
      F.bracket(enter)(entered => if entered then F.blocking(operation) else F.raiseError(new IllegalStateException("the client is closed")))(
        entered => leave.whenA(entered)
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

  private def nativeClient(acquire: F[CVoidPtr], destroy: CVoidPtr => Unit): Resource[F, NativeClient] =
    Resource.eval(Ref.of[F, Gate](Open(0))).flatMap: gate =>
      Resource.make(acquire.map(handle => new NativeClient(handle, gate)))(client => client.close >> F.blocking(destroy(client.handle)))

  /** Owns producer batches outside their acknowledgement fibers, so resource release can cancel those fibers, flush and destroy the producer within
    * its configured bound, and only then free any batch slots the backend could still have referenced.
    */
  private def producerHandle(acquire: F[CVoidPtr], destroy: CVoidPtr => Unit): Resource[F, NativeProducerHandle] =
    for
      owned  <- Resource.make(Ref.of[F, Set[CVoidPtr]](Set.empty))(releaseBatches)
      client <- nativeClient(acquire, destroy)
      // Release cancels acknowledgement fibers before closing the client instead of waiting indefinitely for them.
      supervisor <- Supervisor[F](await = false)
      batches    <- Resource.eval(Semaphore[F](1))
    yield NativeProducerHandle(client, supervisor, batches, owned)

  private def releaseBatches(owned: Ref[F, Set[CVoidPtr]]): F[Unit] =
    owned.getAndSet(Set.empty).flatMap(values => F.blocking(values.foreach(Bindings.xkafka_batch_free)))

  private final case class NativeProducerHandle(client: NativeClient, supervisor: Supervisor[F], batches: Semaphore[F], owned: Ref[F, Set[CVoidPtr]])

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    producerHandle(
      createProducer(settings.client, settings.properties ++ settings.managedProperties),
      handle => Bindings.xkafka_producer_destroy(handle, settings.closeTimeout.toMillis.toInt)
    ).map(handle => new LibrdkafkaProducer(handle.client, handle.supervisor, handle.batches, handle.owned, settings))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], selection: Selection): Resource[F, KafkaConsumer[F, K, V]] =
    for
      client <- nativeClient(createConsumer(settings), Bindings.xkafka_consumer_destroy)
      leases <- Resource.eval(AssignmentLeases[F](settings.assignmentFencing))
      _      <- Resource.eval(client(select(client.handle, selection)))
      // No rebalance ever revokes a partition named directly, so each keeps the lease it starts with.
      _ <-
        Resource.eval(
          initialAssignment(selection).toList
            .traverse_(value => leases.assign(List(LeaseKey(value.topic.value, value.partition.value)), replacing = false))
        )
      polled      <- Resource.eval(Channel.bounded[F, ReadRecord](RecordQueueSize))
      rebalances  <- Resource.eval(Queue.unbounded[F, NativeRebalance])
      assignments <- Resource.eval(SignallingRef[F, Set[TopicPartition]](initialAssignment(selection)))
      // Set by `stopConsuming`, and read before each poll so that no records already fetched are dropped.
      stopping <- Resource.eval(Deferred[F, Unit])
      // A poll that fails takes the rebalance callback down with it, so the failure is kept and reported to whoever
      // reads the records instead of leaving a consumer that never receives anything.
      failure <- Resource.eval(Deferred[F, Throwable])
      // The commit recovery spreads its retries, so the consumer carries the randomness that does the spreading.
      random <- Resource.eval(Random.scalaUtilRandom[F])
      consumer = new LibrdkafkaConsumer(client, settings, leases, polled, rebalances, assignments, stopping, random, failure)
      // Both are started after the client and cancelled before it, so neither is using the handle when it is destroyed.
      _ <- consumer.rebalancing.compile.drain.onError(failure.complete(_).void).background
      _ <- consumer.pollLoop.compile.drain.onError(failure.complete(_).void).background
    yield consumer

  private def createProducer(client: ClientSettings, properties: Map[String, String]): F[CVoidPtr] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) =
          nativeProperties(client.properties ++ properties ++ SecurityProperties.librdkafka(client.security) ++ ClientProperties(client))
        val producer =
          Bindings.xkafka_producer_new(
            toCString(client.bootstrapServers.toList.mkString(",")),
            client.clientId.map(toCString).orNull,
            names,
            values,
            propertySize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if producer == null then throw nativeError(error, errorCode)
        producer

  private def createConsumer[K, V](settings: ConsumerSettings[F, K, V]): F[CVoidPtr] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) =
          nativeProperties(
            settings.client.properties ++ settings.properties.updated("isolation.level", isolationLevel(settings.isolationLevel)) ++
              SecurityProperties.librdkafka(settings.client.security) ++ ClientProperties(settings.client)
          )
        val consumer =
          Bindings.xkafka_consumer_new(
            toCString(settings.client.bootstrapServers.toList.mkString(",")),
            settings.client.clientId.map(toCString).orNull,
            toCString(settings.groupId.value),
            toCString(
              settings.autoOffsetReset match
                case AutoOffsetReset.Earliest => "earliest"
                case AutoOffsetReset.Latest   => "latest"
            ),
            names,
            values,
            propertySize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if consumer == null then throw nativeError(error, errorCode)
        consumer

  private def nativeProperties(properties: Map[String, String])(using Zone): (Ptr[CString], Ptr[CString], CSize) =
    val entries = properties.toVector
    if entries.isEmpty then (null, null, 0.toUSize)
    else
      val names  = alloc[CString](entries.size)
      val values = alloc[CString](entries.size)
      entries.iterator.zipWithIndex.foreach:
        case ((name, value), index) =>
          names(index) = toCString(name)
          values(index) = toCString(value)
      (names, values, entries.size.toUSize)

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
          handle => Bindings.xkafka_producer_destroy(handle, settings.producer.closeTimeout.toMillis.toInt)
        )
      _ <- Resource.eval(handle.client(initTransactions(handle.client.handle, timeoutMillis)))
      // One transactional id carries one transaction at a time, so this keeps concurrent callers out of each other's.
      lock <- Resource.eval(Mutex[F])
    yield new LibrdkafkaTransactionalProducer(
      handle.client,
      new LibrdkafkaProducer(handle.client, handle.supervisor, handle.batches, handle.owned, settings.producer),
      lock,
      timeoutMillis
    )

  private def initTransactions(producer: CVoidPtr, timeoutMillis: Int): Unit =
    Zone.acquire: zone =>
      given Zone                                                         = zone
      val (error, errorCode, fatal, retriable, transactionAbortRequired) = classifiedErrorSlots
      val result                                                         =
        Bindings.xkafka_producer_init_transactions(
          producer,
          timeoutMillis,
          error,
          ErrorBufferSize.toUSize,
          errorCode,
          fatal,
          retriable,
          transactionAbortRequired
        )
      if result != 0 then throw classifiedNativeError(error, errorCode, fatal, retriable, transactionAbortRequired)

  private final class LibrdkafkaTransactionalProducer[K, V](client: NativeClient, records: KafkaProducer[F, K, V], lock: Mutex[F], timeoutMillis: Int)
      extends KafkaTransactionalProducer[F, K, V]:

    override def transactionally[A](use: Transaction[F, K, V] => F[A]): F[A] =
      lock.lock.surround(client(begin()) *> use(transaction).guaranteeCase:
        case Outcome.Succeeded(_) => client(commit())
        case Outcome.Canceled()   => client(abort())
        // The failure that caused the abort is the one worth reporting, so a failed abort is carried along with it.
        case Outcome.Errored(error) => client(abort()).handleErrorWith(failure => F.delay(error.addSuppressed(failure))))

    private val transaction: Transaction[F, K, V] =
      new Transaction[F, K, V]:
        /** A transaction cannot commit records the broker has not acknowledged, so this waits for them. */
        override def produce(values: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]] = records.produceAndAwait(values)

        override def commitOffsets(batch: CommittableOffsetBatch[F]): F[Unit] = GroupMembership.resolve(batch).use(commitMemberships)

        private def commitMemberships(memberships: List[(GroupHandle, Map[TopicPartition, Offset])]): F[Unit] =
          memberships.traverse_ : membership =>
            membership match
              case (LibrdkafkaGroupHandle(metadata), offsets) => client(sendOffsets(metadata, offsets))
              case (other, _)                                 => F.raiseError(GroupMembership.unrecognised(other))

    private def begin(): Unit =
      Zone.acquire: zone =>
        given Zone                                                         = zone
        val (error, errorCode, fatal, retriable, transactionAbortRequired) = classifiedErrorSlots
        val result                                                         =
          Bindings
            .xkafka_producer_begin_transaction(client.handle, error, ErrorBufferSize.toUSize, errorCode, fatal, retriable, transactionAbortRequired)
        if result != 0 then throw classifiedNativeError(error, errorCode, fatal, retriable, transactionAbortRequired)

    private def commit(): Unit =
      Zone.acquire: zone =>
        given Zone                                                         = zone
        val (error, errorCode, fatal, retriable, transactionAbortRequired) = classifiedErrorSlots
        val result                                                         =
          Bindings.xkafka_producer_commit_transaction(
            client.handle,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode,
            fatal,
            retriable,
            transactionAbortRequired
          )
        if result != 0 then throw classifiedNativeError(error, errorCode, fatal, retriable, transactionAbortRequired)

    private def abort(): Unit =
      Zone.acquire: zone =>
        given Zone                                                         = zone
        val (error, errorCode, fatal, retriable, transactionAbortRequired) = classifiedErrorSlots
        val result                                                         =
          Bindings.xkafka_producer_abort_transaction(
            client.handle,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode,
            fatal,
            retriable,
            transactionAbortRequired
          )
        if result != 0 then throw classifiedNativeError(error, errorCode, fatal, retriable, transactionAbortRequired)

    private def sendOffsets(metadata: CVoidPtr, offsets: Map[TopicPartition, Offset]): Unit =
      if offsets.nonEmpty then
        Zone.acquire: zone =>
          given Zone        = zone
          val entries       = offsets.toVector
          val topics        = alloc[CString](entries.size)
          val partitions    = alloc[CInt](entries.size)
          val nativeOffsets = alloc[CLongLong](entries.size)
          entries.iterator.zipWithIndex.foreach:
            case ((topicPartition, offset), index) =>
              topics(index) = toCString(topicPartition.topic.value)
              partitions(index) = topicPartition.partition.value
              nativeOffsets(index) = offset.value
          val (error, errorCode, fatal, retriable, transactionAbortRequired) = classifiedErrorSlots
          val result                                                         =
            Bindings.xkafka_producer_send_offsets(
              client.handle,
              metadata,
              topics,
              partitions,
              nativeOffsets,
              entries.size.toUSize,
              timeoutMillis,
              error,
              ErrorBufferSize.toUSize,
              errorCode,
              fatal,
              retriable,
              transactionAbortRequired
            )
          if result != 0 then throw classifiedNativeError(error, errorCode, fatal, retriable, transactionAbortRequired)

  override def admin(settings: ClientSettings): Resource[F, KafkaAdminClient[F]] =
    nativeClient(createAdmin(settings), Bindings.xkafka_admin_destroy).map(new LibrdkafkaAdminClient(_))

  private def createAdmin(settings: ClientSettings): F[CVoidPtr] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) =
          nativeProperties(settings.properties ++ SecurityProperties.librdkafka(settings.security) ++ ClientProperties(settings))
        val client =
          Bindings.xkafka_admin_new(
            toCString(settings.bootstrapServers.toList.mkString(",")),
            settings.clientId.map(toCString).orNull,
            names,
            values,
            propertySize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if client == null then throw nativeError(error, errorCode)
        client

  private final class LibrdkafkaAdminClient(client: NativeClient) extends KafkaAdminClient[F]:
    override def createTopics(topics: NonEmptySet[NewTopic]): F[Unit] = client(create(topics.toSortedSet.toVector))

    override def deleteTopics(topics: NonEmptySet[Topic]): F[Unit] = client(delete(topics.toSortedSet.toVector))

    override def createPartitions(topic: Topic, count: Int): F[Unit] =
      F.fromEither(KafkaAdminClient.validatePartitionCount(count)).flatMap(valid => client(addPartitions(topic, valid)))

    /** Topic metadata already reports what the cluster holds, so this asks for that rather than a second admin call. */
    override def describeTopics(topics: NonEmptySet[Topic]): F[Map[Topic, Set[Partition]]] =
      client(readMetadata).map(_.view.filterKeys(topics.contains).toMap).flatMap: described =>
        topics.find(topic => !described.contains(topic)) match
          case Some(missing) => F.raiseError(unknownTopic(missing))
          case None          => F.pure(described)

    private def unknownTopic(topic: Topic): KafkaException.BackendFailure =
      new KafkaException.BackendFailure(
        s"the cluster has no topic '${topic.value}'",
        Some(ErrorCode.UnknownTopicOrPartition),
        retriable = Some(false)
      )

    private def create(topics: Vector[NewTopic]): Unit =
      Zone.acquire: zone =>
        given Zone       = zone
        val names        = alloc[CString](topics.size)
        val partitions   = alloc[CInt](topics.size)
        val replication  = alloc[CInt](topics.size)
        val configCounts = alloc[CSize](topics.size)
        val configured   = topics.flatMap(_.configuration.toVector)
        val configNames  = if configured.isEmpty then null else alloc[CString](configured.size)
        val configValues = if configured.isEmpty then null else alloc[CString](configured.size)
        topics.iterator.zipWithIndex.foreach:
          case (value, index) =>
            names(index) = toCString(value.topic.value)
            partitions(index) = value.partitions
            replication(index) = value.replicationFactor.toInt
            configCounts(index) = value.configuration.size.toUSize
        configured.iterator.zipWithIndex.foreach:
          case ((name, value), index) =>
            configNames(index) = toCString(name)
            configValues(index) = toCString(value)
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_admin_create_topics(
            client.handle,
            names,
            partitions,
            replication,
            configNames,
            configValues,
            configCounts,
            topics.size.toUSize,
            requestTimeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)

    private def delete(topics: Vector[Topic]): Unit =
      Zone.acquire: zone =>
        given Zone = zone
        val names  = alloc[CString](topics.size)
        topics.iterator.zipWithIndex.foreach((value, index) => names(index) = toCString(value.value))
        val (error, errorCode) = errorSlots
        val result             =
          Bindings
            .xkafka_admin_delete_topics(client.handle, names, topics.size.toUSize, requestTimeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)

    private def addPartitions(topic: Topic, count: Int): Unit =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_admin_create_partitions(
            client.handle,
            toCString(topic.value),
            count,
            requestTimeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)

    private def readMetadata: Map[Topic, Set[Partition]] =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val metadata = Bindings.xkafka_consumer_metadata(client.handle, null, requestTimeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if metadata == null then throw nativeError(error, errorCode)

        try Vector.tabulate(Bindings.xkafka_metadata_topic_count(metadata).toInt): topicIndex =>
            val topicValue = fromCString(Bindings.xkafka_metadata_topic_at(metadata, topicIndex.toUSize))
            val topic      = Topic.from(topicValue).fold(error => throw invalidBackendValue("metadata topic", topicValue, error), identity)
            val partitions =
              Vector.tabulate(Bindings.xkafka_metadata_partition_count_at(metadata, topicIndex.toUSize).toInt): partitionIndex =>
                val partitionValue = Bindings.xkafka_metadata_partition_at(metadata, topicIndex.toUSize, partitionIndex.toUSize)
                Partition.from(partitionValue).fold(error => throw invalidBackendValue("metadata partition", partitionValue, error), identity)
              .toSet
            topic -> partitions
          .toMap
        finally Bindings.xkafka_metadata_destroy(metadata)

    private val requestTimeoutMillis = AdminTimeoutMillis
  private def select(consumer: CVoidPtr, selection: Selection): Unit =
    selection match
      case Selection.Partitions(topicPartitions) => assignPartitions(consumer, topicPartitions)
      case subscription: Selection.Subscription  => subscribeTopics(consumer, subscription)

  private def initialAssignment(selection: Selection): Set[TopicPartition] =
    selection match
      case Selection.Partitions(topicPartitions) => topicPartitions.toSortedSet.toSet
      case _: Selection.Subscription             => Set.empty

  /** Names the partitions to read directly, so the consumer joins no group and its assignment never changes. */
  private def assignPartitions(consumer: CVoidPtr, topicPartitions: NonEmptySet[TopicPartition]): Unit =
    Zone.acquire: zone =>
      given Zone     = zone
      val entries    = topicPartitions.toSortedSet.toVector
      val topics     = alloc[CString](entries.size)
      val partitions = alloc[CInt](entries.size)
      entries.iterator.zipWithIndex.foreach:
        case (topicPartition, index) =>
          topics(index) = toCString(topicPartition.topic.value)
          partitions(index) = topicPartition.partition.value
      val (error, errorCode) = errorSlots
      val result = Bindings.xkafka_consumer_assign(consumer, topics, partitions, entries.size.toUSize, error, ErrorBufferSize.toUSize, errorCode)
      if result != 0 then throw nativeError(error, errorCode)

  private def subscribeTopics(consumer: CVoidPtr, subscription: Selection.Subscription): Unit =
    Zone.acquire: zone =>
      given Zone = zone
      val topics =
        subscription match
          case Selection.Topics(values)   => values.toNonEmptyList.map(_.value)
          case Selection.Pattern(pattern) => NonEmptyList.one(pattern.anchored)
      val nativeSubscription = Bindings.xkafka_subscription_new(topics.length.toUSize)
      if nativeSubscription == null then throw backendFailure("could not allocate a subscription")

      try
        topics.toList.foreach(topic => Bindings.xkafka_subscription_add(nativeSubscription, toCString(topic)))
        val (error, errorCode) = errorSlots
        val result             = Bindings.xkafka_consumer_subscribe(consumer, nativeSubscription, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)
      finally Bindings.xkafka_subscription_destroy(nativeSubscription)

  /** Builds a failure from the message and code the call wrote into its own out-parameters. */
  private def readTopicMetadata(handle: CVoidPtr, requestTimeoutMillis: Int, requestedTopic: Option[Topic]): Map[Topic, Set[Partition]] =
    Zone.acquire: zone =>
      given Zone             = zone
      val (error, errorCode) = errorSlots
      val metadata           =
        Bindings.xkafka_consumer_metadata(
          handle,
          requestedTopic.map(topic => toCString(topic.value)).orNull,
          requestTimeoutMillis,
          error,
          ErrorBufferSize.toUSize,
          errorCode
        )
      if metadata == null then throw nativeError(error, errorCode)

      try Vector.tabulate(Bindings.xkafka_metadata_topic_count(metadata).toInt): topicIndex =>
          val topicValue = fromCString(Bindings.xkafka_metadata_topic_at(metadata, topicIndex.toUSize))
          val topic      = Topic.from(topicValue).fold(error => throw invalidBackendValue("metadata topic", topicValue, error), identity)
          val partitions =
            Vector.tabulate(Bindings.xkafka_metadata_partition_count_at(metadata, topicIndex.toUSize).toInt): partitionIndex =>
              val partitionValue = Bindings.xkafka_metadata_partition_at(metadata, topicIndex.toUSize, partitionIndex.toUSize)
              Partition.from(partitionValue).fold(error => throw invalidBackendValue("metadata partition", partitionValue, error), identity)
            .toSet
          topic -> partitions
        .toMap
      finally Bindings.xkafka_metadata_destroy(metadata)

  private def readPartitionsFor(handle: CVoidPtr, requestTimeoutMillis: Int, topic: Topic): Set[Partition] =
    readTopicMetadata(handle, requestTimeoutMillis, Some(topic))
      .getOrElse(topic, throw new KafkaException.InvalidBackendResponse(s"missing metadata for topic '${topic.value}'"))

  private def nativeError(error: CString, code: Ptr[CInt]): KafkaException.BackendFailure =
    val classified = ErrorCode.fromLibrdkafka(!code)
    new KafkaException.BackendFailure(fromCString(error), Some(classified), retriable = Some(classified.retriable))

  private def classifiedNativeError(
      error: CString,
      code: Ptr[CInt],
      fatal: Ptr[CInt],
      retriable: Ptr[CInt],
      transactionAbortRequired: Ptr[CInt]
  ): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(
      fromCString(error),
      Some(ErrorCode.fromLibrdkafka(!code)),
      nativeFlag(retriable),
      nativeFlag(fatal),
      nativeFlag(transactionAbortRequired)
    )

  private def nativeFlag(value: Ptr[CInt]): Option[Boolean] = Option.when(!value >= 0)(!value != 0)

  /** Allocates the out-parameters a shim call reports a failure through. They live in this frame, so concurrent calls cannot share them. */
  private def errorSlots(using Zone): (CString, Ptr[CInt]) =
    val error = alloc[CChar](ErrorBufferSize)
    val code  = alloc[CInt](1)
    !code = 0
    (error, code)

  private def classifiedErrorSlots(using Zone): (CString, Ptr[CInt], Ptr[CInt], Ptr[CInt], Ptr[CInt]) =
    val (error, code)            = errorSlots
    val fatal                    = alloc[CInt](1)
    val retriable                = alloc[CInt](1)
    val transactionAbortRequired = alloc[CInt](1)
    !fatal = -1
    !retriable = -1
    !transactionAbortRequired = -1
    (error, code, fatal, retriable, transactionAbortRequired)

  private def backendFailure(detail: String): KafkaException.BackendFailure = new KafkaException.BackendFailure(detail)

  private def invalidBackendValue(field: String, value: Any, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error")

  /** Records carry their payload through here, so both directions move the bytes in one block. */
  private def cBytes(value: Option[Chunk[Byte]])(using Zone): (CVoidPtr, CSize) =
    value.fold[(CVoidPtr, CSize)]((null, 0.toUSize)): bytes =>
      val pointer = alloc[CChar](math.max(1, bytes.size))
      if bytes.nonEmpty then
        val slice = bytes.toArraySlice
        memcpy(pointer, slice.values.at(slice.offset), bytes.size.toUSize): Unit
      (pointer, bytes.size.toUSize)

  private def chunk(pointer: CVoidPtr, size: CSize): Chunk[Byte] =
    val count = size.toInt
    val bytes = new Array[Byte](count)
    if count > 0 then memcpy(bytes.at(0), pointer, size): Unit
    Chunk.array(bytes)

  private final class LibrdkafkaProducer[K, V](
      client: NativeClient,
      supervisor: Supervisor[F],
      batches: Semaphore[F],
      owned: Ref[F, Set[CVoidPtr]],
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
              batch <- client(allocateBatch(encoded.size))
              _     <- owned.update(_ + batch)
              // Delivery reports are served from whichever batch is being awaited, so the slots they write into are
              // only ever touched by one call at a time.
              enqueued <- batches.permit.use(_ => client(enqueue(batch, encoded))).attempt
              result   <-
                enqueued match
                  case Right(_) => superviseBatch(batch, awaitDelivery(batch) *> batches.permit.use(_ => client(awaitBatch(batch, records))))
                  // A prefix can already belong to librdkafka when a later enqueue fails. It keeps its supervised cleanup,
                  // while the caller receives the failure saying that the complete batch was not accepted.
                  case Left(failure) => superviseBatch(batch, awaitDelivery(batch)).void *> F.raiseError(failure)
            yield result
      yield result

    private def superviseBatch[A](batch: CVoidPtr, result: F[A]): F[F[A]] =
      supervisor.supervise(result.guaranteeCase(_ => releaseBatchWhenComplete(batch))).map(_.joinWithNever)

    private def releaseBatchWhenComplete(batch: CVoidPtr): F[Unit] =
      batches.permit.use(_ => F.blocking(Bindings.xkafka_batch_pending(batch).toInt)).flatMap: pending =>
        if pending == 0 then
          owned.modify(current => (current - batch, current.contains(batch))).flatMap(F.whenA(_)(F.blocking(Bindings.xkafka_batch_free(batch))))
        else F.unit

    /** Serves delivery reports until this batch has all of its own.
      *
      * The lock is taken for one poll at a time, so the reports a poll delivers still reach their slots one thread at a time, and a batch waiting
      * here does not hold up the next enqueue.
      */
    private def awaitDelivery(batch: CVoidPtr): F[Unit] =
      batches.permit.use { _ =>
        client:
          Bindings.xkafka_producer_poll(client.handle, DeliveryPollTimeoutMillis)
          Bindings.xkafka_batch_pending(batch).toInt
      }.flatMap(pending => if pending == 0 then F.unit else awaitDelivery(batch))

    private def encodeRecord(record: ProducerRecord[K, V]): F[EncodedRecord] =
      (
        settings.keySerializer.serialize(record.topic, record.headers, record.key),
        settings.valueSerializer.serialize(record.topic, record.headers, record.value)
      ).mapN((key, value) => EncodedRecord(record.topic, record.partition, record.timestamp, record.headers, key, value))

    private def allocateBatch(size: Int): CVoidPtr =
      val batch = Bindings.xkafka_batch_new(size.toUSize)
      if batch == null then throw backendFailure("could not allocate a producer batch") else batch

    private def enqueue(batch: CVoidPtr, records: NonEmptyList[EncodedRecord]): Unit =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        records.toList.foreach: record =>
          val (keyPointer, keySize)     = cBytes(record.key)
          val (valuePointer, valueSize) = cBytes(record.value)
          val headers                   = nativeHeaders(record.headers, error, errorCode)
          val result                    =
            Bindings.xkafka_batch_add(
              client.handle,
              batch,
              toCString(record.topic.value),
              record.partition.fold(UnassignedPartition)(_.value),
              record.timestamp.fold(-1L)(_.epochMillis),
              keyPointer,
              keySize,
              valuePointer,
              valueSize,
              headers,
              error,
              ErrorBufferSize.toUSize,
              errorCode
            )
          if result != 0 then throw nativeError(error, errorCode)

    private def nativeHeaders(headers: Headers, error: CString, errorCode: Ptr[CInt])(using Zone): CVoidPtr =
      val native = Bindings.xkafka_headers_new(headers.values.size.toUSize)
      if native == null then throw backendFailure("could not allocate message headers")

      try
        headers.values.foreach: header =>
          val (value, size) = cBytes(header.value)
          val result        = Bindings.xkafka_headers_add(native, toCString(header.key), value, size, error, ErrorBufferSize.toUSize, errorCode)
          if result != 0 then throw nativeError(error, errorCode)
        native
      catch
        case failure: Throwable =>
          Bindings.xkafka_headers_destroy(native)
          throw failure

    private def awaitBatch(batch: CVoidPtr, records: NonEmptyList[ProducerRecord[K, V]]): ProducerResult[K, V] =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val result             = Bindings.xkafka_batch_await(client.handle, batch, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)

        val reported = Bindings.xkafka_batch_count(batch).toInt
        if reported != records.size then throw new KafkaException.InvalidBackendResponse(s"expected ${records.size} delivery reports, got $reported")

        ProducerResult(records.zipWithIndex.map((record, index) => record -> Some(metadataAt(batch, index, record.topic))))

    private def metadataAt(batch: CVoidPtr, index: Int, topic: Topic): RecordMetadata =
      val partitionValue = Bindings.xkafka_batch_partition_at(batch, index.toUSize)
      val offsetValue    = Bindings.xkafka_batch_offset_at(batch, index.toUSize)
      val timestampValue = Bindings.xkafka_batch_timestamp_at(batch, index.toUSize)
      val partition      = Partition.from(partitionValue).fold(error => throw invalidBackendValue("partition", partitionValue, error), identity)
      val offset         =
        Option.when(offsetValue >= 0L)(Offset.from(offsetValue).fold(error => throw invalidBackendValue("offset", offsetValue, error), identity))

      RecordMetadata(TopicPartition(topic, partition), offset, Option.when(timestampValue >= 0L)(timestampValue).map(Timestamp.fromEpochMillis))

  private final case class EncodedRecord(
      topic: Topic,
      partition: Option[Partition],
      timestamp: Option[Timestamp],
      headers: Headers,
      key: Option[Chunk[Byte]],
      value: Option[Chunk[Byte]]
  )

  private final case class NativeRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Option[Long],
      key: Option[Chunk[Byte]],
      value: Option[Chunk[Byte]],
      headers: Headers
  )

  private final case class ReadRecord(record: NativeRecord, lease: Option[Lease])

  /** A rebalance the callback left for the consumer to apply, which holds the group until it is. `event` is owned until it is applied. */
  private final case class NativeRebalance(event: CVoidPtr, kind: Int, partitions: List[LeaseKey])

  private final class LibrdkafkaConsumer[K, V](
      client: NativeClient,
      settings: ConsumerSettings[F, K, V],
      leases: AssignmentLeases[F],
      polled: Channel[F, ReadRecord],
      rebalances: Queue[F, NativeRebalance],
      assignments: SignallingRef[F, Set[TopicPartition]],
      stopping: Deferred[F, Unit],
      random: Random[F],
      failure: Deferred[F, Throwable]
  ) extends KafkaConsumer[F, K, V]:

    private val requestTimeoutMillis = settings.requestTimeout.toMillis.toInt
    private val commitTimeoutMillis  = settings.commitTimeout.toMillis.toInt

    private val offsetCommitter: OffsetCommitter[F] =
      CommitRecovery.recovering(
        new OffsetCommitter[F]:
          override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] = client(commitOffsets(offsets))

          /** Each attempt holds the offsets' leases while it runs, so a revocation waits for a commit already sent. */
          override private[xkafka] def commitLeased(offsets: Map[TopicPartition, (Offset, Option[Lease])]): F[Unit] =
            leases.holding(offsets.values.map(_._2).toList)(commit(offsets.view.mapValues(_._1).toMap))

          /** librdkafka names a group by freshly allocated metadata, which also fences a member the group has already replaced. */
          override private[xkafka] val membership: GroupMembership[F] =
            GroupMembership.Backend(client(LibrdkafkaGroupHandle(Bindings.xkafka_consumer_group_metadata(client.handle))), releaseGroupMetadata)
        ,
        settings.commitRecovery,
        random
      )

    private def releaseGroupMetadata(handle: GroupHandle): F[Unit] =
      handle match
        case LibrdkafkaGroupHandle(metadata) => F.blocking(Bindings.xkafka_consumer_group_metadata_destroy(metadata))
        case _                               => F.unit

    /** The consumer's single poll, which both its records and its rebalances come from.
      *
      * librdkafka advances group membership only from this call, so it belongs to the consumer and not to whoever happens to be reading records. Each
      * record is labelled with the lease it was read under, and each rebalance the poll raised is handed to `rebalancing`.
      */
    val pollLoop: Stream[F, Nothing] =
      Stream.repeatEval(stopping.tryGet).takeWhile(_.isEmpty)
        .evalMap(_ => leases.reading(pollOnce)(record => LeaseKey(record.topic, record.partition)))
        // Sending outside the permit keeps a full queue from holding the handle that a close is waiting for.
        .evalMap(_.traverse_((record, lease) => polled.send(ReadRecord(record, lease)).void)).drain.onFinalize(polled.close.void)

    private def pollOnce: F[List[NativeRecord]] =
      client((poll(), takeRebalances())).flatMap((record, taken) => taken.traverse_(rebalances.offer).as(record.toList))

    /** Applies each rebalance in the order the poll raised it.
      *
      * A revocation ends its leases and waits for any transaction still recording an offset under one of them before it is applied, and until then
      * the group cannot hand those partitions to another member. Polling carries on meanwhile. Whatever is cancelled or left is still applied,
      * because the close cannot finish while a rebalance is waiting.
      */
    val rebalancing: Stream[F, Nothing] =
      Stream.fromQueueUnterminated(rebalances).evalMap(rebalanced).drain.onFinalize(rebalances.tryTakeN(None).flatMap(_.traverse_(taken =>
        client(Bindings.xkafka_consumer_apply_rebalance(client.handle, taken.event))
      )))

    private def rebalanced(taken: NativeRebalance): F[Unit] =
      val leasing =
        taken.kind match
          case 1 => client(Bindings.xkafka_consumer_cooperative(client.handle) != 0)
              .flatMap(cooperative => leases.assign(taken.partitions, replacing = !cooperative))
          case 0 => leases.revoke(taken.partitions)
          // Anything else clears the assignment to resynchronise, which ends every lease with it.
          case _ => leases.assign(Nil, replacing = true)
      F.uncancelable(poll => poll(leasing).guarantee(client(Bindings.xkafka_consumer_apply_rebalance(client.handle, taken.event)))) *>
        client(readAssignment()).flatMap(assignments.set)

    private def takeRebalances(): List[NativeRebalance] =
      Iterator.continually(Bindings.xkafka_consumer_take_rebalance(client.handle)).takeWhile(_ != null).map: event =>
        val partitions = Bindings.xkafka_rebalance_partitions(event)
        NativeRebalance(
          event,
          Bindings.xkafka_rebalance_kind(event),
          List.tabulate(Bindings.xkafka_assignment_count(partitions).toInt): index =>
            LeaseKey(
              fromCString(Bindings.xkafka_assignment_topic_at(partitions, index.toUSize)),
              Bindings.xkafka_assignment_partition_at(partitions, index.toUSize)
            )
        )
      .toList

    override def stopConsuming: F[Unit] = stopping.complete(()).attempt.void

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      polled.stream.evalMap(decode).concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    override def assignment: F[Set[TopicPartition]] = client(readAssignment())

    override val assignmentChanges: Stream[F, Set[TopicPartition]] =
      assignments.discrete.changes.concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty) else client(readCommitted(topicPartitions))

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "beginning offset", (low, _) => low)

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "end offset", (_, high) => high)

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty) else client(readOffsetsForTimes(timestampsToSearch))

    override def partitionsFor(topic: Topic): F[Set[Partition]] = client(readPartitionsFor(client.handle, requestTimeoutMillis, topic))

    override def listTopics: F[Map[Topic, Set[Partition]]] = client(readTopicMetadata(client.handle, requestTimeoutMillis, None))

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] = client(seekTo(topicPartition, offset.value))

    override def seekToBeginning(topicPartitions: Set[TopicPartition]): F[Unit] = client(topicPartitions.foreach(seekTo(_, BeginningOffset)))

    override def seekToEnd(topicPartitions: Set[TopicPartition]): F[Unit] = client(topicPartitions.foreach(seekTo(_, EndOffset)))

    override def position(topicPartition: TopicPartition): F[Option[Offset]] = client(readPosition(topicPartition))

    private def poll(): Option[NativeRecord] =
      Zone.acquire: zone =>
        given Zone             = zone
        val status             = stackalloc[CInt]()
        val (error, errorCode) = errorSlots
        val message            =
          Bindings.xkafka_consumer_poll(client.handle, settings.pollTimeout.toMillis.toInt, status, error, ErrorBufferSize.toUSize, errorCode)

        if !status < 0 then
          val failure = nativeError(error, errorCode)
          // librdkafka reports a subscribed topic that does not exist yet through the poll, and the topic can still be
          // created afterwards, so this keeps polling for it.
          if failure.code.contains(ErrorCode.UnknownTopicOrPartition) then None else throw failure
        else if !status == 0 then None
        else
          try Some(copyMessage(message))
          finally Bindings.xkafka_message_destroy(message)

    private def copyMessage(message: CVoidPtr): NativeRecord =
      val keyPointer   = Bindings.xkafka_message_key(message)
      val valuePointer = Bindings.xkafka_message_value(message)
      val timestamp    = Bindings.xkafka_message_timestamp(message)
      NativeRecord(
        topic = fromCString(Bindings.xkafka_message_topic(message)),
        partition = Bindings.xkafka_message_partition(message),
        offset = Bindings.xkafka_message_offset(message),
        timestamp = Option.when(timestamp >= 0L)(timestamp),
        key = Option(keyPointer).map(pointer => chunk(pointer, Bindings.xkafka_message_key_size(message))),
        value = Option(valuePointer).map(pointer => chunk(pointer, Bindings.xkafka_message_value_size(message))),
        headers = copyHeaders(message)
      )

    private def copyHeaders(message: CVoidPtr): Headers =
      val count  = Bindings.xkafka_message_header_count(message)
      val values =
        Vector.tabulate(count.toInt): index =>
          val name      = stackalloc[CString]()
          val value     = stackalloc[Ptr[Byte]]()
          val valueSize = stackalloc[CSize]()
          val hasValue  = stackalloc[CInt]()
          val result    = Bindings.xkafka_message_header_at(message, index.toUSize, name, value, valueSize, hasValue)
          if result != 0 then throw backendFailure(s"could not read message header at index $index")
          Header(fromCString(!name), Option.when(!hasValue != 0)(chunk(!value, !valueSize)))
      Headers.fromVector(values)

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
              leases.membership(
                LibrdkafkaConsumer.this,
                read.lease,
                client(LibrdkafkaGroupHandle(Bindings.xkafka_consumer_group_metadata(client.handle))),
                releaseGroupMetadata
              )

        CommittableConsumerRecord(record, committable)

    private def commitOffsets(offsets: Map[TopicPartition, Offset]): Unit =
      if offsets.nonEmpty then
        Zone.acquire: zone =>
          given Zone        = zone
          val entries       = offsets.toVector
          val topics        = alloc[CString](entries.size)
          val partitions    = alloc[CInt](entries.size)
          val nativeOffsets = alloc[CLongLong](entries.size)
          entries.iterator.zipWithIndex.foreach:
            case ((topicPartition, offset), index) =>
              topics(index) = toCString(topicPartition.topic.value)
              partitions(index) = topicPartition.partition.value
              nativeOffsets(index) = offset.value
          val (error, errorCode) = errorSlots
          val result             =
            Bindings.xkafka_consumer_commit(
              client.handle,
              topics,
              partitions,
              nativeOffsets,
              entries.size.toUSize,
              commitTimeoutMillis,
              error,
              ErrorBufferSize.toUSize,
              errorCode
            )
          if result != 0 then throw nativeError(error, errorCode)

    private def readAssignment(): Set[TopicPartition] =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val assignment         = Bindings.xkafka_consumer_assignment(client.handle, error, ErrorBufferSize.toUSize, errorCode)
        if assignment == null then throw nativeError(error, errorCode)

        try Vector.tabulate(Bindings.xkafka_assignment_count(assignment).toInt): index =>
            val topicValue     = fromCString(Bindings.xkafka_assignment_topic_at(assignment, index.toUSize))
            val partitionValue = Bindings.xkafka_assignment_partition_at(assignment, index.toUSize)
            val topic          = Topic.from(topicValue).fold(error => throw invalidBackendValue("assigned topic", topicValue, error), identity)
            val partition      =
              Partition.from(partitionValue).fold(error => throw invalidBackendValue("assigned partition", partitionValue, error), identity)
            TopicPartition(topic, partition)
          .toSet
        finally Bindings.xkafka_assignment_destroy(assignment)

    override val pausing: PartitionPausing[F] =
      new PartitionPausing.Backend[F]:
        override def pause(topicPartitions: Set[TopicPartition]): F[Unit] =
          if topicPartitions.isEmpty then F.unit else client(setPaused(topicPartitions, paused = true))

        override def resume(topicPartitions: Set[TopicPartition]): F[Unit] =
          if topicPartitions.isEmpty then F.unit else client(setPaused(topicPartitions, paused = false))

    private def setPaused(topicPartitions: Set[TopicPartition], paused: Boolean): Unit =
      Zone.acquire: zone =>
        given Zone     = zone
        val entries    = topicPartitions.toVector
        val topics     = alloc[CString](entries.size)
        val partitions = alloc[CInt](entries.size)
        entries.iterator.zipWithIndex.foreach:
          case (topicPartition, index) =>
            topics(index) = toCString(topicPartition.topic.value)
            partitions(index) = topicPartition.partition.value
        val (error, errorCode) = errorSlots
        val result             =
          if paused then
            Bindings.xkafka_consumer_pause(client.handle, topics, partitions, entries.size.toUSize, error, ErrorBufferSize.toUSize, errorCode)
          else Bindings.xkafka_consumer_resume(client.handle, topics, partitions, entries.size.toUSize, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)

    private def readCommitted(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[Offset]] =
      Zone.acquire: zone =>
        given Zone           = zone
        val entries          = topicPartitions.toVector
        val topics           = alloc[CString](entries.size)
        val partitions       = alloc[CInt](entries.size)
        val committedOffsets = alloc[CLongLong](entries.size)
        entries.iterator.zipWithIndex.foreach:
          case (topicPartition, index) =>
            topics(index) = toCString(topicPartition.topic.value)
            partitions(index) = topicPartition.partition.value
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_committed(
            client.handle,
            topics,
            partitions,
            entries.size.toUSize,
            committedOffsets,
            requestTimeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
        entries.iterator.zipWithIndex.map:
          case (topicPartition, index) =>
            val offsetValue = committedOffsets(index)
            val offset      =
              Option.when(offsetValue >= 0L):
                Offset.from(offsetValue).fold(error => throw invalidBackendValue("committed offset", offsetValue, error), identity)
            topicPartition -> offset
        .toMap

    private def boundaryOffsets(topicPartitions: Set[TopicPartition], field: String, select: (Long, Long) => Long): F[Map[TopicPartition, Offset]] =
      if topicPartitions.isEmpty then F.pure(Map.empty) else client(readBoundaryOffsets(topicPartitions, field, select))

    private def readBoundaryOffsets(topicPartitions: Set[TopicPartition], field: String, select: (Long, Long) => Long): Map[TopicPartition, Offset] =
      Zone.acquire: zone =>
        given Zone = zone
        topicPartitions.iterator.map: topicPartition =>
          val low                = stackalloc[CLongLong]()
          val high               = stackalloc[CLongLong]()
          val (error, errorCode) = errorSlots
          val result             =
            Bindings.xkafka_consumer_watermark_offsets(
              client.handle,
              toCString(topicPartition.topic.value),
              topicPartition.partition.value,
              low,
              high,
              requestTimeoutMillis,
              error,
              ErrorBufferSize.toUSize,
              errorCode
            )
          if result != 0 then throw nativeError(error, errorCode)
          val value  = select(!low, !high)
          val offset = Offset.from(value).fold(error => throw invalidBackendValue(field, value, error), identity)
          topicPartition -> offset
        .toMap

    private def readOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): Map[TopicPartition, Option[Offset]] =
      Zone.acquire: zone =>
        given Zone           = zone
        val entries          = timestampsToSearch.toVector
        val topics           = alloc[CString](entries.size)
        val partitions       = alloc[CInt](entries.size)
        val timestamps       = alloc[CLongLong](entries.size)
        val timestampOffsets = alloc[CLongLong](entries.size)
        entries.iterator.zipWithIndex.foreach:
          case ((topicPartition, timestamp), index) =>
            topics(index) = toCString(topicPartition.topic.value)
            partitions(index) = topicPartition.partition.value
            timestamps(index) = timestamp.epochMillis
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_offsets_for_times(
            client.handle,
            topics,
            partitions,
            timestamps,
            entries.size.toUSize,
            timestampOffsets,
            requestTimeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
        entries.iterator.zipWithIndex.map:
          case ((topicPartition, _), index) =>
            val value  = timestampOffsets(index)
            val offset =
              Option.when(value >= 0L)(Offset.from(value).fold(error => throw invalidBackendValue("timestamp offset", value, error), identity))
            topicPartition -> offset
        .toMap

    private def readPosition(topicPartition: TopicPartition): Option[Offset] =
      Zone.acquire: zone =>
        given Zone     = zone
        val topics     = alloc[CString](1)
        val partitions = alloc[CInt](1)
        val positions  = alloc[CLongLong](1)
        topics(0) = toCString(topicPartition.topic.value)
        partitions(0) = topicPartition.partition.value
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_position(client.handle, topics, partitions, 1.toUSize, positions, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)
        Option.when(positions(0) >= 0L)(Offset.from(positions(0)).fold(error => throw invalidBackendValue("position", positions(0), error), identity))

    private def seekTo(topicPartition: TopicPartition, offset: Long): Unit =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_seek(
            client.handle,
            toCString(topicPartition.topic.value),
            topicPartition.partition.value,
            offset,
            requestTimeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
