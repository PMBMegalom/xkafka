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

import cats.data.NonEmptyList
import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.implicits.*
import cats.effect.std.{Queue, Semaphore, Supervisor}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.concurrent.SignallingRef
import internal.ClientProperties
import internal.librdkafka.Bindings
import internal.security.SecurityProperties

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new LibrdkafkaClient[F]

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

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    for
      client <- nativeClient(createProducer(settings), Bindings.xkafka_producer_destroy)
      // Outstanding acknowledgements finish before the client.handle they poll is destroyed.
      supervisor <- Supervisor[F](await = true)
      batches    <- Resource.eval(Semaphore[F](1))
    yield new LibrdkafkaProducer(client, supervisor, batches, settings)

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      client      <- nativeClient(createConsumer(settings), Bindings.xkafka_consumer_destroy)
      _           <- Resource.eval(client(subscribe(client.handle, subscription)))
      polled      <- Resource.eval(Queue.bounded[F, NativeRecord](RecordQueueSize))
      assignments <- Resource.eval(SignallingRef[F, Set[TopicPartition]](Set.empty))
      // A poll that fails takes the rebalance callback down with it, so the failure is kept and reported to whoever
      // reads the records instead of leaving a consumer that never receives anything.
      failure <- Resource.eval(Deferred[F, Throwable])
      consumer = new LibrdkafkaConsumer(client, settings, polled, assignments, failure)
      // Started after the client and cancelled before it, so no poll is in flight when the handle is destroyed.
      _ <- consumer.pollLoop.compile.drain.onError(failure.complete(_).void).background
    yield consumer

  private def createProducer[K, V](settings: ProducerSettings[F, K, V]): F[CVoidPtr] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) =
          nativeProperties(
            settings.client.properties ++ settings.properties ++ SecurityProperties.librdkafka(settings.client.security) ++
              ClientProperties(settings.client)
          )
        val producer =
          Bindings.xkafka_producer_new(
            toCString(settings.client.bootstrapServers.toList.mkString(",")),
            settings.client.clientId.map(toCString).orNull,
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
            settings.client.properties ++ settings.properties ++ SecurityProperties.librdkafka(settings.client.security) ++
              ClientProperties(settings.client)
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

  private def subscribe(consumer: CVoidPtr, subscription: Subscription): Unit =
    Zone.acquire: zone =>
      given Zone = zone
      val topics =
        subscription match
          case Subscription.Topics(values)   => values.map(_.value)
          case Subscription.Pattern(pattern) => NonEmptyList.one(pattern.anchored)
      val nativeSubscription = Bindings.xkafka_subscription_new(topics.length.toUSize)
      if nativeSubscription == null then throw backendFailure("could not allocate a subscription")

      try
        topics.toList.foreach(topic => Bindings.xkafka_subscription_add(nativeSubscription, toCString(topic)))
        val (error, errorCode) = errorSlots
        val result             = Bindings.xkafka_consumer_subscribe(consumer, nativeSubscription, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)
      finally Bindings.xkafka_subscription_destroy(nativeSubscription)

  /** Builds a failure from the message and code the call wrote into its own out-parameters. */
  private def nativeError(error: CString, code: Ptr[CInt]): KafkaException.BackendFailure =
    val classified = ErrorCode.fromLibrdkafka(!code)
    new KafkaException.BackendFailure(fromCString(error), Some(classified), retriable = Some(retriable(classified)), fatal = Some(false))

  /** Allocates the out-parameters a shim call reports a failure through. They live in this frame, so concurrent calls cannot share them. */
  private def errorSlots(using Zone): (CString, Ptr[CInt]) =
    val error = alloc[CChar](ErrorBufferSize)
    val code  = alloc[CInt](1)
    !code = 0
    (error, code)

  private def retriable(code: ErrorCode): Boolean =
    code match
      case ErrorCode.NetworkException | ErrorCode.RequestTimedOut | ErrorCode.LeaderNotAvailable | ErrorCode.NotLeaderOrFollower | ErrorCode
            .BrokerNotAvailable | ErrorCode.CoordinatorNotAvailable | ErrorCode.NotCoordinator | ErrorCode.CoordinatorLoadInProgress => true
      case _ => false

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
      settings: ProducerSettings[F, K, V]
  ) extends KafkaProducer[F, K, V]:

    /** Enqueues the whole batch in one pass and serves its delivery reports once, so a batch of any size costs a single round trip. */
    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[ProducerResult[K, V]]] =
      for
        encoded <- records.traverse(encodeRecord)
        // Delivery reports are served from whichever batch is being awaited, so the slots they write into are
        // only ever touched by one call at a time.
        batch <- batches.permit.use(_ => client(enqueue(encoded)))
        // The batch is heap allocated and librdkafka writes into it after the enqueue returns, so releasing it
        // is owned by a supervised fiber. Dropping the acknowledgement therefore cannot leak it.
        awaiting <-
          supervisor.supervise(
            (awaitDelivery(batch) *> batches.permit.use(_ => client(awaitBatch(batch, records))))
              .guarantee(batches.permit.use(_ => client(Bindings.xkafka_batch_destroy(client.handle, batch))))
          )
      yield awaiting.joinWithNever

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

    private def enqueue(records: NonEmptyList[EncodedRecord]): CVoidPtr =
      val batch = Bindings.xkafka_batch_new(records.size.toUSize)
      if batch == null then throw backendFailure("could not allocate a producer batch")

      try
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
        batch
      catch
        case failure: Throwable =>
          Bindings.xkafka_batch_destroy(client.handle, batch)
          throw failure

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

  private final class LibrdkafkaConsumer[K, V](
      client: NativeClient,
      settings: ConsumerSettings[F, K, V],
      polled: Queue[F, NativeRecord],
      assignments: SignallingRef[F, Set[TopicPartition]],
      failure: Deferred[F, Throwable]
  ) extends KafkaConsumer[F, K, V]:

    private val requestTimeoutMillis = settings.requestTimeout.toMillis.toInt

    private val offsetCommitter: OffsetCommitter[F] =
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] = client(commitOffsets(offsets))

    /** The consumer's single poll, which both its records and its assignment come from.
      *
      * librdkafka advances group membership only from this call, so it belongs to the consumer and not to whoever happens to be reading records. The
      * generation counter the rebalance callback bumps says when the assignment is worth reading again.
      */
    val pollLoop: Stream[F, Nothing] =
      Stream.repeatEval(client((poll(), Bindings.xkafka_consumer_generation(client.handle))))
        // Offering outside the permit keeps a full queue from holding the handle that a close is waiting for.
        .evalMap((record, generation) => record.traverse_(polled.offer).as(generation)).changes
        .evalMap(_ => client(readAssignment()).flatMap(assignments.set)).drain

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      Stream.fromQueueUnterminated(polled).evalMap(decode).concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    override def assignment: F[Set[TopicPartition]] = client(readAssignment())

    override val assignmentChanges: Stream[F, Set[TopicPartition]] =
      assignments.discrete.concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty) else client(readCommitted(topicPartitions))

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "beginning offset", (low, _) => low)

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "end offset", (_, high) => high)

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty) else client(readOffsetsForTimes(timestampsToSearch))

    override def partitionsFor(topic: Topic): F[Set[Partition]] = client(readPartitionsFor(topic))

    override def listTopics: F[Map[Topic, Set[Partition]]] = client(readTopicMetadata(None))

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] = client(seekTo(topicPartition, offset))

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

    private def decode(source: NativeRecord): F[CommittableConsumerRecord[F, K, V]] =
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

    private def readTopicMetadata(requestedTopic: Option[Topic]): Map[Topic, Set[Partition]] =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val metadata           =
          Bindings.xkafka_consumer_metadata(
            client.handle,
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

    private def readPartitionsFor(topic: Topic): Set[Partition] =
      readTopicMetadata(Some(topic)).getOrElse(topic, throw new KafkaException.InvalidBackendResponse(s"missing metadata for topic '${topic.value}'"))

    private def seekTo(topicPartition: TopicPartition, offset: Offset): Unit =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_seek(
            client.handle,
            toCString(topicPartition.topic.value),
            topicPartition.partition.value,
            offset.value,
            requestTimeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
