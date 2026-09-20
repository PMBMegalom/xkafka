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

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import cats.data.NonEmptyList
import cats.effect.{Async, Resource}
import cats.effect.implicits.*
import cats.effect.std.{Semaphore, Supervisor}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import internal.librdkafka.Bindings

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new LibrdkafkaClient[F]

private[xkafka] object LibrdkafkaPlatform:
  def version: String = fromCString(Bindings.xkafka_version_str())

private final class LibrdkafkaClient[F[_]](using F: Async[F]) extends KafkaClient[F]:
  private val ErrorBufferSize     = 512
  private val PollTimeoutMillis   = 100
  private val UnassignedPartition = -1

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    for
      semaphore <- Resource.eval(Semaphore[F](1))
      handle <- Resource.make(createProducer(settings))(producer => semaphore.permit.use(_ => F.blocking(Bindings.xkafka_producer_destroy(producer))))
      // Outstanding acknowledgements finish before the handle they poll is destroyed.
      supervisor <- Supervisor[F](await = true)
    yield new LibrdkafkaProducer(handle, semaphore, supervisor, settings)

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      semaphore <- Resource.eval(Semaphore[F](1))
      handle <- Resource.make(createConsumer(settings))(consumer => semaphore.permit.use(_ => F.blocking(Bindings.xkafka_consumer_destroy(consumer))))
      _      <- Resource.eval(semaphore.permit.use(_ => F.blocking(subscribe(handle, subscription))))
    yield new LibrdkafkaConsumer(handle, semaphore, settings)

  private def createProducer[K, V](settings: ProducerSettings[F, K, V]): F[CVoidPtr] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) = nativeProperties(settings.client.properties ++ settings.properties)
        val producer                      =
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
        val (names, values, propertySize) = nativeProperties(settings.client.properties ++ settings.properties)
        val consumer                      =
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

  private def cBytes(value: Option[Chunk[Byte]])(using Zone): (CVoidPtr, CSize) =
    value.fold[(CVoidPtr, CSize)]((null, 0.toUSize)): bytes =>
      val pointer = alloc[CChar](math.max(1, bytes.size))
      bytes.iterator.zipWithIndex.foreach:
        case (byte, index) => pointer(index) = byte
      (pointer, bytes.size.toUSize)

  private def chunk(pointer: CVoidPtr, size: CSize): Chunk[Byte] =
    val bytes = pointer.asInstanceOf[Ptr[Byte]]
    Chunk.array(Array.tabulate(size.toInt)(bytes(_)))

  private final class LibrdkafkaProducer[K, V](
      handle: CVoidPtr,
      semaphore: Semaphore[F],
      supervisor: Supervisor[F],
      settings: ProducerSettings[F, K, V]
  ) extends KafkaProducer[F, K, V]:

    /** Enqueues the whole batch in one pass and serves its delivery reports once, so a batch of any size costs a single round trip. */
    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[ProducerResult[K, V]]] =
      for
        encoded <- records.traverse(encodeRecord)
        batch   <- semaphore.permit.use(_ => F.blocking(enqueue(encoded)))
        // The batch is heap allocated and librdkafka writes into it after the enqueue returns, so releasing it
        // is owned by a supervised fiber. Dropping the acknowledgement therefore cannot leak it.
        awaiting <-
          supervisor.supervise(
            semaphore.permit.use(_ => F.blocking(awaitBatch(batch, records)))
              .guarantee(semaphore.permit.use(_ => F.blocking(Bindings.xkafka_batch_destroy(handle, batch))))
          )
      yield awaiting.joinWithNever

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
                handle,
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
          Bindings.xkafka_batch_destroy(handle, batch)
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
        val result             = Bindings.xkafka_batch_await(handle, batch, error, ErrorBufferSize.toUSize, errorCode)
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

  private final class LibrdkafkaConsumer[K, V](handle: CVoidPtr, semaphore: Semaphore[F], settings: ConsumerSettings[F, K, V])
      extends KafkaConsumer[F, K, V]:

    private val offsetCommitter: OffsetCommitter[F] =
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] = semaphore.permit.use(_ => F.blocking(commitOffsets(offsets)))

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      Stream.repeatEval(semaphore.permit.use(_ => F.blocking(poll()))).unNone.evalMap(decode)

    override def assignment: F[Set[TopicPartition]] = semaphore.permit.use(_ => F.blocking(readAssignment()))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty) else semaphore.permit.use(_ => F.blocking(readCommitted(topicPartitions)))

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "beginning offset", (low, _) => low)

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "end offset", (_, high) => high)

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty) else semaphore.permit.use(_ => F.blocking(readOffsetsForTimes(timestampsToSearch)))

    override def partitionsFor(topic: Topic): F[Set[Partition]] = semaphore.permit.use(_ => F.blocking(readPartitionsFor(topic)))

    override def listTopics: F[Map[Topic, Set[Partition]]] = semaphore.permit.use(_ => F.blocking(readTopicMetadata(None)))

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] = semaphore.permit.use(_ => F.blocking(seekTo(topicPartition, offset)))

    private def poll(): Option[NativeRecord] =
      Zone.acquire: zone =>
        given Zone             = zone
        val status             = stackalloc[CInt]()
        val (error, errorCode) = errorSlots
        val message            = Bindings.xkafka_consumer_poll(handle, PollTimeoutMillis, status, error, ErrorBufferSize.toUSize, errorCode)

        if !status < 0 then throw nativeError(error, errorCode)
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
            Bindings
              .xkafka_consumer_commit(handle, topics, partitions, nativeOffsets, entries.size.toUSize, error, ErrorBufferSize.toUSize, errorCode)
          if result != 0 then throw nativeError(error, errorCode)

    private def readAssignment(): Set[TopicPartition] =
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val assignment         = Bindings.xkafka_consumer_assignment(handle, error, ErrorBufferSize.toUSize, errorCode)
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
          Bindings
            .xkafka_consumer_committed(handle, topics, partitions, entries.size.toUSize, committedOffsets, error, ErrorBufferSize.toUSize, errorCode)
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
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else semaphore.permit.use(_ => F.blocking(readBoundaryOffsets(topicPartitions, field, select)))

    private def readBoundaryOffsets(topicPartitions: Set[TopicPartition], field: String, select: (Long, Long) => Long): Map[TopicPartition, Offset] =
      Zone.acquire: zone =>
        given Zone = zone
        topicPartitions.iterator.map: topicPartition =>
          val low                = stackalloc[CLongLong]()
          val high               = stackalloc[CLongLong]()
          val (error, errorCode) = errorSlots
          val result             =
            Bindings.xkafka_consumer_watermark_offsets(
              handle,
              toCString(topicPartition.topic.value),
              topicPartition.partition.value,
              low,
              high,
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
            handle,
            topics,
            partitions,
            timestamps,
            entries.size.toUSize,
            timestampOffsets,
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
          Bindings
            .xkafka_consumer_metadata(handle, requestedTopic.map(topic => toCString(topic.value)).orNull, error, ErrorBufferSize.toUSize, errorCode)
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
            handle,
            toCString(topicPartition.topic.value),
            topicPartition.partition.value,
            offset.value,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
