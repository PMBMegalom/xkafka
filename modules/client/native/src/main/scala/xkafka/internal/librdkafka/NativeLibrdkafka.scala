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
package librdkafka

import java.nio.charset.StandardCharsets

import scala.scalanative.libc.string.memcpy
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import cats.data.NonEmptyList
import cats.effect.Sync
import fs2.Chunk
import driver.{Delivery, EncodedRecord, KafkaDriver, PolledRecord, Rebalance, StartAt, StoredOffset}

/** librdkafka's group metadata, which outlives the consumer it came from. */
private[xkafka] final case class NativeGroupMetadata(pointer: CVoidPtr) extends GroupHandle

/** The shim reached through Scala Native's C interface. Every call runs as a blocking call, since librdkafka may wait inside any of them. */
private[xkafka] final class NativeLibrdkafka[F[_]](using F: Sync[F]) extends KafkaDriver[F]:
  type Client        = CVoidPtr
  type Batch         = CVoidPtr
  type Event         = CVoidPtr
  type GroupMetadata = NativeGroupMetadata

  private val ErrorBufferSize     = 512
  private val UnassignedPartition = -1

  def version: String = fromCString(Bindings.xkafka_version_str())

  override val deliveriesShareQueue: Boolean = true

  override def producer(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) = nativeProperties(properties)
        val producer                      =
          Bindings.xkafka_producer_new(
            toCString(bootstrapServers),
            clientId.map(toCString).orNull,
            names,
            values,
            propertySize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if producer == null then throw nativeError(error, errorCode)
        producer

  override def destroyProducer(producer: Client, timeoutMillis: Int): F[Unit] = F.blocking(Bindings.xkafka_producer_destroy(producer, timeoutMillis))

  override def consumer(
      bootstrapServers: String,
      clientId: Option[String],
      groupId: String,
      autoOffsetReset: String,
      properties: Map[String, String]
  ): F[Client] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) = nativeProperties(properties)
        val consumer                      =
          Bindings.xkafka_consumer_new(
            toCString(bootstrapServers),
            clientId.map(toCString).orNull,
            toCString(groupId),
            toCString(autoOffsetReset),
            names,
            values,
            propertySize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if consumer == null then throw nativeError(error, errorCode)
        consumer

  override def destroyConsumer(consumer: Client): F[Unit] = F.blocking(Bindings.xkafka_consumer_destroy(consumer))

  override def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (error, errorCode)            = errorSlots
        val (names, values, propertySize) = nativeProperties(properties)
        val client                        =
          Bindings.xkafka_admin_new(
            toCString(bootstrapServers),
            clientId.map(toCString).orNull,
            names,
            values,
            propertySize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if client == null then throw nativeError(error, errorCode)
        client

  override def destroyAdmin(admin: Client): F[Unit] = F.blocking(Bindings.xkafka_admin_destroy(admin))

  override def batch(capacity: Int): F[Batch] =
    F.blocking:
      val batch = Bindings.xkafka_batch_new(capacity.toUSize)
      if batch == null then throw Failures.plain("could not allocate a producer batch") else batch

  override def freeBatch(batch: Batch): F[Unit] = F.blocking(Bindings.xkafka_batch_free(batch))

  override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        records.toList.foreach: record =>
          val (keyPointer, keySize)     = cBytes(record.key)
          val (valuePointer, valueSize) = cBytes(record.value)
          val headers                   = nativeHeaders(record.headers, error, errorCode)
          val result                    =
            Bindings.xkafka_batch_add(
              producer,
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
    if native == null then throw Failures.plain("could not allocate message headers")

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

  override def serveDeliveries(producer: Client, timeoutMillis: Int): F[Unit] = F.blocking(Bindings.xkafka_producer_poll(producer, timeoutMillis))

  override def flush(producer: Client, timeoutMillis: Int): F[Unit] = F.blocking(Bindings.xkafka_producer_flush(producer, timeoutMillis))

  /** Reports reach the batch through `serveDeliveries`, so this only reads how many are outstanding. */
  override def awaitDeliveries(batch: Batch, timeoutMillis: Int): F[Int] = pending(batch)

  override def pending(batch: Batch): F[Int] = F.blocking(Bindings.xkafka_batch_pending(batch).toInt)

  override def deliveries(producer: Client, batch: Batch): F[Vector[Either[KafkaException, Delivery]]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        Vector.tabulate(Bindings.xkafka_batch_count(batch).toInt): index =>
          if Bindings.xkafka_batch_error_at(producer, batch, index.toUSize, error, ErrorBufferSize.toUSize, errorCode) != 0 then
            Left(nativeError(error, errorCode))
          else
            Right(Delivery(
              Bindings.xkafka_batch_partition_at(batch, index.toUSize),
              Bindings.xkafka_batch_offset_at(batch, index.toUSize),
              Bindings.xkafka_batch_timestamp_at(batch, index.toUSize)
            ))

  override def initTransactions(producer: Client, timeoutMillis: Int): F[Unit] =
    transactional((error, code, fatal, retriable, abort) =>
      Bindings.xkafka_producer_init_transactions(producer, timeoutMillis, error, ErrorBufferSize.toUSize, code, fatal, retriable, abort)
    )

  override def beginTransaction(producer: Client): F[Unit] =
    transactional((error, code, fatal, retriable, abort) =>
      Bindings.xkafka_producer_begin_transaction(producer, error, ErrorBufferSize.toUSize, code, fatal, retriable, abort)
    )

  override def commitTransaction(producer: Client, timeoutMillis: Int): F[Unit] =
    transactional((error, code, fatal, retriable, abort) =>
      Bindings.xkafka_producer_commit_transaction(producer, timeoutMillis, error, ErrorBufferSize.toUSize, code, fatal, retriable, abort)
    )

  override def abortTransaction(producer: Client, timeoutMillis: Int): F[Unit] =
    transactional((error, code, fatal, retriable, abort) =>
      Bindings.xkafka_producer_abort_transaction(producer, timeoutMillis, error, ErrorBufferSize.toUSize, code, fatal, retriable, abort)
    )

  override def sendOffsets(
      producer: Client,
      metadata: GroupMetadata,
      offsets: Vector[(TopicPartition, OffsetAndMetadata)],
      timeoutMillis: Int
  ): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                                         = zone
        val (topics, partitions, values, epochs, comments) = commitArrays(offsets)
        failOnClassified((error, code, fatal, retriable, abort) =>
          Bindings.xkafka_producer_send_offsets(
            producer,
            metadata.pointer,
            topics,
            partitions,
            values,
            epochs,
            comments,
            offsets.size.toUSize,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            code,
            fatal,
            retriable,
            abort
          )
        )

  override def groupMetadata(consumer: Client): F[GroupMetadata] = F.blocking(NativeGroupMetadata(Bindings.xkafka_consumer_group_metadata(consumer)))

  override def destroyGroupMetadata(metadata: GroupMetadata): F[Unit] = F.blocking(Bindings.xkafka_consumer_group_metadata_destroy(metadata.pointer))

  override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
    handle match
      case metadata: NativeGroupMetadata => Some(metadata)
      case _                             => None

  override def assign(consumer: Client, partitions: Vector[TopicPartition], starts: Vector[StartAt]): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                                          = zone
        val (topics, values)                                = partitionArrays(partitions)
        val (startTopics, startPartitions, offsets, epochs) = startArrays(starts)
        val (error, errorCode)                              = errorSlots
        val result                                          =
          Bindings.xkafka_consumer_assign(
            consumer,
            topics,
            values,
            partitions.size.toUSize,
            startTopics,
            startPartitions,
            offsets,
            epochs,
            starts.size.toUSize,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)

  override def subscribe(consumer: Client, topics: NonEmptyList[String]): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val nativeSubscription = Bindings.xkafka_subscription_new(topics.length.toUSize)
        if nativeSubscription == null then throw Failures.plain("could not allocate a subscription")

        try
          topics.toList.foreach(topic => Bindings.xkafka_subscription_add(nativeSubscription, toCString(topic)))
          val (error, errorCode) = errorSlots
          val result             = Bindings.xkafka_consumer_subscribe(consumer, nativeSubscription, error, ErrorBufferSize.toUSize, errorCode)
          if result != 0 then throw nativeError(error, errorCode)
        finally Bindings.xkafka_subscription_destroy(nativeSubscription)

  override def poll(consumer: Client, timeoutMillis: Int): F[List[PolledRecord]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val status             = stackalloc[CInt]()
        val (error, errorCode) = errorSlots
        val message            = Bindings.xkafka_consumer_poll(consumer, timeoutMillis, status, error, ErrorBufferSize.toUSize, errorCode)

        if !status < 0 then throw nativeError(error, errorCode)
        else if !status == 0 then Nil
        else
          try List(copyMessage(message))
          finally Bindings.xkafka_message_destroy(message)

  private def copyMessage(message: CVoidPtr): PolledRecord =
    val keyPointer   = Bindings.xkafka_message_key(message)
    val valuePointer = Bindings.xkafka_message_value(message)
    val timestamp    = Bindings.xkafka_message_timestamp(message)
    PolledRecord(
      topic = fromCString(Bindings.xkafka_message_topic(message)),
      partition = Bindings.xkafka_message_partition(message),
      offset = Bindings.xkafka_message_offset(message),
      timestamp = Option.when(timestamp >= 0L)(timestamp),
      timestampType = Bindings.xkafka_message_timestamp_type(message),
      leaderEpoch = Bindings.xkafka_message_leader_epoch(message),
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
        if result != 0 then throw Failures.plain(s"could not read message header at index $index")
        Header(fromCString(!name), Option.when(!hasValue != 0)(chunk(!value, !valueSize)))
    Headers.fromVector(values)

  override def takeRebalances(consumer: Client): F[List[Rebalance[Event]]] =
    F.blocking:
      Iterator.continually(Bindings.xkafka_consumer_take_rebalance(consumer)).takeWhile(_ != null).map: event =>
        val partitions = Bindings.xkafka_rebalance_partitions(event)
        Rebalance(
          event,
          Bindings.xkafka_rebalance_kind(event),
          List.tabulate(Bindings.xkafka_assignment_count(partitions).toInt): index =>
            LeaseKey(
              fromCString(Bindings.xkafka_assignment_topic_at(partitions, index.toUSize)),
              Bindings.xkafka_assignment_partition_at(partitions, index.toUSize)
            )
        )
      .toList

  override def applyRebalance(consumer: Client, event: Event, starts: Vector[StartAt]): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                                          = zone
        val (startTopics, startPartitions, offsets, epochs) = startArrays(starts)
        Bindings.xkafka_consumer_apply_rebalance(consumer, event, startTopics, startPartitions, offsets, epochs, starts.size.toUSize)

  override def cooperative(consumer: Client): F[Boolean] = F.blocking(Bindings.xkafka_consumer_cooperative(consumer) != 0)

  override def assignment(consumer: Client): F[Vector[(String, Int)]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val assignment         = Bindings.xkafka_consumer_assignment(consumer, error, ErrorBufferSize.toUSize, errorCode)
        if assignment == null then throw nativeError(error, errorCode)

        try Vector.tabulate(Bindings.xkafka_assignment_count(assignment).toInt): index =>
            fromCString(Bindings.xkafka_assignment_topic_at(assignment, index.toUSize)) ->
              Bindings.xkafka_assignment_partition_at(assignment, index.toUSize)
        finally Bindings.xkafka_assignment_destroy(assignment)

  override def commit(consumer: Client, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                                         = zone
        val (topics, partitions, values, epochs, comments) = commitArrays(offsets)
        val (error, errorCode)                             = errorSlots
        val result                                         =
          Bindings.xkafka_consumer_commit(
            consumer,
            topics,
            partitions,
            values,
            epochs,
            comments,
            offsets.size.toUSize,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)

  override def pause(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    partitionCall(partitions)((topics, values, count, error, code) =>
      Bindings.xkafka_consumer_pause(consumer, topics, values, count, error, ErrorBufferSize.toUSize, code)
    )

  override def resume(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    partitionCall(partitions)((topics, values, count, error, code) =>
      Bindings.xkafka_consumer_resume(consumer, topics, values, count, error, ErrorBufferSize.toUSize, code)
    )

  override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[StoredOffset]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (topics, values)   = partitionArrays(partitions)
        val (error, errorCode) = errorSlots
        val committed          =
          Bindings
            .xkafka_consumer_committed(consumer, topics, values, partitions.size.toUSize, timeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if committed == null then throw nativeError(error, errorCode)

        try Vector.tabulate(partitions.size): index =>
            val size     = stackalloc[CSize]()
            val metadata = Bindings.xkafka_committed_metadata_at(committed, index.toUSize, size)
            StoredOffset(
              Bindings.xkafka_committed_offset_at(committed, index.toUSize),
              Bindings.xkafka_committed_leader_epoch_at(committed, index.toUSize),
              if metadata == null then "" else new String(chunk(metadata, !size).toArray, StandardCharsets.UTF_8)
            )
        finally Bindings.xkafka_committed_destroy(committed)

  override def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): F[(Long, Long)] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val low                = stackalloc[CLongLong]()
        val high               = stackalloc[CLongLong]()
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_watermark_offsets(
            consumer,
            toCString(partition.topic.value),
            partition.partition.value,
            low,
            high,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
        (!low, !high)

  override def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): F[Vector[Long]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone           = zone
        val (topics, values) = partitionArrays(searches.map(_._1))
        val timestamps       = alloc[CLongLong](searches.size)
        val timestampOffsets = alloc[CLongLong](searches.size)
        searches.iterator.zipWithIndex.foreach((search, index) => timestamps(index) = search._2.epochMillis)
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_offsets_for_times(
            consumer,
            topics,
            values,
            timestamps,
            searches.size.toUSize,
            timestampOffsets,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)
        Vector.tabulate(searches.size)(timestampOffsets(_))

  override def positions(consumer: Client, partitions: Vector[TopicPartition]): F[Vector[Long]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (topics, values)   = partitionArrays(partitions)
        val positions          = alloc[CLongLong](partitions.size)
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_position(consumer, topics, values, partitions.size.toUSize, positions, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)
        Vector.tabulate(partitions.size)(positions(_))

  override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_consumer_seek(
            consumer,
            toCString(partition.topic.value),
            partition.partition.value,
            offset,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)

  override def metadata(client: Client, topic: Option[String], timeoutMillis: Int): F[Vector[(String, Vector[Int])]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val metadata           =
          Bindings.xkafka_consumer_metadata(client, topic.map(toCString).orNull, timeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if metadata == null then throw nativeError(error, errorCode)

        try Vector.tabulate(Bindings.xkafka_metadata_topic_count(metadata).toInt): topicIndex =>
            fromCString(Bindings.xkafka_metadata_topic_at(metadata, topicIndex.toUSize)) -> Vector.tabulate(
              Bindings.xkafka_metadata_partition_count_at(metadata, topicIndex.toUSize).toInt
            )(partitionIndex => Bindings.xkafka_metadata_partition_at(metadata, topicIndex.toUSize, partitionIndex.toUSize))
        finally Bindings.xkafka_metadata_destroy(metadata)

  override def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): F[Unit] =
    F.blocking:
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
            admin,
            names,
            partitions,
            replication,
            configNames,
            configValues,
            configCounts,
            topics.size.toUSize,
            timeoutMillis,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        if result != 0 then throw nativeError(error, errorCode)

  override def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone = zone
        val names  = alloc[CString](topics.size)
        topics.iterator.zipWithIndex.foreach((value, index) => names(index) = toCString(value.value))
        val (error, errorCode) = errorSlots
        val result = Bindings.xkafka_admin_delete_topics(admin, names, topics.size.toUSize, timeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)

  override def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (error, errorCode) = errorSlots
        val result             =
          Bindings.xkafka_admin_create_partitions(admin, toCString(topic.value), count, timeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if result != 0 then throw nativeError(error, errorCode)

  override def deleteRecords(admin: Client, cuts: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Vector[Either[KafkaException, Offset]]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone                        = zone
        val (topics, partitions, offsets) = offsetArrays(cuts)
        val codes                         = alloc[CInt](cuts.size)
        val lowWatermarks                 = alloc[CLongLong](cuts.size)
        val (error, errorCode)            = errorSlots
        val result                        =
          Bindings.xkafka_admin_delete_records(
            admin,
            topics,
            partitions,
            offsets,
            cuts.size.toUSize,
            timeoutMillis,
            codes,
            lowWatermarks,
            error,
            ErrorBufferSize.toUSize,
            errorCode
          )
        val answer =
          if result != 0 then DeletedRecords.Refused(nativeError(error, errorCode), !errorCode)
          else DeletedRecords.Answered(Vector.tabulate(cuts.size)(index => (codes(index), lowWatermarks(index))))
        LibrdkafkaAnswers.deleted(cuts, answer, errorString)

  override def describeTopicConfigs(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicConfiguration]]] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone = zone
        val names  = alloc[CString](topics.size)
        topics.iterator.zipWithIndex.foreach((value, index) => names(index) = toCString(value.value))
        val (error, errorCode) = errorSlots
        val configs            =
          Bindings.xkafka_admin_describe_topic_configs(admin, names, topics.size.toUSize, timeoutMillis, error, ErrorBufferSize.toUSize, errorCode)
        if configs == null then throw nativeError(error, errorCode)

        try LibrdkafkaAnswers
            .configurations(topics, Vector.tabulate(Bindings.xkafka_configs_resource_count(configs).toInt)(index => resource(configs, index.toUSize)))
        finally Bindings.xkafka_configs_destroy(configs)

  private def resource(configs: CVoidPtr, index: CSize): ConfigResource =
    val error = Bindings.xkafka_configs_resource_error(configs, index)
    ConfigResource(
      fromCString(Bindings.xkafka_configs_resource_name(configs, index)),
      error,
      if error == 0 then "" else fromCString(Bindings.xkafka_configs_resource_error_string(configs, index)),
      if error != 0 then Vector.empty
      else
        Vector.tabulate(Bindings.xkafka_configs_entry_count(configs, index).toInt): entryIndex =>
          val entry = entryIndex.toUSize
          ConfigEntry(
            fromCString(Bindings.xkafka_configs_entry_name(configs, index, entry)),
            Option(Bindings.xkafka_configs_entry_value(configs, index, entry)).map(fromCString(_)),
            Bindings.xkafka_configs_entry_source(configs, index, entry),
            Bindings.xkafka_configs_entry_is_sensitive(configs, index, entry) != 0,
            Bindings.xkafka_configs_entry_is_read_only(configs, index, entry) != 0
          )
    )

  /** librdkafka's description of an error code. */
  private def errorString(code: Int): String = fromCString(Bindings.xkafka_error_string(code))

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

  private def partitionArrays(partitions: Vector[TopicPartition])(using Zone): (Ptr[CString], Ptr[CInt]) =
    val topics = alloc[CString](partitions.size)
    val values = alloc[CInt](partitions.size)
    partitions.iterator.zipWithIndex.foreach: (topicPartition, index) =>
      topics(index) = toCString(topicPartition.topic.value)
      values(index) = topicPartition.partition.value
    (topics, values)

  private def offsetArrays(offsets: Vector[(TopicPartition, Offset)])(using Zone): (Ptr[CString], Ptr[CInt], Ptr[CLongLong]) =
    val (topics, partitions) = partitionArrays(offsets.map(_._1))
    val values               = alloc[CLongLong](offsets.size)
    offsets.iterator.zipWithIndex.foreach((entry, index) => values(index) = entry._2.value)
    (topics, partitions, values)

  /** Where partitions start, as the shim takes them: names, offsets, and leader epochs, negative for none. */
  private def startArrays(starts: Vector[StartAt])(using Zone): (Ptr[CString], Ptr[CInt], Ptr[CLongLong], Ptr[CInt]) =
    val (topics, partitions) = partitionArrays(starts.map(_.partition))
    val offsets              = alloc[CLongLong](starts.size.max(1))
    val epochs               = alloc[CInt](starts.size.max(1))
    starts.iterator.zipWithIndex.foreach: (start, index) =>
      offsets(index) = start.offset
      epochs(index) = start.leaderEpoch
    (topics, partitions, offsets, epochs)

  /** Offsets to commit, with each one's leader epoch, negative for none, and its metadata. */
  private def commitArrays(offsets: Vector[(TopicPartition, OffsetAndMetadata)])(using
      Zone
  ): (Ptr[CString], Ptr[CInt], Ptr[CLongLong], Ptr[CInt], Ptr[CString]) =
    val (topics, partitions) = partitionArrays(offsets.map(_._1))
    val values               = alloc[CLongLong](offsets.size)
    val epochs               = alloc[CInt](offsets.size)
    val metadata             = alloc[CString](offsets.size)
    offsets.iterator.zipWithIndex.foreach: (entry, index) =>
      values(index) = entry._2.offset.value
      epochs(index) = entry._2.leaderEpoch.fold(-1)(_.value)
      metadata(index) = toCString(entry._2.metadata)
    (topics, partitions, values, epochs, metadata)

  /** A call that names partitions and reports a failure through the usual out-parameters. */
  private def partitionCall(partitions: Vector[TopicPartition])(call: (Ptr[CString], Ptr[CInt], CSize, CString, Ptr[CInt]) => CInt): F[Unit] =
    F.blocking:
      Zone.acquire: zone =>
        given Zone             = zone
        val (topics, values)   = partitionArrays(partitions)
        val (error, errorCode) = errorSlots
        if call(topics, values, partitions.size.toUSize, error, errorCode) != 0 then throw nativeError(error, errorCode)

  private def transactional(call: (CString, Ptr[CInt], Ptr[CInt], Ptr[CInt], Ptr[CInt]) => CInt): F[Unit] =
    F.blocking(Zone.acquire(zone => failOnClassified(call)(using zone)))

  private def failOnClassified(call: (CString, Ptr[CInt], Ptr[CInt], Ptr[CInt], Ptr[CInt]) => CInt)(using Zone): Unit =
    val (error, code) = errorSlots
    val fatal         = alloc[CInt](1)
    val retriable     = alloc[CInt](1)
    val abort         = alloc[CInt](1)
    !fatal = -1
    !retriable = -1
    !abort = -1
    if call(error, code, fatal, retriable, abort) != 0 then throw Failures.classified(fromCString(error), !code, !fatal, !retriable, !abort)

  /** Builds a failure from the message and code the call wrote into its own out-parameters. */
  private def nativeError(error: CString, code: Ptr[CInt]): KafkaException.BackendFailure = Failures.coded(fromCString(error), !code)

  /** Allocates the out-parameters a shim call reports a failure through. They live in this frame, so concurrent calls cannot share them. */
  private def errorSlots(using Zone): (CString, Ptr[CInt]) =
    val error = alloc[CChar](ErrorBufferSize)
    val code  = alloc[CInt](1)
    !code = 0
    (error, code)

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
