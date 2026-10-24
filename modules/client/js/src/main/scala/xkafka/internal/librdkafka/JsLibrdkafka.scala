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

import java.nio.charset.StandardCharsets.UTF_8

import scala.scalajs.js
import scala.scalajs.js.typedarray.{byteArray2Int8Array, int8Array2ByteArray, Int8Array}

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all.*
import fs2.Chunk
import driver.{Delivery, EncodedRecord, Enqueued, KafkaDriver, PolledRecord, Rebalance, StartAt, StoredOffset}

/** librdkafka's group metadata, which outlives the consumer it came from. */
private[xkafka] final case class JsGroupMetadata(pointer: js.Any) extends GroupHandle

/** The shim reached through koffi.
  *
  * A call that can wait on the network runs on a thread the shim starts for it and reports back through a callback, so the event loop keeps running
  * while librdkafka waits, and no call holds one of the few worker threads Node shares between file, DNS and crypto work. Calls that only read or
  * update memory run directly.
  */
private[xkafka] final class JsLibrdkafka[F[_]](using F: Async[F]) extends KafkaDriver[F]:
  type Client        = js.Any
  type Batch         = js.Any
  type Event         = js.Any
  type GroupMetadata = JsGroupMetadata

  private val ErrorBufferSize     = 512
  private val UnassignedPartition = -1

  private def shim: F[Shim] = F.fromEither(Shim.loaded)

  override val deliveriesShareQueue: Boolean = true

  /** Starts a shim call on a thread of its own and completes once it has reported. Its results are in the buffers passed to it. */
  private def background(function: Shim => js.Dynamic)(arguments: js.Any*): F[Unit] =
    shim.flatMap: loaded =>
      F.async_[Unit]: callback =>
        val token   = Completions.expect(() => callback(Right(())))
        val started = function(loaded)((arguments ++ Seq[js.Any](Completions.done, bigInt(token)))*)
        if started.asInstanceOf[Int] != 0 then
          Completions.forget(token)
          callback(Left(Failures.plain("could not start a thread for a librdkafka call")))

  /** Runs a call that does not wait, on the calling thread. */
  private def direct[A](call: Shim => A): F[A] = shim.flatMap(loaded => F.delay(call(loaded)))

  override def producer(bootstrapServers: String, clientId: Option[String], properties: Map[String, String], enqueueTimeoutMillis: Int): F[Client] =
    F.defer:
      val error                  = ErrorSlot()
      val (names, values, count) = nativeProperties(properties)
      direct(_.producerNew(bootstrapServers, optional(clientId), names, values, count, error.message, ErrorBufferSize, error.code)).map(error.orFail)

  override def destroyProducer(producer: Client, timeoutMillis: Int): F[Unit] = background(_.producerDestroy)(producer, timeoutMillis)

  override def consumer(
      bootstrapServers: String,
      clientId: Option[String],
      groupId: String,
      autoOffsetReset: String,
      properties: Map[String, String]
  ): F[Client] =
    F.defer:
      val error                  = ErrorSlot()
      val (names, values, count) = nativeProperties(properties)
      direct(_.consumerNew(
        bootstrapServers,
        optional(clientId),
        groupId,
        autoOffsetReset,
        names,
        values,
        count,
        error.message,
        ErrorBufferSize,
        error.code
      )).map(error.orFail)

  // Closing a consumer leaves its group, which waits on the coordinator.
  override def destroyConsumer(consumer: Client): F[Unit] = background(_.consumerDestroy)(consumer)

  override def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client] =
    F.defer:
      val error                  = ErrorSlot()
      val (names, values, count) = nativeProperties(properties)
      direct(_.adminNew(bootstrapServers, optional(clientId), names, values, count, error.message, ErrorBufferSize, error.code)).map(error.orFail)

  override def destroyAdmin(admin: Client): F[Unit] = background(_.adminDestroy)(admin)

  override def batch(capacity: Int): F[Batch] =
    direct(_.batchNew(capacity))
      .flatMap(batch => if batch == null then F.raiseError(Failures.plain("could not allocate a producer batch")) else F.pure(batch))

  override def freeBatch(batch: Batch): F[Unit] = direct(_.batchFree(batch)).void

  override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): F[Enqueued] =
    direct: shim =>
      val error                                 = ErrorSlot()
      def taken(record: EncodedRecord): Boolean =
        val (key, keySize)     = bytes(record.key)
        val (value, valueSize) = bytes(record.value)
        val headers            = nativeHeaders(shim, record.headers, error)
        val result             =
          shim.batchAdd(
            producer,
            batch,
            record.topic.value,
            record.partition.fold(UnassignedPartition)(_.value),
            bigInt(record.timestamp.fold(-1L)(_.epochMillis)),
            key,
            keySize,
            value,
            valueSize,
            headers,
            error.message,
            ErrorBufferSize,
            error.code
          )
        result.asInstanceOf[Int] match
          case 0 => true
          case 1 => false
          case _ => throw error.failure
      Enqueued(records.iterator.takeWhile(taken).size, gaveUp = false)

  private def nativeHeaders(shim: Shim, headers: Headers, error: ErrorSlot): js.Any =
    val native = shim.headersNew(headers.values.size)
    if native == null then throw Failures.plain("could not allocate message headers")

    try
      headers.values.foreach: header =>
        val (value, size) = bytes(header.value)
        val result        = shim.headersAdd(native, header.key, value, size, error.message, ErrorBufferSize, error.code)
        if result.asInstanceOf[Int] != 0 then throw error.failure
      native
    catch
      case failure: Throwable =>
        shim.headersDestroy(native): Unit
        throw failure

  override def serveDeliveries(producer: Client, timeoutMillis: Int): F[Unit] = background(_.producerPoll)(producer, timeoutMillis).void

  override def flush(producer: Client, timeoutMillis: Int): F[Unit] = background(_.producerFlush)(producer, timeoutMillis)

  /** Reports reach the batch through `serveDeliveries`, so this only reads how many are outstanding. */
  override def awaitDeliveries(batch: Batch, timeoutMillis: Int): F[Int] = pending(batch)

  override def pending(batch: Batch): F[Int] = direct(_.batchPending(batch).asInstanceOf[Double].toInt)

  override def deliveries(producer: Client, batch: Batch): F[Vector[Either[KafkaException, Delivery]]] =
    direct: shim =>
      Vector.tabulate(shim.batchCount(batch).asInstanceOf[Double].toInt): index =>
        val error = ErrorSlot()
        if shim.batchErrorAt(producer, batch, index, error.message, ErrorBufferSize, error.code).asInstanceOf[Int] != 0 then Left(error.failure)
        else
          Right(Delivery(
            shim.batchPartitionAt(batch, index).asInstanceOf[Int],
            long(shim.batchOffsetAt(batch, index)),
            long(shim.batchTimestampAt(batch, index))
          ))

  override def initTransactions(producer: Client, timeoutMillis: Int): F[Unit] =
    transactional((error, result) =>
      background(_.initTransactions)((Seq[js.Any](producer, timeoutMillis) ++ error.classifiedArguments :+ result.value)*)
    )

  // Beginning a transaction only changes the producer's own state, so it waits on nothing.
  override def beginTransaction(producer: Client): F[Unit] =
    F.defer:
      val error = ErrorSlot()
      direct(_.beginTransaction((producer +: error.classifiedArguments)*)).flatMap: result =>
        if result.asInstanceOf[Int] != 0 then F.raiseError(error.classifiedFailure) else F.unit

  override def commitTransaction(producer: Client, timeoutMillis: Int): F[Unit] =
    transactional((error, result) =>
      background(_.commitTransaction)((Seq[js.Any](producer, timeoutMillis) ++ error.classifiedArguments :+ result.value)*)
    )

  override def abortTransaction(producer: Client, timeoutMillis: Int): F[Unit] =
    transactional((error, result) =>
      background(_.abortTransaction)((Seq[js.Any](producer, timeoutMillis) ++ error.classifiedArguments :+ result.value)*)
    )

  override def sendOffsets(
      producer: Client,
      metadata: GroupMetadata,
      offsets: Vector[(TopicPartition, OffsetAndMetadata)],
      timeoutMillis: Int
  ): F[Unit] =
    F.defer:
      val (topics, partitions)       = partitionArrays(offsets.map(_._1))
      val (values, epochs, comments) = commitArrays(offsets.map(_._2))
      transactional((error, result) =>
        background(_.sendOffsets)(
          (Seq[js.Any](producer, metadata.pointer, topics, partitions, values, epochs, comments, offsets.size, timeoutMillis) ++
            error.classifiedArguments :+ result.value)*
        )
      )

  override def groupMetadata(consumer: Client): F[GroupMetadata] = direct(_.groupMetadata(consumer)).map(JsGroupMetadata(_))

  override def destroyGroupMetadata(metadata: GroupMetadata): F[Unit] = direct(_.groupMetadataDestroy(metadata.pointer)).void

  override def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata] =
    handle match
      case metadata: JsGroupMetadata => Some(metadata)
      case _                         => None

  override def assign(consumer: Client, partitions: Vector[TopicPartition], starts: Vector[StartAt]): F[Unit] =
    val (topics, values) = partitionArrays(partitions)
    val error            = ErrorSlot()
    val arguments        =
      Seq[js.Any](consumer, topics, values, partitions.size) ++ startArrays(starts) ++ Seq[js.Any](error.message, ErrorBufferSize, error.code)
    direct(_.assign(arguments*)).flatMap(error.check)

  override def subscribe(consumer: Client, topics: NonEmptyList[String]): F[Unit] =
    direct: shim =>
      val subscription = shim.subscriptionNew(topics.length)
      if subscription == null then throw Failures.plain("could not allocate a subscription")

      try
        topics.toList.foreach(topic => shim.subscriptionAdd(subscription, topic))
        val error = ErrorSlot()
        if shim.subscribe(consumer, subscription, error.message, ErrorBufferSize, error.code).asInstanceOf[Int] != 0 then throw error.failure
      finally shim.subscriptionDestroy(subscription): Unit

  override def poll(consumer: Client, timeoutMillis: Int): F[List[PolledRecord]] =
    F.defer:
      val error  = ErrorSlot()
      val status = Buffers.alloc(4)
      val result = Result()
      background(_.poll)(consumer, timeoutMillis, status, error.message, ErrorBufferSize, error.code, result.value) *> F.defer:
        status.readInt32LE(0) match
          case negative if negative < 0 => F.raiseError(error.failure)
          case 0                        => F.pure(Nil)
          case _                        => direct: shim =>
              val message = result.pointer
              try List(copyMessage(shim, message))
              finally shim.messageDestroy(message): Unit

  private def copyMessage(shim: Shim, message: js.Any): PolledRecord =
    val timestamp = long(shim.messageTimestamp(message))
    PolledRecord(
      topic = shim.messageTopic(message).asInstanceOf[String],
      partition = shim.messagePartition(message).asInstanceOf[Int],
      offset = long(shim.messageOffset(message)),
      timestamp = Option.when(timestamp >= 0L)(timestamp),
      timestampType = shim.messageTimeType(message).asInstanceOf[Int],
      leaderEpoch = shim.messageEpoch(message).asInstanceOf[Int],
      key = chunk(shim.messageKey(message), shim.messageKeySize(message)),
      value = chunk(shim.messageValue(message), shim.messageValueSize(message)),
      headers = copyHeaders(shim, message)
    )

  private def copyHeaders(shim: Shim, message: js.Any): Headers =
    val values =
      Vector.tabulate(shim.messageHeaderCnt(message).asInstanceOf[Double].toInt): index =>
        val name      = js.Array[js.Any](null)
        val value     = js.Array[js.Any](null)
        val valueSize = js.Array[js.Any](0)
        val hasValue  = js.Array[Int](0)
        if shim.messageHeaderAt(message, index, name, value, valueSize, hasValue).asInstanceOf[Int] != 0 then
          throw Failures.plain(s"could not read message header at index $index")
        val copied = if hasValue(0) != 0 then Some(chunk(value(0), valueSize(0)).getOrElse(Chunk.empty)) else None
        Header(name(0).asInstanceOf[String], copied)
    Headers.fromVector(values)

  override def takeRebalances(consumer: Client): F[List[Rebalance[Event]]] =
    direct: shim =>
      Iterator.continually(shim.takeRebalance(consumer): js.Any).takeWhile(_ != null).map: event =>
        val partitions = shim.rebalancePartitions(event)
        Rebalance[Event](
          event,
          shim.rebalanceKind(event).asInstanceOf[Int],
          List.tabulate(shim.assignmentCount(partitions).asInstanceOf[Double].toInt): index =>
            LeaseKey(shim.assignmentTopicAt(partitions, index).asInstanceOf[String], shim.assignmentPartitionAt(partitions, index).asInstanceOf[Int])
        )
      .toList

  override def applyRebalance(consumer: Client, event: Event, starts: Vector[StartAt]): F[Unit] =
    direct(_.applyRebalance((Seq[js.Any](consumer, event) ++ startArrays(starts))*)).void

  override def cooperative(consumer: Client): F[Boolean] = direct(_.cooperative(consumer).asInstanceOf[Int] != 0)

  override def assignment(consumer: Client): F[Vector[(String, Int)]] =
    direct: shim =>
      val error      = ErrorSlot()
      val assignment = shim.assignment(consumer, error.message, ErrorBufferSize, error.code)
      if assignment == null then throw error.failure

      try Vector.tabulate(shim.assignmentCount(assignment).asInstanceOf[Double].toInt): index =>
          shim.assignmentTopicAt(assignment, index).asInstanceOf[String] -> shim.assignmentPartitionAt(assignment, index).asInstanceOf[Int]
      finally shim.assignmentDestroy(assignment): Unit

  override def commit(consumer: Client, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): F[Unit] =
    F.defer:
      val (topics, partitions)       = partitionArrays(offsets.map(_._1))
      val (values, epochs, comments) = commitArrays(offsets.map(_._2))
      val error                      = ErrorSlot()
      val result                     = Result()
      background(_.commit)(
        consumer,
        topics,
        partitions,
        values,
        epochs,
        comments,
        offsets.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> error.checked(result)

  override def pause(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] = partitionCall(_.pause, consumer, partitions)

  override def resume(consumer: Client, partitions: Vector[TopicPartition]): F[Unit] = partitionCall(_.resume, consumer, partitions)

  override def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[StoredOffset]] =
    F.defer:
      val (topics, values) = partitionArrays(partitions)
      val error            = ErrorSlot()
      val result           = Result()
      background(_.committed)(consumer, topics, values, partitions.size, timeoutMillis, error.message, ErrorBufferSize, error.code, result.value) *>
        F.defer:
          val committed = result.pointer
          if committed == null then F.raiseError(error.failure)
          else
            direct: shim =>
              try Vector.tabulate(partitions.size): index =>
                  val size     = js.Array[js.Any](0)
                  val metadata = chunk(shim.committedMetadataAt(committed, index, size), size(0)).fold("")(bytes => new String(bytes.toArray, UTF_8))
                  StoredOffset(long(shim.committedOffsetAt(committed, index)), shim.committedEpochAt(committed, index).asInstanceOf[Int], metadata)
              finally shim.committedDestroy(committed): Unit

  override def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): F[(Long, Long)] =
    F.defer:
      val low    = Buffers.alloc(8)
      val high   = Buffers.alloc(8)
      val error  = ErrorSlot()
      val result = Result()
      background(_.watermarks)(
        consumer,
        partition.topic.value,
        partition.partition.value,
        low,
        high,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> error.checked(result) *> F.delay((readLongs(low, 1).head, readLongs(high, 1).head))

  override def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): F[Vector[Long]] =
    F.defer:
      val (topics, partitions) = partitionArrays(searches.map(_._1))
      val offsets              = Buffers.alloc(8 * searches.size.max(1))
      val error                = ErrorSlot()
      val result               = Result()
      background(_.offsetsForTimes)(
        consumer,
        topics,
        partitions,
        longs(searches.map(_._2.epochMillis)),
        searches.size,
        offsets,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> error.checked(result) *> F.delay(readLongs(offsets, searches.size))

  override def positions(consumer: Client, partitions: Vector[TopicPartition]): F[Vector[Long]] =
    F.defer:
      val (topics, values) = partitionArrays(partitions)
      val offsets          = Buffers.alloc(8 * partitions.size.max(1))
      val error            = ErrorSlot()
      direct(_.position(consumer, topics, values, partitions.size, offsets, error.message, ErrorBufferSize, error.code)).flatMap(error.check) *>
        F.delay(readLongs(offsets, partitions.size))

  override def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): F[Unit] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.seek)(
        consumer,
        partition.topic.value,
        partition.partition.value,
        bigInt(offset),
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> error.checked(result)

  override def metadata(client: Client, topic: Option[String], timeoutMillis: Int): F[Vector[(String, Vector[Int])]] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.metadata)(client, optional(topic), timeoutMillis, error.message, ErrorBufferSize, error.code, result.value) *> F.defer:
        val metadata = result.pointer
        if metadata == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try Vector.tabulate(shim.metadataTopicCount(metadata).asInstanceOf[Double].toInt): topicIndex =>
                shim.metadataTopicAt(metadata, topicIndex).asInstanceOf[String] -> Vector.tabulate(
                  shim.metadataPartitionCount(metadata, topicIndex).asInstanceOf[Double].toInt
                )(partitionIndex => shim.metadataPartitionAt(metadata, topicIndex, partitionIndex).asInstanceOf[Int])
            finally shim.metadataDestroy(metadata): Unit

  override def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): F[Unit] =
    F.defer:
      val configured   = topics.flatMap(_.configuration.toVector)
      val configCounts = Buffers.alloc(8 * topics.size.max(1))
      topics.iterator.zipWithIndex.foreach((topic, index) => configCounts.writeBigUInt64LE(bigInt(topic.configuration.size.toLong), 8 * index): Unit)
      val error  = ErrorSlot()
      val result = Result()
      background(_.createTopics)(
        admin,
        strings(topics.map(_.topic.value)),
        ints(topics.map(_.partitions)),
        ints(topics.map(_.replicationFactor.toInt)),
        if configured.isEmpty then null else strings(configured.map(_._1)),
        if configured.isEmpty then null else strings(configured.map(_._2)),
        configCounts,
        topics.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> error.checked(result)

  override def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Unit] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.deleteTopics)(
        admin,
        strings(topics.map(_.value)),
        topics.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> error.checked(result)

  override def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): F[Unit] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.createPartitions)(admin, topic.value, count, timeoutMillis, error.message, ErrorBufferSize, error.code, result.value) *>
        error.checked(result)

  override def deleteRecords(admin: Client, cuts: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Vector[Either[KafkaException, Offset]]] =
    F.defer:
      val (topics, partitions) = partitionArrays(cuts.map(_._1))
      val codes                = Buffers.alloc(4 * cuts.size.max(1))
      val lowWatermarks        = Buffers.alloc(8 * cuts.size.max(1))
      val error                = ErrorSlot()
      val result               = Result()
      background(_.deleteRecords)(
        admin,
        topics,
        partitions,
        longs(cuts.map(_._2.value)),
        cuts.size,
        timeoutMillis,
        codes,
        lowWatermarks,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> F.delay:
        val answer =
          if result.int != 0 then DeletedRecords.Refused(error.failure, error.codeValue)
          else
            DeletedRecords.Answered(Vector.tabulate(cuts.size)(index => (codes.readInt32LE(4 * index), readLongs(lowWatermarks, cuts.size)(index))))
        LibrdkafkaAnswers.deleted(cuts, answer, errorString)

  override def describeTopicConfigs(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicConfiguration]]] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.describeConfigs)(
        admin,
        strings(topics.map(_.value)),
        topics.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> F.defer:
        val configs = result.pointer
        if configs == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers
                .configurations(topics, Vector.tabulate(shim.configsResourceCount(configs).asInstanceOf[Double].toInt)(resource(shim, configs, _)))
            finally shim.configsDestroy(configs): Unit

  override def alterTopicConfigs(
      admin: Client,
      changes: Vector[(Topic, NonEmptyList[(String, ConfigurationChange)])],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, Unit]]] =
    F.defer:
      val entries = LibrdkafkaAnswers.configurationChanges(changes)
      val error   = ErrorSlot()
      val result  = Result()
      background(_.alterTopicConfigs)(
        (Seq[js.Any](
          admin,
          strings(entries.map(_._1)),
          strings(entries.map(_._2)),
          js.Array(entries.map(_._3.orNull)*),
          ints(entries.map(_._4)),
          entries.size,
          timeoutMillis
        ) ++ errorArguments(error) :+ result.value)*
      ) *> F.defer:
        val configs = result.pointer
        if configs == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers.changed(
                changes.map(_._1),
                Vector.tabulate(shim.configsResourceCount(configs).asInstanceOf[Double].toInt)(resource(shim, configs, _))
              )
            finally shim.configsDestroy(configs): Unit

  override def electPreferredLeaders(admin: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[Either[KafkaException, Unit]]] =
    val (topics, values) = partitionArrays(partitions)
    F.defer:
      val codes  = Buffers.alloc(4 * partitions.size.max(1))
      val error  = ErrorSlot()
      val result = Result()
      background(_.electPreferredLeaders)(
        (Seq[js.Any](admin, topics, values, partitions.size, timeoutMillis, codes) ++ errorArguments(error) :+ result.value)*
      ) *> F.delay:
        val answer =
          if result.int != 0 then Coded.Refused(error.failure)
          else Coded.Answered(Vector.tabulate(partitions.size)(index => codes.readInt32LE(4 * index)))
        LibrdkafkaAnswers.elected(partitions, answer, errorString)

  override def listGroups(admin: Client, timeoutMillis: Int): F[Vector[(ConsumerGroup, ConsumerGroupState)]] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.listGroups)(admin, timeoutMillis, error.message, ErrorBufferSize, error.code, result.value) *> F.defer:
        val groups = result.pointer
        if groups == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers.listed(
                Vector.tabulate(int(shim.groupsCount(groups))): index =>
                  (shim.groupsIdAt(groups, index).asInstanceOf[String], int(shim.groupsStateAt(groups, index)))
              )
            finally shim.groupsDestroy(groups): Unit

  override def describeCluster(admin: Client, timeoutMillis: Int): F[ClusterDescription] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.describeCluster)(admin, timeoutMillis, error.message, ErrorBufferSize, error.code, result.value) *> F.defer:
        val cluster = result.pointer
        if cluster == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers.cluster(DescribedCluster(
                optionalString(shim.clusterId(cluster)),
                Option(shim.clusterController(cluster)).map(node(shim, _)),
                Vector.tabulate(int(shim.clusterBrokerCount(cluster)))(index => node(shim, shim.clusterBrokerAt(cluster, index)))
              ))
            finally shim.clusterDestroy(cluster): Unit

  private def node(shim: Shim, node: js.Any): Node =
    Node(int(shim.nodeId(node)), shim.nodeHost(node).asInstanceOf[String], int(shim.nodePort(node)), optionalString(shim.nodeRack(node)))

  override def describeTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicDescription]]] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.describeTopics)(
        admin,
        strings(topics.map(_.value)),
        topics.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> F.defer:
        val described = result.pointer
        if described == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers.topics(topics, Vector.tabulate(int(shim.topicsCount(described)))(describedTopic(shim, described, _)))
            finally shim.topicsDestroy(described): Unit

  private def describedTopic(shim: Shim, topics: js.Any, topic: Int): DescribedTopic =
    val error = int(shim.topicError(topics, topic))
    DescribedTopic(
      shim.topicName(topics, topic).asInstanceOf[String],
      error,
      shim.topicErrorString(topics, topic).asInstanceOf[String],
      (long(shim.topicIdMost(topics, topic)), long(shim.topicIdLeast(topics, topic))),
      int(shim.topicInternal(topics, topic)) != 0,
      if error != 0 then Vector.empty
      else
        Vector.tabulate(int(shim.topicPartitionCount(topics, topic))): partition =>
          DescribedPartition(
            int(shim.topicPartitionAt(topics, topic, partition)),
            int(shim.topicPartitionLeader(topics, topic, partition)),
            Vector
              .tabulate(int(shim.topicReplicaCount(topics, topic, partition)))(index => int(shim.topicReplicaAt(topics, topic, partition, index))),
            Vector.tabulate(int(shim.topicInSyncCount(topics, topic, partition)))(index => int(shim.topicInSyncAt(topics, topic, partition, index)))
          )
    )

  override def describeGroups(
      admin: Client,
      groups: Vector[ConsumerGroup],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, ConsumerGroupDescription]]] =
    F.defer:
      val error  = ErrorSlot()
      val result = Result()
      background(_.describeGroups)(
        admin,
        strings(groups.map(_.value)),
        groups.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> F.defer:
        val descriptions = result.pointer
        if descriptions == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers.described(groups, Vector.tabulate(int(shim.descriptionsCount(descriptions)))(describedGroup(shim, descriptions, _)))
            finally shim.descriptionsDestroy(descriptions): Unit

  private def describedGroup(shim: Shim, descriptions: js.Any, group: Int): DescribedGroup =
    val error = int(shim.descriptionError(descriptions, group))
    DescribedGroup(
      shim.descriptionGroup(descriptions, group).asInstanceOf[String],
      error,
      shim.descriptionErrorString(descriptions, group).asInstanceOf[String],
      int(shim.descriptionSimple(descriptions, group)) != 0,
      int(shim.descriptionState(descriptions, group)),
      optionalString(shim.descriptionAssignor(descriptions, group)),
      Option.when(error == 0 && int(shim.descriptionCoordinatorId(descriptions, group)) >= 0):
        Node(
          int(shim.descriptionCoordinatorId(descriptions, group)),
          shim.descriptionCoordinatorHost(descriptions, group).asInstanceOf[String],
          int(shim.descriptionCoordinatorPort(descriptions, group)),
          optionalString(shim.descriptionCoordinatorRack(descriptions, group))
        )
      ,
      if error != 0 then Nil
      else
        List.tabulate(int(shim.descriptionMemberCount(descriptions, group))): member =>
          DescribedMember(
            shim.memberId(descriptions, group, member).asInstanceOf[String],
            optionalString(shim.memberInstanceId(descriptions, group, member)),
            shim.memberClientId(descriptions, group, member).asInstanceOf[String],
            shim.memberHost(descriptions, group, member).asInstanceOf[String],
            Vector.tabulate(int(shim.memberPartitionCount(descriptions, group, member))): index =>
              (
                shim.memberTopicAt(descriptions, group, member, index).asInstanceOf[String],
                int(shim.memberPartitionAt(descriptions, group, member, index))
              )
          )
    )

  override def groupOffsets(
      admin: Client,
      group: ConsumerGroup,
      partitions: Option[Vector[TopicPartition]],
      timeoutMillis: Int
  ): F[Vector[(TopicPartition, StoredOffset)]] =
    F.defer:
      val named            = partitions.getOrElse(Vector.empty)
      val (topics, values) = partitionArrays(named)
      val error            = ErrorSlot()
      val result           = Result()
      background(_.listGroupOffsets)(
        admin,
        group.value,
        topics,
        values,
        named.size,
        timeoutMillis,
        error.message,
        ErrorBufferSize,
        error.code,
        result.value
      ) *> F.defer:
        val committed = result.pointer
        if committed == null then F.raiseError(error.failure)
        else
          direct: shim =>
            try LibrdkafkaAnswers.committed(
                Vector.tabulate(int(shim.assignmentCount(committed))): index =>
                  val size     = js.Array[js.Any](0)
                  val metadata = chunk(shim.committedMetadataAt(committed, index, size), size(0)).fold("")(bytes => new String(bytes.toArray, UTF_8))
                  (
                    shim.assignmentTopicAt(committed, index).asInstanceOf[String],
                    int(shim.assignmentPartitionAt(committed, index)),
                    StoredOffset(long(shim.committedOffsetAt(committed, index)), int(shim.committedEpochAt(committed, index)), metadata)
                  )
              )
            finally shim.committedDestroy(committed): Unit

  override def alterGroupOffsets(
      admin: Client,
      group: ConsumerGroup,
      offsets: Vector[(TopicPartition, OffsetAndMetadata)],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, Unit]]] =
    val (topics, partitions)       = partitionArrays(offsets.map(_._1))
    val (values, epochs, comments) = commitArrays(offsets.map(_._2))
    codedCall(offsets.map(_._1), _.show)((codes, error, result) =>
      background(_.alterGroupOffsets)(
        (Seq[js.Any](admin, group.value, topics, partitions, values, epochs, comments, offsets.size, timeoutMillis, codes) ++ errorArguments(error) :+
          result.value)*
      )
    )

  override def deleteGroupOffsets(
      admin: Client,
      group: ConsumerGroup,
      partitions: Vector[TopicPartition],
      timeoutMillis: Int
  ): F[Vector[Either[KafkaException, Unit]]] =
    val (topics, values) = partitionArrays(partitions)
    codedCall(partitions, _.show)((codes, error, result) =>
      background(_.deleteGroupOffsets)(
        (Seq[js.Any](admin, group.value, topics, values, partitions.size, timeoutMillis, codes) ++ errorArguments(error) :+ result.value)*
      )
    )

  override def deleteGroups(admin: Client, groups: Vector[ConsumerGroup], timeoutMillis: Int): F[Vector[Either[KafkaException, Unit]]] =
    codedCall(groups, _.value)((codes, error, result) =>
      background(_.deleteGroups)(
        (Seq[js.Any](admin, strings(groups.map(_.value)), groups.size, timeoutMillis, codes) ++ errorArguments(error) :+ result.value)*
      )
    )

  /** A call that writes a code for each requested item, or fails as a whole through the usual out-parameters. */
  private def codedCall[K](requested: Vector[K], name: K => String)(
      call: (Buffer, ErrorSlot, Result) => F[Unit]
  ): F[Vector[Either[KafkaException, Unit]]] =
    F.defer:
      val codes  = Buffers.alloc(4 * requested.size.max(1))
      val error  = ErrorSlot()
      val result = Result()
      call(codes, error, result) *> F.delay:
        val answer =
          if result.int != 0 then Coded.Refused(error.failure)
          else Coded.Answered(Vector.tabulate(requested.size)(index => codes.readInt32LE(4 * index)))
        LibrdkafkaAnswers.eachCoded(requested, answer, name, errorString)

  private def errorArguments(error: ErrorSlot): Seq[js.Any] = Seq(error.message, ErrorBufferSize, error.code)

  private def optionalString(value: js.Any): Option[String] = Option(value).map(_.asInstanceOf[String])

  private def int(value: js.Any): Int = value.asInstanceOf[Double].toInt

  private def resource(shim: Shim, configs: js.Any, index: Int): ConfigResource =
    val error = shim.configsResourceError(configs, index).asInstanceOf[Int]
    ConfigResource(
      shim.configsResourceName(configs, index).asInstanceOf[String],
      error,
      if error == 0 then "" else shim.configsResourceString(configs, index).asInstanceOf[String],
      if error != 0 then Vector.empty
      else
        Vector.tabulate(shim.configsEntryCount(configs, index).asInstanceOf[Double].toInt): entry =>
          ConfigEntry(
            shim.configsEntryName(configs, index, entry).asInstanceOf[String],
            Option(shim.configsEntryValue(configs, index, entry).asInstanceOf[String]),
            shim.configsEntrySource(configs, index, entry).asInstanceOf[Int],
            shim.configsEntrySensitive(configs, index, entry).asInstanceOf[Int] != 0,
            shim.configsEntryReadOnly(configs, index, entry).asInstanceOf[Int] != 0
          )
    )

  /** librdkafka's description of an error code. */
  private def errorString(code: Int): String = Shim.loaded.fold(_ => s"librdkafka error $code", _.errorString(code).asInstanceOf[String])

  private def partitionCall(function: Shim => js.Dynamic, consumer: Client, partitions: Vector[TopicPartition]): F[Unit] =
    val (topics, values) = partitionArrays(partitions)
    val error            = ErrorSlot()
    direct(function(_)(consumer, topics, values, partitions.size, error.message, ErrorBufferSize, error.code)).flatMap(error.check)

  private def transactional(call: (ErrorSlot, Result) => F[Unit]): F[Unit] =
    val error  = ErrorSlot()
    val result = Result()
    call(error, result) *> F.defer(if result.int != 0 then F.raiseError(error.classifiedFailure) else F.unit)

  /** What a call that ran on its own thread returned: an `int` or a pointer. */
  private final class Result:
    val value: Buffer = Buffers.alloc(8)

    def int: Int = value.readInt32LE(0)

    def pointer: js.Any = Koffi.decode(value, "void *")

  /** The out-parameters a shim call reports a failure through, fresh for each call so concurrent calls cannot share them. The flags start at -1,
    * which the shim leaves in place when librdkafka does not say.
    */
  private final class ErrorSlot:
    val message: Buffer   = Buffers.alloc(ErrorBufferSize)
    val code: Buffer      = Buffers.alloc(4)
    val fatal: Buffer     = unknownFlag
    val retriable: Buffer = unknownFlag
    val abort: Buffer     = unknownFlag

    private def unknownFlag: Buffer =
      val flag = Buffers.alloc(4)
      flag.writeInt32LE(-1, 0): Unit
      flag

    def classifiedArguments: Seq[js.Any] = Seq(message, ErrorBufferSize, code, fatal, retriable, abort)

    def codeValue: Int = code.readInt32LE(0)

    def text: String =
      val end = message.indexOf(0)
      message.toString("utf8", 0, if end < 0 then ErrorBufferSize else end)

    def failure: KafkaException.BackendFailure = Failures.coded(text, codeValue)

    def classifiedFailure: KafkaException.BackendFailure = Failures.classified(text, codeValue, fatal.readInt32LE(0), abort.readInt32LE(0))

    /** A handle, or the failure the call reported in place of one. */
    def orFail(handle: js.Any): js.Any = if handle == null then throw failure else handle

    def check(result: js.Any): F[Unit] = if result.asInstanceOf[Int] != 0 then F.raiseError(failure) else F.unit

    /** Fails with what the call reported where the `int` it returned says it failed. */
    def checked(result: Result): F[Unit] = F.defer(if result.int != 0 then F.raiseError(failure) else F.unit)

  private def optional(value: Option[String]): js.Any = value.fold[js.Any](null)(identity)

  private def nativeProperties(properties: Map[String, String]): (js.Any, js.Any, Int) =
    val entries = properties.toVector
    if entries.isEmpty then (null, null, 0) else (strings(entries.map(_._1)), strings(entries.map(_._2)), entries.size)

  private def strings(values: Vector[String]): js.Array[String] = js.Array(values*)

  private def partitionArrays(partitions: Vector[TopicPartition]): (js.Array[String], Buffer) =
    (strings(partitions.map(_.topic.value)), ints(partitions.map(_.partition.value)))

  /** Where partitions start, as the shim takes them: names, offsets, leader epochs, and how many. */
  private def startArrays(starts: Vector[StartAt]): Seq[js.Any] =
    val (topics, partitions) = partitionArrays(starts.map(_.partition))
    Seq[js.Any](topics, partitions, longs(starts.map(_.offset)), ints(starts.map(_.leaderEpoch)), starts.size)

  /** Offsets to commit, with each one's leader epoch, negative for none, and its metadata. */
  private def commitArrays(offsets: Vector[OffsetAndMetadata]): (Buffer, Buffer, js.Array[String]) =
    (longs(offsets.map(_.offset.value)), ints(offsets.map(_.leaderEpoch.fold(-1)(_.value))), strings(offsets.map(_.metadata)))

  private def ints(values: Vector[Int]): Buffer =
    val buffer = Buffers.alloc(4 * values.size.max(1))
    values.iterator.zipWithIndex.foreach((value, index) => buffer.writeInt32LE(value, 4 * index): Unit)
    buffer

  private def longs(values: Vector[Long]): Buffer =
    val buffer = Buffers.alloc(8 * values.size.max(1))
    values.iterator.zipWithIndex.foreach((value, index) => buffer.writeBigInt64LE(bigInt(value), 8 * index): Unit)
    buffer

  private def readLongs(buffer: Buffer, count: Int): Vector[Long] = Vector.tabulate(count)(index => long(buffer.readBigInt64LE(8 * index)))

  private def bigInt(value: Long): js.BigInt = js.BigInt(value.toString)

  /** koffi returns a 64-bit integer as a number where it fits one exactly and as a BigInt otherwise. */
  private def long(value: js.Any): Long =
    if js.typeOf(value) == "bigint" then value.asInstanceOf[js.BigInt].toString.toLong else value.asInstanceOf[Double].toLong

  /** Bytes for a `const void *` argument. Empty bytes still get a pointer, since the shim reads a null pointer as no value at all. */
  private def bytes(value: Option[Chunk[Byte]]): (js.Any, Int) =
    value.fold[(js.Any, Int)]((null, 0)): chunk =>
      if chunk.isEmpty then (new Int8Array(1), 0) else (byteArray2Int8Array(chunk.toArray), chunk.size)

  /** Copies bytes out of librdkafka's memory, which the message owns. */
  private def chunk(pointer: js.Any, size: js.Any): Option[Chunk[Byte]] =
    if pointer == null then None
    else
      val count = size.asInstanceOf[Double].toInt
      if count == 0 then Some(Chunk.empty) else Some(Chunk.array(int8Array2ByteArray(new Int8Array(Koffi.view(pointer, count.toDouble)))))

private[xkafka] object JsLibrdkafka:
  /** The librdkafka version the loaded library carries. */
  def version: String = Shim.loaded.fold(throw _, _.versionStr().asInstanceOf[String])
