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

import cats.data.NonEmptyList
import fs2.Chunk

/** The C shim's functions, in Scala types, as each platform reaches them.
  *
  * Every call that can wait on the network returns once it has an answer, so a platform runs it where waiting blocks nothing else. A call that fails
  * raises the `KafkaException.BackendFailure` the shim described, built with [[Failures]]. Handles are the platform's own pointers and are valid
  * until the matching destroy.
  */
private[xkafka] trait Librdkafka[F[_]]:
  /** A librdkafka client: a producer, a consumer or an administration client. */
  type Client

  /** The delivery slots of one produced batch, owned until freed. */
  type Batch

  /** A rebalance the consumer received and has not applied yet. */
  type Event

  /** A consumer's group identity, owned until destroyed, which a transaction records offsets against. */
  type GroupMetadata <: GroupHandle

  def version: String

  def producer(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client]

  /** Delivers what the producer already accepted for up to the timeout, then destroys it. */
  def destroyProducer(producer: Client, timeoutMillis: Int): F[Unit]

  def consumer(
      bootstrapServers: String,
      clientId: Option[String],
      groupId: String,
      autoOffsetReset: String,
      properties: Map[String, String]
  ): F[Client]

  def destroyConsumer(consumer: Client): F[Unit]

  def admin(bootstrapServers: String, clientId: Option[String], properties: Map[String, String]): F[Client]

  def destroyAdmin(admin: Client): F[Unit]

  def batch(capacity: Int): F[Batch]

  def freeBatch(batch: Batch): F[Unit]

  /** Enqueues every record in order, failing at the first one librdkafka refuses; the records before it stay enqueued. */
  def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): F[Unit]

  /** Serves delivery reports for up to the timeout and answers how many of the batch's reports are still outstanding. */
  def serveDeliveries(producer: Client, batch: Batch, timeoutMillis: Int): F[Int]

  def pending(batch: Batch): F[Int]

  /** What each record of a fully reported batch was delivered as, in enqueue order, failing with the first failed delivery. */
  def deliveries(producer: Client, batch: Batch): F[Vector[Delivery]]

  def initTransactions(producer: Client, timeoutMillis: Int): F[Unit]

  def beginTransaction(producer: Client): F[Unit]

  def commitTransaction(producer: Client, timeoutMillis: Int): F[Unit]

  def abortTransaction(producer: Client, timeoutMillis: Int): F[Unit]

  def sendOffsets(producer: Client, metadata: GroupMetadata, offsets: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Unit]

  def groupMetadata(consumer: Client): F[GroupMetadata]

  def destroyGroupMetadata(metadata: GroupMetadata): F[Unit]

  /** The handle as group metadata of this platform, if it is one. Handles from every client on the platform qualify. */
  def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata]

  def assign(consumer: Client, partitions: Vector[TopicPartition]): F[Unit]

  /** Subscribes to topic names, or to patterns spelled the way librdkafka reads them. */
  def subscribe(consumer: Client, topics: NonEmptyList[String]): F[Unit]

  /** One record, or none when the timeout passes first or the poll only reached the end of a partition. */
  def poll(consumer: Client, timeoutMillis: Int): F[Option[PolledRecord]]

  /** Every rebalance the consumer received since the last call, oldest first. */
  def takeRebalances(consumer: Client): F[List[Rebalance[Event]]]

  def applyRebalance(consumer: Client, event: Event): F[Unit]

  def cooperative(consumer: Client): F[Boolean]

  def assignment(consumer: Client): F[Vector[(String, Int)]]

  def commit(consumer: Client, offsets: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Unit]

  def pause(consumer: Client, partitions: Vector[TopicPartition]): F[Unit]

  def resume(consumer: Client, partitions: Vector[TopicPartition]): F[Unit]

  /** The committed offset of each partition in request order, negative where none is committed. */
  def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[Long]]

  /** The low and high watermarks of one partition. */
  def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): F[(Long, Long)]

  /** The first offset at or after each timestamp in request order, negative where there is none. */
  def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): F[Vector[Long]]

  /** The position of each partition in request order, negative where there is none. */
  def positions(consumer: Client, partitions: Vector[TopicPartition]): F[Vector[Long]]

  def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): F[Unit]

  /** Each topic's name and partitions, for one topic or for all of them. */
  def metadata(client: Client, topic: Option[String], timeoutMillis: Int): F[Vector[(String, Vector[Int])]]

  def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): F[Unit]

  def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Unit]

  def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): F[Unit]

  def deleteRecords(admin: Client, cuts: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[DeletedRecords]

  def describeTopicConfigs(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[ConfigResource]]

  /** librdkafka's description of an error code. */
  def errorString(code: Int): String

/** A record ready for the producer, serialized. */
private[xkafka] final case class EncodedRecord(
    topic: Topic,
    partition: Option[Partition],
    timestamp: Option[Timestamp],
    headers: Headers,
    key: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]]
)

/** Where a record was delivered. A negative offset or timestamp is one the broker did not report. */
private[xkafka] final case class Delivery(partition: Int, offset: Long, timestamp: Long)

/** A record the consumer polled, copied out of librdkafka's memory. */
private[xkafka] final case class PolledRecord(
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Option[Long],
    key: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]],
    headers: Headers
)

/** A rebalance and the partitions it names. `kind` is 1 for an assignment, 0 for a revocation, and anything else for a failure to resynchronise. */
private[xkafka] final case class Rebalance[E](event: E, kind: Int, partitions: List[LeaseKey])

/** What deleting records answered. */
private[xkafka] enum DeletedRecords:
  /** One failure for the whole call, with librdkafka's code for it. */
  case Refused(failure: KafkaException.BackendFailure, code: Int)

  /** A code and low watermark for each requested partition, in request order; `Failures.NoAnswer` where the result left one out. */
  case Answered(results: Vector[(Int, Long)])

/** One described topic: its error code and text, and its entries when the code is 0. */
private[xkafka] final case class ConfigResource(name: String, error: Int, errorString: String, entries: Vector[ConfigEntry])

private[xkafka] final case class ConfigEntry(name: String, value: Option[String], source: Int, sensitive: Boolean, readOnly: Boolean)

/** How the shim's failures become exceptions, the same on every platform. */
private[xkafka] object Failures:
  /** What the shim leaves in place of a code for a partition its result does not mention. */
  val NoAnswer: Int = Int.MinValue

  /** A failure the shim reported with a librdkafka code. */
  def coded(message: String, code: Int): KafkaException.BackendFailure =
    val classified = ErrorCode.fromLibrdkafka(code)
    new KafkaException.BackendFailure(message, Some(classified), retriable = Some(classified.retriable))

  /** A transactional failure, whose flags are -1 when librdkafka did not say. */
  def classified(message: String, code: Int, fatal: Int, retriable: Int, transactionAbortRequired: Int): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(message, Some(ErrorCode.fromLibrdkafka(code)), flag(retriable), flag(fatal), flag(transactionAbortRequired))

  def plain(detail: String): KafkaException.BackendFailure = new KafkaException.BackendFailure(detail)

  private def flag(value: Int): Option[Boolean] = Option.when(value >= 0)(value != 0)
