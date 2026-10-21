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
package driver

import cats.data.NonEmptyList
import fs2.Chunk

/** The Kafka client each platform drives, in Scala types: librdkafka through the C shim on Scala.js and Scala Native, and the Java client on the JVM.
  *
  * Every call that can wait on the network returns once it has an answer, so a platform runs it where waiting blocks nothing else. A call that fails
  * raises a `KafkaException.BackendFailure` classified the same way on every platform. Handles are the platform's own and are valid until the
  * matching destroy.
  */
private[xkafka] trait KafkaDriver[F[_]]:
  /** A client of the driver: a producer, a consumer or an administration client. */
  type Client

  /** The delivery slots of one produced batch, owned until freed. */
  type Batch

  /** A rebalance the consumer received and has not applied yet. */
  type Event

  /** A consumer's group identity, owned until destroyed, which a transaction records offsets against. */
  type GroupMetadata <: GroupHandle

  /** Whether every batch's delivery reports arrive through one queue that has to be served, as librdkafka's do. The Java client reports each record
    * on its own thread as the broker acknowledges it.
    */
  def deliveriesShareQueue: Boolean

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

  /** Enqueues every record in order, failing at the first one the producer refuses; the records before it stay enqueued. */
  def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): F[Unit]

  /** Serves the shared queue's delivery reports for up to the timeout. Called only where deliveries share a queue, by one fiber at a time, while
    * enqueues go on alongside it.
    */
  def serveDeliveries(producer: Client, timeoutMillis: Int): F[Unit]

  /** Waits up to the timeout for the batch's own delivery reports and answers how many are still outstanding. Called only where deliveries do not
    * share a queue.
    */
  def awaitDeliveries(batch: Batch, timeoutMillis: Int): F[Int]

  def pending(batch: Batch): F[Int]

  /** What each record of a fully reported batch was delivered as, in enqueue order, failing with the first failed delivery. */
  def deliveries(producer: Client, batch: Batch): F[Vector[Delivery]]

  def initTransactions(producer: Client, timeoutMillis: Int): F[Unit]

  def beginTransaction(producer: Client): F[Unit]

  def commitTransaction(producer: Client, timeoutMillis: Int): F[Unit]

  def abortTransaction(producer: Client, timeoutMillis: Int): F[Unit]

  def sendOffsets(producer: Client, metadata: GroupMetadata, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): F[Unit]

  def groupMetadata(consumer: Client): F[GroupMetadata]

  def destroyGroupMetadata(metadata: GroupMetadata): F[Unit]

  /** The handle as group metadata of this platform, if it is one. Handles from every client on the platform qualify. */
  def asGroupMetadata(handle: GroupHandle): Option[GroupMetadata]

  /** Assigns `partitions`, each of `starts` starting at its offset and the rest where they would. */
  def assign(consumer: Client, partitions: Vector[TopicPartition], starts: Vector[StartAt]): F[Unit]

  /** Subscribes to topic names, or to one pattern, which starts with `^` as no topic name can. */
  def subscribe(consumer: Client, topics: NonEmptyList[String]): F[Unit]

  /** The records one poll returned, none when the timeout passed first or the poll only reached the end of a partition. */
  def poll(consumer: Client, timeoutMillis: Int): F[List[PolledRecord]]

  /** Every rebalance the consumer received since the last call, oldest first. */
  def takeRebalances(consumer: Client): F[List[Rebalance[Event]]]

  /** Applies a rebalance, and for an assignment starts each of `starts` at its offset and the rest where they would. */
  def applyRebalance(consumer: Client, event: Event, starts: Vector[StartAt]): F[Unit]

  def cooperative(consumer: Client): F[Boolean]

  def assignment(consumer: Client): F[Vector[(String, Int)]]

  def commit(consumer: Client, offsets: Vector[(TopicPartition, OffsetAndMetadata)], timeoutMillis: Int): F[Unit]

  def pause(consumer: Client, partitions: Vector[TopicPartition]): F[Unit]

  def resume(consumer: Client, partitions: Vector[TopicPartition]): F[Unit]

  /** What is committed for each partition, in request order. */
  def committed(consumer: Client, partitions: Vector[TopicPartition], timeoutMillis: Int): F[Vector[StoredOffset]]

  /** The low and high watermarks of one partition. */
  def watermarks(consumer: Client, partition: TopicPartition, timeoutMillis: Int): F[(Long, Long)]

  /** The first offset at or after each timestamp in request order, negative where there is none. */
  def offsetsForTimes(consumer: Client, searches: Vector[(TopicPartition, Timestamp)], timeoutMillis: Int): F[Vector[Long]]

  /** The position of each partition in request order, negative where there is none. */
  def positions(consumer: Client, partitions: Vector[TopicPartition]): F[Vector[Long]]

  /** Moves to an offset, or to either end of the log for [[KafkaDriver.BeginningOffset]] and [[KafkaDriver.EndOffset]]. */
  def seek(consumer: Client, partition: TopicPartition, offset: Long, timeoutMillis: Int): F[Unit]

  /** Each topic's name and partitions, for one topic or for all of them. */
  def metadata(client: Client, topic: Option[String], timeoutMillis: Int): F[Vector[(String, Vector[Int])]]

  def createTopics(admin: Client, topics: Vector[NewTopic], timeoutMillis: Int): F[Unit]

  def deleteTopics(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Unit]

  def createPartitions(admin: Client, topic: Topic, count: Int, timeoutMillis: Int): F[Unit]

  /** Each partition's new low watermark, or why it was not cut, in request order. */
  def deleteRecords(admin: Client, cuts: Vector[(TopicPartition, Offset)], timeoutMillis: Int): F[Vector[Either[KafkaException, Offset]]]

  /** Each topic's configuration, or why it was not described, in request order. */
  def describeTopicConfigs(admin: Client, topics: Vector[Topic], timeoutMillis: Int): F[Vector[Either[KafkaException, TopicConfiguration]]]

private[xkafka] object KafkaDriver:
  /** The offset `seek` reads as the beginning of a partition's log. */
  val BeginningOffset: Long = -2L

  /** The offset `seek` reads as the end of a partition's log. */
  val EndOffset: Long = -1L

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

/** A committed offset as the broker stores it. A negative offset or leader epoch is one that is not there. */
private[xkafka] final case class StoredOffset(offset: Long, leaderEpoch: Int, metadata: String)

/** A record the consumer polled, copied out of the client's own memory.
  *
  * `timestampType` is 1 for a creation time and 2 for a log append time, as librdkafka numbers them, and anything else where the record has no
  * timestamp. A negative `leaderEpoch` is one the backend did not report.
  */
private[xkafka] final case class PolledRecord(
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Option[Long],
    timestampType: Int,
    leaderEpoch: Int,
    key: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]],
    headers: Headers
)

/** Where an assigned partition starts. `leaderEpoch` is negative where none is known. */
private[xkafka] final case class StartAt(partition: TopicPartition, offset: Long, leaderEpoch: Int)

/** A rebalance and the partitions it names. `kind` is 1 for an assignment, 0 for a revocation, 2 for a revocation of partitions the group has already
  * given away, and anything else for a failure to resynchronise.
  */
private[xkafka] final case class Rebalance[E](event: E, kind: Int, partitions: List[LeaseKey])
