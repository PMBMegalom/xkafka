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

import cats.MonadThrow
import cats.effect.Temporal
import cats.syntax.all.*
import driver.{KafkaDriver, PartitionEnd, PolledRecord, StartAt}

/** Refills stores from their changelogs. */
private[xkafka] object Restore:
  /** How long each poll waits for records. */
  val PollMillis = 500

  /** How long to wait to learn where a partition ends, as the engine waits for it elsewhere. */
  val EndsTimeoutMillis = 30000

  /** How long to wait before asking again where a partition ends. */
  private val EndsBackoff = 100.millis

  /** Reads each changelog partition `changelogs` names, with the store it backs, from where `from` says, or its start, to its end, through
    * `consumer`, which reads under read_committed and reports the ends it reaches, into `stores`, each poll's records written as one change. Each
    * store holds the latest value of each key, and a record with no value removes its key. Records without a key, which no store writes, are skipped.
    */
  def apply[F[_]](
      driver: KafkaDriver[F],
      consumer: driver.Client,
      changelogs: Map[TopicPartition, String],
      from: Map[TopicPartition, Long],
      stores: Map[String, TaskStore[F]]
  )(using F: Temporal[F]): F[Unit] =
    val backing  = changelogs.map((partition, store) => (partition.topic.value, partition.partition.value) -> store)
    val starting = changelogs.keys.toVector
    def read(left: Set[(String, Int)], ends: Ends): F[Unit] =
      if left.isEmpty then F.unit
      else
        driver.poll(consumer, PollMillis).flatMap: polled =>
          val remaining = unfinished(left, polled.ends, ends)
          // Each store records how far it now holds its changelog: past the last record read, or at the end, for a partition finished.
          val past      = polled.records.groupMapReduce(record => (record.topic, record.partition))(_.offset + 1)(math.max)
          val finished  = (left -- remaining).map(partition => partition -> ends.getOrElse(partition, 0L)).toMap
          val positions = (past.toList ++ finished.toList).flatMap((partition, at) => backing.get(partition).map(_ -> at))
            .groupMapReduce(_._1)(_._2)(math.max)
          written(stores, changes(polled.records.flatMap(record => backing.get((record.topic, record.partition)).map(_ -> record))), positions) *>
            read(remaining, ends)
    driver.assign(consumer, starting, starting.map(partition => StartAt(partition, from.getOrElse(partition, 0L), -1))) *>
      endsOf(driver, consumer, starting).flatMap(read(backing.keySet, _))

  /** The changes `records` make to the stores they are paired with, by the store's name, in the order read. */
  def changes(records: Iterable[(String, PolledRecord)]): Map[String, StoreChanges] =
    records.foldLeft(Map.empty[String, StoreChanges])((made, paired) => changed(made, paired._1, paired._2))

  /** The changes `made` so far with one record of `store`'s changelog: its key's latest value, or its removal; a record without a key changes
    * nothing.
    */
  def changed(made: Map[String, StoreChanges], store: String, record: PolledRecord): Map[String, StoreChanges] =
    record.key.fold(made): key =>
      made.updated(store, made.getOrElse(store, StoreContents.unchanged).updated(key, record.value.map(Stored(_, record.timestamp.getOrElse(-1L)))))

  /** Writes each store's changes into it, with where it holds its changelog up to, where `positions` says. */
  def written[F[_]](stores: Map[String, TaskStore[F]], changes: Map[String, StoreChanges], positions: Map[String, Long] = Map.empty)(using
      F: MonadThrow[F]
  ): F[Unit] = (changes.keySet ++ positions.keySet).toList.traverse_ : store =>
    F.fromOption(stores.get(store), new IllegalStateException(s"no store '$store' to restore"))
      .flatMap(_.write(changes.getOrElse(store, StoreContents.unchanged), positions.get(store)))

  /** Where each partition being caught up on ends, by topic name and number. */
  type Ends = Map[(String, Int), Long]

  /** Where each of `partitions` ends now, as Java's restore asks before it starts: for a consumer under read_committed, the last stable offset. */
  def endsOf[F[_]](driver: KafkaDriver[F], consumer: driver.Client, partitions: Vector[TopicPartition])(using F: Temporal[F]): F[Ends] =
    partitions.traverse(partition =>
      whileMoving(driver.watermarks(consumer, partition, EndsTimeoutMillis))
        .map(marks => (partition.topic.value, partition.partition.value) -> marks._2)
    ).map(_.toMap)

  /** The answer to `ask`, asked again while it fails because the broker is moving, as a partition without a leader yet does, or because the client
    * does not know the partition yet, as librdkafka's does not for a moment after the topic is made, both of which Java's client also waits out,
    * until `EndsTimeoutMillis` have passed. A partition asked about here belongs to a topic already found to exist.
    */
  def whileMoving[F[_], A](ask: F[A])(using F: Temporal[F]): F[A] =
    def attempt(start: FiniteDuration): F[A] =
      ask.recoverWith:
        case failure: KafkaException.BackendFailure if failure.retriable.contains(true) || failure.code.contains(ErrorCode.UnknownTopicOrPartition) =>
          F.monotonic.flatMap: now =>
            if now - start < EndsTimeoutMillis.millis then F.sleep(EndsBackoff) *> attempt(start) else F.raiseError(failure)
    F.monotonic.flatMap(attempt)

  /** The partitions of `left` still to catch up on after a poll reporting `reached`: a report of an end before the one asked for finishes nothing,
    * since the records up to that one are still to come, and a later report follows them.
    */
  def unfinished(left: Set[(String, Int)], reached: List[PartitionEnd], ends: Ends): Set[(String, Int)] =
    left -- reached.collect { case end if end.offset >= ends.getOrElse((end.topic, end.partition), 0L) => (end.topic, end.partition) }
