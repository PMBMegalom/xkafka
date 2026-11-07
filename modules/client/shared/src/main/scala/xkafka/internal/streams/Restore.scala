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

import cats.effect.Temporal
import cats.syntax.all.*
import driver.{KafkaDriver, PartitionEnd, PolledRecord, StartAt}
import fs2.Chunk

/** Refills stores from their changelogs. */
private[xkafka] object Restore:
  /** How long each poll waits for records. */
  val PollMillis = 500

  /** How long to wait to learn where a partition ends, as the engine waits for it elsewhere. */
  val EndsTimeoutMillis = 30000

  /** How long to wait before asking again where a partition ends. */
  private val EndsBackoff = 100.millis

  /** Reads each changelog partition `changelogs` names, with the store it backs, from where `from` says, or its start, to its end, through
    * `consumer`, which reads under read_committed and reports the ends it reaches, onto what `held` already holds. Each store holds the latest value
    * of each key, and a record with no value removes its key. Records without a key, which no store writes, are skipped.
    */
  def apply[F[_]](
      driver: KafkaDriver[F],
      consumer: driver.Client,
      changelogs: Map[TopicPartition, String],
      from: Map[TopicPartition, Long] = Map.empty,
      held: Map[String, StoreContents] = Map.empty,
      times: Map[String, Chunk[Byte] => Long] = Map.empty
  )(using F: Temporal[F]): F[Map[String, StoreContents]] =
    val stores   = changelogs.map((partition, store) => (partition.topic.value, partition.partition.value) -> store)
    val starting = changelogs.keys.toVector
    def read(left: Set[(String, Int)], ends: Ends, restored: Map[String, StoreContents]): F[Map[String, StoreContents]] =
      if left.isEmpty then F.pure(restored)
      else
        driver.poll(consumer, PollMillis).flatMap: polled =>
          val after =
            polled.records.foldLeft(restored)((now, record) => stores.get((record.topic, record.partition)).fold(now)(applied(now, _, record, times)))
          read(unfinished(left, polled.ends, ends), ends, after)
    driver.assign(consumer, starting, starting.map(partition => StartAt(partition, from.getOrElse(partition, 0L), -1))) *>
      endsOf(driver, consumer, starting).flatMap: ends =>
        read(stores.keySet, ends, stores.values.map(store => store -> held.getOrElse(store, StoreContents.empty)).toMap)

  /** Where each partition being caught up on ends, by topic name and number. */
  type Ends = Map[(String, Int), Long]

  /** Where each of `partitions` ends now, as Java's restore asks before it starts: for a consumer under read_committed, the last stable offset. */
  def endsOf[F[_]](driver: KafkaDriver[F], consumer: driver.Client, partitions: Vector[TopicPartition])(using F: Temporal[F]): F[Ends] =
    partitions.traverse(partition =>
      whileMoving(driver.watermarks(consumer, partition, EndsTimeoutMillis))
        .map(marks => (partition.topic.value, partition.partition.value) -> marks._2)
    ).map(_.toMap)

  /** The answer to `ask`, asked again while it fails because the broker is moving, as a partition without a leader yet does, which Java's client also
    * waits out, until `EndsTimeoutMillis` have passed.
    */
  def whileMoving[F[_], A](ask: F[A])(using F: Temporal[F]): F[A] =
    def attempt(start: FiniteDuration): F[A] =
      ask.recoverWith:
        case failure: KafkaException.BackendFailure if failure.retriable.contains(true) =>
          F.monotonic.flatMap: now =>
            if now - start < EndsTimeoutMillis.millis then F.sleep(EndsBackoff) *> attempt(start) else F.raiseError(failure)
    F.monotonic.flatMap(attempt)

  /** The partitions of `left` still to catch up on after a poll reporting `reached`: a report of an end before the one asked for finishes nothing,
    * since the records up to that one are still to come, and a later report follows them.
    */
  def unfinished(left: Set[(String, Int)], reached: List[PartitionEnd], ends: Ends): Set[(String, Int)] =
    left -- reached.collect { case end if end.offset >= ends.getOrElse((end.topic, end.partition), 0L) => (end.topic, end.partition) }

  /** The stores with one changelog record applied to `store`. A removal from a store `times` can read a time from counts towards its clock. */
  def applied(
      restored: Map[String, StoreContents],
      store: String,
      record: PolledRecord,
      times: Map[String, Chunk[Byte] => Long] = Map.empty
  ): Map[String, StoreContents] =
    record.key.fold(restored): key =>
      val contents = restored.getOrElse(store, StoreContents.empty)
      def removed  = times.get(store).fold(contents)(time => contents.removedAt(time(key))) - key
      restored.updated(store, record.value.fold(removed)(value => contents.updated(key, Stored(value, record.timestamp.getOrElse(-1L)))))
