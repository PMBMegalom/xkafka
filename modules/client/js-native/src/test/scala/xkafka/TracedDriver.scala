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

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import internal.driver.{EncodedRecord, Enqueued, KafkaDriver}

/** A driver that does what `underlying` does, and keeps what a producer's deliveries went through, so a test whose wait for them stops can say where.
  */
final class TracedDriver private (val underlying: KafkaDriver[IO], trace: Ref[IO, TracedDriver.Trace]) extends KafkaDriver[IO]:
  export underlying.{enqueue as _, pending as _, serveDeliveries as _, *}

  override def enqueue(producer: Client, batch: Batch, records: NonEmptyList[EncodedRecord]): IO[Enqueued] =
    underlying.enqueue(producer, batch, records).attempt.flatTap: outcome =>
      trace.update(current => current.copy(enqueues = current.enqueues :+ s"${records.size} offered: ${outcome.fold(_.toString, _.toString)}"))
    .rethrow

  override def serveDeliveries(producer: Client, timeoutMillis: Int): IO[Unit] =
    trace.update(_.copy(serving = true)) *> underlying.serveDeliveries(producer, timeoutMillis)
      .guarantee(trace.update(current => current.copy(serving = false, served = current.served + 1)))

  override def pending(batch: Batch): IO[Int] = underlying.pending(batch).flatTap(count => trace.update(_.copy(pending = Some(count))))

  /** What the deliveries went through so far. */
  def report: IO[String] = trace.get.map(_.toString)

object TracedDriver:
  /** The enqueues and what each answered, how often the shared delivery queue was served and whether it is being served now, and the outstanding
    * count a batch last had.
    */
  final case class Trace(enqueues: Vector[String], served: Long, serving: Boolean, pending: Option[Int])

  def apply(underlying: KafkaDriver[IO]): IO[TracedDriver] =
    Ref.of[IO, Trace](Trace(Vector.empty, 0L, false, None)).map(new TracedDriver(underlying, _))
