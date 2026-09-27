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

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import munit.CatsEffectSuite

final class ProducerPipeSuite extends CatsEffectSuite:
  private val topic     = Topic.from("events").toOption.get
  private val partition = Partition.from(0).toOption.get

  private def batch(value: String): NonEmptyList[ProducerRecord[String, String]] = NonEmptyList.one(ProducerRecord(topic, "key", value))

  test("the pipe enqueues later batches while earlier ones are still waiting for the broker"):
    val values = List("a", "b", "c")

    for
      enqueued    <- Ref[IO].of(0)
      allEnqueued <- Deferred[IO, Unit]
      acknowledge <- Deferred[IO, Unit]
      producer =
        new KafkaProducer[IO, String, String]:
          override def produce(records: NonEmptyList[ProducerRecord[String, String]]): IO[IO[ProducerResult[String, String]]] =
            enqueued.updateAndGet(_ + 1).flatMap(count => allEnqueued.complete(()).attempt.void.whenA(count == values.size))
              .as(acknowledge.get.as(ProducerResult(records.map(_ -> None))))

          override def partitionsFor(value: Topic): IO[Set[Partition]] = IO.pure(Set(partition))
      running <- Stream.emits(values.map(batch)).through(producer.pipe()).compile.toList.start
      // Nothing has been acknowledged yet, because that waits on a signal this test has not given, so reaching
      // here is what shows the enqueueing did not wait for it.
      _        <- allEnqueued.get.timeout(10.seconds)
      observed <- enqueued.get
      _        <- acknowledge.complete(())
      results  <- running.joinWithNever.timeout(10.seconds)
    yield
      assertEquals(observed, values.size, "every batch should be enqueued before any is acknowledged")
      assertEquals(results.flatMap(_.records.toList.map(_._1.value)), values, "results should arrive in the order the batches did")

  test("the pipe bounds how many batches wait for the broker at once"):
    val values = List("a", "b", "c", "d")

    for
      waiting     <- Ref[IO].of(0)
      highest     <- Ref[IO].of(0)
      acknowledge <- Deferred[IO, Unit]
      producer =
        new KafkaProducer[IO, String, String]:
          override def produce(records: NonEmptyList[ProducerRecord[String, String]]): IO[IO[ProducerResult[String, String]]] =
            IO.pure(
              waiting.updateAndGet(_ + 1).flatMap(count => highest.update(_.max(count))) >> acknowledge.get >> waiting.update(_ - 1)
                .as(ProducerResult(records.map(_ -> None)))
            )

          override def partitionsFor(value: Topic): IO[Set[Partition]] = IO.pure(Set(partition))
      running <- Stream.emits(values.map(batch)).through(producer.pipe(maxInFlight = 2)).compile.toList.start
      // Two are held by the bound, so the count cannot climb past it however many batches are offered.
      _        <- waiting.get.iterateUntil(_ == 2).timeout(10.seconds)
      _        <- acknowledge.complete(())
      results  <- running.joinWithNever.timeout(10.seconds)
      observed <- highest.get
    yield
      assertEquals(observed, 2, "the bound should hold, whatever the stream offers")
      assertEquals(results.size, values.size)

  test("the pipe rejects a non-positive bound"):
    val producer =
      new KafkaProducer[IO, String, String]:
        override def produce(records: NonEmptyList[ProducerRecord[String, String]]): IO[IO[ProducerResult[String, String]]] =
          IO.pure(IO.pure(ProducerResult(records.map(_ -> None))))

        override def partitionsFor(value: Topic): IO[Set[Partition]] = IO.pure(Set(partition))

    Stream.emits(List(batch("a"))).through(producer.pipe(0)).compile.drain.attempt.map:
      case Left(error: IllegalArgumentException) => assertEquals(error.getMessage, "maxInFlight must be positive")
      case Left(error)                           => fail(s"unexpected error: $error")
      case Right(())                             => fail("expected a non-positive bound to fail")
