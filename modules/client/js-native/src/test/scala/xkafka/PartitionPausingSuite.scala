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
import cats.effect.IO
import munit.CatsEffectSuite

/** What the kernel's partition router needs from a backend whose records all arrive from one source.
  *
  * Compiled by the Scala.js and Scala Native builds, which are the two that offer it.
  */
final class PartitionPausingSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val bootstrapServers = PlatformKafkaClient.integrationBootstrapServers

  private def pausing(name: String): munit.TestOptions = if bootstrapServers.isEmpty then munit.TestOptions(name).ignore else munit.TestOptions(name)

  test(pausing("a paused partition stops arriving and resumes where the reader got to")):
    bootstrapServers.fold(IO.unit): server =>
      val suffix  = s"${PlatformKafkaClient.name}-${System.nanoTime()}"
      val utf8In  = Serializer.utf8[IO]
      val utf8Out = Deserializer.utf8[IO]
      val early   = List("e0", "e1")
      val late    = List("l0", "l1", "l2", "l3")

      for
        topic     <- Topic.from(s"xkafka-pausing-$suffix").liftTo[IO]
        group     <- ConsumerGroup.from(s"xkafka-pausing-$suffix").liftTo[IO]
        partition <- Partition.from(0).liftTo[IO]
        client    <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        producer  <- ProducerSettings.from(client, utf8In, utf8In).liftTo[IO]
        consumer  <- ConsumerSettings.from(client, group, utf8Out, utf8Out, AutoOffsetReset.Earliest).liftTo[IO]
        send =
          (values: List[String]) =>
            PlatformKafkaClient().producer(producer).use(_.produceAndAwait(
              NonEmptyList.fromListUnsafe(values.map(value => ProducerRecord(topic, "k", value, partition = Some(partition))))
            ))
        _      <- send(early)
        result <-
          PlatformKafkaClient().consumer(consumer, Subscription.Topics(NonEmptyList.one(topic))).use: value =>
            // A backend that kept the no-op default would fail the assertions below.
            val capability = value.pausing
            for
              first <- value.records.take(early.size.toLong).compile.toList.timeout(60.seconds)
              _     <- capability.pause(Set(TopicPartition(topic, partition)))
              // Produced while paused, so nothing may be fetched for it until the partition is resumed.
              _      <- send(late)
              silent <- value.records.take(1).compile.toList.timeout(8.seconds).attempt
              _      <- capability.resume(Set(TopicPartition(topic, partition)))
              rest   <- value.records.take(late.size.toLong).compile.toList.timeout(60.seconds)
            yield (first.map(_.record.value), silent.isLeft, rest.map(_.record.value))
        (first, silentWhilePaused, rest) = result
      yield
        assertEquals(first, early)
        assert(silentWhilePaused, "records arrived for a partition that was paused")
        // Resuming continues from the reader's position, so nothing is lost and nothing repeats.
        assertEquals(rest, late)
