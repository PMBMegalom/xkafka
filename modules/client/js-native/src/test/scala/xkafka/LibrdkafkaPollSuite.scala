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
import cats.effect.{IO, Resource}
import munit.CatsEffectSuite
import internal.driver.StartAt
import internal.librdkafka.MockCluster

/** What one poll of a librdkafka consumer returns, against librdkafka's in-process mock cluster. */
final class LibrdkafkaPollSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 1.minute

  private val topic = Topic.from("events").toOption.get

  test("a poll takes the records librdkafka already has, in order, and the partition's end after the last of them"):
    val driver = KafkaClientPlatform.driver[IO]
    LibrdkafkaPlatform.mockCluster[IO](Map(MockCluster.Brokers)).use: cluster =>
      for
        _        <- cluster.createTopic(topic.value, 1)
        client   <- ClientSettings.from(NonEmptyList.one(cluster.bootstrapServers)).liftTo[IO]
        settings <- ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).liftTo[IO]
        _        <-
          KafkaClient[IO].producer(settings)
            .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(List.tabulate(50)(n => ProducerRecord(topic, s"key-$n", "v")))))
        partition = TopicPartition(topic, Partition.from(0).toOption.get)
        polls <-
          Resource.make(driver.consumer(cluster.bootstrapServers, None, "polls", "earliest", Map.empty, reportEnds = true))(driver.destroyConsumer)
            .use: consumer =>
              driver.assign(consumer, Vector(partition), Vector(StartAt(partition, 0L, -1))) *>
                // Polls until the partition's end, keeping those that returned anything.
                fs2.Stream.repeatEval(driver.poll(consumer, 500)).filter(polled => polled.records.nonEmpty || polled.ends.nonEmpty)
                  .takeThrough(_.ends.isEmpty).compile.toList
      yield
        val records = polls.flatMap(_.records)
        assert(clue(polls.map(_.records.size)).head > 1)
        assertEquals(records.map(_.offset), List.range(0L, 50L))
        assertEquals(polls.flatMap(_.ends).map(_.offset), List(50L))
