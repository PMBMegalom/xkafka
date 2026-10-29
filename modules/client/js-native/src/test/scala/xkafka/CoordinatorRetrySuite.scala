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

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import munit.CatsEffectSuite
import internal.librdkafka.MockCluster

/** A consumer naming its partitions while its group's coordinator does not yet answer for it, against librdkafka's in-process mock cluster. */
final class CoordinatorRetrySuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 1.minute

  private def valid[A](result: Either[ValidationError, A]): A = result.fold(error => fail(error.message), identity)

  private val text = Deserializer.utf8[IO]

  test("a consumer naming its partitions reads them once the group's coordinator answers for it, after several answers that it is not"):
    LibrdkafkaPlatform.mockCluster[IO](Map(MockCluster.Brokers)).use: cluster =>
      val topic      = valid(Topic.from("events"))
      val partitions = List(0, 1).map(n => TopicPartition(topic, valid(Partition.from(n))))
      for
        _         <- cluster.createTopic(topic.value, partitions.size)
        client    <- ClientSettings.from(NonEmptyList.one(cluster.bootstrapServers)).liftTo[IO]
        producing <- ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).liftTo[IO]
        _         <-
          KafkaClient[IO].producer(producing).use: producer =>
            producer.produceAndAwait(
              NonEmptyList.fromListUnsafe(partitions.map(p => ProducerRecord(topic, s"k${p.partition.value}", "v", Some(p.partition))))
            )
        // Each read of the group's committed offsets is answered that this broker is not its coordinator, three times over.
        _         <- cluster.failRequests(MockCluster.OffsetFetch, List.fill(3)(MockCluster.NotCoordinator))
        group     <- ConsumerGroup.from("coordinator-retry").liftTo[IO]
        consuming <- ConsumerSettings.from(client, group, text, text, AutoOffsetReset.Earliest).liftTo[IO]
        read      <-
          KafkaClient[IO].consumer(consuming, Selection.Partitions(NonEmptySet.of(partitions.head, partitions.tail*))).use:
            _.records.take(partitions.size.toLong).map(_.record.key).compile.toList
          .timeout(30.seconds)
      yield assertEquals(read.toSet, Set("k0", "k1"))

  test("the mock cluster's hooks refuse a client librdkafka made no mock cluster for"):
    LibrdkafkaPlatform.mockCluster[IO](Map.empty).use_.attempt.map:
      case Left(failure: KafkaException.BackendFailure) =>
        assert(failure.detail.contains("not one librdkafka made a mock cluster for"), failure.detail)
      case other => fail(s"expected the hooks to refuse an ordinary client, got $other")
