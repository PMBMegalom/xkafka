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
import cats.syntax.all.*
import munit.{CatsEffectSuite, TestOptions}

/** Behaviour that needs replicas on more than one broker, against the fixture's cluster of three. */
final class KafkaClusterSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val bootstrapServers = PlatformKafkaClient.environment("XKAFKA_CLUSTER_BOOTSTRAP_SERVERS")

  /** The fixture makes one for each backend, led by broker 1 while broker 2 is its preferred replica. */
  private val electionTopic = PlatformKafkaClient.environment("XKAFKA_CLUSTER_ELECTION_TOPIC").getOrElse("xkafka-election")

  private def cluster(name: String): TestOptions = if bootstrapServers.isEmpty then TestOptions(name).ignore else TestOptions(name)

  test(cluster("a partition's replicas start with its preferred one, which an election makes its leader, and electing again answers as done")):
    bootstrapServers.fold(IO.unit): servers =>
      val partition = TopicPartition(Topic.from(s"$electionTopic-${PlatformKafkaClient.name}").toOption.get, Partition.from(0).toOption.get)
      val broker    = (id: Int) => BrokerId.from(id).toOption.get

      for
        client   <- ClientSettings.from(NonEmptyList.fromListUnsafe(servers.split(',').toList)).liftTo[IO]
        observed <-
          PlatformKafkaClient().admin(client).use: admin =>
            def placement =
              admin.describeTopics(NonEmptySet.one(partition.topic))
                .map(_(partition.topic).flatMap(_.toOption).flatMap(_.partitions(partition.partition)))
            for
              before  <- placement
              elected <- admin.electPreferredLeaders(NonEmptySet.one(partition))
              after   <- placement.iterateUntil(_.flatMap(_.leader).contains(broker(2))).timeout(60.seconds)
              again   <- admin.electPreferredLeaders(NonEmptySet.one(partition))
            yield (before, elected, after, again)
          .timeout(120.seconds)
      yield
        val (before, elected, after, again) = observed
        assertEquals(before.map(_.replicas), Some(NonEmptyList.of(broker(2), broker(1), broker(3))))
        assertEquals(before.flatMap(_.leader), Some(broker(1)))
        assertEquals(elected(partition).map(_.leftMap(_.getMessage)), Some(Right(())))
        assertEquals(after.map(_.replicas), Some(NonEmptyList.of(broker(2), broker(1), broker(3))))
        assertEquals(again(partition).map(_.leftMap(_.getMessage)), Some(Right(())))
