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
package internal.streams

import java.util.{List as JavaList, Properties}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import munit.CatsEffectSuite
import org.apache.kafka.clients.admin.{
  Admin, AdminClientConfig, DescribeStreamsGroupsOptions, StreamsGroupTopologyDescription as Stored, StreamsGroupTopologyDescriptionStatus
}
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription as Pushed
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Named as JavaNamed, Produced}
import xkafka.streams.{ApplicationId, JavaStreams, KStream, Named, Repartition, Serde, Store, StreamsSettings, Topology}

/** A streams application's description as the broker keeps it, read back through Java's admin client, on each broker release from 4.4, where
  * `scripts/with-kafka.sh` starts brokers with a topology description plugin.
  */
final class DescriptionBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val brokers =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map: entry =>
      val Array(release, server) = entry.split('=')
      (release, server)

  /** A node as a description gives it, whoever made the description: its kind, name, topics, stores and successors. */
  private final case class Node(kind: Int, name: String, topics: List[String], sink: Option[String], stores: List[String], successors: List[String])

  private def stored(description: Stored): Map[String, Set[Node]] =
    description.subtopologies.asScala.toList.map: subtopology =>
      subtopology.id -> subtopology.nodes.asScala.toSet.map:
        case source: Stored.Source => Node(1, source.name, source.topics.asScala.toList.sorted, None, Nil, source.successors.asScala.toList.sorted)
        case processor: Stored.Processor =>
          Node(2, processor.name, Nil, None, processor.stores.asScala.toList.sorted, processor.successors.asScala.toList.sorted)
        case sink: Stored.Sink => Node(3, sink.name, Nil, Option(sink.topic.orElse(null)), Nil, Nil)
    .toMap

  private def pushed(description: Pushed): Map[String, Set[Node]] =
    description.subtopologies.asScala.toList.map: subtopology =>
      subtopology.subtopologyId -> subtopology.nodes.asScala.toSet.map: node =>
        Node(
          node.nodeType.toInt,
          node.name,
          node.sourceTopics.asScala.toList,
          Option(node.sinkTopic),
          node.stores.asScala.toList,
          node.successors.asScala.toList
        )
    .toMap

  private def javaAdmin(server: String): Resource[IO, Admin] =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    Resource.fromAutoCloseable(IO(Admin.create(properties)))

  /** The group's members and the description the broker keeps for it, or none while the broker does not know the group yet. */
  private def kept(admin: Admin, group: String): IO[Option[(Int, StreamsGroupTopologyDescriptionStatus, Option[Stored])]] =
    val options = DescribeStreamsGroupsOptions().includeTopologyDescription(true)
    IO.fromCompletableFuture(IO(admin.describeStreamsGroups(JavaList.of(group), options).all.toCompletionStage.toCompletableFuture)).map(_.get(group))
      .map(described => Option((described.members.size, described.topologyDescriptionStatus, Option(described.topologyDescription.orElse(null)))))
      .recover { case _: GroupIdNotFoundException => None }

  /** What `kept` gives once `done` holds of it, or a minute on. */
  private def until(admin: Admin, group: String)(
      done: Option[(Int, StreamsGroupTopologyDescriptionStatus, Option[Stored])] => Boolean
  ): IO[Option[(Int, StreamsGroupTopologyDescriptionStatus, Option[Stored])]] =
    def loop(left: Int): IO[Option[(Int, StreamsGroupTopologyDescriptionStatus, Option[Stored])]] =
      kept(admin, group).flatMap(found => if left <= 0 || done(found) then IO.pure(found) else IO.sleep(500.millis) *> loop(left - 1))
    loop(120)

  private val text = Serde.utf8[IO]

  /** A program with a repartition and a store, and the same program in Java. */
  private def program(input: Topic, output: Topic): Topology[IO] =
    Topology.of(
      KStream(Named("read"), input, text, text).selectKey(Named("by-value"))((_, v) => v).groupByKey(Repartition("rekeyed"), text, text)
        .count(Named("count"), Store("counts")).toStream(Named("to-stream")).to(Named("write"), output, text, Serde.long[IO])
    ).orFail

  private def javaProgram(application: String, input: String, output: String): Pushed =
    val s = Serdes.String
    JavaStreams.pushed(application): builder =>
      builder.stream[String, String](input, Consumed.`with`(s, s).withName("read")).selectKey((_, v) => v, JavaNamed.as("by-value"))
        .groupByKey(Grouped.`with`("rekeyed", s, s)).count(JavaNamed.as("count"), Materialized.as("counts")).toStream(JavaNamed.as("to-stream"))
        .to(output, Produced.`with`(s, Serdes.Long).withName("write"))

  /** Runs the program as an application of its own, with `push` choosing whether it pushes its description, and `test` once it is running. */
  private def running(server: String, push: Boolean)(test: (String, Topic, Topic) => IO[Unit]): IO[Unit] =
    Names.fresh:
      val application = summon[Names](s"xkafka-described-${PlatformKafkaClient.name}")
      val input       = Topic.from(s"$application-in").orFail
      val output      = Topic.from(s"$application-out").orFail
      val client      = ClientSettings.from(NonEmptyList.of(server)).orFail
      val settings    = StreamsSettings.from(client, ApplicationId.from(application).orFail, topologyDescriptionPush = push).orFail
      val topics      = NonEmptySet.of(NewTopic.from(input, 2, 1).orFail, NewTopic.from(output, 2, 1).orFail)
      PlatformKafkaClient().admin(client).use(_.createTopics(topics)) *> PlatformKafkaClient().streams(settings, program(input, output))
        .use(_ => test(application, input, output))

  brokers.filterNot((release, _) => List("4.2.", "4.3.").exists(release.startsWith)).foreach: (release, server) =>
    test(s"$release: the broker keeps the description an application pushes, the same Java Streams pushes for the same program"):
      running(server, push = true): (application, input, output) =>
        javaAdmin(server).use: admin =>
          until(admin, application)(_.exists(_._2 == StreamsGroupTopologyDescriptionStatus.AVAILABLE)).map: found =>
            assertEquals(found.map(_._2), Some(StreamsGroupTopologyDescriptionStatus.AVAILABLE))
            assertEquals(found.flatMap(_._3).map(stored), Some(pushed(javaProgram(application, input.value, output.value))))

    test(s"$release: an application that pushes no description leaves the broker without one"):
      running(server, push = false): (application, _, _) =>
        javaAdmin(server).use: admin =>
          // The other test finds the broker keeping a member's description within seconds of the member joining.
          until(admin, application)(_.exists(_._1 > 0)) *> IO.sleep(10.seconds) *> kept(admin, application).map: found =>
            assertEquals(found, Some((1, StreamsGroupTopologyDescriptionStatus.NOT_STORED, None)))
