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
package streams

import java.util.{Optional, Properties, UUID}
import java.util.function.Supplier

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import munit.FunSuite
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, Topology as JavaTopology}
import org.apache.kafka.streams.kstream.{Grouped, Materialized, Produced}
import org.apache.kafka.streams.processor.internals.{InternalTopologyBuilder, StreamThread, TopologyMetadata}
import internal.protocol.StreamsGroup
import internal.streams.Declaration

/** The topology the engine declares for a program, against what Java Streams declares for the same program, field by field. */
final class TopologyGoldenSuite extends FunSuite:
  import TopologyGoldenSuite.*

  private def topic(name: String): Topic = Topic.from(name).orFail

  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]

  private def ours(first: Pipeline[IO], rest: Pipeline[IO]*): StreamsGroup.Topology = Declaration("app", Topology.of(first, rest*).orFail)

  test("a stateless pipeline is one sub-topology reading its input"):
    assertEquals(
      ours(KStream(topic("in"), text, text).mapValues(_ + "!").to(topic("out"), text, text)),
      declaredByJava(_.stream[String, String]("in").mapValues(v => v + "!").to("out"))
    )

  test("counting by key keeps one sub-topology, with the store's changelog"):
    assertEquals(
      ours(KStream(topic("in"), text, text).groupByKey(text, text).count(Store("counts")).toStream.to(topic("out"), text, count)),
      declaredByJava(
        _.stream[String, String]("in").groupByKey.count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("grouping by a new key splits the pipeline at a repartition topic"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v).count(Store("counts")).toStream
          .to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("sub-topologies are numbered in the order the pipelines are made, a repartitioned segment before a later pipeline"):
    assertEquals(
      ours(
        KStream(topic("b"), text, text).groupBy(Repartition("regroup"), text, text)((_, v) => v).reduce(Store("joined"))(_ + _).toStream
          .to(topic("out-b"), text, text),
        KStream(topic("a"), text, text).to(topic("out-a"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("b").groupBy((_, v) => v, Grouped.as("regroup")).reduce((x, y) => x + y, Materialized.as("joined")).toStream
          .to("out-b")
        builder.stream[String, String]("a").to("out-a")
    )

  test("a key changed before grouping by key needs the named repartition"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).selectKey((_, v) => v).groupByKey(Repartition("rekeyed"), text, text)
          .aggregate(Store("agg"), text)("")((_, v, a) => a + v).toStream.to(topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").selectKey((_, v) => v).groupByKey(Grouped.as("rekeyed"))
          .aggregate(() => "", (_, v, a) => a + v, Materialized.as("agg")).toStream.to("out")
      )
    )

  test("a repartition name given for a key that did not change makes no repartition topic"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).filter((_, v) => v.nonEmpty).groupByKey(Repartition("unused"), text, text).count(Store("counts")).toStream
          .to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").filter((_, v) => v.nonEmpty).groupByKey(Grouped.as("unused")).count(Materialized.as("counts")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("every stateless step keeps its sub-topology, and a key changed by flatMap is repartitioned"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).filter((_, v) => v.nonEmpty).peek((_, _) => IO.unit).evalMapValues(v => IO.pure(v.trim))
          .flatMap((k, v) => List(v -> k)).mapValues(identity).groupByKey(Repartition("flipped"), text, text).reduce(Store("latest"))((_, v) => v)
          .toStream.map((k, v) => (v, k)).to(topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").filter((_, v) => v.nonEmpty).peek((_, _) => ()).mapValues(v => v.trim)
          .flatMap((k, v) => java.util.List.of(org.apache.kafka.streams.KeyValue(v, k))).mapValues(v => v).groupByKey(Grouped.as("flipped"))
          .reduce((_, v) => v, Materialized.as("latest")).toStream.map((k, v) => org.apache.kafka.streams.KeyValue(v, k)).to("out")
      )
    )

  test("grouping a table's changes again makes a third sub-topology"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).groupBy(Repartition("first"), text, text)((_, v) => v).count(Store("counts")).toStream.mapValues(_.toString)
          .groupBy(Repartition("second"), text, text)((_, n) => n).reduce(Store("by-count"))((_, v) => v).toStream.to(topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("first")).count(Materialized.as("counts")).toStream.mapValues(n => n.toString)
          .groupBy((_, n) => n, Grouped.as("second")).reduce((_, v) => v, Materialized.as("by-count")).toStream.to("out")
      )
    )

  test("two stores in one sub-topology are listed by name, not in the order the program makes them"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).groupByKey(text, text).count(Store("zeta")).toStream.groupByKey(text, count)
          .reduce(Store("alpha"))((_, n) => n).toStream.to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey.count(Materialized.as("zeta")).toStream.groupByKey(Grouped.valueSerde(Serdes.Long))
          .reduce((_, n) => n, Materialized.as("alpha")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

object TopologyGoldenSuite:
  /** What Java Streams 4.4.0 declares for the program `build` makes, under application id `app`, taken from its own code
    * (`StreamThread.initStreamsRebalanceData`) and sorted as its client sorts it before sending (`StreamsGroupHeartbeatRequestManager`).
    */
  def declaredByJava(build: StreamsBuilder => Unit): StreamsGroup.Topology =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val field    = classOf[JavaTopology].getDeclaredField("internalTopologyBuilder")
    field.setAccessible(true)
    val config   = StreamsConfig(properties)
    val metadata = TopologyMetadata(field.get(topology).asInstanceOf[InternalTopologyBuilder], config)
    metadata.buildAndRewriteTopology()
    val init = classOf[StreamThread].getDeclaredMethods.find(_.getName == "initStreamsRebalanceData").get
    init.setAccessible(true)
    val none: Supplier[java.util.Map[StreamsRebalanceData.TaskId, java.lang.Long]] = () => java.util.Map.of()
    val data = init.invoke(null, UUID.randomUUID, config, Optional.empty, Optional.empty, metadata, none, none).asInstanceOf[StreamsRebalanceData]
    def infos(topics: java.util.Map[String, StreamsRebalanceData.TopicInfo], partitions: Boolean) =
      topics.asScala.toList.sortBy(_._1).map: (name, info) =>
        StreamsGroup.TopicInfo(
          name,
          if partitions then info.numPartitions.orElse(0) else 0,
          info.replicationFactor.map(_.toShort).orElse(0.toShort),
          info.topicConfigs.asScala.toList.sortBy(_._1)
        )
    val subtopologies =
      data.subtopologies.asScala.toList.sortBy(_._1).map: (id, subtopology) =>
        StreamsGroup.Subtopology(
          id,
          subtopology.sourceTopics.asScala.toList.sorted,
          Nil,
          infos(subtopology.stateChangelogTopics, partitions = false),
          subtopology.repartitionSinkTopics.asScala.toList.sorted,
          infos(subtopology.repartitionSourceTopics, partitions = true),
          if subtopology.copartitionGroups.isEmpty then Nil else sys.error("copartition groups are not compared yet")
        )
    StreamsGroup.Topology(0, subtopologies)
