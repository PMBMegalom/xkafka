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

import java.util.Properties

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import xkafka.streams.{KStream, Named, Pipeline, Repartition, Serde, Store, Topology}

/** What a task writes and keeps for a program, against what Java Streams' `TopologyTestDriver` writes and keeps for the same program and records,
  * with Java's record cache off so that it, too, passes on every change.
  */
final class TaskGoldenSuite extends CatsEffectSuite:
  import TaskGoldenSuite.*

  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val in    = Topic.from("in").orFail
  private val out   = Topic.from("out").orFail

  private val inputs =
    List(
      Input("a", "x", 1000L, List("trace" -> "1")),
      Input("b", "y", 3000L, Nil),
      Input("a", "z", 2000L, List("trace" -> "3")),
      Input("c", "", 4000L, Nil),
      Input("b", "x", 2500L, List("trace" -> "5", "trace" -> "6"))
    )

  test("stateless steps write what Java writes, with each record's timestamp and headers"):
    same(
      KStream(Named("read-1"), in, text, text).filter(Named("filter-1"))((_, v) => v.nonEmpty).peek(Named("peek-1"))((_, _) => IO.unit)
        .map(Named("map-1"))((k, v) => (v, k)).mapValues(Named("map-values-1"))(_.toUpperCase)
        .flatMap(Named("flat-map-1"))((k, v) => List(k -> v, s"$k!" -> v)).selectKey(Named("select-key-1"))((k, v) => k + v)
        .evalMapValues(Named("eval-map-values-1"))(v => IO.pure(v + v)).evalFilter(Named("eval-filter-1"))((k, _) => IO.pure(!k.contains("!")))
        .evalMap(Named("eval-map-1"))((k, v) => IO.pure((v, s"$k:$v"))).to(Named("write-1"), out, text, text),
      _.stream[String, String]("in").filter((_, v) => v.nonEmpty).peek((_, _) => ()).map((k, v) => KeyValue(v, k)).mapValues(_.toUpperCase)
        .flatMap((k, v) => java.util.List.of(KeyValue(k, v), KeyValue(s"$k!", v))).selectKey((k, v) => k + v).mapValues(v => v + v)
        .filter((k, _) => !k.contains("!")).map((k, v) => KeyValue(v, s"$k:$v")).to("out"),
      Nil
    )

  test("counting by key writes and keeps what Java does, a key's timestamp never going back"):
    same(
      KStream(Named("read-2"), in, text, text).groupByKey(text, text).count(Named("count-1"), Store("counts")).toStream(Named("to-stream-1"))
        .to(Named("write-2"), out, text, count),
      _.stream[String, String]("in").groupByKey.count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("counts" -> shown(count.deserializer))
    )

  test("grouping by a new key writes and keeps what Java does, through the repartition topic"):
    same(
      KStream(Named("read-3"), in, text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v).count(Named("count-2"), Store("counts"))
        .toStream(Named("to-stream-2")).to(Named("write-3"), out, text, count),
      _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("counts" -> shown(count.deserializer))
    )

  test("reducing and aggregating write and keep what Java does"):
    same(
      KStream(Named("read-4"), in, text, text).groupByKey(text, text).reduce(Named("reduce-1"), Store("joined"))(_ + _).toStream(Named("to-stream-3"))
        .groupByKey(text, text).aggregate(Named("aggregate-1"), Store("lengths"), count)(10L)((_, v, n) => n + v.length)
        .toStream(Named("to-stream-4")).to(Named("write-4"), out, text, count),
      _.stream[String, String]("in").groupByKey.reduce((x, y) => x + y, Materialized.as("joined")).toStream.groupByKey.aggregate(
        () => java.lang.Long.valueOf(10L),
        (_, v, n) => java.lang.Long.valueOf(n + v.length),
        Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("lengths").withValueSerde(Serdes.Long)
      ).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("joined" -> shown(text.deserializer), "lengths" -> shown(count.deserializer))
    )

  test("a record grouped under no key is dropped before the repartition topic, as Java drops a null key"):
    val keyOrNone =
      Serde[IO, String](
        Serializer.instance((topic, headers, key) => if key == "-" then IO.pure(None) else text.serializer.serialize(topic, headers, key)),
        text.deserializer
      )
    same(
      KStream(Named("read-5"), in, text, text).groupBy(Repartition("by-value"), keyOrNone, text)((_, v) => if v == "y" then "-" else v)
        .count(Named("count-3"), Store("counts")).toStream(Named("to-stream-5")).to(Named("write-5"), out, text, count),
      _.stream[String, String]("in").groupBy((_, v) => if v == "y" then null else v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("counts" -> shown(count.deserializer))
    )

  /** Runs `inputs` through the program both ways and compares the output topic's records, and the contents of the stores `stores` names. */
  /** Values as `values` reads them, written as text to compare with Java's. */
  private def shown[A](values: Deserializer[IO, A]): Deserializer[IO, String] = values.map(_.toString)

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, stores: List[(String, Deserializer[IO, String])]): IO[Unit] =
    for
      ran <- ours(pipeline)
      (written, states) = ran
      kept <-
        stores.traverse: (name, values) =>
          states.toList.flatMap(_.store(name).toList).traverse: (key, stored) =>
            values.deserialize(in, Headers.empty, Some(stored.value)).map(v => String(key.toArray) -> (v, stored.timestamp))
          .map(name -> _.toMap)
    yield
      val (javaOut, javaStores) = declaredRun(java, stores.map(_._1))
      assert(javaOut.nonEmpty, "Java wrote nothing, so there is nothing to compare")
      assertEquals(written.filter(_.topic == out).map(output), javaOut)
      assertEquals(kept.toMap, javaStores)

  /** Each segment's task on partition 0, a repartitioned record going on to the next segment at once, as Java's test driver runs it. */
  private def ours(pipeline: Pipeline[IO]): IO[(Vector[TaskOutput], Vector[TaskState])] =
    val tasks = Topology.of(pipeline).orFail.pipelines.head.segments.toList.toVector.map(Task[IO]("app", _, 0))
    def run(index: Int, record: TaskInput, states: Vector[TaskState]): IO[(Vector[TaskOutput], Vector[TaskState])] =
      tasks(index).process(states(index), record, 0L).flatMap: (state, outputs) =>
        outputs.foldLeftM((Vector.empty[TaskOutput], states.updated(index, state))):
          case ((done, now), written) if written.topic.value.endsWith("-repartition") =>
            run(index + 1, TaskInput(written.key, written.value, written.timestamp, written.headers), now).map((more, after) => (done ++ more, after))
          case ((done, now), written) => IO.pure((done :+ written, now))
    inputs.foldLeftM((Vector.empty[TaskOutput], Vector.fill(tasks.size)(TaskState.empty))):
      case ((done, states), input) =>
        val headers = Headers.fromVector(input.headers.map((k, v) => Header(k, Some(Chunk.array(v.getBytes)))).toVector)
        run(0, TaskInput(Some(Chunk.array(input.key.getBytes)), Some(Chunk.array(input.value.getBytes)), input.timestamp, headers), states)
          .map((more, after) => (done ++ more, after))

  /** The output topic's records and the named stores' contents, as Java's test driver gives them for `inputs`. */
  private def declaredRun(build: StreamsBuilder => Unit, stores: List[String]): (Vector[Output], Map[String, Map[String, (String, Long)]]) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    val builder = StreamsBuilder()
    build(builder)
    val driver = TopologyTestDriver(builder.build(properties), properties)
    try
      val input  = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      val output =
        driver.createOutputTopic(
          "out",
          org.apache.kafka.common.serialization.ByteArrayDeserializer(),
          org.apache.kafka.common.serialization.ByteArrayDeserializer()
        )
      inputs.foreach: record =>
        val headers = RecordHeaders(record.headers.map((k, v) => RecordHeader(k, v.getBytes): org.apache.kafka.common.header.Header).asJava)
        input.pipeInput(TestRecord(record.key, record.value, headers, java.lang.Long.valueOf(record.timestamp)))
      val written =
        output.readRecordsToList.asScala.toVector.map: r =>
          Output(
            Option(r.key).map(String(_)),
            Option(r.value).map(_.toVector),
            r.timestamp,
            r.headers.toArray.toVector.map(h => h.key -> Option(h.value).map(String(_)))
          )
      val kept =
        stores.map: name =>
          val store = driver.getTimestampedKeyValueStore[String, Any](name)
          name -> store.all.asScala.toList.map(kv => kv.key -> (kv.value.value.toString, kv.value.timestamp)).toMap
      (written, kept.toMap)
    finally driver.close()

  private def output(written: TaskOutput): Output =
    Output(
      written.key.map(k => String(k.toArray)),
      written.value.map(_.toVector),
      written.timestamp,
      written.headers.values.map(h => h.key -> h.value.map(v => String(v.toArray)))
    )

object TaskGoldenSuite:
  final case class Input(key: String, value: String, timestamp: Long, headers: List[(String, String)])

  /** An output record with its value as bytes, so text and number values compare alike. */
  final case class Output(key: Option[String], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[(String, Option[String])])
