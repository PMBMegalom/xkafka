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
import xkafka.streams.{KStream, Pipeline, Repartition, Serde, Store, Topology}

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
      KStream(in, text, text).filter((_, v) => v.nonEmpty).peek((_, _) => IO.unit).map((k, v) => (v, k)).mapValues(_.toUpperCase)
        .flatMap((k, v) => List(k -> v, s"$k!" -> v)).selectKey((k, v) => k + v).evalMapValues(v => IO.pure(v + v)).to(out, text, text),
      _.stream[String, String]("in").filter((_, v) => v.nonEmpty).peek((_, _) => ()).map((k, v) => KeyValue(v, k)).mapValues(_.toUpperCase)
        .flatMap((k, v) => java.util.List.of(KeyValue(k, v), KeyValue(s"$k!", v))).selectKey((k, v) => k + v).mapValues(v => v + v).to("out"),
      Nil
    )

  test("counting by key writes and keeps what Java does, a key's timestamp never going back"):
    same(
      KStream(in, text, text).groupByKey(text, text).count(Store("counts")).toStream.to(out, text, count),
      _.stream[String, String]("in").groupByKey.count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("counts" -> count.deserializer)
    )

  test("grouping by a new key writes and keeps what Java does, through the repartition topic"):
    same(
      KStream(in, text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v).count(Store("counts")).toStream.to(out, text, count),
      _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("counts" -> count.deserializer)
    )

  test("reducing and aggregating write and keep what Java does"):
    same(
      KStream(in, text, text).groupByKey(text, text).reduce(Store("joined"))(_ + _).toStream.groupByKey(text, text)
        .aggregate(Store("lengths"), count)(10L)((_, v, n) => n + v.length).toStream.to(out, text, count),
      _.stream[String, String]("in").groupByKey.reduce((x, y) => x + y, Materialized.as("joined")).toStream.groupByKey.aggregate(
        () => java.lang.Long.valueOf(10L),
        (_, v, n) => java.lang.Long.valueOf(n + v.length),
        Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("lengths").withValueSerde(Serdes.Long)
      ).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("joined" -> text.deserializer, "lengths" -> count.deserializer)
    )

  test("a record grouped under no key is dropped before the repartition topic, as Java drops a null key"):
    val keyOrNone =
      Serde[IO, String](
        Serializer.instance((topic, headers, key) => if key == "-" then IO.pure(None) else text.serializer.serialize(topic, headers, key)),
        text.deserializer
      )
    same(
      KStream(in, text, text).groupBy(Repartition("by-value"), keyOrNone, text)((_, v) => if v == "y" then "-" else v).count(Store("counts")).toStream
        .to(out, text, count),
      _.stream[String, String]("in").groupBy((_, v) => if v == "y" then null else v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      List("counts" -> count.deserializer)
    )

  /** Runs `inputs` through the program both ways and compares the output topic's records, and the contents of the stores `stores` names. */
  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, stores: List[(String, Deserializer[IO, ?])]): IO[Unit] =
    for
      ran <- ours(pipeline)
      (written, states) = ran
      kept <-
        stores.traverse: (name, values) =>
          states.toList.flatMap(_.store(name).toList).traverse: (key, stored) =>
            values.deserialize(in, Headers.empty, Some(stored.value)).map(v => String(key.toArray) -> (v.toString, stored.timestamp))
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
      tasks(index).process(states(index), record).flatMap: (state, outputs) =>
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
