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

import java.util.Properties

import scala.jdk.CollectionConverters.*
import scala.util.Random

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Consumed, KTable as JavaKTable, Materialized, Named as JavaNamed, Produced, TableJoined}
import org.apache.kafka.streams.state.KeyValueStore as JavaStore
import org.apache.kafka.streams.test.TestRecord
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What a foreign-key join writes to every topic it touches, and how it is described, against Java's for the same program and records, through Java
  * Streams' `TopologyTestDriver`.
  */
final class ForeignKeyGoldenSuite extends CatsEffectSuite:
  import ForeignKeyGoldenSuite.*

  private val text  = Serde.utf8[IO]
  private val left  = Topic.from("left").orFail
  private val other = Topic.from("other").orFail
  private val out   = Topic.from("out").orFail

  private def noneIsAbsent(value: String): Option[String] = Option.when(value != "none")(value)

  private def leftTable  = KTable(Named("read-left"), left, text, text, Store("left-store"))
  private def otherTable = KTable(Named("read-other"), other, text, text, Store("other-store"))

  private def inner = leftTable.joinOnForeignKey(Named("fk"), otherTable, text, text, text)((_, value) => noneIsAbsent(value))((l, r) => s"$l+$r")
  private def outer =
    leftTable.leftJoinOnForeignKey(Named("fk"), otherTable, text, text, text)((_, value) => noneIsAbsent(value))((l, r) => s"$l+${r.getOrElse("?")}")

  /** Every record our tasks write for `inputs`, by topic, passing each one a task reads to that task, as the driver passes on its internal topics. */
  private def ours(pipeline: Pipeline[IO], inputs: List[Input]): IO[Map[String, Vector[Output]]] =
    val subtopologies = Topology.of(pipeline).orFail.subtopologies
    subtopologies.zipWithIndex.traverse((subtopology, at) => Task.inMemory[IO]("app", subtopology, 0).map(at -> _)).flatMap: tasks =>
      val readers = subtopologies.zipWithIndex.flatMap((subtopology, at) => subtopology.origins.map(Task.topicOf("app", _) -> at)).toMap
      def one(input: TaskInput, states: Map[Int, TaskState], offset: Long): IO[(Map[Int, TaskState], Vector[TaskOutput], Long)] =
        val at = readers(input.topic)
        tasks.toMap.apply(at).process(states(at), input.copy(offset = offset), 0L).flatMap: (after, written) =>
          tasks.toMap.apply(at).stored(after).map(stored => (states.updated(at, stored), written, offset + 1))
      def drained(
          queue: Vector[TaskInput],
          states: Map[Int, TaskState],
          done: Vector[TaskOutput],
          offset: Long
      ): IO[(Map[Int, TaskState], Vector[TaskOutput], Long)] =
        queue.headOption match
          case None        => IO.pure((states, done, offset))
          case Some(input) => one(input, states, offset).flatMap: (after, written, next) =>
              val passed =
                written.filter(output => readers.contains(output.topic.value)).map: output =>
                  TaskInput(output.key, output.value, output.timestamp, output.headers, output.topic.value, 0, 0L)
              drained(queue.tail ++ passed, after, done ++ written, next)
      val empty: Map[Int, TaskState] = tasks.map((at, _) => at -> TaskState.empty).toMap
      inputs.foldLeftM((empty, Vector.empty[TaskOutput], 0L)):
        case ((states, done, offset), input) =>
          val headers = Headers.fromVector(input.headers.map((k, v) => Header(k, Some(Chunk.array(v.getBytes)))).toVector)
          val bytes   = (value: String) => Chunk.array(value.getBytes)
          drained(
            Vector(TaskInput(Some(bytes(input.key)), input.value.map(bytes), input.timestamp, headers, input.topic, 0, 0L)),
            states,
            done,
            offset
          )
      .map(_._2.groupBy(_.topic.value).view.mapValues(_.map(output)).toMap)

  private def output(written: TaskOutput): Output =
    Output(
      written.key.map(_.toVector),
      written.value.map(_.toVector),
      written.timestamp,
      written.headers.values.map(h => h.key -> h.value.map(v => String(v.toArray)))
    )

  private def theirs(build: StreamsBuilder => Unit, inputs: List[Input], topics: List[String]): (Map[String, Vector[Output]], String) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-foreign-key-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val driver   = TopologyTestDriver(topology, properties)
    try
      inputs.foreach: input =>
        val headers = RecordHeaders(input.headers.map((k, v) => RecordHeader(k, v.getBytes): org.apache.kafka.common.header.Header).asJava)
        driver.createInputTopic(input.topic, StringSerializer(), StringSerializer())
          .pipeInput(TestRecord(input.key, input.value.orNull, headers, java.lang.Long.valueOf(input.timestamp)))
      val written =
        topics.map: topic =>
          topic -> driver.createOutputTopic(topic, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: record =>
            Output(
              Option(record.key).map(_.toVector),
              Option(record.value).map(_.toVector),
              record.timestamp,
              record.headers.toArray.toVector.map(h => h.key -> Option(h.value).map(String(_)))
            )
      (written.toMap, topology.describe.toString)
    finally driver.close()

  private def javaMaterialized(name: String)    = Materialized.as[String, String, JavaStore[Bytes, Array[Byte]]](name)
  private def javaLeft(builder: StreamsBuilder) =
    builder.table[String, String]("left", Consumed.as[String, String]("read-left"), javaMaterialized("left-store"))
  private def javaOther(builder: StreamsBuilder) =
    builder.table[String, String]("other", Consumed.as[String, String]("read-other"), javaMaterialized("other-store"))
  private def javaKey(value: String): String = if value == "none" then null else value

  private def javaJoined(l: JavaKTable[String, String], r: JavaKTable[String, String]) =
    l.join[String, String, String](r, (value: String) => javaKey(value), (a: String, b: String) => s"$a+$b", TableJoined.as[String, String]("fk"))

  private def javaOuter(l: JavaKTable[String, String], r: JavaKTable[String, String]) =
    l.leftJoin[String, String, String](
      r,
      (value: String) => javaKey(value),
      (a: String, b: String) => s"$a+${Option(b).getOrElse("?")}",
      TableJoined.as[String, String]("fk")
    )

  private val topics =
    List(
      "out",
      "app-fk-subscription-registration-topic",
      "app-fk-subscription-response-topic",
      "app-fk-subscription-store-changelog",
      "app-left-store-changelog",
      "app-other-store-changelog"
    )

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, inputs: List[Input], more: List[String] = Nil): IO[Unit] =
    ours(pipeline, inputs).map: written =>
      val (javaWritten, description) = theirs(java, inputs, topics ++ more)
      assert(javaWritten("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      (topics ++ more).foreach(topic => assertEquals(written.getOrElse(topic, Vector.empty), javaWritten(topic), topic))
      assertEquals(Topology.of(pipeline).orFail.describe.toString, description)

  private val story =
    List(
      Input("left", "k1", Some("a"), 1000L, List("trace" -> "t")),
      Input("other", "a", Some("A"), 2000L, List("from" -> "other")),
      Input("left", "k1", Some("a"), 1500L, Nil),
      Input("other", "b", Some("B"), 3000L, Nil),
      Input("left", "k1", Some("b"), 2500L, List("trace" -> "moved")),
      Input("left", "k1", Some("none"), 2600L, Nil),
      Input("left", "k2", Some("b"), 2700L, Nil),
      Input("left", "k3", Some("missing"), 2800L, Nil),
      Input("other", "b", None, 4000L, Nil),
      Input("other", "b", Some(""), 4050L, Nil),
      Input("left", "k2", None, 4100L, Nil),
      Input("left", "k9", None, 4200L, Nil),
      Input("other", "unused", None, 4300L, Nil),
      Input("left", "k3", Some("a"), 900L, Nil)
    )

  private def shuffled(seed: Long): List[Input] =
    val random = Random(seed)
    (1 to 400).toList.map: at =>
      val headers = if random.nextBoolean() then List("trace" -> s"t$at") else Nil
      if random.nextInt(3) == 0 then
        Input("other", s"f${random.nextInt(4)}", Option.when(random.nextInt(4) != 0)(s"o$at"), 1000L + random.nextInt(5000), headers)
      else
        val value =
          random.nextInt(7) match
            case 0 => None
            case 1 => Some("none")
            case n => Some(s"f${n % 5}")
        Input("left", s"k${random.nextInt(5)}", value, 1000L + random.nextInt(5000), headers)

  private def written(table: KTable.Flowing[IO, String, String, Option, Pipeline[IO]]) =
    table.toStream(Named("changes")).to(Named("write"), out, text, text.option)

  private def javaWrite(table: JavaKTable[String, String]) = table.toStream(JavaNamed.as("changes")).to("out", Produced.as[String, String]("write"))

  test("a join writes what Java's does at each step of a row's life"):
    same(
      written(inner),
      builder =>
        val l = javaLeft(builder); javaWrite(javaJoined(l, javaOther(builder)))
      ,
      story
    )

  test("a left join writes what Java's does at each step of a row's life"):
    same(
      written(outer),
      builder =>
        val l = javaLeft(builder); javaWrite(javaOuter(l, javaOther(builder)))
      ,
      story
    )

  (1L to 4L).foreach: seed =>
    test(s"a join and a left join write what Java's do over many changes in any order, seed $seed"):
      val inputs = shuffled(seed)
      same(
        written(inner),
        builder =>
          val l = javaLeft(builder); javaWrite(javaJoined(l, javaOther(builder)))
        ,
        inputs
      ) *> same(
        written(outer),
        builder =>
          val l = javaLeft(builder); javaWrite(javaOuter(l, javaOther(builder)))
        ,
        inputs
      )

  test("a result kept in a store logs and passes on what Java's does"):
    val kept =
      leftTable
        .joinOnForeignKey(Named("fk"), otherTable, text, text, text, Store("result"), text)((_, value) => noneIsAbsent(value))((l, r) => s"$l+$r")
    same(
      written(kept),
      builder =>
        val l = javaLeft(builder)
        javaWrite(l.join[String, String, String](
          javaOther(builder),
          (value: String) => javaKey(value),
          (a: String, b: String) => s"$a+$b",
          TableJoined.as[String, String]("fk"),
          javaMaterialized("result")
        ))
      ,
      shuffled(11L),
      List("app-result-changelog")
    )

  test("a left join's result kept in a store logs and passes on what Java's does"):
    val kept =
      leftTable.leftJoinOnForeignKey(Named("fk"), otherTable, text, text, text, Store("result"), text)((_, value) => noneIsAbsent(value))((l, r) =>
        s"$l+${r.getOrElse("?")}"
      )
    same(
      written(kept),
      builder =>
        val l = javaLeft(builder)
        javaWrite(l.leftJoin[String, String, String](
          javaOther(builder),
          (value: String) => javaKey(value),
          (a: String, b: String) => s"$a+${Option(b).getOrElse("?")}",
          TableJoined.as[String, String]("fk"),
          javaMaterialized("result")
        ))
      ,
      shuffled(12L),
      List("app-result-changelog")
    )

  test("a kept result grouped by its value takes each old value out of its group, as Java's does"):
    val kept =
      leftTable
        .joinOnForeignKey(Named("fk"), otherTable, text, text, text, Store("result"), text)((_, value) => noneIsAbsent(value))((l, r) => s"$l+$r")
    same(
      kept.groupBy(Repartition("by-value"), text, text)((key, value) => (value, key)).count(Named("count"), Store("counts"))
        .toStream(Named("changes")).to(Named("write"), out, text, Serde.long[IO].option),
      builder =>
        val l = javaLeft(builder)
        l.join[String, String, String](
          javaOther(builder),
          (value: String) => javaKey(value),
          (a: String, b: String) => s"$a+$b",
          TableJoined.as[String, String]("fk"),
          javaMaterialized("result")
        ).groupBy(
          (key: String, value: String) => org.apache.kafka.streams.KeyValue.pair(value, key),
          org.apache.kafka.streams.kstream.Grouped.as[String, String]("by-value")
        ).count(
          JavaNamed.as("count"),
          Materialized.as[String, java.lang.Long, JavaStore[Bytes, Array[Byte]]]("counts").withKeySerde(Serdes.String).withValueSerde(Serdes.Long)
        ).toStream(JavaNamed.as("changes")).to("out", Produced.`with`(Serdes.String, Serdes.Long).withName("write"))
      ,
      shuffled(31L),
      List("app-result-changelog", "app-counts-changelog")
    )

  test("tables that filter and change their values read as Java's do"):
    val l      = leftTable.filter(Named("kept-left"))((_, value) => value != "f3").mapValues(Named("up"))(_.toUpperCase)
    val r      = otherTable.mapValues(Named("marked"))(value => s"<$value>")
    val joined = l.joinOnForeignKey(Named("fk"), r, text, text, text)((_, value) => noneIsAbsent(value.toLowerCase))((a, b) => s"$a+$b")
    same(
      written(joined),
      builder =>
        val keep =
          javaLeft(builder).filter((_: String, value: String) => value != "f3", JavaNamed.as("kept-left"))
            .mapValues((value: String) => value.toUpperCase, JavaNamed.as("up"))
        val right = javaOther(builder).mapValues((value: String) => s"<$value>", JavaNamed.as("marked"))
        javaWrite(keep.join[String, String, String](
          right,
          (value: String) => javaKey(value.toLowerCase),
          (a: String, b: String) => s"$a+$b",
          TableJoined.as[String, String]("fk")
        ))
      ,
      shuffled(21L)
    )

  private def described(pipeline: Pipeline[IO], java: StreamsBuilder => Unit) =
    val builder = StreamsBuilder()
    java(builder)
    assertEquals(Topology.of(pipeline).orFail.describe.toString, builder.build.describe.toString)

  private def javaCounted(builder: StreamsBuilder, read: String, topic: String, repartition: String, count: String, store: String) =
    builder.stream[String, String](topic, Consumed.as[String, String](read))
      .groupBy[String]((_: String, value: String) => value, org.apache.kafka.streams.kstream.Grouped.as[String, String](repartition)).count(
        JavaNamed.as(count),
        Materialized.as[String, java.lang.Long, JavaStore[Bytes, Array[Byte]]](store).withKeySerde(Serdes.String).withValueSerde(Serdes.Long)
      )

  private def counted(read: String, topic: String, repartition: String, count: String, store: String) =
    KStream(Named(read), Topic.from(topic).orFail, text, text).groupBy(Repartition(repartition), text, text)((_, value) => value)
      .count(Named(count), Store(store))

  test("joining a table counted after a repartition is numbered as Java numbers it, whichever side it is on"):
    val long = Serde.long[IO]
    described(
      written(
        leftTable.joinOnForeignKey(Named("fk"), counted("read-other", "other", "by-other", "count", "counts"), text, text, long)((_, value) =>
          Some(value)
        )((l, r) => s"$l+$r")
      ),
      builder =>
        val l = javaLeft(builder)
        javaWrite(l.join[String, String, java.lang.Long](
          javaCounted(builder, "read-other", "other", "by-other", "count", "counts"),
          (value: String) => value,
          (a: String, b: java.lang.Long) => s"$a+$b",
          TableJoined.as[String, String]("fk")
        ))
    )
    described(
      written(
        counted("read-left", "left", "by-left", "count", "counts")
          .joinOnForeignKey(Named("fk"), otherTable, long, text, text)((_, value) => Some(value.toString))((l, r) => s"$l+$r")
      ),
      builder =>
        val l = javaCounted(builder, "read-left", "left", "by-left", "count", "counts")
        javaWrite(l.join[String, String, String](
          javaOther(builder),
          (value: java.lang.Long) => value.toString,
          (a: java.lang.Long, b: String) => s"$a+$b",
          TableJoined.as[String, String]("fk")
        ))
    )

  test("the kept result of one join joined again is numbered as Java numbers it"):
    val third = KTable(Named("read-third"), Topic.from("third").orFail, text, text, Store("third-store"))
    val first =
      leftTable.joinOnForeignKey(Named("fk"), otherTable, text, text, text, Store("result"), text)((_, value) => Some(value))((l, r) => s"$l+$r")
    val second = first.joinOnForeignKey(Named("fk2"), third, text, text, text)((_, value) => Some(value))((l, r) => s"$l+$r")
    described(
      written(second),
      builder =>
        val l   = javaLeft(builder)
        val one =
          l.join[String, String, String](
            javaOther(builder),
            (value: String) => value,
            (a: String, b: String) => s"$a+$b",
            TableJoined.as[String, String]("fk"),
            javaMaterialized("result")
          )
        val other = builder.table[String, String]("third", Consumed.as[String, String]("read-third"), javaMaterialized("third-store"))
        javaWrite(
          one.join[String, String, String](other, (value: String) => value, (a: String, b: String) => s"$a+$b", TableJoined.as[String, String]("fk2"))
        )
    )

object ForeignKeyGoldenSuite:
  final case class Input(topic: String, key: String, value: Option[String], timestamp: Long, headers: List[(String, String)])
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[(String, Option[String])])
