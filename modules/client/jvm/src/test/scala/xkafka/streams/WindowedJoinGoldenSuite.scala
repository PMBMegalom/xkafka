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

import java.time.Duration
import java.util.Properties

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{
  Consumed, KTable as JavaKTable, Materialized, Named as JavaNamed, Produced, SessionWindows as JavaSessionWindows,
  SlidingWindows as JavaSlidingWindows, TimeWindows as JavaTimeWindows, Windowed as JavaWindowed, WindowedSerdes
}
import org.apache.kafka.streams.state.{KeyValueStore as JavaStore, SessionStore as JavaSessionStore, WindowStore as JavaWindowStore}
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What joins of windowed tables write, and how they are described, against Java's for the same program and records. */
final class WindowedJoinGoldenSuite extends CatsEffectSuite:
  import WindowedJoinGoldenSuite.*

  private val text   = Serde.utf8[IO]
  private val clicks = Topic.from("clicks").orFail
  private val views  = Topic.from("views").orFail
  private val out    = Topic.from("out").orFail
  private val size   = 10.seconds

  private def ours(pipeline: Pipeline[IO], inputs: List[Input]): IO[Map[String, Vector[Output]]] =
    val subtopologies = Topology.of(pipeline).orFail.subtopologies
    assertEquals(subtopologies.size, 1)
    Task.inMemory[IO]("app", subtopologies.head, 0).flatMap: task =>
      inputs.zipWithIndex.foldLeftM((TaskState.empty, Vector.empty[TaskOutput])):
        case ((state, done), (input, offset)) =>
          val read =
            TaskInput(
              Some(Chunk.array(input.key.getBytes)),
              Some(Chunk.array(input.value.getBytes)),
              input.timestamp,
              Headers.empty,
              input.topic,
              0,
              offset.toLong
            )
          task.process(state, read, 0L).flatMap((after, written) => task.stored(after).map(_ -> (done ++ written)))
      .map(
        _._2.groupBy(_.topic.value).view
          .mapValues(_.map(written => Output(written.key.map(_.toVector), written.value.map(_.toVector), written.timestamp))).toMap
      )

  private def theirs(build: StreamsBuilder => Unit, inputs: List[Input], topics: List[String]): (Map[String, Vector[Output]], String) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-windowed-join-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val driver   = TopologyTestDriver(topology, properties)
    try
      inputs.foreach(input =>
        driver.createInputTopic(input.topic, StringSerializer(), StringSerializer()).pipeInput(input.key, input.value, input.timestamp)
      )
      val written =
        topics.map: topic =>
          topic -> driver.createOutputTopic(topic, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: record =>
            Output(Option(record.key).map(_.toVector), Option(record.value).map(_.toVector), record.timestamp)
      (written.toMap, topology.describe.toString)
    finally driver.close()

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, inputs: List[Input], topics: List[String]): IO[Unit] =
    ours(pipeline, inputs).map: written =>
      val (javaWritten, description) = theirs(java, inputs, topics)
      assert(javaWritten("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      topics.foreach(topic => assertEquals(written.getOrElse(topic, Vector.empty), javaWritten(topic), topic))
      assertEquals(Topology.of(pipeline).orFail.describe.toString, description)

  /** Records on both topics, keys of four users, times that move on unevenly on each so that each table's windows expire on their own. */
  private def inputs(seed: Long): List[Input] =
    val random = Random(seed)
    (1 to 300).toList.scanLeft(Input("clicks", "u0", "x", 1000L)): (last, n) =>
      val topic = if random.nextInt(3) == 0 then "views" else "clicks"
      val step  = random.nextInt(6000).toLong - 1500L
      Input(topic, s"u${random.nextInt(4)}", s"v$n", math.max(0L, last.timestamp + step))
    .tail

  private def counted(topic: Topic, read: String, count: String, store: String, windows: TimeWindows | SlidingWindows, retention: FiniteDuration) =
    val grouped = KStream(Named(read), topic, text, text).groupByKey(text, text)
    windows match
      case time: TimeWindows       => grouped.windowedBy(time).withRetention(retention).count(Named(count), Store(store))
      case sliding: SlidingWindows => grouped.windowedBy(sliding).withRetention(retention).count(Named(count), Store(store))

  private def javaCounted(
      builder: StreamsBuilder,
      topic: String,
      read: String,
      count: String,
      store: String,
      windows: Either[JavaTimeWindows, JavaSlidingWindows],
      retention: Duration
  ) =
    val grouped = builder.stream[String, String](topic, Consumed.as[String, String](read)).groupByKey
    val kept    =
      Materialized.as[String, java.lang.Long, JavaWindowStore[Bytes, Array[Byte]]](store).withKeySerde(Serdes.String).withValueSerde(Serdes.Long)
        .withRetention(retention)
    windows.fold(
      time => grouped.windowedBy(time).count(JavaNamed.as(count), kept),
      sliding => grouped.windowedBy(sliding).count(JavaNamed.as(count), kept)
    )

  private val windowedKey                                              = Windowed.timeSerde(text, size)
  private def javaOut(table: JavaKTable[JavaWindowed[String], String]) =
    table.toStream(JavaNamed.as("changes"))
      .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], size.toMillis), Serdes.String).withName("write"))

  private def written(table: KTable.Flowing[IO, Windowed[String], String, Option, Pipeline[IO]]) =
    table.toStream(Named("changes")).to(Named("write"), out, windowedKey, text.option)

  private val tumbling     = TimeWindows.tumbling(size, grace = 5.seconds)
  private val javaTumbling = JavaTimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5))
  private val short        = 20.seconds

  def javaTables(builder: StreamsBuilder) = (
    javaCounted(builder, "clicks", "read-clicks", "count-clicks", "clicks", Left(javaTumbling), Duration.ofSeconds(20)),
    javaCounted(builder, "views", "read-views", "count-views", "views", Left(javaTumbling), Duration.ofSeconds(20))
  )

  (1L to 3L).foreach: seed =>
    test(s"windowed counts joined, left joined and outer joined write what Java's do, while each side's windows expire on their own, seed $seed"):
      val left    = counted(clicks, "read-clicks", "count-clicks", "clicks", tumbling, short)
      val right   = counted(views, "read-views", "count-views", "views", tumbling, short)
      val records = inputs(seed)
      same(
        written(left.join(Named("rate"), right)((c, v) => s"$c/$v")),
        builder =>
          val (l, r) = javaTables(builder); javaOut(l.join(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/$v", JavaNamed.as("rate")))
        ,
        records,
        List("out")
      ) *> same(
        written(
          counted(clicks, "read-clicks", "count-clicks", "clicks", tumbling, short)
            .leftJoin(Named("rate"), counted(views, "read-views", "count-views", "views", tumbling, short))((c, v) => s"$c/${v.getOrElse(-1L)}")
        ),
        builder =>
          val (l, r) = javaTables(builder);
          javaOut(l.leftJoin(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/${Option(v).getOrElse(-1L)}", JavaNamed.as("rate")))
        ,
        records,
        List("out")
      ) *> same(
        written(
          counted(clicks, "read-clicks", "count-clicks", "clicks", tumbling, short)
            .outerJoin(Named("rate"), counted(views, "read-views", "count-views", "views", tumbling, short))(
              _.fold(c => s"$c/-", v => s"-/$v", (c, v) => s"$c/$v")
            )
        ),
        builder =>
          val (l, r) = javaTables(builder)
          javaOut(
            l.outerJoin(r, (c: java.lang.Long, v: java.lang.Long) => s"${Option(c).getOrElse("-")}/${Option(v).getOrElse("-")}", JavaNamed.as("rate"))
          )
        ,
        records,
        List("out")
      )

  test("a windowed join's result kept in a store logs what Java's does"):
    val left  = counted(clicks, "read-clicks", "count-clicks", "clicks", tumbling, short)
    val right = counted(views, "read-views", "count-views", "views", tumbling, short)
    same(
      written(left.join(Named("rate"), right, Store("rates"), text)((c, v) => s"$c/$v")),
      builder =>
        val l     = javaCounted(builder, "clicks", "read-clicks", "count-clicks", "clicks", Left(javaTumbling), Duration.ofSeconds(20))
        val r     = javaCounted(builder, "views", "read-views", "count-views", "views", Left(javaTumbling), Duration.ofSeconds(20))
        val rates =
          Materialized.as[JavaWindowed[String], String, JavaStore[Bytes, Array[Byte]]]("rates").withValueSerde(Serdes.String)
            .withKeySerde(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], size.toMillis))
        javaOut(l.join(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/$v", JavaNamed.as("rate"), rates))
      ,
      inputs(7L),
      List("out", "app-rates-changelog")
    )

  test("sliding windows joined write what Java's do"):
    val sliding     = SlidingWindows(size, grace = 5.seconds)
    val javaSliding = JavaSlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5))
    same(
      written(
        counted(clicks, "read-clicks", "count-clicks", "clicks", sliding, 30.seconds)
          .join(Named("rate"), counted(views, "read-views", "count-views", "views", sliding, 30.seconds))((c, v) => s"$c/$v")
      ),
      builder =>
        val l = javaCounted(builder, "clicks", "read-clicks", "count-clicks", "clicks", Right(javaSliding), Duration.ofSeconds(30))
        val r = javaCounted(builder, "views", "read-views", "count-views", "views", Right(javaSliding), Duration.ofSeconds(30))
        javaOut(l.join(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/$v", JavaNamed.as("rate")))
      ,
      inputs(8L),
      List("out")
    )

  test("a stream keyed by window joined with a windowed table writes what Java's does"):
    val left  = counted(clicks, "read-clicks", "count-clicks", "clicks", tumbling, short)
    val right = counted(views, "read-views", "count-views", "views", tumbling, short)
    same(
      left.toStream(Named("click-changes")).join(Named("with-views"), right)((c, v) => s"$c/$v").to(Named("write"), out, windowedKey, text),
      builder =>
        val l = javaCounted(builder, "clicks", "read-clicks", "count-clicks", "clicks", Left(javaTumbling), Duration.ofSeconds(20))
        val r = javaCounted(builder, "views", "read-views", "count-views", "views", Left(javaTumbling), Duration.ofSeconds(20))
        l.toStream(JavaNamed.as("click-changes"))
          .join(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/$v", org.apache.kafka.streams.kstream.Joined.as("with-views"))
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], size.toMillis), Serdes.String).withName("write"))
      ,
      inputs(9L),
      List("out")
    )

  test("session counts joined write what Java's do, each session found by its start and end"):
    val sessions                                                            = SessionWindows(3.seconds, grace = 5.seconds)
    def sessioned(topic: Topic, read: String, count: String, store: String) =
      KStream(Named(read), topic, text, text).groupByKey(text, text).windowedBy(sessions).withRetention(30.seconds).count(Named(count), Store(store))
    def javaSessioned(builder: StreamsBuilder, topic: String, read: String, count: String, store: String) =
      builder.stream[String, String](topic, Consumed.as[String, String](read)).groupByKey
        .windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(3), Duration.ofSeconds(5))).count(
          JavaNamed.as(count),
          Materialized.as[String, java.lang.Long, JavaSessionStore[Bytes, Array[Byte]]](store).withKeySerde(Serdes.String).withValueSerde(Serdes.Long)
            .withRetention(Duration.ofSeconds(30))
        )
    // Sessions of the two topics match only where both have the same start and end, so each view also counts as a click.
    val records = inputs(10L).flatMap(input => if input.topic == "views" then List(input, input.copy(topic = "clicks")) else List(input))
    same(
      sessioned(clicks, "read-clicks", "count-clicks", "clicks")
        .leftJoin(Named("rate"), sessioned(views, "read-views", "count-views", "views"))((c, v) => s"$c/${v.getOrElse(-1L)}")
        .toStream(Named("changes")).to(Named("write"), out, Windowed.sessionSerde(text), text.option),
      builder =>
        val l = javaSessioned(builder, "clicks", "read-clicks", "count-clicks", "clicks")
        val r = javaSessioned(builder, "views", "read-views", "count-views", "views")
        l.leftJoin(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/${Option(v).getOrElse(-1L)}", JavaNamed.as("rate"))
          .toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.String).withName("write"))
      ,
      records,
      List("out")
    )

  test("a window the other table has dropped after its retention finds nothing, as in Java, while its own table still takes records for it"):
    // The views table moves on a minute and drops its first window; a click late enough for the clicks table's own grace then looks for it.
    val records =
      List(
        Input("clicks", "u0", "a", 1000L),
        Input("views", "u0", "b", 1000L),
        Input("views", "u0", "c", 60000L),
        Input("clicks", "u0", "d", 2000L),
        Input("views", "u1", "e", 61000L),
        Input("clicks", "u0", "f", 3000L)
      )
    val left  = counted(clicks, "read-clicks", "count-clicks", "clicks", tumbling, short)
    val right = counted(views, "read-views", "count-views", "views", tumbling, short)
    same(
      written(left.leftJoin(Named("rate"), right)((c, v) => s"$c/${v.getOrElse(-1L)}")),
      builder =>
        val (l, r) = javaTables(builder)
        javaOut(l.leftJoin(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/${Option(v).getOrElse(-1L)}", JavaNamed.as("rate")))
      ,
      records,
      List("out")
    )

  test("a session the other table has dropped after its retention finds nothing, as in Java"):
    val sessions                                                            = SessionWindows(3.seconds, grace = 30.seconds)
    def sessioned(topic: Topic, read: String, count: String, store: String) =
      KStream(Named(read), topic, text, text).groupByKey(text, text).windowedBy(sessions).withRetention(35.seconds).count(Named(count), Store(store))
    def javaSessioned(builder: StreamsBuilder, topic: String, read: String, count: String, store: String) =
      builder.stream[String, String](topic, Consumed.as[String, String](read)).groupByKey
        .windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(3), Duration.ofSeconds(30))).count(
          JavaNamed.as(count),
          Materialized.as[String, java.lang.Long, JavaSessionStore[Bytes, Array[Byte]]](store).withKeySerde(Serdes.String).withValueSerde(Serdes.Long)
            .withRetention(Duration.ofSeconds(35))
        )
    val records =
      List(Input("clicks", "u0", "a", 1000L), Input("views", "u0", "b", 1000L), Input("views", "u1", "c", 90000L), Input("clicks", "u0", "d", 1000L))
    same(
      sessioned(clicks, "read-clicks", "count-clicks", "clicks")
        .leftJoin(Named("rate"), sessioned(views, "read-views", "count-views", "views"))((c, v) => s"$c/${v.getOrElse(-1L)}")
        .toStream(Named("changes")).to(Named("write"), out, Windowed.sessionSerde(text), text.option),
      builder =>
        val l = javaSessioned(builder, "clicks", "read-clicks", "count-clicks", "clicks")
        val r = javaSessioned(builder, "views", "read-views", "count-views", "views")
        l.leftJoin(r, (c: java.lang.Long, v: java.lang.Long) => s"$c/${Option(v).getOrElse(-1L)}", JavaNamed.as("rate"))
          .toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.String).withName("write"))
      ,
      records,
      List("out")
    )

object WindowedJoinGoldenSuite:
  final case class Input(topic: String, key: String, value: String, timestamp: Long)
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long)
