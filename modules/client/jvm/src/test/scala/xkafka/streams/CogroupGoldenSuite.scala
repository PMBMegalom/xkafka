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

import scala.collection.immutable.Queue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{
  Aggregator, CogroupedKStream as JavaCogrouped, Consumed, Grouped, Initializer, Joined, KGroupedStream as JavaGrouped, Materialized, Merger,
  Named as JavaNamed, Produced, SessionWindows as JavaSessionWindows, SlidingWindows as JavaSlidingWindows, Suppressed as JavaSuppressed,
  TimeWindows as JavaTimeWindows, WindowedSerdes
}
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import scodec.bits.ByteVector
import internal.protocol.{StreamsGroup, StreamsGroupTopologyDescriptionUpdateRequest}
import internal.streams.{Declaration, Task, TaskInput, TaskOutput, TaskState}

/** Cogroups, against the same programs in Java: the description and topology, and every record tasks write, to their repartition topics and changelog
  * too, with Java's record cache off so that it, too, passes on every change.
  */
final class CogroupGoldenSuite extends CatsEffectSuite:
  import CogroupGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val s    = Serdes.String

  private def topic(name: String) = Topic.from(name).orFail

  private def stored = Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("kept").withValueSerde(s)

  private val tagged: (String, String, String) => String  = (_, v, total) => s"$total+$v"
  private val marked: (String, String, String) => String  = (_, v, total) => s"$total*$v"
  private val dashed: (String, String, String) => String  = (_, v, total) => s"$total-$v"
  private val joining: (String, String, String) => String = (_, first, second) => s"($first|$second)"

  private val javaTagged: Aggregator[String, String, String] = (_, v, total) => s"$total+$v"
  private val javaMarked: Aggregator[String, String, String] = (_, v, total) => s"$total*$v"
  private val javaDashed: Aggregator[String, String, String] = (_, v, total) => s"$total-$v"
  private val javaJoining: Merger[String, String]            = (_, first, second) => s"($first|$second)"
  private val javaStart: Initializer[String]                 = () => "start"

  private val records =
    List(
      Input("a", "k", Some("1"), 1000L),
      Input("b", "k", Some("2"), 3000L),
      Input("a", "j", Some("3"), 2000L),
      Input("b", "k", Some("4"), 1500L),
      Input("a", "k", Some("5"), 6000L),
      Input("b", "j", Some("6"), 4000L),
      Input("a", "k", None, 7000L),
      Input("b", "j", None, 3500L)
    )

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, outputs: List[String], inputs: List[Input] = records): IO[Unit] =
    ours(pipeline, inputs).map: written =>
      val theirs = javaRun(java, outputs, inputs)
      outputs.foreach(name => assert(theirs(name).nonEmpty, s"Java wrote nothing to $name, so there is nothing to compare"))
      assertEquals(outputs.map(name => name -> written.getOrElse(name, Vector.empty)).toMap, theirs)

  private def ours(pipeline: Pipeline[IO], inputs: List[Input]): IO[Map[String, Vector[Output]]] =
    val topology = Topology.of(pipeline).orFail
    val tasks    = topology.subtopologies.toVector.map(Task[IO]("app", _, 0))
    val reading  = topology.subtopologies.zipWithIndex.flatMap((sub, id) => sub.origins.map(Task.topicOf("app", _) -> id)).toMap
    inputs.foldLeftM((Vector.fill(tasks.size)(TaskState.empty), Vector.empty[(String, Output)])):
      case ((states, out), input) =>
        val first = Queue(TaskInput(Some(bytes(input.key)), input.value.map(bytes), input.timestamp, headers(input), input.topic))
        runAll(tasks, reading, first, states, out)
    .map(_._2.groupMap(_._1)(_._2))

  private def runAll(
      tasks: Vector[Task[IO]],
      reading: Map[String, Int],
      queue: Queue[TaskInput],
      states: Vector[TaskState],
      out: Vector[(String, Output)]
  ): IO[(Vector[TaskState], Vector[(String, Output)])] =
    queue.dequeueOption match
      case None                 => IO.pure((states, out))
      case Some((record, rest)) =>
        val at = reading(record.topic)
        tasks(at).process(states(at), record, 0L).flatMap: (state, written) =>
          val read = written.filter(_.topic.value.endsWith("-repartition")).map(r => TaskInput(r.key, r.value, r.timestamp, r.headers, r.topic.value))
          runAll(tasks, reading, rest ++ read, states.updated(at, state), out ++ written.map(r => r.topic.value -> output(r)))

  private def bytes(text: String) = Chunk.array(text.getBytes)

  private def headers(input: Input) = Headers.empty.append(Header("seen", Some(Chunk.array(input.timestamp.toString.getBytes))))

  private def output(written: TaskOutput): Output =
    Output(
      written.key.map(_.toVector),
      written.value.map(_.toVector),
      written.timestamp,
      written.headers.values.toList.map(header => header.key -> header.value.map(_.toVector).getOrElse(Vector.empty))
    )

  private def javaRun(build: StreamsBuilder => Unit, outputs: List[String], inputs: List[Input]): Map[String, Vector[Output]] =
    val properties = Properties()
    val builder    = StreamsBuilder()
    build(builder)
    val driver = TopologyTestDriver(builder.build(properties), properties)
    try
      val topics = inputs.map(_.topic).distinct.map(name => name -> driver.createInputTopic(name, StringSerializer(), StringSerializer())).toMap
      inputs.foreach: input =>
        val headers = RecordHeaders().add("seen", input.timestamp.toString.getBytes)
        topics(input.topic).pipeInput(TestRecord(input.key, input.value.orNull, headers, java.lang.Long.valueOf(input.timestamp)))
      outputs.map: name =>
        val read = driver.createOutputTopic(name, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector
        name -> read.map: r =>
          val written = r.headers.toArray.toList.map(header => header.key -> Option(header.value).map(_.toVector).getOrElse(Vector.empty))
          Output(Option(r.key).map(_.toVector), Option(r.value).map(_.toVector), r.timestamp, written)
      .toMap
    finally driver.close()

  private def Properties() =
    val properties = java.util.Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-cogroup-golden").toString)
    properties

  private def stream(name: String)  = KStream(Named(name), topic(name), text, text.option)
  private def present(name: String) =
    stream(name).filter(Named(s"$name-present"))((_, v) => v.isDefined).mapValues(Named(s"$name-value"))(_.getOrElse(""))
  private def plain(name: String) = present(name).groupByKey(text, text)
  private def moved(name: String) = present(name).groupBy(Repartition(s"$name-r"), text, text)((_, v) => v)

  private def javaStream(builder: StreamsBuilder, name: String) =
    builder.stream[String, String](name, Consumed.as[String, String](name)).filter((_, v) => v != null, JavaNamed.as(s"$name-present"))
      .mapValues((v: String) => v, JavaNamed.as(s"$name-value"))
  private def javaPlain(builder: StreamsBuilder, name: String): JavaGrouped[String, String] =
    javaStream(builder, name).groupByKey(Grouped.`with`(s, s))
  private def javaMoved(builder: StreamsBuilder, name: String): JavaGrouped[String, String] =
    javaStream(builder, name).selectKey((_, v) => v, JavaNamed.as(s"$name-r")).groupByKey(Grouped.`with`(s"$name-r", s, s))

  private val changelog = "app-kept-changelog"

  private def tableToOut(cogrouped: KTable.Flowing[IO, String, String, KTable.Updated, Pipeline[IO]]): Pipeline[IO] =
    cogrouped.toStream(Named("changes")).to(Named("write"), topic("out"), text, text)

  private def javaToOut(cogrouped: JavaCogrouped[String, String]): Unit =
    cogrouped.aggregate(javaStart, JavaNamed.as("co"), stored).toStream(JavaNamed.as("changes")).to("out", Produced.`with`(s, s).withName("write"))

  private def aggregated(cogrouped: CogroupedKStream[IO, String, String, Pipeline[IO]]) =
    cogrouped.aggregate(Named("co"), Store("kept"), text)("start")

  private def two = plain("a").cogroup(tagged).cogroup(plain("b"))(marked)

  private def javaTwo(builder: StreamsBuilder) = javaPlain(builder, "a").cogroup(javaTagged).cogroup(javaPlain(builder, "b"), javaMarked)

  test("two groups fold into one store, each record by its own group's function, writing what Java writes to the store's changelog and the output"):
    same(tableToOut(aggregated(two)), builder => javaToOut(javaTwo(builder)), List("out", changelog))

  test("one group alone folds as Java's does"):
    same(
      tableToOut(aggregated(plain("a").cogroup(tagged))),
      builder => javaToOut(javaPlain(builder, "a").cogroup(javaTagged)),
      List("out", changelog),
      records.filter(_.topic == "a")
    )

  test("three groups fold into one store"):
    same(
      tableToOut(aggregated(two.cogroup(plain("c"))(dashed))),
      builder => javaToOut(javaTwo(builder).cogroup(javaPlain(builder, "c"), javaDashed)),
      List("out", changelog),
      records ++ List(Input("c", "k", Some("7"), 500L), Input("c", "z", Some("8"), 8000L))
    )

  test("groups of different value types fold by their own functions"):
    val lengths = stream("b").filter(Named("b-present"))((_, v) => v.isDefined).mapValues(Named("b-value"))(_.fold(0L)(_.length.toLong))
    same(
      lengths.groupByKey(text, Serde.long[IO]).cogroup[String]((_, n, total) => s"$total#$n").cogroup(plain("a"))(tagged)
        .aggregate(Named("co"), Store("kept"), text)("start").toStream(Named("changes")).to(Named("write"), topic("out"), text, text),
      builder =>
        val longs =
          builder.stream[String, String]("b", Consumed.as[String, String]("b")).filter((_, v) => v != null, JavaNamed.as("b-present"))
            .mapValues((v: String) => java.lang.Long.valueOf(v.length.toLong), JavaNamed.as("b-value")).groupByKey(Grouped.`with`(s, Serdes.Long))
        val sized: Aggregator[String, java.lang.Long, String] = (_, n, total) => s"$total#$n"
        javaToOut(longs.cogroup(sized).cogroup(javaPlain(builder, "a"), javaTagged))
      ,
      List("out", changelog)
    )

  test("one group through a repartition topic folds as Java's does"):
    same(
      tableToOut(aggregated(moved("a").cogroup(tagged))),
      builder => javaToOut(javaMoved(builder, "a").cogroup(javaTagged)),
      List("out", changelog, "app-a-r-repartition"),
      records.filter(_.topic == "a")
    )

  test("a repartitioned group and one read as it is fold as Java's do, either way round"):
    same(
      tableToOut(aggregated(moved("a").cogroup(tagged).cogroup(plain("b"))(marked))),
      builder => javaToOut(javaMoved(builder, "a").cogroup(javaTagged).cogroup(javaPlain(builder, "b"), javaMarked)),
      List("out", changelog, "app-a-r-repartition")
    ) *> same(
      tableToOut(aggregated(plain("a").cogroup(tagged).cogroup(moved("b"))(marked))),
      builder => javaToOut(javaPlain(builder, "a").cogroup(javaTagged).cogroup(javaMoved(builder, "b"), javaMarked)),
      List("out", changelog, "app-b-r-repartition")
    )

  test("two repartitioned groups fold as Java's do"):
    same(
      tableToOut(aggregated(moved("a").cogroup(tagged).cogroup(moved("b"))(marked))),
      builder => javaToOut(javaMoved(builder, "a").cogroup(javaTagged).cogroup(javaMoved(builder, "b"), javaMarked)),
      List("out", changelog, "app-a-r-repartition", "app-b-r-repartition")
    )

  test("two paths of one stream fold as two groups, in the order Java's forwards them"):
    val read =
      KStream(Named("a"), topic("a"), text, text.option).filter(Named("present"))((_, v) => v.isDefined).mapValues(Named("value"))(_.getOrElse(""))
    val low  = read.flow.filter(Named("low"))((_, v) => v < "4").groupByKey(text, text)
    val high = read.flow.filterNot(Named("high"))((_, v) => v < "3").groupByKey(text, text)
    same(
      read.into(
        low.cogroup(tagged).cogroup(high)(marked).aggregate(Named("co"), Store("kept"), text)("start").toStream(Named("changes"))
          .to(Named("write"), topic("out"), text, text)
      ),
      builder =>
        val java =
          builder.stream[String, String]("a", Consumed.as[String, String]("a")).filter((_, v) => v != null, JavaNamed.as("present"))
            .mapValues((v: String) => v, JavaNamed.as("value"))
        val low  = java.filter((_, v) => v < "4", JavaNamed.as("low")).groupByKey(Grouped.`with`(s, s))
        val high = java.filterNot((_, v) => v < "3", JavaNamed.as("high")).groupByKey(Grouped.`with`(s, s))
        javaToOut(low.cogroup(javaTagged).cogroup(high, javaMarked))
      ,
      List("out", changelog),
      records.filter(_.topic == "a")
    )

  test("the table a cogroup makes gives a filter, a stream join and a grouping what they ask for"):
    val orders = List(Input("o", "k", Some("x"), 500L), Input("o", "k", Some("y"), 9000L), Input("o", "z", Some("n"), 9100L))
    same(
      KStream(Named("o"), topic("o"), text, text).join(Named("joined"), aggregated(two))((o, t) => s"$o:$t")
        .to(Named("write"), topic("out"), text, text),
      builder =>
        val orders = builder.stream[String, String]("o", Consumed.as[String, String]("o"))
        orders.join(
          javaTwo(builder).aggregate(javaStart, JavaNamed.as("co"), stored),
          (o: String, t: String) => s"$o:$t",
          Joined.as[String, String, String]("joined")
        ).to("out", Produced.`with`(s, s).withName("write"))
      ,
      List("out"),
      records.take(4) ++ orders ++ records.drop(4)
    ) *> same(
      aggregated(two).filter(Named("long"))((_, t) => t.length > 9).toStream(Named("changes")).to(Named("write"), topic("out"), text, text.option),
      builder =>
        javaTwo(builder).aggregate(javaStart, JavaNamed.as("co"), stored).filter((_, t) => t.length > 9, JavaNamed.as("long"))
          .toStream(JavaNamed.as("changes")).to("out", Produced.`with`(s, s).withName("write")),
      List("out")
    ) *> same(
      aggregated(two).groupBy(Repartition("g"), text, text)((k, t) => (t.takeRight(1), k)).count(Named("counted"), Store("counts"))
        .toStream(Named("changes")).to(Named("write"), topic("out"), text, Serde.long[IO].option),
      builder =>
        javaTwo(builder).aggregate(javaStart, JavaNamed.as("co"), stored)
          .groupBy((k, t) => KeyValue.pair(t.takeRight(1), k), Grouped.`with`("g", s, s)).count(
            JavaNamed.as("counted"),
            Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("counts").withValueSerde(Serdes.Long)
          ).toStream(JavaNamed.as("changes")).to("out", Produced.`with`(s, Serdes.Long).withName("write")),
      List("out", "app-g-repartition")
    )

  private val windowedSerde = Serde.timeWindowed(text, 10.seconds)
  private val javaWindowed  = WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L)
  private def kept          = Materialized.as[String, String, WindowStore[Bytes, Array[Byte]]]("kept").withKeySerde(s).withValueSerde(s)
  private def session       = Materialized.as[String, String, SessionStore[Bytes, Array[Byte]]]("kept").withKeySerde(s).withValueSerde(s)

  // Each group's latest time differs, so a record can be late by one group's time and not by the other's, or the store's clock.
  private val lateness =
    List(
      Input("a", "k", Some("1"), 1000L),
      Input("b", "k", Some("2"), 2000L),
      Input("a", "k", Some("3"), 25000L),
      Input("b", "k", Some("4"), 3000L),
      Input("a", "k", Some("5"), 4000L),
      Input("b", "k", Some("6"), 40000L),
      Input("a", "k", Some("7"), 26000L),
      Input("b", "k", Some("8"), 5000L)
    )

  test("tumbling windows fold both groups, a record late by its own group's time being dropped and one late by the other's kept, as Java's do"):
    same(
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 2.seconds)).withRetention(1.minute).aggregate(Named("co"), Store("kept"), text)("start")
        .toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text),
      builder =>
        javaTwo(builder).windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept.withRetention(java.time.Duration.ofMinutes(1))).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out", changelog),
      lateness
    )

  test("windows expired by the store's clock are not kept, whichever group's record moved it, as Java's are not"):
    same(
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 2.seconds)).aggregate(Named("co"), Store("kept"), text)("start")
        .toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text),
      builder =>
        javaTwo(builder).windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out", changelog),
      lateness
    )

  test("hopping windows fold both groups as Java's do"):
    same(
      two.windowedBy(TimeWindows.hopping(10.seconds, 5.seconds, grace = 2.seconds)).withRetention(1.minute)
        .aggregate(Named("co"), Store("kept"), text)("start").toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text),
      builder =>
        javaTwo(builder).windowedBy(
          JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)).advanceBy(java.time.Duration.ofSeconds(5))
        ).aggregate(javaStart, JavaNamed.as("co"), kept.withRetention(java.time.Duration.ofMinutes(1))).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out", changelog),
      lateness
    )

  test("sliding windows fold both groups, each group's own time deciding what is late, as Java's do"):
    same(
      two.windowedBy(SlidingWindows(10.seconds, grace = 2.seconds)).withRetention(2.minutes).aggregate(Named("co"), Store("kept"), text)("start")
        .toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text),
      builder =>
        javaTwo(builder).windowedBy(JavaSlidingWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept.withRetention(java.time.Duration.ofMinutes(2))).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out", changelog),
      lateness
    )

  test("sessions fold both groups and merge as Java's do, each group's own time deciding what is late"):
    same(
      two.windowedBy(SessionWindows(5.seconds, grace = 2.seconds)).withRetention(2.minutes)
        .aggregate(Named("co"), Store("kept"), text)("start")(joining).toStream(Named("changes"))
        .to(Named("write"), topic("out"), Serde.sessionWindowed(text), text.option),
      builder =>
        javaTwo(builder).windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, javaJoining, JavaNamed.as("co"), session.withRetention(java.time.Duration.ofMinutes(2)))
          .toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), s).withName("write")),
      List("out", changelog),
      lateness ++ List(Input("a", "k", Some("9"), 28000L), Input("b", "k", Some("10"), 44000L), Input("b", "k", Some("11"), 42000L))
    )

  test("a suppressed windowed cogroup holds its results until the window closes, as Java's does"):
    same(
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 2.seconds)).withRetention(1.minute).aggregate(Named("co"), Store("kept"), text)("start")
        .suppress(Suppressed.untilWindowCloses("final")).toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text),
      builder =>
        javaTwo(builder).windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept.withRetention(java.time.Duration.ofMinutes(1)))
          .suppress(JavaSuppressed.untilWindowCloses(JavaSuppressed.BufferConfig.unbounded).withName("final")).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out"),
      lateness
    )

  test("a window's first change has the initial value as its old value, which a filter passing only that turns into a delete, as Java's does"):
    same(
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 2.seconds)).withRetention(1.minute).aggregate(Named("co"), Store("kept"), text)("start")
        .filter(Named("untouched"))((_, t) => t == "start").toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text.option),
      builder =>
        javaTwo(builder).windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept.withRetention(java.time.Duration.ofMinutes(1)))
          .filter((_, t) => t == "start", JavaNamed.as("untouched")).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out"),
      lateness
    )

  test("a suppressed time window cogroup waits out the grace, as Java's does"):
    same(
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 2.seconds)).withRetention(1.minute).aggregate(Named("co"), Store("kept"), text)("start")
        .suppress(Suppressed.untilWindowCloses("final")).toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text),
      builder =>
        javaTwo(builder).windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept.withRetention(java.time.Duration.ofMinutes(1)))
          .suppress(JavaSuppressed.untilWindowCloses(JavaSuppressed.BufferConfig.unbounded).withName("final")).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write")),
      List("out"),
      List(
        Input("a", "k", Some("1"), 1000L),
        Input("b", "k", Some("2"), 11000L),
        Input("a", "k", Some("3"), 9000L),
        Input("b", "k", Some("4"), 12500L)
      )
    )

  test("a suppressed session cogroup holds its results until the session closes, as Java's does"):
    same(
      two.windowedBy(SessionWindows(5.seconds, grace = 2.seconds)).withRetention(2.minutes)
        .aggregate(Named("co"), Store("kept"), text)("start")(joining).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("changes"))
        .to(Named("write"), topic("out"), Serde.sessionWindowed(text), text.option),
      builder =>
        javaTwo(builder).windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, javaJoining, JavaNamed.as("co"), session.withRetention(java.time.Duration.ofMinutes(2)))
          .suppress(JavaSuppressed.untilWindowCloses(JavaSuppressed.BufferConfig.unbounded).withName("final")).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), s).withName("write")),
      List("out"),
      lateness
    )

  private final case class Declared(text: String, pushed: ByteVector, topology: StreamsGroup.Topology)

  private def declared(pipeline: Pipeline[IO]): Declared =
    val topology = Topology.of(pipeline).orFail
    Declared(
      topology.describe.toString,
      StreamsGroupTopologyDescriptionUpdateRequest("app", "member", 3, Declaration.described("app", topology)).body,
      Declaration("app", topology)
    )

  private def javas(build: StreamsBuilder => Unit): Declared =
    val (topology, data) = JavaStreams.prepared("app")(build)
    val request          =
      new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("app").setMemberId("member").setTopologyEpoch(3)
        .setTopologyDescription(data.wireTopologyDescription())
    Declared(
      topology.describe().toString,
      ByteVector(MessageUtil.toByteBufferAccessor(request, 0.toShort).buffer()),
      TopologyGoldenSuite.declaredByJava(build)
    )

  test("a cogroup is described, pushed and declared as Java's, however many groups read their topics as they are"):
    assertEquals(
      declared(tableToOut(aggregated(plain("a").cogroup(tagged)))),
      javas(builder => javaToOut(javaPlain(builder, "a").cogroup(javaTagged)))
    )
    assertEquals(declared(tableToOut(aggregated(two))), javas(builder => javaToOut(javaTwo(builder))))
    assertEquals(
      declared(tableToOut(aggregated(two.cogroup(plain("c"))(dashed)))),
      javas(builder => javaToOut(javaTwo(builder).cogroup(javaPlain(builder, "c"), javaDashed)))
    )

  test("a cogroup of topics Java may list in another order says so as a join does, and others are declared as Java's"):
    def named(first: String, second: String) = tableToOut(aggregated(plain(first).cogroup(tagged).cogroup(plain(second))(marked)))
    assertEquals(Declaration.unordered("app", Topology.of(named("orders", "owners")).orFail), List(List("orders", "owners")))
    assertEquals(Declaration.unordered("app", Topology.of(named("orders", "users")).orFail), Nil)
    assertEquals(
      declared(named("orders", "users")),
      javas(builder => javaToOut(javaPlain(builder, "orders").cogroup(javaTagged).cogroup(javaPlain(builder, "users"), javaMarked)))
    )

  test("one group through a repartition topic has its first topic copartitioned where Java declares it, in the sub-topology before the repartition"):
    assertEquals(
      declared(tableToOut(aggregated(moved("a").cogroup(tagged)))),
      javas(builder => javaToOut(javaMoved(builder, "a").cogroup(javaTagged)))
    )

  test("windowed cogroups are described, pushed and declared as Java's"):
    assertEquals(
      declared(
        two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 2.seconds)).aggregate(Named("co"), Store("kept"), text)("start")
          .toStream(Named("changes")).to(Named("write"), topic("out"), windowedSerde, text)
      ),
      javas: builder =>
        javaTwo(builder).windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write"))
    )
    assertEquals(
      declared(
        two.windowedBy(SlidingWindows(10.seconds, grace = 2.seconds)).aggregate(Named("co"), Store("kept"), text)("start").toStream(Named("changes"))
          .to(Named("write"), topic("out"), windowedSerde, text)
      ),
      javas: builder =>
        javaTwo(builder).windowedBy(JavaSlidingWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, JavaNamed.as("co"), kept).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(javaWindowed, s).withName("write"))
    )
    assertEquals(
      declared(
        two.windowedBy(SessionWindows(5.seconds, grace = 2.seconds)).aggregate(Named("co"), Store("kept"), text)("start")(joining)
          .toStream(Named("changes")).to(Named("write"), topic("out"), Serde.sessionWindowed(text), text.option)
      ),
      javas: builder =>
        javaTwo(builder).windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)))
          .aggregate(javaStart, javaJoining, JavaNamed.as("co"), session).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), s).withName("write"))
    )

  test("a table a cogroup makes adds no topic to what a later join copartitions, as Java's adds none"):
    assertEquals(
      declared(
        KStream(Named("o"), topic("o"), text, text).join(Named("joined"), aggregated(two))((o, t) => s"$o:$t")
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders = builder.stream[String, String]("o", Consumed.as[String, String]("o"))
        orders.join(
          javaTwo(builder).aggregate(javaStart, JavaNamed.as("co"), stored),
          (o: String, t: String) => s"$o:$t",
          Joined.as[String, String, String]("joined")
        ).to("out", Produced.`with`(s, s).withName("write"))
    )

  private def describedByJava(build: StreamsBuilder => Unit): String =
    val builder = StreamsBuilder()
    build(builder)
    builder.build().describe().toString

  // Java cannot run these: it copartitions the groups' topics from before the repartition, which are in different sub-topologies, and refuses to start
  // under the streams protocol. If a Java release lifts this, these tests fail, and xkafka's declaration can be compared with Java's here.
  private def refusedByJava(build: StreamsBuilder => Unit) =
    val refused = intercept[java.lang.reflect.InvocationTargetException](JavaStreams.prepared("app")(build))
    assertEquals(refused.getCause.getMessage, "Not all copartition groups were converted to broker topology")

  private def groupOf(pipeline: Pipeline[IO]): List[List[List[String]]] =
    val topology = Topology.of(pipeline).orFail
    topology.subtopologies.map(_.copartitioned.map(_.map(Task.topicOf("app", _))))

  test("groups through repartition topics are numbered as Java describes them, and copartition the topics that meet, where Java cannot start"):
    val left                             = tableToOut(aggregated(moved("a").cogroup(tagged).cogroup(plain("b"))(marked)))
    val right                            = tableToOut(aggregated(plain("a").cogroup(tagged).cogroup(moved("b"))(marked)))
    val both                             = tableToOut(aggregated(moved("a").cogroup(tagged).cogroup(moved("b"))(marked)))
    val javaLeft: StreamsBuilder => Unit =
      builder => javaToOut(javaMoved(builder, "a").cogroup(javaTagged).cogroup(javaPlain(builder, "b"), javaMarked))
    val javaRight: StreamsBuilder => Unit =
      builder => javaToOut(javaPlain(builder, "a").cogroup(javaTagged).cogroup(javaMoved(builder, "b"), javaMarked))
    val javaBoth: StreamsBuilder => Unit =
      builder => javaToOut(javaMoved(builder, "a").cogroup(javaTagged).cogroup(javaMoved(builder, "b"), javaMarked))
    List(javaLeft, javaRight, javaBoth).foreach(refusedByJava)
    assertEquals(Topology.of(left).orFail.describe.toString, describedByJava(javaLeft))
    assertEquals(Topology.of(right).orFail.describe.toString, describedByJava(javaRight))
    assertEquals(Topology.of(both).orFail.describe.toString, describedByJava(javaBoth))
    assertEquals(groupOf(left), List(Nil, List(List("app-a-r-repartition", "b"))))
    assertEquals(groupOf(right), List(List(List("a", "app-b-r-repartition")), Nil))
    assertEquals(groupOf(both), List(Nil, Nil, List(List("app-a-r-repartition", "app-b-r-repartition"))))

  test("what Java cannot start is noted once, with the topics that meet, and nothing else is"):
    def noted(pipeline: Pipeline[IO]) =
      val subtopologies = Topology.of(pipeline).orFail.subtopologies
      (subtopologies.flatMap(_.unstartable.map(_.map(Task.topicOf("app", _)))), subtopologies.flatMap(_.buildOrdered))
    assertEquals(
      noted(tableToOut(aggregated(moved("a").cogroup(tagged).cogroup(moved("b"))(marked)))),
      (List(List("app-a-r-repartition", "app-b-r-repartition")), Nil)
    )
    assertEquals(noted(tableToOut(aggregated(moved("a").cogroup(tagged).cogroup(plain("b"))(marked)))), (List(List("app-a-r-repartition", "b")), Nil))
    assertEquals(noted(tableToOut(aggregated(plain("a").cogroup(tagged).cogroup(moved("b"))(marked)))), (List(List("a", "app-b-r-repartition")), Nil))
    assertEquals(noted(tableToOut(aggregated(two))), (Nil, Nil))
    assertEquals(noted(tableToOut(aggregated(moved("a").cogroup(tagged)))), (Nil, Nil))

object CogroupGoldenSuite:
  final case class Input(topic: String, key: String, value: Option[String], timestamp: Long)
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: List[(String, Vector[Byte])])
