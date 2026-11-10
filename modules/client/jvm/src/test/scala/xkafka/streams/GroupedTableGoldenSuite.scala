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
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{
  Consumed, Grouped, Materialized, Named as JavaNamed, Produced, Suppressed as JavaSuppressed, TimeWindows as JavaTimeWindows
}
import org.apache.kafka.streams.kstream.internals.{Change, ChangedDeserializer, ChangedSerializer}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import scodec.bits.ByteVector
import internal.protocol.{StreamsGroup, StreamsGroupTopologyDescriptionUpdateRequest}
import internal.streams.{Declaration, Task, TaskInput, TaskOutput, TaskState}

/** Tables grouped anew, against the same programs in Java: the description and topology, what the repartition topic holds, and every record tasks
  * write, to its repartition topic and its changelog too, with Java's record cache off so that it, too, passes on every change.
  */
final class GroupedTableGoldenSuite extends CatsEffectSuite:
  import GroupedTableGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val long = Serde.long[IO]
  private val s    = Serdes.String
  private val l    = Serdes.Long

  private def topic(name: String) = Topic.from(name).orFail

  private def stored[V](name: String) = Materialized.as[String, V, KeyValueStore[Bytes, Array[Byte]]](name)

  private def users = KTable(Named("users"), topic("in"), text, text, Store("users"))

  private def javaUsers(builder: StreamsBuilder) = builder.table[String, String]("in", Consumed.as[String, String]("users"), stored[String]("users"))

  private def group(value: String)  = value.takeWhile(_ != ':')
  private def amount(value: String) = value.dropWhile(_ != ':').drop(1)

  private val changes =
    List(
      Input("in", "a", Some("x:1"), 1000L),
      Input("in", "b", Some("x:2"), 3000L),
      Input("in", "a", Some("x:30"), 2000L),
      Input("in", "a", Some("y:4"), 1500L),
      Input("in", "c", Some("y:5"), 4000L),
      Input("in", "b", None, 2500L),
      Input("in", "d", None, 5000L),
      Input("in", "a", None, 6000L),
      Input("in", "c", Some("x:5"), 7000L),
      Input("in", "b", Some("y:6"), 3500L),
      Input("in", "b", Some("y:6"), 3600L),
      Input("in", "c", None, 8000L)
    )

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, outputs: List[String], inputs: List[Input] = changes): IO[Unit] =
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
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-grouped-golden").toString)
    properties

  private val internal = List("app-g-repartition", "app-kept-changelog")

  private def javaGrouped(builder: StreamsBuilder) =
    javaUsers(builder).groupBy((_, v) => KeyValue.pair(group(v), amount(v)), Grouped.`with`("g", s, s))

  private def sum(a: String, b: String)  = (a.toLong + b.toLong).toString
  private def less(a: String, b: String) = (a.toLong - b.toLong).toString

  private def grouped = users.groupBy(Repartition("g"), text, text)((_, v) => (group(v), amount(v)))

  test("a count of a table's groups follows values appearing, moving between groups and being deleted, writing what Java writes to each topic"):
    same(
      grouped.count(Named("counted"), Store("kept")).toStream(Named("changes")).to(Named("write"), topic("out"), text, long.option),
      builder =>
        javaGrouped(builder).count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream
          .to("out", Produced.`with`(s, l)),
      "out" :: internal
    )

  test("a reduction of a table's groups takes out each value leaving and puts in each value joining"):
    same(
      grouped.reduce(Named("summed"), Store("kept"))(sum)(less).toStream(Named("changes")).to(Named("write"), topic("out"), text, text.option),
      builder =>
        javaGrouped(builder).reduce((a, b) => sum(a, b), (a, b) => less(a, b), JavaNamed.as("summed"), stored[String]("kept")).toStream.to("out"),
      "out" :: internal
    )

  test("an aggregate of a table's groups starts from its initial value, and a group that loses its last value keeps what is left"):
    same(
      grouped.aggregate(Named("listed"), Store("kept"), text)("start")((k, v, acc) => s"$acc+$k$v")((k, v, acc) => s"$acc-$k$v")
        .toStream(Named("changes")).to(Named("write"), topic("out"), text, text.option),
      builder =>
        javaGrouped(builder).aggregate(
          () => "start",
          (k, v, acc) => s"$acc+$k$v",
          (k, v, acc) => s"$acc-$k$v",
          JavaNamed.as("listed"),
          stored[String]("kept").withValueSerde(s)
        ).toStream.to("out"),
      "out" :: internal
    )

  test("values of any length go through the repartition topic, the joining value's length written as Java writes it"):
    val long1  = "1" * 63
    val long2  = "2" * 64
    val long3  = "3" * 8191
    val long4  = "4" * 8192
    val long5  = "5" * 70000
    val inputs = List("", long1, long2, long3, long4, long5, "x").zipWithIndex.map((v, i) => Input("in", "a", Some(v), 1000L + i))
    same(
      users.groupBy(Repartition("g"), text, text)((_, _) => ("all", "")).count(Named("no-values"), Store("first")).consumer,
      builder =>
        val _ =
          javaUsers(builder).groupBy((_, _) => KeyValue.pair("all", ""), Grouped.`with`("g", s, s))
            .count(JavaNamed.as("no-values"), stored[java.lang.Long]("first").withValueSerde(l))
      ,
      List("app-g-repartition"),
      inputs
    ) *> same(
      users.groupBy(Repartition("g"), text, text)((_, v) => ("all", v)).reduce(Named("last"), Store("kept"))((_, b) => b)((a, _) => a).consumer,
      builder =>
        val _ =
          javaUsers(builder).groupBy((_, v) => KeyValue.pair("all", v), Grouped.`with`("g", s, s))
            .reduce((_, b) => b, (a, _) => a, JavaNamed.as("last"), stored[String]("kept"))
      ,
      internal,
      inputs
    )

  test("a repartition other than a grouped table's passes on a record without a value, as Java does"):
    val inputs = List(Input("in", "a", Some("x"), 1000L), Input("in", "b", None, 2000L))
    same(
      KStream(Named("read"), topic("in"), text, text.option).selectKey(Named("moved"))((k, _) => s"$k!")
        .repartition(Repartition("g"), text, text.option).to(Named("write"), topic("out"), text, text.option),
      builder =>
        builder.stream[String, String]("in").selectKey((k, _) => s"$k!").repartition(org.apache.kafka.streams.kstream.Repartitioned.as("g"))
          .to("out"),
      List("out", "app-g-repartition"),
      inputs
    )

  test("a table's filter and mapValues, kept in no store, give the groupBy the old values it asks for"):
    same(
      users.filter(Named("big"))((_, v) => amount(v).length < 2).mapValues(Named("marked"))(v => s"$v!")
        .groupBy(Repartition("g"), text, text)((_, v) => (group(v), v)).count(Named("counted"), Store("kept")).toStream(Named("changes"))
        .to(Named("write"), topic("out"), text, long.option),
      builder =>
        javaUsers(builder).filter((_, v) => amount(v).length < 2).mapValues((v: String) => s"$v!")
          .groupBy((_, v) => KeyValue.pair(group(v), v), Grouped.`with`("g", s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream.to("out", Produced.`with`(s, l)),
      "out" :: internal
    )

  test("a table's mapValues kept in a store gives the groupBy the old values it kept"):
    same(
      users.mapValues(Named("marked"), Store("marks"), text)(v => s"$v!").groupBy(Repartition("g"), text, text)((_, v) => (group(v), v))
        .count(Named("counted"), Store("kept")).toStream(Named("changes")).to(Named("write"), topic("out"), text, long.option),
      builder =>
        javaUsers(builder).mapValues((v: String) => s"$v!", JavaNamed.as("marked"), stored[String]("marks"))
          .groupBy((_, v) => KeyValue.pair(group(v), v), Grouped.`with`("g", s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream.to("out", Produced.`with`(s, l)),
      "out" :: internal
    )

  test("a table made from a stream, then grouped, follows its changes"):
    same(
      KStream(Named("read"), topic("in"), text, text.option).toTable[String](Named("t"), Store("t"), text, text)
        .groupBy(Repartition("g"), text, text)((_, v) => (group(v), v)).count(Named("counted"), Store("kept")).toStream(Named("changes"))
        .to(Named("write"), topic("out"), text, long.option),
      builder =>
        builder.stream[String, String]("in").toTable(JavaNamed.as("t"), stored[String]("t"))
          .groupBy((_, v) => KeyValue.pair(group(v), v), Grouped.`with`("g", s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream.to("out", Produced.`with`(s, l)),
      "out" :: internal
    )

  test("a count grouped by its value, as a histogram, moves each key from one count to the next"):
    val inputs = List("a", "b", "a", "c", "a", "b").zipWithIndex.map((key, i) => Input("in", key, Some("v"), 1000L + i))
    same(
      KStream(Named("read"), topic("in"), text, text).groupByKey(text, text).count(Named("per-key"), Store("per-key"))
        .groupBy(Repartition("g"), long, text)((k, n) => (n, k)).count(Named("counted"), Store("kept")).toStream(Named("changes"))
        .to(Named("write"), topic("out"), long, long.option),
      builder =>
        builder.stream[String, String]("in").groupByKey().count(JavaNamed.as("per-key"), stored[java.lang.Long]("per-key").withValueSerde(l))
          .groupBy((k, n) => KeyValue.pair(n, k), Grouped.`with`("g", l, s)).count(
            JavaNamed.as("counted"),
            Materialized.as[java.lang.Long, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("kept").withValueSerde(l).withKeySerde(l)
          ).toStream.to("out", Produced.`with`(l, l)),
      "out" :: internal,
      inputs
    )

  test("two tables joined and kept in no store, then grouped, take their old values from the join"):
    val inputs =
      List(
        Input("in", "a", Some("x:1"), 1000L),
        Input("other", "a", Some("p"), 2000L),
        Input("in", "a", Some("y:2"), 3000L),
        Input("other", "a", Some("q"), 4000L),
        Input("other", "a", None, 5000L)
      )
    val other = KTable(Named("others"), topic("other"), text, text, Store("others"))
    same(
      users.join(Named("joined"), other)((a, b) => s"$a$b").groupBy(Repartition("g"), text, text)((_, v) => (group(v), v))
        .count(Named("counted"), Store("kept")).toStream(Named("changes")).to(Named("write"), topic("out"), text, long.option),
      builder =>
        javaUsers(builder)
          .join(builder.table[String, String]("other", stored[String]("others")), (a: String, b: String) => s"$a$b", JavaNamed.as("joined"))
          .groupBy((_, v) => KeyValue.pair(group(v), v), Grouped.`with`("g", s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream.to("out", Produced.`with`(s, l)),
      "out" :: internal,
      inputs
    )

  test("a windowed table grouped by its key takes each window's change out and puts it in"):
    val inputs =
      List(
        Input("in", "a", Some("1"), 1000L),
        Input("in", "a", Some("2"), 2000L),
        Input("in", "a", Some("3"), 61000L),
        Input("in", "b", Some("4"), 61500L)
      )
    same(
      KStream(Named("read"), topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumblingWithNoGrace(1.minute))
        .count(Named("per-window"), Store("per-window")).groupBy(Repartition("g"), text, long)((w, n) => (w.key, n))
        .reduce(Named("summed"), Store("kept"))(_ + _)(_ - _).toStream(Named("changes")).to(Named("write"), topic("out"), text, long.option),
      builder =>
        builder.stream[String, String]("in").groupByKey().windowedBy(JavaTimeWindows.ofSizeWithNoGrace(java.time.Duration.ofMinutes(1))).count(
          JavaNamed.as("per-window"),
          Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.WindowStore[Bytes, Array[Byte]]]("per-window").withValueSerde(l)
        ).groupBy((w, n) => KeyValue.pair(w.key, n), Grouped.`with`("g", s, l))
          .reduce((a, b) => a + b, (a, b) => a - b, JavaNamed.as("summed"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream
          .to("out", Produced.`with`(s, l)),
      "out" :: internal,
      inputs
    )

  test("what reads a grouped table is given its old values: its filter, its stream join, and a second group"):
    same(
      grouped.count(Named("counted"), Store("kept")).filter(Named("many"))((_, n) => n > 1).toStream(Named("changes"))
        .mapValues(Named("shown"))(_.fold("none")(_.toString)).to(Named("write"), topic("out"), text, text),
      builder =>
        javaGrouped(builder).count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).filter((_, n) => n > 1).toStream
          .mapValues((n: java.lang.Long) => Option(n).fold("none")(_.toString)).to("out"),
      "out" :: internal
    ) *> same(
      grouped.count(Named("counted"), Store("kept")).groupBy(Repartition("second"), long, text)((k, n) => (n, k))
        .count(Named("again"), Store("again")).toStream(Named("changes")).to(Named("write"), topic("out"), long, long.option),
      builder =>
        javaGrouped(builder).count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l))
          .groupBy((k, n) => KeyValue.pair(n, k), Grouped.`with`("second", l, s)).count(
            JavaNamed.as("again"),
            Materialized.as[java.lang.Long, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("again").withValueSerde(l).withKeySerde(l)
          ).toStream.to("out", Produced.`with`(l, l)),
      List("out", "app-second-repartition", "app-again-changelog")
    )

  test("a suppressed table grouped takes the old values its buffer passes on"):
    val inputs =
      List(
        Input("in", "a", Some("x:1"), 1000L),
        Input("in", "a", Some("y:2"), 1500L),
        Input("in", "b", Some("x:3"), 5000L),
        Input("in", "a", Some("x:4"), 9000L)
      )
    same(
      users.suppress(Suppressed.untilTimeLimit("held", 2.seconds)).groupBy(Repartition("g"), text, text)((_, v) => (group(v), v))
        .count(Named("counted"), Store("kept")).toStream(Named("changes")).to(Named("write"), topic("out"), text, long.option),
      builder =>
        javaUsers(builder)
          .suppress(JavaSuppressed.untilTimeLimit(java.time.Duration.ofSeconds(2), JavaSuppressed.BufferConfig.unbounded).withName("held"))
          .groupBy((_, v) => KeyValue.pair(group(v), v), Grouped.`with`("g", s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream.to("out", Produced.`with`(s, l)),
      "out" :: internal,
      inputs
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

  test("a grouped table is described, pushed and declared as Java's, its sink and source named after the aggregate"):
    assertEquals(
      declared(grouped.count(Named("counted"), Store("kept")).toStream(Named("changes")).to(Named("write"), topic("out"), text, long.option)),
      javas: builder =>
        javaGrouped(builder).count(JavaNamed.as("counted"), stored[java.lang.Long]("kept").withValueSerde(l)).toStream(JavaNamed.as("changes"))
          .to("out", Produced.`with`(s, l).withName("write"))
    )

  test("a table joined after being grouped has its sub-topologies numbered as Java's"):
    assertEquals(
      declared(
        KStream(Named("orders"), topic("orders"), text, text)
          .join(Named("enriched"), grouped.reduce(Named("summed"), Store("kept"))(sum)(less))((o, n) => s"$o$n")
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders = builder.stream[String, String]("orders", Consumed.as[String, String]("orders"))
        val table  = javaGrouped(builder).reduce((a, b) => sum(a, b), (a, b) => less(a, b), JavaNamed.as("summed"), stored[String]("kept"))
        orders.join(table, (o: String, n: String) => s"$o$n", org.apache.kafka.streams.kstream.Joined.as[String, String, String]("enriched"))
          .to("out", Produced.`with`(s, s).withName("write"))
    )

  private val changeSerde = TableChange.serde(text)
  private val at          = topic("t")

  private val javaWriter = ChangedSerializer[String](StringSerializer())
  private val javaReader = ChangedDeserializer[String](StringDeserializer())

  private val samples = List("", "x", "y" * 63, "y" * 64, "z" * 8191, "z" * 8192, "w" * 1048575, "w" * 1048576)

  private def eachChange: List[(Option[String], Option[String])] =
    samples.map(v => (Some(v), None)) ++ samples.map(v => (None, Some(v))) ++
      samples.flatMap(a => List("", "o", "p" * 100).map(b => (Some(a), Some(b))))

  test("a change is written byte for byte as Java's ChangedSerializer writes it"):
    eachChange.traverse_ : (current, old) =>
      changeSerde.serializer.serialize(at, Headers.empty, TableChange(current, old)).map: written =>
        assertEquals(written.map(_.toVector), Some(javaWriter.serialize("t", RecordHeaders(), Change[String](current.orNull, old.orNull)).toVector))

  test("a change Java's ChangedSerializer wrote reads back, and one written here reads back in Java"):
    eachChange.traverse_ : (current, old) =>
      val java = Chunk.array(javaWriter.serialize("t", RecordHeaders(), Change[String](current.orNull, old.orNull)))
      changeSerde.deserializer.deserialize(at, Headers.empty, Some(java)).map: read =>
        assertEquals(read, TableChange(current, old))
        val back = javaReader.deserialize("t", RecordHeaders(), java.toArray)
        assertEquals((Option(back.newValue), Option(back.oldValue)), (current, old))

  private def withLatest(java: Array[Byte], flag: Byte, latest: Byte) = Chunk.array(java.init) ++ Chunk(latest, flag)

  test("a change with the flags saying if it is the latest, which Java's ChangedDeserializer also reads, reads the same"):
    eachChange.traverse_ : (current, old) =>
      val java = javaWriter.serialize("t", RecordHeaders(), Change[String](current.orNull, old.orNull))
      List[Byte](0, 1).traverse_ : latest =>
        val flagged = withLatest(java, (java.last + 3).toByte, latest)
        val theirs  = javaReader.deserialize("t", RecordHeaders(), flagged.toArray)
        changeSerde.deserializer.deserialize(at, Headers.empty, Some(flagged)).map: read =>
          assertEquals((read.current, read.old), (Option(theirs.newValue), Option(theirs.oldValue)))

  test("a record that is no change is refused, as Java refuses it"):
    def bad(record: Option[Chunk[Byte]]) =
      changeSerde.deserializer.deserialize(at, Headers.empty, record).attempt.map(result => assert(result.isLeft, result))
    bad(None) *> bad(Some(Chunk.empty)) *> bad(Some(Chunk[Byte](6))) *> bad(Some(Chunk[Byte](-1))) *> bad(Some(Chunk[Byte](3))) *>
      bad(Some(Chunk[Byte](4))) *> bad(Some(Chunk[Byte](5))) *> bad(Some(Chunk[Byte](2))) *> bad(Some(Chunk[Byte](100, 2))) *>
      bad(Some(Chunk[Byte](1, 2))) *> bad(Some(Chunk[Byte](-128, -128, -128, -128, -128, 2))) *>
      bad(Some(Chunk[Byte](-128, -128, -128, -128, -128, 0, 97, 2)))

  test("a change with neither value is written as none, so no record is written for a group of a value its serializer writes as none"):
    val maybe = Serde.utf8[IO].contramapNoneOnEmpty
    val tasks =
      Topology.of(users.groupBy(Repartition("g"), text, maybe)((_, v) => (group(v), amount(v))).count(Named("counted"), Store("kept")).consumer)
        .orFail.subtopologies
    val first = Task[IO]("app", tasks.head, 0)
    val input = TaskInput(Some(bytes("a")), Some(bytes("x:")), 1000L, Headers.empty, "in")
    first.process(TaskState.empty, input, 0L).map((_, written) => assertEquals(written.filter(_.topic.value.endsWith("-repartition")), Vector.empty))

object GroupedTableGoldenSuite:
  final case class Input(topic: String, key: String, value: Option[String], timestamp: Long)
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: List[(String, Vector[Byte])])

  extension (serde: Serde[IO, String])
    /** Text that is written as none where it is empty. */
    def contramapNoneOnEmpty: Serde[IO, String] =
      Serde(
        Serializer.instance((read, written, value) => if value.isEmpty then IO.pure(None) else serde.serializer.serialize(read, written, value)),
        serde.deserializer
      )
