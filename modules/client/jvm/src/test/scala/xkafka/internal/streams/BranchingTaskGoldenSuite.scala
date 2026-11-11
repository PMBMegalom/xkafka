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

import java.time.Duration
import java.util.Properties

import scala.collection.immutable.Queue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{
  Branched, Grouped, Joined, JoinWindows as JavaJoinWindows, KStream as JavaKStream, Materialized, Named as JavaNamed, Produced, StreamJoined
}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import xkafka.Serde
import xkafka.streams.{Flow, JoinWindows, KStream, KTable, Named, Pipeline, Repartition, Store, Topology}

/** What tasks write for a program that branches, against what Java Streams' `TopologyTestDriver` writes for the same program and records, with Java's
  * record cache off so that it, too, passes on every change. Each sub-topology's task reads what the others write to repartition topics, in the order
  * written, as the driver does.
  */
final class BranchingTaskGoldenSuite extends CatsEffectSuite:
  import BranchingTaskGoldenSuite.*

  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]

  private def topic(name: String) = Topic.from(name).orFail

  private val records =
    List(
      Input("in", "a", "x", 1000L),
      Input("in", "b", "yy", 3000L),
      Input("in", "a", "zzz", 2000L),
      Input("in", "c", "", 4000L),
      Input("in", "b", "wwww", 2500L)
    )

  private def stored[V](name: String) = Materialized.as[String, V, KeyValueStore[Bytes, Array[Byte]]](name)

  private def same(pipelines: List[Pipeline[IO]], java: StreamsBuilder => Unit, outputs: List[String], inputs: List[Input] = records): IO[Unit] =
    ours(pipelines, inputs).map: written =>
      val theirs = javaRun(java, outputs, inputs)
      assert(theirs.values.exists(_.nonEmpty), "Java wrote nothing, so there is nothing to compare")
      assertEquals(outputs.map(name => name -> written.getOrElse(name, Vector.empty)).toMap, theirs)

  /** Every record the program's tasks write outside repartition topics and changelogs, by topic in the order written. */
  private def ours(pipelines: List[Pipeline[IO]], inputs: List[Input]): IO[Map[String, Vector[Output]]] =
    val topology = Topology.of(pipelines.head, pipelines.tail*).orFail
    val tasks    = topology.subtopologies.toVector.map(Task[IO]("app", _, 0))
    val reading  = topology.subtopologies.zipWithIndex.flatMap((sub, id) => sub.origins.map(Task.topicOf("app", _) -> id)).toMap
    inputs.foldLeftM((Vector.fill(tasks.size)(TaskState.empty), Vector.empty[(String, Output)])):
      case ((states, out), input) =>
        val first = Queue(TaskInput(Some(bytes(input.key)), Some(bytes(input.value)), input.timestamp, Headers.empty, input.topic))
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
          val (again, onward) = written.partition(_.topic.value.endsWith("-repartition"))
          val kept            = onward.filterNot(_.topic.value.endsWith("-changelog"))
          val read            = again.map(r => TaskInput(r.key, r.value, r.timestamp, r.headers, r.topic.value))
          runAll(tasks, reading, rest ++ read, states.updated(at, state), out ++ kept.map(r => r.topic.value -> output(r)))

  private def bytes(text: String) = Chunk.array(text.getBytes)

  private def output(written: TaskOutput): Output = Output(written.key.map(k => String(k.toArray)), written.value.map(_.toVector), written.timestamp)

  private def javaRun(build: StreamsBuilder => Unit, outputs: List[String], inputs: List[Input]): Map[String, Vector[Output]] =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-branching-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val driver = TopologyTestDriver(builder.build(properties), properties)
    try
      val topics = inputs.map(_.topic).distinct.map(name => name -> driver.createInputTopic(name, StringSerializer(), StringSerializer())).toMap
      inputs.foreach(input => topics(input.topic).pipeInput(TestRecord(input.key, input.value, null, java.lang.Long.valueOf(input.timestamp))))
      outputs.map: name =>
        val read = driver.createOutputTopic(name, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector
        name -> read.map(r => Output(Option(r.key).map(String(_)), Option(r.value).map(_.toVector), r.timestamp))
      .toMap
    finally driver.close()

  test("each record goes down each path in turn, what each makes written in the order Java writes it"):
    same(
      List(KStream(Named("read"), topic("in"), text, text).fork(
        _.filter(Named("a"))((_, v) => v.length > 1).to(Named("w-a"), topic("a"), text, text),
        _.flatMapValues(Named("b"))(v => List(v, v + "!")).to(Named("w-b"), topic("b"), text, text),
        _.to(Named("w-c"), topic("c"), text, text)
      )),
      builder =>
        val in = builder.stream[String, String]("in")
        in.filter((_, v) => v.length > 1).to("a")
        in.flatMapValues((v: String) => java.util.List.of(v, v + "!")).to("b")
        in.to("c")
      ,
      List("a", "b", "c")
    )

  test("a stream of several topics passes on each record as it comes, sharing its store, its deserializer told the topic the record came from"):
    val inputs    = List(Input("one", "a", "x", 1000L), Input("two", "a", "y", 2000L), Input("three", "b", "z", 3000L), Input("one", "b", "w", 2500L))
    val fromTopic =
      Serde(
        text.serializer,
        Deserializer.instance[IO, String]((read, _, value) => IO.pure(s"${read.value}:${value.fold("")(v => String(v.toArray))}"))
      )
    val javaFromTopic =
      Serdes.serdeFrom(
        StringSerializer(),
        new org.apache.kafka.common.serialization.Deserializer[String]:
          override def deserialize(read: String, value: Array[Byte]): String = s"$read:${String(value)}"
      )
    same(
      List(
        KStream(Named("read"), NonEmptyList.of(topic("one"), topic("two"), topic("three")), text, fromTopic).groupByKey(text, text)
          .reduce(Named("joined"), Store("joined"))(_ + _).toStream(Named("changes")).to(Named("write"), topic("out"), text, text)
      ),
      builder =>
        builder
          .stream[String, String](List("one", "two", "three").asJava, org.apache.kafka.streams.kstream.Consumed.`with`(Serdes.String, javaFromTopic))
          .groupByKey(Grouped.`with`(Serdes.String, Serdes.String)).reduce((a: String, b: String) => a + b, stored[String]("joined")).toStream
          .to("out"),
      List("out"),
      inputs
    )

  test("each topic of a stream of several goes to what reads that stream, when it is not the first thing its sub-topology reads"):
    val inputs = List(Input("three", "a", "x", 1000L), Input("one", "a", "y", 2000L), Input("two", "b", "z", 3000L))
    same(
      List(
        KStream(Named("first"), topic("three"), text, text).mapValues(Named("mark-first"))(v => s"first:$v").merge(
          Named("merged"),
          KStream(Named("several"), NonEmptyList.of(topic("one"), topic("two")), text, text).mapValues(Named("mark"))(v => s"several:$v")
        ).to(Named("write"), topic("out"), text, text)
      ),
      builder =>
        builder.stream[String, String]("three").mapValues((v: String) => s"first:$v")
          .merge(builder.stream[String, String](List("one", "two").asJava).mapValues((v: String) => s"several:$v")).to("out"),
      List("out"),
      inputs
    )

  test("into sends each record to as many paths as it is given, in the order given"):
    val in                 = KStream(Named("read"), topic("in"), text, text)
    def path(name: String) = in.flow.mapValues(Named(name))(value => s"$name:$value").to(Named(s"write-$name"), topic("out"), text, text)
    val names              = List("p1", "p2", "p3", "p4", "p5", "p6")
    same(
      List(in.into(path("p1"), path("p2"), path("p3"), path("p4"), path("p5"), path("p6"))),
      builder =>
        val in = builder.stream[String, String]("in")
        names.foreach(name => in.mapValues((value: String) => s"$name:$value").to("out"))
      ,
      List("out")
    )

  test("into follows the order given whatever order the paths were written in, where Java follows the order they were written in"):
    val in     = KStream(Named("read"), topic("in"), text, text)
    val first  = in.flow.mapValues(Named("first"))(value => s"first:$value").to(Named("write-first"), topic("out"), text, text)
    val second = in.flow.mapValues(Named("second"))(value => s"second:$value").to(Named("write-second"), topic("out"), text, text)
    same(
      List(in.into(second, first)),
      builder =>
        val in = builder.stream[String, String]("in")
        in.mapValues((value: String) => s"second:$value").to("out")
        in.mapValues((value: String) => s"first:$value").to("out")
      ,
      List("out")
    )

  test("a stream joins a table made from a stream, or from two tables joined and kept in no store, as Java does"):
    val inputs =
      List(Input("names", "a", "ann", 1000L), Input("plans", "a", "gold", 1500L), Input("in", "a", "x", 2000L), Input("in", "b", "y", 2500L))
    val names  = KTable(Named("names"), topic("names"), text, text, Store("names"))
    val plans  = KStream(Named("plans"), topic("plans"), text, text).toTable(Named("plans-table"), Store("plans"), text, text)
    val joined = names.join(Named("accounts"), plans)((who, tier) => s"$who on $tier")
    val result = KStream(Named("in"), topic("in"), text, text).join(Named("with-account"), joined)((v, account) => s"$v for $account")
    same(
      List(result.to(Named("write"), topic("out"), text, text)),
      builder =>
        val names  = builder.table[String, String]("names", Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("names"))
        val plans  = builder.stream[String, String]("plans").toTable(JavaNamed.as("plans-table"), stored[String]("plans"))
        val joined = names.join(plans, (who: String, tier: String) => s"$who on $tier", JavaNamed.as("accounts"))
        builder.stream[String, String]("in").join(joined, (v: String, account: String) => s"$v for $account").to("out")
      ,
      List("out"),
      inputs
    )

  test("a fork that merges back writes each path's records, in the order Java forwards them"):
    same(
      List(
        KStream(Named("read"), topic("in"), text, text)
          .forkMerge(Named("back"))(_.mapValues(Named("a"))(v => s"a$v"), _.filter(Named("b"))((_, v) => v.length > 1))
          .to(Named("write"), topic("out"), text, text)
      ),
      builder =>
        val in = builder.stream[String, String]("in")
        in.mapValues((v: String) => s"a$v").merge(in.filter((_, v) => v.length > 1)).to("out")
      ,
      List("out")
    )

  test("two topics merged, each record of either going on"):
    same(
      List(
        KStream(Named("read-a"), topic("in"), text, text).merge(Named("all"), KStream(Named("read-b"), topic("other"), text, text))
          .to(Named("write"), topic("out"), text, text)
      ),
      builder => builder.stream[String, String]("in").merge(builder.stream[String, String]("other")).to("out"),
      List("out"),
      records ++ List(Input("other", "a", "o", 5000L), Input("in", "a", "after", 6000L))
    )

  test("a record goes down the first branch of a split that takes it, and none if no branch does"):
    same(
      List(
        KStream(Named("read"), topic("in"), text, text).split(Named("route-"))
          .branch(Named("big"))((_, v) => v.length > 2)(_.to(Named("w-big"), topic("big"), text, text))
          .branch(Named("mid"))((_, v) => v.length > 1)(_.to(Named("w-mid"), topic("mid"), text, text)).noDefaultBranch
      ),
      builder =>
        val _ =
          builder.stream[String, String]("in").split(JavaNamed.as("route-"))
            .branch((_, v) => v.length > 2, Branched.withConsumer((b: JavaKStream[String, String]) => b.to("big"), "big"))
            .branch((_, v) => v.length > 1, Branched.withConsumer((b: JavaKStream[String, String]) => b.to("mid"), "mid")).noDefaultBranch()
      ,
      List("big", "mid")
    )

  test("the records no branch of a split takes go down its default"):
    same(
      List(
        KStream(Named("read"), topic("in"), text, text).split(Named("route-"))
          .branch(Named("big"))((_, v) => v.length > 2)(_.to(Named("w-big"), topic("big"), text, text))
          .defaultBranch(Named("rest"))(_.to(Named("w-rest"), topic("rest"), text, text))
      ),
      builder =>
        val _ =
          builder.stream[String, String]("in").split(JavaNamed.as("route-"))
            .branch((_, v) => v.length > 2, Branched.withConsumer((b: JavaKStream[String, String]) => b.to("big"), "big"))
            .defaultBranch(Branched.withConsumer((b: JavaKStream[String, String]) => b.to("rest"), "rest"))
      ,
      List("big", "rest")
    )

  test("a stream enriched with its own count reads the count as Java does, the table's path coming first"):
    val clicks = KStream(Named("read"), topic("in"), text, text)
    val counts = clicks.flow.groupByKey(text, text).count(Named("counted"), Store("counts"))
    same(
      List(clicks.into(counts.consumer, clicks.flow.join(Named("enrich"), counts)((v, n) => s"$v#$n").to(Named("write"), topic("out"), text, text))),
      builder =>
        val in     = builder.stream[String, String]("in")
        val counts = in.groupByKey.count(stored[java.lang.Long]("counts").withValueSerde(Serdes.Long))
        in.join(counts, (v: String, n: java.lang.Long) => s"$v#$n", Joined.`with`(Serdes.String, Serdes.String, Serdes.Long)).to("out")
      ,
      List("out")
    )

  test("a stream joined with itself in a window joins each record with those before it and itself, as Java does"):
    val in = KStream(Named("read"), topic("in"), text, text)
    same(
      List(in.into(
        in.flow.join(Named("pairs"), in.flow, JoinWindows(5.seconds, grace = 1.second), Store("pairs"), text, text, text)((a, b) => s"$a+$b")
          .to(Named("write"), topic("out"), text, text)
      )),
      builder =>
        val in = builder.stream[String, String]("in")
        in.join(
          in,
          (a: String, b: String) => s"$a+$b",
          JavaJoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)),
          StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
        ).to("out")
      ,
      List("out")
    )

  test("a table joined with itself sends each change through both sides, as Java does"):
    val table = KTable(Named("users"), topic("in"), text, text, Store("users"))
    same(
      List(
        table.into(table.flow.join(Named("self"), table.flow)(_ + _).toStream(Named("changes")).to(Named("write"), topic("out"), text, text.option))
      ),
      builder =>
        val table = builder.table[String, String]("in", stored[String]("users"))
        table.join(table, (a: String, b: String) => a + b).toStream.to("out")
      ,
      List("out")
    )

  test("a table that forks gives each path's mapValues and filter the old values any path asks for, and a join reads through them"):
    val table = KTable(Named("users"), topic("in"), text, text, Store("users"))
    same(
      List(table.fork(
        _.mapValues(Named("lengths"))(_.length).filter(Named("long"))((_, n) => n > 1).toStream(Named("t-long"))
          .mapValues(Named("shown"))(_.map(_.toString).getOrElse("gone")).to(Named("w-long"), topic("long"), text, text),
        _.toStream(Named("t-all")).mapValues(Named("shown-all"))(_.getOrElse("gone")).to(Named("w-all"), topic("all"), text, text)
      )),
      builder =>
        val table = builder.table[String, String]("in", stored[String]("users"))
        table.mapValues((v: String) => java.lang.Integer.valueOf(v.length)).filter((_, n) => n > 1).toStream
          .mapValues((n: java.lang.Integer) => Option(n).map(_.toString).getOrElse("gone")).to("long")
        table.toStream.mapValues((v: String) => Option(v).getOrElse("gone")).to("all")
      ,
      List("long", "all")
    )

  test("each of a stream's paths counted through a repartition topic of its own writes what Java writes"):
    same(
      List(KStream(Named("read"), topic("in"), text, text).fork(
        _.groupBy(Repartition("r-1"), text, text)((_, v) => v.take(1)).count(Named("c-1"), Store("s-1")).toStream(Named("t-1"))
          .to(Named("w-1"), topic("o-1"), text, count),
        _.groupBy(Repartition("r-2"), text, text)((_, v) => v.length.toString).count(Named("c-2"), Store("s-2")).toStream(Named("t-2"))
          .to(Named("w-2"), topic("o-2"), text, count)
      )),
      builder =>
        val in = builder.stream[String, String]("in")
        in.groupBy((_, v) => v.take(1), Grouped.as("r-1")).count(stored[java.lang.Long]("s-1").withValueSerde(Serdes.Long)).toStream
          .to("o-1", Produced.`with`(Serdes.String, Serdes.Long))
        in.groupBy((_, v) => v.length.toString, Grouped.as("r-2")).count(stored[java.lang.Long]("s-2").withValueSerde(Serdes.Long)).toStream
          .to("o-2", Produced.`with`(Serdes.String, Serdes.Long))
      ,
      List("o-1", "o-2")
    )

  test("records of a flow two paths feed go through its repartition topic from both"):
    val counted =
      Flow[IO, String, String].groupByKey(Repartition("r"), text, text).count(Named("counted"), Store("counts")).toStream(Named("changes"))
        .to(Named("write"), topic("out"), text, count)
    same(
      List(
        KStream(Named("read"), topic("in"), text, text)
          .fork(_.selectKey(Named("by-value"))((_, v) => v).into(counted), _.mapValues(Named("same-key"))(_.toUpperCase).into(counted))
      ),
      builder =>
        val in = builder.stream[String, String]("in")
        in.selectKey((_, v) => v).merge(in.mapValues((v: String) => v.toUpperCase)).groupByKey(Grouped.as("r"))
          .count(stored[java.lang.Long]("counts").withValueSerde(Serdes.Long)).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      ,
      List("out")
    )

object BranchingTaskGoldenSuite:
  final case class Input(topic: String, key: String, value: String, timestamp: Long)
  final case class Output(key: Option[String], value: Option[Vector[Byte]], timestamp: Long)
