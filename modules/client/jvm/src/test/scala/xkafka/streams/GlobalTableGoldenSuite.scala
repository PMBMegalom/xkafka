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
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.errors.TopologyException
import org.apache.kafka.streams.kstream.{Consumed, GlobalKTable as JavaGlobal, Grouped, Materialized, Named as JavaNamed, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import scodec.bits.ByteVector
import internal.driver.PolledRecord
import internal.protocol.{StreamsGroup, StreamsGroupTopologyDescriptionUpdateRequest}
import internal.streams.{Declaration, GlobalStores, Task, TaskInput, TaskOutput, TaskState}

/** Global tables, against the same programs in Java: the description and topology, and every record tasks write as the tables change, with Java's
  * record cache off.
  */
final class GlobalTableGoldenSuite extends CatsEffectSuite:
  import GlobalTableGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val s    = Serdes.String

  private def topic(name: String) = Topic.from(name).orFail

  private def stored(name: String) = Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]](name)

  private def users = GlobalKTable(Named("users"), topic("users"), text, text, Store("user-store"))

  private def javaUsers(builder: StreamsBuilder): JavaGlobal[String, String] =
    builder.globalTable[String, String]("users", Consumed.as[String, String]("users"), stored("user-store"))

  private def orders = KStream(Named("orders"), topic("orders"), text, text)

  private def javaOrders(builder: StreamsBuilder) = builder.stream[String, String]("orders", Consumed.as[String, String]("orders"))

  private def to = Produced.`with`(s, s).withName("write")

  private val records =
    List(
      Input("orders", Some("o1"), Some("alice"), 1000L),
      Input("users", Some("alice"), Some("Alice"), 900L),
      Input("orders", Some("o2"), Some("alice"), 2000L),
      Input("users", Some("alice"), Some("Alicia"), 5000L),
      Input("orders", Some("o3"), Some("alice"), 1500L),
      Input("users", Some("bob"), Some("Bob"), 100L),
      Input("users", None, Some("nobody"), 100L),
      Input("orders", Some("o4"), Some("bob"), 3000L),
      Input("users", Some("bob"), None, 200L),
      Input("users", Some("carol"), None, 200L),
      Input("orders", Some("o5"), Some("bob"), 3500L),
      Input("orders", Some("o6"), Some("carol"), 3600L)
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
    val tables   = topology.globals.map(_._2).map(global => global.topic.value -> global).toMap
    inputs.foldLeftM((Vector.fill(tasks.size)(TaskState.empty), Map.empty[String, internal.streams.StoreContents], Vector.empty[(String, Output)])):
      case ((states, held, out), input) => tables.get(input.topic) match
          case Some(global) =>
            val polled = PolledRecord(input.topic, 0, 0L, Some(input.timestamp), 0, -1, input.key.map(bytes), input.value.map(bytes), headers(input))
            GlobalStores.updated(global, held, polled).map(after => (states, after, out))
          case None =>
            val first = Queue(TaskInput(input.key.map(bytes), input.value.map(bytes), input.timestamp, headers(input), input.topic))
            runAll(tasks, reading, first, states, held, out).map((after, written) => (after, held, written))
    .map(_._3.groupMap(_._1)(_._2))

  private def runAll(
      tasks: Vector[Task[IO]],
      reading: Map[String, Int],
      queue: Queue[TaskInput],
      states: Vector[TaskState],
      held: GlobalStores.Contents,
      out: Vector[(String, Output)]
  ): IO[(Vector[TaskState], Vector[(String, Output)])] =
    queue.dequeueOption match
      case None                 => IO.pure((states, out))
      case Some((record, rest)) =>
        val at = reading(record.topic)
        tasks(at).process(states(at).copy(globals = held), record, 0L).flatMap: (state, written) =>
          val read = written.filter(_.topic.value.endsWith("-repartition")).map(r => TaskInput(r.key, r.value, r.timestamp, r.headers, r.topic.value))
          runAll(tasks, reading, rest ++ read, states.updated(at, state), held, out ++ written.map(r => r.topic.value -> output(r)))

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
        topics(input.topic).pipeInput(TestRecord(input.key.orNull, input.value.orNull, headers, java.lang.Long.valueOf(input.timestamp)))
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
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-global-golden").toString)
    properties

  test("a join reads the table as it stands when each record comes, a record it holds no value for being dropped, as Java's does"):
    same(
      orders.join(Named("named"), users)((_, user) => user)((order, user) => s"$order for $user").to(Named("write"), topic("out"), text, text),
      builder =>
        val table = javaUsers(builder)
        javaOrders(builder).join(table, (_, user) => user, (order: String, user: String) => s"$order for $user", JavaNamed.as("named")).to("out", to)
      ,
      List("out")
    )

  test("a left join passes none where the table holds no value, as Java's does"):
    same(
      orders.leftJoin(Named("named"), users)((_, user) => user)((order, user) => s"$order for ${user.getOrElse("no one")}")
        .to(Named("write"), topic("out"), text, text),
      builder =>
        val table = javaUsers(builder)
        javaOrders(builder).leftJoin(
          table,
          (_, user) => user,
          (order: String, user: String) => s"$order for ${Option(user).getOrElse("no one")}",
          JavaNamed.as("named")
        ).to("out", to)
      ,
      List("out")
    )

  test("a selected key written as none finds nothing, dropping the record from a join and passing none on in a left join, as Java's null key does"):
    val noneOnEmpty =
      Serde(
        Serializer
          .instance[IO, String]((read, written, value) => if value.isEmpty then IO.pure(None) else text.serializer.serialize(read, written, value)),
        text.deserializer
      )
    val table                = GlobalKTable(Named("users"), topic("users"), noneOnEmpty, text, Store("user-store"))
    val inputs               = records ++ List(Input("orders", Some("o7"), Some(""), 4000L))
    def nulled(user: String) = if user.isEmpty then null else user
    same(
      orders.join(Named("named"), table)((_, user) => user)((order, user) => s"$order for $user").to(Named("write"), topic("out"), text, text),
      builder =>
        val java = javaUsers(builder)
        javaOrders(builder).join(java, (_, user) => nulled(user), (order: String, user: String) => s"$order for $user", JavaNamed.as("named"))
          .to("out", to)
      ,
      List("out"),
      inputs
    ) *> same(
      orders.leftJoin(Named("named"), table)((_, user) => user)((order, user) => s"$order for ${user.getOrElse("no one")}")
        .to(Named("write"), topic("out"), text, text),
      builder =>
        val java = javaUsers(builder)
        javaOrders(builder).leftJoin(
          java,
          (_, user) => nulled(user),
          (order: String, user: String) => s"$order for ${Option(user).getOrElse("no one")}",
          JavaNamed.as("named")
        ).to("out", to)
      ,
      List("out"),
      inputs
    )

  test("the select step sees both the record's key and value"):
    same(
      orders.join(Named("named"), users)((key, user) => s"$user$key".dropRight(2))((order, user) => s"$order for $user")
        .to(Named("write"), topic("out"), text, text),
      builder =>
        val table = javaUsers(builder)
        javaOrders(builder)
          .join(table, (key, user) => s"$user$key".dropRight(2), (order: String, user: String) => s"$order for $user", JavaNamed.as("named"))
          .to("out", to)
      ,
      List("out")
    )

  test("a stream keeps its key through the join, so what follows groups it where it is, as Java's does"):
    same(
      orders.join(Named("named"), users)((_, user) => user)((_, user) => user).groupByKey(text, text).count(Named("counted"), Store("counts"))
        .toStream(Named("changes")).to(Named("write"), topic("out"), text, Serde.long[IO]),
      builder =>
        val table = javaUsers(builder)
        javaOrders(builder).join(table, (_, user) => user, (_: String, user: String) => user, JavaNamed.as("named")).groupByKey(Grouped.`with`(s, s))
          .count(
            JavaNamed.as("counted"),
            Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("counts").withValueSerde(Serdes.Long)
          ).toStream(JavaNamed.as("changes")).to("out", Produced.`with`(s, Serdes.Long).withName("write"))
      ,
      List("out", "app-counts-changelog")
    )

  private final case class Declared(text: String, pushed: ByteVector, topology: StreamsGroup.Topology)

  private def declared(pipelines: Pipeline[IO]*): Declared =
    val topology = Topology.of(pipelines.head, pipelines.tail*).orFail
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

  test("a global table is described, pushed and declared as Java's, numbered where a program built after the stream that joins it numbers it"):
    assertEquals(
      declared(orders.join(Named("j"), users)((_, u) => u)((o, u) => o + u).to(Named("write"), topic("out"), text, text)),
      javas: builder =>
        val read = javaOrders(builder)
        read.join(javaUsers(builder), (_, u) => u, (o: String, u: String) => o + u, JavaNamed.as("j")).to("out", to)
    )

  test("a program that builds its global table first numbers its sub-topologies otherwise, which is why the application's state warns of it"):
    val theirs =
      javas: builder =>
        val table = javaUsers(builder)
        javaOrders(builder).join(table, (_, u) => u, (o: String, u: String) => o + u, JavaNamed.as("j")).to("out", to)
    assertNotEquals(
      declared(orders.join(Named("j"), users)((_, u) => u)((o, u) => o + u).to(Named("write"), topic("out"), text, text)).topology,
      theirs.topology
    )
    assertEquals(theirs.topology.subtopologies.map(_.id), List("1"))

  test("two tables take their numbers where each is first joined, and sub-topologies after a repartition go on from them, as Java's do"):
    def second = GlobalKTable(Named("plans"), topic("plans"), text, text, Store("plan-store"))
    assertEquals(
      declared(
        orders.join(Named("j1"), users)((_, u) => u)((o, u) => o + u).join(Named("j2"), second)((_, v) => v)((o, p) => o + p)
          .groupBy(Repartition("again"), text, text)((_, v) => v).count(Named("counted"), Store("counts")).toStream(Named("changes"))
          .to(Named("write"), topic("out"), text, Serde.long[IO])
      ),
      javas: builder =>
        val read = javaOrders(builder)
        val one  = read.join(javaUsers(builder), (_, u) => u, (o: String, u: String) => o + u, JavaNamed.as("j1"))
        one.join(
          builder.globalTable[String, String]("plans", Consumed.as[String, String]("plans"), stored("plan-store")),
          (_, v) => v,
          (o: String, p: String) => o + p,
          JavaNamed.as("j2")
        ).groupBy((_, v) => v, Grouped.`with`("again", s, s)).count(
          JavaNamed.as("counted"),
          Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("counts").withValueSerde(Serdes.Long)
        ).toStream(JavaNamed.as("changes")).to("out", Produced.`with`(s, Serdes.Long).withName("write"))
    )

  test("one table joined by two streams appears once, as Java's does"):
    val table = users
    assertEquals(
      declared(
        orders.join(Named("j1"), table)((_, u) => u)((o, u) => o + u).to(Named("write"), topic("out"), text, text),
        KStream(Named("more"), topic("more"), text, text).leftJoin(Named("j2"), table)((_, u) => u)((o, u) => o + u.getOrElse(""))
          .to(Named("write-2"), topic("out-2"), text, text)
      ),
      javas: builder =>
        val read  = javaOrders(builder)
        val table = javaUsers(builder)
        read.join(table, (_, u) => u, (o: String, u: String) => o + u, JavaNamed.as("j1")).to("out", to)
        builder.stream[String, String]("more", Consumed.as[String, String]("more"))
          .leftJoin(table, (_, u) => u, (o: String, u: String) => o + Option(u).getOrElse(""), JavaNamed.as("j2"))
          .to("out-2", Produced.`with`(s, s).withName("write-2"))
    )

  test("a topic cannot be read both as a global table and as a stream, as Java refuses it"):
    val _ =
      intercept[TopologyException]:
        val builder = StreamsBuilder()
        val _       = javaUsers(builder)
        builder.stream[String, String]("users").to("out")
        builder.build()
    assertEquals(
      Topology.of(
        KStream(Named("read"), topic("users"), text, text).join(Named("j"), users)((_, u) => u)((o, u) => o + u)
          .to(Named("write"), topic("out"), text, text)
      ).left.toOption.map(_.toList),
      Some(List(TopologyError.DuplicateSource(topic("users"))))
    )

object GlobalTableGoldenSuite:
  final case class Input(topic: String, key: Option[String], value: Option[String], timestamp: Long)
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: List[(String, Vector[Byte])])
