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
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Named as JavaNamed, Produced}
import org.apache.kafka.streams.processor.api.{
  FixedKeyProcessor as JavaFixedKeyProcessor, FixedKeyProcessorContext, FixedKeyProcessorSupplier, FixedKeyRecord as JavaFixedKeyRecord,
  Processor as JavaProcessor, ProcessorContext, ProcessorSupplier, Record as JavaRecord
}
import org.apache.kafka.streams.state.{KeyValueStore as JavaStore, Stores}
import org.apache.kafka.streams.test.TestRecord
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What a processor writes, logs and keeps, and how it is described, against what Java's `Processor` does for the same logic and records, through
  * Java Streams' `TopologyTestDriver`.
  */
final class ProcessorGoldenSuite extends CatsEffectSuite:
  import ProcessorGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  private val inputs =
    List(
      Input("a", "x", 1000L, List("trace" -> "1")),
      Input("é", "y", 3000L, Nil),
      Input("a", "z", 2000L, List("trace" -> "3")),
      Input("ab", "", 4000L, Nil),
      Input("é", "x", 2500L, List("trace" -> "5", "trace" -> "6"))
    )

  /** The topics and store contents our task writes, storing after each input as the driver commits. */
  private def ours(pipeline: Pipeline[IO], stores: List[String]): IO[(Vector[TaskOutput], Map[String, Map[String, (Vector[Byte], Long)]])] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0).flatMap: task =>
      inputs.foldLeftM((TaskState.empty, Vector.empty[TaskOutput])):
        case ((state, done), input) =>
          val headers = Headers.fromVector(input.headers.map((k, v) => Header(k, Some(Chunk.array(v.getBytes)))).toVector)
          val read = TaskInput(Some(Chunk.array(input.key.getBytes)), Some(Chunk.array(input.value.getBytes)), input.timestamp, headers, "in", 0, 0L)
          task.process(state, read, 0L).flatMap((after, written) => task.stored(after).map(_ -> (done ++ written)))
      .flatMap: (state, written) =>
        stores.traverse(name =>
          task.contents(state, name)
            .map(held => name -> held.toVector.map((key, stored) => String(key.toArray) -> (stored.value.toVector, stored.timestamp)).toMap)
        ).map(kept => (written, kept.toMap))

  /** The output and changelog records and store contents Java writes, with its description. */
  private def theirs(
      build: StreamsBuilder => Unit,
      stores: List[String]
  ): (Map[String, Vector[Output]], Map[String, Map[String, (Vector[Byte], Long)]], String) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val driver   = TopologyTestDriver(topology, properties)
    try
      val input = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      inputs.foreach: record =>
        val headers = RecordHeaders(record.headers.map((k, v) => RecordHeader(k, v.getBytes): org.apache.kafka.common.header.Header).asJava)
        input.pipeInput(TestRecord(record.key, record.value, headers, java.lang.Long.valueOf(record.timestamp)))
      val topics  = "out" :: stores.map(name => s"app-$name-changelog")
      val written =
        topics.map: topic =>
          topic -> driver.createOutputTopic(topic, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: r =>
            Output(
              Option(r.key).map(String(_)),
              Option(r.value).map(_.toVector),
              r.timestamp,
              r.headers.toArray.toVector.map(h => h.key -> Option(h.value).map(String(_)))
            )
      val kept =
        stores.map: name =>
          name -> driver.getKeyValueStore[String, Array[Byte]](name).all.asScala.toList.map(kv => kv.key -> kv.value.toVector).toMap
      (written.toMap, kept.toMap.view.mapValues(_.view.mapValues(_ -> 0L).toMap).toMap, topology.describe.toString)
    finally driver.close()

  private def output(written: TaskOutput): Output =
    Output(
      written.key.map(k => String(k.toArray)),
      written.value.map(_.toVector),
      written.timestamp,
      written.headers.values.map(h => h.key -> h.value.map(v => String(v.toArray)))
    )

  /** Our program and Java's write the same records to the output topic and each store's changelog, keep the same keys and values, and are described
    * alike.
    */
  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, stores: List[String]): IO[Unit] =
    ours(pipeline, stores).map: (written, kept) =>
      val (javaWritten, javaKept, description) = theirs(java, stores)
      assert(javaWritten("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      assertEquals(written.filter(_.topic == out).map(output), javaWritten("out"))
      stores.foreach: name =>
        assertEquals(written.filter(_.topic.value == s"app-$name-changelog").map(output), javaWritten(s"app-$name-changelog"))
        assertEquals(kept(name).view.mapValues(_._1).toMap, javaKept(name).view.mapValues(_._1).toMap)
      assertEquals(Topology.of(pipeline).orFail.describe.toString, description)

  private def logged(store: String): org.apache.kafka.streams.state.StoreBuilder[JavaStore[String, Array[Byte]]] =
    Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(store), Serdes.String, Serdes.ByteArray).withCachingDisabled

  /** Java's processor over one store of bytes, with `handle` given the context and store. */
  private def javaProcessor(name: String)(
      handle: (JavaRecord[String, String], ProcessorContext[String, String], JavaStore[String, Array[Byte]]) => Unit
  ): ProcessorSupplier[String, String, String, String] =
    () =>
      new JavaProcessor[String, String, String, String]:
        private val context                                               = AtomicReference[ProcessorContext[String, String]]()
        override def init(opened: ProcessorContext[String, String]): Unit = context.set(opened)
        override def process(record: JavaRecord[String, String]): Unit    =
          handle(record, context.get, context.get.getStateStore[JavaStore[String, Array[Byte]]](name))

  private val bytes =
    Serde[IO, Array[Byte]](
      Serializer.instance((_, _, value) => IO.pure(Some(Chunk.array(value)))),
      Deserializer.instance((_, _, value) => IO.pure(value.fold(Array.empty[Byte])(_.toArray)))
    )

  test("a tally that reads, writes and forwards under its record's time and headers writes, logs and keeps what Java does"):
    val store = KeyValueStore.of(Store("tally"), text, bytes)
    same(
      KStream(Named("read"), in, text, text).process(
        Named("proc"),
        Processor.withStore(store): (kept: KeyValueStore[IO, String, Array[Byte]]) =>
          Processor.onRecord: (record: Record[String, String]) =>
            kept.get(record.key).flatMap: seen =>
              val now = seen.fold("")(String(_)) + record.value
              kept.put(record.key, now.getBytes).as(List(record.withValue(now)))
      ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("tally"))
        val handler =
          javaProcessor("tally"): (record, context, kept) =>
            val now = Option(kept.get(record.key)).fold("")(String(_)) + record.value
            kept.put(record.key, now.getBytes)
            context.forward(record.withValue(now))
        builder.stream[String, String]("in", org.apache.kafka.streams.kstream.Consumed.as("read"))
          .process[String, String](handler, JavaNamed.as("proc"), "tally").to("out", Produced.as[String, String]("write"))
      ,
      List("tally")
    )

  test("deleting, putIfAbsent, new keys and several results write, log and keep what Java does"):
    val store = KeyValueStore.of(Store("kept"), text, bytes)
    same(
      KStream(Named("read"), in, text, text).process(
        Named("proc"),
        Processor.withStore(store): (kept: KeyValueStore[IO, String, Array[Byte]]) =>
          Processor.onRecord: (record: Record[String, String]) =>
            for
              first <- kept.putIfAbsent(record.key, record.value.getBytes)
              gone  <- if record.value == "z" then kept.delete(record.key) else IO.pure(None)
              _     <- kept.put(s"${record.key}!", "seen".getBytes)
            yield List(
              record.withKey(record.value).withValue(first.fold("-")(String(_))),
              record.withValue(gone.fold("-")(String(_))).withTimestamp(Timestamp.fromEpochMillis(record.timestamp.epochMillis + 1))
            )
      ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("kept"))
        val handler =
          javaProcessor("kept"): (record, context, kept) =>
            val first = kept.putIfAbsent(record.key, record.value.getBytes)
            val gone  = if record.value == "z" then kept.delete(record.key) else null
            kept.put(s"${record.key}!", "seen".getBytes)
            context.forward(record.withKey(record.value).withValue(Option(first).fold("-")(String(_))))
            context.forward(record.withValue(Option(gone).fold("-")(String(_))).withTimestamp(record.timestamp + 1))
        builder.stream[String, String]("in", org.apache.kafka.streams.kstream.Consumed.as("read"))
          .process[String, String](handler, JavaNamed.as("proc"), "kept").to("out", Produced.as[String, String]("write"))
      ,
      List("kept")
    )

  test("ranges, scans and reverses come back in the order Java gives them"):
    val store                      = KeyValueStore.of(Store("kept"), text, bytes)
    def joined(keys: List[String]) = keys.mkString(",")
    same(
      KStream(Named("read"), in, text, text).process(
        Named("proc"),
        Processor.withStore(store): (kept: KeyValueStore[IO, String, Array[Byte]]) =>
          Processor.onRecord: (record: Record[String, String]) =>
            def keys(entries: fs2.Stream[IO, (String, Array[Byte])]) = entries.map(_._1).compile.toList.map(joined)
            for
              _     <- kept.put(record.key, record.value.getBytes)
              found <-
                List(
                  keys(kept.all),
                  keys(kept.reverseAll),
                  keys(kept.range("a", "ab")),
                  keys(kept.reverseRange("a", "ab")),
                  keys(kept.range("é", "a")),
                  keys(kept.prefixScan("a", Serializer.utf8[IO]))
                ).sequence
            yield found.map(record.withValue(_))
      ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("kept"))
        val handler =
          javaProcessor("kept"): (record, context, kept) =>
            kept.put(record.key, record.value.getBytes)
            def keys(found: org.apache.kafka.streams.state.KeyValueIterator[String, Array[Byte]]) =
              try joined(found.asScala.toList.map(_.key))
              finally found.close()
            List(
              keys(kept.all),
              keys(kept.reverseAll),
              keys(kept.range("a", "ab")),
              keys(kept.reverseRange("a", "ab")),
              keys(kept.range("é", "a")),
              keys(kept.prefixScan[StringSerializer, String]("a", StringSerializer()))
            ).foreach(found => context.forward(record.withValue(found)))
        builder.stream[String, String]("in", org.apache.kafka.streams.kstream.Consumed.as("read"))
          .process[String, String](handler, JavaNamed.as("proc"), "kept").to("out", Produced.as[String, String]("write"))
      ,
      List("kept")
    )

  test("two processors sharing a store are one sub-topology, described and numbered as Java's are"):
    val store                                                  = KeyValueStore.of(Store("kept"), text, bytes)
    def tracking(kept: KeyValueStore[IO, String, Array[Byte]]) =
      Processor.onRecord: (record: Record[String, String]) =>
        kept.put(record.key, record.value.getBytes).as(List(record))
    val first = KStream(Named("r1"), in, text, text).process(Named("p1"), Processor.withStore(store)(tracking)).to(Named("w1"), out, text, text)
    val other =
      KStream(Named("r0"), Topic.from("other").orFail, text, text).mapValues(Named("m0"))(identity)
        .to(Named("w0"), Topic.from("o0").orFail, text, text)
    val second =
      KStream(Named("r2"), Topic.from("in2").orFail, text, text).process(Named("p2"), Processor.withStore(store)(tracking))
        .to(Named("w2"), out, text, text)
    val builder = StreamsBuilder()
    builder.addStateStore(logged("kept"))
    def handler =
      javaProcessor("kept")((record, context, kept) =>
        kept.put(record.key, record.value.getBytes); context.forward(record)
      )
    val consumed = (name: String) => org.apache.kafka.streams.kstream.Consumed.as[String, String](name)
    builder.stream[String, String]("in", consumed("r1")).process[String, String](handler, JavaNamed.as("p1"), "kept")
      .to("out", Produced.as[String, String]("w1"))
    builder.stream[String, String]("other", consumed("r0")).mapValues((v: String) => v, JavaNamed.as("m0"))
      .to("o0", Produced.as[String, String]("w0"))
    builder.stream[String, String]("in2", consumed("r2")).process[String, String](handler, JavaNamed.as("p2"), "kept")
      .to("out", Produced.as[String, String]("w2"))
    assertEquals(Topology.of(first, other, second).orFail.describe.toString, builder.build.describe.toString)

  /** Java's processor that keeps the key, over one store of bytes. */
  private def javaFixedKey(name: String)(
      handle: (JavaFixedKeyRecord[String, String], FixedKeyProcessorContext[String, String], JavaStore[String, Array[Byte]]) => Unit
  ): FixedKeyProcessorSupplier[String, String, String] =
    () =>
      new JavaFixedKeyProcessor[String, String, String]:
        private val context                                                       = AtomicReference[FixedKeyProcessorContext[String, String]]()
        override def init(opened: FixedKeyProcessorContext[String, String]): Unit = context.set(opened)
        override def process(record: JavaFixedKeyRecord[String, String]): Unit    =
          handle(record, context.get, context.get.getStateStore[JavaStore[String, Array[Byte]]](name))

  test("a processor that keeps the key, returning several results at changed times and headers, writes, logs and keeps what Java does"):
    val store  = KeyValueStore.of(Store("kept"), text, bytes)
    val marked = Headers.fromVector(Vector(Header("mark", Some(Chunk.array("m".getBytes)))))
    same(
      KStream(Named("read"), in, text, text).processValues(
        Named("proc"),
        FixedKeyProcessor.withStore(store): (kept: KeyValueStore[IO, String, Array[Byte]]) =>
          FixedKeyProcessor.onRecord: (record: FixedKeyRecord[String, String]) =>
            kept.get(record.key).flatMap: seen =>
              val now  = seen.fold("")(String(_)) + record.value
              val gone = if record.value == "z" then kept.delete(record.key).void else kept.put(record.key, now.getBytes)
              gone.as(List(
                record.withValue(now),
                record.withValue(record.value).withTimestamp(Timestamp.fromEpochMillis(record.timestamp.epochMillis + 1)),
                record.withHeaders(marked)
              ))
      ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("kept"))
        val handler =
          javaFixedKey("kept"): (record, context, kept) =>
            val now = Option(kept.get(record.key)).fold("")(String(_)) + record.value
            if record.value == "z" then kept.delete(record.key) else kept.put(record.key, now.getBytes)
            context.forward(record.withValue(now))
            context.forward(record.withValue(record.value).withTimestamp(record.timestamp + 1))
            context.forward(record.withHeaders(RecordHeaders(List[org.apache.kafka.common.header.Header](RecordHeader("mark", "m".getBytes)).asJava)))
        builder.stream[String, String]("in", org.apache.kafka.streams.kstream.Consumed.as("read"))
          .processValues[String](handler, JavaNamed.as("proc"), "kept").to("out", Produced.as[String, String]("write"))
      ,
      List("kept")
    )

  /** Java's processor that keeps the key and uses no store. */
  private def javaKeeping(handle: (JavaFixedKeyRecord[String, String], FixedKeyProcessorContext[String, String]) => Unit) =
    new FixedKeyProcessorSupplier[String, String, String]:
      override def get =
        new JavaFixedKeyProcessor[String, String, String]:
          private val context                                                       = AtomicReference[FixedKeyProcessorContext[String, String]]()
          override def init(opened: FixedKeyProcessorContext[String, String]): Unit = context.set(opened)
          override def process(record: JavaFixedKeyRecord[String, String]): Unit    = handle(record, context.get)

  /** Java's processor that can change the key and uses no store. */
  private def javaChanging(
      handle: (JavaRecord[String, String], ProcessorContext[String, String]) => Unit
  ): ProcessorSupplier[String, String, String, String] =
    () =>
      new JavaProcessor[String, String, String, String]:
        private val context                                               = AtomicReference[ProcessorContext[String, String]]()
        override def init(opened: ProcessorContext[String, String]): Unit = context.set(opened)
        override def process(record: JavaRecord[String, String]): Unit    = handle(record, context.get)

  /** `headers` with one named `key` in place of any it had, as `Headers.set` does. */
  private def javaSet(headers: org.apache.kafka.common.header.Headers, key: String, value: String) =
    val copy = RecordHeaders(headers.toArray)
    copy.remove(key)
    copy.add(key, value.getBytes)

  private def javaRemoved(headers: org.apache.kafka.common.header.Headers, key: String) =
    val copy = RecordHeaders(headers.toArray)
    copy.remove(key)
    copy

  private def source(builder: StreamsBuilder) = builder.stream[String, String]("in", org.apache.kafka.streams.kstream.Consumed.as("read"))

  test("the steps that keep the key write what Java's processors that keep the key write, and are described alike"):
    same(
      KStream(Named("read"), in, text, text).filterRecord(Named("traced"))(_.headers.getAll("trace").nonEmpty)
        .evalFilterRecord(Named("early"))(record => IO.pure(record.timestamp.epochMillis < 2500L)).peekRecord(Named("look"))(_ => IO.unit)
        .mapRecordValue(Named("change")): record =>
          record.withValue(record.value + "!").withTimestamp(Timestamp.fromEpochMillis(record.timestamp.epochMillis + 1))
            .withHeaders(record.headers.set("trace", "new").append("extra", "e"))
        .evalMapRecordValue(Named("again"))(record => IO.pure(record.withValue(record.value + "?")))
        .flatMapRecordValue(Named("split"))(record => List(record, record.withValue("second").withHeaders(record.headers.remove("trace"))))
        .to(Named("write"), out, text, text),
      builder =>
        def step(name: String)(handle: (JavaFixedKeyRecord[String, String], FixedKeyProcessorContext[String, String]) => Unit) =
          javaKeeping(handle) -> JavaNamed.as(name)
        val (traced, tracedName) = step("traced")((record, context) => if record.headers.lastHeader("trace") != null then context.forward(record))
        val (early, earlyName)   = step("early")((record, context) => if record.timestamp < 2500L then context.forward(record))
        val (look, lookName)     = step("look")((record, context) => context.forward(record))
        val (change, changeName) =
          step("change"): (record, context) =>
            val headers = javaSet(record.headers, "trace", "new").add("extra", "e".getBytes)
            context.forward(record.withValue(record.value + "!").withTimestamp(record.timestamp + 1).withHeaders(headers))
        val (again, againName) = step("again")((record, context) => context.forward(record.withValue(record.value + "?")))
        val (split, splitName) =
          step("split"): (record, context) =>
            context.forward(record)
            context.forward(record.withValue("second").withHeaders(javaRemoved(record.headers, "trace")))
        source(builder).processValues[String](traced, tracedName).processValues[String](early, earlyName).processValues[String](look, lookName)
          .processValues[String](change, changeName).processValues[String](again, againName).processValues[String](split, splitName)
          .to("out", Produced.as[String, String]("write"))
      ,
      Nil
    )

  test("the steps that can change the key write what Java's processors write, and are described alike"):
    same(
      KStream(Named("read"), in, text, text).mapRecord(Named("swap"))(record => record.withKey(record.value).withValue(record.key))
        .evalMapRecord(Named("mark"))(record => IO.pure(record.withKey(record.key + "m").withHeaders(record.headers.set("mark", "m"))))
        .flatMapRecord(Named("split")): record =>
          if record.key == "m" then Nil
          else List(record, record.withKey("other").withTimestamp(Timestamp.fromEpochMillis(record.timestamp.epochMillis - 1)))
        .to(Named("write"), out, text, text),
      builder =>
        val swap = javaChanging((record, context) => context.forward(record.withKey(record.value).withValue(record.key)))
        val mark =
          javaChanging((record, context) => context.forward(record.withKey(record.key + "m").withHeaders(javaSet(record.headers, "mark", "m"))))
        val split =
          javaChanging: (record, context) =>
            if record.key != "m" then
              context.forward(record)
              context.forward(record.withKey("other").withTimestamp(record.timestamp - 1))
        source(builder).process[String, String](swap, JavaNamed.as("swap")).process[String, String](mark, JavaNamed.as("mark"))
          .process[String, String](split, JavaNamed.as("split")).to("out", Produced.as[String, String]("write"))
      ,
      Nil
    )

  test("grouping what a processor that keeps the key gave is described as Java's is, with a repartition topic only after the key was changed"):
    val store                                               = KeyValueStore.of(Store("kept"), text, bytes)
    def fixed(kept: KeyValueStore[IO, String, Array[Byte]]) =
      FixedKeyProcessor.onRecord: (record: FixedKeyRecord[String, String]) =>
        kept.put(record.key, record.value.getBytes).as(List(record))
    val count  = Serde.long[IO]
    val direct =
      KStream(Named("r1"), in, text, text).processValues(Named("p1"), FixedKeyProcessor.withStore(store)(fixed)).groupByKey(text, text)
        .count(Named("c1"), Store("s1")).toStream(Named("t1")).to(Named("w1"), out, text, count)
    val rekeyed =
      KStream(Named("r2"), Topic.from("in2").orFail, text, text).selectKey(Named("k2"))((_, value) => value)
        .processValues(Named("p2"), FixedKeyProcessor.withStore(store)(fixed)).groupByKey(Repartition("by"), text, text)
        .count(Named("c2"), Store("s2")).toStream(Named("t2")).to(Named("w2"), out, text, count)
    val builder = StreamsBuilder()
    builder.addStateStore(logged("kept"))
    def handler =
      javaFixedKey("kept")((record, context, kept) =>
        kept.put(record.key, record.value.getBytes); context.forward(record)
      )
    val consumed = (name: String) => org.apache.kafka.streams.kstream.Consumed.as[String, String](name)
    val counted  =
      (name: String) =>
        org.apache.kafka.streams.kstream.Materialized
          .as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[org.apache.kafka.common.utils.Bytes, Array[Byte]]](name)
          .withKeySerde(Serdes.String).withValueSerde(Serdes.Long)
    val longs = Produced.`with`(Serdes.String, Serdes.Long)
    builder.stream[String, String]("in", consumed("r1")).processValues[String](handler, JavaNamed.as("p1"), "kept").groupByKey()
      .count(JavaNamed.as("c1"), counted("s1")).toStream(JavaNamed.as("t1")).to("out", longs.withName("w1"))
    builder.stream[String, String]("in2", consumed("r2")).selectKey[String]((_: String, value: String) => value, JavaNamed.as("k2"))
      .processValues[String](handler, JavaNamed.as("p2"), "kept").groupByKey(org.apache.kafka.streams.kstream.Grouped.as("by"))
      .count(JavaNamed.as("c2"), counted("s2")).toStream(JavaNamed.as("t2")).to("out", longs.withName("w2"))
    assertEquals(Topology.of(direct, rekeyed).orFail.describe.toString, builder.build.describe.toString)

object ProcessorGoldenSuite:
  final case class Input(key: String, value: String, timestamp: Long, headers: List[(String, String)])

  /** An output record with its value as bytes, so text and number values compare alike. */
  final case class Output(key: Option[String], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[(String, Option[String])])
