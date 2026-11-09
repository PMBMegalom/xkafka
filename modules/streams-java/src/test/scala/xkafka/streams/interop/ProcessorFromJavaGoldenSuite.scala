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
package interop

import java.time.{Duration, Instant}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Consumed, Named as JavaNamed, Produced}
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType}
import org.apache.kafka.streams.processor.api.{
  FixedKeyProcessor as JavaFixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord as JavaFixedKeyRecord, Processor as JavaProcessor,
  ProcessorContext, ProcessorSupplier, Record as JavaRecord
}
import org.apache.kafka.streams.state.{KeyValueStore as JavaKeyValueStore, Stores, TimestampedKeyValueStore, ValueAndTimestamp}
import org.apache.kafka.streams.test.TestRecord
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What a Java processor writes, logs and is described as, run through xkafka, against the same processor run by Java's `TopologyTestDriver`. */
final class ProcessorFromJavaGoldenSuite extends CatsEffectSuite:
  import ProcessorFromJavaGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  /** Counts each key's records in `counts`, sending on each new count with its record's time moved on and a header naming where it came from; by
    * stream time every 10 s sends every count; by the clock every 5 s sends a tick, three times, and then cancels itself.
    */
  private final class Counting(closed: AtomicInteger) extends JavaProcessor[String, String, String, String]:
    private val context = new AtomicReference[ProcessorContext[String, String]]()
    private val ticks   = new AtomicInteger(0)
    private val ticking = new AtomicReference[Cancellable]()
    private def counts  = context.get.getStateStore[JavaKeyValueStore[String, String]]("counts")

    override def init(opened: ProcessorContext[String, String]): Unit =
      context.set(opened)
      opened.schedule(
        Duration.ofSeconds(10),
        PunctuationType.STREAM_TIME,
        at => counts.all().asScala.foreach(kv => opened.forward(new JavaRecord(kv.key, s"total ${kv.value}", at)))
      )
      ticking.set(opened.schedule(
        Duration.ofSeconds(5),
        PunctuationType.WALL_CLOCK_TIME,
        at =>
          opened.forward(new JavaRecord("tick", s"tick ${ticks.incrementAndGet} at $at", at))
          if ticks.get == 3 then ticking.get.cancel()
      ))

    override def process(record: JavaRecord[String, String]): Unit =
      val seen = Option(counts.get(record.key)).fold(0)(_.toInt) + 1
      counts.put(record.key, seen.toString)
      val origin  = context.get.recordMetadata.map(m => s"${m.topic}/${m.partition}/${m.offset}").orElse("none")
      val headers = new RecordHeaders(record.headers.toArray)
      headers.add("from", origin.getBytes)
      context.get.forward(record.withValue(s"$seen at ${context.get.currentStreamTimeMs}").withTimestamp(record.timestamp + 1).withHeaders(headers))

    override def close(): Unit = closed.incrementAndGet: Unit

  /** Keeps each key's latest value in a timestamped store at twice its record's time, sending on what it held before with that time. */
  private final class Stamping extends JavaProcessor[String, String, String, String]:
    private val context                                               = new AtomicReference[ProcessorContext[String, String]]()
    private def kept                                                  = context.get.getStateStore[TimestampedKeyValueStore[String, String]]("stamped")
    override def init(opened: ProcessorContext[String, String]): Unit = context.set(opened)
    override def process(record: JavaRecord[String, String]): Unit    =
      val before = Option(kept.get(record.key)).fold("none")(held => s"${held.value}@${held.timestamp}")
      kept.put(record.key, ValueAndTimestamp.make(record.value, record.timestamp * 2))
      context.get.forward(record.withValue(before))

  /** Upper-cases each value, keeping the key, and sends each on twice. */
  private final class Shouting extends JavaFixedKeyProcessor[String, String, String]:
    private val context                                                       = new AtomicReference[FixedKeyProcessorContext[String, String]]()
    override def init(opened: FixedKeyProcessorContext[String, String]): Unit = context.set(opened)
    override def process(record: JavaFixedKeyRecord[String, String]): Unit    =
      context.get.forward(record.withValue(record.value.toUpperCase))
      context.get.forward(record.withValue(record.value * 2).withTimestamp(record.timestamp + 7))

  private val events =
    List(
      Event.Read("a", "x", 1000L, List("trace" -> "1")),
      Event.Read("b", "y", 4000L, Nil),
      Event.Tick(5000L),
      Event.Read("a", "z", 12000L, Nil),
      Event.Tick(6000L),
      Event.Read("c", "w", 9000L, List("trace" -> "4")),
      Event.Tick(5000L),
      Event.Tick(5000L),
      Event.Read("b", "v", 25000L, Nil),
      Event.Tick(5000L)
    )

  /** What our task writes for `events`, from wall-clock time 0, punctuating after each record and each tick, as the engine does. */
  private def ours(pipeline: Pipeline[IO]): IO[Vector[TaskOutput]] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0).flatMap: task =>
      task.opened(TaskState.empty, 0L).flatMap: opened =>
        // Offsets count the records read, as the driver numbers them; a tick reads none.
        val numbered =
          events.foldLeft((List.empty[(Event, Int)], 0))((so, event) =>
            (
              so._1 :+ (event -> so._2),
              so._2 +
                (event match
                  case Event.Read(_, _, _, _) => 1;
                  case _                      => 0)
            )
          )._1
        numbered.foldLeftM((opened, 0L, Vector.empty[TaskOutput])):
          case ((state, now, done), (Event.Read(key, value, at, headers), offset)) =>
            val read =
              TaskInput(
                Some(Chunk.array(key.getBytes)),
                Some(Chunk.array(value.getBytes)),
                at,
                Headers.fromVector(headers.map((k, v) => Header(k, Some(Chunk.array(v.getBytes)))).toVector),
                "in",
                0,
                offset.toLong
              )
            task.process(state, read, now).flatMap: (after, written) =>
              task.punctuate(after, now).flatMap: (ticked, more) =>
                task.stored(ticked).map(stored => (stored, now, done ++ written ++ more))
          case ((state, now, done), (Event.Tick(by), _)) => task.punctuate(state, now + by)
              .flatMap((after, written) => task.stored(after).map((_, now + by, done ++ written)))
        .flatMap((_, _, written) => task.close.as(written))

  private def theirs(build: StreamsBuilder => Unit, topics: List[String]): (Map[String, Vector[Output]], String) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-processor-from-java-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val driver   = TopologyTestDriver(topology, properties, Instant.ofEpochMilli(0L))
    try
      val input = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      events.foreach:
        case Event.Read(key, value, at, headers) => input.pipeInput(TestRecord(
            key,
            value,
            new RecordHeaders(headers.map((k, v) => new RecordHeader(k, v.getBytes): org.apache.kafka.common.header.Header).asJava),
            java.lang.Long.valueOf(at)
          ))
        case Event.Tick(by) => driver.advanceWallClockTime(Duration.ofMillis(by))
      val written =
        topics.map: topic =>
          topic -> driver.createOutputTopic(topic, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: record =>
            Output(
              Option(record.key).map(String(_)),
              Option(record.value).map(String(_)),
              record.timestamp,
              record.headers.toArray.toVector.map(h => h.key -> Option(h.value).map(String(_)))
            )
      (written.toMap, topology.describe.toString)
    finally driver.close()

  private def output(written: TaskOutput): Output =
    Output(
      written.key.map(k => String(k.toArray)),
      written.value.map(v => String(v.toArray)),
      written.timestamp,
      written.headers.values.map(h => h.key -> h.value.map(v => String(v.toArray)))
    )

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, topics: List[String]): IO[Unit] =
    ours(pipeline).map: written =>
      val (javaWritten, description) = theirs(java, topics)
      assert(javaWritten("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      topics.foreach(topic => assertEquals(written.filter(_.topic.value == topic).map(output), javaWritten(topic), topic))
      assertEquals(Topology.of(pipeline).orFail.describe.toString, description)

  private def logged(name: String) = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(name), Serdes.String, Serdes.String).withCachingDisabled

  test("a Java processor counting in a store, sending on what it counts, and punctuating by stream time and by the clock writes what Java's does"):
    val closed = new AtomicInteger(0)
    same(
      KStream(Named("read"), in, text, text)
        .process(Named("proc"), Processor.fromJava[IO](() => new Counting(closed), JavaStore.keyValue(Store("counts"), text, text)))
        .to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("counts"))
        val supplier: ProcessorSupplier[String, String, String, String] = () => new Counting(new AtomicInteger(0))
        builder.stream[String, String]("in", Consumed.as("read")).process(supplier, JavaNamed.as("proc"), "counts").to("out", Produced.as("write"))
      ,
      List("out", "app-counts-changelog")
    ).map(_ => assertEquals(closed.get, 1, "the task's instance was not closed once"))

  test("a Java processor keeping values in a timestamped store reads back the times it wrote, as Java's does"):
    same(
      KStream(Named("read"), in, text, text)
        .process(Named("proc"), Processor.fromJava[IO](() => new Stamping, JavaStore.timestamped(Store("stamped"), text, text)))
        .to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(
          Stores.timestampedKeyValueStoreBuilder(Stores.inMemoryKeyValueStore("stamped"), Serdes.String, Serdes.String).withCachingDisabled
        )
        val supplier: ProcessorSupplier[String, String, String, String] = () => new Stamping
        builder.stream[String, String]("in", Consumed.as("read")).process(supplier, JavaNamed.as("proc"), "stamped").to("out", Produced.as("write"))
      ,
      List("out", "app-stamped-changelog")
    )

  test("a Java processor keeping each record's key writes what Java's does"):
    same(
      KStream(Named("read"), in, text, text).processValues(Named("proc"), FixedKeyProcessor.fromJava[IO](() => new Shouting))
        .to(Named("write"), out, text, text),
      builder =>
        val supplier: org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier[String, String, String] = () => new Shouting
        builder.stream[String, String]("in", Consumed.as("read")).processValues[String](supplier, JavaNamed.as("proc"))
          .to("out", Produced.as("write"))
      ,
      List("out")
    )

object ProcessorFromJavaGoldenSuite:
  enum Event:
    case Read(key: String, value: String, timestamp: Long, headers: List[(String, String)])
    case Tick(by: Long)

  final case class Output(key: Option[String], value: Option[String], timestamp: Long, headers: Vector[(String, Option[String])])
