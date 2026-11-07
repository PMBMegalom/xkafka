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

import java.time.{Duration as JavaDuration, Instant}
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Consumed, Named as JavaNamed, Produced}
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType}
import org.apache.kafka.streams.processor.api.{
  FixedKeyProcessor as JavaFixedKeyProcessor, FixedKeyProcessorContext, FixedKeyProcessorSupplier, FixedKeyRecord as JavaFixedKeyRecord,
  Processor as JavaProcessor, ProcessorContext, ProcessorSupplier, Record as JavaRecord
}
import org.apache.kafka.streams.state.{KeyValueStore as JavaStore, Stores}
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** When punctuators run, what they write, log and keep, against what Java's punctuators do for the same logic, records and clock, through Java
  * Streams' `TopologyTestDriver`.
  */
final class PunctuationGoldenSuite extends CatsEffectSuite:
  import PunctuationGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  private def utf8(value: String): Chunk[Byte] = Chunk.array(value.getBytes)

  /** Runs `script` through our task, storing after each event as the driver commits. */
  private def ours(pipeline: Pipeline[IO], script: List[Event]): IO[Vector[TaskOutput]] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0).flatMap: task =>
      script.foldLeftM((task.started(TaskState.empty, 0L), 0L, Vector.empty[TaskOutput])):
        case ((state, now, done), Event.Read(key, value, at)) =>
          for
            read      <- task.process(state, TaskInput(Some(utf8(key)), Some(utf8(value)), at, Headers.empty, "in", 0, 0L), now)
            punctuate <- task.punctuate(read._1, now)
            kept      <- task.stored(punctuate._1)
          yield (kept, now, done ++ read._2 ++ punctuate._2)
        case ((state, now, done), Event.Wait(millis)) =>
          val later = now + millis
          task.punctuate(state, later).flatMap((after, written) => task.stored(after).map((_, later, done ++ written)))
      .map(_._3)

  /** The records Java writes for `script` to the output topic and `stores`' changelogs, and its description. */
  private def theirs(build: StreamsBuilder => Unit, script: List[Event], stores: List[String]): (Map[String, Vector[Output]], String) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val driver   = TopologyTestDriver(topology, properties, Instant.ofEpochMilli(0L))
    try
      val input = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      script.foreach:
        case Event.Read(key, value, at) => input.pipeInput(key, value, at)
        case Event.Wait(millis)         => driver.advanceWallClockTime(JavaDuration.ofMillis(millis))
      val written = ("out" :: stores.map(name => s"app-$name-changelog")).map: topic =>
        topic -> driver.createOutputTopic(topic, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: r =>
          Output(Option(r.key).map(String(_)), Option(r.value).map(String(_)), r.timestamp, r.headers.toArray.toVector.map(_.key))
      (written.toMap, topology.describe.toString)
    finally driver.close()

  private def output(written: TaskOutput): Output =
    Output(written.key.map(k => String(k.toArray)), written.value.map(v => String(v.toArray)), written.timestamp, written.headers.values.map(_.key))

  private def same(pipeline: Pipeline[IO], java: StreamsBuilder => Unit, script: List[Event], stores: List[String] = Nil): IO[Unit] =
    ours(pipeline, script).map: written =>
      val (javaWritten, description) = theirs(java, script, stores)
      assert(javaWritten("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      assertEquals(written.filter(_.topic == out).map(output), javaWritten("out"))
      stores.foreach(name => assertEquals(written.filter(_.topic.value == s"app-$name-changelog").map(output), javaWritten(s"app-$name-changelog")))
      assertEquals(Topology.of(pipeline).orFail.describe.toString, description)

  private def logged(store: String) =
    Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(store), Serdes.String, Serdes.String).withCachingDisabled

  /** Java's processor with `punctuators`, each given the context and its time, and `handle` for each record. */
  private def java(
      punctuators: List[(Long, PunctuationType, (Long, ProcessorContext[String, String]) => Unit)],
      handle: (JavaRecord[String, String], ProcessorContext[String, String]) => Unit = (record, context) => context.forward(record)
  ): ProcessorSupplier[String, String, String, String] =
    () =>
      new JavaProcessor[String, String, String, String]:
        private val context                                               = AtomicReference[ProcessorContext[String, String]]()
        override def init(opened: ProcessorContext[String, String]): Unit =
          context.set(opened)
          punctuators.foreach: (interval, kind, run) =>
            val _: Cancellable = opened.schedule(JavaDuration.ofMillis(interval), kind, at => run(at, opened))
        override def process(record: JavaRecord[String, String]): Unit = handle(record, context.get)

  private def stream(builder: StreamsBuilder) = builder.stream[String, String]("in", Consumed.as("read"))

  private def reading = KStream(Named("read"), in, text, text)

  private def tick(name: String)(at: Timestamp) = IO.pure(List(Record(name, at.epochMillis.toString, at)))

  private val passing = Processor.onRecord[IO, String, String, String, String](record => IO.pure(List(record)))

  private def toOut(builder: org.apache.kafka.streams.kstream.KStream[String, String], name: String = "write") =
    builder.to("out", Produced.as[String, String](name))

  private val times =
    List(500L, 900L, 1000L, 1400L, 1300L, 4700L, 4800L, 6000L, 5000L, 20000L).zipWithIndex.map((at, n) => Event.Read(s"k$n", s"v$n", at))

  test("a stream-time punctuator runs at the first record, at multiples of its interval, once however many are passed, and not when records go back"):
    same(
      reading.process(Named("proc"), Processor.withoutStores(Processor.every(Punctuation.streamTime(1.second))(tick("p"))(passing)))
        .to(Named("write"), out, text, text),
      builder =>
        toOut(stream(builder).process[String, String](
          java(List((1000L, PunctuationType.STREAM_TIME, (at, context) => context.forward(JavaRecord("p", at.toString, at))))),
          JavaNamed.as("proc")
        )),
      times
    )

  test("a wall-clock punctuator runs an interval after the task starts, with no records, and once however many intervals were missed"):
    val script =
      List(
        Event.Wait(500),
        Event.Wait(499),
        Event.Wait(1),
        Event.Read("a", "1", 7L),
        Event.Wait(999),
        Event.Wait(1),
        Event.Wait(3700),
        Event.Wait(300),
        Event.Wait(1)
      )
    same(
      reading.process(Named("proc"), Processor.withoutStores(Processor.every(Punctuation.wallClock(1.second))(tick("p"))(passing)))
        .to(Named("write"), out, text, text),
      builder =>
        toOut(stream(builder).process[String, String](
          java(List((1000L, PunctuationType.WALL_CLOCK_TIME, (at, context) => context.forward(JavaRecord("p", at.toString, at))))),
          JavaNamed.as("proc")
        )),
      script
    )

  test("what a punctuator writes reaches its changelog at the time it runs with no headers, and it reads what the handler wrote"):
    val store           = KeyValueStore.of(Store("kept"), text, text)
    val script          = times ++ List(Event.Wait(1000), Event.Read("a", "z", 21000L), Event.Wait(1000))
    def named(at: Long) = s"$at"
    same(
      reading.process(
        Named("proc"),
        Processor.withStore(store): (kept: KeyValueStore[IO, String, String]) =>
          val keys = kept.all.map(_._1).compile.toList.map(_.mkString(","))
          Processor.every(Punctuation.streamTime(2.seconds)): at =>
            keys.flatMap(found => kept.put("seen", named(at.epochMillis)).as(List(Record("s", found, at))))
          .apply:
            Processor.every(Punctuation.wallClock(1.second)): at =>
              kept.delete("seen")
                .flatMap(gone => keys.map(found => List(Record("w", s"${gone.isDefined} $found", Timestamp.fromEpochMillis(at.epochMillis + 1)))))
            .apply:
              Processor.onRecord: record =>
                kept.put(record.key, record.value).as(List(record))
      ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("kept"))
        def of(context: ProcessorContext[String, String])   = context.getStateStore[JavaStore[String, String]]("kept")
        def keys(context: ProcessorContext[String, String]) =
          val found = of(context).all
          try found.asScala.toList.map(_.key).mkString(",")
          finally found.close()
        val supplier =
          java(
            List(
              (
                2000L,
                PunctuationType.STREAM_TIME,
                (at, context) =>
                  val found = keys(context)
                  of(context).put("seen", named(at))
                  context.forward(JavaRecord("s", found, at))
              ),
              (
                1000L,
                PunctuationType.WALL_CLOCK_TIME,
                (at, context) =>
                  val gone = of(context).delete("seen")
                  context.forward(JavaRecord("w", s"${gone != null} ${keys(context)}", at + 1))
              )
            ),
            (record, context) =>
              of(context).put(record.key, record.value)
              context.forward(record)
          )
        toOut(stream(builder).process[String, String](supplier, JavaNamed.as("proc"), "kept"))
      ,
      script,
      List("kept")
    )

  test("what a punctuator returns goes through the steps after it, which run a processor's store and its own punctuators"):
    val down = KeyValueStore.of(Store("down"), text, text)
    same(
      reading.process(Named("up"), Processor.withoutStores(Processor.every(Punctuation.streamTime(1.second))(tick("up"))(passing)))
        .mapValues(Named("loud"))(_.toUpperCase).process(
          Named("down"),
          Processor.withStore(down): (kept: KeyValueStore[IO, String, String]) =>
            Processor.every(Punctuation.streamTime(3.seconds)): at =>
              kept.all.map(_._1).compile.toList.map(found => List(Record("down", found.mkString(","), at)))
            .apply:
              Processor.onRecord: record =>
                kept.put(record.key, record.value).as(List(record))
        ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("down"))
        def of(context: ProcessorContext[String, String]) = context.getStateStore[JavaStore[String, String]]("down")
        val up   = java(List((1000L, PunctuationType.STREAM_TIME, (at, context) => context.forward(JavaRecord("up", at.toString, at)))))
        val down =
          java(
            List((
              3000L,
              PunctuationType.STREAM_TIME,
              (at, context) =>
                val found = of(context).all
                val keys  =
                  try found.asScala.toList.map(_.key).mkString(",")
                  finally found.close()
                context.forward(JavaRecord("down", keys, at))
            )),
            (record, context) =>
              of(context).put(record.key, record.value)
              context.forward(record)
          )
        toOut(
          stream(builder).process[String, String](up, JavaNamed.as("up")).mapValues((v: String) => v.toUpperCase, JavaNamed.as("loud"))
            .process[String, String](down, JavaNamed.as("down"), "down")
        )
      ,
      times,
      List("down")
    )

  test("punctuators due together run in the order Java's run in"):
    val intervals                                                 = List("a" -> 1000L, "b" -> 500L, "c" -> 1000L, "d" -> 250L, "e" -> 1000L)
    def ourProcessor(name: String, interval: Long, wall: Boolean) =
      val schedule = if wall then Punctuation.wallClock(interval.millis) else Punctuation.streamTime(interval.millis)
      Processor.withoutStores(Processor.every(schedule)(tick(name))(Processor.onRecord[IO, String, String, String, String](_ => IO.pure(Nil))))
    def javaProcessor(name: String, interval: Long, wall: Boolean) =
      java(
        List((
          interval,
          if wall then PunctuationType.WALL_CLOCK_TIME else PunctuationType.STREAM_TIME,
          (at, context) => context.forward(JavaRecord(name, at.toString, at))
        )),
        (_, _) => ()
      )
    val script = times ++ List(Event.Wait(1000), Event.Wait(1000), Event.Wait(4000))
    List(false, true).traverse_ : wall =>
      val flows =
        intervals.map((name, interval) => reading.flow.process(Named(name), ourProcessor(name, interval, wall)).to(Named(s"w$name"), out, text, text))
      same(
        reading.into(flows.head, flows(1), flows(2), flows(3), flows(4)),
        builder =>
          val read = stream(builder)
          intervals.foreach((name, interval) =>
            read.process[String, String](javaProcessor(name, interval, wall), JavaNamed.as(name)).to("out", Produced.as[String, String](s"w$name"))
          )
        ,
        script
      )

  test("a punctuator of a processor that keeps the key writes to its store, and sends nothing on"):
    val store = KeyValueStore.of(Store("kept"), text, text)
    same(
      reading.processValues(
        Named("proc"),
        FixedKeyProcessor.withStore(store): (kept: KeyValueStore[IO, String, String]) =>
          FixedKeyProcessor.every(Punctuation.streamTime(1.second)): at =>
            kept.put("last", at.epochMillis.toString)
          .apply:
            FixedKeyProcessor.onRecord: record =>
              IO.pure(List(record))
      ).to(Named("write"), out, text, text),
      builder =>
        builder.addStateStore(logged("kept"))
        val supplier: FixedKeyProcessorSupplier[String, String, String] =
          () =>
            new JavaFixedKeyProcessor[String, String, String]:
              private val context                                                       = AtomicReference[FixedKeyProcessorContext[String, String]]()
              override def init(opened: FixedKeyProcessorContext[String, String]): Unit =
                context.set(opened)
                val kept           = opened.getStateStore[JavaStore[String, String]]("kept")
                val _: Cancellable = opened.schedule(JavaDuration.ofSeconds(1), PunctuationType.STREAM_TIME, at => kept.put("last", at.toString))
              override def process(record: JavaFixedKeyRecord[String, String]): Unit = context.get.forward(record)
        toOut(stream(builder).processValues[String](supplier, JavaNamed.as("proc"), "kept"))
      ,
      times,
      List("kept")
    )

object PunctuationGoldenSuite:
  enum Event:
    case Read(key: String, value: String, timestamp: Long)
    case Wait(millis: Long)

  final case class Output(key: Option[String], value: Option[String], timestamp: Long, headers: Vector[String])
