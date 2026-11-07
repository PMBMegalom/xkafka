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
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Consumed, Named as JavaNamed, Produced, Windowed as JavaWindowed}
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType}
import org.apache.kafka.streams.processor.api.{Processor as JavaProcessor, ProcessorContext, ProcessorSupplier, Record as JavaRecord}
import org.apache.kafka.streams.state.{KeyValueIterator, Stores, WindowStore as JavaStore, WindowStoreIterator}
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What a window store reads, keeps and writes to its changelog, against Java's in-memory window store given the same writes and reads, through Java
  * Streams' `TopologyTestDriver`. Each record's value is a command, and its result goes on.
  */
final class WindowStoreGoldenSuite extends CatsEffectSuite:
  import WindowStoreGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  private def utf8(value: String): Chunk[Byte] = Chunk.array(value.getBytes("UTF-8"))

  private def at(value: String): Timestamp = Timestamp.fromEpochMillis(value.toLong)

  private def single(found: fs2.Stream[IO, (Timestamp, String)]) =
    found.compile.toList.map(_.map((start, held) => s"${start.epochMillis}=$held").mkString(","))

  private def many(found: fs2.Stream[IO, (Windowed[String], String)]) =
    found.compile.toList.map(_.map((key, held) => s"${key.key}@${key.window.start}-${key.window.end}=$held").mkString(","))

  private def ourCommand(kept: WindowStore[IO, String, String], command: String): IO[String] =
    command.split(':').toList match
      case List("put", key, value, start)         => kept.put(key, value, at(start)).as("put")
      case List("delete", key, start)             => kept.delete(key, at(start)).as("deleted")
      case List("get", key, start)                => kept.fetch(key, at(start)).map(_.getOrElse("none"))
      case List("fetch", key, from, to)           => single(kept.fetch(key, at(from), at(to)))
      case List("reverseFetch", key, from, to)    => single(kept.reverseFetch(key, at(from), at(to)))
      case List("range", first, last, from, to)   => many(kept.fetchRange(first, last, at(from), at(to)))
      case List("reverse", first, last, from, to) => many(kept.reverseFetchRange(first, last, at(from), at(to)))
      case List("fetchAll", from, to)             => many(kept.fetchAll(at(from), at(to)))
      case List("reverseFetchAll", from, to)      => many(kept.reverseFetchAll(at(from), at(to)))
      case List("all")                            => many(kept.all)
      case List("reverseAll")                     => many(kept.reverseAll)
      case other                                  => IO.raiseError(new IllegalArgumentException(s"no such command: $other"))

  private def theirCommand(kept: JavaStore[String, String], command: String): String =
    def single(found: WindowStoreIterator[String]) =
      try found.asScala.toList.map(entry => s"${entry.key}=${entry.value}").mkString(",")
      finally found.close()
    def many(found: KeyValueIterator[JavaWindowed[String], String]) =
      def shown(entry: KeyValue[JavaWindowed[String], String]) = s"${entry.key.key}@${entry.key.window.start}-${entry.key.window.end}=${entry.value}"
      try found.asScala.toList.map(shown).mkString(",")
      finally found.close()
    command.split(':').toList match
      case List("put", key, value, start)         => kept.put(key, value, start.toLong); "put"
      case List("delete", key, start)             => kept.put(key, null, start.toLong); "deleted"
      case List("get", key, start)                => Option(kept.fetch(key, start.toLong)).getOrElse("none")
      case List("fetch", key, from, to)           => single(kept.fetch(key, from.toLong, to.toLong))
      case List("reverseFetch", key, from, to)    => single(kept.backwardFetch(key, from.toLong, to.toLong))
      case List("range", first, last, from, to)   => many(kept.fetch(first, last, from.toLong, to.toLong))
      case List("reverse", first, last, from, to) => many(kept.backwardFetch(first, last, from.toLong, to.toLong))
      case List("fetchAll", from, to)             => many(kept.fetchAll(from.toLong, to.toLong))
      case List("reverseFetchAll", from, to)      => many(kept.backwardFetchAll(from.toLong, to.toLong))
      case List("all")                            => many(kept.all)
      case List("reverseAll")                     => many(kept.backwardAll)
      case other                                  => throw new IllegalArgumentException(s"no such command: $other")

  private def ourPipeline(size: FiniteDuration, retention: FiniteDuration, punctuation: Option[Long]): Pipeline[IO] =
    val store = WindowStore.of(Store("kept"), text, text, size = size, retention = retention)
    val logic =
      (kept: WindowStore[IO, String, String]) =>
        val handling =
          Processor.onRecord[IO, String, String, String, String]: record =>
            ourCommand(kept, record.value).map(result => List(record.withValue(result)))
        punctuation.fold(handling): interval =>
          Processor.every(Punctuation.streamTime(interval.millis))((now: Timestamp) =>
            kept.put("ticks", "tick", now) *> many(kept.all).map(found => List(Record("tick", found, now)))
          )(handling)
    KStream(Named("read"), in, text, text).process(Named("proc"), Processor.withStore(store)(logic)).to(Named("write"), out, text, text)

  private def ours(pipeline: Pipeline[IO], script: List[(String, String, Long)]): IO[Vector[TaskOutput]] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0).flatMap: task =>
      script.foldLeftM((task.started(TaskState.empty, 0L), Vector.empty[TaskOutput])):
        case ((state, done), (key, value, time)) =>
          for
            read      <- task.process(state, TaskInput(Some(utf8(key)), Some(utf8(value)), time, Headers.empty, "in", 0, 0L), 0L)
            punctuate <- task.punctuate(read._1, 0L)
            kept      <- task.stored(punctuate._1)
          yield (kept, done ++ read._2 ++ punctuate._2)
      .map(_._2)

  private def supplier(punctuation: Option[Long]): ProcessorSupplier[String, String, String, String] =
    () =>
      new JavaProcessor[String, String, String, String]:
        private val context                                               = AtomicReference[ProcessorContext[String, String]]()
        private def kept                                                  = context.get.getStateStore[JavaStore[String, String]]("kept")
        override def init(opened: ProcessorContext[String, String]): Unit =
          context.set(opened)
          punctuation.foreach: interval =>
            val _: Cancellable =
              opened.schedule(
                JavaDuration.ofMillis(interval),
                PunctuationType.STREAM_TIME,
                now =>
                  kept.put("ticks", "tick", now)
                  opened.forward(JavaRecord("tick", theirCommand(kept, "all"), now))
              )
        override def process(record: JavaRecord[String, String]): Unit = context.get.forward(record.withValue(theirCommand(kept, record.value)))

  private def theirs(
      size: FiniteDuration,
      retention: FiniteDuration,
      punctuation: Option[Long],
      script: List[(String, String, Long)]
  ): Map[String, Vector[Output]] =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    val builder = StreamsBuilder()
    builder.addStateStore(
      Stores.windowStoreBuilder(
        Stores.inMemoryWindowStore("kept", JavaDuration.ofMillis(retention.toMillis), JavaDuration.ofMillis(size.toMillis), false),
        Serdes.String,
        Serdes.String
      ).withCachingDisabled
    )
    builder.stream[String, String]("in", Consumed.as("read")).process[String, String](supplier(punctuation), JavaNamed.as("proc"), "kept")
      .to("out", Produced.as[String, String]("write"))
    val driver = TopologyTestDriver(builder.build(properties), properties, Instant.ofEpochMilli(0L))
    try
      val input = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      script.foreach((key, value, time) => input.pipeInput(key, value, time))
      List("out", "app-kept-changelog").map: topic =>
        topic -> driver.createOutputTopic(topic, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: r =>
          Output(Option(r.key).map(_.toVector), Option(r.value).map(_.toVector), r.timestamp, r.headers.toArray.toVector.map(_.key))
      .toMap
    finally driver.close()

  private def output(written: TaskOutput): Output =
    Output(written.key.map(_.toVector), written.value.map(_.toVector), written.timestamp, written.headers.values.map(_.key))

  private def same(
      commands: List[String],
      size: FiniteDuration = 1.second,
      retention: FiniteDuration = 10.seconds,
      punctuation: Option[Long] = None
  ) =
    // The records' own times only say what the changelog is stamped with.
    val script = commands.zipWithIndex.map((command, n) => ("k", command, 1000L + n))
    ours(ourPipeline(size, retention, punctuation), script).map: written =>
      val java = theirs(size, retention, punctuation, script)
      assert(java("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      assertEquals(written.filter(_.topic == out).map(output), java("out"))
      assertEquals(written.filter(_.topic.value == "app-kept-changelog").map(output), java("app-kept-changelog"))

  private val reads =
    List(
      "fetch:a:0:100000",
      "reverseFetch:a:0:100000",
      "fetchAll:0:100000",
      "reverseFetchAll:0:100000",
      "all",
      "reverseAll",
      "range:a:z:0:100000",
      "reverse:a:z:0:100000"
    )

  test("reads go by window start and then key bytes, both ways, with each window's end, whatever order the writes came in"):
    same(
      List(
        "put:b:b1:3000",
        "put:a:a2:2000",
        "put:é:e1:2000",
        "put:a:a1:1000",
        "put:ab:ab:2000",
        "put:a\u0000:zero:2000",
        "put:Z:Z:2000",
        "put:a:a3:3000"
      ) ++ reads
    )

  test("time and key ranges include both ends, and a range the wrong way round finds nothing"):
    same(
      List("put:a:1:1000", "put:b:2:2000", "put:c:3:3000", "put:d:4:4000") ++ List(
        "fetch:a:1000:1000",
        "fetch:a:1001:5000",
        "fetch:b:0:1999",
        "fetchAll:2000:3000",
        "reverseFetchAll:2000:3000",
        "fetchAll:3000:2000",
        "range:b:c:0:5000",
        "reverse:b:c:0:5000",
        "range:c:b:0:5000",
        "range:b:c:2001:2999",
        "fetch:missing:0:5000",
        "get:missing:1000"
      )
    )

  test("a key is not found by another that starts with it"):
    same(List(
      "put:a:short:1000",
      "put:ab:long:1000",
      "put:a\u0000\u0000:zeros:1000",
      "fetch:a:0:5000",
      "fetch:ab:0:5000",
      "get:a:1000",
      "range:a:a:0:5000"
    ))

  test("a write replaces what the window held, and a delete removes it, writing to the changelog each time"):
    same(List(
      "put:a:1:1000",
      "put:a:2:1000",
      "get:a:1000",
      "delete:a:1000",
      "get:a:1000",
      "all",
      "delete:a:1000",
      "delete:never:5000",
      "all",
      "put:a:3:1000",
      "all"
    ))

  test("a window expires once the latest start written is a whole retention past its start, and a delete moves that time as a write does"):
    same(
      List("put:a:0:0", "put:a:9999:9999", "put:a:10000:10000", "put:a:10001:10001", "all", "put:b:20000:20000", "all") ++
        List("get:a:10000", "get:a:10001", "fetch:a:0:100000", "fetchAll:0:100000", "reverseFetchAll:0:100000") ++
        List("delete:b:30000", "all", "get:a:10001", "get:b:20000")
    )

  test("a write to a window already expired keeps nothing and still reaches the changelog"):
    same(List(
      "put:a:new:50000",
      "put:a:old:1000",
      "get:a:1000",
      "all",
      "delete:a:1000",
      "all",
      "put:a:edge:40000",
      "get:a:40000",
      "put:a:live:40001",
      "all"
    ))

  test("a window ending past the largest time ends at the largest time, and a retention as long as its size is allowed"):
    same(List(s"put:a:end:${Long.MaxValue - 10}", "all", s"fetch:a:0:${Long.MaxValue}")) *>
      same(List("put:a:1:1000", "put:a:2:1999", "put:a:3:2000", "put:a:4:2001", "all"), size = 1.second, retention = 1.second)

  test("a window store's size gives only what reads over several keys say a window ends at"):
    same(List("put:a:1:1000", "put:a:2:61000", "all", "fetch:a:0:100000"), size = 1.minute, retention = 1.hour)

  test("what a punctuator writes reaches the changelog at the time it was given, and it reads what the handler wrote"):
    same(List("put:a:1:1000", "put:b:2:1001", "all", "put:a:3:1002", "get:ticks:1000"), punctuation = Some(1L))

object WindowStoreGoldenSuite:
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[String])
