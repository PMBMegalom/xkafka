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
import org.apache.kafka.streams.kstream.internals.SessionWindow
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType}
import org.apache.kafka.streams.processor.api.{Processor as JavaProcessor, ProcessorContext, ProcessorSupplier, Record as JavaRecord}
import org.apache.kafka.streams.state.{KeyValueIterator, SessionStore as JavaStore, Stores}
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What a session store reads, keeps and writes to its changelog, against Java's in-memory session store given the same writes and reads, through
  * Java Streams' `TopologyTestDriver`. Each record's value is a command, and its result goes on.
  */
final class SessionStoreGoldenSuite extends CatsEffectSuite:
  import SessionStoreGoldenSuite.*

  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  private def utf8(value: String): Chunk[Byte] = Chunk.array(value.getBytes("UTF-8"))

  private def at(value: String): Timestamp = Timestamp.fromEpochMillis(value.toLong)

  private def many(found: fs2.Stream[IO, (Windowed[String], Option[String])]) =
    found.compile.toList.map(_.map((key, held) => s"${key.key}@${key.window.start}-${key.window.end}=${held.getOrElse("none")}").mkString(","))

  private def session(key: String, start: String, end: String) = Windowed(key, Window(start.toLong, end.toLong))

  private def ourCommand(kept: SessionStore[IO, String, Option[String]], command: String): IO[String] =
    command.split(':').toList match
      case List("put", key, value, start, end)               => kept.put(session(key, start, end), Some(value)).as("put")
      case List("putNone", key, start, end)                  => kept.put(session(key, start, end), None).as("put none")
      case List("remove", key, start, end)                   => kept.remove(session(key, start, end)).as("removed")
      case List("get", key, start, end)                      => kept.fetchSession(key, at(start), at(end)).map(_.flatten.getOrElse("none"))
      case List("fetch", key)                                => many(kept.fetch(key))
      case List("reverseFetch", key)                         => many(kept.reverseFetch(key))
      case List("fetchRange", first, last)                   => many(kept.fetchRange(first, last))
      case List("reverseRange", first, last)                 => many(kept.reverseFetchRange(first, last))
      case List("find", key, end, start)                     => many(kept.findSessions(key, at(end), at(start)))
      case List("reverseFind", key, end, start)              => many(kept.reverseFindSessions(key, at(end), at(start)))
      case List("findRange", first, last, end, start)        => many(kept.findSessionsRange(first, last, at(end), at(start)))
      case List("reverseFindRange", first, last, end, start) => many(kept.reverseFindSessionsRange(first, last, at(end), at(start)))
      case List("ending", from, to)                          => many(kept.findSessionsEnding(at(from), at(to)))
      case other                                             => IO.raiseError(new IllegalArgumentException(s"no such command: $other"))

  private def theirCommand(kept: JavaStore[String, String], command: String): String =
    def many(found: KeyValueIterator[JavaWindowed[String], String]) =
      def shown(entry: KeyValue[JavaWindowed[String], String]) = s"${entry.key.key}@${entry.key.window.start}-${entry.key.window.end}=${entry.value}"
      try found.asScala.toList.map(shown).mkString(",")
      finally found.close()
    def session(key: String, start: String, end: String) = JavaWindowed(key, SessionWindow(start.toLong, end.toLong))
    command.split(':').toList match
      case List("put", key, value, start, end)               => kept.put(session(key, start, end), value); "put"
      case List("putNone", key, start, end)                  => kept.put(session(key, start, end), null); "put none"
      case List("remove", key, start, end)                   => kept.remove(session(key, start, end)); "removed"
      case List("get", key, start, end)                      => Option(kept.fetchSession(key, start.toLong, end.toLong)).getOrElse("none")
      case List("fetch", key)                                => many(kept.fetch(key))
      case List("reverseFetch", key)                         => many(kept.backwardFetch(key))
      case List("fetchRange", first, last)                   => many(kept.fetch(first, last))
      case List("reverseRange", first, last)                 => many(kept.backwardFetch(first, last))
      case List("find", key, end, start)                     => many(kept.findSessions(key, end.toLong, start.toLong))
      case List("reverseFind", key, end, start)              => many(kept.backwardFindSessions(key, end.toLong, start.toLong))
      case List("findRange", first, last, end, start)        => many(kept.findSessions(first, last, end.toLong, start.toLong))
      case List("reverseFindRange", first, last, end, start) => many(kept.backwardFindSessions(first, last, end.toLong, start.toLong))
      case List("ending", from, to)                          => many(kept.findSessions(from.toLong, to.toLong))
      case other                                             => throw new IllegalArgumentException(s"no such command: $other")

  private def ourPipeline(retention: FiniteDuration, punctuation: Option[Long]): Pipeline[IO] =
    val store = SessionStore.of(Store("kept"), text, text.option, retention = retention)
    val logic =
      (kept: SessionStore[IO, String, Option[String]]) =>
        val handling =
          Processor.onRecord[IO, String, String, String, String]: record =>
            ourCommand(kept, record.value).map(result => List(record.withValue(result)))
        punctuation.fold(handling): interval =>
          Processor.every(Punctuation.streamTime(interval.millis))((now: Timestamp) =>
            kept.put(Windowed("ticks", Window(now.epochMillis, now.epochMillis)), Some("tick")) *> many(kept.fetch("ticks"))
              .map(found => List(Record("tick", found, now)))
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
                  kept.put(JavaWindowed("ticks", SessionWindow(now, now)), "tick")
                  opened.forward(JavaRecord("tick", theirCommand(kept, "fetch:ticks"), now))
              )
        override def process(record: JavaRecord[String, String]): Unit = context.get.forward(record.withValue(theirCommand(kept, record.value)))

  private def theirs(retention: FiniteDuration, punctuation: Option[Long], script: List[(String, String, Long)]): Map[String, Vector[Output]] =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    val builder = StreamsBuilder()
    builder.addStateStore(
      Stores.sessionStoreBuilder(Stores.inMemorySessionStore("kept", JavaDuration.ofMillis(retention.toMillis)), Serdes.String, Serdes.String)
        .withCachingDisabled
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

  private def same(commands: List[String], retention: FiniteDuration = 10.seconds, punctuation: Option[Long] = None) =
    // The records' own times only say what the changelog is stamped with.
    val script = commands.zipWithIndex.map((command, n) => ("k", command, 1000L + n))
    ours(ourPipeline(retention, punctuation), script).map: written =>
      val java = theirs(retention, punctuation, script)
      assert(java("out").nonEmpty, "Java wrote nothing, so there is nothing to compare")
      assertEquals(written.filter(_.topic == out).map(output), java("out"))
      assertEquals(written.filter(_.topic.value == "app-kept-changelog").map(output), java("app-kept-changelog"))

  private val reads =
    List(
      "fetch:a",
      "reverseFetch:a",
      "fetchRange:a:z",
      "reverseRange:a:z",
      "find:a:0:100000",
      "reverseFind:a:0:100000",
      "findRange:a:z:0:100000",
      "reverseFindRange:a:z:0:100000",
      "ending:0:100000"
    )

  test("reads go by end, then key bytes, then the latest start, both ways, whatever order the writes came in"):
    same(
      List(
        "put:b:b1:1000:3000",
        "put:a:a2:1000:2000",
        "put:é:e1:1500:2000",
        "put:a:a1:500:1000",
        "put:ab:ab:1000:2000",
        "put:a\u0000:zero:1000:2000",
        "put:Z:Z:1000:2000",
        "put:a:a3:1500:2000",
        "put:a:a4:1800:2000",
        "put:a:a5:3000:3000"
      ) ++ reads
    )

  test("finding sessions takes those overlapping the time asked for, both ends included, and ending takes those ending within it"):
    same(
      List("put:a:1:1000:2000", "put:a:2:3000:4000", "put:b:3:3500:3500", "put:c:4:5000:6000") ++ List(
        "find:a:2000:3000",
        "find:a:2001:2999",
        "find:a:2001:3000",
        "find:a:4001:9000",
        "find:a:0:999",
        "findRange:a:b:3500:3500",
        "findRange:a:c:0:100000",
        "reverseFindRange:a:c:2000:5000",
        "findRange:c:a:0:100000",
        "fetchRange:b:c",
        "ending:2000:4000",
        "ending:2001:3999",
        "ending:3500:3500",
        "fetch:missing"
      )
    )

  test("a key is not found by another that starts with it, and fetchSession wants the exact start and end"):
    same(
      List("put:a:short:1000:2000", "put:ab:long:1000:2000", "put:a\u0000\u0000:zeros:1000:2000", "fetch:a", "find:a:0:9000") ++
        List("fetchRange:a:a", "get:a:1000:2000", "get:a:1000:2001", "get:a:999:2000", "get:ab:1000:2000", "get:missing:1000:2000") ++
        List("put:a:earlier:500:2000", "get:a:700:2000", "get:a:1000:2000", "get:a:500:2000", "get:a:400:2000")
    )

  test("a write replaces what the session held, a remove takes it away, and a value written as none does too, each reaching the changelog"):
    same(
      List("put:a:1:1000:2000", "put:a:2:1000:2000", "get:a:1000:2000", "remove:a:1000:2000", "get:a:1000:2000", "fetch:a") ++
        List("remove:a:1000:2000", "remove:never:0:0", "put:a:3:1000:2000", "putNone:a:1000:2000", "fetch:a", "putNone:never:1:1", "fetch:never")
    )

  test("a session expires once the latest end written is a whole retention past its end, by a put or a value written as none"):
    same(
      List(
        "put:a:0:0:0",
        "put:a:9999:0:9999",
        "put:a:10000:0:10000",
        "put:a:10001:0:10001",
        "fetch:a",
        "put:b:20000:20000:20000",
        "fetch:a",
        "fetch:b"
      ) ++ List("get:a:0:10000", "get:a:0:10001", "ending:0:100000", "find:a:0:100000", "findRange:a:b:0:100000") ++
        List("putNone:c:30000:30000", "fetch:a", "fetch:b", "get:a:0:10001", "get:b:20000:20000")
    )

  test("a remove does not count towards what expires"):
    same(List("put:a:old:0:0", "remove:b:50000:50000", "fetch:a", "get:a:0:0", "put:a:new:1000:1000", "fetch:a", "remove:a:0:0", "fetch:a"))

  test("a write to a session already expired keeps nothing and still reaches the changelog"):
    same(List(
      "put:a:new:50000:50000",
      "put:a:old:0:1000",
      "get:a:0:1000",
      "fetch:a",
      "remove:a:0:1000",
      "fetch:a",
      "put:a:edge:0:40000",
      "put:a:live:0:40001",
      "fetch:a"
    ))

  test("a session of one moment holds as any other, and the shortest retention keeps only the latest end"):
    same(List("put:a:1:1000:1000", "fetch:a", "put:a:2:1001:1001", "fetch:a", "find:a:1000:1001"), retention = 1.milli)

  test("what a punctuator writes reaches the changelog at the time it was given"):
    same(List("put:a:1:1000:1000", "put:b:2:1001:1001", "fetch:ticks", "fetch:a"), punctuation = Some(1L))

object SessionStoreGoldenSuite:
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[String])
