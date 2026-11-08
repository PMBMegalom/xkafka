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

import java.time.Duration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.{Processor as JavaProcessor, ProcessorContext, Record as JavaRecord}
import org.apache.kafka.streams.state.KeyValueStore as JavaKeyValueStore
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What only a task of its own shows of a Java processor run through xkafka: an instance for each task, schedules each instance makes for its own
  * task, and the failures of what xkafka does not support.
  */
final class ProcessorFromJavaSuite extends CatsEffectSuite:
  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  private def input(key: String, value: String, at: Long, offset: Long = 0L): TaskInput =
    TaskInput(Some(Chunk.array(key.getBytes)), Some(Chunk.array(value.getBytes)), at, Headers.empty, "in", 0, offset)

  private def pipeline(processor: Processor[IO, String, String, String, String]) =
    Topology.of(KStream(Named("read"), in, text, text).process(Named("proc"), processor).to(Named("write"), out, text, text)).orFail

  private def task(topology: Topology[IO], partition: Int): IO[Task[IO]] = Task.inMemory[IO]("app", topology.subtopologies.head, partition)

  private def values(written: Vector[TaskOutput]): Vector[String] = written.filter(_.topic == out).flatMap(_.value).map(v => String(v.toArray))

  /** A processor numbering the records its own instance sees in a field, which schedules a tick by the clock only in the task of partition 0. */
  private final class Numbering(made: AtomicInteger) extends JavaProcessor[String, String, String, String]:
    private val context = new AtomicReference[ProcessorContext[String, String]]()
    private val seen    = new AtomicInteger(0)
    made.incrementAndGet: Unit
    override def init(opened: ProcessorContext[String, String]): Unit =
      context.set(opened)
      if opened.taskId.partition == 0 then
        opened.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, at => opened.forward(new JavaRecord("tick", s"tick $at", at))): Unit
    override def process(record: JavaRecord[String, String]): Unit =
      context.get.forward(record.withValue(s"${record.value} #${seen.incrementAndGet} in ${context.get.taskId}"))

  test("each task has an instance of its own, keeping its own fields, and schedules only what its own instance asked for"):
    val made     = new AtomicInteger(0)
    val topology = pipeline(Processor.fromJava[IO](() => new Numbering(made)))
    for
      first   <- task(topology, 0)
      second  <- task(topology, 1)
      opened0 <- first.opened(TaskState.empty, 0L)
      opened1 <- second.opened(TaskState.empty, 0L)
      a       <- first.process(opened0, input("k", "a", 10L), 0L)
      b       <- first.process(a._1, input("k", "b", 20L), 0L)
      c       <- second.process(opened1, input("k", "c", 30L), 0L)
      ticked0 <- first.punctuate(b._1, 1500L)
      ticked1 <- second.punctuate(c._1, 1500L)
      _       <- first.close *> second.close
    yield
      assertEquals(made.get, 2)
      assertEquals(values(a._2 ++ b._2), Vector("a #1 in 0_0", "b #2 in 0_0"))
      assertEquals(values(c._2), Vector("c #1 in 0_1"))
      assertEquals(values(ticked0._2), Vector("tick 1500"))
      assertEquals(values(ticked1._2), Vector.empty)

  private def failing(processor: () => JavaProcessor[String, String, String, String], stores: JavaStore[IO]*): IO[Option[String]] =
    val topology = pipeline(Processor.fromJava[IO](processor, stores*))
    task(topology, 0).flatMap(t => t.opened(TaskState.empty, 0L).flatMap(state => t.process(state, input("k", "v", 10L), 0L))).attempt
      .map(_.left.toOption.map(_.getMessage))

  private def processor(
      onInit: ProcessorContext[String, String] => Unit,
      onRecord: (ProcessorContext[String, String], JavaRecord[String, String]) => Unit
  ) =
    () =>
      new JavaProcessor[String, String, String, String]:
        private val context                                               = new AtomicReference[ProcessorContext[String, String]]()
        override def init(opened: ProcessorContext[String, String]): Unit =
          context.set(opened); onInit(opened)
        override def process(record: JavaRecord[String, String]): Unit = onRecord(context.get, record)

  test("what xkafka does not support fails with a message saying what"):
    val counts = JavaStore.keyValue(Store("counts"), text, text)
    for
      written <- failing(processor(_.getStateStore[JavaKeyValueStore[String, String]]("counts").put("k", "v"), (_, _) => ()), counts)
      sent    <- failing(processor(opened => opened.forward(new JavaRecord("k", "v", 0L)), (_, _) => ()))
      missing <- failing(processor(_ => (), (context, _) => context.getStateStore[JavaKeyValueStore[String, String]]("nowhere"): Unit))
      child   <- failing(processor(_ => (), (context, record) => context.forward(record, "elsewhere")))
      short   <- failing(processor(_.schedule(Duration.ZERO, PunctuationType.STREAM_TIME, _ => ()): Unit, (_, _) => ()))
    yield
      assert(written.exists(_.contains("wrote to a store as it started")), written.toString)
      assert(sent.exists(_.contains("sent records on as it started")), sent.toString)
      assert(missing.exists(_.contains("'nowhere'")), missing.toString)
      assert(child.exists(_.contains("'elsewhere'")), child.toString)
      assert(short.exists(_.contains("at least a millisecond")), short.toString)

  test("a punctuator cancelled while a record is processed does not run again, and one scheduled then runs from then on"):
    val ticking  = new AtomicReference[org.apache.kafka.streams.processor.Cancellable]()
    val stopping =
      processor(
        opened =>
          ticking.set(
            opened.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, at => opened.forward(new JavaRecord("tick", s"first $at", at)))
          ),
        (context, record) =>
          if record.value == "stop" then
            ticking.get.cancel()
            context
              .schedule(
                Duration.ofSeconds(2),
                PunctuationType.WALL_CLOCK_TIME,
                at => context.forward(new JavaRecord("tick", s"second $at", at))
              ): Unit
      )
    val topology = pipeline(Processor.fromJava[IO](stopping))
    task(topology, 0).flatMap: t =>
      for
        opened  <- t.opened(TaskState.empty, 0L)
        early   <- t.punctuate(opened, 1000L)
        stopped <- t.process(early._1, input("k", "stop", 10L), 1500L)
        later   <- t.punctuate(stopped._1, 2000L)
        latest  <- t.punctuate(later._1, 3600L)
      yield
        assertEquals(values(early._2), Vector("first 1000"))
        assertEquals(values(later._2), Vector.empty)
        assertEquals(values(latest._2), Vector("second 3600"))

  test(
    "asking a context for its state directory or default serdes fails saying why, and asking to commit or recording to a sensor does nothing else"
  ):
    val asked =
      processor(
        _ => (),
        (context, record) =>
          def why(ask: => Any) = scala.util.Try(ask).failed.toOption.map(_.getMessage).getOrElse("none")
          context.commit()
          context.metrics.addSensor("seen", org.apache.kafka.common.metrics.Sensor.RecordingLevel.INFO).record(1.0)
          context.forward(record.withValue(List(why(context.stateDir), why(context.keySerde), why(context.valueSerde)).mkString("|")))
      )
    val topology = pipeline(Processor.fromJava[IO](asked))
    task(topology, 0).flatMap(t => t.opened(TaskState.empty, 0L).flatMap(state => t.process(state, input("k", "v", 10L), 0L))).map: (_, written) =>
      val reasons = values(written).head.split('|').toList
      assert(reasons.head.contains("in memory"), reasons.toString)
      assert(reasons.tail.forall(_.contains("no default serdes")), reasons.toString)

  test("a punctuator scheduled with a start time runs first at that time, and then each interval after it"):
    val starting =
      processor(
        opened =>
          opened.schedule(
            java.time.Instant.ofEpochMilli(2500L),
            Duration.ofSeconds(1),
            PunctuationType.WALL_CLOCK_TIME,
            at => opened.forward(new JavaRecord("tick", s"$at", at))
          ): Unit,
        (_, _) => ()
      )
    val topology = pipeline(Processor.fromJava[IO](starting))
    task(topology, 0).flatMap: t =>
      for
        opened <- t.opened(TaskState.empty, 0L)
        before <- t.punctuate(opened, 2000L)
        first  <- t.punctuate(before._1, 2600L)
        next   <- t.punctuate(first._1, 3500L)
      yield
        assertEquals(values(before._2), Vector.empty)
        assertEquals(values(first._2), Vector("2600"))
        assertEquals(values(next._2), Vector("3500"))

  test("a Java processor reads what an earlier record wrote, and finds keys by range in the order Java's stores keep them"):
    val counts  = JavaStore.keyValue(Store("counts"), text, text)
    val ranging =
      processor(
        _ => (),
        (context, record) =>
          val store = context.getStateStore[JavaKeyValueStore[String, String]]("counts")
          store.put(record.key, record.value)
          val inRange = scala.jdk.CollectionConverters.IteratorHasAsScala(store.range("b", "d")).asScala.map(_.key).mkString(",")
          val opened  = scala.jdk.CollectionConverters.IteratorHasAsScala(store.range(null, "b")).asScala.map(_.key).mkString(",")
          context.forward(record.withValue(s"$inRange|$opened|${store.approximateNumEntries}"))
      )
    val topology = pipeline(Processor.fromJava[IO](ranging, counts))
    task(topology, 0).flatMap: t =>
      t.opened(TaskState.empty, 0L).flatMap: state =>
        List("e", "a", "c", "b", "d").zipWithIndex.foldLeft(IO.pure((state, Vector.empty[TaskOutput]))): (so, next) =>
          so.flatMap((current, done) => t.process(current, input(next._1, "v", 10L, next._2.toLong), 0L).map((after, more) => (after, done ++ more)))
        .map((_, written) => assertEquals(values(written).last, "b,c,d|a,b|5"))
