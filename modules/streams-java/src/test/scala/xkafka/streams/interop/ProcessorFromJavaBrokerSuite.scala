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

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import munit.CatsEffectSuite
import org.apache.kafka.streams.processor.api.{Processor as JavaProcessor, ProcessorContext, Record as JavaRecord}
import org.apache.kafka.streams.state.KeyValueStore as JavaKeyValueStore

/** A Java processor run through xkafka against a broker: each task's instance starts with the task and closes with it, and its store comes back. */
final class ProcessorFromJavaBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 4.minutes

  private val server =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map(_.split('=')).collectFirst:
      case Array("4.4.0", broker) => broker

  private val text = Serde.utf8[IO]

  /** Counts each key's records in the store `counts`, sending on `key=count`, and counts its own instances' starts and closes. */
  private final class Counting(started: AtomicInteger, closed: AtomicInteger) extends JavaProcessor[String, String, String, String]:
    private val context                                               = new AtomicReference[ProcessorContext[String, String]]()
    override def init(opened: ProcessorContext[String, String]): Unit =
      context.set(opened); started.incrementAndGet: Unit
    override def process(record: JavaRecord[String, String]): Unit =
      val counts = context.get.getStateStore[JavaKeyValueStore[String, String]]("counts")
      val seen   = Option(counts.get(record.key)).fold(0)(_.toInt) + 1
      counts.put(record.key, seen.toString)
      context.get.forward(record.withValue(s"${record.key}=$seen"))
    override def close(): Unit = closed.incrementAndGet: Unit

  for broker <- server.toList do
    test("each task's Java instance starts with the task and closes when the application stops, and a later run goes on from its store"):
      Names.fresh:
        val client            = ClientSettings.from(NonEmptyList.one(broker)).orFail
        val application       = summon[Names]("xkafka-from-java")
        val input             = Topic.from(s"$application-input").orFail
        val output            = Topic.from(s"$application-output").orFail
        val (started, closed) = (new AtomicInteger(0), new AtomicInteger(0))
        val topology          =
          Topology.of(
            KStream(Named("read"), input, text, text)
              .process(Named("count"), Processor.fromJava[IO](() => new Counting(started, closed), JavaStore.keyValue(Store("counts"), text, text)))
              .to(Named("write"), output, text, text)
          ).orFail
        val settings = StreamsSettings.from(client, ApplicationId.from(application).orFail).orFail
        val producer = PlatformKafkaClient().producer(ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
        def produce(keys: List[String]) = producer.use(_.produceAndAwait(NonEmptyList.fromListUnsafe(keys.map(ProducerRecord(input, _, "v")))).void)
        def read(count: Int): IO[List[String]] =
          Names.fresh:
            val reading =
              ConsumerSettings.from(
                client,
                ConsumerGroup.from(summon[Names]("xkafka-from-java-reader")).orFail,
                Deserializer.utf8[IO],
                Deserializer.utf8[IO],
                AutoOffsetReset.Earliest
              ).orFail
            PlatformKafkaClient().consumer(reading, Selection.Topics(NonEmptySet.one(output)))
              .use(_.records.take(count.toLong).map(_.record.value).compile.toList)
              .timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$output never held $count records")))
        def running(test: IO[Unit]) =
          PlatformKafkaClient().streams(settings, topology)
            .use(streams => IO.race(streams.run, test).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the application stopped")), IO.pure)))
        val topics = List(input, output).map(NewTopic.from(_, 2, 1).orFail)
        for
          _      <- PlatformKafkaClient().admin(client).use(_.createTopics(NonEmptySet.of(topics.head, topics.tail*)))
          _      <- produce(List("a", "b", "a"))
          _      <- running(read(3).void)
          first  <- IO((started.get, closed.get))
          _      <- produce(List("a", "b"))
          _      <- running(read(5).void)
          seen   <- read(5)
          second <- IO((started.get, closed.get))
        yield
          // Two partitions, so two tasks, each starting its instance once in each run and closing it when the run stops.
          assertEquals(first, (2, 2))
          assertEquals(second, (4, 4))
          assertEquals(seen.filter(_.startsWith("a")).sorted, List("a=1", "a=2", "a=3"))
          assertEquals(seen.filter(_.startsWith("b")).sorted, List("b=1", "b=2"))

    test("a task moving to another member closes its Java instance on the member it leaves, while that member goes on"):
      Names.fresh:
        val client          = ClientSettings.from(NonEmptyList.one(broker)).orFail
        val application     = summon[Names]("xkafka-from-java-moving")
        val input           = Topic.from(s"$application-input").orFail
        val output          = Topic.from(s"$application-output").orFail
        val (first, second) = ((new AtomicInteger(0), new AtomicInteger(0)), (new AtomicInteger(0), new AtomicInteger(0)))
        def topology(counted: (AtomicInteger, AtomicInteger)) =
          Topology.of(
            KStream(Named("read"), input, text, text).process(
              Named("count"),
              Processor.fromJava[IO](() => new Counting(counted._1, counted._2), JavaStore.keyValue(Store("counts"), text, text))
            ).to(Named("write"), output, text, text)
          ).orFail
        val settings                                           = StreamsSettings.from(client, ApplicationId.from(application).orFail).orFail
        def member(counted: (AtomicInteger, AtomicInteger))    = PlatformKafkaClient().streams(settings, topology(counted))
        def waitFor(what: String)(holds: => Boolean): IO[Unit] = (IO.sleep(250.millis) *> IO(holds)).iterateUntil(identity)
          .timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$what did not happen"))).void
        val topics = List(input, output).map(NewTopic.from(_, 2, 1).orFail)
        PlatformKafkaClient().admin(client).use(_.createTopics(NonEmptySet.of(topics.head, topics.tail*))) *> member(first).use: one =>
          one.run.background.surround:
            waitFor("the first member starting both tasks")(first._1.get == 2) *> member(second).use: two =>
              two.run.background.surround:
                // The second member takes a task, so the first closes that task's instance while it still runs the other.
                waitFor("a task moving to the second member")(second._1.get >= 1 && first._2.get >= 1).map: _ =>
                  assert(first._1.get - first._2.get >= 1, s"the first member started ${first._1.get} and closed ${first._2.get}")
