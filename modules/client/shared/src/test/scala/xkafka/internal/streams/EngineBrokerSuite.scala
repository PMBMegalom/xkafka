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
package internal.streams

import java.nio.ByteBuffer

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{KStream, Pipeline, Repartition, Serde, Store, Topology}

/** The engine running a program against each broker release `scripts/integration-test.sh` starts with `--streams`, without Java members. */
final class EngineBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 5.minutes

  private val brokers =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map: entry =>
      val Array(release, server) = entry.split('=')
      (release, server)

  private val text = Serde.utf8[IO]

  /** An application of its own, counting its input's records into a store, and writing each new count to its output. */
  private final case class Counting(application: String, input: Topic, output: Topic, program: (Topic, Topic) => Pipeline[IO]):
    val changelog: Topic       = Topic.from(s"$application-counts-changelog").orFail
    val topology: Topology[IO] = Topology.of(program(input, output)).orFail

  /** Counts by key. */
  private def byKey(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupByKey(text, text).count(Store("counts")).toStream.to(output, text, Serde.long[IO])

  /** Counts by the value's first letter, through a repartition topic keyed by it. */
  private def byLetter(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupBy(Repartition("by-letter"), text, text)((_, value) => value.take(1)).count(Store("counts")).toStream
      .to(output, text, Serde.long[IO])

  private def counting(server: String, program: (Topic, Topic) => Pipeline[IO] = byKey)(test: Counting => IO[Unit]): IO[Unit] =
    Names.fresh:
      val application = summon[Names](s"xkafka-engine-${PlatformKafkaClient.name}")
      val app         = Counting(application, Topic.from(s"$application-input").orFail, Topic.from(s"$application-output").orFail, program)
      val made        = List(app.input, app.output).map(NewTopic.from(_, 2, 1).orFail)
      admin(server).use(_.createTopics(NonEmptySet.of(made.head, made.tail*))) *> test(app)

  private def client(server: String): ClientSettings = ClientSettings.from(NonEmptyList.one(server)).orFail

  private def admin(server: String) = PlatformKafkaClient().admin(client(server))

  /** Writes `count` records for each key, spread over both partitions by key, each valued by `value` from its key and its number. */
  private def produce(server: String, topic: Topic, counts: Map[String, Int], value: (String, Int) => String = (key, _) => s"$key-value"): IO[Unit] =
    val records = counts.toList.flatMap((key, count) => List.tabulate(count)(n => ProducerRecord(topic, key, value(key, n))))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(records))).void

  /** Reads `topic` from its start until the latest count of each key is `expected`, failing after two minutes. */
  private def latest(server: String, topic: Topic, expected: Map[String, Long]): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          counts,
          AutoOffsetReset.Earliest
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[String, Long])((seen, record) => seen.updated(record.record.key, record.record.value)).find(_ == expected)
          .compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Runs the engine for `app` while `use` runs, failing if the engine stops first. */
  private def running(server: String, app: Counting)(use: IO[Unit]): IO[Unit] =
    Engine(PlatformKafkaClient(), KafkaClientPlatform.driver[IO], client(server), app.application, app.topology, commitInterval = 1.second)
      .use(engine => engine.race(use).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the engine stopped")), IO.pure)))

  private val first  = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
  private val second = Map("a" -> 2, "c" -> 1, "e" -> 5)

  private def total(counts: Map[String, Int]*): Map[String, Long] = counts.toList.combineAll.view.mapValues(_.toLong).toMap

  brokers.foreach: (release, server) =>
    test(s"$release: an engine counts each key's records, writing each count to its output and to its store's changelog"):
      counting(server): app =>
        produce(server, app.input, first) *>
          running(server, app)(latest(server, app.output, total(first)) *> latest(server, app.changelog, total(first)))

    test(s"$release: an engine counts through a repartition topic, which it writes and reads back"):
      // Each key's records have values starting with each letter in turn, so the letters' counts mix records of every key and both partitions.
      val letters = "xyz"
      counting(server, byLetter): app =>
        produce(server, app.input, first, (_, n) => s"${letters(n % 3)}-$n") *>
          running(server, app)(latest(server, app.output, Map("x" -> 5L, "y" -> 3L, "z" -> 2L)))

    test(s"$release: an engine started after another left goes on from the offsets it committed and the counts its store holds"):
      counting(server): app =>
        produce(server, app.input, first) *> running(server, app)(latest(server, app.output, total(first))) *> produce(server, app.input, second) *>
          running(server, app)(latest(server, app.output, total(first, second)))
