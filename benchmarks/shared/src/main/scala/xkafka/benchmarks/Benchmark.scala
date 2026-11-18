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
package benchmarks

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet, ValidatedNel}
import cats.effect.{IO, IOApp, Ref}
import cats.syntax.all.*
import fs2.Stream
import xkafka.streams.{ApplicationId, KStream, Named, Store, StreamsSettings, Topology}

/** Produce, consume and streams rates of one backend against the broker `XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS` names.
  *
  * Each measurement uses topics and groups of its own. A rate counts from the first record seen to the last, so the time a group takes to form is
  * left out of it and reported apart.
  */
object Benchmark extends IOApp.Simple:
  private val bootstrap   = Environment.get("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS").getOrElse("127.0.0.1:19092")
  private val backend     =
    System.getProperty("java.vm.name", "") match
      case "Scala.js"     => "js"
      case "Scala Native" => "native"
      case _              => "jvm"
  private val records     = Environment.get("XKAFKA_BENCH_RECORDS").fold(200000)(_.toInt)
  private val valueBytes  = Environment.get("XKAFKA_BENCH_VALUE_BYTES").fold(100)(_.toInt)
  private val keys        = Environment.get("XKAFKA_BENCH_KEYS").fold(1000)(_.toInt)
  private val partitions  = Environment.get("XKAFKA_BENCH_PARTITIONS").fold(6)(_.toInt)
  private val batch       = 1000
  private val maxInFlight = 64
  private val commitEvery = 1.second

  private val utf8  = Serde.utf8[IO]
  private val value = "x" * valueBytes

  /** When a measurement started, when it saw its first record, and when its last. */
  private final case class Timing(start: FiniteDuration, first: FiniteDuration, end: FiniteDuration):
    def report(name: String, count: Long): IO[Unit] =
      val total  = (end - start).toMillis.max(1L)
      val active = (end - first).toMillis.max(1L)
      val rate   = count * 1000.0 / active
      val mib    = rate * valueBytes / (1024 * 1024)
      IO.println(f"$backend%-6s $name%-28s $count%8d records  total ${total / 1000.0}%7.2f s  from first ${active / 1000.0}%7.2f s  $rate%10.0f records/s  $mib%7.2f MiB/s")

  override val run: IO[Unit] =
    val suffix = System.currentTimeMillis().toString
    for
      _      <- IO.println(s"$backend: $records records of $valueBytes bytes, $keys keys, $partitions partitions, broker $bootstrap")
      client <- validated(ClientSettings.from(NonEmptyList.one(bootstrap)))
      input  <- topic(s"xkafka-bench-input-$suffix")
      plain  <- topic(s"xkafka-bench-stateless-$suffix")
      counts <- topic(s"xkafka-bench-counts-$suffix")
      _      <- KafkaClient[IO].admin(client).use(_.createTopics(NonEmptySet.of(input, plain, counts).map(NewTopic.from(_, partitions, 1).toOption.get)))
      _      <- produce(client, input).flatMap(_.report("produce", records.toLong))
      _      <- consume(client, input, s"xkafka-bench-consume-$suffix").flatMap(_.report("consume", records.toLong))
      _      <- stateless(client, input, plain, suffix).flatMap(_.report("streams without a store", records.toLong))
      _      <- stateful(client, input, counts, suffix).flatMap(_.report("streams counting in a store", records.toLong))
    yield ()

  private def produce(client: ClientSettings, to: Topic): IO[Timing] =
    validated(ProducerSettings.from(client, utf8.serializer, utf8.serializer)).flatMap: settings =>
      KafkaClient[IO].producer(settings).use: producer =>
        val batches = Stream.range(0, records).chunkN(batch).map(chunk =>
          NonEmptyList.fromListUnsafe(chunk.toList.map(n => ProducerRecord(to, s"key-${n % keys}", value)))
        )
        for
          start <- IO.monotonic
          first <- Ref[IO].of(Option.empty[FiniteDuration])
          _     <- batches.through(producer.pipe(maxInFlight)).evalTap(_ => seen(first)).compile.drain
          end   <- IO.monotonic
          at    <- first.get
        yield Timing(start, at.getOrElse(start), end)

  private def consume(client: ClientSettings, from: Topic, group: String): IO[Timing] =
    readUntil(client, from, group, utf8.deserializer)(0L)((count, chunk) => count + chunk.size)(_ >= records)

  /** A program with no store: each record is kept, its value changed, and written on. */
  private def stateless(client: ClientSettings, from: Topic, to: Topic, suffix: String): IO[Timing] =
    val topology =
      Topology.of(
        KStream(Named("read"), from, utf8, utf8).filter(Named("keep"))((_, value) => value.nonEmpty).mapValues(Named("shorten"))(_.take(10))
          .to(Named("write"), to, utf8, utf8)
      )
    timedStreams(client, s"xkafka-bench-stateless-$suffix", topology):
      readUntil(client, to, s"xkafka-bench-stateless-read-$suffix", utf8.deserializer)(0L)((count, chunk) => count + chunk.size)(_ >= records)

  /** A program counting each key's records in a store, done once every key has its final count. */
  private def stateful(client: ClientSettings, from: Topic, to: Topic, suffix: String): IO[Timing] =
    val perKey   = (0 until records).groupBy(_ % keys).view.mapValues(_.size.toLong).toMap.map((n, count) => s"key-$n" -> count)
    val topology =
      Topology.of(
        KStream(Named("read"), from, utf8, utf8).groupByKey(utf8, utf8).count(Named("count"), Store("counts")).toStream(Named("changes"))
          .to(Named("write"), to, utf8, Serde.long[IO])
      )
    timedStreams(client, s"xkafka-bench-stateful-$suffix", topology):
      readUntil(client, to, s"xkafka-bench-stateful-read-$suffix", Serde.long[IO].deserializer)(Map.empty[String, Long])((latest, chunk) =>
        latest ++ chunk.toList.map(record => record.key -> record.value)
      )(_ == perKey)

  /** Runs the program while `read` waits for its output, and fails if the program stops first. */
  private def timedStreams(client: ClientSettings, application: String, topology: Either[NonEmptyList[xkafka.streams.TopologyError], Topology[IO]])(
      read: IO[Timing]
  ): IO[Timing] =
    for
      id       <- IO.fromEither(ApplicationId.from(application).leftMap(error => new IllegalArgumentException(error.message)))
      settings <- IO.fromEither(
        StreamsSettings.from(client, id, commitInterval = Some(commitEvery)).toEither
          .leftMap(errors => new IllegalArgumentException(errors.toList.map(_.message).mkString("; ")))
      )
      program <- IO.fromEither(topology.leftMap(errors => new IllegalArgumentException(errors.toList.map(_.message).mkString("; "))))
      timing  <- KafkaClient[IO].streams(settings, program).use: streams =>
        streams.run.race(read).flatMap(_.fold(_ => IO.raiseError(new IllegalStateException("the streams program stopped")), IO.pure))
    yield timing

  /** Reads `from` with a new group until `done` holds of what `step` has gathered from the records so far. */
  private def readUntil[V, A](client: ClientSettings, from: Topic, group: String, values: Deserializer[IO, V])(initial: A)(
      step: (A, fs2.Chunk[ConsumerRecord[String, V]]) => A
  )(done: A => Boolean): IO[Timing] =
    for
      id       <- IO.fromEither(ConsumerGroup.from(group).leftMap(error => new IllegalArgumentException(error.toString)))
      settings <- validated(ConsumerSettings.from(client, id, utf8.deserializer, values, AutoOffsetReset.Earliest))
      timing   <- KafkaClient[IO].consumer(settings, Selection.Topics(NonEmptySet.one(from))).use: consumer =>
        for
          start <- IO.monotonic
          first <- Ref[IO].of(Option.empty[FiniteDuration])
          _     <- consumer.records.chunks.evalTap(_ => seen(first)).scan(initial)((gathered, chunk) => step(gathered, chunk.map(_.record)))
            .takeThrough(gathered => !done(gathered)).compile.drain
          end <- IO.monotonic
          at  <- first.get
        yield Timing(start, at.getOrElse(start), end)
    yield timing

  private def seen(first: Ref[IO, Option[FiniteDuration]]): IO[Unit] = IO.monotonic.flatMap(now => first.update(_.orElse(Some(now))))

  private def topic(name: String): IO[Topic] = IO.fromEither(Topic.from(name).leftMap(error => new IllegalArgumentException(error.toString)))

  private def validated[A](result: ValidatedNel[SettingsError, A]): IO[A] =
    IO.fromEither(result.toEither.leftMap(errors => new IllegalArgumentException(errors.toList.map(_.message).mkString("; "))))
