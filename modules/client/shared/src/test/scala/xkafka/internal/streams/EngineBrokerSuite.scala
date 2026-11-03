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
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import internal.driver.StartAt
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{
  ApplicationId, JoinWindows, KStream, KTable, Pipeline, ProcessingGuarantee, Repartition, Serde, SessionWindows, SlidingWindows, Store, StreamsPhase,
  StreamsProblem, StreamsSettings, Suppressed, TimeWindows, Topology
}

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

  /** Counts by key in tumbling windows of a minute, taking records up to an hour late. */
  private def byMinute(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 1.hour)).count(Store("counts")).toStream
      .to(output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])

  /** Counts by key in tumbling windows of a minute, taking records up to ten seconds late, kept an hour, writing only each window's final count. */
  private def finalByMinute(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds)).withRetention(1.hour)
      .emitOnWindowClose.count(Store("counts")).toStream.to(output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])

  /** Counts as `finalByMinute` does, passing on each window's final count once it closes through the suppression `final`. */
  private def suppressedByMinute(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds)).count(Store("counts"))
      .suppress(Suppressed.untilWindowCloses("final")).toStream.to(output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])

  /** Reads the input as a table into the store `counts`, writing each change, a delete as a tombstone. */
  private def asTable(input: Topic, output: Topic): Pipeline[IO] = KTable(input, text, text, Store("counts")).toStream.to(output, text, text.option)

  /** Waits up to two minutes until `topic` holds at least `count` records. */
  private def holding(server: String, topic: Topic, count: Int)(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.bytes[IO].option,
          Deserializer.bytes[IO].option,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.take(count.toLong).compile.drain.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $count records")))

  /** Counts by key in sliding windows of five seconds, taking records up to an hour late. */
  private def bySliding(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupByKey(text, text).windowedBy(SlidingWindows(5.seconds, grace = 1.hour)).count(Store("counts")).toStream
      .to(output, Serde.timeWindowed(text, 5.seconds), Serde.long[IO])

  /** Counts by key in sessions with an inactivity gap of five seconds, taking records up to an hour late. */
  private def bySession(input: Topic, output: Topic): Pipeline[IO] =
    KStream(input, text, text).groupByKey(text, text).windowedBy(SessionWindows(5.seconds, grace = 1.hour)).count(Store("counts")).toStream
      .to(output, Serde.sessionWindowed(text), Serde.long[IO].option)

  /** Reads `topic` from its start until the latest count of each key in each session, by its start and end, or its tombstone, is `expected`. */
  private def sessionCounts(server: String, topic: Topic, expected: Map[(String, Long, Long), Option[Long]])(using
      guarantee: ProcessingGuarantee
  ): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Option[Long]]((_, _, value) => IO(value.map(bytes => ByteBuffer.wrap(bytes.toArray).getLong)))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Serde.sessionWindowed(text).deserializer,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[(String, Long, Long), Option[Long]]): (seen, record) =>
          val windowed = record.record.key
          seen.updated((windowed.key, windowed.window.start, windowed.window.end), record.record.value)
        .find(_ == expected).compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Writes a record for each key at each timestamp, in order. */
  private def produceAt(server: String, topic: Topic, records: List[(String, Long)]): IO[Unit] =
    val made = records.map((key, at) => ProducerRecord(topic, key, s"$key-value", timestamp = Some(Timestamp.fromEpochMillis(at))))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(made))).void

  /** Writes each key's value, or a tombstone. */
  private def produceValues(server: String, topic: Topic, records: List[(String, Option[String])]): IO[Unit] =
    val made = records.map((key, value) => ProducerRecord(topic, key, value))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO].option).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(made))).void

  /** Reads `topic` from its start until the latest value of each key, or its tombstone, is `expected`. */
  private def latestValues(server: String, topic: Topic, expected: Map[String, Option[String]])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          Deserializer.utf8[IO].option,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[String, Option[String]])((seen, record) => seen.updated(record.record.key, record.record.value))
          .find(_ == expected).compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Reads `topic` from its start until the latest count of each key in each window, by the window's start, is `expected`. */
  private def windowCounts(server: String, topic: Topic, expected: Map[(String, Long), Long])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Serde.timeWindowed(text, 1.minute).deserializer,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[(String, Long), Long]): (seen, record) =>
          seen.updated((record.record.key.key, record.record.key.window.start), record.record.value)
        .find(_ == expected).compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Reads `topic` from its start until it has held exactly the records `expected` gives, one for each key and window, and no more. */
  private def windowRecords(server: String, topic: Topic, expected: Map[(String, Long), Long])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Serde.timeWindowed(text, 1.minute).deserializer,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Vector.empty[((String, Long), Long)])((seen, record) =>
          seen :+ ((record.record.key.key, record.record.key.window.start) -> record.record.value)
        ).find(seen => seen.size == expected.size && seen.toMap == expected).compile.lastOrError
          .timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held exactly $expected"))).void

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

  /** Reads `topic` from its start until the latest count of each key is `expected`, failing after two minutes: under read_committed where the engine
    * writes exactly once, so that only what its committed transactions wrote is read.
    */
  private def latest(server: String, topic: Topic, expected: Map[String, Long])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[String, Long])((seen, record) => seen.updated(record.record.key, record.record.value)).find(_ == expected)
          .compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Runs the engine for `app` while `use` runs, failing if the engine stops first, committing every second at least once and at Java's interval
    * exactly once.
    */
  private def running(server: String, app: Counting)(use: IO[Unit])(using guarantee: ProcessingGuarantee): IO[Unit] =
    val interval = Option.when(guarantee == ProcessingGuarantee.AtLeastOnce)(1.second)
    Engine(
      PlatformKafkaClient(),
      KafkaClientPlatform.driver[IO],
      client(server),
      app.application,
      app.topology,
      guarantee,
      interval,
      purgeInterval = 1.second
    ).use(engine => engine.run.race(use).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the engine stopped")), IO.pure)))

  /** Waits up to a minute until each partition of `topic` starts where `group` committed it, past some records: under exactly once, a partition ends
    * with the marker of the transaction that committed it, after the offset committed.
    */
  private def purgedTo(server: String, group: String, topic: Topic): IO[Unit] =
    Names.fresh:
      val partitions = Set(0, 1).map(n => TopicPartition(topic, Partition.from(n).orFail))
      val settings   =
        ConsumerSettings
          .from(client(server), ConsumerGroup.from(summon[Names]("xkafka-engine-watermarks")).orFail, Deserializer.utf8[IO], Deserializer.utf8[IO])
          .orFail
      (admin(server), PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptyList.fromListUnsafe(partitions.toList).toNes))).tupled
        .use: (admin, consumer) =>
          def check(left: Int): IO[Unit] =
            (consumer.beginningOffsets(partitions), admin.listConsumerGroupOffsets(ConsumerGroup.from(group).orFail)).tupled.flatMap:
              (beginnings, committed) =>
                val ours = committed.view.filterKeys(partitions).mapValues(_.offset).toMap
                if ours.nonEmpty && ours.forall((partition, offset) => beginnings.get(partition).contains(offset)) && ours.values.exists(_.value > 0)
                then IO.unit
                else if left <= 0 then IO.raiseError(new AssertionError(s"$topic starts at $beginnings, committed at $ours"))
                else IO.sleep(500.millis) *> check(left - 1)
          check(120)

  /** Writes a record for each key in a transaction that aborts. */
  private def aborted(server: String, topic: Topic, keys: List[String]): IO[Unit] =
    Names.fresh:
      val id       = TransactionalId.from(summon[Names]("xkafka-engine-aborted")).orFail
      val settings = TransactionalProducerSettings.from(client(server), id, Serializer.utf8[IO], Serializer.utf8[IO]).orFail
      PlatformKafkaClient().transactionalProducer(settings).use: producer =>
        producer.transactionally: transaction =>
          transaction.produce(NonEmptyList.fromListUnsafe(keys.map(key => ProducerRecord(topic, key, "aborted")))) *>
            IO.raiseError(new RuntimeException("abort"))
        .attempt.void

  /** An engine for `app`, as `running` starts it, whose restores and the changelogs it follows are recorded. */
  private def watched(server: String, app: Counting, restores: Ref[IO, List[Map[TopicPartition, Long]]], follows: Ref[IO, List[Vector[StartAt]]])(
      using guarantee: ProcessingGuarantee
  ) =
    val interval                                         = Option.when(guarantee == ProcessingGuarantee.AtLeastOnce)(1.second)
    def watch(plane: Engine.Plane[IO]): Engine.Plane[IO] =
      new Engine.Plane[IO]:
        export plane.{ends, followed, poll, purge, read, write}
        def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], held: Map[String, Map[Chunk[Byte], Stored]]) =
          restores.update(_ :+ from) *> plane.restore(changelogs, from, held)
        def follow(starts: Vector[StartAt]) = follows.update(_ :+ starts) *> plane.follow(starts)
    Engine(PlatformKafkaClient(), KafkaClientPlatform.driver[IO], client(server), app.application, app.topology, guarantee, interval, watch = watch)

  /** Waits up to a minute for `condition` to hold. */
  private def eventually(what: String)(condition: IO[Boolean]): IO[Unit] =
    def loop(left: Int): IO[Unit] =
      condition.flatMap: holds =>
        if holds then IO.unit
        else if left <= 0 then IO.raiseError(new AssertionError(s"$what did not happen within a minute"))
        else IO.sleep(250.millis) *> loop(left - 1)
    loop(240)

  private val third = Map("b" -> 1, "d" -> 2)

  private val first  = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
  private val second = Map("a" -> 2, "c" -> 1, "e" -> 5)

  private def total(counts: Map[String, Int]*): Map[String, Long] = counts.toList.combineAll.view.mapValues(_.toLong).toMap

  for
    (release, server)         <- brokers
    given ProcessingGuarantee <- ProcessingGuarantee.values.toList
    under = summon[ProcessingGuarantee]
  do
    test(s"$release, $under: an engine counts each key's records, writing each count to its output and to its store's changelog"):
      counting(server): app =>
        produce(server, app.input, first) *>
          running(server, app)(latest(server, app.output, total(first)) *> latest(server, app.changelog, total(first)))

    test(s"$release, $under: a client runs a streams program, whose state reaches running"):
      counting(server): app =>
        val settings =
          StreamsSettings
            .from(client(server), ApplicationId.from(app.application).orFail, under, Option.when(under == ProcessingGuarantee.AtLeastOnce)(1.second))
            .orFail
        produce(server, app.input, first) *> PlatformKafkaClient().streams(settings, app.topology).use: streams =>
          val running = streams.state.find(_.phase == StreamsPhase.Running).compile.lastOrError.timeout(2.minutes).void
          streams.run.race(running *> latest(server, app.output, total(first))).flatMap:
            case Left(_)  => IO.raiseError(new AssertionError("the application stopped"))
            case Right(_) => IO.unit

    test(s"$release, $under: an engine joins each record with its key's value in a table read from a topic copartitioned with the stream"):
      counting(
        server,
        (orders, output) =>
          KStream(orders, text, text).join(KTable(Topic.from(s"${orders.value}-users").orFail, text, text, Store("users")))((o, u) => s"$o:$u")
            .to(output, text, text)
      ): app =>
        val users = Topic.from(s"${app.input.value}-users").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 2, 1).orFail))) *>
          produceValues(server, users, List("a" -> Some("ann"), "b" -> Some("bob"))) *> running(server, app):
            // Written once the users are in the table, so each order finds its user.
            latestValues(server, Topic.from(s"${app.application}-users-changelog").orFail, Map("a" -> Some("ann"), "b" -> Some("bob"))) *>
              produceValues(server, app.input, List("a" -> Some("o1"), "b" -> Some("o2"), "c" -> Some("o3"))) *>
              latestValues(server, app.output, Map("a" -> Some("o1:ann"), "b" -> Some("o2:bob")))

    test(s"$release, $under: an engine outer joins two tables read from copartitioned topics, a change to either updating the result"):
      counting(
        server,
        (plans, output) =>
          KTable(plans, text, text, Store("plans"))
            .outerJoin(KTable(Topic.from(s"${plans.value}-users").orFail, text, text, Store("users")), Store("accounts"), text)(_.fold(
              plan => s"$plan:-",
              user => s"-:$user",
              (plan, user) => s"$plan:$user"
            )).toStream.to(output, text, text.option)
      ): app =>
        val users = Topic.from(s"${app.input.value}-users").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 2, 1).orFail))) *> running(server, app):
          produceValues(server, users, List("a" -> Some("ann"), "b" -> Some("bob"))) *>
            latestValues(server, app.output, Map("a" -> Some("-:ann"), "b" -> Some("-:bob"))) *>
            produceValues(server, app.input, List("a" -> Some("gold"), "c" -> Some("free"))) *>
            latestValues(server, app.output, Map("a" -> Some("gold:ann"), "b" -> Some("-:bob"), "c" -> Some("free:-"))) *>
            produceValues(server, users, List("b" -> None)) *>
            latestValues(server, app.output, Map("a" -> Some("gold:ann"), "b" -> None, "c" -> Some("free:-"))) *> latestValues(
              server,
              Topic.from(s"${app.application}-accounts-changelog").orFail,
              Map("a" -> Some("gold:ann"), "b" -> None, "c" -> Some("free:-"))
            )

    test(s"$release, $under: an engine outer joins two streams in windows, sending a record that matched nothing once its window closes"):
      counting(
        server,
        (orders, output) =>
          KStream(orders, text, text).outerJoin(
            KStream(Topic.from(s"${orders.value}-payments").orFail, text, text),
            JoinWindows.withNoGrace(1.second),
            Store("pairs"),
            text,
            text,
            text
          )(_.fold(order => s"$order:-", payment => s"-:$payment", (order, payment) => s"$order:$payment")).to(output, text, text)
      ): app =>
        val payments = Topic.from(s"${app.input.value}-payments").orFail
        val keys     = List("a", "b", "c")
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(payments, 2, 1).orFail))) *> running(server, app):
          produceAt(server, app.input, List("a" -> 1000L, "b" -> 2000L)) *> produceAt(server, payments, List("a" -> 1500L, "c" -> 3000L)) *>
            latestValues(server, app.output, Map("a" -> Some("a-value:a-value"))) *>
            // A record well after the windows on each key's partition, more than a second of wall clock after the last look, closes them.
            IO.sleep(2.seconds) *> produceAt(server, app.input, keys.map(_ -> 60000L)) *>
            latestValues(server, app.output, Map("a" -> Some("a-value:a-value"), "b" -> Some("b-value:-"), "c" -> Some("-:c-value")))

    test(s"$release, $under: a stream and the table it joins in topics of different partition counts are refused by the broker, as for Java"):
      counting(
        server,
        (orders, output) =>
          KStream(orders, text, text).join(KTable(Topic.from(s"${orders.value}-users").orFail, text, text, Store("users")))((o, u) => s"$o:$u")
            .to(output, text, text)
      ): app =>
        val users    = Topic.from(s"${app.input.value}-users").orFail
        val settings = StreamsSettings.from(client(server), ApplicationId.from(app.application).orFail, under).orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 3, 1).orFail))) *>
          PlatformKafkaClient().streams(settings, app.topology).use: streams =>
            val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.IncorrectlyPartitionedTopics])).compile.lastOrError
            streams.run.race(refused.timeout(2.minutes)).flatMap:
              case Left(_)  => IO.raiseError(new AssertionError("the application stopped"))
              case Right(_) => IO.unit

    test(s"$release, $under: an engine counts through a repartition topic, which it writes and reads back"):
      // Each key's records have values starting with each letter in turn, so the letters' counts mix records of every key and both partitions.
      val letters = "xyz"
      counting(server, byLetter): app =>
        produce(server, app.input, first, (_, n) => s"${letters(n % 3)}-$n") *> running(server, app):
          latest(server, app.output, Map("x" -> 5L, "y" -> 3L, "z" -> 2L)) *>
            // Purged once read and committed.
            purgedTo(server, app.application, Topic.from(s"${app.application}-by-letter-repartition").orFail)

    if under == ProcessingGuarantee.ExactlyOnce then
      test(s"$release, $under: an engine reads only what committed transactions wrote to its input"):
        counting(server): app =>
          aborted(server, app.input, List("a", "b", "e")) *> produce(server, app.input, first) *>
            running(server, app)(latest(server, app.output, total(first)))

    test(s"$release, $under: a standby made active restores only what it had not read, and the counts go on"):
      counting(server): app =>
        val recorded = (IO.ref(List.empty[Map[TopicPartition, Long]]), IO.ref(List.empty[Vector[StartAt]])).tupled
        recorded.flatMap: (restores, follows) =>
          produce(server, app.input, first) *> watched(server, app, restores, follows).allocated.flatMap: (_, leaving) =>
            latest(server, app.output, total(first)) *> watched(server, app, restores, follows).use: stayer =>
              val following = follows.get.map(_.exists(_.nonEmpty))
              val promoted  = restores.get.map(_.exists(from => from.nonEmpty && from.values.exists(_ > 0)))
              val steps     =
                eventually("the second engine standing by for a task")(following) *> produce(server, app.input, second) *>
                  latest(server, app.output, total(first, second)) *> leaving *>
                  eventually("the second engine restoring its standby task from where it read to")(promoted) *> produce(server, app.input, third) *>
                  latest(server, app.output, total(first, second, third))
              stayer.run.race(steps).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the engine stopped")), IO.pure))

    test(s"$release, $under: an engine counts in windows, its window changelog made as Java declares it, and one started later goes on from it"):
      counting(server, byMinute): app =>
        IO.realTime.flatMap: now =>
          // Two whole minutes, a few minutes back, so every record falls where the test says.
          val first  = (now.toMillis / 60000L - 5L) * 60000L
          val second = first + 60000L
          val before = List(("a", first), ("a", first + 1000L), ("b", first + 2000L), ("a", second), ("a", first + 3000L))
          val kept   =
            admin(server).use(_.describeTopicConfigurations(NonEmptySet.one(app.changelog))).map: described =>
              val entries = described(app.changelog).flatMap(_.toOption).get
              (entries.get("cleanup.policy").map(_.value), entries.get("retention.ms").map(_.value))
          produceAt(server, app.input, before) *>
            running(server, app)(windowCounts(server, app.output, Map(("a", first) -> 3L, ("b", first) -> 1L, ("a", second) -> 1L))) *>
            kept.map(found =>
              assertEquals(
                found,
                (Some(ConfigurationValue.Present("compact,delete")), Some(ConfigurationValue.Present((60000L + 3600000L + 86400000L).toString)))
              )
            ) *> produceAt(server, app.input, List(("a", first + 4000L), ("b", second))) *> running(server, app)(
              windowCounts(server, app.output, Map(("a", first) -> 4L, ("b", first) -> 1L, ("a", second) -> 1L, ("b", second) -> 1L))
            )

    test(s"$release, $under: an engine counts in sliding windows, made where records are, its changelog kept as Java declares it"):
      counting(server, bySliding): app =>
        IO.realTime.flatMap: now =>
          val at   = (now.toMillis / 60000L - 5L) * 60000L
          val kept =
            admin(server).use(_.describeTopicConfigurations(NonEmptySet.one(app.changelog))).map: described =>
              described(app.changelog).flatMap(_.toOption).get.get("retention.ms").map(_.value)
          produceAt(server, app.input, List(("a", at), ("a", at + 1000L), ("b", at + 2000L))) *> running(server, app):
            // The first record's window, then the second's: the window starting just after the first, and its own, holding both.
            windowCounts(server, app.output, Map(("a", at - 5000L) -> 1L, ("a", at + 1L) -> 1L, ("a", at - 4000L) -> 2L, ("b", at - 3000L) -> 1L))
          *> kept.map(found => assertEquals(found, Some(ConfigurationValue.Present((2 * 5000L + 3600000L + 86400000L).toString))))

    test(s"$release, $under: an engine counts in sessions, a session merged into another written as a tombstone"):
      counting(server, bySession): app =>
        IO.realTime.flatMap: now =>
          val at = (now.toMillis / 60000L - 5L) * 60000L
          produceAt(server, app.input, List(("a", at), ("b", at + 1000L), ("a", at + 2000L))) *> running(server, app):
            sessionCounts(
              server,
              app.output,
              Map(("a", at, at) -> None, ("b", at + 1000L, at + 1000L) -> Some(1L), ("a", at, at + 2000L) -> Some(2L))
            )

    test(s"$release, $under: an engine emitting windows once they close writes each window's final count once"):
      counting(server, finalByMinute): app =>
        IO.realTime.flatMap: now =>
          val first  = (now.toMillis / 60000L - 5L) * 60000L
          val second = first + 60000L
          running(server, app):
            produceAt(server, app.input, List(("a", first), ("a", first + 1000L), ("b", first + 2000L))) *> holding(server, app.changelog, 3) *>
              // Past the second within which the engine looked for closed windows, a record that closes the first minute.
              IO.sleep(2.seconds) *> produceAt(server, app.input, List(("a", second + 15000L))) *>
              windowRecords(server, app.output, Map(("a", first) -> 2L, ("b", first) -> 1L))

    test(s"$release, $under: an engine passes on what a suppression held for another before it, once the window closes, its buffer compacted"):
      counting(server, suppressedByMinute): app =>
        IO.realTime.flatMap: now =>
          val first  = (now.toMillis / 60000L - 5L) * 60000L
          val buffer = Topic.from(s"${app.application}-final-store-changelog").orFail
          val policy =
            admin(server).use(_.describeTopicConfigurations(NonEmptySet.one(buffer))).map: described =>
              described(buffer).flatMap(_.toOption).get.get("cleanup.policy").map(_.value)
          running(server, app)(
            produceAt(server, app.input, List(("a", first), ("a", first + 1000L), ("b", first + 2000L))) *> holding(server, buffer, 2)
          ) *> policy.map(found => assertEquals(found, Some(ConfigurationValue.Present("compact")))) *> running(server, app):
            produceAt(server, app.input, List(("a", first + 75000L))) *>
              windowRecords(server, app.output, Map(("a", first) -> 2L, ("b", first) -> 1L))

    test(s"$release, $under: an engine reads a topic as a table, writing each change and logging it, a record without a value deleting its key"):
      counting(server, asTable): app =>
        produceValues(server, app.input, List("a" -> Some("1"), "b" -> Some("2"), "a" -> None, "c" -> None)) *> running(server, app):
          val held = Map("a" -> None, "b" -> Some("2"), "c" -> None)
          latestValues(server, app.output, held) *> latestValues(server, app.changelog, held)

    test(s"$release, $under: an engine started after another left goes on from the offsets it committed and the counts its store holds"):
      counting(server): app =>
        produce(server, app.input, first) *> running(server, app)(latest(server, app.output, total(first))) *> produce(server, app.input, second) *>
          running(server, app)(latest(server, app.output, total(first, second)))
