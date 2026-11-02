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
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import internal.driver.StartAt
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{KStream, KTable, ProcessingGuarantee, Repartition, Serde, SessionWindows, SlidingWindows, Store, TimeWindows, Topology}

/** Java Streams and xkafka members in one application while records keep coming, as members join, leave, restart and are killed: counting records by
  * their value's first letter through a repartition topic, or each key's records in tumbling windows, sliding windows and sessions. The Java members
  * run in processes of their own, which the streams fixture starts, stops and kills as these tests ask (`xkafka.fixture.Fleet`), on the broker of the
  * newest release.
  */
final class FleetBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 6.minutes

  private val backend = PlatformKafkaClient.name

  private val server =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map(_.split('=')).collectFirst:
      case Array("4.4.0", broker) => broker

  private val text = Serde.utf8[IO]

  private def client(server: String): ClientSettings = ClientSettings.from(NonEmptyList.one(server)).orFail

  private val letters = "abcde"

  /** What the fleet's members run, by the name the fixture's members know it by. */
  private enum Program(val name: String) derives CanEqual:
    /** Counting records by their value's first letter, through a repartition topic. */
    case Letters extends Program("letters")

    /** Counting each key's records in tumbling windows of 5 s, sliding windows of 2.5 s and sessions with a gap of 1.5 s, each with an hour of grace.
      */
    case Windows extends Program("windows")

    /** Joining each order with its user, from a table read from a topic. */
    case Joins extends Program("joins")

    /** Joining a table of each key's plan with the table of users, into a store. */
    case Tables extends Program("tables")

  private val windowKinds = List("tumbling", "sliding", "sessions")

  /** An application of the fleet's own. */
  private final case class Fleet(
      server: String,
      application: String,
      guarantee: ProcessingGuarantee,
      program: Program,
      statuses: Ref[IO, List[String]]
  ):
    val input: Topic  = Topic.from(s"$application-input").orFail
    val output: Topic = Topic.from(s"$application-output").orFail

    def inputOf(kind: String): Topic  = Topic.from(s"$application-input-$kind").orFail
    def outputOf(kind: String): Topic = Topic.from(s"$application-output-$kind").orFail

    /** The topics the program reads and writes. */
    def topics: List[Topic] =
      program match
        case Program.Letters => List(input, output)
        case Program.Windows => windowKinds.flatMap(kind => List(inputOf(kind), outputOf(kind)))
        case Program.Joins   => List(inputOf("users"), inputOf("orders"), output)
        case Program.Tables  => List(inputOf("users"), inputOf("plans"), output)

    val topology: Topology[IO] =
      program match
        case Program.Letters => Topology.of(
            KStream(input, text, text).groupBy(Repartition("by-letter"), text, text)((_, value) => value.take(1)).count(Store("counts")).toStream
              .to(output, text, Serde.long[IO])
          ).orFail
        case Program.Windows => Topology.of(
            KStream(inputOf("tumbling"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(5.seconds, grace = 1.hour))
              .count(Store("tumbling")).toStream.to(outputOf("tumbling"), Serde.timeWindowed(text, 5.seconds), Serde.long[IO]),
            KStream(inputOf("sliding"), text, text).groupByKey(text, text).windowedBy(SlidingWindows(2500.millis, grace = 1.hour))
              .count(Store("sliding")).toStream.to(outputOf("sliding"), Serde.timeWindowed(text, 2500.millis), Serde.long[IO]),
            KStream(inputOf("sessions"), text, text).groupByKey(text, text).windowedBy(SessionWindows(1500.millis, grace = 1.hour))
              .count(Store("sessions")).toStream.to(outputOf("sessions"), Serde.sessionWindowed(text), Serde.long[IO].option)
          ).orFail
        case Program.Joins => Topology.of(
            KStream(inputOf("orders"), text, text).join(KTable(inputOf("users"), text, text, Store("users")))((order, user) => s"$order:$user")
              .to(output, text, text)
          ).orFail
        case Program.Tables => Topology.of(
            KTable(inputOf("plans"), text, text, Store("plans"))
              .leftJoin(KTable(inputOf("users"), text, text, Store("users")), Store("accounts"), text)((plan, user) =>
                s"$plan:${user.getOrElse("-")}"
              ).toStream.to(output, text, text.option)
          ).orFail

    private def command(text: String): IO[Unit] =
      val control = Topic.from(s"xkafka-fleet-$backend-control").orFail
      PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
        .use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(control, "fleet", text)))).void

    private def reported(news: String, within: FiniteDuration): IO[Unit] =
      def loop(left: FiniteDuration): IO[Unit] =
        statuses.get.flatMap: seen =>
          if seen.contains(news) then IO.unit
          else if left <= Duration.Zero then IO.raiseError(new AssertionError(s"the fleet did not report '$news'"))
          else IO.sleep(250.millis) *> loop(left - 250.millis)
      loop(within)

    /** A Java member, running once acquired, and stopped, with its process ended, once released. */
    def java(id: String): Resource[IO, String] =
      val guaranteed = if guarantee == ProcessingGuarantee.ExactlyOnce then "exactly_once_v2" else "at_least_once"
      Resource.make(command(s"start $id $application $guaranteed ${program.name}") *> reported(s"$id running", 2.minutes).as(id)): _ =>
        command(s"stop $id") *> reported(s"$id ended", 2.minutes)

    /** Kills a Java member outright, waiting until its process has ended. */
    def kill(id: String): IO[Unit] = command(s"kill $id") *> reported(s"$id ended", 1.minute)

    /** An xkafka member, whose restores and the changelogs it follows are recorded. */
    def xkafka(restores: Ref[IO, List[Map[TopicPartition, Long]]], follows: Ref[IO, List[Vector[StartAt]]]): Resource[IO, Unit] =
      def watch(plane: Engine.Plane[IO]): Engine.Plane[IO] =
        new Engine.Plane[IO]:
          export plane.{ends, followed, poll, purge, read, write}
          def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], held: Map[String, Map[Chunk[Byte], Stored]]) =
            restores.update(_ :+ from) *> plane.restore(changelogs, from, held)
          def follow(starts: Vector[StartAt]) = follows.update(_ :+ starts) *> plane.follow(starts)
      val member =
        Engine(PlatformKafkaClient(), KafkaClientPlatform.driver[IO], client(server), application, topology, guarantee, watch = watch)
          .flatMap(engine => engine.run.background.void)
      // Each step is bounded, so a member that cannot start or stop says so.
      Resource.make(member.allocated.timeoutTo(2.minutes, IO.raiseError(new AssertionError("an xkafka member did not start within two minutes")))):
        (_, release) => release.timeoutTo(2.minutes, IO.raiseError(new AssertionError("an xkafka member did not stop within two minutes")))
      .void

  /** A fleet application with its topics, the fixture's news of its members read for as long as the test runs. */
  private def fleet(broker: String, guarantee: ProcessingGuarantee, program: Program)(test: Fleet => IO[Unit]): IO[Unit] =
    Names.fresh:
      val named = summon[Names](s"xkafka-fleet-$backend")
      IO.ref(List.empty[String]).flatMap: statuses =>
        // An id whose joined topics Java lists in an order xkafka knows, without which the broker may refuse the Java members (see
        // StreamsProblem.JavaMembersMayBeRefused).
        val app =
          LazyList.from(0).map(n => Fleet(broker, s"$named-$n", guarantee, program, statuses))
            .find(fleet => Declaration.unordered(fleet.application, fleet.topology).isEmpty).get
        val made = app.topics.map(NewTopic.from(_, 2, 1).orFail)
        val news =
          ConsumerSettings.from(
            client(broker),
            ConsumerGroup.from(summon[Names]("xkafka-fleet-news")).orFail,
            Deserializer.utf8[IO],
            Deserializer.utf8[IO],
            AutoOffsetReset.Earliest
          ).orFail
        val watching =
          PlatformKafkaClient().consumer(news, Selection.Topics(NonEmptySet.one(Topic.from(s"xkafka-fleet-$backend-status").orFail)))
            .use(_.records.evalMap(record => statuses.update(_ :+ record.record.value)).compile.drain)
        PlatformKafkaClient().admin(client(broker)).use(_.createTopics(NonEmptySet.of(made.head, made.tail*))) *>
          watching.background.surround(test(app))

  /** Writes records while it lasts, each valued by a letter in turn, keyed in turn over ten keys. What it gives stops the writing and then gives how
    * many records it wrote of each letter, every one of them written by then.
    */
  private def load(fleet: Fleet): Resource[IO, IO[Map[String, Long]]] =
    val producer = PlatformKafkaClient().producer(ProducerSettings.from(client(fleet.server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
    for
      written <- Resource.eval(IO.ref(Map.empty[String, Long]))
      writer  <- producer
      writing <-
        Resource.make(fs2.Stream.iterate(0L)(_ + 1).evalMap: n =>
          val letter = letters((n % letters.length).toInt).toString
          // Written and counted together, so stopping the load leaves no record uncounted.
          (writer.produceAndAwait(NonEmptyList.one(ProducerRecord(fleet.input, s"k${n % 10}", s"$letter$n"))) *>
            written.update(counts => counts.updated(letter, counts.getOrElse(letter, 0L) + 1))).uncancelable *> IO.sleep(20.millis)
        .compile.drain.start)(_.cancel)
    yield writing.cancel *> written.get

  /** Reads the output from its start until each letter's latest count is what was written, or at least that, failing after three minutes. */
  private def counted(fleet: Fleet, written: Map[String, Long]): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val exactly  = fleet.guarantee == ProcessingGuarantee.ExactlyOnce
      val settings =
        ConsumerSettings.from(
          client(fleet.server),
          ConsumerGroup.from(summon[Names]("xkafka-fleet-reader")).orFail,
          Deserializer.utf8[IO],
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if exactly then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      def done(seen: Map[String, Long]) =
        written.forall((letter, count) => seen.get(letter).exists(found => if exactly then found == count else found >= count))
      IO.ref(Map.empty[String, Long]).flatMap: last =>
        PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(fleet.output))).use: consumer =>
          consumer.records.scan(Map.empty[String, Long])((seen, record) => seen.updated(record.record.key, record.record.value)).evalTap(last.set)
            .find(done).compile.lastOrError.void
            .timeoutTo(3.minutes, last.get.flatMap(seen => IO.raiseError(new AssertionError(s"the fleet's counts reached $seen, never $written"))))

  /** Writes records while it lasts, keyed in turn over ten keys, each at a record time 100 ms after the one before, so each key's records are a
    * second apart and in order, to each of the windows program's inputs. What it gives stops the writing and then gives each key's record times,
    * every record written by then.
    */
  private def windowsLoad(fleet: Fleet): Resource[IO, IO[Map[String, Vector[Long]]]] =
    val producer = PlatformKafkaClient().producer(ProducerSettings.from(client(fleet.server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
    for
      written <- Resource.eval(IO.ref(Map.empty[String, Vector[Long]]))
      // A whole ten seconds, so windows and sessions start where the checks expect.
      base    <- Resource.eval(IO.realTime.map(now => now.toMillis / 10000L * 10000L))
      writer  <- producer
      writing <-
        Resource.make(fs2.Stream.iterate(0L)(_ + 1).evalMap: n =>
          val key     = s"k${n % 10}"
          val at      = base + n * 100L
          val records = windowKinds.map(kind => ProducerRecord(fleet.inputOf(kind), key, s"v$n", timestamp = Some(Timestamp.fromEpochMillis(at))))
          (writer.produceAndAwait(NonEmptyList.fromListUnsafe(records)) *>
            written.update(times => times.updated(key, times.getOrElse(key, Vector.empty) :+ at))).uncancelable *> IO.sleep(20.millis)
        .compile.drain.start)(_.cancel)
    yield writing.cancel *> written.get

  /** Reads `topic` from its start, its keys read by `keys` and its values as counts or tombstones, until `done` holds of the latest count of each key
    * and window, by the window's start and end, failing after three minutes.
    */
  private def latest(fleet: Fleet, topic: Topic, keys: Deserializer[IO, xkafka.streams.Windowed[String]])(
      done: Map[(String, Long, Long), Option[Long]] => Boolean,
      wrong: Map[(String, Long, Long), Option[Long]] => List[String]
  ): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Option[Long]]((_, _, value) => IO(value.map(bytes => ByteBuffer.wrap(bytes.toArray).getLong)))
      val exactly  = fleet.guarantee == ProcessingGuarantee.ExactlyOnce
      val settings =
        ConsumerSettings.from(
          client(fleet.server),
          ConsumerGroup.from(summon[Names]("xkafka-fleet-reader")).orFail,
          keys,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if exactly then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      IO.ref(Map.empty[(String, Long, Long), Option[Long]]).flatMap: last =>
        PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
          consumer.records.scan(Map.empty[(String, Long, Long), Option[Long]]): (seen, record) =>
            val windowed = record.record.key
            seen.updated((windowed.key, windowed.window.start, windowed.window.end), record.record.value)
          .evalTap(last.set).find(done).compile.lastOrError.void.timeoutTo(
            3.minutes,
            last.get.flatMap(seen =>
              IO.raiseError(new AssertionError(s"$topic never counted what its records give: ${wrong(seen).take(20).mkString("; ")}"))
            )
          )

  /** Reads each windows program output until every window and session counts what each key's record times `written` give: exactly under exactly once,
    * at least that under at least once. Each key's records, a second apart, make one session from the first to the last, every other session of the
    * key ending as a tombstone.
    */
  private def windowsCounted(fleet: Fleet, written: Map[String, Vector[Long]]): IO[Unit] =
    val exactly                                           = fleet.guarantee == ProcessingGuarantee.ExactlyOnce
    def matches(found: Long, expected: Long)              = if exactly then found == expected else found >= expected
    def within(times: Vector[Long], from: Long, to: Long) = times.count(at => at >= from && at <= to).toLong
    val tumbling                                          =
      written.toList.flatMap((key, times) => times.groupBy(_ / 5000L * 5000L).map((start, in) => (key, start, start + 5000L) -> in.size.toLong)).toMap
    // In order, each record's window ends at it, and each record followed within the time difference starts a window just after it.
    val sliding =
      written.toList.flatMap: (key, times) =>
        val left  = times.map(at => (key, at - 2500L, at) -> within(times, at - 2500L, at))
        val right =
          times.zip(times.drop(1)).collect:
            case (at, next) if next <= at + 1 + 2500L => (key, at + 1, at + 1 + 2500L) -> within(times, at + 1, at + 1 + 2500L)
        left ++ right
      .toMap
    val sessions = written.map((key, times) => (key, times.head, times.last) -> times.size.toLong)
    def missed(expected: Map[(String, Long, Long), Long])(seen: Map[(String, Long, Long), Option[Long]]) =
      expected.toList.sortBy(_._1).collect:
        case (window, count) if !seen.get(window).flatten.exists(matches(_, count)) => s"$window counted ${seen.get(window).flatten}, not $count"
    def stray(seen: Map[(String, Long, Long), Option[Long]]) =
      seen.toList.sortBy(_._1).collect { case (session, Some(count)) if !sessions.contains(session) => s"$session counted $count, not a tombstone" }
    latest(fleet, fleet.outputOf("tumbling"), Serde.timeWindowed(text, 5.seconds).deserializer)(missed(tumbling)(_).isEmpty, missed(tumbling)) *>
      latest(fleet, fleet.outputOf("sliding"), Serde.timeWindowed(text, 2500.millis).deserializer)(missed(sliding)(_).isEmpty, missed(sliding)) *>
      latest(fleet, fleet.outputOf("sessions"), Serde.sessionWindowed(text).deserializer)(
        seen => missed(sessions)(seen).isEmpty && stray(seen).isEmpty,
        seen => missed(sessions)(seen) ++ stray(seen)
      )

  /** Writes a user for each of ten keys, waiting until the table's changelog holds them, then records to `topic` while it lasts, `o<n>`, keyed in
    * turn over the ten keys. What it gives stops the writing and then gives each record's key, every record written by then.
    */
  private def joinsLoad(fleet: Fleet, topic: String): Resource[IO, IO[Map[String, String]]] =
    val producer  = PlatformKafkaClient().producer(ProducerSettings.from(client(fleet.server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
    val users     = (0 until 10).toList.map(n => ProducerRecord(fleet.inputOf("users"), s"k$n", s"user$n"))
    val changelog =
      Names.fresh:
        val settings =
          ConsumerSettings.from(
            client(fleet.server),
            ConsumerGroup.from(summon[Names]("xkafka-fleet-reader")).orFail,
            Deserializer.utf8[IO],
            Deserializer.utf8[IO],
            AutoOffsetReset.Earliest
          ).orFail
        PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(Topic.from(s"${fleet.application}-users-changelog").orFail))).use:
          _.records.map(_.record.key).scan(Set.empty[String])(_ + _).find(_.size == 10).compile.drain
            .timeoutTo(2.minutes, IO.raiseError(new AssertionError("the users table never held every user")))
    for
      written <- Resource.eval(IO.ref(Map.empty[String, String]))
      writer  <- producer
      _       <- Resource.eval(writer.produceAndAwait(NonEmptyList.fromListUnsafe(users)) *> changelog)
      writing <-
        Resource.make(fs2.Stream.iterate(0L)(_ + 1).evalMap: n =>
          val key = s"k${n % 10}"
          (writer.produceAndAwait(NonEmptyList.one(ProducerRecord(fleet.inputOf(topic), key, s"o$n"))) *> written.update(_.updated(s"o$n", key)))
            .uncancelable *> IO.sleep(20.millis)
        .compile.drain.start)(_.cancel)
    yield writing.cancel *> written.get

  /** Reads the output from its start until every record written is there, joined with its key's user, and nothing else: once under exactly once, at
    * least once under at least once, failing after three minutes.
    */
  private def joined(fleet: Fleet, written: Map[String, String]): IO[Unit] =
    Names.fresh:
      val exactly  = fleet.guarantee == ProcessingGuarantee.ExactlyOnce
      val settings =
        ConsumerSettings.from(
          client(fleet.server),
          ConsumerGroup.from(summon[Names]("xkafka-fleet-reader")).orFail,
          Deserializer.utf8[IO],
          Deserializer.utf8[IO],
          AutoOffsetReset.Earliest,
          isolationLevel = if exactly then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      val expected                                = written.map((order, key) => s"$order:user${key.drop(1)}" -> key)
      def wrong(seen: Map[String, (String, Int)]) =
        expected.toList.sorted.collect { case (value, _) if !seen.get(value).exists((_, n) => if exactly then n == 1 else n >= 1) => value } ++
          seen.toList.sorted.collect { case (value, (key, _)) if !expected.get(value).contains(key) => s"$value under $key" }
      IO.ref(Map.empty[String, (String, Int)]).flatMap: last =>
        PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(fleet.output))).use: consumer =>
          consumer.records.scan(Map.empty[String, (String, Int)]): (seen, record) =>
            seen.updated(record.record.value, (record.record.key, seen.get(record.record.value).fold(0)(_._2) + 1))
          .evalTap(last.set).find(wrong(_).isEmpty).compile.lastOrError.void.timeoutTo(
            3.minutes,
            last.get.flatMap(seen => IO.raiseError(new AssertionError(s"the joined orders were wrong: ${wrong(seen).take(20).mkString("; ")}")))
          )

  /** The program's load, and the check that its outputs count everything the load wrote, for the scenarios to run whichever the program. */
  private def loaded(fleet: Fleet): Resource[IO, IO[Unit]] =
    fleet.program match
      case Program.Letters => load(fleet).map(_.flatMap(counted(fleet, _)))
      case Program.Windows => windowsLoad(fleet).map(_.flatMap(windowsCounted(fleet, _)))
      case Program.Joins   => joinsLoad(fleet, "orders").map(_.flatMap(joined(fleet, _)))
      // Each plan joins the user the table already holds for its key, so it comes out as an order joined with its user does.
      case Program.Tables => joinsLoad(fleet, "plans").map(_.flatMap(joined(fleet, _)))

  private def recording = (IO.ref(List.empty[Map[TopicPartition, Long]]), IO.ref(List.empty[Vector[StartAt]])).tupled

  /** Waits up to a minute for `condition` to hold. */
  private def eventually(what: String)(condition: IO[Boolean]): IO[Unit] =
    def loop(left: Int): IO[Unit] =
      condition.flatMap: holds =>
        if holds then IO.unit
        else if left <= 0 then IO.raiseError(new AssertionError(s"$what did not happen within a minute"))
        else IO.sleep(250.millis) *> loop(left - 1)
    loop(240)

  private val settle = 3.seconds

  for
    broker    <- server.toList
    guarantee <- ProcessingGuarantee.values.toList
    program   <- Program.values.toList
    named = s"${program.name}, $guarantee"
  do
    test(s"$named: tasks move between Java and xkafka members joining and leaving while records come, and every record is counted"):
      fleet(broker, guarantee, program): app =>
        recording.flatMap: (restores, follows) =>
          (app.xkafka(restores, follows).allocated, app.java(s"j1-${app.application}").allocated).tupled.flatMap: (x1, j1) =>
            loaded(app).use: stop =>
              IO.sleep(settle) *> app.xkafka(restores, follows).surround:
                IO.sleep(settle) *> app.java(s"j2-${app.application}").surround:
                  // The first xkafka member and the first Java member leave, one after the other, while the second of each runs.
                  IO.sleep(settle) *> x1._2 *> IO.sleep(settle) *> j1._2 *> IO.sleep(settle) *> stop

    test(s"$named: an xkafka member restarted while records come gives back its tasks and takes them again, and every record is counted"):
      fleet(broker, guarantee, program): app =>
        recording.flatMap: (restores, follows) =>
          app.java(s"j1-${app.application}").surround:
            loaded(app).use: stop =>
              IO.sleep(settle) *> app.xkafka(restores, follows).surround(IO.sleep(settle)) *> app.xkafka(restores, follows)
                .surround(IO.sleep(settle) *> stop)

    test(s"$named: a Java member killed outright while records come leaves its tasks to an xkafka member, and every record is counted"):
      fleet(broker, guarantee, program): app =>
        recording.flatMap: (restores, follows) =>
          app.xkafka(restores, follows).surround:
            app.java(s"j1-${app.application}").allocated.flatMap: (j1, _) =>
              loaded(app).use(stop => IO.sleep(settle) *> app.kill(j1) *> IO.sleep(10.seconds) *> stop)

    test(s"$named: an xkafka member standing by for a Java member restores only what it had not read once the Java member stops"):
      fleet(broker, guarantee, program): app =>
        recording.flatMap: (restores, follows) =>
          app.java(s"j1-${app.application}").allocated.flatMap: (_, stopJ1) =>
            loaded(app).use: stop =>
              IO.sleep(settle) *> app.xkafka(restores, follows).surround:
                eventually("the xkafka member standing by for a task")(follows.get.map(_.exists(_.nonEmpty))) *> IO.sleep(settle) *> stopJ1 *>
                  eventually("the xkafka member restoring a task from where its standby read to")(
                    restores.get.map(_.exists(from => from.values.exists(_ > 0)))
                  ) *> IO.sleep(settle) *> stop
