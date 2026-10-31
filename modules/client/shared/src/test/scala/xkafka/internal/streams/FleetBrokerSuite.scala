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
import xkafka.streams.{KStream, ProcessingGuarantee, Repartition, Serde, Store, Topology}

/** Java Streams and xkafka members in one application, counting records by their value's first letter through a repartition topic while records keep
  * coming, as members join, leave, restart and are killed. The Java members run in processes of their own, which the streams fixture starts, stops
  * and kills as these tests ask (`xkafka.fixture.Fleet`), on the broker of the newest release.
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

  /** An application of the fleet's own. */
  private final case class Fleet(server: String, application: String, guarantee: ProcessingGuarantee, statuses: Ref[IO, List[String]]):
    val input: Topic  = Topic.from(s"$application-input").orFail
    val output: Topic = Topic.from(s"$application-output").orFail

    val topology: Topology[IO] =
      Topology.of(
        KStream(input, text, text).groupBy(Repartition("by-letter"), text, text)((_, value) => value.take(1)).count(Store("counts")).toStream
          .to(output, text, Serde.long[IO])
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
      Resource.make(command(s"start $id $application $guaranteed") *> reported(s"$id running", 2.minutes).as(id)): _ =>
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
  private def fleet(broker: String, guarantee: ProcessingGuarantee)(test: Fleet => IO[Unit]): IO[Unit] =
    Names.fresh:
      val application = summon[Names](s"xkafka-fleet-$backend")
      IO.ref(List.empty[String]).flatMap: statuses =>
        val app  = Fleet(broker, application, guarantee, statuses)
        val made = List(app.input, app.output).map(NewTopic.from(_, 2, 1).orFail)
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
  do
    test(s"$guarantee: tasks move between Java and xkafka members joining and leaving while records come, and every record is counted"):
      fleet(broker, guarantee): app =>
        recording.flatMap: (restores, follows) =>
          (app.xkafka(restores, follows).allocated, app.java(s"j1-${app.application}").allocated).tupled.flatMap: (x1, j1) =>
            load(app).use: stop =>
              IO.sleep(settle) *> app.xkafka(restores, follows).surround:
                IO.sleep(settle) *> app.java(s"j2-${app.application}").surround:
                  // The first xkafka member and the first Java member leave, one after the other, while the second of each runs.
                  IO.sleep(settle) *> x1._2 *> IO.sleep(settle) *> j1._2 *> IO.sleep(settle) *> stop.flatMap(counted(app, _))

    test(s"$guarantee: an xkafka member restarted while records come gives back its tasks and takes them again, and every record is counted"):
      fleet(broker, guarantee): app =>
        recording.flatMap: (restores, follows) =>
          app.java(s"j1-${app.application}").surround:
            load(app).use: stop =>
              IO.sleep(settle) *> app.xkafka(restores, follows).surround(IO.sleep(settle)) *> app.xkafka(restores, follows)
                .surround(IO.sleep(settle) *> stop.flatMap(counted(app, _)))

    test(s"$guarantee: a Java member killed outright while records come leaves its tasks to an xkafka member, and every record is counted"):
      fleet(broker, guarantee): app =>
        recording.flatMap: (restores, follows) =>
          app.xkafka(restores, follows).surround:
            app.java(s"j1-${app.application}").allocated.flatMap: (j1, _) =>
              load(app).use(stop => IO.sleep(settle) *> app.kill(j1) *> IO.sleep(10.seconds) *> stop.flatMap(counted(app, _)))

    test(s"$guarantee: an xkafka member standing by for a Java member restores only what it had not read once the Java member stops"):
      fleet(broker, guarantee): app =>
        recording.flatMap: (restores, follows) =>
          app.java(s"j1-${app.application}").allocated.flatMap: (_, stopJ1) =>
            load(app).use: stop =>
              IO.sleep(settle) *> app.xkafka(restores, follows).surround:
                eventually("the xkafka member standing by for a task")(follows.get.map(_.exists(_.nonEmpty))) *> IO.sleep(settle) *> stopJ1 *>
                  eventually("the xkafka member restoring a task from where its standby read to")(
                    restores.get.map(_.exists(from => from.values.exists(_ > 0)))
                  ) *> IO.sleep(settle) *> stop.flatMap(counted(app, _))
