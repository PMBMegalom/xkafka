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

import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.serialization.Serdes as JavaSerdes
import org.apache.kafka.streams.{KafkaStreams as JavaKafkaStreams, StreamsBuilder, StreamsConfig, Topology as JavaTopology}
import org.apache.kafka.streams.kstream.{Materialized, Produced, SessionWindows as JavaSessionWindows, TimeWindows as JavaTimeWindows, WindowedSerdes}
import org.apache.kafka.streams.state.HostInfo

/** An xkafka member and a Java member of one application, the xkafka member's queries answered for the keys the Java member holds through
  * [[JavaQueries]].
  */
final class JavaQueriesBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 4.minutes

  private val server =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map(_.split('=')).collectFirst:
      case Array("4.4.0", broker) => broker

  private val text = Serde.utf8[IO]

  /** How many records each key gets. */
  private val keys = (0 until 20).map(n => s"key-$n" -> (n % 3 + 1)).toMap

  /** A Java member of `application` running what `program` builds, answering queries at `java:2`. */
  private def javaMember(broker: String, application: String, topology: JavaTopology): Resource[IO, JavaKafkaStreams] =
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, application)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    properties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams")
    properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "java:2")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("xkafka-java-queries").toString)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JavaSerdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JavaSerdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    Resource.make(IO.blocking { val streams = new JavaKafkaStreams(topology, properties); streams.start(); streams })(streams =>
      IO.blocking(streams.close(java.time.Duration.ofSeconds(30))).void
    )

  /** Runs an xkafka member and a Java member of one application, each key's records written to each `<application>-<input>` of `inputs`, until every
    * key `answered` checks answers right from the xkafka member, which asks the Java member through the stores `registered` gives it.
    */
  private def mixed(
      broker: String,
      javaProgram: (StreamsBuilder, String) => Unit,
      xkafka: String => Topology[IO],
      registered: JavaQueries => JavaQueries,
      answered: KafkaStreams[IO] => IO[Boolean],
      javaAnswered: JavaQuerying => Boolean,
      inputs: List[String] = List("input")
  ): IO[Unit] =
    Names.fresh:
      val application = summon[Names]("xkafka-java-queries")
      val client      = ClientSettings.from(NonEmptyList.one(broker)).orFail
      val written     = inputs.map(name => Topic.from(s"$application-$name").orFail)
      val endpoint    = Endpoint.from("java", 2).orFail
      val asked       = new AtomicInteger(0)
      val settings    = StreamsSettings.from(client, ApplicationId.from(application).orFail).orFail.withQueries(Endpoint.from("xkafka", 1).orFail)
      val topics      = (written :+ Topic.from(s"$application-output").orFail).map(NewTopic.from(_, 2, 1).orFail)
      val records     = written.flatMap(input => keys.toList.flatMap((key, count) => List.fill(count)(ProducerRecord(input, key, "v"))))
      def produced    =
        PlatformKafkaClient().producer(ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
          .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(records)).void)
      def eventually(who: String)(condition: IO[Boolean]): IO[Unit] = (IO.sleep(500.millis) *> condition).iterateUntil(identity)
        .timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$who never had every key answered"))).void
      val builder = new StreamsBuilder()
      javaProgram(builder, application)
      val program = builder.build()
      val made    = NonEmptySet.of(topics.head, topics.tail*)
      // A Java member with an endpoint stops if it joins before the broker knows its topics, so it starts once the broker describes them.
      val created =
        PlatformKafkaClient().admin(client).use: admin =>
          val known = admin.describeTopics(made.map(_.topic)).map(_.toSortedMap.values.forall(_.isRight))
          admin.createTopics(made) *> (IO.sleep(100.millis) *> known).iterateUntil(identity)
      created *> javaMember(broker, application, program).use: running =>
        val handler              = registered(JavaQueries.of(running, application))
        val forward: Forward[IO] =
          (to, request) =>
            if to == endpoint then IO(asked.incrementAndGet) *> IO.blocking(Chunk.array(handler.answer(request.toArray)))
            else IO.raiseError(new RuntimeException(s"no member at $to"))
        PlatformKafkaClient().streams(settings, xkafka(application), forward).use: streams =>
          // The Java member asks through its own metadata, reaching the xkafka member's answer and its own handler.
          val back: java.util.function.BiFunction[HostInfo, Array[Byte], Array[Byte]] =
            (to, request) =>
              if to.host == "java" then handler.answer(request)
              else streams.answer(Chunk.array(request)).unsafeRunSync()(using cats.effect.unsafe.implicits.global).toArray
          val asking   = JavaQuerying.of(running, program, application, back)
          def javaSide = IO.blocking(scala.util.Try(javaAnswered(asking)).getOrElse(false))
          streams.run.background.surround(produced *> eventually("the xkafka member")(answered(streams)) *> eventually("the Java member")(javaSide))
            .map(_ => assert(asked.get > 0, "the Java member was never asked"))

  for broker <- server.toList do
    test("an xkafka member and a Java member of one application each answer the other's queries of a count for the keys it holds"):
      val counts = QueryableStore.keyValue(Store("counts"), text, Serde.long[IO])
      mixed(
        broker,
        (builder, application) =>
          builder.stream[String, String](s"$application-input").groupByKey.count(Materialized.as("counts")).toStream
            .to(s"$application-output", Produced.`with`(JavaSerdes.String, JavaSerdes.Long)): Unit,
        application =>
          Topology.of(
            KStream(Named("read"), Topic.from(s"$application-input").orFail, text, text).groupByKey(text, text).count(Named("count"), Store("counts"))
              .toStream(Named("changes")).to(Named("write"), Topic.from(s"$application-output").orFail, text, Serde.long[IO])
          ).orFail,
        _.keyValue("counts", JavaSerdes.String, JavaSerdes.Long),
        streams => keys.toList.traverse((key, count) => streams.query(counts.get(key)).map(_ == Right(Some(count.toLong)))).map(_.forall(identity)),
        // The Java member's own queries, for keys either member holds, and of the whole store.
        asking =>
          keys.forall((key, count) => asking.get("counts", key, JavaSerdes.String, JavaSerdes.Long) == java.util.Optional.of(count.toLong)) &&
            asking.all("counts", JavaSerdes.String, JavaSerdes.Long).size == keys.size
      )

    test("an xkafka member's queries of windows and sessions are answered by a Java member for the keys it holds, each adding up to its records"):
      val windows  = QueryableStore.window(Store("tumbling"), text, Serde.long[IO])
      val sessions = QueryableStore.session(Store("sessions"), text, Serde.long[IO])
      mixed(
        broker,
        (builder, application) =>
          val hour = java.time.Duration.ofHours(1)
          builder.stream[String, String](s"$application-input-tumbling").groupByKey
            .windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(5), hour)).count(Materialized.as("tumbling")).toStream
            .to(s"$application-output", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 5000L), JavaSerdes.Long))
          builder.stream[String, String](s"$application-input-sessions").groupByKey
            .windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(java.time.Duration.ofMinutes(10), hour)).count(Materialized.as("sessions"))
            .toStream.to(s"$application-output", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), JavaSerdes.Long)): Unit
        ,
        application =>
          def read(name: String) = KStream(Named(s"read-$name"), Topic.from(s"$application-input-$name").orFail, text, text).groupByKey(text, text)
          val out                = Topic.from(s"$application-output").orFail
          Topology.of(
            read("tumbling").windowedBy(TimeWindows.tumbling(5.seconds, grace = 1.hour)).count(Named("count-windows"), Store("tumbling"))
              .toStream(Named("windows")).to(Named("write-windows"), out, Serde.timeWindowed(text, 5.seconds), Serde.long[IO]),
            read("sessions").windowedBy(SessionWindows(10.minutes, grace = 1.hour)).count(Named("count-sessions"), Store("sessions"))
              .toStream(Named("sessions")).to(Named("write-sessions"), out, Serde.sessionWindowed(text), Serde.long[IO].option)
          ).orFail
        ,
        _.window("tumbling", JavaSerdes.String, JavaSerdes.Long).session("sessions", JavaSerdes.String, JavaSerdes.Long),
        streams =>
          keys.toList.traverse: (key, count) =>
            (
              streams.query(windows.fetch(key, Timestamp.fromEpochMillis(0L), Timestamp.fromEpochMillis(Long.MaxValue))),
              streams.query(sessions.fetch(key))
            ).mapN((inWindows, inSessions) =>
              inWindows.map(_.map(_._2).sum) == Right(count.toLong) && inSessions.map(_.map(_._2).sum) == Right(count.toLong)
            )
          .map(_.forall(identity)),
        // The Java member's own query of every key's windows, each 5 s long.
        asking =>
          val found =
            asking.fetchAll(
              "tumbling",
              java.time.Instant.EPOCH,
              java.time.Instant.ofEpochMilli(Long.MaxValue),
              java.time.Duration.ofSeconds(5),
              JavaSerdes.String,
              JavaSerdes.Long
            ).asScala.toList
          found.groupMapReduce(_.key.key)(_.value.longValue)(_ + _) == keys.view.mapValues(_.toLong).toMap &&
          found.forall(window => window.key.window.end - window.key.window.start == 5000L)
        ,
        inputs = List("input-tumbling", "input-sessions")
      )
