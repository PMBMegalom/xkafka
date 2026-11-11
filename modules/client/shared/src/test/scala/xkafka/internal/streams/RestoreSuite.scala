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
package internal
package streams

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import driver.PolledRecord

/** Stores refilled from changelogs written in transactions, against the broker, on every platform. */
final class RestoreSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 2.minutes

  private val server  = PlatformKafkaClient.integrationBootstrapServers
  private val driver  = KafkaClientPlatform.driver[IO]
  private val backend = PlatformKafkaClient.name

  private def topic(label: String)(using names: Names): Topic = Topic.from(s"xkafka-restore-$backend-${names(label)}").orFail

  private def partition(topic: Topic, n: Int): TopicPartition = TopicPartition(topic, Partition.from(n).orFail)

  private def text(value: String): Chunk[Byte] = Chunk.array(value.getBytes)

  /** The record for `key` in partition `n`, at `millis`, with no key where `key` is empty and no value where `value` is none. */
  private def record(topic: Topic, n: Int, key: String, value: Option[String], millis: Long): ProducerRecord[Option[String], Option[String]] =
    ProducerRecord(topic, Option.when(key.nonEmpty)(key), value, Some(Partition.from(n).orFail), Some(Timestamp.fromEpochMillis(millis)))

  private def transactionally(server: String)(use: Transaction[IO, Option[String], Option[String]] => IO[Unit]): IO[Unit] =
    for
      client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
      names    <- Names.draw
      id       <- TransactionalId.from(s"xkafka-restore-$backend-${names("transaction")}").liftTo[IO]
      settings <- TransactionalProducerSettings.from(client, id, Serializer.utf8[IO].option, Serializer.utf8[IO].option).liftTo[IO]
      _        <- PlatformKafkaClient().transactionalProducer(settings).use(_.transactionally(use))
    yield ()

  private def create(server: String, topics: Topic*): IO[Unit] =
    for
      client <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
      made   <- topics.toList.traverse(NewTopic.from(_, 2, 1).liftTo[IO])
      _      <-
        PlatformKafkaClient().admin(client)
          .use(_.createTopics(NonEmptySet.fromSetUnsafe(scala.collection.immutable.SortedSet.from(made)(using Ordering.by(_.topic.value)))))
    yield ()

  for
    broker    <- server.toList
    isolation <- List("read_uncommitted", "read_committed")
  do
    test(s"a global table under $isolation has read its whole topic once acquired, however many polls that takes"):
      Names.fresh:
        val table    = topic("global")
        val keys     = (0 until 2000).toList.map(n => s"key-$n")
        val text     = xkafka.Serde.utf8[IO]
        val read     = xkafka.streams.GlobalKTable(xkafka.streams.Named("g"), table, text, text, xkafka.streams.Store("gs"))
        val consumer =
          Resource.make(
            driver.consumer(broker, None, s"xkafka-restore-$backend", "earliest", Map("isolation.level" -> isolation), reportEnds = true)
          )(driver.destroyConsumer)
        create(broker, table) *> transactionally(broker)(
          _.produce(NonEmptyList.fromListUnsafe(keys.zipWithIndex.map((key, n) => record(table, n % 2, key, Some("v"), 1000L)))).void
        ) *> consumer.use: client =>
          GlobalStores(driver, client, List(read), _ => IO.pure(2), TaskStore.memory[IO]).use: held =>
            held.stores("gs").all.map(all => assertEquals(all.map(key => new String(key._1.toArray)).toSet, keys.toSet))

  test(
    "each store's changes from a batch of its changelog keep each key's latest value with its record's time, and a record without a value removes it"
  ):
    def record(key: Option[String], value: Option[String], at: Option[Long]) =
      PolledRecord("c", 0, 0L, at, 0, -1, key.map(text), value.map(text), Headers.empty)
    val made =
      Restore.changes(List(
        "a" -> record(Some("k"), Some("1"), Some(5L)),
        "a" -> record(Some("k"), Some("2"), Some(6L)),
        "b" -> record(Some("k"), None, Some(7L)),
        "a" -> record(None, Some("x"), Some(8L)),
        "a" -> record(Some("j"), Some("3"), None)
      ))
    assertEquals(
      made,
      Map(
        "a" -> StoreContents.unchanged.updated(text("k"), Some(Stored(text("2"), 6L))).updated(text("j"), Some(Stored(text("3"), -1L))),
        "b" -> StoreContents.unchanged.updated(text("k"), None)
      )
    )

  test("each store holds each partition past the last record read from it, or to its end once finished, the furthest of the two"):
    def at(partition: Int, offset: Long) = PolledRecord("c", partition, offset, Some(1L), 0, -1, Some(text("k")), Some(text("v")), Headers.empty)
    val stores                           = Map(("c", 0) -> "a", ("c", 1) -> "a", ("c", 2) -> "b")
    assertEquals(
      Restore.reached(
        List(at(0, 4L), at(0, 2L), at(1, 7L), at(2, 1L)),
        Set(("c", 1), ("c", 2), ("c", 3)),
        Map(("c", 1) -> 5L, ("c", 2) -> 9L),
        stores.get
      ),
      Map("a" -> Map(0 -> 5L, 1 -> 8L), "b" -> Map(2 -> 9L))
    )

  test("a partition is finished by reaching where it was found to end, not by a report of an end before that"):
    def end(partition: Int, offset: Long) = xkafka.internal.driver.PartitionEnd("t", partition, offset)
    val left                              = Set(("t", 0), ("t", 1))
    val ends                              = Map(("t", 0) -> 10L, ("t", 1) -> 0L)
    assertEquals(Restore.unfinished(left, List(end(0, 0L)), ends), left)
    assertEquals(Restore.unfinished(left, List(end(0, 9L)), ends), left)
    assertEquals(Restore.unfinished(left, List(end(0, 10L)), ends), Set(("t", 1)))
    assertEquals(Restore.unfinished(left, List(end(0, 11L), end(1, 0L)), ends), Set.empty)
    assertEquals(Restore.unfinished(left, Nil, ends), left)

  private def failing(code: ErrorCode) = new KafkaException.BackendFailure("no", Some(code), Some(code.retriable))

  test("asking where a partition ends is asked again while the broker is moving, until it answers"):
    TestControl.executeEmbed:
      IO.ref(0).flatMap: asked =>
        val ask = asked.updateAndGet(_ + 1).flatMap(n => if n < 4 then IO.raiseError(failing(ErrorCode.NotLeaderOrFollower)) else IO.pure(n))
        Restore.whileMoving(ask).map(assertEquals(_, 4))

  test("asking where a partition ends gives up on a broker that keeps moving once the time is up"):
    TestControl.executeEmbed:
      for
        start  <- IO.monotonic
        failed <- Restore.whileMoving(IO.raiseError[Int](failing(ErrorCode.LeaderNotAvailable))).attempt
        now    <- IO.monotonic
      yield
        assertEquals(
          failed.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
          Some(ErrorCode.LeaderNotAvailable)
        )
        assert(now - start >= Restore.EndsTimeoutMillis.millis && now - start < Restore.EndsTimeoutMillis.millis + 1.second, now - start)

  test("asking where a partition ends is asked again while the client does not know the partition yet, as for a topic just made"):
    TestControl.executeEmbed:
      IO.ref(0).flatMap: asked =>
        val ask = asked.updateAndGet(_ + 1).flatMap(n => if n < 3 then IO.raiseError(failing(ErrorCode.UnknownTopicOrPartition)) else IO.pure(n))
        Restore.whileMoving(ask).map(assertEquals(_, 3))

  test("asking where a partition ends gives up on a partition still unknown once the time is up"):
    TestControl.executeEmbed:
      for
        start  <- IO.monotonic
        failed <- Restore.whileMoving(IO.raiseError[Int](failing(ErrorCode.UnknownTopicOrPartition))).attempt
        now    <- IO.monotonic
      yield
        assertEquals(
          failed.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
          Some(ErrorCode.UnknownTopicOrPartition)
        )
        assert(now - start >= Restore.EndsTimeoutMillis.millis && now - start < Restore.EndsTimeoutMillis.millis + 1.second, now - start)

  test("asking where a partition ends does not ask again after a failure that is not the broker moving"):
    IO.ref(0).flatMap: asked =>
      val ask = asked.update(_ + 1) *> IO.raiseError[Int](failing(ErrorCode.TopicAuthorizationFailed))
      Restore.whileMoving(ask).attempt *> asked.get.map(assertEquals(_, 1))

  for broker <- server.toList do
    test("a global table keeps the bytes of what its topic held before it was acquired as they are, without reading them through its serdes"):
      Names.fresh:
        val table    = topic("raw")
        val text     = xkafka.Serde.utf8[IO]
        val refusing =
          xkafka.Serde(text.serializer, Deserializer.instance[IO, String]((_, _, _) => IO.raiseError(new IllegalArgumentException("unread"))))
        val read     = xkafka.streams.GlobalKTable(xkafka.streams.Named("g"), table, text, refusing, xkafka.streams.Store("gs"))
        val consumer =
          Resource.make(
            driver.consumer(broker, None, s"xkafka-restore-$backend", "earliest", Map("isolation.level" -> "read_committed"), reportEnds = true)
          )(driver.destroyConsumer)
        create(broker, table) *> transactionally(broker)(_.produce(NonEmptyList.of(record(table, 0, "k", Some("raw"), 1000L))).void) *> consumer.use:
          client =>
            GlobalStores(driver, client, List(read), _ => IO.pure(2), TaskStore.memory[IO]).use: held =>
              held.stores("gs").all
                .map(all => assertEquals(all.map((key, stored) => (new String(key.toArray), new String(stored.value.toArray))), Vector("k" -> "raw")))

    test("a global table acquired again over the store it kept reads on from where it was, without reading its topic again"):
      Names.fresh:
        val table    = topic("again")
        val utf8     = xkafka.Serde.utf8[IO]
        val read     = xkafka.streams.GlobalKTable(xkafka.streams.Named("g"), table, utf8, utf8, xkafka.streams.Store("gs"))
        val consumer =
          Resource.make(
            driver.consumer(broker, None, s"xkafka-restore-$backend", "earliest", Map("isolation.level" -> "read_committed"), reportEnds = true)
          )(driver.destroyConsumer)
        TaskStore.inMemory[IO](StoreContents.empty).flatMap: kept =>
          // The same store each time the table is acquired, as a store on disk outlives the application.
          val open: TaskStore.Open[IO] = (_, _, _) => IO.pure(kept)
          def acquired                 = consumer.use(client => GlobalStores(driver, client, List(read), _ => IO.pure(2), open).use_)
          for
            _     <- create(broker, table) *> transactionally(broker)(_.produce(NonEmptyList.of(record(table, 0, "k", Some("v"), 1000L))).void)
            _     <- acquired
            first <- kept.positions
            // Read from the start again, the topic's record would overwrite this.
            _      <- kept.write(StoreContents.unchanged.updated(text("k"), Some(Stored(text("local"), 1L))))
            _      <- acquired
            second <- kept.get(text("k"))
          yield
            assert(first.get(0).exists(_ > 0L) && first.get(1).contains(0L), first)
            assertEquals(second.map(stored => new String(stored.value.toArray)), Some("local"))

    test("a global table acquired as soon as its topic is created waits for the topic's leaders"):
      Names.fresh:
        val text     = xkafka.Serde.utf8[IO]
        val consumer =
          Resource.make(
            driver.consumer(broker, None, s"xkafka-restore-$backend", "earliest", Map("isolation.level" -> "read_committed"), reportEnds = true)
          )(driver.destroyConsumer)
        (0 until 20).toList.traverse_ : n =>
          val table = topic(s"fresh-$n")
          val read  = xkafka.streams.GlobalKTable(xkafka.streams.Named("g"), table, text, text, xkafka.streams.Store("gs"))
          consumer.use: client =>
            create(broker, table) *> GlobalStores(driver, client, List(read), _ => IO.pure(2), TaskStore.memory[IO]).use: held =>
              held.stores("gs").all.map(all => assertEquals(all.size, 0))

    test("a global table under read_committed does not wait for a transaction still open, and does not hold what it wrote"):
      Names.fresh:
        val table    = topic("open")
        val text     = xkafka.Serde.utf8[IO]
        val read     = xkafka.streams.GlobalKTable(xkafka.streams.Named("g"), table, text, text, xkafka.streams.Store("gs"))
        val consumer =
          Resource.make(
            driver.consumer(broker, None, s"xkafka-restore-$backend", "earliest", Map("isolation.level" -> "read_committed"), reportEnds = true)
          )(driver.destroyConsumer)
        create(broker, table) *> transactionally(broker)(_.produce(NonEmptyList.of(record(table, 0, "done", Some("v"), 1000L))).void) *>
          transactionally(broker): open =>
            open.produce(NonEmptyList.of(record(table, 0, "open", Some("v"), 1000L))).void *> consumer.use: client =>
              GlobalStores(driver, client, List(read), _ => IO.pure(2), TaskStore.memory[IO]).use: held =>
                held.stores("gs").all.map(all => assertEquals(all.map(key => new String(key._1.toArray)).toSet, Set("done")))

  /** A consumer reading under read_committed and reporting the ends it reaches, as the engine's restore consumer does. */
  private def restoring(server: String): Resource[IO, driver.Client] =
    Resource.make(
      driver.consumer(server, None, s"xkafka-restore-$backend", "earliest", Map("isolation.level" -> "read_committed"), reportEnds = true)
    )(driver.destroyConsumer)

  server.foreach: server =>
    test(
      "a store holds the latest committed value of each key, without earlier or aborted writes, keys removed later or records without a key, across partitions"
    ):
      Names.fresh:
        val changelog = topic("latest")
        val empty     = topic("empty")
        for
          // Current times: records written in 1970 fall outside the broker's retention, which deletes them while the test runs.
          now <- IO.realTime.map(_.toMillis)
          _   <- create(server, changelog, empty)
          _   <-
            transactionally(server)(
              _.produce(NonEmptyList.of(
                record(changelog, 0, "k1", Some("a"), now),
                record(changelog, 0, "k2", Some("b"), now + 100),
                record(changelog, 1, "k5", Some("e"), now + 200),
                record(changelog, 1, "", Some("no key"), now + 210)
              )).void
            )
          _ <-
            transactionally(server)(t =>
              t.produce(NonEmptyList.one(record(changelog, 0, "k1", Some("aborted"), now + 250))) *> IO.raiseError(new RuntimeException("abort"))
            ).attempt
          _ <-
            transactionally(server)(
              _.produce(NonEmptyList.of(
                record(changelog, 0, "k2", None, now + 300),
                record(changelog, 0, "k3", Some("c"), now + 400),
                record(changelog, 0, "k1", Some("a2"), now + 500)
              )).void
            )
          kept <- TaskStore.inMemory[IO](StoreContents.empty)
          none <- TaskStore.inMemory[IO](StoreContents.empty)
          _    <-
            restoring(server).use: consumer =>
              Restore(
                driver,
                consumer,
                Map(
                  partition(changelog, 0) -> "kept",
                  partition(changelog, 1) -> "kept",
                  partition(empty, 0)     -> "none",
                  partition(empty, 1)     -> "none"
                ),
                Map.empty,
                Map("kept" -> kept, "none" -> none)
              )
          restored <- (kept.all, none.all).mapN((k, n) => Map("kept" -> StoreContents.of(k), "none" -> StoreContents.of(n)))
          ends     <- restoring(server).use(Restore.endsOf(driver, _, Vector(partition(changelog, 0), partition(changelog, 1))))
          at       <- (kept.position, none.position).tupled
        yield
          assertEquals(
            restored,
            Map(
              "kept" -> StoreContents.of(Map(
                text("k1") -> Stored(text("a2"), now + 500),
                text("k3") -> Stored(text("c"), now + 400),
                text("k5") -> Stored(text("e"), now + 200)
              )),
              "none" -> StoreContents.empty
            )
          )
          // Each store says it holds its changelog to where its partitions end, past the transactions' markers, and to the start of an empty one.
          assertEquals(at, (Some(ends.values.max), Some(0L)))
