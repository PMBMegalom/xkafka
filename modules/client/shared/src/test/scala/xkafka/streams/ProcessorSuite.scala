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

import scala.concurrent.duration.*

import cats.arrow.FunctionK
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.internal.streams.{StoreContents, Stored, Task, TaskInput, TaskOutput, TaskState}

/** What a processor reads, writes and returns, through a task over stores in memory. */
final class ProcessorSuite extends CatsEffectSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val in    = Topic.from("in").orFail
  private val out   = Topic.from("out").orFail

  private def bytes(value: String): Chunk[Byte] = Chunk.array(value.getBytes)

  private def input(key: String, value: String, at: Long = 1000L, headers: Headers = Headers.empty): TaskInput =
    TaskInput(Some(bytes(key)), Some(bytes(value)), at, headers, "in", 3, 7L)

  private def pipeline[K2, V2](processor: Processor[IO, String, String, K2, V2], key: Serde[IO, K2], value: Serde[IO, V2]): Pipeline[IO] =
    KStream(Named("read"), in, text, text).process(Named("proc"), processor).to(Named("write"), out, key, value)

  private def taskOf(pipeline: Pipeline[IO], held: Map[String, StoreContents] = Map.empty): IO[Task[IO]] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0, held)

  private def tally(store: DeclaredStore[IO, KeyValueStore[IO, String, Long]]) =
    Processor.withStore(store): (counts: KeyValueStore[IO, String, Long]) =>
      Processor.onRecord: (record: Record[String, String]) =>
        counts.get(record.key).flatMap: seen =>
          val total = seen.getOrElse(0L) + 1
          counts.put(record.key, total).as(List(record.withValue(total)))

  private def changelog(written: Vector[TaskOutput]) = written.filter(_.topic.value == "app-tally-changelog")

  private def valueOf(written: TaskOutput): Option[Long] = written.value.map(v => BigInt(v.toArray).toLong)

  test("a processor reads back what it wrote before the commit, and a commit keeps it for later records"):
    val store = KeyValueStore.of(Store("tally"), text, count)
    for
      task     <- taskOf(pipeline(tally(store), text, count))
      first    <- task.process(TaskState.empty, input("a", "x"), 0L)
      second   <- task.process(first._1, input("a", "y"), 0L)
      stored   <- task.stored(second._1)
      third    <- task.process(stored, input("a", "z", 1500L), 0L)
      contents <- task.contents(third._1, "tally")
    yield
      assertEquals(contents.get(bytes("a")).map(_.timestamp), Some(1500L))
      assertEquals(first._2.filter(_.topic == out).flatMap(valueOf), Vector(1L))
      assertEquals(second._2.filter(_.topic == out).flatMap(valueOf), Vector(2L))
      assertEquals(third._2.filter(_.topic == out).flatMap(valueOf), Vector(3L))
      assertEquals(contents.size, 1)

  test("a store written before a commit holds nothing until the commit, and what restored it is read"):
    val store = KeyValueStore.of(Store("tally"), text, count)
    val held  =
      Map("tally" -> StoreContents.of(List(bytes("a") -> Stored(Chunk.array(BigInt(41L).toByteArray.reverse.padTo(8, 0.toByte).reverse), 5L))))
    for
      task   <- taskOf(pipeline(tally(store), text, count), held)
      ran    <- task.process(TaskState.empty, input("a", "x"), 0L)
      before <- task.contents(TaskState.empty, "tally")
    yield
      assertEquals(ran._2.filter(_.topic == out).flatMap(valueOf), Vector(42L))
      assertEquals(before.toVector.map(_._2.timestamp), Vector(5L))

  test("a write goes to the changelog as it is made, in the task's partition, with the record's time and no headers"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val tagged = Headers.fromVector(Vector(Header("trace", Some(bytes("1")))))
    val twice  =
      Processor.withStore(store): (counts: KeyValueStore[IO, String, Long]) =>
        Processor.onRecord: (_: Record[String, String]) =>
          counts.put("first", 1L) *> counts.put("first", 2L) *> counts.delete("first").as(List.empty[Record[String, Long]])
    for
      task <- taskOf(pipeline(twice, text, count))
      ran  <- task.process(TaskState.empty, input("a", "x", 1234L, tagged), 0L)
    yield
      assertEquals(changelog(ran._2).map(valueOf), Vector(Some(1L), Some(2L), None))
      assertEquals(changelog(ran._2).map(_.partition), Vector.fill(3)(Some(0)))
      assertEquals(changelog(ran._2).map(_.timestamp), Vector.fill(3)(1234L))
      assertEquals(changelog(ran._2).map(_.headers), Vector.fill(3)(Headers.empty))
      assertEquals(changelog(ran._2).flatMap(_.key).distinct, Vector(bytes("first")))

  test("a handler that fails leaves nothing written, however much it wrote first"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val broken =
      Processor.withStore(store): (counts: KeyValueStore[IO, String, Long]) =>
        Processor.onRecord: (_: Record[String, String]) =>
          counts.put("a", 1L) *> IO.raiseError[List[Record[String, Long]]](new RuntimeException("no"))
    for
      task   <- taskOf(pipeline(broken, text, count))
      failed <- task.process(TaskState.empty, input("a", "x"), 0L).attempt
      held   <- task.contents(TaskState.empty, "tally")
    yield
      assertEquals(failed.left.toOption.map(_.getMessage), Some("no"))
      assert(held.isEmpty)

  test("a result keeps the record's time and headers unless it changes them, and the record says where it came from"):
    val tagged = Headers.fromVector(Vector(Header("trace", Some(bytes("1")))))
    val origin = RecordOrigin(in, Partition.from(3).orFail, Offset.from(7L).orFail)
    val seeing =
      Processor.withoutStores(
        Processor.onRecord[IO, String, String, String, String]: record =>
          IO.pure(List(
            record.withKey("kept").withValue(record.origin.fold("none")(found => if found == origin then "origin" else "other")),
            record.withKey("changed").withTimestamp(Timestamp.fromEpochMillis(9L)).withHeaders(Headers.empty)
          ))
      )
    for
      task <- taskOf(pipeline(seeing, text, text))
      ran  <- task.process(TaskState.empty, input("a", "x", 1234L, tagged), 0L)
    yield
      val written = ran._2.filter(_.topic == out)
      assertEquals(written.map(_.key), Vector(Some(bytes("kept")), Some(bytes("changed"))))
      assertEquals(written.map(_.value), Vector(Some(bytes("origin")), Some(bytes("x"))))
      assertEquals(written.map(_.timestamp), Vector(1234L, 9L))
      assertEquals(written.map(_.headers), Vector(tagged, Headers.empty))

  test("ranges, scans and their reverses follow the keys' bytes, with what the processor has not yet committed"):
    val store = KeyValueStore.of(Store("kept"), text, text)
    val found = IO.ref(Map.empty[String, List[String]])
    found.flatMap: seen =>
      def note(name: String, entries: fs2.Stream[IO, (String, String)]) =
        entries.map(_._1).compile.toList.flatMap(keys => seen.update(_.updated(name, keys)))
      val scanning =
        Processor.withStore(store): (kept: KeyValueStore[IO, String, String]) =>
          Processor.onRecord: (_: Record[String, String]) =>
            List("b", "a", "ab", "é", "c", "gone").traverse_(key => kept.put(key, "v")) *> kept.delete("gone") *> note("all", kept.all) *>
              note("reverseAll", kept.reverseAll) *> note("range", kept.range("a", "b")) *> note("reverseRange", kept.reverseRange("a", "b")) *>
              note("backwards", kept.range("c", "a")) *> note("prefix", kept.prefixScan("a", Serializer.utf8[IO]))
                .as(List.empty[Record[String, String]])
      for
        task <- taskOf(pipeline(scanning, text, text))
        _    <- task.process(TaskState.empty, input("k", "v"), 0L)
        keys <- seen.get
      yield assertEquals(
        keys,
        Map(
          "all"          -> List("a", "ab", "b", "c", "é"),
          "reverseAll"   -> List("é", "c", "b", "ab", "a"),
          "range"        -> List("a", "ab", "b"),
          "reverseRange" -> List("b", "ab", "a"),
          "backwards"    -> Nil,
          "prefix"       -> List("a", "ab")
        )
      )

  test("putIfAbsent writes only over nothing and delete removes, each giving what the key held"):
    val store    = KeyValueStore.of(Store("kept"), text, text)
    val checking =
      Processor.withStore(store): (kept: KeyValueStore[IO, String, String]) =>
        Processor.onRecord: (record: Record[String, String]) =>
          for
            first  <- kept.putIfAbsent("k", "1")
            second <- kept.putIfAbsent("k", "2")
            gone   <- kept.delete("k")
            none   <- kept.delete("k")
            after  <- kept.get("k")
          yield List(record.withValue(List(first, second, gone, none, after).toString))
    for
      task <- taskOf(pipeline(checking, text, text))
      ran  <- task.process(TaskState.empty, input("k", "v"), 0L)
    yield assertEquals(ran._2.filter(_.topic == out).flatMap(_.value), Vector(bytes("List(None, Some(1), Some(1), None, None)")))

  test("a key or prefix written as nothing fails the handler, and a value written as nothing removes the key"):
    val none: Serializer[IO, String] = Serializer.instance((_, _, _) => IO.pure(None))
    val noKeys                       = KeyValueStore.of(Store("no-keys"), Serde(none, text.deserializer), text)
    val noValues                     = KeyValueStore.of(Store("no-values"), text, Serde(none, text.deserializer))
    val prefixes                     = KeyValueStore.of(Store("prefixes"), text, text)
    val writes                       =
      Processor.withStores(noKeys, noValues, prefixes): (keys, values, scanned) =>
        Processor.onRecord: (record: Record[String, String]) =>
          val written =
            record.value match
              case "key"    => keys.put("k", "v")
              case "prefix" => scanned.prefixScan("k", none).compile.drain
              case _        => values.put("k", "v")
          written.as(List.empty[Record[String, String]])
    for
      task   <- taskOf(pipeline(writes, text, text))
      key    <- task.process(TaskState.empty, input("k", "key"), 0L).attempt
      prefix <- task.process(TaskState.empty, input("k", "prefix"), 0L).attempt
      value  <- task.process(TaskState.empty, input("k", "value"), 0L)
    yield
      assert(key.left.exists(_.getMessage.contains("'no-keys'")), key)
      assert(prefix.left.exists(_.getMessage.contains("'prefixes'")), prefix)
      assertEquals(value._2.filter(_.topic.value == "app-no-values-changelog").map(_.value), Vector(None))

  test("a processor's result may change the key, so grouping it needs a repartition topic"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val stream = KStream(Named("read"), in, text, text).process(Named("proc"), tally(store))
    val _: KStream.Flowing[IO, String, Long, KeyState.Changed, Pipeline[IO]] = stream
    assertEquals(compileErrors("""stream.groupByKey(text, count)""").nonEmpty, true)
    val grouped = stream.groupByKey(Repartition("by"), text, count).count(Named("counted"), Store("c"))
    assertEquals(Topology.of(grouped.toStream(Named("s")).to(Named("w"), out, text, count)).orFail.subtopologies.size, 2)

  test("two processors given the same store share it, in one sub-topology, and one store of the name exists"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val left   = KStream(Named("left"), in, text, text).process(Named("p1"), tally(store)).to(Named("w1"), out, text, count)
    val right  = KStream(Named("right"), Topic.from("in2").orFail, text, text).process(Named("p2"), tally(store)).to(Named("w2"), out, text, count)
    val shared = Topology.of(left, right).orFail
    assertEquals(shared.subtopologies.map(_.stores.map(_.value)), List(List("tally")))
    val task = Task.inMemory[IO]("app", shared.subtopologies.head, 0)
    for
      running <- task
      first   <- running.process(TaskState.empty, input("a", "x"), 0L)
      second  <- running.process(first._1, input("a", "y").copy(topic = "in2"), 0L)
    yield assertEquals(second._2.filter(_.topic == out).flatMap(valueOf), Vector(2L))

  test("two stores of one name that are not the same store are refused"):
    val left  = KStream(Named("left"), in, text, text).process(Named("p1"), tally(KeyValueStore.of(Store("tally"), text, count)))
    val right =
      KStream(Named("right"), Topic.from("in2").orFail, text, text).process(Named("p2"), tally(KeyValueStore.of(Store("tally"), text, count)))
    val both = Topology.of(left.to(Named("w1"), out, text, count), right.to(Named("w2"), out, text, count))
    assertEquals(both.left.map(_.toList), Left(List(TopologyError.DuplicateStore("tally"))))

  test("imapK runs the handler and its stores, and keeps a shared store shared"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val left   = KStream(Named("left"), in, text, text).process(Named("p1"), tally(store)).to(Named("w1"), out, text, count)
    val right  = KStream(Named("right"), Topic.from("in2").orFail, text, text).process(Named("p2"), tally(store)).to(Named("w2"), out, text, count)
    val id     = FunctionK.id[IO]
    val mapped = Topology.of(left.imapK(id)(id), right.imapK(id)(id)).orFail
    assertEquals(mapped.subtopologies.map(_.stores.map(_.value)), List(List("tally")))
    for
      running <- Task.inMemory[IO]("app", Topology.of(left, right).orFail.imapK(id)(id).subtopologies.head, 0)
      first   <- running.process(TaskState.empty, input("a", "x"), 0L)
      second  <- running.process(first._1, input("a", "y"), 0L)
    yield assertEquals(second._2.filter(_.topic == out).flatMap(valueOf), Vector(2L))

  test("the types of a processor written in place come from its stream and what it returns"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val stream =
      KStream(Named("read"), in, text, text).process(
        Named("proc"),
        Processor.withStore(store): counts =>
          Processor.onRecord: record =>
            counts.put(record.key, 1L).as(List(record.withValue(1L)))
      )
    val program = stream.to(Named("write"), out, text, count)
    assertEquals(Topology.of(program).orFail.subtopologies.flatMap(_.stores.map(_.value)), List("tally"))

  test("a processor shows in the description with its stores"):
    val store = KeyValueStore.of(Store("tally"), text, count)
    val shown = Topology.of(pipeline(tally(store), text, count)).orFail.describe.toString
    assert(shown.contains("Processor: proc (stores: [tally])"), shown)

  private def fixedPipeline[V2](processor: FixedKeyProcessor[IO, String, String, V2], value: Serde[IO, V2]): Pipeline[IO] =
    KStream(Named("read"), in, text, text).processValues(Named("proc"), processor).to(Named("write"), out, text, value)

  test("a processor that keeps the key reads back what it wrote, and sends its results on under the record's key"):
    val store = KeyValueStore.of(Store("tally"), text, count)
    val tally =
      FixedKeyProcessor.withStore(store): (counts: KeyValueStore[IO, String, Long]) =>
        FixedKeyProcessor.onRecord: (record: FixedKeyRecord[String, String]) =>
          counts.get(record.key).flatMap: seen =>
            val total = seen.getOrElse(0L) + 1
            counts.put(record.key, total).as(List(record.withValue(total)))
    for
      task   <- taskOf(fixedPipeline(tally, count))
      first  <- task.process(TaskState.empty, input("a", "x"), 0L)
      second <- task.process(first._1, input("a", "y", 1500L), 0L)
    yield
      assertEquals(second._2.filter(_.topic == out).flatMap(valueOf), Vector(2L))
      assertEquals(second._2.filter(_.topic == out).flatMap(_.key), Vector(bytes("a")))
      assertEquals(changelog(second._2).map(valueOf), Vector(Some(2L)))
      assertEquals(changelog(second._2).map(_.timestamp), Vector(1500L))

  test("a result of a processor that keeps the key has the record's time and headers unless it changes them, and the record says where it came from"):
    val tagged = Headers.fromVector(Vector(Header("trace", Some(bytes("1")))))
    val origin = RecordOrigin(in, Partition.from(3).orFail, Offset.from(7L).orFail)
    val seeing =
      FixedKeyProcessor.withoutStores(
        FixedKeyProcessor.onRecord[IO, String, String, String]: record =>
          val seen = (record.origin == Some(origin), record.timestamp.epochMillis, record.headers == tagged, record.value)
          IO.pure(List(record.withValue(seen.toString), record.withTimestamp(Timestamp.fromEpochMillis(9L)).withHeaders(Headers.empty)))
      )
    for
      task <- taskOf(fixedPipeline(seeing, text))
      ran  <- task.process(TaskState.empty, input("a", "x", 1234L, tagged), 0L)
    yield
      val written = ran._2.filter(_.topic == out)
      assertEquals(written.map(_.key), Vector(Some(bytes("a")), Some(bytes("a"))))
      assertEquals(written.map(_.value), Vector(Some(bytes("(true,1234,true,x)")), Some(bytes("x"))))
      assertEquals(written.map(_.timestamp), Vector(1234L, 9L))
      assertEquals(written.map(_.headers), Vector(tagged, Headers.empty))

  test("a processor that keeps the key can return nothing, and one that fails leaves nothing written"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val broken =
      FixedKeyProcessor.withStore(store): (counts: KeyValueStore[IO, String, Long]) =>
        FixedKeyProcessor.onRecord: (record: FixedKeyRecord[String, String]) =>
          counts.put(record.key, 1L) *>
            (if record.value == "fail" then IO.raiseError(new RuntimeException("no")) else IO.pure(List.empty[FixedKeyRecord[String, Long]]))
    for
      task   <- taskOf(fixedPipeline(broken, count))
      failed <- task.process(TaskState.empty, input("a", "fail"), 0L).attempt
      ran    <- task.process(TaskState.empty, input("a", "x"), 0L)
      held   <- task.contents(TaskState.empty, "tally")
    yield
      assert(failed.isLeft)
      assertEquals(held.size, 0)
      assertEquals(ran._2.filter(_.topic == out), Vector.empty)

  test("a processor that keeps the key cannot change it, and grouping its results needs a repartition topic only where one was needed before"):
    val same = FixedKeyProcessor.withoutStores(FixedKeyProcessor.onRecord[IO, String, String, String](record => IO.pure(List(record))))
    assert(compileErrors("""FixedKeyProcessor.onRecord[IO, String, String, String](record => IO.pure(List(record.withKey("b"))))""").nonEmpty)
    val kept = KStream(Named("read"), in, text, text).processValues(Named("proc"), same)
    val _: KStream.Flowing[IO, String, String, KeyState.AsRead, Pipeline[IO]] = kept
    val counted = kept.groupByKey(text, text).count(Named("counted"), Store("c")).toStream(Named("s")).to(Named("w"), out, text, count)
    assertEquals(Topology.of(counted).orFail.subtopologies.size, 1)
    val rekeyed = KStream(Named("read"), in, text, text).selectKey(Named("key"))((_, value) => value).processValues(Named("proc"), same)
    val _: KStream.Flowing[IO, String, String, KeyState.Changed, Pipeline[IO]] = rekeyed
    assertEquals(compileErrors("""rekeyed.groupByKey(text, text)""").nonEmpty, true)
    val regrouped =
      rekeyed.groupByKey(Repartition("by"), text, text).count(Named("counted"), Store("c")).toStream(Named("s")).to(Named("w"), out, text, count)
    assertEquals(Topology.of(regrouped).orFail.subtopologies.size, 2)

  test("a flow only through a processor that keeps the key has no use for a repartition topic, and one through a processor that may change it does"):
    val same     = FixedKeyProcessor.withoutStores(FixedKeyProcessor.onRecord[IO, String, String, String](record => IO.pure(List(record))))
    val changing = Processor.withoutStores(Processor.onRecord[IO, String, String, String, String](record => IO.pure(List(record))))
    val read     = KStream(Named("read"), in, text, text)
    val keeping  =
      Flow[IO, String, String].processValues(Named("p"), same).groupByKey(Repartition("r"), text, text).count(Named("c"), Store("s")).consumer
    val moving =
      Flow[IO, String, String].process(Named("p"), changing).groupByKey(Repartition("r"), text, text).count(Named("c"), Store("s")).consumer
    val id                              = FunctionK.id[IO]
    def problems(program: Pipeline[IO]) = Topology.of(program).fold(_.toList, _ => Nil)
    assertEquals(problems(read.into(keeping)), List(TopologyError.UnusedRepartition("r")))
    assertEquals(problems(read.into(keeping).imapK(id)(id)), List(TopologyError.UnusedRepartition("r")))
    assertEquals(problems(read.into(moving)), Nil)

  test("a processor that keeps the key opens each of its stores under its own name, shares them, and survives imapK"):
    val a                                                 = KeyValueStore.of(Store("a"), text, count)
    val b                                                 = KeyValueStore.of(Store("b"), text, count)
    val c                                                 = KeyValueStore.of(Store("c"), text, count)
    def filling(stores: KeyValueStore[IO, String, Long]*) =
      FixedKeyProcessor.onRecord: (record: FixedKeyRecord[String, String]) =>
        stores.toList.zipWithIndex.traverse_((store, n) => store.put(record.key, n.toLong)).as(List(record))
    val two   = FixedKeyProcessor.withStores(a, b)(filling(_, _))
    val three = FixedKeyProcessor.withStores(a, b, c)(filling(_, _, _))
    val id    = FunctionK.id[IO]
    val first = KStream(Named("r1"), in, text, text).processValues(Named("p1"), two).to(Named("w1"), out, text, text)
    val other = KStream(Named("r2"), Topic.from("in2").orFail, text, text).processValues(Named("p2"), three).to(Named("w2"), out, text, text)
    val made  = Topology.of(first, other).orFail
    assertEquals(made.subtopologies.size, 1)
    for
      task  <- Task.inMemory[IO]("app", made.imapK(id)(id).subtopologies.head, 0)
      ran   <- task.process(TaskState.empty, input("k", "x"), 0L)
      again <- task.process(ran._1, TaskInput(Some(bytes("k")), Some(bytes("x")), 1000L, Headers.empty, "in2", 3, 7L), 0L)
      held  <-
        List("a", "b", "c").traverse(name => task.contents(again._1, name).map(_.get(bytes("k")).map(stored => BigInt(stored.value.toArray).toLong)))
    yield assertEquals(held, List(Some(0L), Some(1L), Some(2L)))

  test("the types of a processor that keeps the key written in place come from its stream and what it returns"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val stream =
      KStream(Named("read"), in, text, text).processValues(
        Named("proc"),
        FixedKeyProcessor.withStore(store): counts =>
          FixedKeyProcessor.onRecord: record =>
            counts.put(record.key, 1L).as(List(record.withValue(1L)))
      )
    val program = stream.to(Named("write"), out, text, count)
    assertEquals(Topology.of(program).orFail.subtopologies.flatMap(_.stores.map(_.value)), List("tally"))

  private def ticking(schedule: Punctuation) =
    Processor.withoutStores(Processor.every(schedule)((at: Timestamp) => IO.pure(List(Record("tick", at.epochMillis.toString, at))))(
      Processor.onRecord[IO, String, String, String, String](_ => IO.pure(Nil))
    ))

  test("a punctuator under a millisecond is refused, on any clock, and one of a millisecond is not"):
    def problems(schedule: Punctuation) = Topology.of(pipeline(ticking(schedule), text, text)).left.toOption.map(_.toList).getOrElse(Nil)
    val reason                          = "its interval of 999 microseconds is less than a millisecond"
    assertEquals(problems(Punctuation.streamTime(999.micros)), List(TopologyError.InvalidPunctuation("proc", reason)))
    assertEquals(problems(Punctuation.wallClock(999.micros)), List(TopologyError.InvalidPunctuation("proc", reason)))
    assertEquals(problems(Punctuation.wallClock(Duration.Zero)).size, 1)
    assertEquals(problems(Punctuation.wallClock((-1).second)).size, 1)
    assertEquals(problems(Punctuation.streamTime(1.milli)), Nil)
    assert(TopologyError.InvalidPunctuation("proc", reason).message.contains("step 'proc'"))

  test("a task is due only when a punctuator is, by each clock, and a task without any never is"):
    for
      plain <- taskOf(pipeline(Processor.withoutStores(Processor.onRecord[IO, String, String, String, String](_ => IO.pure(Nil))), text, text))
      wall  <- taskOf(pipeline(ticking(Punctuation.wallClock(100.millis)), text, text))
      time  <- taskOf(pipeline(ticking(Punctuation.streamTime(100.millis)), text, text))
      read  <- time.process(TaskState.empty, input("a", "x", 5L), 0L)
      ran   <- time.punctuate(read._1, 0L)
      none  <- plain.punctuate(TaskState.empty, 1000L)
    yield
      val started = wall.started(TaskState.empty, 1000L)
      assert(!plain.punctuates)
      assert(!plain.due(TaskState.empty, 1000L))
      assertEquals(none._2, Vector.empty)
      assert(wall.punctuates)
      assert(!wall.due(started, 1099L))
      assert(wall.due(started, 1100L))
      assert(!time.due(time.started(TaskState.empty, 0L), 1000000L), "no record has been read")
      assert(time.due(read._1, 0L))
      assert(!time.due(ran._1, 0L))

  test("restarting from a committed stream time runs a stream-time punctuator as soon as it is asked, before any record"):
    for
      task <- taskOf(pipeline(ticking(Punctuation.streamTime(100.millis)), text, text))
      ran  <- task.punctuate(TaskState(Map.empty, Map("in" -> 250L)), 0L)
    yield
      val written = ran._2.filter(_.topic == out)
      assertEquals(written.map(_.value), Vector(Some(bytes("250"))))
      assertEquals(written.map(_.timestamp), Vector(250L))

  test("a punctuator that fails leaves the state as it was, and what a punctuator sends on reads as no record's"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val broken =
      Processor.withStore(store): (counts: KeyValueStore[IO, String, Long]) =>
        Processor.every(Punctuation.wallClock(1.milli)): (_: Timestamp) =>
          counts.put("a", 1L) *> IO.raiseError[List[Record[String, Long]]](new RuntimeException("no"))
        .apply(Processor.onRecord((_: Record[String, String]) => IO.pure(List.empty[Record[String, Long]])))
    val seeing =
      Processor
        .withoutStores(Processor.onRecord[IO, String, String, String, String](record => IO.pure(List(record.withValue(record.origin.toString)))))
    for
      failing <- taskOf(pipeline(broken, text, count))
      started = failing.started(TaskState.empty, 0L)
      failed <- failing.punctuate(started, 10L).attempt
      held   <- failing.contents(started, "tally")
      task   <-
        taskOf(
          KStream(Named("read"), in, text, text).process(Named("proc"), ticking(Punctuation.streamTime(100.millis))).process(Named("see"), seeing)
            .to(Named("write"), out, text, text)
        )
      read <- task.process(TaskState.empty, input("a", "x", 5L), 0L)
      ran  <- task.punctuate(read._1, 0L)
    yield
      assertEquals(failed.left.toOption.map(_.getMessage), Some("no"))
      assertEquals(held.size, 0)
      assertEquals(ran._2.filter(_.topic == out).map(_.value), Vector(Some(bytes("None"))))

  test("imapK keeps a processor's punctuators, those of one that keeps the key, and what they write"):
    val store = KeyValueStore.of(Store("tally"), text, count)
    val id    = FunctionK.id[IO]
    val fixed =
      FixedKeyProcessor.withStore(store): kept =>
        FixedKeyProcessor.every(Punctuation.wallClock(1.second))(at => kept.put("last", at.epochMillis)):
          FixedKeyProcessor.onRecord: (record: FixedKeyRecord[String, String]) =>
            IO.pure(List(record))
    for
      plain <-
        Task
          .inMemory[IO]("app", Topology.of(pipeline(ticking(Punctuation.wallClock(1.second)), text, text).imapK(id)(id)).orFail.subtopologies.head, 0)
      ticks <- plain.punctuate(plain.started(TaskState.empty, 0L), 1000L)
      task  <-
        Task.inMemory[IO](
          "app",
          Topology.of(KStream(Named("read"), in, text, text).processValues(Named("proc"), fixed).to(Named("write"), out, text, text)).orFail
            .imapK(id)(id).subtopologies.head,
          0
        )
      ran  <- task.punctuate(task.started(TaskState.empty, 0L), 1000L)
      held <- task.contents(ran._1, "tally")
    yield
      assertEquals(ticks._2.filter(_.topic == out).map(_.value), Vector(Some(bytes("1000"))))
      assertEquals(ran._2.filter(_.topic == out), Vector.empty)
      assertEquals(changelog(ran._2).map(_.timestamp), Vector(1000L))
      assertEquals(held.get(bytes("last")).map(_.timestamp), Some(1000L))

  test("the types of a punctuator written in place come from its stream and what it returns"):
    val store  = KeyValueStore.of(Store("tally"), text, count)
    val stream =
      KStream(Named("read"), in, text, text).process(
        Named("proc"),
        Processor.withStore(store): counts =>
          Processor.every(Punctuation.streamTime(1.minute)): at =>
            counts.all.compile.toList.map(_.map((user, total) => Record(user, total, at)))
          .apply:
            Processor.onRecord: record =>
              counts.put(record.key, 1L).as(List(record.withValue(1L)))
      )
    val program = stream.to(Named("write"), out, text, count)
    assertEquals(Topology.of(program).orFail.subtopologies.flatMap(_.punctuating.map(_.named)), List("proc"))

  private val windows = WindowStore.of(Store("tally"), text, count, size = 1.second, retention = 10.seconds)

  private def windowed(logic: (WindowStore[IO, String, Long], Record[String, String]) => IO[List[Record[String, Long]]]) =
    pipeline(Processor.withStore(windows)(kept => Processor.onRecord(record => logic(kept, record))), text, count)

  test("a window store whose size is under a millisecond, or whose retention is less than its size, is refused with every reason"):
    def refused(size: FiniteDuration, retention: FiniteDuration) =
      val store = WindowStore.of(Store("tally"), text, count, size = size, retention = retention)
      Topology.of(pipeline(Processor.withStore(store)(_ => Processor.onRecord(_ => IO.pure(List.empty[Record[String, Long]]))), text, count)).swap
        .toOption.toList.flatMap(_.toList)
    assertEquals(refused(1.milli, 1.milli), Nil)
    assertEquals(
      refused(1.second, 999.millis).map(_.message),
      List("the window store 'tally' cannot be used: its retention of 999 milliseconds is less than its window size of 1 second")
    )
    assertEquals(
      refused(1.micro, 1.milli).map(_.message),
      List("the window store 'tally' cannot be used: its window size of 1 microsecond is less than a millisecond")
    )
    assertEquals(refused(0.millis, 0.millis).size, 1)
    assertEquals(refused((-1).second, (-2).seconds).size, 2)

  test("a window store's retention sets its changelog's, so a store restored loses what has expired"):
    assertEquals(Topology.of(windowed((_, _) => IO.pure(Nil))).orFail.subtopologies.head.retentions, Map("tally" -> 10000L))

  test("a window store's writes reach its changelog under the key, the window's start and a sequence number, at the record's time"):
    for
      task <- taskOf(windowed((kept, record) => kept.put(record.key, 5L, Timestamp.fromEpochMillis(16L)).as(Nil)))
      read <- task.process(TaskState.empty, input("a", "x", 2000L), 0L)
    yield
      val written = changelog(read._2)
      assertEquals(written.flatMap(_.key.map(_.toVector)), Vector("a".getBytes.toVector ++ Vector[Byte](0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0)))
      assertEquals(written.map(_.timestamp), Vector(2000L))

  test("writes to a window store at the same moment are all kept"):
    val starts = (1 to 50).toList.map(n => Timestamp.fromEpochMillis(n * 100L))
    for
      task <-
        taskOf(windowed((kept, record) =>
          starts.parTraverse_(start => kept.put(record.key, start.epochMillis, start)) *>
            kept.all.compile.count.map(found => List(record.withValue(found)))
        ))
      read <- task.process(TaskState.empty, input("a", "x"), 0L)
    yield
      assertEquals(read._2.filter(_.topic == out).flatMap(_.value.map(_.size)), Vector(8))
      assertEquals(changelog(read._2).size, 50)
      assertEquals(read._2.filter(_.topic == out).flatMap(_.value).map(value => java.nio.ByteBuffer.wrap(value.toArray).getLong), Vector(50L))

  test("a handler that fails keeps nothing it wrote to a window store"):
    val boom  = new RuntimeException("boom")
    val logic =
      (kept: WindowStore[IO, String, Long], record: Record[String, String]) =>
        if record.value == "fail" then kept.put(record.key, 1L, record.timestamp) *> IO.raiseError(boom)
        else kept.fetch(record.key, Timestamp.fromEpochMillis(5000L)).map(held => List(record.withValue(held.getOrElse(-1L))))
    for
      task   <- taskOf(windowed(logic))
      failed <- task.process(TaskState.empty, input("a", "fail", 5000L), 0L).attempt
      read   <- task.process(TaskState.empty, input("a", "read", 5000L), 0L)
    yield
      assertEquals(failed.left.toOption, Some(boom))
      assertEquals(read._2.filter(_.topic == out).flatMap(_.value).map(value => java.nio.ByteBuffer.wrap(value.toArray).getLong), Vector(-1L))
      assertEquals(changelog(read._2), Vector.empty)

  test("a key a window store cannot write fails the record, naming the store, and a value written as none removes the window"):
    val nothing = Serde.utf8[IO].option
    val store   = WindowStore.of(Store("tally"), nothing, nothing, size = 1.second, retention = 10.seconds)
    val logic   =
      Processor.withStore(store): (kept: WindowStore[IO, Option[String], Option[String]]) =>
        Processor.onRecord[IO, String, String, String, String]: record =>
          record.value match
            case "no-key" => kept.put(None, Some("v"), record.timestamp).as(Nil)
            case _        => kept.put(Some("k"), Some("v"), record.timestamp) *> kept.put(Some("k"), None, record.timestamp) *>
                kept.all.compile.count.map(found => List(record.withValue(found.toString)))
    for
      task   <- taskOf(pipeline(logic, text, text))
      failed <- task.process(TaskState.empty, input("a", "no-key"), 0L).attempt
      read   <- task.process(TaskState.empty, input("a", "gone"), 0L)
    yield
      assertEquals(failed.left.toOption.map(_.getMessage), Some("a key of the store 'tally' serialized to nothing"))
      assertEquals(read._2.filter(_.topic == out).flatMap(_.value.map(value => String(value.toArray))), Vector("0"))

  test("a window store works in a processor that keeps the key, beside a key-value store, shared between processors, and through imapK"):
    val latest                                               = KeyValueStore.of(Store("latest"), text, count)
    val first: FixedKeyProcessor[IO, String, String, String] =
      FixedKeyProcessor.withStores(windows, latest): (kept, last) =>
        FixedKeyProcessor.onRecord: record =>
          kept.put(record.key, 1L, record.timestamp) *> last.put(record.key, 1L).as(List(record.withValue(record.value)))
    val second =
      Processor.withStore(windows): kept =>
        Processor.onRecord: (record: Record[String, String]) =>
          kept.fetchAll(Timestamp.fromEpochMillis(0L), record.timestamp).compile.count.map(found => List(record.withValue(found)))
    val program =
      KStream(Named("read"), in, text, text).processValues(Named("first"), first).process(Named("second"), second)
        .to(Named("write"), out, text, count)
    val mapped = Topology.of(program.imapK(FunctionK.id[IO])(FunctionK.id[IO])).orFail
    assertEquals(mapped.subtopologies.head.retentions, Map("tally" -> 10000L))
    Task.inMemory[IO]("app", mapped.subtopologies.head, 0).flatMap(_.process(TaskState.empty, input("a", "x", 2000L), 0L)).map: (_, written) =>
      assertEquals(written.filter(_.topic == out).flatMap(_.value).map(value => java.nio.ByteBuffer.wrap(value.toArray).getLong), Vector(1L))
      assertEquals(written.filter(_.topic != out).map(_.topic.value).toSet, Set("app-tally-changelog", "app-latest-changelog"))

  test("a punctuator behind many forks that merge back is found once, mapped or not, however many paths lead to it"):
    val depth = 40
    val tail  = Flow.asRead[IO, String, String].process(Named("proc"), ticking(Punctuation.wallClock(1.second))).to(Named("write"), out, text, text)
    val chain = (1 to depth).foldLeft(tail): (next, n) =>
      val back = Flow.asRead[IO, String, String].merged(Named(s"back-$n")).into(next)
      Flow.asRead[IO, String, String].split(Named(s"s-$n-")).branch(Named("a"))((_, v) => v.startsWith("a"))(_.into(back))
        .defaultBranch(Named("b"))(_.into(back))
    val program = KStream(Named("read"), in, text, text).into(chain)
    assertEquals(Topology.of(program).orFail.subtopologies.flatMap(_.punctuating.map(_.named)), List("proc"))
    assertEquals(
      Topology.of(program.imapK(FunctionK.id[IO])(FunctionK.id[IO])).orFail.subtopologies.flatMap(_.punctuating.map(_.named)),
      List("proc")
    )

  test("a punctuator is found however its processor is reached, once, and only in its own sub-topology"):
    val merged = Flow[IO, String, String].process(Named("after"), ticking(Punctuation.wallClock(1.second))).to(Named("write"), out, text, text)
    val first  = KStream(Named("left"), in, text, text).mapValues(Named("m1"))(identity).into(merged)
    val second = KStream(Named("right"), Topic.from("in2").orFail, text, text).mapValues(Named("m2"))(identity).into(merged)
    val apart  =
      KStream(Named("read3"), Topic.from("in3").orFail, text, text).selectKey(Named("sk"))((_, v) => v).repartition(Repartition("r"), text, text)
        .process(Named("far"), ticking(Punctuation.wallClock(1.second))).to(Named("write3"), out, text, text)
    val joined =
      KStream(Named("left4"), Topic.from("in4").orFail, text, text).join(
        Named("j"),
        KStream(Named("right4"), Topic.from("in5").orFail, text, text),
        JoinWindows.withNoGrace(1.minute),
        Store("js"),
        text,
        text,
        text
      )(_ + _).process(Named("after-join"), ticking(Punctuation.wallClock(1.second))).to(Named("write4"), out, text, text)
    assertEquals(Topology.of(joined).orFail.subtopologies.flatMap(_.punctuating.map(_.named)), List("after-join"))
    val made = Topology.of(first, second, apart).orFail
    assertEquals(made.subtopologies.flatMap(_.punctuating.map(_.named)).sorted, List("after", "far"))
    assertEquals(made.imapK(FunctionK.id[IO])(FunctionK.id[IO]).subtopologies.flatMap(_.punctuating.map(_.named)).sorted, List("after", "far"))
