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
