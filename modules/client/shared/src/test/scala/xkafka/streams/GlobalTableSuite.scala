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
import fs2.Chunk
import munit.CatsEffectSuite
import internal.driver.PolledRecord
import internal.streams.{GlobalStores, Stored, Task, TaskInput, TaskState}

/** The parts of a global table that do not need Java to compare with, on every platform: what a record does to it, and which programs are refused. */
final class GlobalTableSuite extends CatsEffectSuite:
  private val text = Serde.utf8[IO]

  private def topic(name: String) = Topic.from(name).orFail

  private def table = GlobalKTable(Named("g"), topic("g"), text, text, Store("gs"))

  private def bytes(text: String) = Chunk.array(text.getBytes)

  private def polled(key: Option[String], value: Option[String], at: Long = 7L) =
    PolledRecord("g", 0, 0L, Some(at), 0, -1, key.map(bytes), value.map(bytes), Headers.empty)

  private def stream(name: String = "in") = KStream(Named(name), topic(name), text, text)

  private def refused(first: Pipeline[IO], rest: Pipeline[IO]*) = Topology.of(first, rest*).left.toOption.fold(Nil)(_.toList)

  private def joined(global: GlobalKTable[IO, String, String], name: String = "in") =
    stream(name).join(Named(s"j-$name"), global)((_, v) => v)(_ + _).to(Named(s"w-$name"), topic(s"out-$name"), text, text)

  test("a record keeps its key's latest value with its time, none deletes the key, and a record without a key changes nothing"):
    val global = table
    for
      kept <- GlobalStores.updated(global, Map.empty, polled(Some("k"), Some("v")))
      _ = assertEquals(kept, Map("gs" -> Map(bytes("k") -> Stored(bytes("v"), 7L))))
      again <- GlobalStores.updated(global, kept, polled(Some("k"), Some("w"), 3L))
      _ = assertEquals(again, Map("gs" -> Map(bytes("k") -> Stored(bytes("w"), 3L))))
      skipped <- GlobalStores.updated(global, again, polled(None, Some("x")))
      _ = assertEquals(skipped, again)
      gone <- GlobalStores.updated(global, skipped, polled(Some("k"), None))
    yield assertEquals(gone, Map("gs" -> Map.empty[Chunk[Byte], Stored]))

  test("a record the table cannot read fails"):
    val refusing = Serde(text.serializer, Deserializer.instance[IO, String]((_, _, _) => IO.raiseError(new IllegalArgumentException("unreadable"))))
    GlobalStores.updated(GlobalKTable(Named("g"), topic("g"), text, refusing, Store("gs")), Map.empty, polled(Some("k"), Some("v"))).attempt
      .map(result => assertEquals(result.left.toOption.map(_.getMessage), Some("unreadable")))

  test("a join reads what the task is given of the table, on every platform"):
    val topology = Topology.of(joined(table)).orFail
    val task     = Task[IO]("app", topology.subtopologies.head, 0)
    def input    = TaskInput(Some(bytes("o")), Some(bytes("k")), 5L, Headers.empty, "in")
    for
      none <- task.process(TaskState.empty, input, 0L)
      _ = assertEquals(none._2, Vector.empty)
      some <- task.process(TaskState.empty.copy(globals = Map("gs" -> Map(bytes("k") -> Stored(bytes("v"), 1L)))), input, 0L)
    yield assertEquals(some._2.map(out => new String(out.value.get.toArray)), Vector("kv"))

  test("a table's two nodes are names of the program, which nothing else can take"):
    assertEquals(
      refused(stream().join(Named("g"), table)((_, v) => v)(_ + _).to(Named("w"), topic("out"), text, text)),
      List(TopologyError.DuplicateName("g"))
    )
    assertEquals(
      refused(stream().join(Named("g-source"), table)((_, v) => v)(_ + _).to(Named("w"), topic("out"), text, text)),
      List(TopologyError.DuplicateName("g-source"))
    )

  test("a table's store cannot be shared, and two tables cannot read one topic"):
    val taken = GlobalKTable(Named("h"), topic("h"), text, text, Store("gs"))
    assertEquals(refused(joined(table), joined(taken, "more")), List(TopologyError.DuplicateStore("gs")))
    val again = GlobalKTable(Named("h"), topic("g"), text, text, Store("hs"))
    assertEquals(refused(joined(table), joined(again, "more")), List(TopologyError.DuplicateSource(topic("g"))))
    val counted =
      stream().groupByKey(text, text).count(Named("n"), Store("gs")).toStream(Named("c")).to(Named("w"), topic("out"), text, Serde.long[IO])
    assert(refused(counted, joined(table, "more")).contains(TopologyError.DuplicateStore("gs")))

  test("a table's names must be valid"):
    val badStore = GlobalKTable(Named("g"), topic("g"), text, text, Store("not valid"))
    assert(refused(joined(badStore)).exists(_.isInstanceOf[TopologyError.InvalidName]))
    val badNode = GlobalKTable(Named("not valid"), topic("g"), text, text, Store("gs"))
    assert(refused(joined(badNode)).exists(_.isInstanceOf[TopologyError.InvalidName]))

  test("one table joined twice is one table, even in pipelines mapped apart, and a mapped program keeps it"):
    val global   = table
    val identity = FunctionK.id[IO]
    val two      =
      stream().join(Named("j1"), global)((_, v) => v)(_ + _).join(Named("j2"), global)((_, v) => v)(_ + _).to(Named("w"), topic("out"), text, text)
    val topology = Topology.of(two).orFail
    assertEquals(topology.globals.map(_._2.store), List(Store("gs")))
    assertEquals(
      topology.mapK(identity).globals.map((id, kept) => (id, kept.store, kept.topic)),
      topology.globals.map((id, kept) => (id, kept.store, kept.topic))
    )
    assertEquals(Topology.of(two.mapK(identity)).map(_.globals.size), Right(1))

  test("the description names the table's nodes and numbers it where it is first joined"):
    val described = Topology.of(joined(table)).orFail.describe
    assertEquals(described.subtopologies.map(_.id), List(0))
    assertEquals(
      described.globalStores,
      List(TopologyDescription.GlobalStore(
        1,
        TopologyDescription.Node.Source("g-source", List("g"), List("g")),
        TopologyDescription.Node.Processor("g", List("gs"), Nil, List("g-source"))
      ))
    )
