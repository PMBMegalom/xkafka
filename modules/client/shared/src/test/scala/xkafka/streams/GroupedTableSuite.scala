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

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite

/** The parts of a table grouped anew that do not need Java to compare with, on every platform: how a change is written, and which programs are
  * refused.
  */
final class GroupedTableSuite extends CatsEffectSuite:
  private val text = Serde.utf8[IO]
  private val long = Serde.long[IO]
  private val at   = Topic.from("t").orFail

  private def topic(name: String) = Topic.from(name).orFail

  private val changes = TableChange.serde(text)

  private def bytes(values: Int*) = Chunk.from(values.map(_.toByte))

  private def written(current: Option[String], old: Option[String]) = changes.serializer.serialize(at, Headers.empty, TableChange(current, old))

  test("a value leaving is written with 0 after it, a value joining with 1, both with the joining value's length first and 2, and neither as none"):
    written(None, Some("ab")).assertEquals(Some(bytes(97, 98, 0))) *> written(Some("ab"), None).assertEquals(Some(bytes(97, 98, 1))) *>
      written(Some("ab"), Some("c")).assertEquals(Some(bytes(4, 97, 98, 99, 2))) *> written(Some(""), Some("")).assertEquals(Some(bytes(0, 2))) *>
      written(None, None).assertEquals(None)

  test("a length takes as many bytes as its zigzag varint needs, read back the same on every platform"):
    List(63 -> List(126), 64 -> List(128, 1), 8191 -> List(254, 127), 8192 -> List(128, 128, 1), 1048576 -> List(128, 128, 128, 1)).traverse_ :
      (length, varint) =>
        val value = "v" * length
        written(Some(value), Some("o")).flatMap: bytesOut =>
          assertEquals(bytesOut.map(_.take(varint.size).toList), Some(varint.map(_.toByte)))
          changes.deserializer.deserialize(at, Headers.empty, bytesOut).assertEquals(TableChange(Some(value), Some("o")))

  test("a change that says if it is the latest reads as one that does not"):
    def read(values: Int*) = changes.deserializer.deserialize(at, Headers.empty, Some(bytes(values*)))
    read(97, 1, 3).assertEquals(TableChange(None, Some("a"))) *> read(97, 0, 4).assertEquals(TableChange(Some("a"), None)) *> read(2, 97, 98, 1, 5)
      .assertEquals(TableChange(Some("a"), Some("b")))

  test("a value staying in its group is one change, and one moving leaves its group before it joins the next, its new pair made first"):
    def select(seen: scala.collection.mutable.ListBuffer[String])(unused: String, value: String) =
      val _ = unused
      seen += value
      (value.take(1), value)
    val seen = scala.collection.mutable.ListBuffer.empty[String]
    assertEquals(TableChange.grouped(select(seen))("k", Some("a2"), Some("a1")), Vector("a" -> TableChange(Some("a2"), Some("a1"))))
    assertEquals(seen.toList, List("a2", "a1"))
    assertEquals(
      TableChange.grouped(select(seen))("k", Some("b"), Some("a")),
      Vector("a" -> TableChange(None, Some("a")), "b" -> TableChange(Some("b"), None))
    )
    assertEquals(TableChange.grouped(select(seen))("k", Some("b"), None), Vector("b" -> TableChange(Some("b"), None)))
    assertEquals(TableChange.grouped(select(seen))("k", None, Some("a")), Vector("a" -> TableChange(None, Some("a"))))
    assertEquals(TableChange.grouped(select(seen))("k", None, None), Vector.empty)

  private def grouped(repartition: String = "g") =
    KTable(Named("users"), topic("in"), text, text, Store("users")).groupBy(Repartition(repartition), text, text)((_, v) => (v, v))

  private def refused(pipeline: Pipeline[IO]*) = Topology.of(pipeline.head, pipeline.tail*).left.toOption.map(_.toList).getOrElse(Nil)

  test("the sink and source of a grouped table's repartition topic are names of the program, which nothing else can take"):
    def counted(next: String) = grouped().count(Named("c"), Store("s")).toStream(Named(next)).to(Named("w"), topic("out"), text, long.option)
    assertEquals(refused(counted("next")), Nil)
    assertEquals(refused(counted("c-sink")), List(TopologyError.DuplicateName("c-sink")))
    assertEquals(refused(counted("c-source")), List(TopologyError.DuplicateName("c-source")))

  test("the select step takes the repartition's name, and two groupings cannot share a repartition topic"):
    val taken =
      KTable(Named("users"), topic("in"), text, text, Store("users")).filter(Named("g"))((_, _) => true)
        .groupBy(Repartition("g"), text, text)((_, v) => (v, v)).count(Named("c"), Store("s")).consumer
    assertEquals(refused(taken), List(TopologyError.DuplicateName("g")))
    val first  = grouped().count(Named("c"), Store("s")).consumer
    val second =
      KTable(Named("others"), topic("other"), text, text, Store("others")).groupBy(Repartition("g"), text, text)((_, v) => (v, v))
        .count(Named("d"), Store("t")).consumer
    assert(refused(first, second).contains(TopologyError.DuplicateRepartition("g")))
