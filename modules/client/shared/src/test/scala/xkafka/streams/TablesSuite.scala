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

import cats.effect.IO
import munit.FunSuite

final class TablesSuite extends FunSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val topic = Topic.from("in").orFail

  test("a table that only updates its keys gives its values as records, and one that can delete them gives options, a delete being none"):
    val counted: KStream.Unkeyed[IO, String, Long] =
      KStream(Named("read-1"), topic, text, text).groupByKey(text, text).count(Named("count-1"), Store("counts")).toStream(Named("to-stream-1"))
    val read: KStream.Unkeyed[IO, String, Option[String]] = KTable(Named("users"), topic, text, text, Store("users")).toStream(Named("to-stream-2"))
    val sessions: KStream.Unkeyed[IO, Windowed[String], Option[Long]] =
      KStream(Named("read-2"), topic, text, text).groupByKey(text, text).windowedBy(SessionWindows(1.second, grace = 0.seconds))
        .count(Named("count-2"), Store("sessions")).toStream(Named("to-stream-3"))
    val written =
      List[Pipeline[IO]](
        counted.to(Named("write-1"), topic, text, count),
        read.to(Named("write-2"), topic, text, text.option),
        sessions.to(Named("write-3"), topic, Windowed.sessionSerde(text), count.option)
      )
    assertEquals(written.map(Topology.of(_).isRight), List(true, true, true))

  test("a table that can delete its keys is written with an option serde, and one that cannot, without"):
    val missing =
      compileErrors(
        "KTable(Named(\"users\"), topic, text, text, Store(\"users\")).toStream(Named(\"changes\")).to(Named(\"out\"), topic, text, text)"
      )
    assert(missing.contains("Required: xkafka.Serde[cats.effect.IO, Option[String]]"), missing)
    assert(
      compileErrors(
        "KStream(Named(\"in\"), topic, text, text).groupByKey(text, text).count(Named(\"count\"), Store(\"counts\")).toStream(Named(\"changes\")).to(Named(\"out\"), topic, text, count.option)"
      ).nonEmpty
    )
    assertEquals(
      compileErrors(
        "KStream(Named(\"in\"), topic, text, text).groupByKey(text, text).count(Named(\"count\"), Store(\"counts\")).toStream(Named(\"changes\")).to(Named(\"out\"), topic, text, count)"
      ),
      ""
    )
    assertEquals(
      compileErrors(
        "KTable(Named(\"users\"), topic, text, text, Store(\"users\")).toStream(Named(\"changes\")).to(Named(\"out\"), topic, text, text.option)"
      ),
      ""
    )
    // A table whose type does not say how it changes cannot be written with either serde.
    val widened: KTable[IO, String, String] = KTable(Named("users"), topic, text, text, Store("users"))
    val _                                   = widened
    assert(compileErrors("widened.toStream(Named(\"changes\")).to(Named(\"out\"), topic, text, text.option)").nonEmpty)
    assert(compileErrors("widened.toStream(Named(\"changes\")).to(Named(\"out\"), topic, text, text)").nonEmpty)

  test(
    "records whose key may have changed are joined with a table only given serdes to repartition them with, and a windowed table cannot be joined yet"
  ):
    val users                           = KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users"))
    def refused(pipeline: Pipeline[IO]) = Topology.of(pipeline).left.toOption.map(_.toList)
    val read                            = KStream(Named("read"), topic, text, text)
    val rekeyed                         = read.selectKey(Named("by-value"))((_, v) => v)
    assert(compileErrors("rekeyed.join(Named(\"by-user\"), users)(_ + _)").contains("so they go through a repartition topic first"))
    assert(compileErrors("rekeyed.leftJoin(Named(\"by-user\"), users)((v, u) => v)").contains("so they go through a repartition topic first"))
    assertEquals(refused(rekeyed.join(Named("by-user"), users, text, text)(_ + _).to(Named("write"), topic, text, text)), None)
    assert(compileErrors("read.join(Named(\"by-user\"), users, text, text)(_ + _)").contains("so they need no repartition topic"))
    // A stream whose type does not say how it is keyed takes serdes, which Topology.of refuses where its key has not changed.
    val widened: KStream.Unkeyed[IO, String, String] = read
    assertEquals(
      refused(widened.join(Named("by-user"), users, text, text)(_ + _).to(Named("write"), topic, text, text)),
      Some(List(TopologyError.UnusedRepartition("by-user")))
    )
    assert(TopologyError.UnusedRepartition("by-user").message.contains("join them without serdes"))
    val windowed =
      KStream(Named("visits"), Topic.from("visits").orFail, text, text).groupByKey(text, text)
        .windowedBy(TimeWindows.tumbling(1.second, grace = 0.seconds)).count(Named("count"), Store("visits"))
    assertEquals(
      refused(
        KStream(Named("read"), Topic.from("windowed-reads").orFail, Windowed.timeSerde(text, 1.second), text)
          .join(Named("join"), windowed)((v, n) => s"$v$n").to(Named("write"), topic, Windowed.timeSerde(text, 1.second), text)
      ),
      None
    )

  test("two tables joined give a table that can delete its keys, whichever kinds of table they are"):
    val counted = KStream(Named("read-3"), topic, text, text).groupByKey(text, text).count(Named("count-3"), Store("counts"))
    val users   = KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users"))
    val joined: KTable.Of[IO, String, String, Option] = counted.join(Named("join-1"), users)((n, u) => s"$u$n")
    val left: KTable.Of[IO, String, String, Option]   = counted.leftJoin(Named("left-join-1"), users)((n, u) => s"${u.getOrElse("")}$n")
    val outer: KTable.Of[IO, String, String, Option]  =
      counted.outerJoin(Named("outer-join-1"), users)(_.fold(_.toString, identity, (n, u) => s"$u$n"))
    val stored: KTable.Of[IO, String, String, Option]         = users.join(Named("joined"), counted, Store("joined"), text)((u, n) => s"$u$n")
    def written(table: KTable.Of[IO, String, String, Option]) =
      Topology.of(table.toStream(Named("to-stream-4")).to(Named("write-4"), Topic.from("out").orFail, text, text.option))
    assertEquals(List(joined, left, outer, stored).map(written(_).isRight), List(true, true, true, true))

  test("a table joined with itself, or with a table read from the same topic, is refused, and one windowed joins"):
    val users                                                 = KTable(Named("users"), topic, text, text, Store("users"))
    def refused(table: KTable.Of[IO, String, String, Option]) =
      Topology.of(table.toStream(Named("to-stream-5")).to(Named("write-5"), Topic.from("out").orFail, text, text.option)).left.toOption.map(_.toList)
    assert(refused(users.join(Named("join-2"), users)(_ + _)).exists(_.contains(TopologyError.DuplicateSource(topic))))
    assert(
      refused(
        users.leftJoin(Named("counts"), KStream(Named("read-4"), topic, text, text).groupByKey(text, text).count(Named("count-4"), Store("counts")))(
          (u, n) => u + n.getOrElse(0L)
        )
      ).exists(_.contains(TopologyError.DuplicateSource(topic)))
    )
    def windowed(name: String) =
      KStream(Named(s"read-$name"), Topic.from(name).orFail, text, text).groupByKey(text, text)
        .windowedBy(TimeWindows.tumbling(1.second, grace = 0.seconds)).count(Named(s"count-$name"), Store(name))
    val both =
      windowed("visits").join(Named("join-3"), windowed("clicks"))(_ + _).toStream(Named("to-stream-6"))
        .to(Named("write-6"), Topic.from("out").orFail, Windowed.timeSerde(text, 1.second), count.option)
    assert(Topology.of(both).isRight, Topology.of(both).toString)

  test("the result of joining tables is suppressed only once it is kept in a store, which gives the suppression a serde"):
    val users                                                    = KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users"))
    val orders                                                   = KTable(Named("orders"), Topic.from("orders").orFail, text, text, Store("orders"))
    def suppressed(table: KTable.Of[IO, String, String, Option]) =
      Topology.of(
        table.suppress(Suppressed.untilTimeLimit("calm", 1.second)).toStream(Named("to-stream-7"))
          .to(Named("write-7"), Topic.from("out").orFail, text, text.option)
      ).left.toOption.map(_.toList)
    assertEquals(suppressed(users.join(Named("join-4"), orders)(_ + _)), Some(List(TopologyError.SuppressedWithoutSerde("calm"))))
    assertEquals(suppressed(users.join(Named("joined"), orders, Store("joined"), text)(_ + _)), None)
    assert(TopologyError.SuppressedWithoutSerde("calm").message.contains("pass a Store and a Serde to the join or mapValues"))

  test("values changed in no store are suppressed only once they are kept in a store, which gives the suppression a serde"):
    val users = KTable(Named("users"), topic, text, text, Store("users"))
    def suppressed(table: KTable.Of[IO, String, String, Option]): Option[List[TopologyError]] =
      Topology.of(table.suppress(Suppressed.untilTimeLimit("calm", 1.second)).toStream(Named("changes")).to(Named("write"), topic, text, text.option))
        .left.toOption.map(_.toList)
    assertEquals(suppressed(users.mapValues(Named("loud"))(identity)), Some(List(TopologyError.SuppressedWithoutSerde("calm"))))
    assertEquals(suppressed(users.mapValues(Named("loud"), Store("loud"), text)(identity)), None)
    // A filter keeps the serde of what it filters.
    assertEquals(suppressed(users.filter(Named("kept"))((_, _) => true)), None)

  test("a table changing values keeps its kind of change, and a filter makes each change an option, a dropped value being a delete"):
    val counted: KTable.Of[IO, String, Long, KTable.Updated] =
      KStream(Named("read"), Topic.from("other").orFail, text, text).groupByKey(text, text).count(Named("count"), Store("counts"))
    val doubled: KStream.Unkeyed[IO, String, Long]          = counted.mapValues(Named("doubled"))(_ * 2).toStream(Named("changes-1"))
    val filtered: KStream.Unkeyed[IO, String, Option[Long]] = counted.filter(Named("big"))((_, n) => n > 1).toStream(Named("changes-2"))
    val read: KStream.Unkeyed[IO, String, Option[Int]]      =
      KTable(Named("users"), topic, text, text, Store("users")).mapValues(Named("lengths"))(_.length).toStream(Named("changes-3"))
    assertEquals(
      List(
        Topology.of(doubled.to(Named("write"), topic, text, count)),
        Topology.of(filtered.to(Named("write"), topic, text, count.option)),
        Topology.of(read.mapValues(Named("shown"))(_.toString).to(Named("write"), topic, text, text))
      ).map(_.isRight),
      List(true, true, true)
    )

  test("a stream's records are made a table with optional values only where its values are options"):
    val optional = KStream(Named("read"), topic, text, text.option)
    val plain    = KStream(Named("read"), topic, text, text)
    assert(
      Topology.of(optional.toTable(Named("t"), Store("t"), text, text).toStream(Named("changes")).to(Named("write"), topic, text, text.option))
        .isRight
    )
    assert(
      Topology.of(plain.toTable(Named("t"), Store("t"), text, text).toStream(Named("changes")).to(Named("write"), topic, text, text.option)).isRight
    )
    assert(compileErrors("""KStream(Named("read"), topic, text, count).toTable(Named("t"), Store("t"), text, text)""").nonEmpty)
