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

final class StreamJoinsSuite extends FunSuite:
  private val text   = Serde.utf8[IO]
  private val left   = KStream(Named("left"), Topic.from("left").orFail, text, text)
  private val right  = KStream(Named("right"), Topic.from("right").orFail, text, text)
  private val out    = Topic.from("out").orFail
  private val within = JoinWindows(5.seconds, grace = 1.second)

  private def segments(pipeline: Pipeline[IO]) = Topology.of(pipeline).orFail.subtopologies
  private def refused(pipeline: Pipeline[IO])  = Topology.of(pipeline).left.toOption.map(_.toList)

  /** The repartitionings its segments write to, by name. */
  private def repartitions(pipeline: Pipeline[IO]) = segments(pipeline).flatMap(_.repartitions.map(_.value))

  test("join windows reach the time difference either way, or as far as each side is set to"):
    val windows = JoinWindows(5.seconds, grace = 1.second)
    assertEquals((windows.before, windows.after, windows.grace), (5.seconds, 5.seconds, 1.second))
    assertEquals(JoinWindows.withNoGrace(2.seconds).grace, 0.seconds)
    assertEquals(
      windows.withBefore(1.second).withAfter(3.seconds),
      JoinWindows(5.seconds, grace = 1.second).withAfter(3.seconds).withBefore(1.second)
    )
    assertEquals((windows.withBefore(1.second).before, windows.withAfter(3.seconds).after), (1.second, 3.seconds))

  test("each kind of join names its stores as Java's does, in one sub-topology reading both topics, copartitioned"):
    def stores(pipeline: Pipeline[IO]) = segments(pipeline).flatMap(_.stores.map(_.value))
    val store                          = Store("pairs")
    assertEquals(
      stores(left.join(Named("join-1"), right, within, store, text, text, text)(_ + _).to(Named("write-1"), out, text, text)),
      List("pairs-this-join-store", "pairs-other-join-store")
    )
    assertEquals(
      stores(
        left.leftJoin(Named("left-join-1"), right, within, store, text, text, text)((v, o) => v + o.getOrElse(""))
          .to(Named("write-2"), out, text, text)
      ),
      List("pairs-this-join-store", "pairs-outer-other-join-store", "pairs-left-shared-join-store")
    )
    val outer =
      left.outerJoin(Named("outer-join-1"), right, within, store, text, text, text)(_.fold(identity, identity, _ + _))
        .to(Named("write-3"), out, text, text)
    assertEquals(stores(outer), List("pairs-outer-this-join-store", "pairs-outer-other-join-store", "pairs-outer-shared-join-store"))
    assertEquals(
      segments(outer).map(_.copartitioned),
      List(List(List(Source.Input(Topic.from("left").orFail), Source.Input(Topic.from("right").orFail))))
    )

  test("a side whose key has changed is repartitioned through a topic named after the join and its side, and the other side is not"):
    val joined =
      left.selectKey(Named("select-key-1"))((_, v) => v).join(Named("pairs"), right, within, Store("pair-store"), text, text, text)(_ + _)
        .to(Named("write-4"), out, text, text)
    assertEquals(repartitions(joined), List("pairs-left"))
    assertEquals(segments(joined)(1).origins, List(Source.Repartitioned(Repartition("pairs-left")), Source.Input(Topic.from("right").orFail)))
    // The stores are named after the store, as Java's `withStoreName` names them, and the repartitions after the join, as its `withName` does.
    assertEquals(segments(joined)(1).stores.map(_.value), List("pair-store-this-join-store", "pair-store-other-join-store"))
    val both =
      left.selectKey(Named("select-key-2"))((_, v) => v)
        .join(Named("pairs"), right.selectKey(Named("select-key-3"))((_, v) => v), within, Store("pairs"), text, text, text)(_ + _)
        .to(Named("write-5"), out, text, text)
    assertEquals(repartitions(both), List("pairs-left", "pairs-right"))

  test("a sub-topology's topics start with its own, the table a stream joins following"):
    val users = KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users"))
    assertEquals(
      segments(left.join(Named("with-user"), users)(_ + _).to(Named("write-13"), out, text, text)).map(_.origins),
      List(List(Source.Input(Topic.from("left").orFail), Source.Input(Topic.from("users").orFail)))
    )

  test("sides that share a repartition topic, or a table's topic, are joined with themselves, and a join's name counts as a step's"):
    def rekeyed(from: KStream.Of[IO, String, String, KeyState.AsRead]) =
      from.groupBy(Repartition("shared"), text, text)((_, v) => v).count(Named(s"count-${from.lastNode}"), Store(s"counts-${from.lastNode}"))
        .toStream(Named(s"to-stream-${from.lastNode}"))
    val shared =
      rekeyed(left).join(Named("pairs"), rekeyed(right), within, Store("pairs"), text, Serde.long[IO], Serde.long[IO])(_ + _)
        .to(Named("write-14"), out, text, Serde.long[IO])
    assert(refused(shared).exists(_.contains(TopologyError.JoinedWithItself)), clue(refused(shared)))
    val users   = KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users"))
    val through =
      left.join(Named("with-user"), users)(_ + _)
        .join(Named("pairs"), KStream(Named("users-again"), Topic.from("users").orFail, text, text), within, Store("pairs"), text, text, text)(_ + _)
        .to(Named("write-15"), out, text, text)
    assert(refused(through).exists(_.contains(TopologyError.JoinedWithItself)), clue(refused(through)))
    assertEquals(
      refused(
        left.join(Named("pairs"), right, within, Store("pairs"), text, text, text)(_ + _).mapValues(Named("pairs"))(identity)
          .to(Named("write-16"), out, text, text)
      ),
      Some(List(TopologyError.DuplicateName("pairs")))
    )
    assertEquals(
      refused(
        users.join(Named("joined"), KTable(Named("plans"), Topic.from("plans").orFail, text, text, Store("plans")))(_ + _)
          .mapValues(Named("joined"))(identity).toStream(Named("to-stream-3")).to(Named("write-17"), out, text, text.option)
      ),
      Some(List(TopologyError.DuplicateName("joined")))
    )

  test("a stream joined with itself or with a stream read from the same topic is refused, and so are windows that cannot be used"):
    assertEquals(
      refused(left.join(Named("pairs"), left, within, Store("pairs"), text, text, text)(_ + _).to(Named("write-6"), out, text, text)),
      Some(List(TopologyError.JoinedWithItself))
    )
    assertEquals(
      refused(
        left.join(Named("pairs"), left.mapValues(Named("map-values-1"))(_.reverse), within, Store("pairs"), text, text, text)(_ + _)
          .to(Named("write-7"), out, text, text)
      ),
      Some(List(TopologyError.JoinedWithItself))
    )
    def windows(joining: JoinWindows) =
      refused(left.join(Named("pairs"), right, joining, Store("pairs"), text, text, text)(_ + _).to(Named("write-8"), out, text, text))
    assertEquals(
      windows(JoinWindows(1.second, grace = 0.seconds).withBefore(-2.seconds)),
      Some(
        List(TopologyError.InvalidWindows("pairs", "the time before and after must not add up to less than nothing, were -2 seconds and 1 second"))
      )
    )
    assertEquals(
      windows(JoinWindows(1.second, grace = -1.second)),
      Some(List(TopologyError.InvalidWindows("pairs", "the grace must not be negative, was -1 seconds")))
    )
    assertEquals(
      windows(JoinWindows(1.second, grace = 1.micro)),
      Some(List(TopologyError.InvalidWindows("pairs", "the time differences and grace must be whole milliseconds")))
    )
    assertEquals(windows(JoinWindows(1.second, grace = 0.seconds).withBefore(-1.second)), None)

  test("a join says Java numbers it by build order where either side made a sub-topology before it, or both made one after it"):
    def ordered(pipeline: Pipeline[IO]) = segments(pipeline).flatMap(_.buildOrdered).distinct
    def source(name: String)            = Source.Input(Topic.from(name).orFail)
    def counted(from: String)           =
      KStream(Named(from), Topic.from(from).orFail, text, text).groupBy(Repartition(s"by-$from"), text, text)((_, v) => v)
        .count(Named(s"count-$from"), Store(s"counts-$from"))
    val before = counted("clicks").toStream(Named("clicks-stream"))
    assertEquals(
      ordered(
        before.join(Named("pairs"), right, within, Store("pairs"), text, Serde.long[IO], text)((n, v) => s"$n$v")
          .to(Named("write-18"), out, text, text)
      ),
      List(List(Source.Repartitioned(Repartition("by-clicks")), source("right")))
    )
    val first  = left.join(Named("with-a"), counted("a"))((v, n) => s"$v$n")
    val second = right.join(Named("with-b"), counted("b"))((v, n) => s"$v$n")
    val a      = List(source("left"), Source.Repartitioned(Repartition("by-a")))
    val b      = List(source("right"), Source.Repartitioned(Repartition("by-b")))
    assertEquals(
      ordered(first.join(Named("pairs"), second, within, Store("pairs"), text, text, text)(_ + _).to(Named("write-19"), out, text, text)),
      List(a, b, a ++ b)
    )
    // A side that made a sub-topology after it stays so through a join with a side that made none, so the next join with such a side says so.
    def plain(name: String)                           = KStream(Named(name), Topic.from(name).orFail, text, text)
    val c                                             = List(source("c"))
    def throughLeft                                   = first.join(Named("pairs-1"), plain("c"), within, Store("pairs-1"), text, text, text)(_ + _)
    def throughRight                                  = plain("c").join(Named("pairs-1"), first, within, Store("pairs-1"), text, text, text)(_ + _)
    def finished(joined: KStream[IO, String, String]) =
      joined.join(Named("pairs-2"), second, within, Store("pairs-2"), text, text, text)(_ + _).to(Named("write-20"), out, text, text)
    assertEquals(ordered(finished(throughLeft)), List(a, b, a ++ c ++ b))
    assertEquals(ordered(finished(throughRight)), List(a, b, a ++ c ++ b))

  test("a join whose sub-topologies Java numbers by which input its program builds first says so, and one Java numbers alike either way does not"):
    def ordered(pipeline: Pipeline[IO]) = segments(pipeline).flatMap(_.buildOrdered).distinct
    val repartitioned                   = right.selectKey(Named("select-key-4"))((_, v) => v)
    val sources                         = List(Source.Input(Topic.from("left").orFail), Source.Repartitioned(Repartition("pairs-right")))
    assertEquals(
      ordered(left.join(Named("pairs"), right, within, Store("pairs"), text, text, text)(_ + _).to(Named("write-9"), out, text, text)),
      Nil
    )
    assertEquals(
      ordered(left.join(Named("pairs"), repartitioned, within, Store("pairs"), text, text, text)(_ + _).to(Named("write-10"), out, text, text)),
      List(sources)
    )
    assertEquals(
      ordered(
        left.join(Named("pairs"), repartitioned, within, Store("pairs"), text, text, text)(_ + _).mapValues(Named("map-values-2"))(_.reverse)
          .to(Named("write-11"), out, text, text)
      ),
      List(sources)
    )
    val users = KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users"))
    assertEquals(
      ordered(
        users.join(Named("join-plans"), KTable(Named("plans"), Topic.from("plans").orFail, text, text, Store("plans")))(_ + _)
          .toStream(Named("to-stream-1")).to(Named("write-12"), out, text, text.option)
      ),
      Nil
    )
