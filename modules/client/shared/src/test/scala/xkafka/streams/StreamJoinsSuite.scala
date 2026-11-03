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

  private def segments(pipeline: Pipeline[IO]) = Topology.of(pipeline).orFail.pipelines.head.segments.toList
  private def refused(pipeline: Pipeline[IO])  = Topology.of(pipeline).left.toOption.map(_.toList)

  /** The repartitionings its segments write to, by name. */
  private def repartitions(pipeline: Pipeline[IO]) =
    segments(pipeline).flatMap(
      _.end match
        case End.Repartitioning(named, _, _) => List(named.value)
        case _                               => Nil
    )

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
    assertEquals(segments(outer).map(_.copartitioned), List(List(Source.Input(Topic.from("left").orFail), Source.Input(Topic.from("right").orFail))))

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

  test("a join whose sub-topologies Java numbers by which input its program builds first says so, and one Java numbers alike either way does not"):
    def ordered(pipeline: Pipeline[IO]) = segments(pipeline).flatMap(_.all).flatMap(_.buildOrdered).distinct
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
