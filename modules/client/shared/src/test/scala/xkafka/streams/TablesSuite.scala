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
    val counted: KStream[IO, String, Long]                    = KStream(topic, text, text).groupByKey(text, text).count(Store("counts")).toStream
    val read: KStream[IO, String, Option[String]]             = KTable(topic, text, text, Store("users")).toStream
    val sessions: KStream[IO, Windowed[String], Option[Long]] =
      KStream(topic, text, text).groupByKey(text, text).windowedBy(SessionWindows(1.second, grace = 0.seconds)).count(Store("sessions")).toStream
    val written =
      List(counted.to(topic, text, count), read.to(topic, text, text.option), sessions.to(topic, Serde.sessionWindowed(text), count.option))
    assertEquals(written.map(Topology.of(_).isRight), List(true, true, true))

  test("a table that can delete its keys is written with an option serde, and one that cannot, without"):
    val missing = compileErrors("KTable(topic, text, text, Store(\"users\")).toStream.to(topic, text, text)")
    assert(missing.contains("Required: xkafka.streams.Serde[cats.effect.IO, Option[String]]"), missing)
    assert(
      compileErrors("KStream(topic, text, text).groupByKey(text, text).count(Store(\"counts\")).toStream.to(topic, text, count.option)").nonEmpty
    )
    assertEquals(compileErrors("KStream(topic, text, text).groupByKey(text, text).count(Store(\"counts\")).toStream.to(topic, text, count)"), "")
    assertEquals(compileErrors("KTable(topic, text, text, Store(\"users\")).toStream.to(topic, text, text.option)"), "")

  test("records joined after their key changed need a repartitioning name, and a windowed table cannot be joined yet"):
    val users                           = KTable(Topic.from("users").orFail, text, text, Store("users"))
    def refused(pipeline: Pipeline[IO]) = Topology.of(pipeline).left.toOption.map(_.toList)
    assertEquals(
      refused(KStream(topic, text, text).selectKey((_, v) => v).join(users)(_ + _).to(topic, text, text)),
      Some(List(TopologyError.UnnamedJoinRepartition))
    )
    assertEquals(
      refused(KStream(topic, text, text).selectKey((_, v) => v).join(users, Repartition("by-user"), text, text)(_ + _).to(topic, text, text)),
      None
    )
    // A name given for records whose key did not change makes no repartition topic.
    assertEquals(refused(KStream(topic, text, text).join(users, Repartition("by-user"), text, text)(_ + _).to(topic, text, text)), None)
    val windowed =
      KStream(Topic.from("visits").orFail, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.second, grace = 0.seconds))
        .count(Store("visits"))
    assertEquals(
      refused(
        KStream(topic, Serde.timeWindowed(text, 1.second), text).join(windowed)((v, n) => s"$v$n").to(topic, Serde.timeWindowed(text, 1.second), text)
      ),
      Some(List(TopologyError.WindowedJoin))
    )
    assertEquals(TopologyError.UnnamedJoinRepartition.message.contains("pass a Repartition to join"), true)
