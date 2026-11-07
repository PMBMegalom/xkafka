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
import cats.effect.{IO, Ref}
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.internal.streams.{Task, TaskInput, TaskOutput, TaskState}

/** What the steps given whole records read and write, through a task. */
final class RecordStepsSuite extends CatsEffectSuite:
  private val text = Serde.utf8[IO]
  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail

  private def read = KStream(Named("read"), in, text, text)

  private val traced = Headers.empty.append("trace", "t1")

  private def input(key: String, value: String, at: Long = 1000L, headers: Headers = traced): TaskInput =
    TaskInput(Some(Chunk.array(key.getBytes)), Some(Chunk.array(value.getBytes)), at, headers, "in", 3, 7L)

  /** What the output topic gets for each of `inputs`: key, value, time, and the trace header. */
  private def written(pipeline: Pipeline[IO], inputs: TaskInput*): IO[Vector[(String, String, Long, Option[String])]] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0).flatMap: task =>
      inputs.toList.foldLeft(IO.pure((TaskState.empty, Vector.empty[TaskOutput]))): (done, next) =>
        done.flatMap((state, so) => task.process(state, next, 0L).map(after => (after._1, so ++ after._2)))
      .map(
        _._2.filter(_.topic == out).map: output =>
          (
            output.key.fold("")(k => String(k.toArray)),
            output.value.fold("")(v => String(v.toArray)),
            output.timestamp,
            output.headers.get[String]("trace").flatMap(_.toOption)
          )
      )

  private def to(stream: KStream.Of[IO, String, String, KeyState.AsRead]) = stream.to(Named("write"), out, text, text)

  test("filterRecord decides by the record's headers, time and origin, and leaves what it keeps as it is"):
    val byHeader = to(read.filterRecord(Named("f"))(_.headers.get[String]("trace").contains(Right("t1"))))
    val byTime   = to(read.filterRecord(Named("f"))(_.timestamp.epochMillis >= 2000L))
    val byOrigin = to(read.filterRecord(Named("f"))(_.origin.map(o => (o.topic, o.partition.value, o.offset.value)).contains((in, 3, 7L))))
    val others   = input("b", "y", 2000L, Headers.empty)
    for
      header <- written(byHeader, input("a", "x"), others)
      time   <- written(byTime, input("a", "x"), others)
      origin <- written(byOrigin, input("a", "x"))
    yield
      assertEquals(header, Vector(("a", "x", 1000L, Some("t1"))))
      assertEquals(time, Vector(("b", "y", 2000L, None)))
      assertEquals(origin, Vector(("a", "x", 1000L, Some("t1"))))

  test("mapRecordValue changes the value, time and headers, under the same key, and keeps what it does not change"):
    val changed =
      read.mapRecordValue(Named("m")): record =>
        record.withValue(record.value + record.headers.get[String]("trace").fold("")(_.fold(_ => "", identity)))
          .withTimestamp(Timestamp.fromEpochMillis(record.timestamp.epochMillis + 1)).withHeaders(record.headers.set("trace", "t2"))
    val valueOnly = read.mapRecordValue(Named("m"))(record => record.withValue(record.value * 2))
    for
      both <- written(to(changed), input("a", "x"))
      kept <- written(to(valueOnly), input("a", "x"))
    yield
      assertEquals(both, Vector(("a", "xt1", 1001L, Some("t2"))))
      assertEquals(kept, Vector(("a", "xx", 1000L, Some("t1"))))

  test("mapRecord can change the key, and flatMapRecord gives any number of records in order, each with its own key, time and headers"):
    val mapped = read.mapRecord(Named("m"))(record => record.withKey(record.value).withValue(record.key))
    val many   =
      read.flatMapRecord(Named("m")): record =>
        if record.value == "none" then Nil
        else
          List(
            record.withKey("k1"),
            record.withKey("k2").withTimestamp(Timestamp.fromEpochMillis(5L)),
            record.withKey("k3").withHeaders(Headers.empty)
          )
    for
      one     <- written(mapped.to(Named("write"), out, text, text), input("a", "x"))
      several <- written(many.to(Named("write"), out, text, text), input("a", "x"), input("a", "none"))
    yield
      assertEquals(one, Vector(("x", "a", 1000L, Some("t1"))))
      assertEquals(several, Vector(("k1", "x", 1000L, Some("t1")), ("k2", "x", 5L, Some("t1")), ("k3", "x", 1000L, None)))

  test("flatMapRecordValue gives any number of records in order, under the same key"):
    val many =
      read.flatMapRecordValue(Named("m")): record =>
        record.value.toList.map(letter => record.withValue(letter.toString).withHeaders(record.headers.set("trace", s"t$letter")))
    written(to(many), input("a", "xy"), input("b", "")).map: results =>
      assertEquals(results, Vector(("a", "x", 1000L, Some("tx")), ("a", "y", 1000L, Some("ty"))))

  test("peekRecord runs its effect once for each whole record, and leaves it as it is"):
    Ref.of[IO, List[(String, Long, Option[String])]](Nil).flatMap: seen =>
      val peeking =
        read.peekRecord(Named("p")): record =>
          seen.update((record.key, record.timestamp.epochMillis, record.headers.get[String]("trace").flatMap(_.toOption)) :: _)
      written(to(peeking), input("a", "x"), input("b", "y", 2000L)).flatMap: results =>
        seen.get.map: peeked =>
          assertEquals(results, Vector(("a", "x", 1000L, Some("t1")), ("b", "y", 2000L, Some("t1"))))
          assertEquals(peeked.reverse, List(("a", 1000L, Some("t1")), ("b", 2000L, Some("t1"))))

  test("the steps that run an effect give what its result says"):
    val filtered = read.evalFilterRecord(Named("e"))(record => IO.pure(record.timestamp.epochMillis < 2000L))
    val values   = read.evalMapRecordValue(Named("e"))(record => IO.pure(record.withValue(record.value.toUpperCase).withHeaders(Headers.empty)))
    val records  = read.evalMapRecord(Named("e"))(record => IO.pure(record.withKey(record.value).withValue(record.timestamp.epochMillis.toString)))
    for
      kept    <- written(to(filtered), input("a", "x"), input("b", "y", 2000L))
      changed <- written(to(values), input("a", "x"))
      rekeyed <- written(records.to(Named("write"), out, text, text), input("a", "x"))
    yield
      assertEquals(kept, Vector(("a", "x", 1000L, Some("t1"))))
      assertEquals(changed, Vector(("a", "X", 1000L, None)))
      assertEquals(rekeyed, Vector(("x", "1000", 1000L, Some("t1"))))

  test("a failing effect fails the record"):
    val failure                                                         = new RuntimeException("no")
    def failed(stream: KStream.Of[IO, String, String, KeyState.AsRead]) =
      written(to(stream), input("a", "x")).attempt.map(assertEquals(_, Left(failure)))
    failed(read.peekRecord(Named("e"))(_ => IO.raiseError(failure))) *> failed(read.evalFilterRecord(Named("e"))(_ => IO.raiseError(failure))) *>
      failed(read.evalMapRecordValue(Named("e"))(_ => IO.raiseError(failure))) *> written(
        read.evalMapRecord(Named("e"))(_ => IO.raiseError[Record[String, String]](failure)).to(Named("write"), out, text, text),
        input("a", "x")
      ).attempt.map(assertEquals(_, Left(failure)))

  test("imapK runs every step in the other effect"):
    val id    = FunctionK.id[IO]
    val steps =
      read.filterRecord(Named("a"))(_.value != "no").peekRecord(Named("b"))(_ => IO.unit).evalFilterRecord(Named("c"))(_ => IO.pure(true))
        .evalMapRecordValue(Named("d"))(record => IO.pure(record.withValue(record.value + "d")))
        .mapRecordValue(Named("e"))(record => record.withValue(record.value + "e"))
        .flatMapRecordValue(Named("f"))(record => List(record.withValue(record.value + "f")))
        .evalMapRecord(Named("g"))(record => IO.pure(record.withKey(record.key + "g")))
        .mapRecord(Named("h"))(record => record.withKey(record.key + "h")).flatMapRecord(Named("i"))(record => List(record.withKey(record.key + "i")))
        .to(Named("write"), out, text, text)
    written(steps.imapK(id)(id), input("a", "x"), input("a", "no")).map: results =>
      assertEquals(results, Vector(("aghi", "xdef", 1000L, Some("t1"))))

  test("grouping needs a repartition topic after a step that can change the key, and none after one that cannot"):
    val kept     = read.mapRecordValue(Named("a"))(identity).flatMapRecordValue(Named("b"))(List(_)).filterRecord(Named("c"))(_ => true)
    val effects  = kept.peekRecord(Named("d"))(_ => IO.unit).evalFilterRecord(Named("e"))(_ => IO.pure(true)).evalMapRecordValue(Named("f"))(IO.pure)
    val mapped   = read.mapRecord(Named("a"))(identity)
    val flat     = read.flatMapRecord(Named("a"))(List(_))
    val effected = read.evalMapRecord(Named("a"))(IO.pure)
    val _        = (effects.groupByKey(text, text), mapped, flat, effected)
    assertEquals(compileErrors("effects.groupByKey(text, text)"), "")
    assertEquals(compileErrors("""mapped.groupByKey(Repartition("r"), text, text)"""), "")
    assert(compileErrors("mapped.groupByKey(text, text)").nonEmpty)
    assert(compileErrors("flat.groupByKey(text, text)").nonEmpty)
    assert(compileErrors("effected.groupByKey(text, text)").nonEmpty)

  test("a record given to the step that keeps the key has no way to change it"):
    assertEquals(compileErrors("""read.mapRecordValue(Named("m"))(record => record.withValue("other"))"""), "")
    assert(compileErrors("""read.mapRecordValue(Named("m"))(record => record.withKey("other"))""").nonEmpty)
