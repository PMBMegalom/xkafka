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
package interop

import java.util.concurrent.atomic.AtomicReference

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.header.Headers as JavaHeaders
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.{
  Deserializer as JavaDeserializer, Serdes, Serializer as JavaSerializer, StringDeserializer, StringSerializer
}

/** Codecs and headers made from Java's write and read what Java's do, and what goes in and out of them. */
final class InteropSuite extends CatsEffectSuite:
  private val topic   = Topic.from("orders").orFail
  private val headers = Headers.empty.append("trace", "t1").append(Header("empty", None)).append("trace", "t2")

  test("a serializer from Java's writes the bytes Java's writes, and nothing for a value Java writes as null"):
    val nulls =
      new JavaSerializer[String]:
        override def serialize(topic: String, data: String): Array[Byte] = if data == "none" then null else data.getBytes
    for
      written <- Serializer.fromJava[IO](new StringSerializer).serialize(topic, headers, "héllo")
      ours    <- Serializer.utf8[IO].serialize(topic, headers, "héllo")
      absent  <- Serializer.fromJava[IO](nulls).serialize(topic, headers, "none")
    yield
      assertEquals(written, ours)
      assertEquals(absent, None)

  test("a Java serializer and deserializer are passed the record's topic and headers, and headers a serializer adds are not written"):
    val seen  = new AtomicReference[(String, Vector[(String, Option[String])])]
    val heard = new AtomicReference[(String, Vector[(String, Option[String])])]
    def record(to: AtomicReference[(String, Vector[(String, Option[String])])], at: String, passed: JavaHeaders) =
      to.set(at -> passed.toArray.toVector.map(header => header.key -> Option(header.value).map(String(_))))
    val writing =
      new JavaSerializer[String]:
        override def serialize(topic: String, data: String): Array[Byte]                      = data.getBytes
        override def serialize(topic: String, passed: JavaHeaders, data: String): Array[Byte] =
          record(seen, topic, passed)
          passed.add("added", "by java".getBytes)
          data.getBytes
    val reading =
      new JavaDeserializer[String]:
        override def deserialize(topic: String, data: Array[Byte]): String                      = String(data)
        override def deserialize(topic: String, passed: JavaHeaders, data: Array[Byte]): String =
          record(heard, topic, passed)
          String(data)
    val expected = Vector("trace" -> Some("t1"), "empty" -> None, "trace" -> Some("t2"))
    for
      _ <- Serializer.fromJava[IO](writing).serialize(topic, headers, "v")
      _ <- Deserializer.fromJava[IO](reading).deserialize(topic, headers, Some(Chunk.array("v".getBytes)))
    yield
      assertEquals(seen.get, "orders"  -> expected)
      assertEquals(heard.get, "orders" -> expected)
      assertEquals(headers.getAll("added"), Vector.empty)

  test("a deserializer from Java's reads what Java's reads, fails on a record with no value unless read with option, and on a value read as null"):
    val nulls =
      new JavaDeserializer[String]:
        override def deserialize(topic: String, data: Array[Byte]): String = null
    val bytes = Some(Chunk.array("héllo".getBytes("UTF-8")))
    for
      read     <- Deserializer.fromJava[IO](new StringDeserializer).deserialize(topic, headers, bytes)
      missing  <- Deserializer.fromJava[IO](new StringDeserializer).deserialize(topic, headers, None).attempt
      optional <- Deserializer.fromJava[IO](new StringDeserializer).option.deserialize(topic, headers, None)
      nulled   <- Deserializer.fromJava[IO](nulls).deserialize(topic, headers, bytes).attempt
    yield
      assertEquals(read, "héllo")
      assert(missing.left.toOption.exists(_.isInstanceOf[Deserializer.NullValueException]), missing.toString)
      assertEquals(optional, None)
      assertEquals(nulled.left.toOption.map(_.getMessage).exists(_.contains("'orders'")), true, nulled.toString)
      assert(nulled.left.toOption.exists(_.isInstanceOf[JavaNullValueException]), nulled.toString)

  test("what a Java codec throws fails the effect, when it is run and not before"):
    val failure = new IllegalStateException("broken")
    val broken  =
      new JavaSerializer[String]:
        override def serialize(topic: String, data: String): Array[Byte] = throw failure
    val pending = Serializer.fromJava[IO](broken).serialize(topic, headers, "v")
    pending.attempt.map(result => assertEquals(result, Left(failure)))

  test("a streams serde from Java's writes and reads what the matching xkafka serde does"):
    val java = xkafka.Serde.fromJava[IO](Serdes.String())
    val ours = xkafka.Serde.utf8[IO]
    for
      written <- java.serializer.serialize(topic, headers, "é")
      expect  <- ours.serializer.serialize(topic, headers, "é")
      read    <- java.deserializer.deserialize(topic, headers, expect)
    yield
      assertEquals(written, expect)
      assertEquals(read, "é")

  test("headers go to Java's and back in their order, with the same names twice and a header with no value kept"):
    val java = headers.toJava
    assertEquals(
      java.toArray.toVector.map(header => header.key -> Option(header.value).map(String(_))),
      Vector("trace" -> Some("t1"), "empty" -> None, "trace" -> Some("t2"))
    )
    assertEquals(Headers.fromJava(java), headers)
    val made =
      new RecordHeaders(Array[org.apache.kafka.common.header.Header](new RecordHeader("k", Array.emptyByteArray), new RecordHeader("n", null)))
    assertEquals(Headers.fromJava(made), Headers(Header("k", Some(Chunk.empty)), Header("n", None)))
