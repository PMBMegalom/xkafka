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

import fs2.Chunk
import munit.FunSuite

final class HeaderSerdeSuite extends FunSuite:
  test("a string round-trips as UTF-8, whatever the runtime's own charset is"):
    val value = "grüße-🌍"

    assertEquals(HeaderSerializer.utf8.serialize(value).toList, value.getBytes("UTF-8").toList)
    assertEquals(Header("k", Some(HeaderSerializer.utf8.serialize(value))).as[String], Right(value))

  test("bytes round-trip untouched"):
    val value = Chunk.array(Array[Byte](1, 2, 3))

    assertEquals(HeaderSerializer.bytes.serialize(value), value)
    assertEquals(Header("k", Some(value)).as[Chunk[Byte]], Right(value))

  test("an int is four bytes, most significant first"):
    assertEquals(HeaderSerializer.int.serialize(1).toList, List[Byte](0, 0, 0, 1))
    assertEquals(HeaderSerializer.int.serialize(-1).toList, List[Byte](-1, -1, -1, -1))
    assertEquals(HeaderSerializer.int.serialize(0x01020304).toList, List[Byte](1, 2, 3, 4))

  test("a long is eight bytes, most significant first"):
    assertEquals(HeaderSerializer.long.serialize(1L).toList, List[Byte](0, 0, 0, 0, 0, 0, 0, 1))
    assertEquals(HeaderSerializer.long.serialize(0x0102030405060708L).toList, List[Byte](1, 2, 3, 4, 5, 6, 7, 8))

  test("every numeric bound round-trips"):
    val ints  = List(0, 1, -1, Int.MaxValue, Int.MinValue)
    val longs = List(0L, 1L, -1L, Long.MaxValue, Long.MinValue)

    assertEquals(ints.map(value => Header("k", Some(HeaderSerializer.int.serialize(value))).as[Int]), ints.map(Right(_)))
    assertEquals(longs.map(value => Header("k", Some(HeaderSerializer.long.serialize(value))).as[Long]), longs.map(Right(_)))

  test("a header with no value cannot be read as anything"):
    assertEquals(Header("k", None).as[String], Left(ValidationError.MissingHeaderValue))
    assertEquals(Header("k", None).as[Int], Left(ValidationError.MissingHeaderValue))
    assertEquals(Header("k", None).as[Chunk[Byte]], Left(ValidationError.MissingHeaderValue))

  test("a numeric value of the wrong width is rejected rather than truncated"):
    val threeBytes = Some(Chunk.array(Array[Byte](1, 2, 3)))

    assertEquals(Header("k", threeBytes).as[Int], Left(ValidationError.InvalidHeaderLength(4, 3)))
    assertEquals(Header("k", threeBytes).as[Long], Left(ValidationError.InvalidHeaderLength(8, 3)))

  test("appending a typed value encodes it, and order is kept"):
    val headers = Headers.empty.append("trace", "abc").append("attempt", 2).append("at", 5L)

    assertEquals(headers.values.map(_.key), Vector("trace", "attempt", "at"))
    assertEquals(headers.values(0).as[String], Right("abc"))
    assertEquals(headers.values(1).as[Int], Right(2))
    assertEquals(headers.values(2).as[Long], Right(5L))

  test("a serializer is contravariant and a deserializer is a functor"):
    val fromLength = HeaderSerializer.int.contramap[String](_.length)
    val asLength   = HeaderDeserializer.utf8.map(_.length)

    assertEquals(fromLength.serialize("abcd").toList, List[Byte](0, 0, 0, 4))
    assertEquals(Header("k", Some(HeaderSerializer.utf8.serialize("abcd"))).as(using asLength), Right(4))

  test("emap rejects a value the bytes could carry"):
    val positive = HeaderDeserializer.int.emap(value => Either.cond(value > 0, value, ValidationError.NegativeOffset(value.toLong)))

    assertEquals(Header("k", Some(HeaderSerializer.int.serialize(3))).as(using positive), Right(3))
    assertEquals(Header("k", Some(HeaderSerializer.int.serialize(-3))).as(using positive), Left(ValidationError.NegativeOffset(-3L)))
