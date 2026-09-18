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

final class ModelSuite extends FunSuite:
  test("topic rejects an empty name") {
    assertEquals(Topic.from(""), Left(ValidationError.EmptyTopic))
  }

  test("partition rejects a negative value") {
    assertEquals(
      Partition.from(-1),
      Left(ValidationError.NegativePartition(-1))
    )
  }

  test("offset next detects overflow") {
    val offset = Offset.from(Long.MaxValue).toOption.get

    assertEquals(offset.next, Left(ValidationError.OffsetOverflow))
  }

  test("headers preserve duplicates and insertion order") {
    val first   = Header("trace", Some(Chunk.array(Array[Byte](1))))
    val second  = Header("other", None)
    val third   = Header("trace", Some(Chunk.array(Array[Byte](2))))
    val headers = Headers(first, second, third)

    assertEquals(headers.values, Vector(first, second, third))
    assertEquals(
      headers.getAll("trace").map(_.map(_.toList)),
      Vector(Some(List[Byte](1)), Some(List[Byte](2)))
    )
  }
