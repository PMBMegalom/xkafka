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

package xkafka.internal.rocksdb

import munit.FunSuite

final class RocksdbEncodingSuite extends FunSuite:
  private def bytes(values: Int*): Array[Byte] = values.map(_.toByte).toArray

  test("a batch is each operation and family, then its byte strings, each after its length in four big-endian bytes"):
    val writes =
      Vector(RocksdbWrite.Put(0, bytes(0x80), bytes(1, 2)), RocksdbWrite.Delete(1, bytes()), RocksdbWrite.DeleteRange(254, bytes(0x00), bytes(0xff)))
    val expected =
      Vector(
        1, 0, 0, 0, 0, 1, 0x80, 0, 0, 0, 2, 1, 2,  // put
        2, 1, 0, 0, 0, 0,                          // delete of the empty key
        3, 254, 0, 0, 0, 1, 0x00, 0, 0, 0, 1, 0xff // delete range
      ).map(_.toByte)
    assertEquals(RocksdbEncoding.batch(writes).map(_.toVector), Right(expected))

  test("a length takes all four bytes"):
    val key     = Array.fill[Byte](0x010203)(7)
    val encoded = RocksdbEncoding.batch(Vector(RocksdbWrite.Delete(0, key))).toOption.get
    assertEquals(encoded.slice(0, 6).toVector, bytes(2, 0, 0, 0x01, 0x02, 0x03).toVector)
    assertEquals(encoded.length, 6 + key.length)

  test("a batch naming a family a byte cannot hold is refused"):
    for family <- Vector(-1, 255, 256) do
      val refused = RocksdbEncoding.batch(Vector(RocksdbWrite.Put(family, bytes(1), bytes(2))))
      assertEquals(refused.left.map(_.getMessage), Left(s"no column family $family"))

  test("entries decode to their keys and values in the order they came"):
    val encoded = bytes(0, 0, 0, 1, 0xff, 0, 0, 0, 0, 0, 0, 0, 2, 0x80, 0x00, 0, 0, 0, 1, 9)
    val decoded = RocksdbEncoding.entries(encoded).map((key, value) => (key.toVector, value.toVector))
    assertEquals(decoded, Vector((bytes(0xff).toVector, Vector.empty[Byte]), (bytes(0x80, 0x00).toVector, bytes(9).toVector)))
    assertEquals(RocksdbEncoding.entries(Array.emptyByteArray), Vector.empty)

  test("the key after another is it followed by a zero byte"):
    assertEquals(RocksdbEncoding.after(bytes(0xff)).toVector, bytes(0xff, 0).toVector)
    assertEquals(RocksdbEncoding.after(Array.emptyByteArray).toVector, bytes(0).toVector)
