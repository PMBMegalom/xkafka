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
package internal
package streams

import munit.FunSuite

/** The partition time in committed offsets' metadata, as fixed strings Java Streams writes and reads, on every platform. */
final class PartitionTimeSuite extends FunSuite:
  test("a partition time is written as base64 of version 2 and the time in eight big-endian bytes, with no processor metadata"):
    assertEquals(PartitionTime.encode(1700000000000L), "AgAAAYvP5WgA")

  test("a partition time written is read back"):
    assertEquals(PartitionTime.decode(PartitionTime.encode(1700000000000L)), Some(1700000000000L))

  test("version 1, which has only the time, is read"):
    assertEquals(PartitionTime.decode("AQAAAAAAAAAq"), Some(42L))

  test("version 2 is read with the processor metadata after the time skipped"):
    assertEquals(PartitionTime.decode("AgAAAAAAAAAqAAAAAQAAAAFhAAAAAAAAAAc="), Some(42L))

  test("metadata that is empty, of an unknown version, not base64, too short, or holding the unknown time gives no time, as Java takes it"):
    assertEquals(List("", "AwAAAAAAAAAq", "not base64!", "AgAA", "Av//////////").map(PartitionTime.decode), List.fill(5)(None))
