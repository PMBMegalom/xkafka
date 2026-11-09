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

import fs2.Chunk
import munit.FunSuite

/** The hash on every platform, against values Java's `Murmur3.hash128` gives. */
final class Murmur3Suite extends FunSuite:
  private val known =
    List(
      ("", -7122613646888064702L, -8341524471658347240L),
      ("a", 3702349557448790408L, -1601067453332442517L),
      ("abcdefgh", -3899774358444468009L, -7475653065840408363L),
      ("abcdefghi", 5468294767348829347L, 9107382057419093116L),
      ("0123456789abcdef", 1904622552327088487L, -2893518741649893711L),
      ("0123456789abcdefg", -6253751916947851771L, -5841899321478313166L),
      ("héllo wörld, this is thirty-two b", -8905638582927538171L, -5420399791493232398L)
    )

  known.foreach: (text, first, second) =>
    test(s"the hash of \"$text\" is Java's"):
      assertEquals(Murmur3.hash128(Chunk.array(text.getBytes("UTF-8"))), (first, second))
