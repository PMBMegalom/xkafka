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
package rocksdb

import munit.FunSuite

/** What only the JavaScript backend does: find the shim and library through the npm package, and check the shim's identity. */
final class JsRocksdbSuite extends FunSuite:
  test("the npm package finds this platform's shim and RocksDB library, which load"):
    assert(clue(PrebuiltRocksdb.error) == null)
    assert(clue(PrebuiltRocksdb.shim) != null)
    assert(clue(PrebuiltRocksdb.library) != null)
    assert(clue(JsRocksdb.loaded).isRight)

  test("a shim reporting another identity is refused"):
    val refused = JsRocksdb.load(PrebuiltRocksdb.shim.nn, PrebuiltRocksdb.library.nn, "10.1.3-000000000000", BuildInfo.rocksdb)
    assertEquals(
      refused.left.map(_.getMessage),
      Left(s"the RocksDB shim at ${PrebuiltRocksdb.shim} is ${BuildInfo.rocksdbShim}, but this xkafka needs 10.1.3-000000000000")
    )
