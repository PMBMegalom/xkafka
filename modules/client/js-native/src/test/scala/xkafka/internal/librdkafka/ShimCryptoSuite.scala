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
package internal.librdkafka

import cats.effect.IO
import munit.CatsEffectSuite
import scodec.bits.*

/** What the shim's cryptography answers when OpenSSL cannot compute what it was asked for. */
final class ShimCryptoSuite extends CatsEffectSuite:
  test("a digest of a size the shim has none of is refused"):
    for
      known   <- ShimCrypto.hashOfSize[IO](256, hex"616263")
      unknown <- ShimCrypto.hashOfSize[IO](384, hex"616263").attempt
    yield
      assertEquals(known, hex"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
      unknown match
        case Left(error: KafkaException.BackendFailure) => assertEquals(error.detail, "OpenSSL could not compute a digest")
        case other                                      => fail(s"expected a refusal, got $other")
