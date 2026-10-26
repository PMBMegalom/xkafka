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
package internal.group

import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite
import scodec.bits.*

/** Each platform's cryptography against published test vectors. */
final class CryptoSuite extends CatsEffectSuite:
  private val crypto = PlatformCrypto[IO]

  private def text(value: String): ByteVector = ByteVector.view(value.getBytes(UTF_8))

  test("SHA-256 and SHA-512 digests (FIPS 180-2), of nothing as well"):
    for
      sha256 <- crypto.hash(Digest.Sha256, text("abc"))
      sha512 <- crypto.hash(Digest.Sha512, text("abc"))
      empty  <- crypto.hash(Digest.Sha256, ByteVector.empty)
    yield
      assertEquals(sha256, hex"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
      assertEquals(
        sha512,
        hex"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
      )
      assertEquals(empty, hex"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

  test("HMAC-SHA-256 and HMAC-SHA-512 (RFC 4231, test case 2)"):
    for
      sha256 <- crypto.hmac(Digest.Sha256, text("Jefe"), text("what do ya want for nothing?"))
      sha512 <- crypto.hmac(Digest.Sha512, text("Jefe"), text("what do ya want for nothing?"))
    yield
      assertEquals(sha256, hex"5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")
      assertEquals(
        sha512,
        hex"164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737"
      )

  test("PBKDF2 over HMAC-SHA-256 and HMAC-SHA-512, for one iteration and for many"):
    for
      once   <- crypto.pbkdf2(Digest.Sha256, text("password"), text("salt"), 1)
      twice  <- crypto.pbkdf2(Digest.Sha256, text("password"), text("salt"), 2)
      many   <- crypto.pbkdf2(Digest.Sha256, text("password"), text("salt"), 4096)
      sha512 <- crypto.pbkdf2(Digest.Sha512, text("password"), text("salt"), 1)
    yield
      assertEquals(once, hex"120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b")
      assertEquals(twice, hex"ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43")
      assertEquals(many, hex"c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a")
      assertEquals(
        sha512,
        hex"867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce"
      )

  test("random bytes come in the number asked for, and differ from one draw to the next"):
    (crypto.randomBytes(32), crypto.randomBytes(32)).mapN: (first, second) =>
      assertEquals(first.size, 32L)
      assertEquals(second.size, 32L)
      assertNotEquals(first, second)
