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

package xkafka.internal.librdkafka

import scala.scalajs.js
import scala.scalajs.js.typedarray.{byteArray2Int8Array, int8Array2ByteArray, Int8Array}

import cats.effect.Sync
import cats.syntax.all.*
import scodec.bits.ByteVector
import xkafka.internal.group.{Crypto, Digest}

/** The OpenSSL in the prebuilt library, through the shim. Each call is short, so it runs on the calling thread. */
private[xkafka] object ShimCrypto:
  def apply[F[_]](using F: Sync[F]): Crypto[F] =
    new Crypto[F]:
      def hash(digest: Digest, data: ByteVector): F[ByteVector] = hashOfSize(digest.bits, data)

      def hmac(digest: Digest, key: ByteVector, data: ByteVector): F[ByteVector] =
        computed("an HMAC", digest.size)((shim, out) => shim.hmac(digest.bits, input(key), key.size.toInt, input(data), data.size.toInt, out))

      def pbkdf2(digest: Digest, password: ByteVector, salt: ByteVector, iterations: Int): F[ByteVector] =
        computed("a PBKDF2 key", digest.size): (shim, out) =>
          shim.pbkdf2(digest.bits, input(password), password.size.toInt, input(salt), salt.size.toInt, iterations, out, digest.size)

      def randomBytes(count: Int): F[ByteVector] = computed("random bytes", count)((shim, out) => shim.randomBytes(out, count))

  /** A digest named by its size in bits, which the shim refuses for a size it has no digest of. */
  def hashOfSize[F[_]: Sync](bits: Int, data: ByteVector): F[ByteVector] =
    computed("a digest", math.max(0, bits / 8))((shim, out) => shim.hash(bits, input(data), data.size.toInt, out))

  /** An empty input still needs a buffer to point at, which C never reads past its size of 0. */
  private def input(bytes: ByteVector): Int8Array = if bytes.isEmpty then new Int8Array(1) else byteArray2Int8Array(bytes.toArray)

  private def computed[F[_]](what: String, size: Int)(call: (Shim, Int8Array) => js.Dynamic)(using F: Sync[F]): F[ByteVector] =
    F.defer(F.fromEither(Shim.loaded)).flatMap: shim =>
      F.delay:
        val out = new Int8Array(math.max(1, size))
        if call(shim, out).asInstanceOf[Int] != 0 then throw Failures.plain(s"OpenSSL could not compute $what")
        ByteVector.view(int8Array2ByteArray(out)).take(size.toLong)
