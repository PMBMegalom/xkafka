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
package librdkafka

import scala.scalanative.libc.string.memcpy
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import cats.effect.Sync
import scodec.bits.ByteVector
import group.{Crypto, Digest}

/** The shim's cryptography, which calls the OpenSSL librdkafka is built against. */
@link("rdkafka") @link("crypto") @extern
private[librdkafka] object CryptoBindings:
  def xkafka_hash(bits: CInt, data: CVoidPtr, dataSize: CInt, out: CVoidPtr): CInt = extern

  def xkafka_hmac(bits: CInt, key: CVoidPtr, keySize: CInt, data: CVoidPtr, dataSize: CInt, out: CVoidPtr): CInt = extern

  def xkafka_pbkdf2(
      bits: CInt,
      password: CVoidPtr,
      passwordSize: CInt,
      salt: CVoidPtr,
      saltSize: CInt,
      iterations: CInt,
      out: CVoidPtr,
      outSize: CInt
  ): CInt = extern

  def xkafka_random_bytes(out: CVoidPtr, size: CInt): CInt = extern

/** OpenSSL through the shim. Each call is short, so it runs on the calling thread. */
private[xkafka] object ShimCrypto:
  import CryptoBindings.*

  def apply[F[_]](using F: Sync[F]): Crypto[F] =
    new Crypto[F]:
      def hash(digest: Digest, data: ByteVector): F[ByteVector] = hashOfSize(digest.bits, data)

      def hmac(digest: Digest, key: ByteVector, data: ByteVector): F[ByteVector] =
        computed("an HMAC", digest.size)(out => xkafka_hmac(digest.bits, input(key), key.size.toInt, input(data), data.size.toInt, out))

      def pbkdf2(digest: Digest, password: ByteVector, salt: ByteVector, iterations: Int): F[ByteVector] =
        computed("a PBKDF2 key", digest.size): out =>
          xkafka_pbkdf2(digest.bits, input(password), password.size.toInt, input(salt), salt.size.toInt, iterations, out, digest.size)

      def randomBytes(count: Int): F[ByteVector] = computed("random bytes", count)(out => xkafka_random_bytes(out, count))

  /** A digest named by its size in bits, which the shim refuses for a size it has no digest of. */
  def hashOfSize[F[_]: Sync](bits: Int, data: ByteVector): F[ByteVector] =
    computed("a digest", math.max(0, bits / 8))(out => xkafka_hash(bits, input(data), data.size.toInt, out))

  /** A copy of the bytes for the call, with room for one byte when there are none, which C never reads past a size of 0. */
  private def input(bytes: ByteVector)(using Zone): CVoidPtr =
    val pointer = alloc[Byte](math.max(1L, bytes.size).toInt)
    if bytes.nonEmpty then memcpy(pointer, bytes.toArray.at(0), bytes.size.toUSize): Unit
    pointer

  private def computed[F[_]](what: String, size: Int)(call: Zone ?=> CVoidPtr => CInt)(using F: Sync[F]): F[ByteVector] =
    F.delay:
      Zone:
        val out = alloc[Byte](math.max(1, size))
        if call(out) != 0 then throw Failures.plain(s"OpenSSL could not compute $what")
        val bytes = new Array[Byte](size)
        if size > 0 then memcpy(bytes.at(0), out, size.toUSize): Unit
        ByteVector.view(bytes)
