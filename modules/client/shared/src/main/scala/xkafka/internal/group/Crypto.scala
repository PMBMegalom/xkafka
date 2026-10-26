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

import scodec.bits.ByteVector

/** The digests the SASL SCRAM exchange is built from, and random bytes for its nonce, from each platform's own cryptography. */
private[xkafka] trait Crypto[F[_]]:
  def hash(digest: Digest, data: ByteVector): F[ByteVector]

  def hmac(digest: Digest, key: ByteVector, data: ByteVector): F[ByteVector]

  /** PBKDF2 over HMAC with the digest, giving as many bytes as the digest does. */
  def pbkdf2(digest: Digest, password: ByteVector, salt: ByteVector, iterations: Int): F[ByteVector]

  def randomBytes(count: Int): F[ByteVector]

/** A digest by its size in bits, which is how the shim names it. */
private[xkafka] enum Digest(val bits: Int) derives CanEqual:
  case Sha256 extends Digest(256)
  case Sha512 extends Digest(512)

  def size: Int = bits / 8
