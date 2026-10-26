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

import scala.concurrent.duration.FiniteDuration

import cats.effect.Resource
import scodec.bits.ByteVector

/** A connection of the group connection's own to one broker, over TLS when the settings ask for it. */
private[xkafka] trait Sockets[F[_]]:
  /** Connects within `timeout`, which then also bounds each read and write.
    *
    * Settings the platform cannot use fail with `InvalidConfiguration` before anything connects, a broker the client does not trust or that refuses
    * the client's certificate with `SslAuthenticationFailed`, and an unreachable broker with `NetworkException`.
    */
  def connect(host: String, port: Int, tls: Option[TlsSettings], timeout: FiniteDuration): Resource[F, Socket[F]]

private[xkafka] trait Socket[F[_]]:
  def write(bytes: ByteVector): F[Unit]

  /** Exactly `count` bytes. A broker that closes the connection first fails it with `NetworkException`, and one that sends nothing for the timeout
    * with `RequestTimedOut`.
    */
  def read(count: Int): F[ByteVector]
