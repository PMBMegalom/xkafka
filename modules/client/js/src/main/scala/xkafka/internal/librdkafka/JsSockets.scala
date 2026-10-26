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

import scala.concurrent.duration.FiniteDuration
import scala.scalajs.js
import scala.scalajs.js.typedarray.{byteArray2Int8Array, int8Array2ByteArray, Int8Array, Uint8Array}

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import scodec.bits.ByteVector
import group.{Socket, Sockets}

/** The shim's sockets. Opening, reading and writing each run on a thread the shim starts for them, so the event loop keeps running while one waits.
  */
private[xkafka] object JsSockets:
  private val ErrorBufferSize = 512

  def apply[F[_]](using F: Async[F]): Sockets[F] =
    (host, port, tls, timeout) =>
      Resource.make(open(host, port, tls, timeout))(socket => shim.flatMap(loaded => F.delay(loaded.socketClose(socket)).void)).map: socket =>
        new Socket[F]:
          def write(bytes: ByteVector): F[Unit] =
            F.defer:
              val data = Buffers.alloc(math.max(1L, bytes.size).toInt)
              view(data, bytes.size.toInt).set(byteArray2Int8Array(bytes.toArray))
              called(_.socketWrite)(socket, data, bytes.size.toDouble)

          def read(count: Int): F[ByteVector] =
            F.defer:
              val out = Buffers.alloc(math.max(1, count))
              // Copied once the read has finished writing into the buffer.
              called(_.socketRead)(socket, out, count.toDouble).map(_ => ByteVector.view(int8Array2ByteArray(view(out, count))))

  private def shim[F[_]](using F: Async[F]): F[Shim] = F.fromEither(Shim.loaded)

  private def view(buffer: Buffer, count: Int): Int8Array =
    val bytes = buffer.asInstanceOf[Uint8Array]
    new Int8Array(bytes.buffer, bytes.byteOffset, count)

  private def open[F[_]](host: String, port: Int, tls: Option[TlsSettings], timeout: FiniteDuration)(using F: Async[F]): F[js.Any] =
    F.defer:
      val settings                         = ShimTls(tls)
      val error                            = Buffers.alloc(ErrorBufferSize)
      val code                             = Buffers.alloc(4)
      val result                           = Buffers.alloc(8)
      def c(value: Option[String]): js.Any = value.fold[js.Any](null)(identity)
      background(_.socketOpen)(
        host,
        port,
        timeout.toMillis.toInt,
        if tls.isDefined then 1 else 0,
        c(settings.caLocation),
        c(settings.caPem),
        c(settings.certificateLocation),
        c(settings.keyLocation),
        c(settings.certificatePem),
        c(settings.keyPem),
        c(settings.keyPassword),
        if settings.verifyHostname then 1 else 0,
        error,
        ErrorBufferSize,
        code,
        result
      ) *> F.defer:
        val socket = Koffi.decode(result, "void *")
        if socket == null then F.raiseError(failure(error, code)) else F.pure(socket)

  /** A read or a write, failing with what the shim reported. */
  private def called[F[_]](function: Shim => js.Dynamic)(arguments: js.Any*)(using F: Async[F]): F[Unit] =
    F.defer:
      val error  = Buffers.alloc(ErrorBufferSize)
      val code   = Buffers.alloc(4)
      val result = Buffers.alloc(4)
      background(function)((arguments ++ Seq[js.Any](error, ErrorBufferSize, code, result))*) *>
        F.defer(if result.readInt32LE(0) != 0 then F.raiseError(failure(error, code)) else F.unit)

  private def background[F[_]](function: Shim => js.Dynamic)(arguments: js.Any*)(using F: Async[F]): F[Unit] =
    shim.flatMap: loaded =>
      F.async_[Unit]: callback =>
        val token   = Completions.expect(() => callback(Right(())))
        val started = function(loaded)((arguments ++ Seq[js.Any](Completions.done, js.BigInt(token.toString)))*)
        if started.asInstanceOf[Int] != 0 then
          Completions.forget(token)
          callback(Left(Failures.plain("could not start a thread for a socket call")))

  private def failure(error: Buffer, code: Buffer): KafkaException.BackendFailure =
    val end = error.indexOf(0)
    Failures.coded(error.toString("utf8", 0, if end < 0 then ErrorBufferSize else end), code.readInt32LE(0))
