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
import scala.scalanative.libc.string.memcpy
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import cats.effect.{Resource, Sync}
import scodec.bits.ByteVector
import group.{Socket, Sockets}

@link("rdkafka") @link("ssl") @link("crypto") @extern
private[librdkafka] object SocketBindings:
  def xkafka_socket_open(
      host: CString,
      port: CInt,
      timeoutMs: CInt,
      tls: CInt,
      caLocation: CString,
      caPem: CString,
      certificateLocation: CString,
      keyLocation: CString,
      certificatePem: CString,
      keyPem: CString,
      keyPassword: CString,
      verifyHostname: CInt,
      error: CString,
      errorSize: CSize,
      errorCode: Ptr[CInt]
  ): CVoidPtr = extern

  def xkafka_socket_write(socket: CVoidPtr, data: CVoidPtr, size: CSize, error: CString, errorSize: CSize, errorCode: Ptr[CInt]): CInt = extern

  def xkafka_socket_read(socket: CVoidPtr, out: CVoidPtr, size: CSize, error: CString, errorSize: CSize, errorCode: Ptr[CInt]): CInt = extern

  def xkafka_socket_close(socket: CVoidPtr): Unit = extern

/** The shim's sockets, each call blocking a thread of the blocking pool for at most the socket's timeout. */
private[xkafka] object ShimSockets:
  import SocketBindings.*

  private val ErrorBufferSize = 512

  def apply[F[_]](using F: Sync[F]): Sockets[F] =
    (host, port, tls, timeout) =>
      Resource.make(F.blocking(open(host, port, tls, timeout)))(socket => F.blocking(xkafka_socket_close(socket))).map: socket =>
        new Socket[F]:
          def write(bytes: ByteVector): F[Unit] =
            F.blocking:
              Zone:
                val data = alloc[Byte](math.max(1L, bytes.size).toInt)
                if bytes.nonEmpty then memcpy(data, bytes.toArray.at(0), bytes.size.toUSize): Unit
                val (error, code) = errorSlot
                if xkafka_socket_write(socket, data, bytes.size.toUSize, error, ErrorBufferSize.toUSize, code) != 0 then throw failure(error, code)

          def read(count: Int): F[ByteVector] =
            F.blocking:
              Zone:
                val out           = alloc[Byte](math.max(1, count))
                val (error, code) = errorSlot
                if xkafka_socket_read(socket, out, count.toUSize, error, ErrorBufferSize.toUSize, code) != 0 then throw failure(error, code)
                val bytes = new Array[Byte](count)
                if count > 0 then memcpy(bytes.at(0), out, count.toUSize): Unit
                ByteVector.view(bytes)

  private def open(host: String, port: Int, tls: Option[TlsSettings], timeout: FiniteDuration): CVoidPtr =
    Zone:
      val settings                          = ShimTls(tls)
      val (error, code)                     = errorSlot
      def c(value: Option[String]): CString = value.fold(null: CString)(toCString(_))
      val socket                            =
        xkafka_socket_open(
          toCString(host),
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
          ErrorBufferSize.toUSize,
          code
        )
      if socket == null then throw failure(error, code)
      socket

  private def errorSlot(using Zone): (CString, Ptr[CInt]) =
    val error = alloc[CChar](ErrorBufferSize)
    val code  = alloc[CInt](1)
    !code = 0
    (error, code)

  private def failure(error: CString, code: Ptr[CInt]): KafkaException.BackendFailure = Failures.coded(fromCString(error), !code)
