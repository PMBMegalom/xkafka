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
package kafkaclients

import java.io.IOException
import java.net.{InetSocketAddress, Socket as JavaSocket, SocketTimeoutException}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

import javax.net.ssl.{SSLException, SSLSocket}
import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
import scodec.bits.ByteVector
import group.{Socket, Sockets}
import security.SecurityProperties

/** JDK sockets. TLS is set up by the engine factory the Java client itself uses, from the same properties, so the group connection accepts and
  * refuses exactly the certificates, keys and authorities the data-plane clients do.
  */
private[xkafka] object JavaSockets:
  def apply[F[_]](using F: Sync[F]): Sockets[F] =
    (host, port, tls, timeout) =>
      Resource.make(F.blocking(open(host, port, tls, timeout)).adaptError(failure))(socket => F.blocking(socket.close())).map: socket =>
        new Socket[F]:
          private val in  = socket.getInputStream
          private val out = socket.getOutputStream

          def write(bytes: ByteVector): F[Unit] = F.blocking { out.write(bytes.toArray); out.flush() }.adaptError(failure)

          def read(count: Int): F[ByteVector] =
            F.blocking(in.readNBytes(count)).adaptError(failure).flatMap: bytes =>
              if bytes.length == count then F.pure(ByteVector.view(bytes))
              else F.raiseError(network(s"the broker closed the connection after ${bytes.length} of $count bytes"))

  private def open(host: String, port: Int, tls: Option[TlsSettings], timeout: FiniteDuration): JavaSocket =
    // The factory reads the authority and the client's key at once, so settings it cannot use fail before anything connects.
    val engines = tls.map(factory)
    val millis  = timeout.toMillis.toInt
    val plain   = new JavaSocket()
    try
      plain.connect(new InetSocketAddress(host, port), millis)
      plain.setSoTimeout(millis)
      plain.setTcpNoDelay(true)
      engines.fold(plain): (settings, factory) =>
        val parameters = factory.createClientSslEngine(host, port, if settings.verifyHostname then "https" else "").getSSLParameters
        val secured    = factory.sslContext.getSocketFactory.createSocket(plain, host, port, true).asInstanceOf[SSLSocket]
        secured.setSSLParameters(parameters)
        secured.startHandshake()
        secured
    catch
      case error: Throwable =>
        plain.close()
        throw error

  private def factory(settings: TlsSettings): (TlsSettings, DefaultSslEngineFactory) =
    val properties = SecurityProperties.javaClient(SecuritySettings.Tls(settings)).filter((name, _) => name.startsWith("ssl."))
    val factory    = new DefaultSslEngineFactory
    factory.configure(new ConfigDef().withClientSslSupport().parse(properties.asJava))
    (settings, factory)

  private val failure: PartialFunction[Throwable, Throwable] =
    JavaFailures.portable.orElse:
      case error: SocketTimeoutException => coded(ErrorCode.RequestTimedOut, error)
      case error: SSLException           => coded(ErrorCode.SslAuthenticationFailed, error)
      case error: IOException            => coded(ErrorCode.NetworkException, error)

  private def coded(code: ErrorCode, error: Throwable): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(Option(error.getMessage).getOrElse(error.getClass.getName), Some(code), Some(code.retriable), cause = error)

  private def network(detail: String): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(detail, Some(ErrorCode.NetworkException), Some(ErrorCode.NetworkException.retriable))
