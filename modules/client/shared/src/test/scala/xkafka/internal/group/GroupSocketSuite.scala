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

import scala.concurrent.duration.*

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite
import internal.protocol.{ApiVersionsRequest, ApiVersionsResponse, Frame, Wire}

/** The group connection's sockets against the broker's plaintext, TLS and mutual TLS listeners, with what the data-plane clients accept and refuse.
  */
final class GroupSocketSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 2.minutes

  private val sockets = PlatformSockets[IO]
  private val timeout = 10.seconds

  private def address(variable: String): Option[(String, Int)] =
    PlatformKafkaClient.environment(variable).map(_.split(',').head).map: server =>
      val separator = server.lastIndexOf(':')
      (server.take(separator), server.drop(separator + 1).toInt)

  private def env(name: String): Option[String] = PlatformKafkaClient.environment(name)

  private def tls(authority: CertificateAuthority, certificate: Option[ClientCertificate] = None): TlsSettings =
    TlsSettings.from(authority, clientCertificate = certificate).fold(errors => fail(s"invalid test TLS settings: $errors"), identity)

  /** Asks the broker which versions it reads, the first request any client sends. */
  private def apiVersions(socket: Socket[IO]): IO[ApiVersionsResponse] =
    val request = ApiVersionsRequest("xkafka", "0.6.0")
    for
      _      <- socket.write(Frame.request(request, 3, 1, "xkafka-group-socket"))
      size   <- socket.read(4).flatMap(bytes => IO.fromEither(Wire.readAll(Wire.readInt32)(bytes).leftMap(new RuntimeException(_))))
      answer <- socket.read(size).flatMap(bytes => IO.fromEither(Frame.response(request, 3, 1)(bytes).leftMap(new RuntimeException(_))))
    yield answer

  private def answers(server: (String, Int), security: Option[TlsSettings]): IO[Unit] =
    sockets.connect(server._1, server._2, security, timeout).use(apiVersions).map: answer =>
      assertEquals(answer.errorCode, 0.toShort)
      assert(answer.apis.exists(_.key == 18), answer)

  private def refused(server: (String, Int), security: Option[TlsSettings], code: ErrorCode): IO[Unit] =
    sockets.connect(server._1, server._2, security, timeout).use(apiVersions).attempt.map:
      case Left(error: KafkaException.BackendFailure) => assertEquals(error.code, Some(code), error.getMessage)
      case other                                      => fail(s"expected a backend failure with $code, got $other")

  address("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS") match
    case None =>
      test("a plaintext connection is answered".ignore)(IO.unit)
      test("an unreachable broker fails as a network failure".ignore)(IO.unit)
      test("a broker that closes the connection fails a read as a network failure".ignore)(IO.unit)
      test("a broker that sends nothing fails a read once the timeout passes".ignore)(IO.unit)
    case Some(server) =>
      test("a plaintext connection is answered"):
        answers(server, None)
      test("an unreachable broker fails as a network failure"):
        refused((server._1, 1), None, ErrorCode.NetworkException)
      test("a broker that closes the connection fails a read as a network failure"):
        // A request of negative size is one the broker refuses by closing the connection.
        sockets.connect(server._1, server._2, None, timeout).use(socket => socket.write(Wire.int32(-1)) *> socket.read(4)).attempt.map:
          case Left(error: KafkaException.BackendFailure) => assertEquals(error.code, Some(ErrorCode.NetworkException), error.getMessage)
          case other                                      => fail(s"expected a network failure, got $other")
      test("a broker that sends nothing fails a read once the timeout passes"):
        sockets.connect(server._1, server._2, None, 2.seconds).use(_.read(4)).timed.attempt.map:
          case Left(error: KafkaException.BackendFailure) => assertEquals(error.code, Some(ErrorCode.RequestTimedOut), error.getMessage)
          case Right((elapsed, _))                        => fail(s"read something after $elapsed")
          case Left(error)                                => fail(s"expected a timeout, got $error")

  (
    address("XKAFKA_SECURITY_TLS_BOOTSTRAP_SERVERS"),
    address("XKAFKA_SECURITY_SASL_BOOTSTRAP_SERVERS"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PLAIN_DIRECTORY"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_HASHED_DIRECTORY")
  ) match
    case (Some(tlsServer), Some(saslServer), Some(authority), Some(pem), Some(plainDir), Some(hashedDir)) =>
      test("a TLS connection is answered, with the authority from a file or held in memory"):
        answers(tlsServer, Some(tls(CertificateAuthority.PemFile(authority)))) *> answers(tlsServer, Some(tls(CertificateAuthority.Pem(pem))))
      test("a TLS connection to the SASL listener is answered before any authentication"):
        answers(saslServer, Some(tls(CertificateAuthority.PemFile(authority))))
      test("authorities held in memory that hold no certificate are refused before connecting"):
        refused(tlsServer, Some(tls(CertificateAuthority.Pem("not a certificate"))), ErrorCode.InvalidConfiguration)
      test("a broker certificate from an authority the client does not trust is refused"):
        refused(tlsServer, Some(tls(CertificateAuthority.SystemDefault)), ErrorCode.SslAuthenticationFailed)
      test("a directory of authorities under their hashed names is read on Scala.js and Scala Native, and refused on the JVM"):
        val security = Some(tls(CertificateAuthority.PemFile(hashedDir)))
        if PlatformKafkaClient.javaRelease.isDefined then refused(tlsServer, security, ErrorCode.InvalidConfiguration)
        else answers(tlsServer, security)
      test("a directory of authorities under other names does not verify the broker on Scala.js and Scala Native"):
        if PlatformKafkaClient.javaRelease.isDefined then IO.unit
        else refused(tlsServer, Some(tls(CertificateAuthority.PemFile(plainDir))), ErrorCode.SslAuthenticationFailed)
    case _ =>
      test("a TLS connection is answered, with the authority from a file or held in memory".ignore)(IO.unit)
      test("a TLS connection to the SASL listener is answered before any authentication".ignore)(IO.unit)
      test("authorities held in memory that hold no certificate are refused before connecting".ignore)(IO.unit)
      test("a broker certificate from an authority the client does not trust is refused".ignore)(IO.unit)
      test("a directory of authorities under their hashed names is read on Scala.js and Scala Native, and refused on the JVM".ignore)(IO.unit)
      test("a directory of authorities under other names does not verify the broker on Scala.js and Scala Native".ignore)(IO.unit)

  (address("XKAFKA_SECURITY_WRONG_NAME_BOOTSTRAP_SERVERS"), env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY")) match
    case (Some(server), Some(authority)) =>
      test("a trusted broker certificate naming another host is refused, unless the client does not check the name"):
        val checked = tls(CertificateAuthority.PemFile(authority))
        refused(server, Some(checked), ErrorCode.SslAuthenticationFailed) *> answers(server, Some(checked.withoutHostnameVerification))
    case _ => test("a trusted broker certificate naming another host is refused, unless the client does not check the name".ignore)(IO.unit)

  (
    address("XKAFKA_SECURITY_MTLS_BOOTSTRAP_SERVERS"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY"),
    env("XKAFKA_SECURITY_CLIENT_PEM"),
    env("XKAFKA_SECURITY_CLIENT_ENCRYPTED_PEM"),
    env("XKAFKA_SECURITY_CLIENT_CERTIFICATE"),
    env("XKAFKA_SECURITY_CLIENT_KEY"),
    env("XKAFKA_SECURITY_CLIENT_KEY_PASSWORD"),
    env("XKAFKA_SECURITY_CLIENT_PBES2_PEM")
  ) match
    case (Some(server), Some(authority), Some(file), Some(encrypted), Some(certificate), Some(key), Some(password), Some(pbes2)) =>
      def presenting(clientCertificate: ClientCertificate): Option[TlsSettings] =
        Some(tls(CertificateAuthority.PemFile(authority), Some(clientCertificate)))
      test("a client certificate is presented from a file, from memory, and with an encrypted key"):
        answers(server, presenting(ClientCertificate.PemFile(file, None))) *>
          answers(server, presenting(ClientCertificate.Pem(certificate, key, None))) *>
          answers(server, presenting(ClientCertificate.PemFile(encrypted, Some(password))))
      test("a client that presents no certificate where one is required is refused"):
        refused(server, Some(tls(CertificateAuthority.PemFile(authority))), ErrorCode.SslAuthenticationFailed)
      test("an encrypted client key with the wrong password is refused before connecting"):
        refused(server, presenting(ClientCertificate.PemFile(encrypted, Some(s"$password-wrong"))), ErrorCode.InvalidConfiguration)
      test("a client key encrypted with PBES2 is read on Scala.js, Scala Native, and Java 21 or newer, and refused on Java 17"):
        val security = presenting(ClientCertificate.PemFile(pbes2, Some(password)))
        PlatformKafkaClient.javaRelease match
          case Some(17)                      => refused(server, security, ErrorCode.InvalidConfiguration)
          case Some(release) if release < 21 => IO.unit
          case _                             => answers(server, security)
    case _ =>
      test("a client certificate is presented from a file, from memory, and with an encrypted key".ignore)(IO.unit)
      test("a client that presents no certificate where one is required is refused".ignore)(IO.unit)
      test("an encrypted client key with the wrong password is refused before connecting".ignore)(IO.unit)
      test("a client key encrypted with PBES2 is read on Scala.js, Scala Native, and Java 21 or newer, and refused on Java 17".ignore)(IO.unit)
