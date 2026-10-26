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

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite
import internal.protocol.*

/** The group connection reaching a group's coordinator through each of the broker's listeners, and refused where the data-plane clients are. */
final class GroupConnectionSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 2.minutes

  private def env(name: String): Option[String] = PlatformKafkaClient.environment(name)

  private def server(variable: String): Option[String] = env(variable).map(_.split(',').head)

  private def connections(servers: NonEmptyList[String], security: SecuritySettings): Connections[IO] =
    val settings =
      ClientSettings.from(servers, Some("xkafka-group-connection"), security = security)
        .fold(errors => fail(s"invalid test settings: $errors"), identity)
    Connections(PlatformSockets[IO], PlatformCrypto[IO], settings, 30.seconds)

  /** Connects to the coordinator of a new group, which, asked again, names the broker it is. */
  private def reachesCoordinator(servers: NonEmptyList[String], security: SecuritySettings, expected: String): IO[Unit] =
    val group = s"xkafka-group-connection-${PlatformKafkaClient.name}-${System.nanoTime()}"
    connections(servers, security).coordinator(group).use(_.send(FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List(group)))).map: answer =>
      assertEquals(answer.map(found => (found.errorCode, s"${found.host}:${found.port}")), List((0: Short, expected)))

  private def refused(server: String, security: SecuritySettings, code: ErrorCode): IO[Unit] =
    connections(NonEmptyList.one(server), security).coordinator("xkafka-group-connection-refused").use_.attempt.map:
      case Left(error: KafkaException.BackendFailure) => assertEquals(error.code, Some(code), error.getMessage)
      case other                                      => fail(s"expected a backend failure with $code, got $other")

  /** Sends the request in the given version straight over the socket and reads the answer. */
  private def exchange[R](socket: Socket[IO], request: Request[R], version: Short): IO[R] =
    for
      _      <- socket.write(Frame.request(request, version, version.toInt, "xkafka-group-connection"))
      size   <- socket.read(4).map(_.toInt())
      answer <-
        socket.read(size).flatMap(bytes => IO.fromEither(Frame.response(request, version, version.toInt)(bytes).leftMap(new RuntimeException(_))))
    yield answer

  private def tls(authority: String, certificate: Option[ClientCertificate] = None): TlsSettings =
    TlsSettings.from(CertificateAuthority.PemFile(authority), clientCertificate = certificate)
      .fold(errors => fail(s"invalid test TLS settings: $errors"), identity)

  private def sasl(mechanism: SaslMechanism, username: String, password: String): SaslSettings =
    SaslSettings.from(mechanism, username, password).fold(errors => fail(s"invalid test SASL settings: $errors"), identity)

  server("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS") match
    case Some(plaintext) =>
      test("the coordinator is reached over plaintext"):
        reachesCoordinator(NonEmptyList.one(plaintext), SecuritySettings.Plaintext, plaintext)
      test("the coordinator is reached through a bootstrap server listed after one that cannot be reached"):
        reachesCoordinator(NonEmptyList.of("127.0.0.1:1", plaintext), SecuritySettings.Plaintext, plaintext)
      test("the broker reads every version of the requests the connection makes, and answers each version of ApiVersions and FindCoordinator"):
        val group           = s"xkafka-group-connection-versions-${PlatformKafkaClient.name}-${System.nanoTime()}"
        val (host, port)    = Connections.address(plaintext)
        def every(api: Api) = (api.lowest.toInt to api.highest.toInt).map(_.toShort).toList
        for
          _       <- connections(NonEmptyList.one(plaintext), SecuritySettings.Plaintext).coordinator(group).use_
          answers <-
            PlatformSockets[IO].connect(host, port, None, 30.seconds).use: socket =>
              for
                listed <- every(Api.ApiVersions).traverse(version => exchange(socket, ApiVersionsRequest("xkafka", "0.6.0"), version))
                found  <-
                  every(Api.FindCoordinator)
                    .traverse(version => exchange(socket, FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List(group)), version))
              yield (listed, found)
        yield
          val (listed, found) = answers
          assertEquals(listed.map(_.errorCode), every(Api.ApiVersions).as(0: Short))
          // Which heartbeat versions a broker reads depends on its release, which the group member's tests check.
          Api.values.filterNot(_ == Api.StreamsGroupHeartbeat).foreach: api =>
            val offered = listed.head.apis.find(_.key == api.key)
            assert(offered.exists(range => range.min <= api.lowest && range.max >= api.highest), s"$api: the broker reads $offered")
          assertEquals(
            found.map(_.map(c => (c.key, c.errorCode, s"${c.host}:${c.port}"))),
            every(Api.FindCoordinator).as(List((group, 0: Short, plaintext)))
          )
    case None =>
      test("the broker reads every version of the requests the connection makes, and answers each version of ApiVersions and FindCoordinator".ignore)(
        IO.unit
      )
      test("the coordinator is reached over plaintext".ignore)(IO.unit)
      test("the coordinator is reached through a bootstrap server listed after one that cannot be reached".ignore)(IO.unit)

  (
    server("XKAFKA_SECURITY_TLS_BOOTSTRAP_SERVERS"),
    server("XKAFKA_SECURITY_MTLS_BOOTSTRAP_SERVERS"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY"),
    env("XKAFKA_SECURITY_CLIENT_PEM")
  ) match
    case (Some(tlsServer), Some(mtlsServer), Some(authority), Some(client)) =>
      test("the coordinator is reached over TLS, and over TLS with a client certificate"):
        reachesCoordinator(NonEmptyList.one(tlsServer), SecuritySettings.Tls(tls(authority)), tlsServer) *> reachesCoordinator(
          NonEmptyList.one(mtlsServer),
          SecuritySettings.Tls(tls(authority, Some(ClientCertificate.PemFile(client, None)))),
          mtlsServer
        )
    case _ => test("the coordinator is reached over TLS, and over TLS with a client certificate".ignore)(IO.unit)

  (
    server("XKAFKA_SECURITY_SASL_BOOTSTRAP_SERVERS"),
    env("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY"),
    env("XKAFKA_SECURITY_USERNAME"),
    env("XKAFKA_SECURITY_PASSWORD")
  ) match
    case (Some(saslServer), Some(authority), Some(username), Some(password)) => SaslMechanism.values.foreach: mechanism =>
        test(s"the coordinator is reached over SASL ${mechanism.name} over TLS"):
          reachesCoordinator(NonEmptyList.one(saslServer), SecuritySettings.SaslTls(tls(authority), sasl(mechanism, username, password)), saslServer)
        test(s"a wrong password is refused over SASL ${mechanism.name} as a SASL authentication failure, as the data-plane clients refuse it"):
          refused(
            saslServer,
            SecuritySettings.SaslTls(tls(authority), sasl(mechanism, username, s"$password-wrong")),
            ErrorCode.SaslAuthenticationFailed
          )
    case _ => SaslMechanism.values.foreach: mechanism =>
        test(s"the coordinator is reached over SASL ${mechanism.name} over TLS".ignore)(IO.unit)
        test(s"a wrong password is refused over SASL ${mechanism.name} as a SASL authentication failure, as the data-plane clients refuse it".ignore)(
          IO.unit
        )
