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

import cats.effect.{MonadCancelThrow, Outcome, Ref, Resource, Temporal}
import cats.effect.std.Mutex
import cats.syntax.all.*
import scodec.bits.ByteVector
import internal.protocol.*

/** A connection of the group connection's own to one broker, which has said which version of each request it reads and, where the settings ask for
  * it, has logged the client in.
  */
private[xkafka] trait Connection[F[_]]:
  /** Sends the request in the highest version both sides read and reads the broker's answer. Requests go one at a time, and a request the broker
    * reads in no version this codec writes fails as an unsupported version.
    */
  def send[R](request: Request[R]): F[R]

/** Connections to the brokers of one cluster, made as `settings` ask, each waiting at most `timeout` for any one read or write. */
private[xkafka] final class Connections[F[_]](sockets: Sockets[F], crypto: Crypto[F], settings: ClientSettings, timeout: FiniteDuration)(using
    F: Temporal[F]
):
  import Connections.*

  private val clientId = settings.clientId.getOrElse(DefaultClientId)

  def to(host: String, port: Int): Resource[F, Connection[F]] =
    for
      socket   <- sockets.connect(host, port, settings.security.tlsSettings, timeout)
      exchange <- Resource.eval((Mutex[F], F.ref(Option(0))).mapN(Exchange(socket, clientId, _, _)))
      versions <- Resource.eval(negotiate(exchange))
      connection =
        new Connection[F]:
          def send[R](request: Request[R]): F[R] = version(versions, request.api).flatMap(exchange(request, _))
      _ <- Resource.eval(settings.security.saslSettings.traverse_(login(connection, _)))
    yield connection

  /** A connection to the first bootstrap server that answers, in the order the settings list them. A server that refuses the client's settings or
    * credentials ends the search, since the others would refuse them too.
    */
  def bootstrap: Resource[F, Connection[F]] =
    val servers = settings.bootstrapServers.map(address)
    servers.tail.foldLeft(to.tupled(servers.head)): (earlier, server) =>
      earlier.recoverWith:
        case error: KafkaException.BackendFailure if error.retriable.contains(true) => to.tupled(server)

  /** A connection to the broker that coordinates `group`, which a bootstrap server names. A coordinator still being chosen or loaded is asked for
    * again every 100 ms, as the Java client does, until `timeout` has passed.
    */
  def coordinator(group: String): Resource[F, Connection[F]] = Resource.eval(bootstrap.use(find(_, group))).flatMap(to.tupled)

  private def find(connection: Connection[F], group: String): F[(String, Int)] =
    def ask(start: FiniteDuration): F[(String, Int)] =
      connection.send(FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List(group))).flatMap: answers =>
        answers.find(_.key == group) match
          case None                                => F.raiseError(invalid(s"the broker named no coordinator for group $group"))
          case Some(found) if found.errorCode == 0 => F.pure((found.host, found.port))
          case Some(found)                         =>
            val error =
              failure(ErrorCode.fromProtocol(found.errorCode), found.errorMessage.getOrElse(s"the broker named no coordinator for group $group"))
            F.monotonic.flatMap: now =>
              if error.retriable.contains(true) && now - start + Backoff < timeout then F.sleep(Backoff) *> ask(start) else F.raiseError(error)
    F.monotonic.flatMap(ask)

  private def negotiate(exchange: Exchange[F]): F[Map[Api, Short]] =
    val request = ApiVersionsRequest(SoftwareName, SoftwareVersion)
    exchange(request, Api.ApiVersions.highest).flatMap: answer =>
      // A broker that does not read the version asked for lists the versions it reads, so the request goes again in one of them.
      if answer.errorCode == UnsupportedVersion then
        version(Api.negotiate(answer.apis), Api.ApiVersions).flatMap(exchange(request, _)).flatMap(accepted)
      else accepted(answer)

  private def accepted(answer: ApiVersionsResponse): F[Map[Api, Short]] =
    if answer.errorCode == 0 then F.pure(Api.negotiate(answer.apis))
    else F.raiseError(failure(ErrorCode.fromProtocol(answer.errorCode), "the broker did not say which versions it reads"))

  private def version(versions: Map[Api, Short], api: Api): F[Short] =
    versions.get(api).liftTo[F](failure(ErrorCode.UnsupportedVersion, s"the broker reads no version of $api from ${api.lowest} to ${api.highest}"))

  private def login(connection: Connection[F], sasl: SaslSettings): F[Unit] =
    val mechanism = sasl.mechanism.name
    val handshake =
      connection.send(SaslHandshakeRequest(mechanism)).flatMap: answer =>
        F.raiseUnless(answer.errorCode == 0):
          failure(ErrorCode.SaslAuthenticationFailed, s"the broker does not take SASL $mechanism, only ${answer.mechanisms.mkString(", ")}")
    handshake *> Sasl.authenticate(sasl, crypto, bytes => connection.send(SaslAuthenticateRequest(bytes)).flatMap(authenticated))

  private def authenticated(answer: SaslAuthenticateResponse): F[ByteVector] =
    if answer.errorCode == 0 then F.pure(answer.authBytes)
    else F.raiseError(failure(ErrorCode.fromProtocol(answer.errorCode), answer.errorMessage.getOrElse("the broker refused the SASL login")))

private[xkafka] object Connections:
  /** What the client calls itself where the settings give it no id. */
  val DefaultClientId = "xkafka"

  /** How the client names itself to the broker, which keeps it for its metrics. */
  val SoftwareName    = "xkafka"
  val SoftwareVersion = "unknown"

  /** How long to wait before asking again for a coordinator, the Java client's `retry.backoff.ms`. */
  val Backoff: FiniteDuration = 100.millis

  private val UnsupportedVersion: Short = 35

  /** A bootstrap server as host and port, with the brackets of an IPv6 address removed. */
  def address(server: String): (String, Int) =
    val trimmed   = server.trim
    val separator = trimmed.lastIndexOf(':')
    (trimmed.take(separator).stripPrefix("[").stripSuffix("]"), trimmed.drop(separator + 1).toInt)

  private[group] def failure(code: ErrorCode, detail: String): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(detail, Some(code), Some(code.retriable))

  private[group] def invalid(detail: String): KafkaException = new KafkaException.InvalidBackendResponse(detail)

/** Requests written one at a time, each answer read before the next request goes, so that the answer read is always the last request's. Once a
  * request fails, its answer may still be on its way, so the connection takes no more requests.
  */
private final class Exchange[F[_]](socket: Socket[F], clientId: String, lock: Mutex[F], next: Ref[F, Option[Int]])(using F: MonadCancelThrow[F]):
  import Connections.*

  def apply[R](request: Request[R], version: Short): F[R] =
    lock.lock.surround:
      next.get.flatMap:
        case None     => F.raiseError(failure(ErrorCode.NetworkException, "an earlier request on this connection failed"))
        case Some(id) =>
          val answered = socket.write(Frame.request(request, version, id, clientId)) *> answer
          F.guaranteeCase(answered.flatMap(bytes => F.fromEither(Frame.response(request, version, id)(bytes).leftMap(invalid)))):
            case Outcome.Succeeded(_) => next.set(Some(id + 1))
            case _                    => next.set(None)

  private def answer: F[ByteVector] =
    socket.read(4).map(_.toInt()).flatMap: size =>
      if size < 0 then F.raiseError(invalid(s"the broker announced an answer of $size bytes")) else socket.read(size)
