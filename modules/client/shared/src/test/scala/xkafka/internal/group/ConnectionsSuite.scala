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

import cats.data.{NonEmptyList, StateT}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scodec.bits.ByteVector
import internal.protocol.*
import internal.protocol.Wire.*

/** The connection's framing, negotiation, login and search for the coordinator, against a broker the tests script. */
final class ConnectionsSuite extends CatsEffectSuite:
  import ConnectionsSuite.*

  private val group = FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List("group"))

  test("a connection asks for versions first, then sends each request in the highest version both sides read, with the next correlation id"):
    for
      broker   <- FakeBroker(standard(versions(findCoordinator = 5)))
      _        <- connections(broker).to("broker", 9092).use(_.send(group))
      received <- broker.received
    yield
      assertEquals(received.map(r => (r.api, r.version, r.correlationId)), Vector((Api.ApiVersions, 4: Short, 0), (Api.FindCoordinator, 5: Short, 1)))
      assertEquals(received.map(_.clientId).distinct, Vector(Some("fake-client")))

  test("a broker that does not read the version asked for is asked again in one it lists"):
    val older = versions(apiVersions = 3)
    for
      broker   <- FakeBroker(scripted(older) { case r if r.api == Api.ApiVersions && r.version == 4 => IO.pure(Reply.Body(apiVersionsV0(older))) })
      answer   <- connections(broker).to("broker", 9092).use(_.send(group))
      received <- broker.received
    yield
      assertEquals(answer.map(_.port), List(9092))
      assertEquals(received.filter(_.api == Api.ApiVersions).map(_.version), Vector[Short](4, 3))

  test("a broker that will not say which versions it reads is refused with its error"):
    FakeBroker(scripted(versions()) { case r if r.api == Api.ApiVersions => IO.pure(Reply.Body(apiVersions(Nil, error = 42))) }).flatMap: broker =>
      refusedWith(connections(broker).to("broker", 9092).use_, ErrorCode.Other(42))

  test("a request the broker reads in no version the codec writes is refused as not supporting the version, without being sent"):
    for
      broker   <- FakeBroker(standard(versions(findCoordinator = 3)))
      outcome  <- connections(broker).to("broker", 9092).use(_.send(group).attempt)
      received <- broker.received
    yield
      assertEquals(code(outcome), Some(ErrorCode.UnsupportedVersion))
      assertEquals(received.map(_.api), Vector(Api.ApiVersions))

  test("an answer to another request fails as an invalid answer, and the connection then takes no more requests"):
    for
      broker <-
        FakeBroker(scripted(versions()) {
          case r if r.api == Api.FindCoordinator =>
            IO.pure(Reply.Body(findCoordinator("group", "broker", 9092), correlationId = Some(r.correlationId + 1)))
        })
      outcomes <- connections(broker).to("broker", 9092).use(connection => (connection.send(group).attempt, connection.send(group).attempt).tupled)
      received <- broker.received
    yield
      assert(outcomes._1.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]), outcomes._1)
      assertEquals(code(outcomes._2), Some(ErrorCode.NetworkException))
      assertEquals(received.count(_.api == Api.FindCoordinator), 1)

  test("an answer announced with a negative size fails as an invalid answer"):
    FakeBroker(scripted(versions()) { case r if r.api == Api.FindCoordinator => IO.pure(Reply.Raw(int32(-1))) }).flatMap: broker =>
      connections(broker).to("broker", 9092).use(_.send(group)).attempt.map: outcome =>
        assert(outcome.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]), outcome)

  test("requests sent together each get their own answer"):
    val keys = (0 until 20).toList.map(index => s"group-$index")
    for
      broker <-
        FakeBroker(scripted(versions()) {
          case r if r.api == Api.FindCoordinator =>
            IO.fromEither(readAll(readInt8 *> readCompactArray(readCompactString) <* skipTags)(r.body).leftMap(new RuntimeException(_)))
              .map(asked => Reply.Body(findCoordinator(asked.head, "broker", 10000 + asked.head.stripPrefix("group-").toInt)))
        })
      answers <-
        connections(broker).to("broker", 9092).use: connection =>
          keys.parTraverse(key => connection.send(FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List(key))))
    yield assertEquals(answers.map(_.map(found => (found.key, found.port))), keys.zipWithIndex.map((key, index) => List((key, 10000 + index))))

  test("a SASL login names its mechanism, then sends the PLAIN message"):
    for
      broker   <- FakeBroker(standard(versions()))
      _        <- connections(broker, sasl(SaslMechanism.Plain)).to("broker", 9092).use_
      received <- broker.received
    yield
      assertEquals(received.map(_.api), Vector(Api.ApiVersions, Api.SaslHandshake, Api.SaslAuthenticate))
      assertEquals(received(1).body, string("PLAIN"))
      assertEquals(received(2).body, compactBytes(Sasl.plain("user", "secret")) ++ noTags)

  test("a SASL mechanism the broker does not take is refused as a SASL authentication failure that names the ones it takes"):
    FakeBroker(scripted(versions()) { case r if r.api == Api.SaslHandshake => IO.pure(Reply.Body(saslHandshake(33, List("SCRAM-SHA-512")))) })
      .flatMap: broker =>
        connections(broker, sasl(SaslMechanism.Plain)).to("broker", 9092).use_.attempt.map: outcome =>
          assertEquals(code(outcome), Some(ErrorCode.SaslAuthenticationFailed))
          assert(outcome.left.exists(_.getMessage.contains("SCRAM-SHA-512")), outcome)

  test("credentials the broker refuses fail as a SASL authentication failure with the broker's message"):
    FakeBroker(scripted(versions()) {
      case r if r.api == Api.SaslAuthenticate => IO.pure(Reply.Body(saslAuthenticate(58, Some("Invalid credentials"))))
    }).flatMap: broker =>
      connections(broker, sasl(SaslMechanism.Plain)).to("broker", 9092).use_.attempt.map: outcome =>
        assertEquals(code(outcome), Some(ErrorCode.SaslAuthenticationFailed))
        assert(outcome.left.exists(_.getMessage.contains("Invalid credentials")), outcome)

  test("the connection to the coordinator goes to the broker a bootstrap server names, once the group has one"):
    for
      asked  <- IO.ref(0)
      broker <-
        FakeBroker(scripted(versions()) {
          case r if r.api == Api.FindCoordinator =>
            asked.getAndUpdate(_ + 1).map: count =>
              Reply.Body(if count == 0 then findCoordinator("group", "", -1, error = 15) else findCoordinator("group", "coordinator", 9999))
        })
      _        <- connections(broker).coordinator("group").use_
      attempts <- broker.attempts
    yield assertEquals(attempts, Vector(("broker-1", 9092), ("coordinator", 9999)))

  test("a group with no coordinator once the timeout has passed fails as the coordinator not being available"):
    for
      asked  <- IO.ref(0)
      broker <-
        FakeBroker(scripted(versions()) {
          case r if r.api == Api.FindCoordinator => asked.update(_ + 1).as(Reply.Body(findCoordinator("group", "", -1, error = 15)))
        })
      outcome <- connections(broker).coordinator("group").use_.attempt
      count   <- asked.get
    yield
      assertEquals(code(outcome), Some(ErrorCode.CoordinatorNotAvailable))
      assert(count > 1, s"asked $count times")

  test("a refusal to name a coordinator that asking again cannot change fails at once"):
    for
      asked  <- IO.ref(0)
      broker <-
        FakeBroker(scripted(versions()) {
          case r if r.api == Api.FindCoordinator => asked.update(_ + 1).as(Reply.Body(findCoordinator("group", "", -1, error = 30)))
        })
      outcome <- connections(broker).coordinator("group").use_.attempt
      count   <- asked.get
    yield
      assertEquals(code(outcome), Some(ErrorCode.GroupAuthorizationFailed))
      assertEquals(count, 1)

  test("an answer that names no coordinator for the group fails as an invalid answer"):
    FakeBroker(scripted(versions()) {
      case r if r.api == Api.FindCoordinator => IO.pure(Reply.Body(findCoordinator("another-group", "broker", 9092)))
    }).flatMap: broker =>
      connections(broker).coordinator("group").use_.attempt.map: outcome =>
        assert(outcome.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]), outcome)

  test("bootstrap servers are tried in order past those that cannot be reached, with the brackets of an IPv6 address removed"):
    for
      broker   <- FakeBroker(standard(versions()), unreachable = Set(("::1", 9092)))
      _        <- connections(broker, servers = NonEmptyList.of("[::1]:9092", "broker-2:9093")).bootstrap.use_
      attempts <- broker.attempts
    yield assertEquals(attempts, Vector(("::1", 9092), ("broker-2", 9093)))

  test("a bootstrap server that refuses the credentials ends the search"):
    for
      broker   <- FakeBroker(scripted(versions()) { case r if r.api == Api.SaslAuthenticate => IO.pure(Reply.Body(saslAuthenticate(58, None))) })
      outcome  <- connections(broker, sasl(SaslMechanism.Plain), NonEmptyList.of("broker-1:9092", "broker-2:9092")).bootstrap.use_.attempt
      attempts <- broker.attempts
    yield
      assertEquals(code(outcome), Some(ErrorCode.SaslAuthenticationFailed))
      assertEquals(attempts, Vector(("broker-1", 9092)))

  private def refusedWith(run: IO[Unit], expected: ErrorCode): IO[Unit] =
    run.attempt.map(outcome => assertEquals(code(outcome), Some(expected), outcome))

  private def code(outcome: Either[Throwable, ?]): Option[ErrorCode] =
    outcome.left.toOption.collect { case error: KafkaException.BackendFailure => error.code }.flatten

  private def sasl(mechanism: SaslMechanism): SecuritySettings =
    SecuritySettings
      .SaslPlaintext(SaslSettings.from(mechanism, "user", "secret").fold(errors => fail(s"invalid test SASL settings: $errors"), identity))

  private def connections(
      broker: FakeBroker,
      security: SecuritySettings = SecuritySettings.Plaintext,
      servers: NonEmptyList[String] = NonEmptyList.one("broker-1:9092")
  ): Connections[IO] =
    val settings =
      ClientSettings.from(servers, Some("fake-client"), security = security).fold(errors => fail(s"invalid test settings: $errors"), identity)
    Connections(broker, PlatformCrypto[IO], settings, 1.second)

object ConnectionsSuite:
  /** A request as the fake broker read it. */
  final case class Received(api: Api, version: Short, correlationId: Int, clientId: Option[String], body: ByteVector)

  enum Reply:
    /** An answer's body, which the broker sends after its size and header. */
    case Body(bytes: ByteVector, correlationId: Option[Int] = None)

    /** Bytes the broker sends as they are. */
    case Raw(bytes: ByteVector)

  /** A broker that reads each request the client writes and queues its answer, recording where the client connected and what it asked. */
  final class FakeBroker(
      reply: Received => IO[Reply],
      unreachable: Set[(String, Int)],
      tried: Ref[IO, Vector[(String, Int)]],
      log: Ref[IO, Vector[Received]]
  ) extends Sockets[IO]:
    def attempts: IO[Vector[(String, Int)]] = tried.get
    def received: IO[Vector[Received]]      = log.get

    def connect(host: String, port: Int, tls: Option[TlsSettings], timeout: FiniteDuration): Resource[IO, Socket[IO]] =
      val opened =
        tried.update(_ :+ (host, port)) *>
          IO.raiseWhen(unreachable((host, port)))(Connections.failure(ErrorCode.NetworkException, s"$host:$port cannot be reached")) *>
          IO.ref(ByteVector.empty)
      Resource.eval(opened).map: outbox =>
        new Socket[IO]:
          def write(bytes: ByteVector): IO[Unit] =
            for
              request <- IO.fromEither(readAll(readInt32 *> header)(bytes).leftMap(new RuntimeException(_)))
              _       <- log.update(_ :+ request)
              answer  <- reply(request)
              _       <- outbox.update(_ ++ framed(request, answer))
            yield ()

          // A platform's socket cannot read a negative number of bytes.
          def read(count: Int): IO[ByteVector] =
            IO.raiseWhen(count < 0)(new IllegalArgumentException(s"a read of $count bytes")) *>
              outbox.modify(bytes => if bytes.size >= count then (bytes.drop(count.toLong), Some(bytes.take(count.toLong))) else (bytes, None))
                .flatMap(IO.fromOption(_)(Connections.failure(ErrorCode.NetworkException, "the fake broker has nothing more to send")))

  object FakeBroker:
    def apply(reply: Received => IO[Reply], unreachable: Set[(String, Int)] = Set.empty): IO[FakeBroker] =
      (IO.ref(Vector.empty[(String, Int)]), IO.ref(Vector.empty[Received])).mapN(new FakeBroker(reply, unreachable, _, _))

  private val header: Read[Received] =
    for
      key      <- readInt16
      version  <- readInt16
      id       <- readInt32
      clientId <- readNullableString
      api      <- Api.values.find(_.key == key).fold(fail[Api](s"no request has key $key"))(_.pure[Read])
      _        <- if api.flexible then skipTags else ().pure[Read]
      body     <- StateT.get[Result, ByteVector] <* StateT.set[Result, ByteVector](ByteVector.empty)
    yield Received(api, version, id, clientId, body)

  /** The answer's size, its header (the first for ApiVersions and requests that are not flexible, the second with tags otherwise) and its body. */
  private def framed(request: Received, reply: Reply): ByteVector =
    reply match
      case Reply.Raw(bytes)              => bytes
      case Reply.Body(body, correlation) =>
        val tags    = if request.api.flexible && request.api != Api.ApiVersions then noTags else ByteVector.empty
        val message = int32(correlation.getOrElse(request.correlationId)) ++ tags ++ body
        int32(message.size.toInt) ++ message

  /** The versions a broker lists, from 0 to the highest given for each request. */
  def versions(apiVersions: Int = 4, findCoordinator: Int = 6, saslHandshake: Int = 1, saslAuthenticate: Int = 2): List[ApiVersion] =
    List(18 -> apiVersions, 10 -> findCoordinator, 17 -> saslHandshake, 36 -> saslAuthenticate)
      .map((key, max) => ApiVersion(key.toShort, 0, max.toShort))

  /** A broker that answers every request well, naming itself as the coordinator. */
  def standard(listed: List[ApiVersion]): Received => IO[Reply] = scripted(listed)(PartialFunction.empty)

  /** A broker that answers as `script` says where it says anything, and otherwise as `standard`. */
  def scripted(listed: List[ApiVersion])(script: PartialFunction[Received, IO[Reply]]): Received => IO[Reply] =
    request =>
      script.applyOrElse(
        request,
        request =>
          IO:
            Reply.Body(
              request.api match
                case Api.ApiVersions           => apiVersions(listed)
                case Api.FindCoordinator       => findCoordinator("group", "broker", 9092)
                case Api.SaslHandshake         => saslHandshake(0, List("PLAIN"))
                case Api.SaslAuthenticate      => saslAuthenticate(0, None)
                case Api.StreamsGroupHeartbeat => throw new IllegalStateException("a test that sends heartbeats scripts their answers")
            )
      )

  def apiVersions(listed: List[ApiVersion], error: Int = 0): ByteVector =
    int16(error) ++ compactArray(listed)(api => int16(api.key.toInt) ++ int16(api.min.toInt) ++ int16(api.max.toInt) ++ noTags) ++ int32(0) ++ noTags

  /** The answer of a broker that does not read the version asked for, in version 0. */
  def apiVersionsV0(listed: List[ApiVersion]): ByteVector =
    int16(35) ++ array(listed)(api => int16(api.key.toInt) ++ int16(api.min.toInt) ++ int16(api.max.toInt))

  def findCoordinator(key: String, host: String, port: Int, error: Int = 0): ByteVector =
    val coordinator = compactString(key) ++ int32(1) ++ compactString(host) ++ int32(port) ++ int16(error) ++ compactNullableString(None) ++ noTags
    int32(0) ++ compactArray(List(coordinator))(identity) ++ noTags

  def saslHandshake(error: Int, mechanisms: List[String]): ByteVector = int16(error) ++ array(mechanisms)(string)

  def saslAuthenticate(error: Int, message: Option[String]): ByteVector =
    int16(error) ++ compactNullableString(message) ++ compactBytes(ByteVector.empty) ++ int64(0) ++ noTags
