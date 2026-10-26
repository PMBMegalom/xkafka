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

import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scodec.bits.ByteVector

/** The client's side of the SASL exchanges, over each platform's cryptography, against a scripted broker. */
final class SaslSuite extends CatsEffectSuite:
  private val crypto = PlatformCrypto[IO]

  private def settings(mechanism: SaslMechanism, username: String, password: String): SaslSettings =
    SaslSettings.from(mechanism, username, password).fold(errors => fail(s"invalid test settings: $errors"), identity)

  /** A broker that answers each message with the next of its answers, recording what it was sent. */
  private final case class Broker(sent: Ref[IO, List[String]], answers: Ref[IO, List[String]]):
    def send(message: ByteVector): IO[ByteVector] =
      sent.update(_ :+ new String(message.toArray, UTF_8)) *> answers.modify(left => (left.drop(1), left.headOption))
        .flatMap(IO.fromOption(_)(new AssertionError("no answer left"))).map(answer => ByteVector.view(answer.getBytes(UTF_8)))

  private def broker(answers: String*): IO[Broker] = (Ref.of[IO, List[String]](Nil), Ref.of[IO, List[String]](answers.toList)).mapN(Broker.apply)

  // RFC 7677, section 3.
  private val user        = settings(SaslMechanism.ScramSha256, "user", "pencil")
  private val clientNonce = "rOprNGfwEbeRWgbNEkqO"
  private val serverNonce = s"$clientNonce%hvYDpWUa2RaTCAfuxFIlj)hNlF$$k0"
  private val serverFirst = s"r=$serverNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"

  private def refusal(outcome: Either[Throwable, Unit], detail: String): Unit =
    outcome match
      case Left(error: KafkaException.BackendFailure) =>
        assertEquals(error.code, Some(ErrorCode.SaslAuthenticationFailed))
        assertEquals(error.detail, detail)
      case other => fail(s"expected a SASL refusal, got $other")

  test("SCRAM-SHA-256 sends the messages of RFC 7677 and accepts the broker's signature"):
    for
      scripted <- broker(serverFirst, "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
      _        <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, scripted.send)
      messages <- scripted.sent.get
    yield assertEquals(messages, List(s"n,,n=user,r=$clientNonce", s"c=biws,r=$serverNonce,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="))

  test("a broker signature that does not match is refused"):
    for
      scripted <- broker(serverFirst, "v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
      outcome  <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, scripted.send).attempt
    yield refusal(outcome, "the broker's SCRAM signature does not match")

  test("a broker that refuses the proof is reported with its reason"):
    for
      scripted <- broker(serverFirst, "e=invalid-proof")
      outcome  <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, scripted.send).attempt
    yield refusal(outcome, "the broker refused the SCRAM proof: invalid-proof")

  test("a broker nonce that does not extend the client's is refused before any proof is sent"):
    List(s"r=$clientNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", s"r=other$serverNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096").traverse_ : first =>
      for
        scripted <- broker(first)
        outcome  <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, scripted.send).attempt
        messages <- scripted.sent.get
      yield
        refusal(outcome, "the broker's SCRAM nonce does not extend the client's")
        assertEquals(messages.size, 1)

  test("a broker asking for fewer iterations than the Java client accepts is refused"):
    for
      scripted <- broker(s"r=$serverNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4095")
      outcome  <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, scripted.send).attempt
    yield refusal(outcome, "the broker asks for 4095 SCRAM iterations, fewer than 4096")

  test("a first broker message without a salt or an iteration count is refused"):
    for
      noSalt            <- broker(s"r=$serverNonce,i=4096")
      withoutSalt       <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, noSalt.send).attempt
      noIterations      <- broker(s"r=$serverNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==")
      withoutIterations <- Sasl.scram(Digest.Sha256, user, clientNonce, crypto, noIterations.send).attempt
    yield
      refusal(withoutSalt, s"the broker's first SCRAM message has no salt: r=$serverNonce,i=4096")
      refusal(withoutIterations, s"the broker's first SCRAM message has no iteration count: r=$serverNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==")

  test("a user name is escaped as a SASL name, and each exchange draws a fresh nonce"):
    val escaped = settings(SaslMechanism.ScramSha512, "a=b,c", "secret")
    for
      first    <- broker("e=unknown-user")
      _        <- Sasl.authenticate(escaped, crypto, first.send).attempt
      second   <- broker("e=unknown-user")
      _        <- Sasl.authenticate(escaped, crypto, second.send).attempt
      messages <- (first.sent.get, second.sent.get).mapN(_ ++ _)
    yield
      val nonces = messages.map(_.stripPrefix("n,,n=a=3Db=2Cc,r="))
      assert(messages.forall(_.startsWith("n,,n=a=3Db=2Cc,r=")), messages)
      assert(nonces.forall(_.matches("[A-Za-z0-9+/]{32}")), nonces)
      assertNotEquals(nonces(0), nonces(1))

  test("PLAIN sends the user name and password with no authorization identity"):
    for
      scripted <- broker("")
      _        <- Sasl.authenticate(settings(SaslMechanism.Plain, "user", "pencil"), crypto, scripted.send)
      messages <- scripted.sent.get
    yield assertEquals(messages, List("\u0000user\u0000pencil"))
