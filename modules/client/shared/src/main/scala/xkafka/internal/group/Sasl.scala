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

import cats.MonadThrow
import cats.syntax.all.*
import scodec.bits.ByteVector

/** The client's side of the SASL exchanges the group connection makes, given a way to send each message and read the broker's answer to it.
  *
  * The messages are those the Java client sends: PLAIN as RFC 4616 with no authorization identity, SCRAM as RFC 5802 with no channel binding, the
  * password as its UTF-8 bytes and the user name escaped as a SASL name.
  */
private[xkafka] object Sasl:
  /** The fewest iterations the Java client accepts from a broker, for either SCRAM digest. */
  val MinIterations = 4096

  def authenticate[F[_]: MonadThrow](settings: SaslSettings, crypto: Crypto[F], send: ByteVector => F[ByteVector]): F[Unit] =
    settings.mechanism match
      case SaslMechanism.Plain       => send(plain(settings.username, settings.password)).void
      case SaslMechanism.ScramSha256 => crypto.randomBytes(24).flatMap(nonce => scram(Digest.Sha256, settings, nonce.toBase64, crypto, send))
      case SaslMechanism.ScramSha512 => crypto.randomBytes(24).flatMap(nonce => scram(Digest.Sha512, settings, nonce.toBase64, crypto, send))

  def plain(username: String, password: String): ByteVector = utf8(s"\u0000$username\u0000$password")

  def scram[F[_]](digest: Digest, settings: SaslSettings, nonce: String, crypto: Crypto[F], send: ByteVector => F[ByteVector])(using
      F: MonadThrow[F]
  ): F[Unit] =
    val bare = s"n=${saslName(settings.username)},r=$nonce"
    for
      serverFirst <- send(utf8(s"n,,$bare")).flatMap(text)
      first       <- F.fromEither(ServerFirst.read(serverFirst, nonce))
      salted      <- crypto.pbkdf2(digest, utf8(settings.password), first.salt, first.iterations)
      clientKey   <- crypto.hmac(digest, salted, utf8("Client Key"))
      storedKey   <- crypto.hash(digest, clientKey)
      withoutProof = s"c=biws,r=${first.nonce}"
      authMessage  = utf8(s"$bare,$serverFirst,$withoutProof")
      signature   <- crypto.hmac(digest, storedKey, authMessage)
      serverKey   <- crypto.hmac(digest, salted, utf8("Server Key"))
      expected    <- crypto.hmac(digest, serverKey, authMessage)
      serverFinal <- send(utf8(s"$withoutProof,p=${(clientKey ^ signature).toBase64}")).flatMap(text)
      _           <- F.fromEither(verify(serverFinal, expected))
    yield ()

  private final case class ServerFirst(nonce: String, salt: ByteVector, iterations: Int)

  private object ServerFirst:
    def read(message: String, clientNonce: String): Either[Throwable, ServerFirst] =
      val fields = attributes(message)
      for
        nonce <- fields.get("r").toRight(refused(s"the broker's first SCRAM message has no nonce: $message"))
        _     <-
          Either.cond(
            nonce.startsWith(clientNonce) && nonce.length > clientNonce.length,
            (),
            refused("the broker's SCRAM nonce does not extend the client's")
          )
        salt       <- fields.get("s").flatMap(ByteVector.fromBase64(_)).toRight(refused(s"the broker's first SCRAM message has no salt: $message"))
        iterations <- fields.get("i").flatMap(_.toIntOption).toRight(refused(s"the broker's first SCRAM message has no iteration count: $message"))
        _ <- Either.cond(iterations >= MinIterations, (), refused(s"the broker asks for $iterations SCRAM iterations, fewer than $MinIterations"))
      yield ServerFirst(nonce, salt, iterations)

  private def verify(message: String, expected: ByteVector): Either[Throwable, Unit] =
    val fields = attributes(message)
    fields.get("e") match
      case Some(error) => Left(refused(s"the broker refused the SCRAM proof: $error"))
      case None        => Either
          .cond(fields.get("v").flatMap(ByteVector.fromBase64(_)).contains(expected), (), refused("the broker's SCRAM signature does not match"))

  /** The attributes of a SCRAM message, each a letter, `=` and a value, which never holds a comma. */
  private def attributes(message: String): Map[String, String] =
    message.split(',').toList.flatMap(field => Option.when(field.length >= 2 && field(1) == '=')(field.take(1) -> field.drop(2))).toMap

  private def saslName(username: String): String = username.replace("=", "=3D").replace(",", "=2C")

  private def utf8(value: String): ByteVector = ByteVector.view(value.getBytes(UTF_8))

  private def text[F[_]](bytes: ByteVector)(using F: MonadThrow[F]): F[String] =
    F.fromEither(bytes.decodeUtf8.leftMap(_ => refused("the broker's SCRAM message is not UTF-8")))

  private def refused(detail: String): Throwable =
    new KafkaException.BackendFailure(detail, Some(ErrorCode.SaslAuthenticationFailed), Some(ErrorCode.SaslAuthenticationFailed.retriable))
