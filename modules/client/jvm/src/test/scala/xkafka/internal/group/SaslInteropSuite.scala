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

import scala.jdk.CollectionConverters.*

import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback}
import javax.security.sasl.Sasl as JavaSasl
import cats.effect.IO
import munit.CatsEffectSuite
import org.apache.kafka.common.security.scram.ScramCredentialCallback
import org.apache.kafka.common.security.scram.internals.{ScramFormatter, ScramMechanism, ScramSaslServer}
import scodec.bits.ByteVector

/** The SASL exchanges against the Java client's own SCRAM server, and PLAIN against the JDK's PLAIN client. */
final class SaslInteropSuite extends CatsEffectSuite:
  private def settings(mechanism: SaslMechanism, username: String, password: String): SaslSettings =
    SaslSettings.from(mechanism, username, password).orFail

  /** A SCRAM server holding the credential for `stored`, as a broker holds it. */
  private def server(mechanism: ScramMechanism, stored: String): ScramSaslServer =
    val credential               = new ScramFormatter(mechanism).generateCredential(stored, 8192)
    val handler: CallbackHandler =
      callbacks =>
        callbacks.foreach:
          case found: ScramCredentialCallback => found.scramCredential(credential)
          case _: Callback                    => ()
    new ScramSaslServer(mechanism, Map.empty[String, Any].asJava, handler)

  private def exchange(server: ScramSaslServer): ByteVector => IO[ByteVector] =
    message => IO.blocking(ByteVector.view(Option(server.evaluateResponse(message.toArray)).getOrElse(Array.emptyByteArray)))

  List(SaslMechanism.ScramSha256 -> ScramMechanism.SCRAM_SHA_256, SaslMechanism.ScramSha512 -> ScramMechanism.SCRAM_SHA_512).foreach:
    (mechanism, java) =>
      test(s"${mechanism.name} authenticates to the Java client's SCRAM server, with a user name to escape and a password beyond ASCII"):
        val scram = server(java, "pässwörd")
        Sasl.authenticate(settings(mechanism, "user=a,b", "pässwörd"), PlatformCrypto[IO], exchange(scram)).map: _ =>
          assert(scram.isComplete)
          assertEquals(scram.getAuthorizationID, "user=a,b")

      test(s"${mechanism.name} with the wrong password is refused by the Java client's SCRAM server"):
        Sasl.authenticate(settings(mechanism, "user", "wrong"), PlatformCrypto[IO], exchange(server(java, "right"))).attempt.map: outcome =>
          assert(outcome.isLeft, outcome)

  test("PLAIN sends what the JDK's PLAIN client sends"):
    val handler: CallbackHandler =
      callbacks =>
        callbacks.foreach:
          case name: NameCallback         => name.setName("user")
          case password: PasswordCallback => password.setPassword("pässwörd".toCharArray)
          case _: Callback                => ()
    val client = JavaSasl.createSaslClient(Array("PLAIN"), null, "kafka", "localhost", null, handler)
    assertEquals(Sasl.plain("user", "pässwörd"), ByteVector.view(client.evaluateChallenge(Array.emptyByteArray)))
