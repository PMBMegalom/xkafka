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

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import munit.FunSuite

/** The README is not run through mdoc as the site is, so what it shows is compiled here. */
final class DocumentedExampleSuite extends FunSuite:
  test("the documented producer lifts its validated parts into the effect"):
    val utf8 = Serializer.utf8[IO]

    val settings =
      ClientSettings.from(NonEmptyList.one("localhost:9092")).andThen: client =>
        ProducerSettings.from(client, utf8, utf8)

    val program =
      for
        topic    <- Topic.from("events").liftTo[IO]
        producer <- settings.liftTo[IO]
        _        <- KafkaClient[IO].producer(producer).use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "value"))))
      yield ()

    assert(program ne null)

  test("the documented secure client is constructed from validated parts"):
    val password = "secret"

    val tls  = TlsSettings.from(CertificateAuthority.PemFile("/etc/kafka/ca.pem"))
    val sasl = SaslSettings.from(SaslMechanism.ScramSha256, "user", password)

    val client =
      (tls, sasl).mapN(SecuritySettings.SaslTls.apply).andThen: security =>
        ClientSettings.from(NonEmptyList.one("broker:9093"), security = security)

    assert(client.isValid)
    assertEquals(client.toOption.get.security.saslSettings.map(_.mechanism), Some(SaslMechanism.ScramSha256))
