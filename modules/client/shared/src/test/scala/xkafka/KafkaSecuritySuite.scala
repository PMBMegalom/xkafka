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

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.IO
import munit.{CatsEffectSuite, TestOptions}

/** Behaviour every backend is expected to share against a TLS and SASL broker.
  *
  * A case listed in `divergent` is marked as expected to fail on the named backends. Fixing the backend makes the marked case fail, so a marker
  * cannot outlive the divergence it records.
  */
final class KafkaSecuritySuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val backend = PlatformKafkaClient.name

  private def secure(name: String, divergent: Set[String]): TestOptions =
    if divergent.contains(backend) then TestOptions(name).fail else TestOptions(name)

  private val configuration =
    for
      authority  <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY")
      pem        <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM")
      tlsServer  <- PlatformKafkaClient.environment("XKAFKA_SECURITY_TLS_BOOTSTRAP_SERVERS")
      saslServer <- PlatformKafkaClient.environment("XKAFKA_SECURITY_SASL_BOOTSTRAP_SERVERS")
      user       <- PlatformKafkaClient.environment("XKAFKA_SECURITY_USERNAME")
      secret     <- PlatformKafkaClient.environment("XKAFKA_SECURITY_PASSWORD")
    yield (authority, pem, tlsServer, saslServer, user, secret)

  configuration match
    case None =>
      test("round trip over TLS".ignore)(IO.unit)
      test("round trip over TLS with an inline certificate authority".ignore)(IO.unit)
      test("round trip over SASL over TLS".ignore)(IO.unit)
      test("refuse a broker certificate from an unknown authority".ignore)(IO.unit)
      test("refuse incorrect SASL credentials".ignore)(IO.unit)
    case Some((authority, pem, tlsServer, saslServer, user, secret)) =>
      val tls  = validTls(CertificateAuthority.PemFile(authority))
      val sasl = validSasl(user, secret)

      test("round trip over TLS"):
        roundTrip(tlsServer, SecuritySettings.Tls(tls))
      test("round trip over TLS with an inline certificate authority"):
        roundTrip(tlsServer, SecuritySettings.Tls(validTls(CertificateAuthority.Pem(pem))))
      test("round trip over SASL over TLS"):
        roundTrip(saslServer, SecuritySettings.SaslTls(tls, sasl))
      // The test authority is not one the runtime already trusts, so reaching the broker with the
      // default authorities would mean the certificate is never verified.
      //
      // The JavaScript backend reports both refusals as a transport failure. Its client collapses
      // librdkafka's SSL and authentication codes into one before they reach the callback, and
      // leaves the cause only in the text of a log event.
      test(secure("refuse a broker certificate from an unknown authority", Set("js"))):
        refuse(tlsServer, SecuritySettings.Tls(validTls(CertificateAuthority.SystemDefault)), ErrorCode.SslAuthenticationFailed)
      test(secure("refuse incorrect SASL credentials", Set("js"))):
        refuse(saslServer, SecuritySettings.SaslTls(tls, validSasl(user, s"$secret-wrong")), ErrorCode.SaslAuthenticationFailed)

  private def roundTrip(bootstrapServer: String, security: SecuritySettings): IO[Unit] =
    val suffix = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topic  = validTopic(s"xkafka-security-$suffix")
    val group  = validConsumerGroup(s"xkafka-security-$suffix")
    val record = ProducerRecord(topic, "security-key", s"security-value-${PlatformKafkaClient.name}")

    for
      clientSettings   <- ClientSettings.from(NonEmptyList.one(bootstrapServer), security = security).liftTo[IO]
      producerSettings <- ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer).liftTo[IO]
      consumerSettings <- ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest).liftTo[IO]
      _                <-
        PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(NonEmptyList.one(record)))
          .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("secure Kafka producer timed out")))
      consumed <-
        PlatformKafkaClient().consumer(consumerSettings, Subscription.Topics(NonEmptyList.one(topic))).use(_.records.take(1).compile.lastOrError)
          .timeoutTo(60.seconds, IO.raiseError(new RuntimeException("secure Kafka consumer timed out")))
    yield
      assertEquals(consumed.record.topicPartition.topic, topic)
      assertEquals(consumed.record.key, record.key)
      assertEquals(consumed.record.value, record.value)

  private def refuse(bootstrapServer: String, security: SecuritySettings, expected: ErrorCode): IO[Unit] =
    val suffix = s"${PlatformKafkaClient.name}-${System.currentTimeMillis()}"
    val topic  = validTopic(s"xkafka-security-refused-$suffix")

    for
      clientSettings   <- ClientSettings.from(NonEmptyList.one(bootstrapServer), security = security).liftTo[IO]
      producerSettings <-
        ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer, PlatformKafkaClient.impatientProducerProperties).liftTo[IO]
      outcome <-
        PlatformKafkaClient().producer(producerSettings).use(_.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "value"))))
          .timeout(90.seconds).attempt
    yield outcome match
      case Right(_)                                   => fail("the producer reached the broker")
      case Left(error: KafkaException.BackendFailure) => assertEquals(error.code, Some(expected), error.getMessage)
      case Left(error)                                => fail(s"expected a backend failure, got $error")

  private val utf8Serializer   = Serializer.utf8[IO]
  private val utf8Deserializer = Deserializer.utf8[IO]

  private def validTls(authority: CertificateAuthority): TlsSettings =
    TlsSettings.from(authority).fold(errors => fail(s"invalid test TLS settings: $errors"), identity)

  private def validSasl(username: String, password: String): SaslSettings =
    SaslSettings.from(SaslMechanism.ScramSha256, username, password).fold(errors => fail(s"invalid test SASL settings: $errors"), identity)

  private def validTopic(value: String): Topic = Topic.from(value).fold(error => fail(s"invalid test topic: $error"), identity)

  private def validConsumerGroup(value: String): ConsumerGroup =
    ConsumerGroup.from(value).fold(error => fail(s"invalid test consumer group: $error"), identity)
