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

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import munit.CatsEffectSuite

/** Behaviour every backend is expected to share against a TLS and SASL broker. */
final class KafkaSecuritySuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val configuration =
    for
      authority  <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY")
      pem        <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PEM")
      tlsServer  <- PlatformKafkaClient.environment("XKAFKA_SECURITY_TLS_BOOTSTRAP_SERVERS")
      saslServer <- PlatformKafkaClient.environment("XKAFKA_SECURITY_SASL_BOOTSTRAP_SERVERS")
      user       <- PlatformKafkaClient.environment("XKAFKA_SECURITY_USERNAME")
      secret     <- PlatformKafkaClient.environment("XKAFKA_SECURITY_PASSWORD")
      plainDir   <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_PLAIN_DIRECTORY")
      hashedDir  <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY_HASHED_DIRECTORY")
    yield (authority, pem, tlsServer, saslServer, user, secret, plainDir, hashedDir)

  private val mutualConfiguration =
    for
      authority   <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CERTIFICATE_AUTHORITY")
      server      <- PlatformKafkaClient.environment("XKAFKA_SECURITY_MTLS_BOOTSTRAP_SERVERS")
      file        <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CLIENT_PEM")
      encrypted   <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CLIENT_ENCRYPTED_PEM")
      certificate <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CLIENT_CERTIFICATE")
      key         <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CLIENT_KEY")
      password    <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CLIENT_KEY_PASSWORD")
      pbes2       <- PlatformKafkaClient.environment("XKAFKA_SECURITY_CLIENT_PBES2_PEM")
    yield (authority, server, file, encrypted, certificate, key, password, pbes2)

  mutualConfiguration match
    case None =>
      test("round trip over TLS with a client certificate file".ignore)(IO.unit)
      test("round trip over TLS with an inline client certificate".ignore)(IO.unit)
      test("round trip over TLS with an encrypted client key".ignore)(IO.unit)
      test("refuse a client that presents no certificate where one is required".ignore)(IO.unit)
      test("refuse an encrypted client key with the wrong password".ignore)(IO.unit)
      test("a client key encrypted with PBES2 is read on Scala.js, Scala Native, and Java 21 or newer, and refused on Java 17".ignore)(IO.unit)
    case Some((authority, server, file, encrypted, certificate, key, password, pbes2)) =>
      def presenting(clientCertificate: ClientCertificate): SecuritySettings =
        SecuritySettings.Tls(
          TlsSettings.from(CertificateAuthority.PemFile(authority), clientCertificate = Some(clientCertificate))
            .fold(errors => fail(s"invalid test TLS settings: $errors"), identity)
        )

      test("round trip over TLS with a client certificate file"):
        roundTrip(server, presenting(ClientCertificate.PemFile(file, None)))
      test("round trip over TLS with an inline client certificate"):
        roundTrip(server, presenting(ClientCertificate.Pem(certificate, key, None)))
      test("round trip over TLS with an encrypted client key"):
        roundTrip(server, presenting(ClientCertificate.PemFile(encrypted, Some(password))))
      test("refuse a client that presents no certificate where one is required"):
        refuse(server, SecuritySettings.Tls(validTls(CertificateAuthority.PemFile(authority))), ErrorCode.SslAuthenticationFailed)
      test("a client key encrypted with PBES2 is read on Scala.js, Scala Native, and Java 21 or newer, and refused on Java 17"):
        val security = presenting(ClientCertificate.PemFile(pbes2, Some(password)))
        PlatformKafkaClient.javaRelease match
          case Some(17)                      => refuse(server, security, ErrorCode.InvalidConfiguration)
          case Some(release) if release < 21 => IO.unit
          case _                             => roundTrip(server, security)
      test("refuse an encrypted client key with the wrong password"):
        val wrong                                                      = presenting(ClientCertificate.PemFile(encrypted, Some(s"$password-wrong")))
        val topic                                                      = validTopic("xkafka-security-undecryptable")
        def refused(name: String, acquired: Resource[IO, ?]): IO[Unit] =
          acquired.use_.attempt.map:
            case Left(error: KafkaException.BackendFailure) =>
              assertEquals(error.code, Some(ErrorCode.InvalidConfiguration), s"$name: ${error.getMessage}")
            case outcome => fail(s"$name: expected a backend failure, got $outcome")
        for
          client   <- ClientSettings.from(NonEmptyList.one(server), security = wrong).liftTo[IO]
          producer <- ProducerSettings.from(client, utf8Serializer, utf8Serializer).liftTo[IO]
          consumer <-
            ConsumerSettings
              .from(client, validConsumerGroup("xkafka-security-undecryptable"), utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest)
              .liftTo[IO]
          _ <- refused("producer", PlatformKafkaClient().producer(producer))
          _ <- refused("consumer", PlatformKafkaClient().consumer(consumer, Selection.Topics(NonEmptySet.one(topic))))
          _ <- refused("admin", PlatformKafkaClient().admin(client))
        yield ()

  configuration match
    case None =>
      test("round trip over TLS".ignore)(IO.unit)
      test("round trip over TLS with an inline certificate authority".ignore)(IO.unit)
      test("round trip over SASL over TLS".ignore)(IO.unit)
      test("refuse a broker certificate from an unknown authority".ignore)(IO.unit)
      test("refuse incorrect SASL credentials".ignore)(IO.unit)
      test("a directory of certificate authorities under their hashed names is read on Scala.js and Scala Native, and refused on the JVM".ignore)(
        IO.unit
      )
      test("a directory of certificate authorities under other names does not verify the broker on Scala.js and Scala Native".ignore)(IO.unit)
    case Some((authority, pem, tlsServer, saslServer, user, secret, plainDir, hashedDir)) =>
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
      test("refuse a broker certificate from an unknown authority"):
        refuse(tlsServer, SecuritySettings.Tls(validTls(CertificateAuthority.SystemDefault)), ErrorCode.SslAuthenticationFailed)
      test("refuse incorrect SASL credentials"):
        refuse(saslServer, SecuritySettings.SaslTls(tls, validSasl(user, s"$secret-wrong")), ErrorCode.SaslAuthenticationFailed)
      test("a directory of certificate authorities under their hashed names is read on Scala.js and Scala Native, and refused on the JVM"):
        val security = SecuritySettings.Tls(validTls(CertificateAuthority.PemFile(hashedDir)))
        if PlatformKafkaClient.javaRelease.isDefined then refuse(tlsServer, security, ErrorCode.InvalidConfiguration)
        else roundTrip(tlsServer, security)
      test("a directory of certificate authorities under other names does not verify the broker on Scala.js and Scala Native"):
        if PlatformKafkaClient.javaRelease.isDefined then IO.unit
        else refuse(tlsServer, SecuritySettings.Tls(validTls(CertificateAuthority.PemFile(plainDir))), ErrorCode.SslAuthenticationFailed)

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
        PlatformKafkaClient().consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.take(1).compile.lastOrError)
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
