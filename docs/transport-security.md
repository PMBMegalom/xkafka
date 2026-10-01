# Transport security

TLS and SASL are part of `ClientSettings`, so the same code selects them on
every backend.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.syntax.all.*

import xkafka.*

val password = "secret"

val tls  = TlsSettings.from(CertificateAuthority.PemFile("/etc/kafka/ca.pem"))
val sasl = SaslSettings.from(SaslMechanism.ScramSha256, "user", password)

val client = (tls, sasl).mapN(SecuritySettings.SaslTls.apply).andThen: security =>
  ClientSettings.from(NonEmptyList.one("broker:9093"), security = security)
```

## Protocols

`SecuritySettings` has one case per protocol: `Plaintext`, `Tls`,
`SaslPlaintext`, and `SaslTls`. Each case carries what its protocol needs, so a
client cannot ask for SASL without supplying credentials.

## Certificate authorities

A `CertificateAuthority` is one of:

| case | meaning |
| --- | --- |
| `SystemDefault` | the authorities the runtime already trusts |
| `PemFile` | a path to a PEM file |
| `Pem` | PEM text held in memory |

Scala.js and Scala Native also accept a directory of certificates as a
`PemFile`.

`TlsSettings` verifies the broker hostname by default. Call
`withoutHostnameVerification` to turn that off.

## Credentials

`SaslSettings` accepts `SaslMechanism.Plain`, `ScramSha256`, and `ScramSha512`,
and keeps its password out of `toString`.

## Failures

A rejected broker certificate or an incorrect password fails the call as
`KafkaException.BackendFailure`.

On the JVM and Native, `BackendFailure.code` distinguishes these as
`SslAuthenticationFailed` and `SaslAuthenticationFailed`. The Confluent
JavaScript binding reports both through its transport error code before the
callback reaches xkafka, so Scala.js exposes `NetworkException` instead. Use the
failure detail and broker logs to distinguish certificate and credential
failures on Scala.js.
