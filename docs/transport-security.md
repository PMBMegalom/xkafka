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

## Client certificates

A broker can require each client to present a certificate of its own. Give
`TlsSettings` a `ClientCertificate`, which is one of:

| case | meaning |
| --- | --- |
| `PemFile` | a path to one PEM file holding the private key and the certificate chain |
| `Pem` | the certificate chain and the private key, as PEM text held in memory |

```scala mdoc:compile-only
import xkafka.*

val tls =
  TlsSettings.from(
    CertificateAuthority.PemFile("/etc/kafka/ca.pem"),
    clientCertificate = Some(ClientCertificate.PemFile("/etc/kafka/client.pem", keyPassword = None))
  )
```

The private key must be PKCS#8, which PEM marks as `PRIVATE KEY`, or as
`ENCRYPTED PRIVATE KEY` when it needs `keyPassword`. The Java client reads no
other form, so an inline key in another form, such as `RSA PRIVATE KEY`, is a
`SettingsError`. `openssl pkcs8 -topk8` converts one. The certificate's key and
password are kept out of `toString`.

Java 17 can't read a key encrypted with PBES2, the scheme `openssl pkcs8`
uses by default, so on the JVM with Java 17 acquiring the client fails with
`InvalidConfiguration`. Encrypt the key with `openssl pkcs8 -topk8 -v1
PBE-SHA1-3DES` instead, which every backend reads. Java 21 reads both.

## Credentials

`SaslSettings` accepts `SaslMechanism.Plain`, `ScramSha256`, and `ScramSha512`,
and keeps its password out of `toString`.

## Failures

Each of these fails as `KafkaException.BackendFailure`, and its `code` says
which:

| failure | `code` |
| --- | --- |
| the broker's certificate is not trusted | `SslAuthenticationFailed` |
| the broker refuses the client's certificate, or wants one and gets none | `SslAuthenticationFailed` |
| the SASL credentials are incorrect | `SaslAuthenticationFailed` |
| `keyPassword` does not decrypt the client's key | `InvalidConfiguration` |

The last one fails when the producer, consumer or admin client is acquired,
before any connection is made.
