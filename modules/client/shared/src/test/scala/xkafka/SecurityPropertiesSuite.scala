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

import munit.FunSuite
import internal.security.SecurityProperties

final class SecurityPropertiesSuite extends FunSuite:
  private val authority = CertificateAuthority.PemFile("/etc/kafka/ca.pem")
  private val tls       = TlsSettings.from(authority).toOption.get
  private val sasl      = SaslSettings.from(SaslMechanism.ScramSha256, "user", "secret").toOption.get

  test("the protocol follows the case that was selected"):
    val plaintext = SecuritySettings.Plaintext
    val saslOnly  = SecuritySettings.SaslPlaintext(sasl)

    assertEquals(SecurityProperties.javaClient(plaintext).get("security.protocol"), Some("PLAINTEXT"))
    assertEquals(SecurityProperties.javaClient(SecuritySettings.Tls(tls)).get("security.protocol"), Some("SSL"))
    assertEquals(SecurityProperties.javaClient(saslOnly).get("security.protocol"), Some("SASL_PLAINTEXT"))
    assertEquals(SecurityProperties.javaClient(SecuritySettings.SaslTls(tls, sasl)).get("security.protocol"), Some("SASL_SSL"))
    assertEquals(SecurityProperties.librdkafka(plaintext).get("security.protocol"), Some("plaintext"))
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.Tls(tls)).get("security.protocol"), Some("ssl"))
    assertEquals(SecurityProperties.librdkafka(saslOnly).get("security.protocol"), Some("sasl_plaintext"))
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.SaslTls(tls, sasl)).get("security.protocol"), Some("sasl_ssl"))

  test("a plaintext client is configured with nothing but its protocol"):
    assertEquals(SecurityProperties.javaClient(SecuritySettings.Plaintext), Map("security.protocol" -> "PLAINTEXT"))
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.Plaintext), Map("security.protocol" -> "plaintext"))

  test("a certificate authority is named by each backend"):
    assertEquals(
      SecurityProperties.javaClient(SecuritySettings.Tls(tls)),
      Map(
        "security.protocol"                     -> "SSL",
        "ssl.truststore.type"                   -> "PEM",
        "ssl.truststore.location"               -> "/etc/kafka/ca.pem",
        "ssl.endpoint.identification.algorithm" -> "https"
      )
    )
    assertEquals(
      SecurityProperties.librdkafka(SecuritySettings.Tls(tls)),
      Map("security.protocol" -> "ssl", "ssl.ca.location" -> "/etc/kafka/ca.pem", "ssl.endpoint.identification.algorithm" -> "https")
    )

  test("an inline certificate authority is carried by value"):
    val inline = TlsSettings.from(CertificateAuthority.Pem("-----BEGIN CERTIFICATE-----")).toOption.get

    assertEquals(SecurityProperties.javaClient(SecuritySettings.Tls(inline)).get("ssl.truststore.certificates"), Some("-----BEGIN CERTIFICATE-----"))
    assertEquals(SecurityProperties.javaClient(SecuritySettings.Tls(inline)).get("ssl.truststore.location"), None)
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.Tls(inline)).get("ssl.ca.pem"), Some("-----BEGIN CERTIFICATE-----"))
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.Tls(inline)).get("ssl.ca.location"), None)

  test("the runtime authorities are left to the backend"):
    val system = TlsSettings.from(CertificateAuthority.SystemDefault).toOption.get

    assertEquals(SecurityProperties.javaClient(SecuritySettings.Tls(system)).get("ssl.truststore.location"), None)
    assertEquals(SecurityProperties.javaClient(SecuritySettings.Tls(system)).get("ssl.truststore.type"), None)
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.Tls(system)).get("ssl.ca.location"), None)

  test("hostname verification is turned off with the value each backend expects"):
    val unverified = TlsSettings.from(authority).toOption.get.withHostnameVerification(false)

    assertEquals(SecurityProperties.javaClient(SecuritySettings.Tls(unverified)).get("ssl.endpoint.identification.algorithm"), Some(""))
    assertEquals(SecurityProperties.librdkafka(SecuritySettings.Tls(unverified)).get("ssl.endpoint.identification.algorithm"), Some("none"))

  test("credentials become a login module on the JVM and a username and password elsewhere"):
    val java = SecurityProperties.javaClient(SecuritySettings.SaslTls(tls, sasl))

    assertEquals(java.get("sasl.mechanism"), Some("SCRAM-SHA-256"))
    assertEquals(
      java.get("sasl.jaas.config"),
      Some("""org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="secret";""")
    )
    assertEquals(
      SecurityProperties.librdkafka(SecuritySettings.SaslTls(tls, sasl)).filter((name, _) => name.startsWith("sasl.")),
      Map("sasl.mechanism" -> "SCRAM-SHA-256", "sasl.username" -> "user", "sasl.password" -> "secret")
    )

  test("each mechanism selects its own login module"):
    def module(mechanism: SaslMechanism): Option[String] =
      SecurityProperties.javaClient(SecuritySettings.SaslPlaintext(SaslSettings.from(mechanism, "user", "secret").toOption.get))
        .get("sasl.jaas.config").map(_.takeWhile(_ != ' '))

    assertEquals(module(SaslMechanism.Plain), Some("org.apache.kafka.common.security.plain.PlainLoginModule"))
    assertEquals(module(SaslMechanism.ScramSha256), Some("org.apache.kafka.common.security.scram.ScramLoginModule"))
    assertEquals(module(SaslMechanism.ScramSha512), Some("org.apache.kafka.common.security.scram.ScramLoginModule"))

  test("a credential containing a quote stays inside its JAAS value"):
    val awkward = SaslSettings.from(SaslMechanism.Plain, """us"er""", """se\cret""").toOption.get

    assertEquals(
      SecurityProperties.javaClient(SecuritySettings.SaslPlaintext(awkward)).get("sasl.jaas.config"),
      Some("""org.apache.kafka.common.security.plain.PlainLoginModule required username="us\"er" password="se\\cret";""")
    )
