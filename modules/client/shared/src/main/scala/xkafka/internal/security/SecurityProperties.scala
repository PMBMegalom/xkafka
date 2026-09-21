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
package internal.security

/** The backend spellings of the portable security settings.
  *
  * Both tables live here so that the one place the backends disagree stays visible in one file.
  */
private[xkafka] object SecurityProperties:
  def javaClient(settings: SecuritySettings): Map[String, String] =
    val protocol =
      settings match
        case SecuritySettings.Plaintext        => "PLAINTEXT"
        case SecuritySettings.Tls(_)           => "SSL"
        case SecuritySettings.SaslPlaintext(_) => "SASL_PLAINTEXT"
        case SecuritySettings.SaslTls(_, _)    => "SASL_SSL"

    val tls =
      settings.tlsSettings.fold(Map.empty[String, String]): value =>
        val authority =
          value.certificateAuthority match
            case CertificateAuthority.SystemDefault     => Map.empty[String, String]
            case CertificateAuthority.PemFile(path)     => Map("ssl.truststore.type" -> "PEM", "ssl.truststore.location" -> path)
            case CertificateAuthority.Pem(certificates) => Map("ssl.truststore.type" -> "PEM", "ssl.truststore.certificates" -> certificates)
        // The Java client turns endpoint identification off through an empty algorithm.
        authority.updated("ssl.endpoint.identification.algorithm", if value.verifyHostname then "https" else "")

    val sasl =
      settings.saslSettings.fold(Map.empty[String, String]): value =>
        Map("sasl.mechanism" -> value.mechanism.name, "sasl.jaas.config" -> jaasConfig(value))

    tls ++ sasl.updated("security.protocol", protocol)

  def librdkafka(settings: SecuritySettings): Map[String, String] =
    val protocol =
      settings match
        case SecuritySettings.Plaintext        => "plaintext"
        case SecuritySettings.Tls(_)           => "ssl"
        case SecuritySettings.SaslPlaintext(_) => "sasl_plaintext"
        case SecuritySettings.SaslTls(_, _)    => "sasl_ssl"

    val tls =
      settings.tlsSettings.fold(Map.empty[String, String]): value =>
        val authority =
          value.certificateAuthority match
            case CertificateAuthority.SystemDefault     => Map.empty[String, String]
            case CertificateAuthority.PemFile(path)     => Map("ssl.ca.location" -> path)
            case CertificateAuthority.Pem(certificates) => Map("ssl.ca.pem" -> certificates)
        authority.updated("ssl.endpoint.identification.algorithm", if value.verifyHostname then "https" else "none")

    val sasl =
      settings.saslSettings.fold(Map.empty[String, String]): value =>
        Map("sasl.mechanism" -> value.mechanism.name, "sasl.username" -> value.username, "sasl.password" -> value.password)

    tls ++ sasl.updated("security.protocol", protocol)

  private def jaasConfig(settings: SaslSettings): String =
    val module =
      settings.mechanism match
        case SaslMechanism.Plain                                   => "org.apache.kafka.common.security.plain.PlainLoginModule"
        case SaslMechanism.ScramSha256 | SaslMechanism.ScramSha512 => "org.apache.kafka.common.security.scram.ScramLoginModule"
    s"""$module required username="${quoted(settings.username)}" password="${quoted(settings.password)}";"""

  /** A JAAS value is a quoted string, so a credential containing a quote or a backslash has to escape it. */
  private def quoted(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"")
