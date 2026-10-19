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

import cats.data.ValidatedNel

/** Where a broker certificate is verified from. */
enum CertificateAuthority derives CanEqual:
  /** The authorities the runtime already trusts. */
  case SystemDefault

  /** A PEM file. Scala.js and Scala Native also accept a directory of them. */
  case PemFile(path: String)

  /** PEM encoded authorities held in memory. */
  case Pem(certificates: String)

/** The certificate a client presents to a broker that asks for one, with its private key.
  *
  * The key must be PKCS#8, which PEM marks as `PRIVATE KEY`, or `ENCRYPTED PRIVATE KEY` when it takes the password. The Java client reads no other
  * form.
  */
enum ClientCertificate derives CanEqual:
  /** One PEM file holding the private key and the certificate chain. */
  case PemFile(path: String, keyPassword: Option[String])

  /** The certificate chain and the private key, PEM encoded and held in memory. */
  case Pem(certificateChain: String, privateKey: String, keyPassword: Option[String])

  def keyPassword: Option[String]

  override def toString: String =
    this match
      case PemFile(path, password) => s"PemFile($path,${password.map(_ => "<redacted>")})"
      case Pem(chain, _, password) => s"Pem(${chain.length} characters,<redacted>,${password.map(_ => "<redacted>")})"

/** SASL mechanisms every backend implements with the same name. */
enum SaslMechanism(val name: String) derives CanEqual:
  case Plain       extends SaslMechanism("PLAIN")
  case ScramSha256 extends SaslMechanism("SCRAM-SHA-256")
  case ScramSha512 extends SaslMechanism("SCRAM-SHA-512")

sealed abstract case class TlsSettings private (
    certificateAuthority: CertificateAuthority,
    verifyHostname: Boolean,
    clientCertificate: Option[ClientCertificate]
):
  def withCertificateAuthority(value: CertificateAuthority): ValidatedNel[SettingsError, TlsSettings] =
    TlsSettings.from(value, verifyHostname, clientCertificate)

  /** Checks that the broker's certificate names the host the client connected to, which is the default. */
  def withHostnameVerification: TlsSettings = new TlsSettings(certificateAuthority, true, clientCertificate) {}

  /** Accepts a broker certificate that is otherwise trusted, whatever host it names. */
  def withoutHostnameVerification: TlsSettings = new TlsSettings(certificateAuthority, false, clientCertificate) {}

  /** Presents this certificate to a broker that asks the client for one. */
  def withClientCertificate(value: ClientCertificate): ValidatedNel[SettingsError, TlsSettings] =
    TlsSettings.from(certificateAuthority, verifyHostname, Some(value))

  /** Presents no certificate, which is the default. */
  def withoutClientCertificate: TlsSettings = new TlsSettings(certificateAuthority, verifyHostname, None) {}

object TlsSettings:
  def from(
      certificateAuthority: CertificateAuthority = CertificateAuthority.SystemDefault,
      verifyHostname: Boolean = true,
      clientCertificate: Option[ClientCertificate] = None
  ): ValidatedNel[SettingsError, TlsSettings] =
    val blank =
      certificateAuthority match
        case CertificateAuthority.SystemDefault     => false
        case CertificateAuthority.PemFile(path)     => path.trim.isEmpty
        case CertificateAuthority.Pem(certificates) => certificates.trim.isEmpty
    val errors = Option.when(blank)(SettingsError.BlankCertificateAuthority).toList ++ clientCertificate.toList.flatMap(certificateErrors)
    validateSettings(errors).map(_ => new TlsSettings(certificateAuthority, verifyHostname, clientCertificate) {})

  private val Pkcs8Headers = List("-----BEGIN PRIVATE KEY-----", "-----BEGIN ENCRYPTED PRIVATE KEY-----")

  private def certificateErrors(certificate: ClientCertificate): List[SettingsError] =
    val parts =
      certificate match
        case ClientCertificate.PemFile(path, _)   => Option.when(path.trim.isEmpty)(SettingsError.BlankClientCertificate).toList
        case ClientCertificate.Pem(chain, key, _) =>
          if chain.trim.isEmpty || key.trim.isEmpty then List(SettingsError.BlankClientCertificate)
          else Option.when(!Pkcs8Headers.exists(key.contains))(SettingsError.PrivateKeyNotPkcs8).toList
    parts ++ Option.when(certificate.keyPassword.exists(_.isEmpty))(SettingsError.BlankKeyPassword)

sealed abstract case class SaslSettings private (mechanism: SaslMechanism, username: String, password: String):
  def withMechanism(value: SaslMechanism): SaslSettings = new SaslSettings(value, username, password) {}

  def withCredentials(user: String, secret: String): ValidatedNel[SettingsError, SaslSettings] = SaslSettings.from(mechanism, user, secret)

  override def toString: String = s"SaslSettings($mechanism,$username,<redacted>)"

object SaslSettings:
  def from(mechanism: SaslMechanism, username: String, password: String): ValidatedNel[SettingsError, SaslSettings] =
    val errors =
      Option.when(username.trim.isEmpty)(SettingsError.BlankSaslUsername).toList ++ Option.when(password.isEmpty)(SettingsError.BlankSaslPassword)
        .toList
    validateSettings(errors).map(_ => new SaslSettings(mechanism, username, password) {})

/** How a client authenticates and encrypts its broker connections.
  *
  * The case selects the protocol, so a client cannot ask for SASL without supplying credentials.
  */
enum SecuritySettings derives CanEqual:
  case Plaintext
  case Tls(tls: TlsSettings)
  case SaslPlaintext(sasl: SaslSettings)
  case SaslTls(tls: TlsSettings, sasl: SaslSettings)

  def tlsSettings: Option[TlsSettings] =
    this match
      case Plaintext | SaslPlaintext(_) => None
      case Tls(tls)                     => Some(tls)
      case SaslTls(tls, _)              => Some(tls)

  def saslSettings: Option[SaslSettings] =
    this match
      case Plaintext | Tls(_)  => None
      case SaslPlaintext(sasl) => Some(sasl)
      case SaslTls(_, sasl)    => Some(sasl)
