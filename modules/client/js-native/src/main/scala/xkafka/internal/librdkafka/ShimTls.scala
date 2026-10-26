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
package internal
package librdkafka

/** The TLS settings as the shim's socket takes them, which are the ones librdkafka is given. */
private[xkafka] final case class ShimTls(
    caLocation: Option[String],
    caPem: Option[String],
    certificateLocation: Option[String],
    keyLocation: Option[String],
    certificatePem: Option[String],
    keyPem: Option[String],
    keyPassword: Option[String],
    verifyHostname: Boolean
)

private[xkafka] object ShimTls:
  def apply(settings: Option[TlsSettings]): ShimTls =
    settings.fold(ShimTls(None, None, None, None, None, None, None, false)): tls =>
      val (caLocation, caPem) =
        tls.certificateAuthority match
          case CertificateAuthority.SystemDefault     => (None, None)
          case CertificateAuthority.PemFile(path)     => (Some(path), None)
          case CertificateAuthority.Pem(certificates) => (None, Some(certificates))
      // A file holding both the key and the chain is named for each, as librdkafka is given it.
      tls.clientCertificate match
        case None                                            => ShimTls(caLocation, caPem, None, None, None, None, None, tls.verifyHostname)
        case Some(ClientCertificate.PemFile(path, password)) =>
          ShimTls(caLocation, caPem, Some(path), Some(path), None, None, password, tls.verifyHostname)
        case Some(ClientCertificate.Pem(chain, key, password)) =>
          ShimTls(caLocation, caPem, None, None, Some(chain), Some(key), password, tls.verifyHostname)
