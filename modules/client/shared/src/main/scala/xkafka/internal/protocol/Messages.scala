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
package internal.protocol

import cats.syntax.all.*
import scodec.bits.ByteVector
import Wire.*

/** A request the group connection makes, with the versions this codec writes it in.
  *
  * Each request is written the same way in every version listed for it, which is what lets the connection take the highest one the broker also reads.
  */
private[xkafka] enum Api(val key: Short, val lowest: Short, val highest: Short, val flexible: Boolean) derives CanEqual:
  case FindCoordinator  extends Api(10, 4, 6, true)
  case SaslHandshake    extends Api(17, 1, 1, false)
  case ApiVersions      extends Api(18, 3, 4, true)
  case SaslAuthenticate extends Api(36, 2, 2, true)

private[xkafka] object Api:
  /** The highest version of each request that both this codec and the broker read. */
  def negotiate(broker: List[ApiVersion]): Result[Map[Api, Short]] =
    Api.values.toList
      .traverse(api => chosen(api, broker).toRight(s"the broker reads no version of $api from ${api.lowest} to ${api.highest}").map(api -> _))
      .map(_.toMap)

  private def chosen(api: Api, broker: List[ApiVersion]): Option[Short] =
    broker.find(_.key == api.key).flatMap: offered =>
      val version = math.min(offered.max, api.highest).toShort
      Option.when(version >= offered.min && version >= api.lowest)(version)

private[xkafka] final case class ApiVersion(key: Short, min: Short, max: Short)

/** A request and how to read the broker's answer to it. */
private[xkafka] sealed trait Request[R]:
  def api: Api
  def body: ByteVector
  def response: Read[R]

private[xkafka] final case class ApiVersionsRequest(clientSoftwareName: String, clientSoftwareVersion: String) extends Request[ApiVersionsResponse]:
  val api: Api                            = Api.ApiVersions
  def body: ByteVector                    = compactString(clientSoftwareName) ++ compactString(clientSoftwareVersion) ++ noTags
  def response: Read[ApiVersionsResponse] =
    // A broker that does not read the version asked for answers in version 0, which says so and lists what it reads.
    readInt16.flatMap: errorCode =>
      if errorCode == UnsupportedVersion then
        readArray((readInt16, readInt16, readInt16).mapN(ApiVersion.apply)).map(ApiVersionsResponse(errorCode, _))
      else
        (readCompactArray((readInt16, readInt16, readInt16).mapN(ApiVersion.apply) <* skipTags) <* readInt32 <* skipTags)
          .map(ApiVersionsResponse(errorCode, _))

  private val UnsupportedVersion: Short = 35

private[xkafka] final case class ApiVersionsResponse(errorCode: Short, apis: List[ApiVersion])

private[xkafka] final case class FindCoordinatorRequest(keyType: Byte, keys: List[String]) extends Request[List[Coordinator]]:
  val api: Api                          = Api.FindCoordinator
  def body: ByteVector                  = int8(keyType.toInt) ++ compactArray(keys)(compactString) ++ noTags
  def response: Read[List[Coordinator]] =
    val coordinator =
      (readCompactString, readInt32, readCompactString, readInt32, readInt16, readCompactNullableString).mapN(Coordinator.apply) <* skipTags
    readInt32 *> readCompactArray(coordinator) <* skipTags

private[xkafka] object FindCoordinatorRequest:
  val GroupKey: Byte = 0

private[xkafka] final case class Coordinator(key: String, nodeId: Int, host: String, port: Int, errorCode: Short, errorMessage: Option[String])

private[xkafka] final case class SaslHandshakeRequest(mechanism: String) extends Request[SaslHandshakeResponse]:
  val api: Api                              = Api.SaslHandshake
  def body: ByteVector                      = string(mechanism)
  def response: Read[SaslHandshakeResponse] = (readInt16, readArray(readString)).mapN(SaslHandshakeResponse.apply)

private[xkafka] final case class SaslHandshakeResponse(errorCode: Short, mechanisms: List[String])

private[xkafka] final case class SaslAuthenticateRequest(authBytes: ByteVector) extends Request[SaslAuthenticateResponse]:
  val api: Api                                 = Api.SaslAuthenticate
  def body: ByteVector                         = compactBytes(authBytes) ++ noTags
  def response: Read[SaslAuthenticateResponse] =
    (readInt16, readCompactNullableString, readCompactBytes, readInt64).mapN(SaslAuthenticateResponse.apply) <* skipTags

private[xkafka] final case class SaslAuthenticateResponse(
    errorCode: Short,
    errorMessage: Option[String],
    authBytes: ByteVector,
    sessionLifetimeMs: Long
)

/** Requests as they go on the wire, and the answers as they come back. */
private[xkafka] object Frame:
  /** The request with its header, after its size. */
  def request(request: Request[?], version: Short, correlationId: Int, clientId: String): ByteVector =
    val header  = int16(request.api.key.toInt) ++ int16(version.toInt) ++ int32(correlationId) ++ nullableString(Some(clientId))
    val message = header ++ (if request.api.flexible then noTags else ByteVector.empty) ++ request.body
    int32(message.size.toInt) ++ message

  /** Reads the answer to a request from what followed its size. */
  def response[R](request: Request[R], correlationId: Int)(bytes: ByteVector): Result[R] =
    // The answer to ApiVersions keeps the first header, so that a broker can answer a version it does not read.
    val tags   = if request.api.flexible && request.api != Api.ApiVersions then skipTags else ().pure[Read]
    val header =
      readInt32.flatMap(found => if found == correlationId then tags else fail(s"an answer to request $found came for request $correlationId"))
    readAll(header *> request.response)(bytes)
