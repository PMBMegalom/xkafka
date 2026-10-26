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

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters.*

import munit.FunSuite
import org.apache.kafka.common.message.*
import org.apache.kafka.common.protocol.{ApiKeys, Message, MessageUtil}
import org.apache.kafka.common.protocol.types.RawTaggedField
import scodec.bits.ByteVector

/** The group connection's requests and answers, compared with the bytes the Java client writes for each version this codec writes. */
final class ProtocolGoldenSuite extends FunSuite:
  private val CorrelationId = 7
  private val ClientId      = "xkafka-golden"

  private def bytes(message: Message, version: Short): ByteVector = ByteVector(MessageUtil.toByteBufferAccessor(message, version).buffer())

  private def framed(header: ByteVector, body: ByteVector): ByteVector =
    ByteVector(ByteBuffer.allocate(4).putInt((header ++ body).size.toInt).array()) ++ header ++ body

  private def javaRequest(key: ApiKeys, version: Short, body: Message): ByteVector =
    val header = new RequestHeaderData().setRequestApiKey(key.id).setRequestApiVersion(version).setCorrelationId(CorrelationId).setClientId(ClientId)
    framed(bytes(header, key.requestHeaderVersion(version)), bytes(body, version))

  /** An answer as the broker sends it, with a tagged field this codec does not know in every flexible structure that has room for one. */
  private def javaResponse(key: ApiKeys, version: Short, body: Message): ByteVector =
    val headerVersion = key.responseHeaderVersion(version)
    val header        = new ResponseHeaderData().setCorrelationId(CorrelationId)
    bytes(if headerVersion >= 1 then tagged(header) else header, headerVersion) ++ bytes(body, version)

  private def tagged[M <: Message](message: M): M =
    val _ = message.unknownTaggedFields().add(new RawTaggedField(90, Array[Byte](1, 2, 3)))
    message

  private def versions(api: Api): List[Short] = (api.lowest.toInt to api.highest.toInt).map(_.toShort).toList

  test("every version this codec writes is one the Java client writes, under the same key"):
    List(
      Api.FindCoordinator  -> ApiKeys.FIND_COORDINATOR,
      Api.SaslHandshake    -> ApiKeys.SASL_HANDSHAKE,
      Api.ApiVersions      -> ApiKeys.API_VERSIONS,
      Api.SaslAuthenticate -> ApiKeys.SASL_AUTHENTICATE
    ).foreach: (api, key) =>
      assertEquals(api.key, key.id)
      versions(api).foreach: version =>
        assert(key.isVersionSupported(version), s"$api $version")
        assertEquals(api.flexible, key.requestHeaderVersion(version) == 2.toShort, s"$api $version")

  test("ApiVersions"):
    versions(Api.ApiVersions).foreach: version =>
      val request = ApiVersionsRequest("xkafka", "0.6.0")
      assertEquals(
        Frame.request(request, version, CorrelationId, ClientId),
        javaRequest(ApiKeys.API_VERSIONS, version, new ApiVersionsRequestData().setClientSoftwareName("xkafka").setClientSoftwareVersion("0.6.0"))
      )
      val keys = new ApiVersionsResponseData.ApiVersionCollection()
      keys.add(new ApiVersionsResponseData.ApiVersion().setApiKey(10).setMinVersion(0).setMaxVersion(6))
      keys.add(new ApiVersionsResponseData.ApiVersion().setApiKey(18).setMinVersion(0).setMaxVersion(5))
      keys.iterator().asScala.foreach(tagged)
      val features = new ApiVersionsResponseData.SupportedFeatureKeyCollection()
      features.add(new ApiVersionsResponseData.SupportedFeatureKey().setName("metadata.version").setMinVersion(1).setMaxVersion(27))
      val body =
        tagged(
          new ApiVersionsResponseData().setErrorCode(0).setApiKeys(keys).setThrottleTimeMs(5).setSupportedFeatures(features)
            .setFinalizedFeaturesEpoch(3)
        )
      assertEquals(
        Frame.response(request, CorrelationId)(javaResponse(ApiKeys.API_VERSIONS, version, body)),
        Right(ApiVersionsResponse(0, List(ApiVersion(10, 0, 6), ApiVersion(18, 0, 5))))
      )

  test("a broker that does not read the ApiVersions version asked for answers in version 0, listing what it reads"):
    val keys = new ApiVersionsResponseData.ApiVersionCollection()
    keys.add(new ApiVersionsResponseData.ApiVersion().setApiKey(18).setMinVersion(0).setMaxVersion(2))
    val body = new ApiVersionsResponseData().setErrorCode(35).setApiKeys(keys)
    assertEquals(
      Frame.response(ApiVersionsRequest("xkafka", "0.6.0"), CorrelationId)(javaResponse(ApiKeys.API_VERSIONS, 0, body)),
      Right(ApiVersionsResponse(35, List(ApiVersion(18, 0, 2))))
    )

  test("FindCoordinator"):
    versions(Api.FindCoordinator).foreach: version =>
      val request = FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List("app", "other"))
      assertEquals(
        Frame.request(request, version, CorrelationId, ClientId),
        javaRequest(ApiKeys.FIND_COORDINATOR, version, new FindCoordinatorRequestData().setKeyType(0).setCoordinatorKeys(List("app", "other").asJava))
      )
      val found =
        tagged(
          new FindCoordinatorResponseData.Coordinator().setKey("app").setNodeId(1).setHost("broker").setPort(9092).setErrorCode(0)
            .setErrorMessage(null)
        )
      val missing =
        new FindCoordinatorResponseData.Coordinator().setKey("other").setNodeId(-1).setHost("").setPort(-1).setErrorCode(15)
          .setErrorMessage("not available")
      val body = tagged(new FindCoordinatorResponseData().setThrottleTimeMs(0).setCoordinators(List(found, missing).asJava))
      assertEquals(
        Frame.response(request, CorrelationId)(javaResponse(ApiKeys.FIND_COORDINATOR, version, body)),
        Right(List(Coordinator("app", 1, "broker", 9092, 0, None), Coordinator("other", -1, "", -1, 15, Some("not available"))))
      )

  test("SaslHandshake"):
    versions(Api.SaslHandshake).foreach: version =>
      val request = SaslHandshakeRequest("SCRAM-SHA-512")
      assertEquals(
        Frame.request(request, version, CorrelationId, ClientId),
        javaRequest(ApiKeys.SASL_HANDSHAKE, version, new SaslHandshakeRequestData().setMechanism("SCRAM-SHA-512"))
      )
      val body = new SaslHandshakeResponseData().setErrorCode(33).setMechanisms(List("PLAIN", "SCRAM-SHA-256").asJava)
      assertEquals(
        Frame.response(request, CorrelationId)(javaResponse(ApiKeys.SASL_HANDSHAKE, version, body)),
        Right(SaslHandshakeResponse(33, List("PLAIN", "SCRAM-SHA-256")))
      )

  test("SaslAuthenticate"):
    versions(Api.SaslAuthenticate).foreach: version =>
      val token   = ByteVector("n,,n=user,r=nonce".getBytes("UTF-8"))
      val request = SaslAuthenticateRequest(token)
      assertEquals(
        Frame.request(request, version, CorrelationId, ClientId),
        javaRequest(ApiKeys.SASL_AUTHENTICATE, version, new SaslAuthenticateRequestData().setAuthBytes(token.toArray))
      )
      val body =
        tagged(
          new SaslAuthenticateResponseData().setErrorCode(58).setErrorMessage("wrong").setAuthBytes(Array[Byte](9, 8)).setSessionLifetimeMs(1234L)
        )
      assertEquals(
        Frame.response(request, CorrelationId)(javaResponse(ApiKeys.SASL_AUTHENTICATE, version, body)),
        Right(SaslAuthenticateResponse(58, Some("wrong"), ByteVector(9, 8), 1234L))
      )
