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

  private val keys: List[(Api, ApiKeys)] =
    List(
      Api.ApiVersions      -> ApiKeys.API_VERSIONS,
      Api.FindCoordinator  -> ApiKeys.FIND_COORDINATOR,
      Api.SaslHandshake    -> ApiKeys.SASL_HANDSHAKE,
      Api.SaslAuthenticate -> ApiKeys.SASL_AUTHENTICATE
    )

  /** How each version this codec does not write compares with the versions it does: whether its request, and then its answer, has the same bytes. */
  private val outside: Map[(Api, Int), (Boolean, Boolean)] =
    Map(
      // Versions 0 to 2 have no body and answer without tagged fields.
      (Api.ApiVersions, 0) -> (false, false),
      (Api.ApiVersions, 1) -> (false, false),
      (Api.ApiVersions, 2) -> (false, false),
      // Version 5 asks the broker to check the cluster and node it is (KIP-1242); its answer is laid out as version 4's.
      (Api.ApiVersions, 5) -> (false, true),
      // Versions 0 to 3 find one coordinator at a time.
      (Api.FindCoordinator, 0) -> (false, false),
      (Api.FindCoordinator, 1) -> (false, false),
      (Api.FindCoordinator, 2) -> (false, false),
      (Api.FindCoordinator, 3) -> (false, false),
      // Version 0 has version 1's bytes, but after it the client sends bare SASL tokens instead of SaslAuthenticate requests.
      (Api.SaslHandshake, 0) -> (true, true),
      // Versions 0 and 1 are not flexible, and version 0 answers without the session lifetime.
      (Api.SaslAuthenticate, 0) -> (false, false),
      (Api.SaslAuthenticate, 1) -> (false, false)
    )

  private def every(key: ApiKeys): List[Short] = (key.oldestVersion.toInt to key.latestVersion(true).toInt).map(_.toShort).toList

  test("every version the Java client defines is one this codec writes or one whose difference is recorded here"):
    keys.foreach: (api, key) =>
      every(key).foreach: version =>
        val written = version >= api.lowest && version <= api.highest
        assert(written != outside.contains((api, version.toInt)), s"$api $version: written by the codec, or its difference recorded, but not both")
    outside.keys.foreach: (api, version) =>
      assert(
        keys.toMap.get(api).exists(key => every(key).contains(version.toShort)),
        s"$api $version is recorded but the Java client does not define it"
      )

  /** One request and answer, as this codec writes and reads them and as the Java client does in a given version. */
  private final case class Sample(request: Request[?], javaRequest: Message, javaAnswer: Message, read: Any)

  private def sample(api: Api, version: Short): Sample =
    api match
      case Api.ApiVersions =>
        val listed = new ApiVersionsResponseData.ApiVersionCollection()
        listed.add(new ApiVersionsResponseData.ApiVersion().setApiKey(18).setMinVersion(0).setMaxVersion(4))
        Sample(
          ApiVersionsRequest("xkafka", "0.6.0"),
          if version < 3 then new ApiVersionsRequestData()
          else new ApiVersionsRequestData().setClientSoftwareName("xkafka").setClientSoftwareVersion("0.6.0"),
          new ApiVersionsResponseData().setErrorCode(0).setApiKeys(listed),
          ApiVersionsResponse(0, List(ApiVersion(18, 0, 4)))
        )
      case Api.FindCoordinator => Sample(
          FindCoordinatorRequest(FindCoordinatorRequest.GroupKey, List("app")),
          if version < 4 then new FindCoordinatorRequestData().setKey("app")
          else new FindCoordinatorRequestData().setCoordinatorKeys(List("app").asJava),
          if version < 4 then new FindCoordinatorResponseData().setErrorCode(0).setErrorMessage(null).setNodeId(1).setHost("broker").setPort(9092)
          else
            new FindCoordinatorResponseData().setCoordinators(
              List(new FindCoordinatorResponseData.Coordinator().setKey("app").setNodeId(1).setHost("broker").setPort(9092).setErrorMessage(null))
                .asJava
            )
          ,
          List(Coordinator("app", 1, "broker", 9092, 0, None))
        )
      case Api.SaslHandshake => Sample(
          SaslHandshakeRequest("PLAIN"),
          new SaslHandshakeRequestData().setMechanism("PLAIN"),
          new SaslHandshakeResponseData().setErrorCode(0).setMechanisms(List("PLAIN").asJava),
          SaslHandshakeResponse(0, List("PLAIN"))
        )
      case Api.SaslAuthenticate =>
        val token = ByteVector("n,,n=user,r=nonce".getBytes("UTF-8"))
        Sample(
          SaslAuthenticateRequest(token),
          new SaslAuthenticateRequestData().setAuthBytes(token.toArray),
          new SaslAuthenticateResponseData().setErrorCode(0).setErrorMessage(null).setAuthBytes(Array[Byte](9, 8)),
          SaslAuthenticateResponse(0, None, ByteVector(9, 8), 0L)
        )

  test("each version this codec does not write differs from the ones it writes as recorded"):
    keys.foreach: (api, key) =>
      every(key).filterNot(version => version >= api.lowest && version <= api.highest).foreach: version =>
        val example                   = sample(api, version)
        val (sameRequest, sameAnswer) = outside((api, version.toInt))
        val written                   = Frame.request(example.request, version, CorrelationId, ClientId)
        assertEquals(written == javaRequest(key, version, example.javaRequest), sameRequest, s"$api $version request")
        val read = Frame.response(example.request, CorrelationId)(javaResponse(key, version, example.javaAnswer))
        assertEquals(read == Right(example.read), sameAnswer, s"$api $version answer: $read")

  test("the samples above are read and written as the Java client does in every version this codec writes"):
    keys.foreach: (api, key) =>
      versions(api).foreach: version =>
        val example = sample(api, version)
        assertEquals(
          Frame.request(example.request, version, CorrelationId, ClientId),
          javaRequest(key, version, example.javaRequest),
          s"$api $version"
        )
        assertEquals[Either[String, Any], Either[String, Any]](
          Frame.response(example.request, CorrelationId)(javaResponse(key, version, example.javaAnswer)),
          Right(example.read),
          s"$api $version"
        )
