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

import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll
import scodec.bits.*
import Wire.*

/** How the group connection's codec chooses versions and refuses answers it cannot read, on every platform. */
final class ProtocolSuite extends ScalaCheckSuite:
  private val everything = List(ApiVersion(10, 0, 6), ApiVersion(17, 0, 1), ApiVersion(18, 0, 4), ApiVersion(36, 0, 2))

  private def offering(versions: ApiVersion*): List[ApiVersion] =
    versions.foldLeft(everything)((offered, version) => offered.filterNot(_.key == version.key) :+ version)

  test("each request is sent in the highest version both sides read"):
    assertEquals(
      Api.negotiate(offering(ApiVersion(10, 0, 5), ApiVersion(18, 0, 9))),
      Right(Map(Api.FindCoordinator -> 5.toShort, Api.SaslHandshake -> 1.toShort, Api.ApiVersions -> 4.toShort, Api.SaslAuthenticate -> 2.toShort))
    )

  test("a broker that reads no version this codec writes is refused"):
    assert(Api.negotiate(everything.filterNot(_.key == 36)).isLeft, "SaslAuthenticate missing")
    assert(Api.negotiate(offering(ApiVersion(10, 0, 3))).isLeft, "FindCoordinator only below 4")
    assert(Api.negotiate(offering(ApiVersion(17, 2, 3))).isLeft, "SaslHandshake only above 1")

  property("an unsigned varint reads back as written"):
    forAll((value: Int) => readAll(readUnsignedVarint)(unsignedVarint(value)) == Right(value))

  test("an unsigned varint takes one byte for each seven bits"):
    assertEquals(unsignedVarint(127), hex"7f")
    assertEquals(unsignedVarint(128), hex"8001")
    assertEquals(unsignedVarint(-1), hex"ffffffff0f")

  test("an unsigned varint longer than five bytes is refused"):
    assert(readAll(readUnsignedVarint)(hex"ffffffff8f00").isLeft)

  test("an answer cut short is refused"):
    assertEquals(readAll(readInt32)(hex"000000"), Left("an int32 is cut short: 4 bytes wanted, 3 left"))

  test("an answer with bytes left over is refused"):
    assertEquals(readAll(readInt16)(hex"000102"), Left("1 bytes are left over"))

  test("a null where a value is required is refused"):
    assertEquals(readAll(readString)(hex"ffff"), Left("a string is null"))
    assertEquals(readAll(readCompactString)(hex"00"), Left("a string is null"))
    assertEquals(readAll(readCompactBytes)(hex"00"), Left("a byte string is null"))
    assertEquals(readAll(readArray(readInt8))(hex"ffffffff"), Left("an array is null"))
    assertEquals(readAll(readCompactArray(readInt8))(hex"00"), Left("an array is null"))

  test("a negative length that does not mean null is refused"):
    assertEquals(readAll(readNullableString)(hex"fffe"), Left("a string has a negative length"))

  test("nullable strings read a null as nothing"):
    assertEquals(readAll(readNullableString)(hex"ffff"), Right(None))
    assertEquals(readAll(readCompactNullableString)(hex"00"), Right(None))
    assertEquals(readAll(readNullableString)(nullableString(Some("é"))), Right(Some("é")))

  test("a string that is not UTF-8 is refused"):
    assert(readAll(readCompactString)(hex"02ff").isLeft)

  test("a count of more items than bytes left is refused before any is read"):
    assertEquals(readAll(readArray(readInt8))(hex"7fffffff00"), Left("2147483647 array items cannot fit in 1 bytes"))
    assertEquals(readAll(readCompactArray(readInt8))(unsignedVarint(Int.MaxValue)), Left("2147483646 array items cannot fit in 0 bytes"))
    assertEquals(readAll(skipTags)(unsignedVarint(-1)), Left("4294967295 tagged fields cannot fit in 0 bytes"))

  test("an answer to another request is refused"):
    val request = SaslHandshakeRequest("PLAIN")
    assertEquals(Frame.response(request, 3)(int32(4) ++ int16(0) ++ array(List("PLAIN"))(string)), Left("an answer to request 4 came for request 3"))
    assertEquals(Frame.response(request, 3)(int32(3) ++ int16(0) ++ array(List("PLAIN"))(string)), Right(SaslHandshakeResponse(0, List("PLAIN"))))
