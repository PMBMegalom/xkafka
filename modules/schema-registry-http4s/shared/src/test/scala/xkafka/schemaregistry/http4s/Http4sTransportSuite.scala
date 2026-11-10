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

package xkafka.schemaregistry.http4s

import cats.effect.{IO, Ref}
import fs2.Chunk
import munit.CatsEffectSuite
import org.http4s.{HttpApp, Request, Response, Status}
import org.http4s.client.Client
import org.typelevel.ci.CIString
import xkafka.schemaregistry.{Method, RegistryRequest, RegistryResponse}

class Http4sTransportSuite extends CatsEffectSuite:
  /** Answers every request with `status` and `body`, keeping the requests with their bodies. */
  private def answering(status: Status, body: String): IO[(Ref[IO, Vector[(Request[IO], Chunk[Byte])]], Client[IO])] =
    Ref.of[IO, Vector[(Request[IO], Chunk[Byte])]](Vector.empty).map: seen =>
      val app =
        HttpApp[IO](request =>
          request.body.compile.to(Chunk).flatMap(read => seen.update(_ :+ (request, read))).as(Response[IO](status).withEntity(body))
        )
      (seen, Client.fromHttpApp(app))

  test("a request is sent with its method, URL, headers and body, and the answer comes back with its status and body"):
    val sent =
      RegistryRequest(
        Method.Post,
        "http://registry:8081/subjects/orders-value/versions",
        List("Content-Type" -> "application/vnd.schemaregistry.v1+json", "Accept" -> "application/json"),
        Some(Chunk.array("""{"schema":"\"string\""}""".getBytes))
      )
    for
      prepared <- answering(Status.Conflict, """{"error_code":409,"message":"incompatible"}""")
      (seen, client) = prepared
      answer   <- Http4sTransport(client)(sent)
      requests <- seen.get
    yield
      val (request, body) = requests.head
      assertEquals(answer.status, 409)
      assertEquals(new String(answer.body.toArray), """{"error_code":409,"message":"incompatible"}""")
      assertEquals(request.method, org.http4s.Method.POST)
      assertEquals(request.uri.renderString, "http://registry:8081/subjects/orders-value/versions")
      assertEquals(request.headers.get(CIString("Content-Type")).map(_.map(_.value).toList), Some(List("application/vnd.schemaregistry.v1+json")))
      assertEquals(request.headers.get(CIString("Accept")).map(_.head.value), Some("application/json"))
      assertEquals(request.headers.get(CIString("Content-Length")).map(_.head.value), Some(body.size.toString))
      assertEquals(new String(body.toArray), """{"schema":"\"string\""}""")

  test("a request without a body is sent without one"):
    for
      prepared <- answering(Status.Ok, """{"schema":"\"string\""}""")
      (seen, client) = prepared
      answer   <- Http4sTransport(client)(RegistryRequest(Method.Get, "http://registry:8081/schemas/ids/1", Nil, None))
      requests <- seen.get
    yield
      assertEquals(answer, RegistryResponse(200, Chunk.array("""{"schema":"\"string\""}""".getBytes)))
      assertEquals(requests.head._1.method, org.http4s.Method.GET)
      assertEquals(requests.head._2, Chunk.empty[Byte])

  test("a URL http4s cannot parse fails"):
    answering(Status.Ok, "")
      .flatMap((_, client) => Http4sTransport(client)(RegistryRequest(Method.Get, "http://registry:8081/a b", Nil, None)).attempt)
      .map(result => assert(result.isLeft, result.toString))
