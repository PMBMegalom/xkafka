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

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk
import org.http4s.{Header, Headers, Method as Http4sMethod, Request, Uri}
import org.http4s.client.Client
import org.typelevel.ci.CIString
import xkafka.schemaregistry.{Method, RegistryResponse, Transport}

/** Reaches the registry through an http4s client, which decides on TLS, authentication, proxies and timeouts as it is built and wrapped. */
object Http4sTransport:
  def apply[F[_]](client: Client[F])(using F: Concurrent[F]): Transport[F] =
    request =>
      F.fromEither(Uri.fromString(request.url)).flatMap: uri =>
        val headers = Headers(request.headers.map((name, value) => Header.Raw(CIString(name), value)))
        val sent    =
          request.body.fold(Request[F](method(request.method), uri))(body => Request[F](method(request.method), uri).withEntity(body.toArray))
        // The entity's own Content-Length stays, and the registry's Content-Type replaces the one the entity sets.
        client.run(sent.transformHeaders(_ ++ headers))
          .use(response => response.body.compile.to(Chunk).map(RegistryResponse(response.status.code, _)))

  private def method(method: Method): Http4sMethod =
    method match
      case Method.Get    => Http4sMethod.GET
      case Method.Post   => Http4sMethod.POST
      case Method.Put    => Http4sMethod.PUT
      case Method.Delete => Http4sMethod.DELETE
