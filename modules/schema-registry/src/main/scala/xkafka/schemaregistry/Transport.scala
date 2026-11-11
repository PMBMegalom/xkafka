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

package xkafka.schemaregistry

import fs2.Chunk

/** The HTTP methods the registry's API uses. */
enum Method derives CanEqual:
  case Get, Post, Put, Delete

/** One request to the registry: an absolute URL, the headers to send, and a JSON body where the request has one. */
final case class RegistryRequest(method: Method, url: String, headers: List[(String, String)], body: Option[Chunk[Byte]])

/** The registry's answer: its status code and its body. */
final case class RegistryResponse(status: Int, body: Chunk[Byte])

/** Sends a request to the registry through whatever HTTP client the application uses, which also decides on TLS, authentication, proxies and
  * timeouts. A failure to reach the registry is raised in `F`; any answer it gives, an error status included, is a response.
  */
type Transport[F[_]] = RegistryRequest => F[RegistryResponse]
