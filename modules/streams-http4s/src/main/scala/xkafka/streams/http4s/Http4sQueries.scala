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

package xkafka.streams.http4s

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk
import org.http4s.{HttpRoutes, Method, Request, Response, Status, Uri}
import org.http4s.client.Client
import xkafka.streams.{Endpoint, Forward, KafkaStreams}

/** Queries between the members of a streams application over HTTP, with http4s: each member posts the queries it forwards to the member that holds
  * what they need, and serves a route that answers the queries posted to it.
  */
object Http4sQueries:
  /** The path queries are posted to, unless another is given. */
  val defaultPath: Uri.Path = Uri.Path.unsafeFromString("/xkafka/query")

  /** The URI of `path` at `member`'s endpoint. */
  def uri(member: Endpoint, path: Uri.Path = defaultPath, scheme: Uri.Scheme = Uri.Scheme.http): Uri =
    val host = Uri.Host.fromString(member.host).getOrElse(Uri.RegName(member.host))
    Uri(scheme = Some(scheme), authority = Some(Uri.Authority(host = host, port = Some(member.port))), path = path)

  /** Posts each query to `path` at the endpoint of the member that holds what it needs, through `client`, which decides on TLS, authentication and
    * timeouts; use `scheme` `https` to reach members over TLS. An answer that is not a success fails the query with `QueryProblem.Unreachable`.
    */
  def forward[F[_]: Concurrent](client: Client[F], path: Uri.Path = defaultPath, scheme: Uri.Scheme = Uri.Scheme.http): Forward[F] =
    (member, query) => client.expect[Array[Byte]](Request[F](Method.POST, uri(member, path, scheme)).withEntity(query.toArray)).map(Chunk.array(_))

  /** The route answering the queries other members post to `path`, from `streams`. Serve it at this member's endpoint, and keep it from the outside
    * world: it answers any query, of any store.
    */
  def routes[F[_]: Concurrent](streams: KafkaStreams[F], path: Uri.Path = defaultPath): HttpRoutes[F] =
    HttpRoutes.of[F]:
      case request if request.method == Method.POST && request.pathInfo == path =>
        request.as[Array[Byte]].flatMap(query => streams.answer(Chunk.array(query))).map(answer => Response[F](Status.Ok).withEntity(answer.toArray))
