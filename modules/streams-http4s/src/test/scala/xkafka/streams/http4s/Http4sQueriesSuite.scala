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

import cats.effect.{IO, Ref, Resource}
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.client.{Client, UnexpectedStatus}
import xkafka.streams.*

class Http4sQueriesSuite extends CatsEffectSuite:
  private val member = Endpoint.from("orders-3.internal", 8080).toOption.get

  /** A member whose every answer is the query it was asked, reversed. */
  private val streams =
    new KafkaStreams[IO]:
      override def run: IO[Unit]                                                                      = IO.never
      override def state: Stream[IO, StreamsState]                                                    = Stream.empty
      override def query[A](query: Query[IO, A], allowStandby: Boolean): IO[Either[QueryProblem, A]] = IO.pure(Left(QueryProblem.Rebalancing))
      override def answer(request: Chunk[Byte]): IO[Chunk[Byte]]                                     = IO.pure(Chunk.from(request.toList.reverse))

  /** A client that serves `routes` itself, noting each request it is given. */
  private def client(routes: org.http4s.HttpRoutes[IO], seen: Ref[IO, List[Request[IO]]]): Client[IO] =
    val app = routes.orNotFound
    Client(request => Resource.eval(seen.update(_ :+ request) *> app.run(request)))

  test("a forwarded query is posted to the member's endpoint, and the route there answers it from the member's streams"):
    for
      seen   <- Ref.of[IO, List[Request[IO]]](Nil)
      forward: Forward[IO] = Http4sQueries.forward(client(Http4sQueries.routes(streams), seen))
      answer <- forward(member, Chunk[Byte](1, 2, 3))
      asked  <- seen.get
    yield
      assertEquals(answer, Chunk[Byte](3, 2, 1))
      assertEquals(asked.map(request => (request.method, request.uri)), List((Method.POST, Uri.unsafeFromString("http://orders-3.internal:8080/xkafka/query"))))

  test("another path and scheme are posted to and answered at"):
    val path = Uri.Path.unsafeFromString("/internal/queries")
    for
      seen   <- Ref.of[IO, List[Request[IO]]](Nil)
      forward: Forward[IO] = Http4sQueries.forward(client(Http4sQueries.routes(streams, path), seen), path, Uri.Scheme.https)
      answer <- forward(member, Chunk[Byte](4, 5))
      asked  <- seen.get
    yield
      assertEquals(answer, Chunk[Byte](5, 4))
      assertEquals(asked.map(_.uri.renderString), List("https://orders-3.internal:8080/internal/queries"))

  test("an answer that is not a success fails the forwarded query, and the route answers nothing but posts to its path"):
    for
      seen    <- Ref.of[IO, List[Request[IO]]](Nil)
      other   = Http4sQueries.routes(streams, Uri.Path.unsafeFromString("/other"))
      forward: Forward[IO] = Http4sQueries.forward(client(other, seen))
      failed  <- forward(member, Chunk[Byte](1)).attempt
      fetched <- Http4sQueries.routes(streams).orNotFound.run(Request[IO](Method.GET, Uri.unsafeFromString("/xkafka/query")))
    yield
      failed match
        case Left(error: UnexpectedStatus) => assertEquals(error.status, Status.NotFound)
        case other                         => fail(s"expected the query to fail, got $other")
      assertEquals(fetched.status, Status.NotFound)
