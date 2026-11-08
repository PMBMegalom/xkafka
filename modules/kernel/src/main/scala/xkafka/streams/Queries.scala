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
package streams

import cats.MonadThrow
import cats.arrow.FunctionK
import cats.syntax.all.*
import fs2.Chunk

/** Where a member answers the queries other members of the application forward to it, as Java's `application.server` names it. */
final case class Endpoint private (host: String, port: Int)

object Endpoint:
  /** The endpoint at `host` and `port`, refused for an empty host or a port outside 1 to 65535. */
  def from(host: String, port: Int): Either[StreamsSettingsError, Endpoint] =
    Either.cond(host.trim.nonEmpty && port >= 1 && port <= 65535, new Endpoint(host, port), StreamsSettingsError.InvalidEndpoint(host, port))

/** Sends a query to the member at an endpoint and gives back its answer, through whatever the application uses to reach its members; the member
  * answers it with [[KafkaStreams.answer]].
  */
type Forward[F[_]] = (Endpoint, Chunk[Byte]) => F[Chunk[Byte]]

object Forward:
  /** No way to reach other members, so a query only one of them can answer fails as [[QueryProblem.Unreachable]]. */
  def none[F[_]](using F: MonadThrow[F]): Forward[F] =
    (to, _) => F.raiseError(new UnsupportedOperationException(s"no way to forward a query to $to was given"))

/** Why a query could not be answered. */
enum QueryProblem derives CanEqual:
  /** The part of the store the query needs is moving between members or being filled again, so asking again shortly may succeed. */
  case Rebalancing

  /** The member holding what the query needs could not be asked, or failed to answer. */
  case Unreachable(member: Endpoint, reason: String)

  /** The member holding what the query needs answered in a way xkafka does not read, as a Java Kafka Streams member would. */
  case NotXkafka(member: Endpoint)

  /** The program has no key-value store of this name to query. */
  case UnknownStore(store: String)

/** A question for one store of the application, whose answer is an `A`, as [[QueryableStore]] makes one. */
final class Query[F[_], A] private[xkafka] (
    private[xkafka] val store: Store,
    private[xkafka] val request: xkafka.Topic => F[QueryRequest],
    private[xkafka] val answer: (xkafka.Topic, Vector[(Chunk[Byte], Chunk[Byte])]) => F[A]
):
  private[xkafka] def mapK[G[_]](fk: FunctionK[F, G]): Query[G, A] =
    new Query(store, topic => fk(request(topic)), (topic, entries) => fk(answer(topic, entries)))

/** What a query asks of a store, by serialized keys. */
private[xkafka] enum QueryRequest derives CanEqual:
  /** What `key` holds. */
  case Get(store: String, key: Chunk[Byte])

  /** The keys from `from` to `to`, both included, a bound not given leaving that side open, in order. */
  case Range(store: String, from: Option[Chunk[Byte]], to: Option[Chunk[Byte]])

  def storeName: String =
    this match
      case Get(store, _)      => store
      case Range(store, _, _) => store

/** The stores of a running application that can be queried, wherever the key's partition is held. */
object QueryableStore:
  /** The key-value store `store`, which a table, an aggregation or a processor keeps, read with `key` and `value`. */
  def keyValue[F[_]: MonadThrow, K, V](store: Store, key: Serde[F, K], value: Serde[F, V]): KeyValueQueries[F, K, V] =
    new KeyValueQueries(store, key, value)

/** The queries of a key-value store. */
final class KeyValueQueries[F[_], K, V] private[streams] (store: Store, key: Serde[F, K], value: Serde[F, V])(using F: MonadThrow[F]):
  private def bytes(topic: xkafka.Topic, of: K): F[Chunk[Byte]] =
    key.serializer.serialize(topic, xkafka.Headers.empty, of)
      .flatMap(F.fromOption(_, new IllegalArgumentException(s"the key's serializer wrote no bytes to query ${store.value} by")))

  private def entries(topic: xkafka.Topic, found: Vector[(Chunk[Byte], Chunk[Byte])]): F[Vector[(K, V)]] =
    found.traverse: (k, v) =>
      (key.deserializer.deserialize(topic, xkafka.Headers.empty, Some(k)), value.deserializer.deserialize(topic, xkafka.Headers.empty, Some(v)))
        .tupled

  /** What the store holds for `of`, if anything. */
  def get(of: K): Query[F, Option[V]] =
    new Query(
      store,
      topic => bytes(topic, of).map(QueryRequest.Get(store.value, _)),
      (topic, found) => found.headOption.traverse((_, v) => value.deserializer.deserialize(topic, xkafka.Headers.empty, Some(v)))
    )

  /** The keys from `from` to `to`, both included, and what each holds, in the order of their serialized bytes, from every member. */
  def range(from: K, to: K): Query[F, Vector[(K, V)]] =
    new Query(
      store,
      topic => (bytes(topic, from), bytes(topic, to)).mapN((first, last) => QueryRequest.Range(store.value, Some(first), Some(last))),
      entries
    )

  /** Every key and what it holds, in the order of their serialized bytes, from every member. */
  def all: Query[F, Vector[(K, V)]] = new Query(store, _ => F.pure(QueryRequest.Range(store.value, None, None)), entries)
