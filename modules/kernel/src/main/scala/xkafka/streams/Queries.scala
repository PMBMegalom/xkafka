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

  /** The program has no store of this name of the kind queried: key-value, window or session. */
  case UnknownStore(store: String)

/** A question for one store of the application, whose answer is an `A`, as [[QueryableStore]] makes one. */
final class Query[F[_], A] private[xkafka] (
    private[xkafka] val store: Store,
    private[xkafka] val request: xkafka.Topic => F[QueryRequest],
    private[xkafka] val answer: (Answering, Vector[(Chunk[Byte], Chunk[Byte])]) => F[A]
):
  private[xkafka] def mapK[G[_]](fk: FunctionK[F, G]): Query[G, A] =
    new Query(store, topic => fk(request(topic)), (answering, entries) => fk(answer(answering, entries)))

/** What reading a query's answer needs from the program: the topic its store's keys and values are written for, and, for a window store, the size of
  * its windows, which give each window its end.
  */
private[xkafka] final case class Answering(topic: xkafka.Topic, windowSize: Option[Long])

/** What a query asks of a store, by serialized keys. */
private[xkafka] object QueryRequest:
  /** Bytes in the order a store keeps keys in: each taken as unsigned, a shorter key before a longer one it starts. */
  val unsigned: Ordering[Chunk[Byte]] =
    (a, b) =>
      val common = math.min(a.size, b.size)
      (0 until common).iterator.map(at => Integer.compare(a(at) & 0xff, b(at) & 0xff)).find(_ != 0).getOrElse(Integer.compare(a.size, b.size))

private[xkafka] enum QueryRequest derives CanEqual:
  /** What `key` holds. */
  case Get(store: String, key: Chunk[Byte])

  /** The keys from `from` to `to`, both included, a bound not given leaving that side open, in order. */
  case Range(store: String, from: Option[Chunk[Byte]], to: Option[Chunk[Byte]])

  /** The windows of a window store starting from `from` to `to`, both included: those of `key`, or of every key. */
  case Windows(store: String, key: Option[Chunk[Byte]], from: Long, to: Long)

  /** The sessions of `key` in a session store. */
  case Sessions(store: String, key: Chunk[Byte])

  def storeName: String =
    this match
      case Get(store, _)           => store
      case Range(store, _, _)      => store
      case Windows(store, _, _, _) => store
      case Sessions(store, _)      => store

/** The stores of a running application that can be queried, wherever the key's partition is held. */
object QueryableStore:
  /** The key-value store `store`, which a table, an aggregation or a processor keeps, read with `key` and `value`. */
  def keyValue[F[_]: MonadThrow, K, V](store: Store, key: Serde[F, K], value: Serde[F, V]): KeyValueQueries[F, K, V] =
    new KeyValueQueries(store, key, value)

  /** The window store `store`, which a windowed aggregation or a processor keeps, read with `key` and `value`. */
  def window[F[_]: MonadThrow, K, V](store: Store, key: Serde[F, K], value: Serde[F, V]): WindowQueries[F, K, V] =
    new WindowQueries(store, key, value)

  /** The session store `store`, which an aggregation by session windows or a processor keeps, read with `key` and `value`. */
  def session[F[_]: MonadThrow, K, V](store: Store, key: Serde[F, K], value: Serde[F, V]): SessionQueries[F, K, V] =
    new SessionQueries(store, key, value)

  /** The serialized key `of`, which a query needs bytes of. */
  private[streams] def bytesOf[F[_], K](store: Store, key: Serde[F, K], topic: xkafka.Topic, of: K)(using F: MonadThrow[F]): F[Chunk[Byte]] =
    key.serializer.serialize(topic, xkafka.Headers.empty, of)
      .flatMap(F.fromOption(_, new IllegalArgumentException(s"the key's serializer wrote no bytes to query ${store.value} by")))

  /** A long from eight big-endian bytes of `bytes`, starting at `at`. */
  private[streams] def longAt(bytes: Chunk[Byte], at: Int): Long = java.nio.ByteBuffer.wrap(bytes.drop(at).take(8).toArray).getLong

/** The queries of a key-value store. */
final class KeyValueQueries[F[_], K, V] private[streams] (store: Store, key: Serde[F, K], value: Serde[F, V])(using F: MonadThrow[F]):
  private def bytes(topic: xkafka.Topic, of: K): F[Chunk[Byte]] = QueryableStore.bytesOf(store, key, topic, of)

  private def entries(topic: xkafka.Topic, found: Vector[(Chunk[Byte], Chunk[Byte])]): F[Vector[(K, V)]] =
    found.traverse: (k, v) =>
      (key.deserializer.deserialize(topic, xkafka.Headers.empty, Some(k)), value.deserializer.deserialize(topic, xkafka.Headers.empty, Some(v)))
        .tupled

  /** What the store holds for `of`, if anything. */
  def get(of: K): Query[F, Option[V]] =
    new Query(
      store,
      topic => bytes(topic, of).map(QueryRequest.Get(store.value, _)),
      (answering, found) => found.headOption.traverse((_, v) => value.deserializer.deserialize(answering.topic, xkafka.Headers.empty, Some(v)))
    )

  /** The keys from `from` to `to`, both included, and what each holds, in the order of their serialized bytes, from every member. */
  def range(from: K, to: K): Query[F, Vector[(K, V)]] =
    new Query(
      store,
      topic => (bytes(topic, from), bytes(topic, to)).mapN((first, last) => QueryRequest.Range(store.value, Some(first), Some(last))),
      (answering, found) => entries(answering.topic, found)
    )

  /** Every key and what it holds, in the order of their serialized bytes, from every member. */
  def all: Query[F, Vector[(K, V)]] =
    new Query(store, _ => F.pure(QueryRequest.Range(store.value, None, None)), (answering, found) => entries(answering.topic, found))

/** The queries of a window store, whose keys are each key and its window's start. */
final class WindowQueries[F[_], K, V] private[streams] (store: Store, key: Serde[F, K], value: Serde[F, V])(using F: MonadThrow[F]):
  private def windows(topic: xkafka.Topic, found: Vector[(Chunk[Byte], Chunk[Byte])]): F[Vector[(K, Long, V)]] =
    // A window store's key is the serialized key, then the window's start in eight bytes and a sequence number in four.
    found.traverse: (stored, held) =>
      val serialized = stored.take(stored.size - 12)
      (
        key.deserializer.deserialize(topic, xkafka.Headers.empty, Some(serialized)),
        value.deserializer.deserialize(topic, xkafka.Headers.empty, Some(held))
      ).mapN((k, v) => (k, QueryableStore.longAt(stored, stored.size - 12), v))

  /** The windows of `of` starting from `from` to `to`, both included, each by its start, in order of their starts. */
  def fetch(of: K, from: xkafka.Timestamp, to: xkafka.Timestamp): Query[F, Vector[(Long, V)]] =
    new Query(
      store,
      topic =>
        QueryableStore.bytesOf(store, key, topic, of).map(bytes => QueryRequest.Windows(store.value, Some(bytes), from.epochMillis, to.epochMillis)),
      (answering, found) => windows(answering.topic, found).map(_.map((_, start, v) => (start, v)).sortBy(_._1))
    )

  /** The windows of every key starting from `from` to `to`, both included, from every member, each with its key, in order of their starts and then of
    * their keys' bytes. Each window ends its store's window size after it starts, as the program the store belongs to says.
    */
  def fetchAll(from: xkafka.Timestamp, to: xkafka.Timestamp): Query[F, Vector[(Windowed[K], V)]] =
    new Query(
      store,
      _ => F.pure(QueryRequest.Windows(store.value, None, from.epochMillis, to.epochMillis)),
      (answering, found) =>
        val ordered =
          found.sortBy((stored, _) => (QueryableStore.longAt(stored, stored.size - 12), stored.take(stored.size - 12)))(using
            Ordering.Tuple2(using Ordering.Long, QueryRequest.unsigned)
          )
        F.fromOption(answering.windowSize, new IllegalStateException(s"the store ${store.value} has no window size")).flatMap: size =>
          windows(answering.topic, ordered).map(_.map((k, start, v) => (Windowed(k, Window(start, start + size)), v)))
    )

/** The queries of a session store, whose keys are each key and its session's end and start. */
final class SessionQueries[F[_], K, V] private[streams] (store: Store, key: Serde[F, K], value: Serde[F, V])(using F: MonadThrow[F]):
  /** The sessions of `of`, each by its window, in order of their ends and then of their starts. */
  def fetch(of: K): Query[F, Vector[(Window, V)]] =
    new Query(
      store,
      topic => QueryableStore.bytesOf(store, key, topic, of).map(QueryRequest.Sessions(store.value, _)),
      (answering, found) =>
        // A session store's key is the serialized key, then the session's end and its start, each in eight bytes.
        found.traverse((stored, held) =>
          value.deserializer.deserialize(answering.topic, xkafka.Headers.empty, Some(held))
            .map(v => (Window(QueryableStore.longAt(stored, stored.size - 8), QueryableStore.longAt(stored, stored.size - 16)), v))
        ).map(_.sortBy((window, _) => (window.end, window.start)))
    )
