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

import scala.concurrent.duration.*

import cats.Functor
import cats.arrow.FunctionK
import cats.syntax.all.*
import fs2.{Chunk, Stream}

/** The topic, partition and offset a record a processor reads came from. */
final case class RecordOrigin(topic: Topic, partition: Partition, offset: Offset)

/** A record as a processor reads and returns it. A result keeps the time and headers of the record it was made from, if it is made with `withKey` or
  * `withValue`, and `origin` is only for reading.
  */
final case class Record[K, V](key: K, value: V, timestamp: Timestamp, headers: Headers = Headers.empty, origin: Option[RecordOrigin] = None):
  def withKey[K2](key: K2): Record[K2, V]               = copy(key = key)
  def withValue[V2](value: V2): Record[K, V2]           = copy(value = value)
  def withTimestamp(timestamp: Timestamp): Record[K, V] = copy(timestamp = timestamp)
  def withHeaders(headers: Headers): Record[K, V]       = copy(headers = headers)

/** A store a processor reads and writes, by key, in the order of its keys' serialized bytes, which is not always the order of `K`: the strings "10"
  * and "9" sort in that order, and a long written as text is not in numeric order. The processor reads what it has written since the last commit; a
  * commit makes those writes durable, and a failed one discards them.
  */
sealed trait KeyValueStore[F[_], K, V]:
  def get(key: K): F[Option[V]]

  def put(key: K, value: V): F[Unit]

  /** Writes `value` if the key holds nothing, and gives what it held. */
  def putIfAbsent(key: K, value: V): F[Option[V]]

  def putAll(entries: Iterable[(K, V)]): F[Unit]

  /** Removes the key, and gives what it held. */
  def delete(key: K): F[Option[V]]

  /** The keys from `from` to `to`, both included. */
  def range(from: K, to: K): Stream[F, (K, V)]

  /** The keys from `to` down to `from`, both included. */
  def reverseRange(from: K, to: K): Stream[F, (K, V)]

  /** Every key. */
  def all: Stream[F, (K, V)]

  /** Every key, from the last to the first. */
  def reverseAll: Stream[F, (K, V)]

  /** The keys whose serialized bytes start with what `serializer` writes `prefix` as. */
  def prefixScan[P](prefix: P, serializer: Serializer[F, P]): Stream[F, (K, V)]

object KeyValueStore:
  /** A key-value store for a processor to declare, which `Processor.withStore` opens. Processors given the same value share one store, and so run in
    * one sub-topology, as Java's do.
    */
  def of[F[_]: cats.MonadThrow, K, V](store: Store, key: Serde[F, K], value: Serde[F, V]): DeclaredStore[F, KeyValueStore[F, K, V]] =
    new DeclaredStore(store, access => opened(store, access, key, value), StoreAccess.idle(store))

  private def opened[F[_], K, V](store: Store, access: StoreAccess[F], key: Serde[F, K], value: Serde[F, V])(using
      F: cats.MonadThrow[F]
  ): KeyValueStore[F, K, V] =
    new KeyValueStore[F, K, V]:
      private def keyBytes(written: K): F[Chunk[Byte]] =
        key.serializer.serialize(access.topic, Headers.empty, written).flatMap:
          _.liftTo[F](new IllegalArgumentException(s"a key of the store '${store.value}' serialized to nothing"))

      private def read(bytes: Chunk[Byte]): F[V] = value.deserializer.deserialize(access.topic, Headers.empty, Some(bytes))

      private def entries(
          found: F[Vector[(Chunk[Byte], Chunk[Byte])]],
          order: Vector[(Chunk[Byte], Chunk[Byte])] => Vector[(Chunk[Byte], Chunk[Byte])]
      ) =
        Stream.eval(found).flatMap(held => Stream.emits(order(held))).evalMap: (k, v) =>
          (key.deserializer.deserialize(access.topic, Headers.empty, Some(k)), read(v)).tupled

      def get(k: K): F[Option[V]] = keyBytes(k).flatMap(access.get).flatMap(_.traverse(read))

      def put(k: K, v: V): F[Unit] = (keyBytes(k), value.serializer.serialize(access.topic, Headers.empty, v)).tupled.flatMap(access.put(_, _))

      def putIfAbsent(k: K, v: V): F[Option[V]] = get(k).flatTap(held => put(k, v).whenA(held.isEmpty))

      def putAll(all: Iterable[(K, V)]): F[Unit] = all.toList.traverse_((k, v) => put(k, v))

      def delete(k: K): F[Option[V]] = keyBytes(k).flatMap(bytes => access.get(bytes).flatMap(_.traverse(read)).flatTap(_ => access.put(bytes, None)))

      def range(from: K, to: K): Stream[F, (K, V)] = entries((keyBytes(from), keyBytes(to)).tupled.flatMap(access.between(_, _)), identity)

      def reverseRange(from: K, to: K): Stream[F, (K, V)] = entries((keyBytes(from), keyBytes(to)).tupled.flatMap(access.between(_, _)), _.reverse)

      def all: Stream[F, (K, V)] = entries(access.all, identity)

      def reverseAll: Stream[F, (K, V)] = entries(access.all, _.reverse)

      def prefixScan[P](prefix: P, serializer: Serializer[F, P]): Stream[F, (K, V)] =
        val bytes =
          serializer.serialize(access.topic, Headers.empty, prefix).flatMap:
            _.liftTo[F](new IllegalArgumentException(s"a prefix for the store '${store.value}' serialized to nothing"))
        entries(bytes.flatMap(access.prefixed), identity)

/** A store a processor reads and writes by key and window start, as Java's `WindowStore`, each key holding one value for each window. A window has
  * expired once the latest window start written is a whole retention past its start: reads do not find it, and writing to it keeps nothing. Reads
  * over several keys go by window start and then by the order of the keys' serialized bytes. The processor reads what it has written since the last
  * commit; a commit makes those writes durable, and a failed one discards them.
  */
sealed trait WindowStore[F[_], K, V]:
  /** Writes `value` as the key's window starting at `windowStart`. */
  def put(key: K, value: V, windowStart: Timestamp): F[Unit]

  /** Removes the key's window starting at `windowStart`. */
  def delete(key: K, windowStart: Timestamp): F[Unit]

  /** The key's window starting at `windowStart`. */
  def fetch(key: K, windowStart: Timestamp): F[Option[V]]

  /** The key's windows starting from `from` to `to`, both included, earliest first, with their starts. */
  def fetch(key: K, from: Timestamp, to: Timestamp): Stream[F, (Timestamp, V)]

  /** The key's windows starting from `from` to `to`, both included, latest first, with their starts. */
  def reverseFetch(key: K, from: Timestamp, to: Timestamp): Stream[F, (Timestamp, V)]

  /** The windows starting from `from` to `to` of the keys from `keyFrom` to `keyTo`, all included. */
  def fetchRange(keyFrom: K, keyTo: K, from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)]

  /** The windows starting from `from` to `to` of the keys from `keyFrom` to `keyTo`, all included, last first. */
  def reverseFetchRange(keyFrom: K, keyTo: K, from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)]

  /** The windows of every key starting from `from` to `to`, both included. */
  def fetchAll(from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)]

  /** The windows of every key starting from `from` to `to`, both included, last first. */
  def reverseFetchAll(from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)]

  /** Every window. */
  def all: Stream[F, (Windowed[K], V)]

  /** Every window, last first. */
  def reverseAll: Stream[F, (Windowed[K], V)]

object WindowStore:
  /** A window store for a processor to declare, which `Processor.withStore` opens. `size` gives each window read over several keys its end, and
    * `retention` how long a window lasts. Processors given the same value share one store, and so run in one sub-topology, as Java's do.
    */
  def of[F[_]: cats.MonadThrow, K, V](
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      size: FiniteDuration,
      retention: FiniteDuration
  ): DeclaredStore[F, WindowStore[F, K, V]] =
    val problems =
      List(
        Option.when(size < 1.milli)(s"its window size of $size is less than a millisecond"),
        Option.when(retention < size)(s"its retention of $retention is less than its window size of $size")
      ).flatten
    new DeclaredStore(
      store,
      access => opened(store, access, key, value, size.toMillis, retention.toMillis),
      StoreAccess.idle(store),
      Some(retention.toMillis),
      problems
    )

  private def opened[F[_], K, V](store: Store, access: StoreAccess[F], key: Serde[F, K], value: Serde[F, V], size: Long, retention: Long)(using
      F: cats.MonadThrow[F]
  ): WindowStore[F, K, V] =
    new WindowStore[F, K, V]:
      private def keyBytes(written: K): F[Chunk[Byte]] =
        key.serializer.serialize(access.topic, Headers.empty, written).flatMap:
          _.liftTo[F](new IllegalArgumentException(s"a key of the store '${store.value}' serialized to nothing"))

      private def read(bytes: Chunk[Byte]): F[V] = value.deserializer.deserialize(access.topic, Headers.empty, Some(bytes))

      private def single(found: F[Vector[(Long, Chunk[Byte])]]): Stream[F, (Timestamp, V)] =
        Stream.eval(found).flatMap(Stream.emits).evalMap((start, held) => read(held).map(Timestamp.fromEpochMillis(start) -> _))

      private def many(found: F[Vector[(Chunk[Byte], Long, Chunk[Byte])]]): Stream[F, (Windowed[K], V)] =
        Stream.eval(found).flatMap(Stream.emits).evalMap: (k, start, held) =>
          // As Java, a window ending past the largest time ends at the largest time.
          val end = if start + size < 0 then Long.MaxValue else start + size
          (key.deserializer.deserialize(access.topic, Headers.empty, Some(k)), read(held))
            .mapN((keyRead, v) => Windowed(keyRead, Window(start, end)) -> v)

      private def between(keyFrom: K, keyTo: K, from: Timestamp, to: Timestamp) =
        (keyBytes(keyFrom), keyBytes(keyTo)).tupled.flatMap(keys => access.windows(from.epochMillis, to.epochMillis, Some(keys)))

      def put(k: K, v: V, windowStart: Timestamp): F[Unit] =
        (keyBytes(k), value.serializer.serialize(access.topic, Headers.empty, v)).tupled
          .flatMap(access.windowPut(_, windowStart.epochMillis, _, retention))

      def delete(k: K, windowStart: Timestamp): F[Unit] = keyBytes(k).flatMap(access.windowPut(_, windowStart.epochMillis, None, retention))

      def fetch(k: K, windowStart: Timestamp): F[Option[V]] =
        keyBytes(k).flatMap(access.windowsOf(_, windowStart.epochMillis, windowStart.epochMillis, retention))
          .flatMap(_.headOption.traverse(held => read(held._2)))

      def fetch(k: K, from: Timestamp, to: Timestamp): Stream[F, (Timestamp, V)] =
        single(keyBytes(k).flatMap(access.windowsOf(_, from.epochMillis, to.epochMillis, retention)))

      def reverseFetch(k: K, from: Timestamp, to: Timestamp): Stream[F, (Timestamp, V)] =
        single(keyBytes(k).flatMap(access.windowsOf(_, from.epochMillis, to.epochMillis, retention)).map(_.reverse))

      def fetchRange(keyFrom: K, keyTo: K, from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)] = many(between(keyFrom, keyTo, from, to))

      def reverseFetchRange(keyFrom: K, keyTo: K, from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)] =
        many(between(keyFrom, keyTo, from, to).map(_.reverse))

      def fetchAll(from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)] = many(access.windows(from.epochMillis, to.epochMillis, None))

      def reverseFetchAll(from: Timestamp, to: Timestamp): Stream[F, (Windowed[K], V)] =
        many(access.windows(from.epochMillis, to.epochMillis, None).map(_.reverse))

      def all: Stream[F, (Windowed[K], V)] = many(access.windows(Long.MinValue, Long.MaxValue, None))

      def reverseAll: Stream[F, (Windowed[K], V)] = many(access.windows(Long.MinValue, Long.MaxValue, None).map(_.reverse))

/** A store a processor reads and writes by session, as Java's `SessionStore`: a key with a start and an end, each session holding one value. A
  * session has expired once the latest end written is a whole retention past its end: reads do not find it, and writing to it keeps nothing. A
  * `remove` does not move that latest end, but once restored the store counts it. Reads over several sessions go by end, then by the order of the
  * keys' serialized bytes, then the latest start first, and `reverse` reads the exact opposite. The processor reads what it has written since the
  * last commit; a commit makes those writes durable, and a failed one discards them.
  */
sealed trait SessionStore[F[_], K, V]:
  /** Writes `value` as the session, which fails if it ends before it starts. */
  def put(session: Windowed[K], value: V): F[Unit]

  /** Removes the session, which fails if it ends before it starts. */
  def remove(session: Windowed[K]): F[Unit]

  /** The value of the key's session from exactly `start` to exactly `end`. */
  def fetchSession(key: K, start: Timestamp, end: Timestamp): F[Option[V]]

  /** Every session of the key. */
  def fetch(key: K): Stream[F, (Windowed[K], V)]

  /** Every session of the key, last first. */
  def reverseFetch(key: K): Stream[F, (Windowed[K], V)]

  /** Every session of the keys from `keyFrom` to `keyTo`, both included. */
  def fetchRange(keyFrom: K, keyTo: K): Stream[F, (Windowed[K], V)]

  /** Every session of the keys from `keyFrom` to `keyTo`, both included, last first. */
  def reverseFetchRange(keyFrom: K, keyTo: K): Stream[F, (Windowed[K], V)]

  /** The key's sessions ending at or after `earliestEnd` and starting at or before `latestStart`. */
  def findSessions(key: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)]

  /** The key's sessions ending at or after `earliestEnd` and starting at or before `latestStart`, last first. */
  def reverseFindSessions(key: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)]

  /** The sessions of the keys from `keyFrom` to `keyTo`, both included, ending at or after `earliestEnd` and starting at or before `latestStart`. */
  def findSessionsRange(keyFrom: K, keyTo: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)]

  /** The sessions of the keys from `keyFrom` to `keyTo`, both included, ending at or after `earliestEnd` and starting at or before `latestStart`,
    * last first.
    */
  def reverseFindSessionsRange(keyFrom: K, keyTo: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)]

  /** The sessions of every key ending from `earliestEnd` to `latestEnd`, both included. */
  def findSessionsEnding(earliestEnd: Timestamp, latestEnd: Timestamp): Stream[F, (Windowed[K], V)]

object SessionStore:
  /** A session store for a processor to declare, which `Processor.withStore` opens. `retention` is how long a session lasts. Processors given the
    * same value share one store, and so run in one sub-topology, as Java's do.
    */
  def of[F[_]: cats.MonadThrow, K, V](
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      retention: FiniteDuration
  ): DeclaredStore[F, SessionStore[F, K, V]] =
    new DeclaredStore(
      store,
      access => opened(store, access, key, value, retention.toMillis),
      StoreAccess.idle(store),
      Some(retention.toMillis),
      Option.when(retention < 1.milli)(s"its retention of $retention is less than a millisecond").toList,
      session = true
    )

  private def opened[F[_], K, V](store: Store, access: StoreAccess[F], key: Serde[F, K], value: Serde[F, V], retention: Long)(using
      F: cats.MonadThrow[F]
  ): SessionStore[F, K, V] =
    new SessionStore[F, K, V]:
      private def keyBytes(written: K): F[Chunk[Byte]] =
        key.serializer.serialize(access.topic, Headers.empty, written).flatMap:
          _.liftTo[F](new IllegalArgumentException(s"a key of the store '${store.value}' serialized to nothing"))

      private def read(bytes: Chunk[Byte]): F[V] = value.deserializer.deserialize(access.topic, Headers.empty, Some(bytes))

      private def valid(session: Windowed[K]): F[Chunk[Byte]] =
        if session.window.end < session.window.start then
          F.raiseError(new IllegalArgumentException(s"a session of the store '${store.value}' ends at ${session.window
              .end}, before its start at ${session.window.start}"))
        else keyBytes(session.key)

      private def many(found: F[Vector[(Chunk[Byte], Long, Long, Chunk[Byte])]]): Stream[F, (Windowed[K], V)] =
        Stream.eval(found).flatMap(Stream.emits).evalMap: (k, start, end, held) =>
          (key.deserializer.deserialize(access.topic, Headers.empty, Some(k)), read(held))
            .mapN((keyRead, v) => Windowed(keyRead, Window(start, end)) -> v)

      private def ofKeys(keyFrom: K, keyTo: K, earliestEnd: Long, latestStart: Long) =
        (keyBytes(keyFrom), keyBytes(keyTo)).tupled.flatMap(keys => access.sessions(Some(keys), earliestEnd, Long.MaxValue, latestStart))

      def put(session: Windowed[K], v: V): F[Unit] =
        (valid(session), value.serializer.serialize(access.topic, Headers.empty, v)).tupled.flatMap: (k, written) =>
          access.sessionWrite(k, session.window.start, session.window.end, written, moving = true, retention)

      def remove(session: Windowed[K]): F[Unit] =
        valid(session).flatMap(access.sessionWrite(_, session.window.start, session.window.end, None, moving = false, retention))

      def fetchSession(k: K, start: Timestamp, end: Timestamp): F[Option[V]] =
        keyBytes(k).flatMap(bytes => access.sessions(Some((bytes, bytes)), end.epochMillis, end.epochMillis, start.epochMillis)).flatMap:
          _.collectFirst { case (_, found, _, held) if found == start.epochMillis => held }.traverse(read)

      def fetch(k: K): Stream[F, (Windowed[K], V)] = fetchRange(k, k)

      def reverseFetch(k: K): Stream[F, (Windowed[K], V)] = reverseFetchRange(k, k)

      def fetchRange(keyFrom: K, keyTo: K): Stream[F, (Windowed[K], V)] = many(ofKeys(keyFrom, keyTo, Long.MinValue, Long.MaxValue))

      def reverseFetchRange(keyFrom: K, keyTo: K): Stream[F, (Windowed[K], V)] =
        many(ofKeys(keyFrom, keyTo, Long.MinValue, Long.MaxValue).map(_.reverse))

      def findSessions(k: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)] =
        findSessionsRange(k, k, earliestEnd, latestStart)

      def reverseFindSessions(k: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)] =
        reverseFindSessionsRange(k, k, earliestEnd, latestStart)

      def findSessionsRange(keyFrom: K, keyTo: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)] =
        many(ofKeys(keyFrom, keyTo, earliestEnd.epochMillis, latestStart.epochMillis))

      def reverseFindSessionsRange(keyFrom: K, keyTo: K, earliestEnd: Timestamp, latestStart: Timestamp): Stream[F, (Windowed[K], V)] =
        many(ofKeys(keyFrom, keyTo, earliestEnd.epochMillis, latestStart.epochMillis).map(_.reverse))

      def findSessionsEnding(earliestEnd: Timestamp, latestEnd: Timestamp): Stream[F, (Windowed[K], V)] =
        many(access.sessions(None, earliestEnd.epochMillis, latestEnd.epochMillis, Long.MaxValue))

/** A store a processor declares, which it uses as an `H`. */
final class DeclaredStore[F[_], H] private[xkafka] (
    val store: Store,
    private[xkafka] val open: StoreAccess[F] => H,
    private[xkafka] val idle: StoreAccess[F],
    retention: Option[Long] = None,
    problems: List[String] = Nil,
    session: Boolean = false
):
  private[xkafka] val kept: KeptStore = KeptStore(store, retention = retention, session = session, shared = Some(this), problems = problems)

/** A store as bytes, for a task to give a processor, with what the processor wrote since its last commit in it. */
private[xkafka] trait StoreAccess[F[_]]:
  /** The topic the store's serdes are given, its changelog as Java's are. */
  def topic: Topic

  def get(key: Chunk[Byte]): F[Option[Chunk[Byte]]]

  /** Writes `value` at `key`, or removes the key if there is none. */
  def put(key: Chunk[Byte], value: Option[Chunk[Byte]]): F[Unit]

  def between(from: Chunk[Byte], to: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]]

  def all: F[Vector[(Chunk[Byte], Chunk[Byte])]]

  def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]]

  /** Writes `value` as the window of `key` starting at `start`, or removes it if there is none, in a window store keeping windows for `retention`. */
  def windowPut(key: Chunk[Byte], start: Long, value: Option[Chunk[Byte]], retention: Long): F[Unit]

  /** The windows of `key` starting from `from` to `to`, earliest first, with their starts. */
  def windowsOf(key: Chunk[Byte], from: Long, to: Long, retention: Long): F[Vector[(Long, Chunk[Byte])]]

  /** The windows starting from `from` to `to` of the keys `keys` spans, or of every key, by start and then key, each with its key and start. */
  def windows(from: Long, to: Long, keys: Option[(Chunk[Byte], Chunk[Byte])]): F[Vector[(Chunk[Byte], Long, Chunk[Byte])]]

  /** Writes `value` as the session of `key` from `start` to `end`, or removes it if there is none, in a session store keeping sessions for
    * `retention`. Only if `moving` does the end count towards the store's clock.
    */
  def sessionWrite(key: Chunk[Byte], start: Long, end: Long, value: Option[Chunk[Byte]], moving: Boolean, retention: Long): F[Unit]

  /** The sessions of the keys `keys` spans, or of every key, ending from `earliestEnd` to `latestEnd` and starting by `latestStart`, by end, then
    * key, then the latest start first, each with its key, start and end.
    */
  def sessions(
      keys: Option[(Chunk[Byte], Chunk[Byte])],
      earliestEnd: Long,
      latestEnd: Long,
      latestStart: Long
  ): F[Vector[(Chunk[Byte], Long, Long, Chunk[Byte])]]

  final def imapK[G[_]](fk: FunctionK[F, G]): StoreAccess[G] =
    val self = this
    new StoreAccess[G]:
      def topic: Topic                                                                                   = self.topic
      def get(key: Chunk[Byte]): G[Option[Chunk[Byte]]]                                                  = fk(self.get(key))
      def put(key: Chunk[Byte], value: Option[Chunk[Byte]]): G[Unit]                                     = fk(self.put(key, value))
      def between(from: Chunk[Byte], to: Chunk[Byte]): G[Vector[(Chunk[Byte], Chunk[Byte])]]             = fk(self.between(from, to))
      def all: G[Vector[(Chunk[Byte], Chunk[Byte])]]                                                     = fk(self.all)
      def prefixed(prefix: Chunk[Byte]): G[Vector[(Chunk[Byte], Chunk[Byte])]]                           = fk(self.prefixed(prefix))
      def windowPut(key: Chunk[Byte], start: Long, value: Option[Chunk[Byte]], retention: Long): G[Unit] =
        fk(self.windowPut(key, start, value, retention))
      def windowsOf(key: Chunk[Byte], from: Long, to: Long, retention: Long): G[Vector[(Long, Chunk[Byte])]] =
        fk(self.windowsOf(key, from, to, retention))
      def windows(from: Long, to: Long, keys: Option[(Chunk[Byte], Chunk[Byte])]): G[Vector[(Chunk[Byte], Long, Chunk[Byte])]] =
        fk(self.windows(from, to, keys))
      def sessionWrite(key: Chunk[Byte], start: Long, end: Long, value: Option[Chunk[Byte]], moving: Boolean, retention: Long): G[Unit] =
        fk(self.sessionWrite(key, start, end, value, moving, retention))
      def sessions(
          keys: Option[(Chunk[Byte], Chunk[Byte])],
          earliestEnd: Long,
          latestEnd: Long,
          latestStart: Long
      ): G[Vector[(Chunk[Byte], Long, Long, Chunk[Byte])]] = fk(self.sessions(keys, earliestEnd, latestEnd, latestStart))

private[xkafka] object StoreAccess:
  /** Access to `store` that refuses every use, for seeing what a processor declares without running it. */
  def idle[F[_]](store: Store)(using F: cats.ApplicativeThrow[F]): StoreAccess[F] =
    def refused[A]: F[A] = F.raiseError(new IllegalStateException(s"the store '${store.value}' was used while its processor was being declared"))
    new StoreAccess[F]:
      val topic: Topic                                                                                       = Topic.from("idle").toOption.get
      def get(key: Chunk[Byte]): F[Option[Chunk[Byte]]]                                                      = refused
      def put(key: Chunk[Byte], value: Option[Chunk[Byte]]): F[Unit]                                         = refused
      def between(from: Chunk[Byte], to: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]]                 = refused
      def all: F[Vector[(Chunk[Byte], Chunk[Byte])]]                                                         = refused
      def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]]                               = refused
      def windowPut(key: Chunk[Byte], start: Long, value: Option[Chunk[Byte]], retention: Long): F[Unit]     = refused
      def windowsOf(key: Chunk[Byte], from: Long, to: Long, retention: Long): F[Vector[(Long, Chunk[Byte])]] = refused
      def windows(from: Long, to: Long, keys: Option[(Chunk[Byte], Chunk[Byte])]): F[Vector[(Chunk[Byte], Long, Chunk[Byte])]]          = refused
      def sessionWrite(key: Chunk[Byte], start: Long, end: Long, value: Option[Chunk[Byte]], moving: Boolean, retention: Long): F[Unit] = refused
      def sessions(
          keys: Option[(Chunk[Byte], Chunk[Byte])],
          earliestEnd: Long,
          latestEnd: Long,
          latestStart: Long
      ): F[Vector[(Chunk[Byte], Long, Long, Chunk[Byte])]] = refused

/** When a punctuator runs. */
final class Punctuation private (private[xkafka] val wallClock: Boolean, val interval: FiniteDuration):
  /** Why it cannot be used, if it cannot. */
  private[xkafka] def problem: Option[String] = Option.when(interval < 1.milli)(s"its interval of $interval is less than a millisecond")

  /** The interval in milliseconds. */
  private[xkafka] def millis: Long = interval.toMillis

object Punctuation:
  /** As the task's stream time, the latest time of a record it has read, passes each multiple of `interval`, and as soon as it has a stream time. It
    * runs when records arrive, and once however many multiples they pass.
    */
  def streamTime(interval: FiniteDuration): Punctuation = new Punctuation(false, interval)

  /** Each time `interval` has passed by the clock, from when the task starts, with or without records. It runs once however many intervals were
    * missed.
    */
  def wallClock(interval: FiniteDuration): Punctuation = new Punctuation(true, interval)

/** What runs on a schedule, given the time it runs at, and the records it returns. */
private[xkafka] final class Punctuator[F[_], K2, V2](val schedule: Punctuation, val run: Timestamp => F[List[Record[K2, V2]]]):
  def mapK[G[_]](fk: FunctionK[F, G]): Punctuator[G, K2, V2] = new Punctuator(schedule, at => fk(run(at)))

/** What a processor does with each record and on its schedules, given the stores it opened. */
final class ProcessorLogic[F[_], K, V, K2, V2] private[xkafka] (
    private[xkafka] val onRecord: Record[K, V] => F[List[Record[K2, V2]]],
    private[xkafka] val punctuators: List[Punctuator[F, K2, V2]] = Nil
):
  private[xkafka] def mapK[G[_]](fk: FunctionK[F, G]): ProcessorLogic[G, K, V, K2, V2] =
    new ProcessorLogic(record => fk(onRecord(record)), punctuators.map(_.mapK(fk)))

  private[xkafka] def punctuating(punctuator: Punctuator[F, K2, V2]): ProcessorLogic[F, K, V, K2, V2] =
    new ProcessorLogic(onRecord, punctuator :: punctuators)

/** A step of a stream that a program writes itself, as Java's `Processor`, with the stores it declares handed to it already typed, so that it cannot
  * read a store it did not declare. It runs once for each task, so what it reads and writes is the task's own part of each store.
  */
final class Processor[F[_], K, V, K2, V2] private (
    private[xkafka] val kept: List[KeptStore],
    private[xkafka] val idle: Map[String, StoreAccess[F]],
    private[xkafka] val handling: Map[String, StoreAccess[F]] => ProcessorLogic[F, K, V, K2, V2]
):
  /** The schedules it declares, in order. */
  private[xkafka] lazy val schedules: List[Punctuation] = handling(idle).punctuators.map(_.schedule)

  /** The same processor with its effects in `G`: its stores come from `G` and its handlers go to it. */
  private[xkafka] def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Processor[G, K, V, K2, V2] =
    new Processor(kept, idle.view.mapValues(_.imapK(fk)).toMap, access => handling(access.view.mapValues(_.imapK(gk)).toMap).mapK(fk))

object Processor:
  /** What to do with each record: the records it returns go on to the next step. */
  def onRecord[F[_], K, V, K2, V2](handle: Record[K, V] => F[List[Record[K2, V2]]]): ProcessorLogic[F, K, V, K2, V2] = new ProcessorLogic(handle)

  /** `logic`, and `punctuate` run on `schedule`: the records it returns go on to the next step, and what it writes to the stores is kept as what a
    * handler writes is, at the time it is given. Wrap `logic` in several to run several, in the order they are written.
    */
  def every[F[_], K, V, K2, V2](schedule: Punctuation)(punctuate: Timestamp => F[List[Record[K2, V2]]])(
      logic: ProcessorLogic[F, K, V, K2, V2]
  ): ProcessorLogic[F, K, V, K2, V2] = logic.punctuating(new Punctuator(schedule, punctuate))

  /** A processor keeping no store. */
  def withoutStores[F[_], K, V, K2, V2](logic: ProcessorLogic[F, K, V, K2, V2]): Processor[F, K, V, K2, V2] =
    new Processor(Nil, Map.empty, _ => logic)

  /** A processor keeping one store. */
  def withStore[F[_], H, K, V, K2, V2](store: DeclaredStore[F, H])(logic: H => ProcessorLogic[F, K, V, K2, V2]): Processor[F, K, V, K2, V2] =
    new Processor(List(store.kept), Map(store.store.value -> store.idle), access => logic(store.open(access(store.store.value))))

  /** A processor keeping two stores. */
  def withStores[F[_], H1, H2, K, V, K2, V2](first: DeclaredStore[F, H1], second: DeclaredStore[F, H2])(
      logic: (H1, H2) => ProcessorLogic[F, K, V, K2, V2]
  ): Processor[F, K, V, K2, V2] =
    new Processor(
      List(first.kept, second.kept),
      Map(first.store.value -> first.idle, second.store.value -> second.idle),
      access => logic(first.open(access(first.store.value)), second.open(access(second.store.value)))
    )

  /** A processor keeping three stores. */
  def withStores[F[_], H1, H2, H3, K, V, K2, V2](first: DeclaredStore[F, H1], second: DeclaredStore[F, H2], third: DeclaredStore[F, H3])(
      logic: (H1, H2, H3) => ProcessorLogic[F, K, V, K2, V2]
  ): Processor[F, K, V, K2, V2] =
    new Processor(
      List(first.kept, second.kept, third.kept),
      Map(first.store.value -> first.idle, second.store.value -> second.idle, third.store.value -> third.idle),
      access => logic(first.open(access(first.store.value)), second.open(access(second.store.value)), third.open(access(third.store.value)))
    )

/** A record as a processor given to `processValues` reads and returns it, which can change its value, time and headers but not its key. A result
  * keeps the time and headers of the record it was made from unless it changes them.
  */
final class FixedKeyRecord[K, V] private[xkafka] (private[xkafka] val record: Record[K, V]):
  def key: K                       = record.key
  def value: V                     = record.value
  def timestamp: Timestamp         = record.timestamp
  def headers: Headers             = record.headers
  def origin: Option[RecordOrigin] = record.origin

  def withValue[V2](value: V2): FixedKeyRecord[K, V2]           = new FixedKeyRecord(record.withValue(value))
  def withTimestamp(timestamp: Timestamp): FixedKeyRecord[K, V] = new FixedKeyRecord(record.withTimestamp(timestamp))
  def withHeaders(headers: Headers): FixedKeyRecord[K, V]       = new FixedKeyRecord(record.withHeaders(headers))

/** What a processor given to `processValues` does with each record, given the stores it opened. */
final class FixedKeyLogic[F[_], K, V, V2] private[xkafka] (private[xkafka] val logic: ProcessorLogic[F, K, V, K, V2])

/** A step of a stream that a program writes itself and that keeps each record's key, as Java's `FixedKeyProcessor`, so that grouping or joining its
  * results needs no repartition topic it would not have needed before. It declares its stores as a [[Processor]] does.
  */
final class FixedKeyProcessor[F[_], K, V, V2] private (private[xkafka] val processor: Processor[F, K, V, K, V2])

object FixedKeyProcessor:
  /** What to do with each record: the records it returns go on to the next step. */
  def onRecord[F[_]: Functor, K, V, V2](handle: FixedKeyRecord[K, V] => F[List[FixedKeyRecord[K, V2]]]): FixedKeyLogic[F, K, V, V2] =
    new FixedKeyLogic(Processor.onRecord(record => handle(new FixedKeyRecord(record)).map(_.map(_.record))))

  /** `logic`, and `punctuate` run on `schedule`. What it writes to the stores is kept as what a handler writes is, at the time it is given. It sends
    * nothing on, since a record that keeps its key can only be made from a record read. Wrap `logic` in several to run several, in the order they are
    * written.
    */
  def every[F[_]: Functor, K, V, V2](schedule: Punctuation)(punctuate: Timestamp => F[Unit])(
      logic: FixedKeyLogic[F, K, V, V2]
  ): FixedKeyLogic[F, K, V, V2] = new FixedKeyLogic(Processor.every(schedule)(at => punctuate(at).as(List.empty[Record[K, V2]]))(logic.logic))

  /** A processor keeping no store. */
  def withoutStores[F[_], K, V, V2](logic: FixedKeyLogic[F, K, V, V2]): FixedKeyProcessor[F, K, V, V2] =
    new FixedKeyProcessor(Processor.withoutStores(logic.logic))

  /** A processor keeping one store. */
  def withStore[F[_], H, K, V, V2](store: DeclaredStore[F, H])(logic: H => FixedKeyLogic[F, K, V, V2]): FixedKeyProcessor[F, K, V, V2] =
    new FixedKeyProcessor(Processor.withStore(store)(logic(_).logic))

  /** A processor keeping two stores. */
  def withStores[F[_], H1, H2, K, V, V2](first: DeclaredStore[F, H1], second: DeclaredStore[F, H2])(
      logic: (H1, H2) => FixedKeyLogic[F, K, V, V2]
  ): FixedKeyProcessor[F, K, V, V2] = new FixedKeyProcessor(Processor.withStores(first, second)(logic(_, _).logic))

  /** A processor keeping three stores. */
  def withStores[F[_], H1, H2, H3, K, V, V2](first: DeclaredStore[F, H1], second: DeclaredStore[F, H2], third: DeclaredStore[F, H3])(
      logic: (H1, H2, H3) => FixedKeyLogic[F, K, V, V2]
  ): FixedKeyProcessor[F, K, V, V2] = new FixedKeyProcessor(Processor.withStores(first, second, third)(logic(_, _, _).logic))
