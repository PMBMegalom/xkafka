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

/** A store a processor declares, which it uses as an `H`. */
final class DeclaredStore[F[_], H] private[xkafka] (
    val store: Store,
    private[xkafka] val open: StoreAccess[F] => H,
    private[xkafka] val idle: StoreAccess[F]
):
  private[xkafka] val kept: KeptStore = KeptStore(store, shared = Some(this))

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

  final def imapK[G[_]](fk: FunctionK[F, G]): StoreAccess[G] =
    val self = this
    new StoreAccess[G]:
      def topic: Topic                                                                       = self.topic
      def get(key: Chunk[Byte]): G[Option[Chunk[Byte]]]                                      = fk(self.get(key))
      def put(key: Chunk[Byte], value: Option[Chunk[Byte]]): G[Unit]                         = fk(self.put(key, value))
      def between(from: Chunk[Byte], to: Chunk[Byte]): G[Vector[(Chunk[Byte], Chunk[Byte])]] = fk(self.between(from, to))
      def all: G[Vector[(Chunk[Byte], Chunk[Byte])]]                                         = fk(self.all)
      def prefixed(prefix: Chunk[Byte]): G[Vector[(Chunk[Byte], Chunk[Byte])]]               = fk(self.prefixed(prefix))

private[xkafka] object StoreAccess:
  /** Access to `store` that refuses every use, for seeing what a processor declares without running it. */
  def idle[F[_]](store: Store)(using F: cats.ApplicativeThrow[F]): StoreAccess[F] =
    def refused[A]: F[A] = F.raiseError(new IllegalStateException(s"the store '${store.value}' was used while its processor was being declared"))
    new StoreAccess[F]:
      val topic: Topic                                                                       = Topic.from("idle").toOption.get
      def get(key: Chunk[Byte]): F[Option[Chunk[Byte]]]                                      = refused
      def put(key: Chunk[Byte], value: Option[Chunk[Byte]]): F[Unit]                         = refused
      def between(from: Chunk[Byte], to: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]] = refused
      def all: F[Vector[(Chunk[Byte], Chunk[Byte])]]                                         = refused
      def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Chunk[Byte])]]               = refused

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
