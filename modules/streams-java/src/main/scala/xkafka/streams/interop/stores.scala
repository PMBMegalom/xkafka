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
package interop

import cats.MonadThrow
import cats.syntax.all.*
import fs2.Chunk
import org.apache.kafka.common.serialization.Serializer as JavaSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.{
  KeyValueIterator, KeyValueStore as JavaKeyValueStore, TimestampedKeyValueStore as JavaTimestampedKeyValueStore, ValueAndTimestamp
}

/** A store a Java processor reads and writes through `getStateStore`, by its name, as the kind of store its code takes it for. */
final class JavaStore[F[_]] private[interop] (
    private[interop] val kept: KeptStore,
    private[interop] val idle: StoreAccess[F],
    private[interop] val view: (() => StoreAccess[F], Runner[F]) => StateStore
):
  private[interop] def name: String = kept.store.value

object JavaStore:
  /** A key-value store, which the Java processor takes as a `KeyValueStore<K, V>`. */
  def keyValue[F[_]: MonadThrow, K <: AnyRef, V >: Null <: AnyRef](store: Store, key: Serde[F, K], value: Serde[F, V]): JavaStore[F] =
    val declared = KeyValueStore.of(store, key, value)
    new JavaStore(declared.kept, declared.idle, (access, run) => new KeyValueView(store.value, key, value, access, run))

  /** A key-value store keeping the time of each value, which the Java processor takes as a `TimestampedKeyValueStore<K, V>`. */
  def timestamped[F[_]: MonadThrow, K <: AnyRef, V >: Null <: AnyRef](store: Store, key: Serde[F, K], value: Serde[F, V]): JavaStore[F] =
    val declared = KeyValueStore.of(store, key, value)
    new JavaStore(declared.kept, declared.idle, (access, run) => new TimestampedView(store.value, key, value, access, run))

/** Runs an effect to its end for a Java call that waits for it. */
private[interop] trait Runner[F[_]]:
  def apply[A](effect: F[A]): A

/** A Java iterator over what was found, which holds nothing open. */
private final class Found[K, V](found: List[(K, V)]) extends KeyValueIterator[K, V]:
  private val left           = found.iterator.buffered
  def hasNext: Boolean       = left.hasNext
  def next(): KeyValue[K, V] =
    val (key, value) = left.next()
    KeyValue.pair(key, value)
  def peekNextKey(): K = left.head._1
  def close(): Unit    = ()

/** What every view of a store does as a Java `StateStore`: it is open as long as its processor's instance is, and keeps nothing on disk. */
private abstract class View[F[_], K <: AnyRef, V <: AnyRef, S <: AnyRef](
    val storeName: String,
    key: Serde[F, K],
    value: Serde[F, V],
    access: () => StoreAccess[F]
)(using F: MonadThrow[F]):
  def name(): String                                           = storeName
  def init(context: StateStoreContext, root: StateStore): Unit = ()
  def close(): Unit                                            = ()
  def persistent(): Boolean                                    = false
  def isOpen(): Boolean                                        = true

  protected def keyBytes(written: K): F[Chunk[Byte]] =
    key.serializer.serialize(access().topic, Headers.empty, written)
      .flatMap(_.liftTo[F](new IllegalArgumentException(s"a key of the store '$storeName' was written as none")))

  protected def keyOf(bytes: Chunk[Byte]): F[K]                = key.deserializer.deserialize(access().topic, Headers.empty, Some(bytes))
  protected def valueOf(bytes: Chunk[Byte]): F[V]              = value.deserializer.deserialize(access().topic, Headers.empty, Some(bytes))
  protected def valueBytes(written: V): F[Option[Chunk[Byte]]] = value.serializer.serialize(access().topic, Headers.empty, written)

  /** What a range of keys finds, by the order of their bytes, the first and last included where given, as Java's stores find them, or the keys
    * starting with a prefix.
    */
  protected def scanned(from: Option[K], to: Option[K], prefix: Option[Chunk[Byte]]): F[Vector[(K, S)]] =
    (from.traverse(keyBytes), to.traverse(keyBytes)).tupled.flatMap: (first, last) =>
      access().timedScan(prefix).map(_.filter((k, _, _) => first.forall(View.order(_, k) <= 0) && last.forall(View.order(k, _) <= 0)))
        .flatMap(_.traverse((k, v, at) => (keyOf(k), held(v, at)).tupled))

  /** What a value read at its time is to the Java processor. */
  protected def held(bytes: Chunk[Byte], at: Long): F[S]

  protected def prefixOf[P](prefix: P, serializer: JavaSerializer[P]): Chunk[Byte] = Chunk.array(serializer.serialize(access().topic.value, prefix))

private object View:
  /** The order of keys' bytes as unsigned, as Java's stores keep them. */
  def order(left: Chunk[Byte], right: Chunk[Byte]): Int =
    left.toArray.zip(right.toArray).collectFirst { case (a, b) if a != b => (a & 0xff) - (b & 0xff) }.getOrElse(left.size - right.size)

/** A store as Java's `KeyValueStore`, each call waiting for what the store does. */
private final class KeyValueView[F[_]: MonadThrow, K <: AnyRef, V >: Null <: AnyRef](
    storeName: String,
    key: Serde[F, K],
    value: Serde[F, V],
    access: () => StoreAccess[F],
    run: Runner[F]
) extends View[F, K, V, V](storeName, key, value, access) with JavaKeyValueStore[K, V]:
  protected def held(bytes: Chunk[Byte], at: Long): F[V] = valueOf(bytes)

  private def found(key: K): F[Option[V]] = keyBytes(key).flatMap(access().get).flatMap(_.traverse(valueOf))

  def get(key: K): V = run(found(key)).orNull

  def put(key: K, value: V): Unit = run(keyBytes(key).flatMap(k => Option(value).flatTraverse(valueBytes).flatMap(access().put(k, _))))

  def putIfAbsent(key: K, value: V): V =
    run(found(key).flatTap(old => (keyBytes(key), Option(value).flatTraverse(valueBytes)).tupled.flatMap(access().put).whenA(old.isEmpty))).orNull

  def putAll(entries: java.util.List[KeyValue[K, V]]): Unit = entries.forEach(entry => put(entry.key, entry.value))

  def delete(key: K): V = run(found(key).flatTap(_ => keyBytes(key).flatMap(access().put(_, None)))).orNull

  def range(from: K, to: K): KeyValueIterator[K, V]                 = new Found(run(scanned(Option(from), Option(to), None)).toList)
  override def reverseRange(from: K, to: K): KeyValueIterator[K, V] = new Found(run(scanned(Option(from), Option(to), None)).reverse.toList)
  def all(): KeyValueIterator[K, V]                                 = new Found(run(scanned(None, None, None)).toList)
  override def reverseAll(): KeyValueIterator[K, V]                 = new Found(run(scanned(None, None, None)).reverse.toList)
  override def prefixScan[PS <: JavaSerializer[P], P](prefix: P, serializer: PS): KeyValueIterator[K, V] =
    new Found(run(scanned(None, None, Some(prefixOf(prefix, serializer)))).toList)
  def approximateNumEntries(): Long = run(scanned(None, None, None)).size.toLong

/** A store as Java's `TimestampedKeyValueStore`, each value with the time it was written at. */
private final class TimestampedView[F[_]: MonadThrow, K <: AnyRef, V >: Null <: AnyRef](
    storeName: String,
    key: Serde[F, K],
    value: Serde[F, V],
    access: () => StoreAccess[F],
    run: Runner[F]
) extends View[F, K, V, ValueAndTimestamp[V]](storeName, key, value, access) with JavaTimestampedKeyValueStore[K, V]:
  protected def held(bytes: Chunk[Byte], at: Long): F[ValueAndTimestamp[V]] = valueOf(bytes).map(ValueAndTimestamp.make(_, at))

  private def found(key: K): F[Option[ValueAndTimestamp[V]]] = keyBytes(key).flatMap(access().timed).flatMap(_.traverse(held))

  private def written(key: K, value: ValueAndTimestamp[V]): F[Unit] =
    keyBytes(key).flatMap: k =>
      Option(value).fold(access().put(k, None))(timed => valueBytes(timed.value).flatMap(access().putAt(k, _, timed.timestamp)))

  def get(key: K): ValueAndTimestamp[V] = run(found(key)).orNull

  def put(key: K, value: ValueAndTimestamp[V]): Unit = run(written(key, value))

  def putIfAbsent(key: K, value: ValueAndTimestamp[V]): ValueAndTimestamp[V] =
    run(found(key).flatTap(old => written(key, value).whenA(old.isEmpty))).orNull

  def putAll(entries: java.util.List[KeyValue[K, ValueAndTimestamp[V]]]): Unit = entries.forEach(entry => put(entry.key, entry.value))

  def delete(key: K): ValueAndTimestamp[V] = run(found(key).flatTap(_ => keyBytes(key).flatMap(access().put(_, None)))).orNull

  def range(from: K, to: K): KeyValueIterator[K, ValueAndTimestamp[V]] = new Found(run(scanned(Option(from), Option(to), None)).toList)
  override def reverseRange(from: K, to: K): KeyValueIterator[K, ValueAndTimestamp[V]] =
    new Found(run(scanned(Option(from), Option(to), None)).reverse.toList)
  def all(): KeyValueIterator[K, ValueAndTimestamp[V]]                 = new Found(run(scanned(None, None, None)).toList)
  override def reverseAll(): KeyValueIterator[K, ValueAndTimestamp[V]] = new Found(run(scanned(None, None, None)).reverse.toList)
  override def prefixScan[PS <: JavaSerializer[P], P](prefix: P, serializer: PS): KeyValueIterator[K, ValueAndTimestamp[V]] =
    new Found(run(scanned(None, None, Some(prefixOf(prefix, serializer)))).toList)
  def approximateNumEntries(): Long = run(scanned(None, None, None)).size.toLong
