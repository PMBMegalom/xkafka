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
package internal
package streams

import scala.collection.immutable.TreeMap

import fs2.Chunk

/** What one store of a task holds, by serialized key, in the order of the keys' bytes taken as unsigned, which is the order a store on disk keeps
  * them in, so that all of a key's windows or sessions can be found from where they start, without going through the rest.
  */
private[xkafka] final case class StoreContents(entries: TreeMap[Chunk[Byte], Stored]):
  def get(key: Chunk[Byte]): Option[Stored] = entries.get(key)

  def apply(key: Chunk[Byte]): Stored = entries(key)

  def contains(key: Chunk[Byte]): Boolean = entries.contains(key)

  def isEmpty: Boolean = entries.isEmpty

  def updated(key: Chunk[Byte], stored: Stored): StoreContents = StoreContents(entries.updated(key, stored))

  def -(key: Chunk[Byte]): StoreContents = StoreContents(entries - key)

  def --(keys: IterableOnce[Chunk[Byte]]): StoreContents = StoreContents(entries -- keys)

  def filter(keep: (Chunk[Byte], Stored) => Boolean): StoreContents = StoreContents(entries.filter(keep(_, _)))

  /** Every key and what it holds, in order. */
  def toVector: Vector[(Chunk[Byte], Stored)] = entries.toVector

  def keysIterator: Iterator[Chunk[Byte]] = entries.keysIterator

  def keySet: Set[Chunk[Byte]] = entries.keySet.toSet

  def values: Iterable[Stored] = entries.values

  def size: Int = entries.size

  /** The keys starting with `prefix` and what they hold, in order. */
  def prefixed(prefix: Chunk[Byte]): Vector[(Chunk[Byte], Stored)] = StoreContents.starting(entries, prefix).toVector

  /** What it holds once `changes` are made, a key with nothing to hold being removed. */
  def after(changes: IterableOnce[(Chunk[Byte], Option[Stored])]): StoreContents =
    StoreContents(
      changes.iterator.foldLeft(entries):
        case (held, (key, Some(stored))) => held.updated(key, stored)
        case (held, (key, None))         => held - key
    )

/** Changes to a store by serialized key, in order: what a key is to hold, or nothing where it is removed. */
private[xkafka] type StoreChanges = TreeMap[Chunk[Byte], Option[Stored]]

private[xkafka] object StoreContents:
  /** Keys by their bytes taken as unsigned, a shorter key before a longer one it starts, as Java and a store on disk order them. */
  val keys: Ordering[Chunk[Byte]] =
    (a, b) =>
      val common = math.min(a.size, b.size)
      @scala.annotation.tailrec
      def from(at: Int): Int =
        if at == common then Integer.compare(a.size, b.size)
        else
          val order = Integer.compare(a(at) & 0xff, b(at) & 0xff)
          if order != 0 then order else from(at + 1)
      from(0)

  val empty: StoreContents = StoreContents(TreeMap.empty(using keys))

  /** No changes to a store. */
  val unchanged: StoreChanges = TreeMap.empty(using keys)

  /** The entries of `entries` whose keys start with `prefix`, in order. */
  def starting[V](entries: TreeMap[Chunk[Byte], V], prefix: Chunk[Byte]): Iterator[(Chunk[Byte], V)] =
    entries.iteratorFrom(prefix).takeWhile((key, _) => key.take(prefix.size) == prefix)

  def of(entries: Iterable[(Chunk[Byte], Stored)]): StoreContents = StoreContents(TreeMap.from(entries)(using keys))
