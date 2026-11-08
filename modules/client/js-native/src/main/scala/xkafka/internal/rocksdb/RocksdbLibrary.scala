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

package xkafka.internal.rocksdb

/** RocksDB as the JavaScript and Native backends reach it, through the C shim `xkafka_rocksdb.c`.
  *
  * A store holds column families, which every call names by their index in the list the store was opened with. Keys and values are bytes, and RocksDB
  * orders keys by comparing them as unsigned bytes.
  *
  * These are the shim's primitives and nothing more: on Native a call blocks the thread that makes it, and on JavaScript it runs on a thread of its
  * own and answers through a `Future`. A call fails with a [[RocksdbException]] carrying RocksDB's own message. A store must not be used once it is
  * closed, nor closed while another call on it is running.
  */
private[xkafka] trait RocksdbLibrary:
  /** What a call answers: the value itself on Native, a `Future` of it on JavaScript. */
  type Result[A]

  /** An open store. */
  type Store

  /** Opens the store at `path`, creating its directory, the store and any of `families` that are missing. The default family is always opened, as
    * RocksDB requires, and reached by index only where `families` names it.
    */
  def open(path: String, families: Vector[String]): Result[Store]

  /** The value `key` has in a family, if any. */
  def get(store: Store, family: Int, key: Array[Byte]): Result[Option[Array[Byte]]]

  /** Applies the writes as one atomic batch. Writes skip the write-ahead log, so they are durable only once the store is flushed. */
  def write(store: Store, writes: Vector[RocksdbWrite]): Result[Unit]

  /** The entries of a family from `from`, included, to `to`, excluded, in key order, at most `limit` of them, or every one for a limit of 0. A range
    * is read in pages by asking again from [[RocksdbEncoding.after]] the last key, until fewer than `limit` entries come back.
    */
  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]], limit: Int): Result[Vector[(Array[Byte], Array[Byte])]]

  /** Writes every family's memtables to disk and waits for it, which makes what was written durable. */
  def flush(store: Store): Result[Unit]

  /** Closes the store, which flushes what was written since the last flush, and releases it. */
  def close(store: Store): Result[Unit]

/** One write of a batch. */
private[xkafka] enum RocksdbWrite:
  /** Sets `key` to `value`. */
  case Put(family: Int, key: Array[Byte], value: Array[Byte])

  /** Removes `key`. */
  case Delete(family: Int, key: Array[Byte])

  /** Removes every key from `from`, included, to `to`, excluded. */
  case DeleteRange(family: Int, from: Array[Byte], to: Array[Byte])

/** A failure RocksDB or the shim reported, with its message. */
private[xkafka] final class RocksdbException(message: String) extends RuntimeException(message)

/** The byte layouts the shim takes batches in and answers ranges in, which `xkafka_rocksdb.c` documents. */
private[xkafka] object RocksdbEncoding:
  private val PutOperation         = 1
  private val DeleteOperation      = 2
  private val DeleteRangeOperation = 3

  /** The most column families a store can have, since a batch names a family in one byte. */
  val MaxFamilies: Int = 255

  /** The key just after `key` in RocksDB's order, from which the next page of a range starts. */
  def after(key: Array[Byte]): Array[Byte] = key :+ 0.toByte

  /** A batch as the shim takes it, or the reason it cannot be one. */
  def batch(writes: Vector[RocksdbWrite]): Either[RocksdbException, Array[Byte]] =
    writes.find(write => family(write) < 0 || family(write) >= MaxFamilies) match
      case Some(write) => Left(new RocksdbException(s"no column family ${family(write)}"))
      case None        =>
        val out = new java.io.ByteArrayOutputStream()
        writes.foreach:
          case RocksdbWrite.Put(family, key, value)       => operation(out, PutOperation, family, Vector(key, value))
          case RocksdbWrite.Delete(family, key)           => operation(out, DeleteOperation, family, Vector(key))
          case RocksdbWrite.DeleteRange(family, from, to) => operation(out, DeleteRangeOperation, family, Vector(from, to))
        Right(out.toByteArray)

  /** The entries of a range as the shim answers them: each key, then its value, each preceded by its length. */
  def entries(bytes: Array[Byte]): Vector[(Array[Byte], Array[Byte])] =
    def read(at: Int): (Array[Byte], Int) =
      val length = ((bytes(at) & 0xff) << 24) | ((bytes(at + 1) & 0xff) << 16) | ((bytes(at + 2) & 0xff) << 8) | (bytes(at + 3) & 0xff)
      (java.util.Arrays.copyOfRange(bytes, at + 4, at + 4 + length), at + 4 + length)
    Iterator.unfold(0): at =>
      Option.when(at < bytes.length):
        val (key, afterKey)     = read(at)
        val (value, afterValue) = read(afterKey)
        ((key, value), afterValue)
    .toVector

  private def family(write: RocksdbWrite): Int =
    write match
      case RocksdbWrite.Put(family, _, _)         => family
      case RocksdbWrite.Delete(family, _)         => family
      case RocksdbWrite.DeleteRange(family, _, _) => family

  private def operation(out: java.io.ByteArrayOutputStream, code: Int, family: Int, strings: Vector[Array[Byte]]): Unit =
    out.write(code)
    out.write(family)
    strings.foreach: bytes =>
      out.write(bytes.length >>> 24)
      out.write(bytes.length >>> 16)
      out.write(bytes.length >>> 8)
      out.write(bytes.length)
      out.write(bytes, 0, bytes.length)
