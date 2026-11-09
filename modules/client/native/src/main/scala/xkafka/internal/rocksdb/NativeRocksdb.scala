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
package rocksdb

import scala.scalanative.libc.string.memcpy
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** The RocksDB shim, which Scala Native compiles into the program wherever these bindings are reachable. It opens RocksDB with dlopen, so the program
  * links only the dynamic loader. The calls that reach RocksDB may wait on the disk, so they are marked blocking, which lets the garbage collector
  * run meanwhile; they are passed only memory the collector does not manage.
  */
@link("dl") @extern
private[rocksdb] object RocksdbBindings:
  @blocking
  def xkafka_rocksdb_load(path: CString, expected: CString, error: Ptr[CString]): CInt = extern

  def xkafka_rocksdb_version(): CString = extern

  @blocking
  def xkafka_rocksdb_open(path: CString, families: Ptr[CString], count: CSize, error: Ptr[CString]): CVoidPtr = extern

  @blocking
  def xkafka_rocksdb_get(
      store: CVoidPtr,
      family: CSize,
      key: CVoidPtr,
      keyLength: CSize,
      value: Ptr[CVoidPtr],
      valueLength: Ptr[CSize],
      found: Ptr[CInt],
      error: Ptr[CString]
  ): CInt = extern

  @blocking
  def xkafka_rocksdb_write(store: CVoidPtr, batch: CVoidPtr, length: CSize, error: Ptr[CString]): CInt = extern

  @blocking
  def xkafka_rocksdb_range(
      store: CVoidPtr,
      family: CSize,
      from: CVoidPtr,
      fromLength: CSize,
      to: CVoidPtr,
      toLength: CSize,
      limit: CSize,
      entries: Ptr[CVoidPtr],
      length: Ptr[CSize],
      error: Ptr[CString]
  ): CInt = extern

  @blocking
  def xkafka_rocksdb_flush(store: CVoidPtr, error: Ptr[CString]): CInt = extern

  @blocking
  def xkafka_rocksdb_close(store: CVoidPtr): Unit = extern

  def xkafka_rocksdb_free(pointer: CVoidPtr): Unit = extern

/** RocksDB on Native, where every call blocks the thread that makes it.
  *
  * The shim finds the RocksDB library itself: the file XKAFKA_ROCKSDB_LIBRARY names, where it is set, or else the rocksdbjni jar's library for the
  * platform, such as `librocksdbjni-linux64.so`, beside the program or through the system's library search. It is loaded with the first store opened,
  * and must be the RocksDB release this build was made with.
  */
private[xkafka] object NativeRocksdb extends RocksdbLibrary:
  import RocksdbBindings.*

  type Result[A] = A
  type Store     = CVoidPtr

  /** Loads RocksDB as the shim finds it, if it is not loaded yet, and checks it is release `expected`. Answers the loaded release. */
  def load(expected: String): Either[RocksdbException, String] =
    Zone.acquire: zone =>
      given Zone = zone
      val error  = alloc[CString](1)
      if xkafka_rocksdb_load(null, toCString(expected), error) != 0 then Left(taken(!error)) else Right(fromCString(xkafka_rocksdb_version()))

  private lazy val loaded: Either[RocksdbException, String] = load(BuildInfo.rocksdb)

  def open(path: String, families: Vector[String]): Store =
    loaded.fold(failure => throw failure, _ => ())
    Zone.acquire: zone =>
      given Zone = zone
      val names  = alloc[CString](families.size.max(1))
      val error  = alloc[CString](1)
      families.iterator.zipWithIndex.foreach((name, index) => names(index) = toCString(name))
      val store = xkafka_rocksdb_open(toCString(path), names, families.size.toUSize, error)
      if store == null then throw taken(!error) else store

  def get(store: Store, family: Int, key: Array[Byte]): Option[Array[Byte]] =
    Zone.acquire: zone =>
      given Zone  = zone
      val value   = alloc[CVoidPtr](1)
      val length  = alloc[CSize](1)
      val found   = alloc[CInt](1)
      val error   = alloc[CString](1)
      val (in, n) = copied(key)
      checked(xkafka_rocksdb_get(store, family.toUSize, in, n, value, length, found, error), error)
      Option.when(!found != 0)(taking(!value, !length))

  def write(store: Store, writes: Vector[RocksdbWrite]): Unit =
    val batch = RocksdbEncoding.batch(writes).fold(failure => throw failure, identity)
    Zone.acquire: zone =>
      given Zone      = zone
      val error       = alloc[CString](1)
      val (in, count) = copied(batch)
      checked(xkafka_rocksdb_write(store, in, count, error), error)

  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]], limit: Int): Vector[(Array[Byte], Array[Byte])] =
    Zone.acquire: zone =>
      given Zone            = zone
      val entries           = alloc[CVoidPtr](1)
      val length            = alloc[CSize](1)
      val error             = alloc[CString](1)
      val (start, startLen) = from.fold[(CVoidPtr, CSize)]((null, 0.toUSize))(copied)
      val (end, endLen)     = to.fold[(CVoidPtr, CSize)]((null, 0.toUSize))(copied)
      checked(xkafka_rocksdb_range(store, family.toUSize, start, startLen, end, endLen, math.max(limit, 0).toUSize, entries, length, error), error)
      RocksdbEncoding.entries(taking(!entries, !length))

  def flush(store: Store): Unit =
    Zone.acquire: zone =>
      given Zone = zone
      val error  = alloc[CString](1)
      checked(xkafka_rocksdb_flush(store, error), error)

  def close(store: Store): Unit = xkafka_rocksdb_close(store)

  /** A copy of the bytes outside the collected heap, with room for one byte when there are none, since the shim reads a null pointer as no key. */
  private def copied(bytes: Array[Byte])(using Zone): (CVoidPtr, CSize) =
    val pointer = alloc[Byte](math.max(1, bytes.length))
    if bytes.nonEmpty then memcpy(pointer, bytes.at(0), bytes.length.toUSize): Unit
    (pointer, bytes.length.toUSize)

  /** Copies bytes the shim allocated and frees them. */
  private def taking(pointer: CVoidPtr, length: CSize): Array[Byte] =
    val bytes = new Array[Byte](length.toInt)
    if bytes.nonEmpty then memcpy(bytes.at(0), pointer, length): Unit
    xkafka_rocksdb_free(pointer)
    bytes

  private def checked(result: CInt, error: Ptr[CString]): Unit = if result != 0 then throw taken(!error)

  /** The failure the shim reported, whose message it allocated. */
  private def taken(message: CString): RocksdbException =
    val text = if message == null then "the RocksDB shim failed without saying why" else fromCString(message)
    xkafka_rocksdb_free(message)
    new RocksdbException(text)
