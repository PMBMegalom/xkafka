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

import scala.concurrent.{Future, Promise}
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport
import scala.scalajs.js.typedarray.{byteArray2Int8Array, int8Array2ByteArray, Int8Array}
import scala.util.Try

import librdkafka.{Buffer, Buffers, Completions, Koffi, KoffiLibrary}

/** The RocksDB shim's functions, declared to koffi from their prototypes. Each one that reaches RocksDB is the `async` form, which starts the call on
  * a thread of its own and reports through the callback, so none of them holds the event loop or one of Node's shared worker threads.
  */
private[rocksdb] final class RocksdbShim(library: KoffiLibrary):
  private def f(prototype: String): js.Dynamic = library.func(prototype)

  private val Done = s"${Completions.declared} *done, int64_t token"

  val identity = f("const char *xkafka_rocksdb_identity(void)")
  val load     = f("int xkafka_rocksdb_load(const char *path, const char *expected, void **error)")
  val version  = f("const char *xkafka_rocksdb_version(void)")
  val open     = f(s"int xkafka_rocksdb_async_open(const char *path, const char **families, size_t count, void **error, void **result, $Done)")
  val get      =
    f(
      "int xkafka_rocksdb_async_get(void *store, size_t family, const void *key, size_t key_length, void **value, size_t *value_length, " +
        s"int *found, void **error, int32_t *result, $Done)"
    )
  val write = f(s"int xkafka_rocksdb_async_write(void *store, const void *batch, size_t length, void **error, int32_t *result, $Done)")
  val range =
    f(
      "int xkafka_rocksdb_async_range(void *store, size_t family, const void *from, size_t from_length, const void *to, size_t to_length, " +
        s"size_t limit, void **entries, size_t *length, void **error, int32_t *result, $Done)"
    )
  val flush = f(s"int xkafka_rocksdb_async_flush(void *store, void **error, int32_t *result, $Done)")
  val close = f(s"int xkafka_rocksdb_async_close(void *store, $Done)")
  val free  = f("void xkafka_rocksdb_free(void *pointer)")

/** The prebuilt shim and RocksDB library @xkafka/rocksdb found for this platform, or why it found none. */
@js.native
@JSImport("@xkafka/rocksdb", JSImport.Default)
private[rocksdb] object PrebuiltRocksdb extends js.Object:
  val shim: String | Null    = js.native
  val library: String | Null = js.native
  val error: String | Null   = js.native

/** RocksDB on Node.js, through the shim and the RocksDB library the @xkafka/rocksdb package carries for the platform. Every call answers a `Future`
  * that completes on the event loop once the call's own thread has finished.
  */
private[xkafka] object JsRocksdb extends RocksdbLibrary:
  type Result[A] = Future[A]
  type Store     = js.Any

  /** The shim at `shim`, which must report `identity`, with the RocksDB library at `library` loaded through it, which must be release `release`. */
  def load(shim: String, library: String, identity: String, release: String): Either[RocksdbException, RocksdbShim] =
    for
      opened <-
        Try(new RocksdbShim(Koffi.load(shim))).toEither.left.map(error => new RocksdbException(s"could not load the RocksDB shim at $shim: $error"))
      reported = text(opened.identity())
      _ <- Either.cond(reported == identity, (), new RocksdbException(s"the RocksDB shim at $shim is $reported, but this xkafka needs $identity"))
      error = OutSlot()
      _ <- Either.cond(integer(opened.load(library, release, error.buffer)) == 0, (), error.take(opened))
    yield opened

  /** The shim and library the platform package carries, loaded once for the whole process, or why they could not be. */
  lazy val loaded: Either[RocksdbException, RocksdbShim] =
    for
      shim    <- Option(PrebuiltRocksdb.shim).toRight(new RocksdbException(prebuiltError))
      library <- Option(PrebuiltRocksdb.library).toRight(new RocksdbException(prebuiltError))
      opened  <- load(shim, library, BuildInfo.rocksdbShim, BuildInfo.rocksdb)
    yield opened

  def open(path: String, families: Vector[String]): Future[Store] =
    val (error, result) = (OutSlot(), OutSlot())
    started(_.open, path, js.Array(families*), families.size, error.buffer, result.buffer): shim =>
      Option(result.pointer).getOrElse(throw error.take(shim))

  def get(store: Store, family: Int, key: Array[Byte]): Future[Option[Array[Byte]]] =
    val (error, result) = (OutSlot(), OutSlot())
    val (value, length) = (OutSlot(), OutSlot())
    val found           = OutSlot()
    started(_.get, store, family, bytes(key), key.length, value.buffer, length.buffer, found.buffer, error.buffer, result.buffer): shim =>
      result.check(error, shim)
      Option.when(found.int != 0)(taking(shim, value.pointer, length.size))

  def write(store: Store, writes: Vector[RocksdbWrite]): Future[Unit] =
    RocksdbEncoding.batch(writes) match
      case Left(failure) => Future.failed(failure)
      case Right(batch)  =>
        val (error, result) = (OutSlot(), OutSlot())
        started(_.write, store, bytes(batch), batch.length, error.buffer, result.buffer)(result.check(error, _))

  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]], limit: Int): Future[Vector[(Array[Byte], Array[Byte])]] =
    val (error, result)   = (OutSlot(), OutSlot())
    val (entries, length) = (OutSlot(), OutSlot())
    val (start, startLen) = from.fold[(js.Any, Int)]((null, 0))(key => (bytes(key), key.length))
    val (end, endLen)     = to.fold[(js.Any, Int)]((null, 0))(key => (bytes(key), key.length))
    started(_.range, store, family, start, startLen, end, endLen, math.max(limit, 0), entries.buffer, length.buffer, error.buffer, result.buffer):
      shim =>
        result.check(error, shim)
        RocksdbEncoding.entries(taking(shim, entries.pointer, length.size))

  def flush(store: Store): Future[Unit] =
    val (error, result) = (OutSlot(), OutSlot())
    started(_.flush, store, error.buffer, result.buffer)(result.check(error, _))

  def close(store: Store): Future[Unit] = started(_.close, store)(_ => ())

  private def prebuiltError: String = Option(PrebuiltRocksdb.error).getOrElse("@xkafka/rocksdb found no library")

  /** Starts a call on its thread, then reads what it wrote with `read` once it reports. Its arguments are held until then. */
  private def started[A](function: RocksdbShim => js.Dynamic, arguments: js.Any*)(read: RocksdbShim => A): Future[A] =
    loaded match
      case Left(failure) => Future.failed(failure)
      case Right(shim)   =>
        val promise = Promise[A]()
        val token   = Completions.expect(arguments)(() => promise.complete(Try(read(shim))))
        val code    = function(shim)((arguments ++ Seq[js.Any](Completions.done, js.BigInt(token.toString)))*)
        if integer(code) != 0 then
          Completions.forget(token)
          promise.failure(new RocksdbException("could not start a thread for a RocksDB call"))
        promise.future

  /** A copy of the bytes in memory outside the JavaScript heap, which stays put while the call's thread reads it. Empty bytes still get a pointer,
    * since the shim reads a null pointer as no key.
    */
  private def bytes(value: Array[Byte]): Buffer =
    val buffer = Buffers.alloc(math.max(1, value.length))
    if value.nonEmpty then buffer.set(byteArray2Int8Array(value), 0)
    buffer

  /** Copies bytes the shim allocated and frees them. */
  private def taking(shim: RocksdbShim, pointer: js.Any, length: Int): Array[Byte] =
    val copy = if length == 0 then Array.emptyByteArray else int8Array2ByteArray(new Int8Array(Koffi.view(pointer, length.toDouble)))
    shim.free(pointer): Unit
    copy

  private def integer(value: js.Any): Int =
    (value: Any) match
      case number: Int => number
      case other       => throw new RocksdbException(s"the RocksDB shim answered $other where it should answer a number")

  private def text(value: js.Any): String =
    (value: Any) match
      case string: String => string
      case other          => throw new RocksdbException(s"the RocksDB shim answered $other where it should answer a string")

  /** A slot a call writes one value into: a pointer, a size or an `int`, read back once the call has reported. */
  private final class OutSlot:
    val buffer: Buffer = Buffers.alloc(8)

    def pointer: js.Any = Koffi.decode(buffer, "void *")

    def int: Int = buffer.readInt32LE(0)

    def size: Int = buffer.readBigInt64LE(0).toString.toInt

    /** Fails with the message in `error` where this slot holds a failed call's result. */
    def check(error: OutSlot, shim: RocksdbShim): Unit = if int != 0 then throw error.take(shim)

    /** The failure whose message the shim allocated into this slot, which is freed. */
    def take(shim: RocksdbShim): RocksdbException =
      val message = pointer
      if message == null then new RocksdbException("the RocksDB shim failed without saying why")
      else
        val failure = new RocksdbException(text(Koffi.decode(message, "char", -1)))
        shim.free(message): Unit
        failure

  private object OutSlot:
    def apply(): OutSlot = new OutSlot
