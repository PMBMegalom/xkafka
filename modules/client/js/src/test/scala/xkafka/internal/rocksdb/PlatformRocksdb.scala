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

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

import cats.effect.{IO, Resource}

/** The JavaScript bindings, through the shim and library the tests install from this platform's npm package. */
private[rocksdb] object PlatformRocksdb extends RocksdbCalls:
  type Store = JsRocksdb.Store

  private def await[A](call: => Future[A]): IO[A] = IO.fromFuture(IO(call))

  def open(path: String, families: Vector[String]): IO[Store] = await(JsRocksdb.open(path, families))

  def get(store: Store, family: Int, key: Array[Byte]): IO[Option[Array[Byte]]] = await(JsRocksdb.get(store, family, key))

  def write(store: Store, writes: Vector[RocksdbWrite]): IO[Unit] = await(JsRocksdb.write(store, writes))

  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]], limit: Int): IO[Vector[(Array[Byte], Array[Byte])]] =
    await(JsRocksdb.range(store, family, from, to, limit))

  def flush(store: Store): IO[Unit] = await(JsRocksdb.flush(store))

  def close(store: Store): IO[Unit] = await(JsRocksdb.close(store))

  def loadRelease(release: String): IO[Either[Throwable, String]] =
    IO:
      for
        shim    <- Option(PrebuiltRocksdb.shim).toRight(new RocksdbException("no shim"))
        library <- Option(PrebuiltRocksdb.library).toRight(new RocksdbException("no library"))
        loaded  <- JsRocksdb.load(shim, library, BuildInfo.rocksdbShim, release)
      yield (loaded.version(): Any) match
        case version: String => version
        case other           => s"$other"

  def temporaryDirectory: Resource[IO, String] =
    Resource.make(IO(NodeFs.mkdtempSync(s"${NodeOs.tmpdir()}/xkafka-rocksdb-")))(directory =>
      IO(NodeFs.rmSync(directory, js.Dynamic.literal(recursive = true, force = true)))
    )

@js.native
@JSImport("fs", JSImport.Namespace)
private object NodeFs extends js.Object:
  def mkdtempSync(prefix: String): String            = js.native
  def rmSync(path: String, options: js.Object): Unit = js.native

@js.native
@JSImport("os", JSImport.Namespace)
private object NodeOs extends js.Object:
  def tmpdir(): String = js.native
