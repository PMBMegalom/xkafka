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

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import rocksdb.{JsRocksdb, RocksdbWrite}

/** The state directory on Node.js, its stores kept by RocksDB through the shim, whose calls run on threads of their own. */
private[xkafka] object PlatformStateDirectory:
  /** The directory `application` keeps under `root`, held for the resource's life; one another instance holds is refused. */
  def apply[F[_]](root: String, application: String)(using F: Async[F]): Resource[F, StateDirectory[F]] =
    LibraryStateDirectory(new Access[F], new NodeFiles[F], root, application)

  private final class Access[F[_]](using F: Async[F]) extends RocksdbAccess[F]:
    type Store = JsRocksdb.Store

    private def awaited[A](call: => Future[A]): F[A] = F.fromFuture(F.delay(call))

    def open(path: String, families: Vector[String]): F[Store]                   = awaited(JsRocksdb.open(path, families))
    def get(store: Store, family: Int, key: Array[Byte]): F[Option[Array[Byte]]] = awaited(JsRocksdb.get(store, family, key))
    def write(store: Store, writes: Vector[RocksdbWrite]): F[Unit]               = awaited(JsRocksdb.write(store, writes))
    def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]]): F[Vector[(Array[Byte], Array[Byte])]] =
      awaited(JsRocksdb.range(store, family, from, to, 0))
    def close(store: Store): F[Unit] = awaited(JsRocksdb.close(store))

  /** The file operations, through Node's `fs`. */
  private final class NodeFiles[F[_]](using F: Sync[F]) extends DirectoryFiles[F]:
    def exists(at: String): F[Boolean]      = F.blocking(NodeFs.existsSync(at))
    def made(at: String): F[Unit]           = F.blocking(NodeFs.mkdirSync(at, js.Dynamic.literal(recursive = true))).void
    def removed(at: String): F[Unit]        = F.blocking(NodeFs.rmSync(at, js.Dynamic.literal(recursive = true, force = true)))
    def read(at: String): F[Option[String]] =
      F.blocking(Option.when(NodeFs.existsSync(at) && NodeFs.statSync(at).isFile())(NodeFs.readFileSync(at, "utf8")))
    def written(at: String, text: String): F[Unit] = F.blocking(NodeFs.writeFileSync(at, text))
    def directories(at: String): F[List[String]]   =
      F.blocking(NodeFs.readdirSync(at).toList.filter(name => NodeFs.statSync(s"$at/$name").isDirectory()))
    def modified(at: String): F[Long]            = F.blocking(NodeFs.statSync(at).mtimeMs.toLong)
    def touched(at: String, when: Long): F[Unit] = F.blocking(NodeFs.utimesSync(at, when / 1000.0, when / 1000.0))

  @js.native
  @JSImport("node:fs", JSImport.Namespace)
  private object NodeFs extends js.Object:
    def existsSync(path: String): Boolean                            = js.native
    def mkdirSync(path: String, options: js.Object): js.Any          = js.native
    def rmSync(path: String, options: js.Object): Unit               = js.native
    def readFileSync(path: String, encoding: String): String         = js.native
    def writeFileSync(path: String, data: String): Unit              = js.native
    def readdirSync(path: String): js.Array[String]                  = js.native
    def statSync(path: String): Stats                                = js.native
    def utimesSync(path: String, atime: Double, mtime: Double): Unit = js.native

  @js.native
  private trait Stats extends js.Object:
    def mtimeMs: Double        = js.native
    def isFile(): Boolean      = js.native
    def isDirectory(): Boolean = js.native
