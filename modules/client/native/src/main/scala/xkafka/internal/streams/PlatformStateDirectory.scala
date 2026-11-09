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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.FileTime

import scala.jdk.CollectionConverters.*

import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import rocksdb.{NativeRocksdb, RocksdbWrite}

/** The state directory on Native, its stores kept by RocksDB through the shim. */
private[xkafka] object PlatformStateDirectory:
  /** The directory `application` keeps under `root`, held for the resource's life; one another instance holds is refused. */
  def apply[F[_]](root: String, application: String)(using F: Async[F]): Resource[F, StateDirectory[F]] =
    LibraryStateDirectory(new Access[F], new NioFiles[F], root, application)

  /** RocksDB's calls, each run as a call that blocks its thread. */
  private final class Access[F[_]](using F: Sync[F]) extends RocksdbAccess[F]:
    type Store = NativeRocksdb.Store

    def open(path: String, families: Vector[String]): F[Store]                   = F.blocking(NativeRocksdb.open(path, families))
    def get(store: Store, family: Int, key: Array[Byte]): F[Option[Array[Byte]]] = F.blocking(NativeRocksdb.get(store, family, key))
    def write(store: Store, writes: Vector[RocksdbWrite]): F[Unit]               = F.blocking(NativeRocksdb.write(store, writes))
    def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]]): F[Vector[(Array[Byte], Array[Byte])]] =
      F.blocking(NativeRocksdb.range(store, family, from, to, 0))
    def close(store: Store): F[Unit] = F.blocking(NativeRocksdb.close(store))

  /** The file operations, through `java.nio.file`. */
  private final class NioFiles[F[_]](using F: Sync[F]) extends DirectoryFiles[F]:
    private def path(at: String): Path = Paths.get(at)

    def exists(at: String): F[Boolean] = F.blocking(Files.exists(path(at)))
    def made(at: String): F[Unit]      = F.blocking(Files.createDirectories(path(at))).void
    def removed(at: String): F[Unit]   =
      F.blocking:
        if Files.exists(path(at)) then
          val walked = Files.walk(path(at))
          try walked.iterator.asScala.toList.reverse.foreach(Files.delete)
          finally walked.close()
    def read(at: String): F[Option[String]] =
      F.blocking(Option.when(Files.isRegularFile(path(at)))(new String(Files.readAllBytes(path(at)), StandardCharsets.UTF_8)))
    def written(at: String, text: String): F[Unit] = F.blocking(Files.write(path(at), text.getBytes(StandardCharsets.UTF_8))).void
    def directories(at: String): F[List[String]]   =
      F.blocking:
        val listed = Files.list(path(at))
        try listed.iterator.asScala.toList.filter(Files.isDirectory(_)).map(_.getFileName.toString)
        finally listed.close()
    def modified(at: String): F[Long]            = F.blocking(Files.getLastModifiedTime(path(at)).toMillis)
    def touched(at: String, when: Long): F[Unit] = F.blocking(Files.setLastModifiedTime(path(at), FileTime.fromMillis(when))).void
