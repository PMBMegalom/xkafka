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

import java.io.File
import java.nio.file.Files

import cats.effect.{IO, Resource}

/** The Native bindings, each blocking call run as one. The tests name the RocksDB library through XKAFKA_ROCKSDB_LIBRARY, as the build sets it. */
private[rocksdb] object PlatformRocksdb extends RocksdbCalls:
  type Store = NativeRocksdb.Store

  def open(path: String, families: Vector[String]): IO[Store] = IO.blocking(NativeRocksdb.open(path, families))

  def get(store: Store, family: Int, key: Array[Byte]): IO[Option[Array[Byte]]] = IO.blocking(NativeRocksdb.get(store, family, key))

  def write(store: Store, writes: Vector[RocksdbWrite]): IO[Unit] = IO.blocking(NativeRocksdb.write(store, writes))

  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]], limit: Int): IO[Vector[(Array[Byte], Array[Byte])]] =
    IO.blocking(NativeRocksdb.range(store, family, from, to, limit))

  def flush(store: Store): IO[Unit] = IO.blocking(NativeRocksdb.flush(store))

  def close(store: Store): IO[Unit] = IO.blocking(NativeRocksdb.close(store))

  def loadRelease(release: String): IO[Either[Throwable, String]] = IO.blocking(NativeRocksdb.load(release))

  def temporaryDirectory: Resource[IO, String] =
    Resource.make(IO.blocking(Files.createTempDirectory("xkafka-rocksdb").toFile))(directory => IO.blocking(remove(directory))).map(_.getPath)

  private def remove(file: File): Unit =
    Option(file.listFiles).foreach(_.foreach(remove))
    file.delete(): Unit
