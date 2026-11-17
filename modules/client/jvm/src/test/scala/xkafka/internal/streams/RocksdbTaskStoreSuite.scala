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

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import fs2.Chunk

/** The laws, for a store RocksDB keeps on disk, and what only a store on disk shows: that what it holds and its clock outlast closing it. */
final class RocksdbTaskStoreSuite extends TaskStoreLaws("on disk"):
  def opened(time: Option[TaskStore.TimeOf]): IO[TaskStore[IO]] =
    IO.blocking(Files.createTempDirectory("xkafka-rocksdb")).flatMap(RocksdbStore.open[IO](_, time))

  test("a store opened again from its directory holds what it held, finds it by time, and keeps its clock and position"):
    val time: TaskStore.TimeOf  = key => key(0).toLong
    def key(at: Int, last: Int) = Chunk.array(Array(at.toByte, last.toByte))
    for
      directory <- IO.blocking(Files.createTempDirectory("xkafka-rocksdb"))
      first     <- RocksdbStore.open[IO](directory, Some(time))
      _         <-
        first.write(StoreContents.unchanged.updated(key(3, 1), Some(Stored(Chunk.array("v".getBytes), 7L))).updated(key(9, 0), None), Map(0 -> 42L))
      _        <- first.close
      again    <- RocksdbStore.open[IO](directory, Some(time))
      held     <- again.all
      found    <- again.timed(0L, 5L)
      clock    <- again.clock
      position <- again.position
      _        <- again.close
    yield
      assertEquals(held, Vector(key(3, 1) -> Stored(Chunk.array("v".getBytes), 7L)))
      assertEquals(found.map(_._1), Vector(key(3, 1)))
      assertEquals(clock, 9L)
      assertEquals(position, Some(42L))

  test("a store that fails to open after RocksDB opened it lets go of it, so that it can be opened again"):
    def damage(directory: java.nio.file.Path): Unit =
      val names   = List(org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY, "time".getBytes, "meta".getBytes)
      val handles = new java.util.ArrayList[org.rocksdb.ColumnFamilyHandle]()
      val db      = org.rocksdb.RocksDB.open(directory.toString, names.map(new org.rocksdb.ColumnFamilyDescriptor(_)).asJava, handles)
      // A clock is eight bytes, so one of three cannot be read.
      try db.put(handles.get(2), "clock".getBytes, Array[Byte](1, 2, 3))
      finally
        handles.forEach(_.close())
        db.close()
    for
      directory <- IO.blocking(Files.createTempDirectory("xkafka-rocksdb"))
      _         <- RocksdbStore.open[IO](directory, None).flatMap(_.close)
      _         <- IO.blocking(damage(directory))
      first     <- RocksdbStore.open[IO](directory, None).attempt
      again     <- RocksdbStore.open[IO](directory, None).attempt
    yield
      assert(first.left.exists(_.isInstanceOf[java.nio.BufferUnderflowException]), first)
      assert(again.left.exists(_.isInstanceOf[java.nio.BufferUnderflowException]), again)
