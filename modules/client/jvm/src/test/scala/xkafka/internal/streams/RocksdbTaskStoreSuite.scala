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
      _ <- first.write(StoreContents.unchanged.updated(key(3, 1), Some(Stored(Chunk.array("v".getBytes), 7L))).updated(key(9, 0), None), Some(42L))
      _ <- first.close
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
