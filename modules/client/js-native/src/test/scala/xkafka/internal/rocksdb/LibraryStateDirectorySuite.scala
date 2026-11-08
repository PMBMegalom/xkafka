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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.{AnyFixture, CatsEffectSuite}
import streams.{PlatformStateDirectory, StateDirectory, StoreContents, Stored, TaskStore, TaskStoreLaws}

/** The laws, for a store on disk through this platform's RocksDB calls. */
final class LibraryTaskStoreSuite extends TaskStoreLaws("on disk"):
  private val directory =
    ResourceSuiteLocalFixture("directory", PlatformRocksdb.temporaryDirectory.flatMap(root => PlatformStateDirectory[IO](root, "app")))

  override def munitFixtures: Seq[AnyFixture[?]] = List(directory) ++ super.munitFixtures

  private val made = new AtomicInteger(0)

  def opened(time: Option[TaskStore.TimeOf]): IO[TaskStore[IO]] = IO(made.incrementAndGet).flatMap(n => directory().open(s"0_$n", "store", time))

/** The state directory on this platform: one instance at a time, a process id kept between runs, stores emptied where they say nothing of how far
  * they hold their changelog, and directories of tasks given up removed once the cleanup delay has passed.
  */
final class LibraryStateDirectorySuite extends CatsEffectSuite:
  private def value(text: String) = Some(Stored(Chunk.array(text.getBytes), 1L))

  test("a second instance of the application is refused the directory while the first holds it, and given it once the first lets go"):
    PlatformRocksdb.temporaryDirectory.use: root =>
      PlatformStateDirectory[IO](root, "app").use(_ => PlatformStateDirectory[IO](root, "app").use_.attempt).flatMap: second =>
        PlatformStateDirectory[IO](root, "app").use_.as(assert(second.left.exists(_.getMessage.contains("in use by another instance")), second))

  test("the process id is the first run's in every later run"):
    PlatformRocksdb.temporaryDirectory.use: root =>
      for
        first  <- PlatformStateDirectory[IO](root, "app").use(_.processId("first"))
        second <- PlatformStateDirectory[IO](root, "app").use(_.processId("second"))
      yield assertEquals((first, second), ("first", "first"))

  test("a store opened again keeps what it held with its position and clock, and one holding data without a position is emptied"):
    val key                    = Chunk.array("k".getBytes)
    val time: TaskStore.TimeOf = _ => 9L
    PlatformRocksdb.temporaryDirectory.use: root =>
      PlatformStateDirectory[IO](root, "app").use: directory =>
        for
          positioned <- directory.open("0_1", "counts", Some(time))
          _          <- positioned.write(StoreContents.unchanged.updated(key, value("kept")), Some(3L))
          _          <- positioned.close
          unplaced   <- directory.open("0_2", "counts", None)
          _          <- unplaced.write(StoreContents.unchanged.updated(key, value("lost")))
          _          <- unplaced.close
          again      <- directory.open("0_1", "counts", Some(time))
          kept       <- again.get(key)
          position   <- again.position
          clock      <- again.clock
          _          <- again.close
          emptied    <- directory.open("0_2", "counts", None)
          lost       <- emptied.get(key)
          _          <- emptied.close
        yield
          assertEquals((kept, position, clock), (value("kept"), Some(3L), 9L))
          assertEquals(lost, None)

  test("tasks given up keep where their stores are, and only those not held here are removed once the delay has passed, never the global tables'"):
    PlatformRocksdb.temporaryDirectory.use: root =>
      PlatformStateDirectory[IO](root, "app").use: directory =>
        def made(owner: String) = directory.open(owner, "counts", None).flatMap(_.close)
        for
          _       <- List("0_1", "0_2", StateDirectory.Global).traverse_(made)
          _       <- directory.released("0_1", Map("counts" -> 7L)) *> directory.released("0_2", Map("counts" -> 2L))
          held    <- directory.held
          _       <- directory.cleaned(Set("0_2"), 1.hour)
          recent  <- directory.held
          _       <- directory.cleaned(Set("0_2"), Duration.Zero)
          cleaned <- directory.held
          global  <- directory.open(StateDirectory.Global, "counts", None).flatMap(store => store.position.flatTap(_ => store.close))
        yield
          assertEquals(held, Map("0_1" -> Map("counts" -> 7L), "0_2" -> Map("counts" -> 2L)))
          assertEquals(recent, held)
          assertEquals(cleaned, Map("0_2" -> Map("counts" -> 2L)))
          assertEquals(global, None)
