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

import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime

import scala.concurrent.duration.*

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite

/** The state directory on the JVM: one instance at a time, a process id kept between runs, stores emptied where they say nothing of how far they hold
  * their changelog, and directories of tasks given up removed once the cleanup delay has passed.
  */
final class StateDirectorySuite extends CatsEffectSuite:
  private val root = IO.blocking(Files.createTempDirectory("xkafka-state"))

  private def value(text: String) = Some(Stored(Chunk.array(text.getBytes), 1L))

  test("a second instance of the application is refused the directory while the first holds it, and given it once the first lets go"):
    root.flatMap: at =>
      PlatformStateDirectory[IO](at.toString, "app").use(_ => PlatformStateDirectory[IO](at.toString, "app").use_.attempt).flatMap: second =>
        PlatformStateDirectory[IO](at.toString, "app").use_.as:
          assert(second.left.exists(_.getMessage.contains("in use by another instance")), second)

  test("RocksDB of another release than xkafka's is refused, saying which, and its own release is used"):
    assertEquals(PlatformStateDirectory.refused(BuildInfo.rocksdb), None)
    assertEquals(
      PlatformStateDirectory.refused("9.10.0"),
      Some(s"keeping stores on disk needs RocksDB ${BuildInfo.rocksdb}, but org.rocksdb:rocksdbjni is 9.10.0")
    )
    assertEquals(BuildInfo.rocksdb, "10.1.3")
    assert(PlatformStateDirectory.missing.contains("org.rocksdb:rocksdbjni:10.1.3"), PlatformStateDirectory.missing)

  test("the process id is the first run's in every later run"):
    root.flatMap: at =>
      for
        first  <- PlatformStateDirectory[IO](at.toString, "app").use(_.processId("first"))
        second <- PlatformStateDirectory[IO](at.toString, "app").use(_.processId("second"))
      yield assertEquals((first, second), ("first", "first"))

  test("a store opened again keeps what it held with its position, and one holding data without a position is emptied"):
    val key = Chunk.array("k".getBytes)
    root.flatMap: at =>
      PlatformStateDirectory[IO](at.toString, "app").use: directory =>
        for
          positioned <- directory.open("0_1", "counts", None)
          _          <- positioned.write(StoreContents.unchanged.updated(key, value("kept")), Map(0 -> 3L))
          _          <- positioned.close
          unplaced   <- directory.open("0_2", "counts", None)
          _          <- unplaced.write(StoreContents.unchanged.updated(key, value("lost")))
          _          <- unplaced.close
          again      <- directory.open("0_1", "counts", None)
          kept       <- again.get(key)
          position   <- again.position
          _          <- again.close
          emptied    <- directory.open("0_2", "counts", None)
          lost       <- emptied.get(key)
          _          <- emptied.close
        yield
          assertEquals((kept, position), (value("kept"), Some(3L)))
          assertEquals(lost, None)

  test(
    "only the directories of tasks not held here, given up longer ago than the delay, are removed, never the global tables', and a task given up keeps where its stores are"
  ):
    def made(at: Path, owner: String, idle: FiniteDuration) =
      IO.realTime.flatMap(now =>
        IO.blocking(Files.setLastModifiedTime(Files.createDirectories(at.resolve("app").resolve(owner)), FileTime.fromMillis((now - idle).toMillis)))
      )
    root.flatMap: at =>
      PlatformStateDirectory[IO](at.toString, "app").use: directory =>
        for
          _    <- made(at, "0_1", 11.minutes)
          _    <- made(at, "0_2", 11.minutes)
          _    <- made(at, "0_3", 1.minute)
          _    <- made(at, StateDirectory.Global, 11.minutes)
          _    <- made(at, "0_4", 11.minutes) *> directory.released("0_4", Map("counts" -> 7L, "sums" -> 2L))
          held <- directory.held
          _    <- directory.cleaned(Set("0_2"), 10.minutes)
          left <- IO.blocking(List("0_1", "0_2", "0_3", "0_4", StateDirectory.Global).filter(owner => Files.exists(at.resolve("app").resolve(owner))))
        yield
          assertEquals(left, List("0_2", "0_3", "0_4", StateDirectory.Global))
          assertEquals(held, Map("0_4" -> Map("counts" -> 7L, "sums" -> 2L)))
