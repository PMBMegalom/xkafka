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

import java.nio.channels.{FileChannel, FileLock, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.nio.file.attribute.FileTime

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

import cats.effect.{Async, Resource}
import cats.syntax.all.*

/** The state directory on the JVM, its stores kept by RocksDB. */
private[xkafka] object PlatformStateDirectory:
  /** The directory `application` keeps under `root`, held for the resource's life; one another instance holds is refused, and so is a RocksDB missing
    * from the classpath or of another release than the one xkafka is built with.
    */
  def apply[F[_]](root: String, application: String)(using F: Async[F]): Resource[F, StateDirectory[F]] =
    val base    = Paths.get(root, application)
    val present = F.blocking(scala.util.Try(Class.forName("org.rocksdb.RocksDB")).isSuccess)
    val checked =
      present.flatMap: found =>
        if !found then F.raiseError[Unit](new IllegalStateException(missing))
        else F.blocking(release()).flatMap(found => refused(found).fold(F.unit)(reason => F.raiseError(new IllegalStateException(reason))))
    Resource.eval(checked) *> Resource.make(F.blocking(locked(base)))((channel, lock) => F.blocking { lock.release(); channel.close() })
      .as(new Directory(base))

  /** Why a state directory cannot be kept without RocksDB on the classpath. */
  val missing: String = s"keeping stores on disk needs org.rocksdb:rocksdbjni:${BuildInfo.rocksdb} among the application's dependencies"

  /** Why RocksDB of release `found` cannot keep stores, if it cannot: any release other than the one xkafka is built with. */
  def refused(found: String): Option[String] =
    Option.when(found != BuildInfo.rocksdb)(s"keeping stores on disk needs RocksDB ${BuildInfo.rocksdb}, but org.rocksdb:rocksdbjni is $found")

  /** The release of the RocksDB on the classpath, its library loaded. */
  private def release(): String =
    org.rocksdb.RocksDB.loadLibrary()
    org.rocksdb.RocksDB.rocksdbVersion().toString

  private def locked(base: Path): (FileChannel, FileLock) =
    Files.createDirectories(base)
    val channel = FileChannel.open(base.resolve(".lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    val lock    =
      try Option(channel.tryLock())
      catch case _: OverlappingFileLockException => None
    lock.fold {
      channel.close()
      throw new IllegalStateException(s"the state directory $base is in use by another instance of the application")
    }(channel -> _)

  private def removed(path: Path): Unit =
    if Files.exists(path) then
      val walked = Files.walk(path)
      try walked.iterator.asScala.toList.reverse.foreach(Files.delete)
      finally walked.close()

  private final class Directory[F[_]](base: Path)(using F: Async[F]) extends StateDirectory[F]:
    def open(owner: String, store: String, time: Option[TaskStore.TimeOf]): F[TaskStore[F]] =
      val directory = base.resolve(owner).resolve(store)
      F.blocking(Files.exists(directory)).flatMap: existed =>
        RocksdbStore.open[F](directory, time).flatMap: opened =>
          opened.position.flatMap:
            case None if existed => opened.close *> F.blocking(removed(directory)) *> RocksdbStore.open[F](directory, time)
            case _               => F.pure(opened)

    def processId(fresh: String): F[String] =
      val file = base.resolve("process-id")
      F.blocking:
        if Files.exists(file) then new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim
        else
          Files.write(file, fresh.getBytes(StandardCharsets.UTF_8))
          fresh

    def released(owner: String, positions: Map[String, Long]): F[Unit] =
      F.realTime.flatMap: now =>
        F.blocking:
          val directory = base.resolve(owner)
          if Files.exists(directory) then
            val listed = positions.toList.sorted.map((store, at) => s"$store $at").mkString("", "\n", "\n")
            Files.write(directory.resolve("positions"), listed.getBytes(StandardCharsets.UTF_8))
            Files.setLastModifiedTime(directory, FileTime.fromMillis(now.toMillis)): Unit

    def held: F[Map[String, Map[String, Long]]] =
      F.blocking:
        val listed = Files.list(base)
        try listed.iterator.asScala.toList.map(_.resolve("positions")).filter(Files.isRegularFile(_)).map: file =>
            val lines = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).linesIterator.toList
            file.getParent.getFileName.toString -> lines.flatMap(
              _.split(' ') match
                case Array(store, at) => at.toLongOption.map(store -> _)
                case _                => None
            ).toMap
          .toMap
        finally listed.close()

    def cleaned(held: Set[String], delay: FiniteDuration): F[Unit] =
      F.realTime.flatMap: now =>
        F.blocking:
          val listed = Files.list(base)
          try listed.iterator.asScala.toList.filter(Files.isDirectory(_)).foreach: directory =>
              val owner = directory.getFileName.toString
              val idle  = now.toMillis - Files.getLastModifiedTime(directory).toMillis
              if owner != StateDirectory.Global && !held(owner) && idle >= delay.toMillis then removed(directory)
          finally listed.close()
