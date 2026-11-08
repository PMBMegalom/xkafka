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

import java.nio.ByteBuffer

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Async, Concurrent, Ref, Resource}
import cats.syntax.all.*
import fs2.Chunk
import rocksdb.RocksdbWrite

/** A platform's RocksDB calls, in `F`. */
private[xkafka] trait RocksdbAccess[F[_]]:
  type Store

  def open(path: String, families: Vector[String]): F[Store]
  def get(store: Store, family: Int, key: Array[Byte]): F[Option[Array[Byte]]]
  def write(store: Store, writes: Vector[RocksdbWrite]): F[Unit]

  /** Every entry of a family from `from`, included, to `to`, left out, in key order. */
  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]]): F[Vector[(Array[Byte], Array[Byte])]]

  /** Closes the store, writing out what it holds first. */
  def close(store: Store): F[Unit]

/** The few file operations a state directory makes, as a platform makes them. */
private[xkafka] trait DirectoryFiles[F[_]]:
  def exists(path: String): F[Boolean]

  /** Makes the directory and any directories it is in that are missing. */
  def made(path: String): F[Unit]

  /** Removes the file or directory and everything in it, where it exists. */
  def removed(path: String): F[Unit]

  def read(path: String): F[Option[String]]
  def written(path: String, text: String): F[Unit]

  /** The names of the directories directly in `path`. */
  def directories(path: String): F[List[String]]

  /** When the file or directory last changed, in milliseconds since the epoch. */
  def modified(path: String): F[Long]

  def touched(path: String, at: Long): F[Unit]

/** A task's store kept on disk by RocksDB through a platform's calls, laid out as the JVM's [[RocksdbStore]] lays its own: what each key holds, as
  * the record's timestamp in eight big-endian bytes and then the value; for a store with time, each key again after the time it names; and the
  * store's clock and position.
  */
private[xkafka] object LibraryStore:
  private val ClockKey = "clock".getBytes
  private val Position = "position-"
  private val Families = Vector("default", "time", "meta")
  private val Entries  = 0
  private val ByTime   = 1
  private val Meta     = 2

  /** Opens the store at `path`, making it where there is none, finding keys by the time `time` reads if given. */
  def open[F[_]](access: RocksdbAccess[F], path: String, time: Option[TaskStore.TimeOf])(using F: Concurrent[F]): F[TaskStore[F]] =
    access.open(path, Families).flatMap: db =>
      val prefix = Position.getBytes
      (access.get(db, Meta, ClockKey), access.range(db, Meta, Some(prefix), StoreContents.past(Chunk.array(prefix)).map(_.toArray))).tupled.flatMap:
        (clock, held) =>
          val positions = held.flatMap((key, value) => new String(key).drop(Position.length).toIntOption.map(_ -> long(value))).toMap
          (Ref.of[F, Long](clock.fold(WindowStore.NoTime)(long)), Ref.of[F, Map[Int, Long]](positions)).mapN(new Store(access, db, time, _, _))

  private def long(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  private def longBytes(at: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(at).array

  /** A time as eight bytes that order as the times do: big-endian, with the sign bit flipped. */
  private def timeBytes(at: Long): Array[Byte] = longBytes(at ^ Long.MinValue)

  private def held(bytes: Array[Byte]): Stored = Stored(Chunk.array(bytes, 8, bytes.length - 8), ByteBuffer.wrap(bytes, 0, 8).getLong)

  private def heldBytes(stored: Stored): Array[Byte] =
    ByteBuffer.allocate(8 + stored.value.size).putLong(stored.timestamp).put(stored.value.toArray).array

  private final class Store[F[_]](
      access: RocksdbAccess[F],
      db: access.Store,
      time: Option[TaskStore.TimeOf],
      clocked: Ref[F, Long],
      positioned: Ref[F, Map[Int, Long]]
  )(using F: Concurrent[F])
      extends TaskStore[F]:
    def get(key: Chunk[Byte]): F[Option[Stored]] = access.get(db, Entries, key.toArray).map(_.map(held))

    def range(from: Option[Chunk[Byte]], to: Option[Chunk[Byte]]): F[Vector[(Chunk[Byte], Stored)]] =
      if (from, to).tupled.exists((first, last) => StoreContents.keys.gteq(first, last)) then F.pure(Vector.empty)
      else access.range(db, Entries, from.map(_.toArray), to.map(_.toArray)).map(_.map((key, value) => Chunk.array(key) -> held(value)))

    def timed(from: Long, to: Long): F[Vector[(Chunk[Byte], Stored)]] =
      if time.isEmpty || from > to then F.pure(Vector.empty)
      else
        access.range(db, ByTime, Some(timeBytes(from)), StoreContents.past(Chunk.array(timeBytes(to))).map(_.toArray)).flatMap: indexed =>
          indexed.traverseFilter((key, _) => access.get(db, Entries, key.drop(8)).map(_.map(value => Chunk.array(key.drop(8)) -> held(value))))

    def clock: F[Long] = clocked.get

    def positions: F[Map[Int, Long]] = positioned.get

    def write(changes: StoreChanges, positions: Map[Int, Long]): F[Unit] =
      clocked.get.flatMap: before =>
        val after  = time.fold(before)(of => changes.keysIterator.map(of).foldLeft(before)(math.max))
        val writes =
          changes.toVector.flatMap: (key, change) =>
            val bytes = key.toArray
            val entry = change.fold(RocksdbWrite.Delete(Entries, bytes))(stored => RocksdbWrite.Put(Entries, bytes, heldBytes(stored)))
            entry +: time.toVector.map: of =>
              val indexed = timeBytes(of(key)) ++ bytes
              change.fold(RocksdbWrite.Delete(ByTime, indexed))(_ => RocksdbWrite.Put(ByTime, indexed, Array.emptyByteArray))
        val meta =
          Option.when(after != before)(RocksdbWrite.Put(Meta, ClockKey, longBytes(after))).toVector ++
            positions.toVector.map((partition, at) => RocksdbWrite.Put(Meta, s"$Position$partition".getBytes, longBytes(at)))
        access.write(db, writes ++ meta).unlessA(writes.isEmpty && meta.isEmpty) *> clocked.set(after) *> positioned.update(_ ++ positions)

    def close: F[Unit] = access.close(db)

/** A state directory kept through a platform's RocksDB calls and file operations, as the JVM's is kept: each task's stores under `<task id>/<store>`,
  * the process id in `process-id`, and what each task given up held in its directory's `positions`. RocksDB's own lock on a store kept at `.lock`
  * keeps a second instance out, in this process or another.
  */
private[xkafka] object LibraryStateDirectory:
  /** The directory `application` keeps under `root`, held for the resource's life; one another instance holds is refused. */
  def apply[F[_]](access: RocksdbAccess[F], files: DirectoryFiles[F], root: String, application: String)(using
      F: Async[F]
  ): Resource[F, StateDirectory[F]] =
    val base = s"$root/$application"
    val held =
      files.made(base) *> access.open(s"$base/.lock", Vector("default")).adaptError:
        case failure if Option(failure.getMessage).exists(_.toLowerCase.contains("lock")) =>
          new IllegalStateException(s"the state directory $base is in use by another instance of the application (${failure.getMessage})")
    Resource.make(held)(access.close).as(new Directory(access, files, base))

  private final class Directory[F[_]](access: RocksdbAccess[F], files: DirectoryFiles[F], base: String)(using F: Async[F]) extends StateDirectory[F]:
    def open(owner: String, store: String, time: Option[TaskStore.TimeOf]): F[TaskStore[F]] =
      val directory = s"$base/$owner/$store"
      files.exists(directory).flatMap: existed =>
        files.made(s"$base/$owner") *> LibraryStore.open(access, directory, time).flatMap: opened =>
          opened.position.flatMap:
            case None if existed => opened.close *> files.removed(directory) *> LibraryStore.open(access, directory, time)
            case _               => F.pure(opened)

    def processId(fresh: String): F[String] =
      val file = s"$base/process-id"
      files.read(file).flatMap(_.map(_.trim).filter(_.nonEmpty).fold(files.written(file, fresh).as(fresh))(F.pure))

    def released(owner: String, positions: Map[String, Long]): F[Unit] =
      val directory = s"$base/$owner"
      files.exists(directory).flatMap: there =>
        (F.realTime.flatMap: now =>
          files.written(s"$directory/positions", positions.toList.sorted.map((store, at) => s"$store $at\n").mkString) *>
            files.touched(directory, now.toMillis)).whenA(there)

    def held: F[Map[String, Map[String, Long]]] =
      files.directories(base).flatMap: owners =>
        owners.filterNot(_.startsWith(".")).flatTraverse: owner =>
          files.read(s"$base/$owner/positions").map(
            _.toList.map: text =>
              owner -> text.linesIterator.toList.flatMap(
                _.split(' ') match
                  case Array(store, at) => at.toLongOption.map(store -> _)
                  case _                => None
              ).toMap
          )
        .map(_.toMap)

    def cleaned(held: Set[String], delay: FiniteDuration): F[Unit] =
      (F.realTime, files.directories(base)).tupled.flatMap: (now, owners) =>
        owners.filterNot(owner => owner.startsWith(".") || owner == StateDirectory.Global || held(owner)).traverse_ : owner =>
          files.modified(s"$base/$owner").flatMap(at => files.removed(s"$base/$owner").whenA(now.toMillis - at >= delay.toMillis))
