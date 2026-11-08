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
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import cats.effect.{Ref, Sync}
import cats.syntax.all.*
import fs2.Chunk
import org.rocksdb.{
  BlockBasedTableConfig, BloomFilter, ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, CompactionStyle, CompressionType, DBOptions,
  FlushOptions, InfoLogLevel, LRUCache, ReadOptions, RocksDB, Slice, WriteBatch, WriteOptions
}

/** A task's store kept on disk by RocksDB, with the options Kafka Streams gives its stores. It keeps three column families: what each key holds, as
  * the record's timestamp in eight big-endian bytes and then the value; for a store with time, each key again after the time it names, so that keys
  * are found by time; and the store's clock. Each write goes as one batch, without RocksDB's write-ahead log, as Kafka Streams writes, since a store
  * is filled again from its changelog; a flush writes all three families out together.
  */
private[xkafka] object RocksdbStore:
  private val ClockKey    = "clock".getBytes
  private val PositionKey = "position".getBytes

  /** Opens the store kept in `directory`, making it where there is none, finding keys by the time `time` reads if given. */
  def open[F[_]](directory: Path, time: Option[TaskStore.TimeOf])(using F: Sync[F]): F[TaskStore[F]] =
    F.blocking {
      RocksDB.loadLibrary()
      Files.createDirectories(directory)
      val cache  = new LRUCache(50L * 1024 * 1024)
      val filter = new BloomFilter()
      val table  = new BlockBasedTableConfig().setBlockCache(cache).setBlockSize(4096L).setFilterPolicy(filter)
      val family =
        new ColumnFamilyOptions().setTableFormatConfig(table).setWriteBufferSize(16L * 1024 * 1024).setCompressionType(CompressionType.NO_COMPRESSION)
          .setCompactionStyle(CompactionStyle.UNIVERSAL).setMaxWriteBufferNumber(3).setOptimizeFiltersForHits(true)
      val options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setAtomicFlush(true).setInfoLogLevel(InfoLogLevel.ERROR_LEVEL)
          .setIncreaseParallelism(math.max(Runtime.getRuntime.availableProcessors, 2))
      val names                  = List(RocksDB.DEFAULT_COLUMN_FAMILY, "time".getBytes, "meta".getBytes)
      val handles                = new java.util.ArrayList[ColumnFamilyHandle]()
      val db                     = RocksDB.open(options, directory.toString, names.map(new ColumnFamilyDescriptor(_, family)).asJava, handles)
      def long(key: Array[Byte]) = Option(db.get(handles.get(2), key)).map(ByteBuffer.wrap(_).getLong)
      Opened(db, handles.asScala.toList, List(options, family, cache, filter), time, long(ClockKey).getOrElse(WindowStore.NoTime), long(PositionKey))
    }.flatMap(opened => (Ref.of[F, Long](opened.clock), Ref.of[F, Option[Long]](opened.position)).mapN(new Store(opened, _, _)))

  /** What an open store holds open, the column families to close before the database and the options after it, and its clock and position. */
  private final case class Opened(
      db: RocksDB,
      families: List[ColumnFamilyHandle],
      options: List[AutoCloseable],
      time: Option[TaskStore.TimeOf],
      clock: Long,
      position: Option[Long]
  )

  /** A time as eight bytes that order as the times do: big-endian, with the sign bit flipped. */
  private def timeBytes(at: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(at ^ Long.MinValue).array

  private def held(bytes: Array[Byte]): Stored = Stored(Chunk.array(bytes, 8, bytes.length - 8), ByteBuffer.wrap(bytes, 0, 8).getLong)

  private def heldBytes(stored: Stored): Array[Byte] =
    ByteBuffer.allocate(8 + stored.value.size).putLong(stored.timestamp).put(stored.value.toArray).array

  private final class Store[F[_]](opened: Opened, clocked: Ref[F, Long], positioned: Ref[F, Option[Long]])(using F: Sync[F]) extends TaskStore[F]:
    import opened.{db, time}
    private val entries = opened.families(0)
    private val byTime  = opened.families(1)
    private val meta    = opened.families(2)
    private val writing = new WriteOptions().setDisableWAL(true)

    def get(key: Chunk[Byte]): F[Option[Stored]] = F.blocking(Option(db.get(entries, key.toArray)).map(held))

    def range(from: Option[Chunk[Byte]], to: Option[Chunk[Byte]]): F[Vector[(Chunk[Byte], Stored)]] =
      F.blocking:
        if (from, to).tupled.exists((first, last) => StoreContents.keys.gteq(first, last)) then Vector.empty
        else scanned(entries, from.map(_.toArray), to.map(_.toArray))((key, value) => Chunk.array(key) -> held(value))

    def timed(from: Long, to: Long): F[Vector[(Chunk[Byte], Stored)]] =
      F.blocking:
        if time.isEmpty || from > to then Vector.empty
        else
          val keys = scanned(byTime, Some(timeBytes(from)), StoreContents.past(Chunk.array(timeBytes(to))).map(_.toArray))((key, _) => key.drop(8))
          keys.zip(db.multiGetAsList(List.fill(keys.size)(entries).asJava, keys.asJava).asScala).collect:
            case (key, value) if value != null => Chunk.array(key) -> held(value)

    def clock: F[Long] = clocked.get

    def position: F[Option[Long]] = positioned.get

    def write(changes: StoreChanges, position: Option[Long]): F[Unit] =
      clocked.get.flatMap: before =>
        val after = time.fold(before)(of => changes.keysIterator.map(of).foldLeft(before)(math.max))
        F.blocking {
          val batch = new WriteBatch()
          try
            changes.foreach: (key, change) =>
              val bytes = key.toArray
              change.fold(batch.delete(entries, bytes))(stored => batch.put(entries, bytes, heldBytes(stored)))
              time.foreach: of =>
                val indexed = timeBytes(of(key)) ++ bytes
                change.fold(batch.delete(byTime, indexed))(_ => batch.put(byTime, indexed, Array.emptyByteArray))
            if after != before then batch.put(meta, ClockKey, ByteBuffer.allocate(8).putLong(after).array)
            position.foreach(at => batch.put(meta, PositionKey, ByteBuffer.allocate(8).putLong(at).array))
            db.write(writing, batch)
          finally batch.close()
        } *> clocked.set(after) *> position.traverse_(at => positioned.set(Some(at)))

    def close: F[Unit] =
      F.blocking:
        val flushing = new FlushOptions().setWaitForFlush(true)
        try db.flush(flushing, opened.families.asJava)
        finally
          flushing.close()
          writing.close()
          opened.families.foreach(_.close())
          db.close()
          opened.options.foreach(_.close())

    /** The entries of `family` from `from`, included, to `to`, left out, in RocksDB's order, which is the keys' bytes taken as unsigned. */
    private def scanned[A](family: ColumnFamilyHandle, from: Option[Array[Byte]], to: Option[Array[Byte]])(
        read: (Array[Byte], Array[Byte]) => A
    ): Vector[A] =
      val bound   = to.map(new Slice(_))
      val reading = bound.fold(new ReadOptions())(new ReadOptions().setIterateUpperBound(_))
      val cursor  = db.newIterator(family, reading)
      try
        from.fold(cursor.seekToFirst())(cursor.seek)
        Vector.unfold(())(_ => Option.when(cursor.isValid)((read(cursor.key, cursor.value), cursor.next())))
      finally
        cursor.close()
        reading.close()
        bound.foreach(_.close())
