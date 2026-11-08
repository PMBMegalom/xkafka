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

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import munit.CatsEffectSuite

/** A platform's RocksDB bindings, as the shared tests call them. */
private[rocksdb] trait RocksdbCalls:
  type Store

  def open(path: String, families: Vector[String]): IO[Store]
  def get(store: Store, family: Int, key: Array[Byte]): IO[Option[Array[Byte]]]
  def write(store: Store, writes: Vector[RocksdbWrite]): IO[Unit]
  def range(store: Store, family: Int, from: Option[Array[Byte]], to: Option[Array[Byte]], limit: Int): IO[Vector[(Array[Byte], Array[Byte])]]
  def flush(store: Store): IO[Unit]
  def close(store: Store): IO[Unit]

  /** Asks for RocksDB as release `release`, answering the release loaded or why it was refused. */
  def loadRelease(release: String): IO[Either[Throwable, String]]

  /** A new directory, removed with everything in it afterwards. */
  def temporaryDirectory: Resource[IO, String]

/** The shim's calls through each backend's bindings, with the RocksDB release this build pins. */
final class RocksdbLibrarySuite extends CatsEffectSuite:
  private val calls    = PlatformRocksdb
  private val families = Vector("default", "offsets")

  private def bytes(values: Int*): Array[Byte] = values.map(_.toByte).toArray

  private def hex(key: Array[Byte]): String = key.map(byte => f"${byte & 0xff}%02x").mkString

  private def text(value: String): Array[Byte] = value.getBytes("UTF-8")

  private def store(directory: String, names: Vector[String] = families): Resource[IO, calls.Store] =
    Resource.make(calls.open(s"$directory/store", names))(calls.close)

  /** The keys of a range, as hex, with each value as text. */
  private def listed(entries: Vector[(Array[Byte], Array[Byte])]): Vector[(String, String)] =
    entries.map((key, value) => (hex(key), new String(value, "UTF-8")))

  private def value(found: Option[Array[Byte]]): Option[String] = found.map(new String(_, "UTF-8"))

  private val keys = Vector(0x01, 0x7f, 0x80, 0xff).map(bytes(_)) :+ bytes(0xff, 0x00)

  test("opens a store with column families, each holding its own keys"):
    calls.temporaryDirectory.flatMap(store(_)).use: opened =>
      for
        _       <- calls.write(opened, Vector(RocksdbWrite.Put(0, text("key"), text("data")), RocksdbWrite.Put(1, text("key"), text("offset"))))
        data    <- calls.get(opened, 0, text("key"))
        offset  <- calls.get(opened, 1, text("key"))
        missing <- calls.get(opened, 0, text("other"))
        empty   <- calls.write(opened, Vector(RocksdbWrite.Put(0, text("empty"), Array.emptyByteArray))) *> calls.get(opened, 0, text("empty"))
      yield
        assertEquals(value(data), Some("data"))
        assertEquals(value(offset), Some("offset"))
        assertEquals(value(missing), None)
        assertEquals(value(empty), Some(""))

  test("a batch puts, deletes a key and deletes a range at once"):
    calls.temporaryDirectory.flatMap(store(_)).use: opened =>
      val puts = Vector(0x10, 0x20, 0x30, 0x40).map(key => RocksdbWrite.Put(0, bytes(key), text(f"$key%02x")))
      for
        _      <- calls.write(opened, puts)
        _      <- calls.write(opened, Vector(RocksdbWrite.Delete(0, bytes(0x40)), RocksdbWrite.DeleteRange(0, bytes(0x10), bytes(0x30))))
        _      <- calls.write(opened, Vector(RocksdbWrite.Put(0, bytes(0x20), text("again"))))
        stored <- calls.range(opened, 0, None, None, 0)
      yield assertEquals(listed(stored), Vector("20" -> "again", "30" -> "30"))

  test("a range is in unsigned byte order, within its bounds"):
    calls.temporaryDirectory.flatMap(store(_)).use: opened =>
      // Written in reverse, so the order read back is RocksDB's.
      val puts = keys.reverse.map(key => RocksdbWrite.Put(0, key, text(hex(key))))
      for
        _      <- calls.write(opened, puts)
        all    <- calls.range(opened, 0, None, None, 0)
        from   <- calls.range(opened, 0, Some(bytes(0x80)), None, 0)
        to     <- calls.range(opened, 0, None, Some(bytes(0x80)), 0)
        within <- calls.range(opened, 0, Some(bytes(0x7f)), Some(bytes(0xff)), 0)
        none   <- calls.range(opened, 0, Some(bytes(0x80)), Some(bytes(0x80)), 0)
        other  <- calls.range(opened, 1, None, None, 0)
      yield
        assertEquals(all.map((key, _) => hex(key)), Vector("01", "7f", "80", "ff", "ff00"))
        assertEquals(listed(all).map(_._2), listed(all).map(_._1))
        assertEquals(from.map((key, _) => hex(key)), Vector("80", "ff", "ff00"))
        assertEquals(to.map((key, _) => hex(key)), Vector("01", "7f"))
        assertEquals(within.map((key, _) => hex(key)), Vector("7f", "80"))
        assertEquals(none, Vector.empty)
        assertEquals(other, Vector.empty)

  test("a range is read in pages of its limit, each from the key after the last one read"):
    calls.temporaryDirectory.flatMap(store(_)).use: opened =>
      def pages(from: Option[Array[Byte]], read: Vector[Vector[String]]): IO[Vector[Vector[String]]] =
        calls.range(opened, 0, from, None, 2).flatMap: page =>
          val next = read :+ page.map((key, _) => hex(key))
          if page.size < 2 then IO.pure(next) else pages(Some(RocksdbEncoding.after(page.last._1)), next)
      calls.write(opened, keys.map(key => RocksdbWrite.Put(0, key, key))) *> pages(None, Vector.empty).map: read =>
        assertEquals(read, Vector(Vector("01", "7f"), Vector("80", "ff"), Vector("ff00")))

  test("a flushed store keeps what was written when it is closed and opened again"):
    calls.temporaryDirectory.use: directory =>
      val written =
        store(directory).use: opened =>
          calls.write(opened, Vector(RocksdbWrite.Put(0, text("kept"), text("yes")), RocksdbWrite.Put(1, text("offset"), text("42")))) *>
            calls.flush(opened)
      val reread =
        store(directory).use: opened =>
          (calls.get(opened, 0, text("kept")), calls.get(opened, 1, text("offset"))).tupled
      (written *> reread).map: (kept, offset) =>
        assertEquals(value(kept), Some("yes"))
        assertEquals(value(offset), Some("42"))

  test("a failure comes back as an exception with RocksDB's message"):
    calls.temporaryDirectory.use: directory =>
      for
        _         <- store(directory).use_
        unopened  <- store(directory, Vector("default")).use_.attempt
        unknown   <- store(directory).use(opened => calls.get(opened, 2, text("key"))).attempt
        malformed <- store(directory).use(opened => calls.write(opened, Vector(RocksdbWrite.Delete(300, text("key"))))).attempt
      yield
        assert(
          clue(unopened).left.exists(failure => failure.isInstanceOf[RocksdbException] && failure.getMessage.contains("Column families not opened"))
        )
        assertEquals(unknown.left.map(_.getMessage), Left("the store has no column family 2"))
        assertEquals(malformed.left.map(_.getMessage), Left("no column family 300"))

  test("RocksDB is the release this build pins, and another release is refused"):
    for
      pinned  <- calls.loadRelease(BuildInfo.rocksdb)
      refused <- calls.loadRelease("0.0.0")
    yield
      assertEquals(pinned, Right(BuildInfo.rocksdb))
      assert(clue(refused).left.exists(_.getMessage.contains(s"is release ${BuildInfo.rocksdb}, but xkafka needs 0.0.0")))
