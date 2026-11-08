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

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Chunk
import munit.{AnyFixture, CatsEffectSuite}

/** What every kind of task store does, run for each kind by a suite extending this one with how to open a store of it. */
abstract class TaskStoreLaws(kind: String) extends CatsEffectSuite:
  /** A new, empty store of this kind, reading times with `time` if given. */
  def opened(time: Option[TaskStore.TimeOf]): IO[TaskStore[IO]]

  /** The stores these tests opened, each closed once the suite is done, as a store on disk holds files and threads open. */
  private val stores =
    ResourceSuiteLocalFixture(
      "stores",
      Resource.make(IO(new java.util.concurrent.ConcurrentLinkedQueue[TaskStore[IO]]()))(opened =>
        scala.jdk.CollectionConverters.IteratorHasAsScala(opened.iterator).asScala.toList.traverse_(_.close)
      )
    )

  override def munitFixtures: Seq[AnyFixture[?]] = super.munitFixtures ++ List(stores)

  private def fresh(time: Option[TaskStore.TimeOf]): IO[TaskStore[IO]] = opened(time).flatTap(made => IO(stores().add(made)).void)

  private def bytes(values: Int*): Chunk[Byte] = Chunk.array(values.map(_.toByte).toArray)

  private def held(at: Long) = Stored(bytes(at.toInt), at)

  private def filled(keys: Chunk[Byte]*): IO[TaskStore[IO]] =
    fresh(None).flatTap(_.write(StoreContents.unchanged ++ keys.zipWithIndex.map((key, at) => key -> Some(held(at.toLong)))))

  test(s"$kind: a range holds the keys from its start to before its end, by their bytes taken as unsigned, either end open"):
    val keys = List(bytes(0x01), bytes(0x01, 0x00), bytes(0x02), bytes(0x7f), bytes(0x80), bytes(0xff), bytes(0xff, 0xff))
    filled(keys.reverse*).flatMap: store =>
      (
        store.range(Some(bytes(0x01, 0x00)), Some(bytes(0x80))),
        store.range(None, Some(bytes(0x02))),
        store.range(Some(bytes(0x80)), None),
        store.range(Some(bytes(0x80)), Some(bytes(0x02))),
        store.all
      ).mapN: (middle, start, end, backwards, all) =>
        assertEquals(middle.map(_._1), Vector(bytes(0x01, 0x00), bytes(0x02), bytes(0x7f)))
        assertEquals(start.map(_._1), Vector(bytes(0x01), bytes(0x01, 0x00)))
        assertEquals(end.map(_._1), Vector(bytes(0x80), bytes(0xff), bytes(0xff, 0xff)))
        assertEquals(backwards, Vector.empty)
        assertEquals(all.map(_._1), keys.toVector)

  test(s"$kind: a prefix finds the keys starting with it, a prefix ending in 0xff included, and all of them for a prefix of 0xff alone"):
    filled(bytes(0x01, 0xff), bytes(0x01, 0xff, 0x00), bytes(0x02), bytes(0xff), bytes(0xff, 0x01)).flatMap: store =>
      (store.prefixed(bytes(0x01, 0xff)), store.prefixed(bytes(0xff)), store.prefixed(bytes(0x03))).mapN: (ending, last, none) =>
        assertEquals(ending.map(_._1), Vector(bytes(0x01, 0xff), bytes(0x01, 0xff, 0x00)))
        assertEquals(last.map(_._1), Vector(bytes(0xff), bytes(0xff, 0x01)))
        assertEquals(none, Vector.empty)

  test(s"$kind: a write applies as one, a key with nothing to hold removed, and what a key holds is its latest"):
    for
      store <- filled(bytes(1), bytes(2))
      _     <- store.write(StoreContents.unchanged.updated(bytes(1), None).updated(bytes(2), Some(held(9L))).updated(bytes(3), Some(held(8L))))
      one   <- store.get(bytes(1))
      two   <- store.get(bytes(2))
      all   <- store.all
    yield
      assertEquals(one, None)
      assertEquals(two, Some(held(9L)))
      assertEquals(all, Vector(bytes(2) -> held(9L), bytes(3) -> held(8L)))

  /** A key whose time is its first byte, then a key byte. */
  private def timedKey(time: Int, key: Int) = bytes(time, key)

  private val firstByte: TaskStore.TimeOf = key => key(0).toLong

  test(s"$kind: keys are found by the time they name, both ends included, in order of time and then of key, removed keys left out"):
    for
      store <- fresh(Some(firstByte))
      _     <- store.write(StoreContents.unchanged ++ List(timedKey(5, 2), timedKey(3, 9), timedKey(5, 1), timedKey(7, 0)).map(_ -> Some(held(1L))))
      _     <- store.write(StoreContents.unchanged.updated(timedKey(7, 0), None))
      some  <- store.timed(3L, 5L)
      later <- store.timed(4L, 100L)
      none  <- store.timed(6L, 5L)
    yield
      assertEquals(some.map(_._1), Vector(timedKey(3, 9), timedKey(5, 1), timedKey(5, 2)))
      assertEquals(later.map(_._1), Vector(timedKey(5, 1), timedKey(5, 2)))
      assertEquals(none, Vector.empty)

  test(s"$kind: keys with times before zero come first, and a read from the earliest time finds every key up to its end"):
    for
      store <- fresh(Some(firstByte))
      // A first byte of 0xf0 names the time -16.
      _ <- store.write(StoreContents.unchanged ++ List(timedKey(5, 0), timedKey(0xf0, 0), timedKey(3, 0), timedKey(9, 0)).map(_ -> Some(held(1L))))
      early <- store.timed(Long.MinValue, 5L)
      all   <- store.timed(Long.MinValue, Long.MaxValue)
    yield
      assertEquals(early.map(_._1), Vector(timedKey(0xf0, 0), timedKey(3, 0), timedKey(5, 0)))
      assertEquals(all.map(_._1), Vector(timedKey(0xf0, 0), timedKey(3, 0), timedKey(5, 0), timedKey(9, 0)))

  test(s"$kind: a store's clock is the latest time written to it, a removal's included, and never goes back"):
    for
      store   <- fresh(Some(firstByte))
      before  <- store.clock
      _       <- store.write(StoreContents.unchanged.updated(timedKey(4, 0), Some(held(1L))))
      written <- store.clock
      _       <- store.write(StoreContents.unchanged.updated(timedKey(9, 0), None))
      removed <- store.clock
      _       <- store.write(StoreContents.unchanged.updated(timedKey(4, 0), None).updated(timedKey(2, 0), Some(held(1L))))
      after   <- store.clock
      plain   <- fresh(None).flatTap(_.write(StoreContents.unchanged.updated(timedKey(4, 0), Some(held(1L))))).flatMap(_.clock)
    yield assertEquals(List(before, written, removed, after, plain), List(WindowStore.NoTime, 4L, 9L, 9L, WindowStore.NoTime))

  test(s"$kind: a store records the position a write gives with it, keeps it through writes that give none, and starts with none"):
    for
      store <- fresh(None)
      none  <- store.position
      _     <- store.write(StoreContents.unchanged.updated(bytes(1), Some(held(1L))), Some(5L))
      five  <- store.position
      _     <- store.write(StoreContents.unchanged.updated(bytes(2), Some(held(2L))))
      kept  <- store.position
      _     <- store.write(StoreContents.unchanged, Some(9L))
      nine  <- store.position
      all   <- store.all
    yield
      assertEquals(List(none, five, kept, nine), List(None, Some(5L), Some(5L), Some(9L)))
      assertEquals(all.map(_._1), Vector(bytes(1), bytes(2)))

/** The laws, for a store in memory. */
final class InMemoryTaskStoreSuite extends TaskStoreLaws("in memory"):
  def opened(time: Option[TaskStore.TimeOf]): IO[TaskStore[IO]] = TaskStore.inMemory[IO](StoreContents.empty, time)
