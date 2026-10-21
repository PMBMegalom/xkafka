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

import scala.concurrent.duration.*

import cats.effect.IO
import munit.CatsEffectSuite

final class RevocationHoldSuite extends CatsEffectSuite:
  private val first  = LeaseKey("events", 0)
  private val second = LeaseKey("events", 1)

  test("a revocation waits until every record handed out of its partitions is committed"):
    for
      hold    <- RevocationHold[IO](Some(1.minute))
      _       <- hold.handOut(first, 3L) *> hold.handOut(second, 7L)
      revoked <- hold.revoke(List(first, second)).timed.start
      _       <- IO.sleep(200.millis) *> hold.committed(List(first -> 3L))
      _       <- IO.sleep(200.millis) *> hold.committed(List(second -> 7L))
      outcome <- revoked.joinWithNever.timeout(10.seconds)
    yield
      val (waited, uncommitted) = outcome
      assertEquals(uncommitted, Set.empty[LeaseKey])
      assert(waited >= 400.millis, s"the revocation ended after $waited, before the second partition was committed")

  test("a revocation stops waiting at its limit and reports the partitions still uncommitted"):
    for
      hold    <- RevocationHold[IO](Some(300.millis))
      _       <- hold.handOut(first, 3L) *> hold.handOut(second, 7L) *> hold.committed(List(first -> 3L, second -> 5L))
      outcome <- hold.revoke(List(first, second)).timed.timeout(10.seconds)
    yield
      val (waited, uncommitted) = outcome
      assertEquals(uncommitted, Set(second))
      assert(waited >= 300.millis, s"the revocation ended after $waited, before its limit")

  test("a commit below one already recorded does not move a partition back"):
    for
      hold        <- RevocationHold[IO](Some(1.second))
      _           <- hold.handOut(first, 3L) *> hold.committed(List(first -> 3L)) *> hold.committed(List(first -> 1L))
      uncommitted <- hold.revoke(List(first))
    yield assertEquals(uncommitted, Set.empty[LeaseKey])

  test("a partition being revoked hands out nothing more until the group assigns it again"):
    for
      hold    <- RevocationHold[IO](Some(1.second))
      _       <- hold.handOut(first, 1L) *> hold.committed(List(first -> 1L))
      _       <- hold.revoke(List(first)) *> hold.revoked(List(first))
      revoked <- hold.handOut(first, 2L)
      _       <- hold.assign(List(first))
      again   <- hold.handOut(first, 2L)
    yield
      assert(!revoked, "a revoked partition handed out a record")
      assert(again, "a partition assigned again did not hand out its record")

  test("a partition assigned again starts afresh, so what it handed out before is not waited for"):
    for
      hold        <- RevocationHold[IO](Some(300.millis))
      _           <- hold.handOut(first, 5L) *> hold.revoke(List(first)) *> hold.revoked(List(first)) *> hold.assign(List(first))
      uncommitted <- hold.revoke(List(first)).timeout(200.millis)
    yield assertEquals(uncommitted, Set.empty[LeaseKey])

  test("a lost partition is not waited for, and hands out nothing more"):
    for
      hold   <- RevocationHold[IO](Some(1.minute))
      _      <- hold.handOut(first, 5L)
      _      <- hold.lose(List(first)).timeout(1.second)
      handed <- hold.handOut(first, 6L)
    yield assert(!handed, "a lost partition handed out a record")

  test("without a limit nothing is held and every record is handed out"):
    for
      hold        <- RevocationHold[IO](None)
      _           <- hold.handOut(first, 5L)
      uncommitted <- hold.revoke(List(first)).timeout(1.second)
      handed      <- hold.handOut(first, 6L)
    yield
      assertEquals(uncommitted, Set.empty[LeaseKey])
      assert(handed)
