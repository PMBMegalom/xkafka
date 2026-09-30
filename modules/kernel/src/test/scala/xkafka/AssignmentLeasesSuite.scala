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

import cats.effect.{IO, Resource}
import munit.CatsEffectSuite

final class AssignmentLeasesSuite extends CatsEffectSuite:
  test("fencing is on unless it is turned off"):
    val client   = ClientSettings.from(cats.data.NonEmptyList.one("localhost:9092")).toOption.get
    val group    = ConsumerGroup.from("workers").toOption.get
    val settings = ConsumerSettings.from(client, group, Deserializer.utf8[IO], Deserializer.utf8[IO]).toOption.get
    assert(settings.assignmentFencing)
    assert(!settings.withAssignmentFencing(false).assignmentFencing)

  test("an offset read under a lease is recorded while the lease is held, and rejected once it is revoked, even after the partition returns"):
    for
      leases  <- AssignmentLeases[IO, String](enabled = true)
      _       <- leases.assign(List("p"), replacing = false)
      earlier <- leases.current("p")
      held    <- recorded(leases, earlier)
      _       <- leases.revoke(List("p"))
      revoked <- recorded(leases, earlier)
      _       <- leases.assign(List("p"), replacing = false)
      later   <- leases.current("p")
      stale   <- recorded(leases, earlier)
      current <- recorded(leases, later)
    yield
      assertEquals(held, Right(()))
      assert(illegalGeneration(revoked), revoked.toString)
      assert(illegalGeneration(stale), stale.toString)
      assertEquals(current, Right(()))

  test("a revocation completes only once no transaction is recording an offset under the lease"):
    for
      leases  <- AssignmentLeases[IO, String](enabled = true)
      _       <- leases.assign(List("p"), replacing = false)
      lease   <- leases.current("p")
      outcome <-
        handleOf(leases, lease).use: _ =>
          for
            revoking <- leases.revoke(List("p")).start
            early    <- revoking.join.timeout(100.millis).attempt
          yield (revoking, early)
      (revoking, early) = outcome
      _ <- revoking.joinWithNever.timeout(1.second)
    yield assert(early.isLeft, "the revocation completed while an offset under its lease was still being recorded")

  test("an item read while its partition was revoked is left without a lease, and one read after a plain assignment keeps its lease"):
    for
      leases   <- AssignmentLeases[IO, String](enabled = true)
      _        <- leases.assign(List("p"), replacing = false)
      spanning <- leases.reading(leases.revoke(List("p")) *> leases.assign(List("p"), replacing = false).as(List("p")))(identity)
      assigned <- leases.reading(leases.assign(List("q"), replacing = false).as(List("q")))(identity)
    yield
      assertEquals(spanning.map(_._2), List(None))
      assert(assigned.head._2.isDefined)

  test("the eager protocol replaces the whole assignment, so a partition assigned again takes a new lease"):
    for
      leases  <- AssignmentLeases[IO, String](enabled = true)
      _       <- leases.assign(List("p", "q"), replacing = false)
      earlier <- leases.current("p")
      _       <- leases.assign(List("p"), replacing = true)
      later   <- leases.current("p")
      gone    <- leases.current("q")
    yield
      assert(earlier.isDefined && later.isDefined && earlier != later)
      assertEquals(gone, None)

  test("with fencing off every offset names the consumer's current membership and a revocation waits for nothing"):
    for
      leases  <- AssignmentLeases[IO, String](enabled = false)
      _       <- leases.assign(List("p"), replacing = false)
      read    <- leases.reading(IO.pure(List("p")))(identity)
      outcome <- handleOf(leases, None).use(_ => leases.revoke(List("p")).timeout(1.second).attempt)
      stale   <- recorded(leases, None)
    yield
      assertEquals(read.map(_._2), List(None))
      assertEquals(outcome, Right(()))
      assertEquals(stale, Right(()))

  private case object TestHandle extends GroupHandle

  private def handleOf(leases: AssignmentLeases[IO, String], lease: Option[Lease[String]]): Resource[IO, GroupHandle] =
    val membership = leases.membership(this, lease, IO.pure(TestHandle), _ => IO.unit).handle.get
    Resource.make(membership.acquire)(membership.release)

  private def recorded(leases: AssignmentLeases[IO, String], lease: Option[Lease[String]]): IO[Either[Throwable, Unit]] =
    handleOf(leases, lease).use_.attempt

  private def illegalGeneration(outcome: Either[Throwable, Unit]): Boolean =
    outcome.left.exists:
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.IllegalGeneration)
      case _                                      => false
