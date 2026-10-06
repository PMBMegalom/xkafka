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

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Channel
import munit.CatsEffectSuite

final class PollQueueSuite extends CatsEffectSuite:
  private val boom = new RuntimeException("the poll failed")

  private val queued: IO[(Channel[IO, Int], Deferred[IO, Throwable])] = (Channel.bounded[IO, Int](8), Deferred[IO, Throwable]).tupled

  test("a poll that stops ends the records without a failure"):
    queued.flatMap: (queue, failure) =>
      PollQueue.feed(Stream(1, 2), queue, failure).compile.drain *> PollQueue.read(queue, failure).compile.toList.map(assertEquals(_, List(1, 2)))

  // Each ordering problem below only shows up on some schedules, so every case runs many times in parallel, which is what exposes it on one machine.
  test("a failed poll records its failure before its queue closes"):
    queued.flatMap: (queue, failure) =>
      PollQueue.feed(Stream(1) ++ Stream.raiseError[IO](boom), queue, failure).compile.drain.attempt.start *> queue.closed *> failure.tryGet
    .parReplicateA(2000)
      .map(recorded => assert(recorded.forall(_.contains(boom)), s"some queues closed before their failure was recorded: $recorded"))

  test("a reader reaching the end of a failed poll's queue fails with that failure"):
    queued.flatMap: (queue, failure) =>
      PollQueue.feed(Stream(1, 2) ++ Stream.raiseError[IO](boom), queue, failure).compile.drain.attempt *> PollQueue.read(queue, failure)
        .compile.toList.attempt
    .parReplicateA(2000).map(outcomes => assert(outcomes.forall(_ == Left(boom)), s"some readers finished without the failure: ${outcomes.distinct}"))

  test("a reader running alongside a failing poll fails with that failure"):
    queued.flatMap: (queue, failure) =>
      PollQueue.read(queue, failure).compile.toList.attempt
        .both(PollQueue.feed(Stream(1) ++ Stream.raiseError[IO](boom), queue, failure).compile.drain.attempt).map(_._1)
    .parReplicateA(2000).map(outcomes => assert(outcomes.forall(_ == Left(boom)), s"some readers finished without the failure: ${outcomes.distinct}"))
