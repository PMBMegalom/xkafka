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

import cats.effect.{Concurrent, Deferred}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Channel

/** The queue between a librdkafka consumer's single poll and whoever reads its records.
  *
  * A poll that fails records its failure before it closes the queue, and a reader that reaches the end of the queue raises a recorded failure, so a
  * reader never takes a failed poll for a stream that simply ended.
  */
private[xkafka] object PollQueue:
  /** Sends every polled value to the queue and closes it once polling ends, recording a failure first. */
  def feed[F[_]: Concurrent, A](polls: Stream[F, A], queue: Channel[F, A], failure: Deferred[F, Throwable]): Stream[F, Nothing] =
    polls.evalMap(queue.send(_).void).drain.handleErrorWith(error => Stream.exec(failure.complete(error).void) ++ Stream.raiseError[F](error))
      .onFinalize(queue.close.void)

  /** The queued values, failing once a failure is recorded.
    *
    * `concurrently` stops watching for the failure when the queue ends, so a failure recorded just before the end can go unreported by it alone. The
    * reader therefore also looks for one at the end.
    */
  def read[F[_], A](queue: Channel[F, A], failure: Deferred[F, Throwable])(using F: Concurrent[F]): Stream[F, A] =
    (queue.stream ++ Stream.exec(failure.tryGet.flatMap(_.traverse_(F.raiseError[Unit]))))
      .concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))
