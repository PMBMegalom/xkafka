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
package internal.group

import java.net.ServerSocket

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import munit.CatsEffectSuite

/** Native's sockets let the garbage collector run while a thread waits on one, which it could not if the thread did not say it was waiting. */
final class PlatformSocketsCollectorSuite extends CatsEffectSuite:
  test("a socket waiting for the broker's answer does not hold up a collection"):
    // A listener that accepts the connection and never answers, so the read waits out its timeout.
    val collection =
      Resource.make(IO.blocking(new ServerSocket(0)))(server => IO.blocking(server.close())).use: server =>
        PlatformSockets[IO].connect("127.0.0.1", server.getLocalPort, None, 3.seconds).use: socket =>
          socket.read(4).attempt.start.flatMap(reading => (IO.sleep(300.millis) *> IO.blocking(System.gc()).timed.map(_._1)) <* reading.join)
    collection.map(took => assert(took < 1.second, s"a collection took $took while a socket waited"))
