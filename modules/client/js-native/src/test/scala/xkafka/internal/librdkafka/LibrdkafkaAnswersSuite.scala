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
package librdkafka

final class LibrdkafkaAnswersSuite extends munit.FunSuite:
  test("listing groups leaves out a group with an empty name, which no ConsumerGroup can name"):
    assertEquals(
      LibrdkafkaAnswers.listed(Vector("" -> 5, "workers" -> 3)),
      Vector(ConsumerGroup.from("workers").toOption.get -> ConsumerGroupState.Stable)
    )

  test("a transactional failure is retriable as its code says, whatever librdkafka's own flag"):
    val timedOut = Failures.classified("timed out", -185, fatal = 0, transactionAbortRequired = 1)
    val fenced   = Failures.classified("fenced", -144, fatal = 1, transactionAbortRequired = -1)
    assertEquals(
      (timedOut.code, timedOut.retriable, timedOut.fatal, timedOut.transactionAbortRequired),
      (Some(ErrorCode.RequestTimedOut), Some(true), Some(false), Some(true))
    )
    assertEquals(
      (fenced.code, fenced.retriable, fenced.fatal, fenced.transactionAbortRequired),
      (Some(ErrorCode.Fenced), Some(false), Some(true), None)
    )
