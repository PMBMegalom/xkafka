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

import munit.FunSuite

final class ErrorSuite extends FunSuite:
  test("backend failures retain portable classification and their cause"):
    val cause = new RuntimeException("boom")
    val error =
      new KafkaException.BackendFailure("request failed", code = Some("TIMED_OUT"), retriable = Some(true), fatal = Some(false), cause = cause)

    assertEquals(error.detail, "request failed")
    assertEquals(error.code, Some("TIMED_OUT"))
    assertEquals(error.retriable, Some(true))
    assertEquals(error.fatal, Some(false))
    assertEquals(error.getCause, cause)
    assertEquals(error.getMessage, "Kafka backend failure [TIMED_OUT]: request failed")

  test("invalid backend responses retain their detail"):
    val error = new KafkaException.InvalidBackendResponse("negative offset")

    assertEquals(error.detail, "negative offset")
    assertEquals(error.getMessage, "Kafka backend returned an invalid response: negative offset")
