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
      new KafkaException.BackendFailure(
        "request failed",
        code = Some(ErrorCode.RequestTimedOut),
        retriable = Some(true),
        fatal = Some(false),
        cause = cause
      )

    assertEquals(error.detail, "request failed")
    assertEquals(error.code, Some(ErrorCode.RequestTimedOut))
    assertEquals(error.retriable, Some(true))
    assertEquals(error.fatal, Some(false))
    assertEquals(error.getCause, cause)
    assertEquals(error.getMessage, "Kafka backend failure [RequestTimedOut]: request failed")

  test("protocol codes classify the same on every backend"):
    assertEquals(ErrorCode.fromProtocol(3), ErrorCode.UnknownTopicOrPartition)
    assertEquals(ErrorCode.fromProtocol(1), ErrorCode.OffsetOutOfRange)
    assertEquals(ErrorCode.fromProtocol(27), ErrorCode.RebalanceInProgress)
    assertEquals(ErrorCode.fromProtocol(999), ErrorCode.Other(999))

  test("librdkafka client-side codes map onto the portable ones"):
    assertEquals(ErrorCode.fromLibrdkafka(-195), ErrorCode.NetworkException)
    assertEquals(ErrorCode.fromLibrdkafka(-187), ErrorCode.NetworkException)
    assertEquals(ErrorCode.fromLibrdkafka(-185), ErrorCode.RequestTimedOut)
    assertEquals(ErrorCode.fromLibrdkafka(-193), ErrorCode.NetworkException)
    assertEquals(ErrorCode.fromLibrdkafka(-192), ErrorCode.RequestTimedOut)
    assertEquals(ErrorCode.fromLibrdkafka(-169), ErrorCode.SaslAuthenticationFailed)
    // _OUTDATED and _FAIL have no portable meaning, so they stay raw.
    assertEquals(ErrorCode.fromLibrdkafka(-167), ErrorCode.Other(-167))
    assertEquals(ErrorCode.fromLibrdkafka(-196), ErrorCode.Other(-196))
    // Positive librdkafka codes are protocol codes, so they classify identically.
    assertEquals(ErrorCode.fromLibrdkafka(3), ErrorCode.UnknownTopicOrPartition)
    assertEquals(ErrorCode.fromLibrdkafka(-1), ErrorCode.Other(-1))

  test("invalid backend responses retain their detail"):
    val error = new KafkaException.InvalidBackendResponse("negative offset")

    assertEquals(error.detail, "negative offset")
    assertEquals(error.getMessage, "Kafka backend returned an invalid response: negative offset")
