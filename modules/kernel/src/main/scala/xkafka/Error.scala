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

sealed abstract class KafkaException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object KafkaException:
  /** A failure reported by a Kafka backend.
    *
    * `None` means that the backend did not make the corresponding classification available.
    */
  final class BackendFailure(
      val detail: String,
      val code: Option[String] = None,
      val retriable: Option[Boolean] = None,
      val fatal: Option[Boolean] = None,
      cause: Throwable = null
  ) extends KafkaException(BackendFailure.message(detail, code), cause)

  object BackendFailure:
    private def message(detail: String, code: Option[String]): String = s"Kafka backend failure${code.fold("")(value => s" [$value]")}: $detail"

  /** A backend response that cannot be represented by the portable API. */
  final class InvalidBackendResponse(val detail: String, cause: Throwable = null)
      extends KafkaException(s"Kafka backend returned an invalid response: $detail", cause)
