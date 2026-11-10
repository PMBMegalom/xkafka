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

package xkafka.schemaregistry

import cats.data.NonEmptyList
import xkafka.Topic

/** A failure to use the registry, or to write or read a record with the schemas it keeps. */
sealed abstract class RegistryException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object RegistryException:
  /** None of the registry's URLs could be reached; `cause` is why the last could not. */
  final class Unreachable(val urls: NonEmptyList[String], cause: Throwable)
      extends RegistryException(s"the schema registry could not be reached at ${urls.toList.mkString(", ")}: ${cause.getMessage}", cause)

  /** The registry refused a request with `status`, giving its own error `code`, such as 40401 for an unknown subject or 40901 for an incompatible
    * schema, and its `reason`.
    */
  final class Refused(val status: Int, val code: Int, val reason: String)
      extends RegistryException(s"the schema registry refused the request with status $status, error $code: $reason")

  /** The registry answered with `status` in a way that is not one of its answers, as a proxy in front of it might. */
  final class Unreadable(val status: Int, val reason: String)
      extends RegistryException(s"the schema registry's answer with status $status could not be read: $reason")

  /** A value of topic `topic` could not be written, or a record of it read, with the schemas the registry keeps. */
  final class Unserializable(val topic: Topic, val reason: String)
      extends RegistryException(s"a record of topic '${topic.value}' could not be written or read with its schema: $reason")
