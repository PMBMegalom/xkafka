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
package kafkaclients

import munit.FunSuite
import org.apache.kafka.common.KafkaException as JavaKafkaException
import org.apache.kafka.common.errors.{
  ClusterAuthorizationException, GroupAuthorizationException, NotEnoughReplicasException, ProducerFencedException, RecordTooLargeException,
  TopicAuthorizationException, TransactionalIdAuthorizationException
}

/** Whether the Java client's failures are fatal: true for those it holds fatal, false for those it retries, and unknown for the rest. */
final class JavaFailuresSuite extends FunSuite:
  private def portable(error: Throwable): KafkaException.BackendFailure =
    JavaFailures.portable(error) match
      case failure: KafkaException.BackendFailure => failure
      case other                                  => fail(s"expected a backend failure, got $other")

  test("a failure the Java producer holds fatal is fatal, one it retries is not, and of any other it is not known"):
    assertEquals(portable(new TransactionalIdAuthorizationException("refused")).fatal, Some(true))
    assertEquals(portable(new ClusterAuthorizationException("refused")).fatal, Some(true))
    assertEquals(portable(new ProducerFencedException("fenced")).fatal, Some(true))
    assertEquals(portable(new NotEnoughReplicasException("later")).fatal, Some(false))
    assertEquals(portable(new RecordTooLargeException("large")).fatal, None)

  test("an authorization failure is reported by its own code, though Kafka's class for it extends its configuration failure"):
    assertEquals(portable(new TopicAuthorizationException("refused")).code, Some(ErrorCode.TopicAuthorizationFailed))
    assertEquals(portable(new GroupAuthorizationException("refused")).code, Some(ErrorCode.GroupAuthorizationFailed))
    assertEquals(portable(new ClusterAuthorizationException("refused")).code, Some(ErrorCode.ClusterAuthorizationFailed))

  test("the error state a failure left the producer in is reported as that failure, with its code and fatal"):
    val wrapped =
      portable(new JavaKafkaException(
        "Cannot execute transactional method because we are in an error state",
        new TransactionalIdAuthorizationException("refused")
      ))
    assertEquals(wrapped.code, Some(ErrorCode.Other(53)))
    assertEquals(wrapped.fatal, Some(true))
