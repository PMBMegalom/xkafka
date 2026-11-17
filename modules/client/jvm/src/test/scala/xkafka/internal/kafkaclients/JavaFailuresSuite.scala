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
  ClusterAuthorizationException, GroupAuthorizationException, InvalidTxnStateException, NotEnoughReplicasException, ProducerFencedException,
  RecordTooLargeException, TimeoutException, TopicAuthorizationException, TransactionAbortableException, TransactionalIdAuthorizationException,
  UnsupportedVersionException
}

/** Whether the Java client's failures are fatal: true for those it holds fatal, false for those it retries, and unknown for the rest. */
final class JavaFailuresSuite extends FunSuite:
  private def classified(classify: PartialFunction[Throwable, Throwable])(error: Throwable): KafkaException.BackendFailure =
    classify(error) match
      case failure: KafkaException.BackendFailure => failure
      case other                                  => fail(s"expected a backend failure, got $other")

  private def portable(error: Throwable) = classified(JavaFailures.portable)(error)
  private def produced(error: Throwable) = classified(JavaFailures.produced)(error)

  private def errorState(cause: Throwable) = new JavaKafkaException("Cannot execute transactional method because we are in an error state", cause)

  test("a failure the Java producer holds fatal is fatal, one it retries is not, and of any other it is not known"):
    assertEquals(produced(new TransactionalIdAuthorizationException("refused")).fatal, Some(true))
    assertEquals(produced(new ClusterAuthorizationException("refused")).fatal, Some(true))
    assertEquals(produced(new ProducerFencedException("fenced")).fatal, Some(true))
    assertEquals(produced(new UnsupportedVersionException("old")).fatal, Some(true))
    assertEquals(produced(new NotEnoughReplicasException("later")).fatal, Some(false))
    assertEquals(produced(new RecordTooLargeException("large")).fatal, None)

  test("an administration client or a consumer recovers from the failures a producer holds fatal, so none of theirs is known fatal"):
    assertEquals(portable(new UnsupportedVersionException("old")).fatal, None)
    assertEquals(portable(new ClusterAuthorizationException("refused")).fatal, None)
    assertEquals(portable(new NotEnoughReplicasException("later")).fatal, Some(false))

  test("an authorization failure is reported by its own code, though Kafka's class for it extends its configuration failure"):
    assertEquals(portable(new TopicAuthorizationException("refused")).code, Some(ErrorCode.TopicAuthorizationFailed))
    assertEquals(portable(new GroupAuthorizationException("refused")).code, Some(ErrorCode.GroupAuthorizationFailed))
    assertEquals(portable(new ClusterAuthorizationException("refused")).code, Some(ErrorCode.ClusterAuthorizationFailed))

  test("the error state a fatal failure left the producer in is reported as that failure, with its code, fatal and no abort to make"):
    val wrapped = produced(errorState(new TransactionalIdAuthorizationException("refused")))
    assertEquals(
      (wrapped.code, wrapped.retriable, wrapped.fatal, wrapped.transactionAbortRequired),
      (Some(ErrorCode.Other(53)), Some(false), Some(true), Some(false))
    )

  test("an error state the Java client entered for an answer it did not expect, and an abort that failed, are fatal, as it holds them"):
    val unexpected = produced(errorState(new JavaKafkaException("Unhandled error in EndTxnResponse: x")))
    assertEquals((unexpected.retriable, unexpected.fatal, unexpected.transactionAbortRequired), (Some(false), Some(true), Some(false)))
    val abort = produced(new JavaKafkaException("Failed to abort transaction", new TransactionAbortableException("x")))
    assertEquals((abort.retriable, abort.fatal, abort.transactionAbortRequired), (Some(false), Some(true), Some(false)))

  /** A transactional producer the Java client has made, connected to nothing, whose transaction manager the test can make fatal. */
  private def transactionalProducer: org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]] =
    val properties = new java.util.Properties()
    properties.put("bootstrap.servers", "localhost:1")
    properties.put("transactional.id", "failures")
    val bytes = new org.apache.kafka.common.serialization.ByteArraySerializer()
    new org.apache.kafka.clients.producer.KafkaProducer(properties, bytes, bytes)

  test("a producer's failure is fatal exactly where the producer holds a fatal error, as the Java client says, whatever the failure's class"):
    val producer = transactionalProducer
    try
      assertEquals(JavaFailures.fatalHeld(producer), Some(false), "the Java client's transaction manager could not be asked")
      // An error state Java leaves by an abort, which it reports with a plain client exception as its cause.
      val abortable = errorState(new JavaKafkaException("Could not add partitions to transaction due to errors: {t-0=UNKNOWN_SERVER_ERROR}"))
      val held      = JavaFailures.producedBy(producer)(abortable).asInstanceOf[KafkaException.BackendFailure]
      assertEquals((held.retriable, held.fatal, held.transactionAbortRequired), (Some(false), Some(false), Some(true)))
      // A failure Java always holds fatal is fatal though the producer has not marked itself yet, as it may not have when it reports one.
      val fenced = JavaFailures.producedBy(producer)(new ProducerFencedException("fenced")).asInstanceOf[KafkaException.BackendFailure]
      assertEquals((fenced.retriable, fenced.fatal, fenced.transactionAbortRequired), (Some(false), Some(true), Some(false)))
      val field = producer.getClass.getDeclaredField("transactionManager")
      field.setAccessible(true)
      val manager = field.get(producer)
      val fatal   = manager.getClass.getDeclaredMethod("transitionToFatalError", classOf[RuntimeException])
      fatal.setAccessible(true)
      fatal.invoke(manager, new InvalidTxnStateException("refused"))
      val invalid = JavaFailures.producedBy(producer)(new InvalidTxnStateException("refused")).asInstanceOf[KafkaException.BackendFailure]
      assertEquals((invalid.retriable, invalid.fatal, invalid.transactionAbortRequired), (Some(false), Some(true), Some(false)))
    finally producer.close(java.time.Duration.ZERO)

  test("the error state a retriable failure left the producer in cannot be retried, only aborted, as librdkafka says of the same state"):
    val wrapped = produced(errorState(new TimeoutException("expired")))
    assertEquals(
      (wrapped.code, wrapped.retriable, wrapped.fatal, wrapped.transactionAbortRequired),
      (Some(ErrorCode.RequestTimedOut), Some(false), Some(false), Some(true))
    )
