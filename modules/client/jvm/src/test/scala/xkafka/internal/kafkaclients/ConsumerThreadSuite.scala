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

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.List as JavaList
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import munit.FunSuite
import org.apache.kafka.clients.consumer.{ConsumerRecord as JavaConsumerRecord, ConsumerRecords, MockConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition as JavaTopicPartition}

/** The thread that owns a Java consumer, against the Java client's own mock, so each step of a rebalance can be held. */
final class ConsumerThreadSuite extends FunSuite:
  // Each body runs as a future, so a thread that never answers fails its test instead of holding up the run.
  override val munitTimeout: scala.concurrent.duration.Duration = 30.seconds

  private val topic     = "events"
  private val partition = new JavaTopicPartition(topic, 0)

  bounded("a call made while the group waits on a rebalance is made before the rebalance is applied, and the poll's records are kept"):
    val consumer = subscribable()
    val thread   = subscribed(consumer)
    consumer.schedulePollTask(() =>
      consumer.rebalance(JavaList.of(partition))
      consumer.updateBeginningOffsets(java.util.Map.of(partition, Long.box(0L)))
      consumer.addRecord(new JavaConsumerRecord(topic, 0, 0L, bytes("key"), bytes("value")))
    )
    // The poll that raises the rebalance answers with nothing, so the rebalance can be taken while the listener still holds the poll.
    assertEquals(answer(thread.poll(Duration.ofSeconds(5))), Nil)
    val taken = answer(thread.takeRebalances)
    assertEquals(taken.map(rebalance => rebalance.kind -> rebalance.partitions), List(ConsumerThread.Assigned -> List(LeaseKey(topic, 0))))
    assertEquals(answer(thread.call(_.assignment.asScala.toSet)), Set(partition))
    answer(thread.apply(taken.head, _ => ()))
    // The records the poll went on to return once the listener was done reach the next poll.
    assertEquals(answer(thread.poll(Duration.ofSeconds(5))).map(_.offset), List(0L))
    answer(thread.close)

  bounded("a consumer closed while the group waits on a rebalance closes, and refuses calls afterwards"):
    val consumer = subscribable()
    val thread   = subscribed(consumer)
    consumer.schedulePollTask(() => consumer.rebalance(JavaList.of(partition)))
    assertEquals(answer(thread.poll(Duration.ofSeconds(5))), Nil)
    assertEquals(answer(thread.takeRebalances).size, 1)
    // Nothing applies the rebalance, so only the close lets the listener return.
    answer(thread.close)
    assert(consumer.closed, "the consumer was not closed")
    val refused = await(thread.call(_.assignment))
    assert(refused.left.exists(_.isInstanceOf[IllegalStateException]), s"a call after the close should be refused, got $refused")

  bounded("a call made while a poll waits for records is made once that poll returns"):
    val events   = new ConcurrentLinkedQueue[String]
    val consumer =
      new MockConsumer[Array[Byte], Array[Byte]]("earliest"):
        override def poll(timeout: Duration): ConsumerRecords[Array[Byte], Array[Byte]] =
          Thread.sleep(timeout.toMillis)
          val records = super.poll(timeout)
          events.add("poll returned")
          records
    consumer.updatePartitions(topic, JavaList.of(new PartitionInfo(topic, 0, null, Array.empty, Array.empty)))
    val thread  = subscribed(consumer)
    val polling = new CompletableFuture[Either[Throwable, List[ConsumerThread.Record]]]
    thread.poll(Duration.ofMillis(300))(outcome => polling.complete(outcome): Unit)
    answer(thread.call(_ => events.add("call"): Unit))
    assertEquals(polling.get(5, TimeUnit.SECONDS).map(_.size), Right(0))
    assertEquals(events.asScala.toList, List("poll returned", "call"))
    answer(thread.close)

  private def bounded(name: String)(body: => Unit): Unit = test(name)(Future(body)(using ExecutionContext.global))

  private def subscribable(): MockConsumer[Array[Byte], Array[Byte]] =
    val consumer = new MockConsumer[Array[Byte], Array[Byte]]("earliest")
    consumer.updatePartitions(topic, JavaList.of(new PartitionInfo(topic, 0, null, Array.empty, Array.empty)))
    consumer

  private def subscribed(consumer: MockConsumer[Array[Byte], Array[Byte]]): ConsumerThread =
    val thread = new ConsumerThread(consumer, "xkafka-consumer-test")
    thread.start()
    answer(thread.call(_.subscribe(JavaList.of(topic), thread.listener)))
    thread

  private def await[A](register: (Either[Throwable, A] => Unit) => Unit): Either[Throwable, A] =
    val outcome = new CompletableFuture[Either[Throwable, A]]
    register(value => outcome.complete(value): Unit)
    outcome.get(5, TimeUnit.SECONDS)

  private def answer[A](register: (Either[Throwable, A] => Unit) => Unit): A = await(register).fold(throw _, identity)

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)
