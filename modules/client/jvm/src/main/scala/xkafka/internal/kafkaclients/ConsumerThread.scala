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

import java.time.Duration
import java.util.Collection as JavaCollection
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.kafka.clients.consumer.{Consumer as JavaConsumer, ConsumerRebalanceListener, ConsumerRecord as JavaConsumerRecord}
import org.apache.kafka.common.TopicPartition as JavaTopicPartition

/** One Java consumer and the only thread that calls it.
  *
  * The Java consumer refuses calls from two threads at once, so every call is queued here and made in turn. The consumer runs its rebalance listener
  * inside a poll, and the group waits for the listener to return. The listener here hands each rebalance to whoever takes rebalances, which is the
  * shared client, and keeps making queued calls until that rebalance is applied. A commit or a transaction's group metadata that a revocation waits
  * for is therefore still made, on this thread, while the revocation holds the poll.
  *
  * A poll waiting while the listener runs returns what it has, which is nothing, so the client can take the rebalance. The records the consumer
  * returns once the listener is done are kept for the next poll. A poll is made in slices of at most 10 ms, so a call queued during one waits for the
  * slice in progress.
  *
  * With `reportEnds`, a poll also reports each assigned partition whose position has caught up with the end the consumer last fetched, which is the
  * last stable offset under read_committed, once each time it gets there. A poll that reaches an end returns without waiting out its timeout.
  */
private[kafkaclients] final class ConsumerThread(consumer: JavaConsumer[Array[Byte], Array[Byte]], name: String, reportEnds: Boolean):
  import ConsumerThread.*

  private val mailbox = new LinkedBlockingQueue[Command]

  // Read and written only on the consumer's thread.
  private val buffered = mutable.ArrayDeque.empty[Record]
  private val ends     = mutable.ArrayDeque.empty[End]
  private val reached  = mutable.Map.empty[JavaTopicPartition, Long]
  private val failed   = new AtomicReference[Option[Throwable]](None)
  private val waiting  = new AtomicReference[Option[Waiting]](None)
  private val raised   = mutable.ArrayDeque.empty[Rebalancing]
  private val stopping = new AtomicReference[Option[Close]](None)

  private val closing  = new AtomicBoolean(false)
  private val finished = new AtomicBoolean(false)

  private val thread =
    val created = new Thread(() => loop(), name)
    created.setDaemon(true)
    created

  /** The listener every subscription is made with. */
  val listener: ConsumerRebalanceListener =
    new ConsumerRebalanceListener:
      override def onPartitionsRevoked(partitions: JavaCollection[JavaTopicPartition]): Unit = rebalanced(Revoked, partitions)

      override def onPartitionsLost(partitions: JavaCollection[JavaTopicPartition]): Unit = rebalanced(Lost, partitions)

      override def onPartitionsAssigned(partitions: JavaCollection[JavaTopicPartition]): Unit = rebalanced(Assigned, partitions)

  def start(): Unit = thread.start()

  /** Makes a call on the consumer's thread and reports how it went. */
  def call[A](task: JavaConsumer[Array[Byte], Array[Byte]] => A)(reply: Either[Throwable, A] => Unit): Unit =
    send(Call(consumer => reply(Try(task(consumer)).toEither), error => reply(Left(error))), reply)

  /** The records and ends of one poll, or what an earlier poll kept. */
  def poll(timeout: Duration)(reply: Either[Throwable, Polled] => Unit): Unit = send(Poll(timeout, reply), reply)

  /** Makes a seek of `partition`, and drops the records and the end kept for it, which were fetched before the seek. */
  def seek(partition: JavaTopicPartition)(task: JavaConsumer[Array[Byte], Array[Byte]] => Unit)(reply: Either[Throwable, Unit] => Unit): Unit =
    call(consumer =>
      task(consumer)
      buffered.filterInPlace(record => record.topic != partition.topic || record.partition != partition.partition)
      forget(partition)
    )(reply)

  /** Makes an assignment, after which every assigned partition's end is reported again once reached. */
  def assign(task: JavaConsumer[Array[Byte], Array[Byte]] => Unit)(reply: Either[Throwable, Unit] => Unit): Unit =
    call(consumer =>
      task(consumer)
      reached.clear()
      ends.clear()
    )(reply)

  /** Every rebalance the listener raised since the last take, oldest first. */
  def takeRebalances(reply: Either[Throwable, List[Rebalancing]] => Unit): Unit = call(_ => raised.removeAll().toList)(reply)

  /** Makes `before` and lets the listener holding `rebalancing` return, which it does even where `before` fails. */
  def apply(rebalancing: Rebalancing, before: JavaConsumer[Array[Byte], Array[Byte]] => Unit)(reply: Either[Throwable, Unit] => Unit): Unit =
    call(consumer =>
      try before(consumer)
      finally rebalancing.applied.set(true)
    )(reply)

  /** Closes the consumer, once the poll it may be inside has returned, and stops the thread. */
  def close(reply: Either[Throwable, Unit] => Unit): Unit = send(Close(reply), reply)

  private def send(command: Command, reply: Either[Throwable, Nothing] => Unit): Unit =
    mailbox.put(command)
    // A command that arrives after the thread has stopped is never taken, so it is refused here instead.
    if finished.get && mailbox.remove(command) then reply(Left(closed))

  private def loop(): Unit =
    while !finished.get do
      mailbox.take() match
        case Call(task, _)       => task(consumer)
        case Poll(timeout, sent) => polling(timeout, sent)
        case close: Close        => stopping.set(Some(close))
      stopping.getAndSet(None).foreach(finish)

  private def polling(timeout: Duration, reply: Either[Throwable, Polled] => Unit): Unit =
    if buffered.nonEmpty || ends.nonEmpty || failed.get.nonEmpty then deliver(reply)
    else
      val deadline = System.nanoTime + timeout.toNanos
      waiting.set(Some(Waiting(reply, deadline)))
      pollInSlices(deadline)
      waiting.getAndSet(None).foreach(waited => deliver(waited.reply))

  /** Polls a slice at a time until records or an end arrive, the deadline passes, a call is queued, or the listener has already answered the poll, so
    * a call queued meanwhile waits for one slice and not for the whole timeout.
    */
  @tailrec
  private def pollInSlices(deadline: Long): Unit =
    val slice = (deadline - System.nanoTime).min(SliceNanos).max(0L)
    Try(consumer.poll(Duration.ofNanos(slice))).fold(error => failed.set(Some(error)), records => buffered.appendAll(records.asScala): Unit)
    if reportEnds && failed.get.isEmpty then Try(reachEnds()).failed.foreach(error => failed.set(Some(error)))
    if buffered.isEmpty && ends.isEmpty && failed.get.isEmpty && waiting.get.nonEmpty && mailbox.isEmpty && System.nanoTime < deadline then
      pollInSlices(deadline)

  /** Keeps an end for each assigned partition that has caught up with it at a position where its end was not already reported. */
  private def reachEnds(): Unit =
    consumer.assignment.asScala.foreach: partition =>
      val lag = consumer.currentLag(partition)
      if lag.isPresent && lag.getAsLong == 0L then
        val position = consumer.position(partition)
        if !reached.get(partition).contains(position) then
          reached.update(partition, position)
          ends.append(End(partition, position))

  private def forget(partition: JavaTopicPartition): Unit =
    reached -= partition
    ends.filterInPlace(_.partition != partition): Unit

  /** The kept records and ends, or else the failure of the poll that could not return its own, or else nothing. */
  private def deliver(reply: Either[Throwable, Polled] => Unit): Unit =
    if buffered.nonEmpty || ends.nonEmpty then reply(Right(Polled(buffered.removeAll().toList, ends.removeAll().toList)))
    else reply(failed.getAndSet(None).map(Left(_)).getOrElse(Right(Polled.empty)))

  /** Raises one rebalance and makes queued calls until it is applied, or until the consumer is closing and nothing will apply it. */
  private def rebalanced(kind: Int, partitions: JavaCollection[JavaTopicPartition]): Unit =
    if !closing.get && !partitions.isEmpty then
      val rebalancing = new Rebalancing(kind, partitions.asScala.toList.map(value => LeaseKey(value.topic, value.partition)))
      raised.append(rebalancing)
      // Records are only polled once the kept ones are taken, so nothing is kept here and the poll returns nothing.
      waiting.getAndSet(None).foreach(_.reply(Right(Polled.empty)))
      while !rebalancing.applied.get && !closing.get do
        val wait = waiting.get.fold(Long.MaxValue)(_.deadline - System.nanoTime).max(0L)
        Option(mailbox.poll(wait, TimeUnit.NANOSECONDS)).foreach:
          case Call(task, _)       => task(consumer)
          case Poll(timeout, sent) => waiting.getAndSet(Some(Waiting(sent, System.nanoTime + timeout.toNanos))).foreach(_.reply(Right(Polled.empty)))
          case close: Close        =>
            closing.set(true)
            stopping.set(Some(close))
        waiting.get.filter(_.deadline <= System.nanoTime).foreach: expired =>
          waiting.set(None)
          expired.reply(Right(Polled.empty))

  private def finish(close: Close): Unit =
    closing.set(true)
    waiting.getAndSet(None).foreach(_.reply(Right(Polled.empty)))
    val outcome = Try(consumer.close()).toEither.map(_ => ())
    finished.set(true)
    close.reply(outcome)
    Iterator.continually(mailbox.poll()).takeWhile(_ != null).foreach:
      case Call(_, fail)    => fail(closed)
      case Poll(_, sent)    => sent(Left(closed))
      case Close(remaining) => remaining(Right(()))

private[kafkaclients] object ConsumerThread:
  type Record = JavaConsumerRecord[Array[Byte], Array[Byte]]

  /** A poll's records, and the partitions whose end it reached with the offset there. */
  final case class Polled(records: List[Record], ends: List[End])

  object Polled:
    val empty: Polled = Polled(Nil, Nil)

  final case class End(partition: JavaTopicPartition, offset: Long)

  /** The longest a queued call waits for a poll in progress. */
  private val SliceNanos = 10L * 1000 * 1000

  /** The kinds `Rebalance` reports. */
  val Assigned = 1
  val Revoked  = 0
  val Lost     = 2

  /** A rebalance the listener is holding until the shared client applies it. */
  final class Rebalancing(val kind: Int, val partitions: List[LeaseKey]):
    val applied = new AtomicBoolean(false)

  private sealed trait Command
  private final case class Call(task: JavaConsumer[Array[Byte], Array[Byte]] => Unit, fail: Throwable => Unit) extends Command
  private final case class Poll(timeout: Duration, reply: Either[Throwable, Polled] => Unit)                   extends Command
  private final case class Close(reply: Either[Throwable, Unit] => Unit)                                       extends Command

  private final case class Waiting(reply: Either[Throwable, Polled] => Unit, deadline: Long)

  private def closed: IllegalStateException = new IllegalStateException("the consumer is closed")
