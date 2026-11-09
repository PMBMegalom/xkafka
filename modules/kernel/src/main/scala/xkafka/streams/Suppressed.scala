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

package xkafka.streams

import scala.concurrent.duration.FiniteDuration

/** How much a suppression's buffer holds, by its number of changes and its size in bytes as Java Kafka Streams counts it, and what it does once it
  * holds more: send the earliest changes on early, or stop the application.
  */
sealed trait BufferLimit:
  private[xkafka] def maxRecords: Long
  private[xkafka] def maxBytes: Long
  private[xkafka] def emitEarly: Boolean

object BufferLimit:
  /** A limit that stops the application once the buffer holds more, as Java's `StrictBufferConfig`, so no change goes on before its time. */
  final case class Strict private[xkafka] (maxRecords: Long, maxBytes: Long) extends BufferLimit:
    private[xkafka] def emitEarly: Boolean = false

    def withMaxRecords(records: Long): Strict = copy(maxRecords = records)
    def withMaxBytes(bytes: Long): Strict     = copy(maxBytes = bytes)

  /** A limit that sends the earliest changes on early once the buffer holds more, as Java's `EagerBufferConfig`. */
  final case class Eager private[xkafka] (maxRecords: Long, maxBytes: Long) extends BufferLimit:
    private[xkafka] def emitEarly: Boolean = true

    def withMaxRecords(records: Long): Eager = copy(maxRecords = records)
    def withMaxBytes(bytes: Long): Eager     = copy(maxBytes = bytes)

    /** Sends the earliest changes on early once the buffer holds more, which this limit does already. */
    def emitEarlyWhenFull: Eager = this

    /** Stops the application once the buffer holds more. */
    def shutDownWhenFull: Strict = Strict(maxRecords, maxBytes)

  /** No limit: the buffer holds every change until its time. */
  val unbounded: Strict = Strict(Long.MaxValue, Long.MaxValue)

  /** At most `records` changes. */
  def records(records: Long): Eager = Eager(records, Long.MaxValue)

  /** At most `bytes` bytes, as Java counts a buffer's size. */
  def bytes(bytes: Long): Eager = Eager(Long.MaxValue, bytes)

/** Holds back a table's changes, as Java Kafka Streams' `suppress`, in a buffer named `<name>-store` whose changelog is
  * `<application id>-<name>-store-changelog`.
  */
sealed trait Suppressed:
  def name: String
  def limit: BufferLimit

  /** Why this suppression cannot be used, if it cannot. */
  private[xkafka] def problem: Option[String] =
    val limits = Option.when(limit.maxRecords < 0 || limit.maxBytes < 0)("its buffer's limits must not be negative")
    this match
      case Suppressed.UntilWindowCloses(_, _)          => limits
      case Suppressed.UntilTimeLimit(_, timeToWait, _) =>
        if timeToWait.length < 0 then Some(s"the time to wait must not be negative, was $timeToWait")
        else if timeToWait.toNanos % 1000000L != 0L then Some("the time to wait must be whole milliseconds")
        else limits

object Suppressed:
  /** Each window's final result only, once the window has closed, its grace over. */
  final case class UntilWindowCloses private[xkafka] (name: String, limit: BufferLimit.Strict) extends Suppressed

  /** Each key's latest change once `timeToWait` has passed since the first change held, by the records' own time. */
  final case class UntilTimeLimit private[xkafka] (name: String, timeToWait: FiniteDuration, limit: BufferLimit) extends Suppressed

  /** Each window's final result only, once the window has closed. */
  def untilWindowCloses(name: String, limit: BufferLimit.Strict = BufferLimit.unbounded): UntilWindowCloses = UntilWindowCloses(name, limit)

  /** Each key's latest change once `timeToWait` has passed since the first change held. */
  def untilTimeLimit(name: String, timeToWait: FiniteDuration, limit: BufferLimit = BufferLimit.unbounded): UntilTimeLimit =
    UntilTimeLimit(name, timeToWait, limit)
