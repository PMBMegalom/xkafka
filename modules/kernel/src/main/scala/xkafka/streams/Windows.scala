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

/** A span of time in epoch milliseconds, from `start`, included, to `end`, excluded. */
final case class Window(start: Long, end: Long)

/** A key and the window its value covers. */
final case class Windowed[K](key: K, window: Window)

/** Windows of a fixed `size`, starting every `advance`: tumbling when the advance is the size, so each record falls in one window, and hopping when
  * it is shorter, so windows overlap. A record later than its window's end by more than `grace` no longer counts.
  */
final case class TimeWindows private (size: FiniteDuration, advance: FiniteDuration, grace: FiniteDuration):
  private[xkafka] def sizeMillis: Long    = size.toMillis
  private[xkafka] def advanceMillis: Long = advance.toMillis
  private[xkafka] def graceMillis: Long   = grace.toMillis

  /** How long a window is kept: its size and its grace, as Java keeps it. */
  private[xkafka] def retentionMillis: Long = sizeMillis + graceMillis

  /** The windows a record at `timestamp` falls in, earliest first, as Java finds them. */
  private[xkafka] def windowsFor(timestamp: Long): List[Window] =
    val first = (math.max(0L, timestamp - sizeMillis + advanceMillis) / advanceMillis) * advanceMillis
    if timestamp < first then Nil
    else List.iterate(first, ((timestamp - first) / advanceMillis + 1).toInt)(_ + advanceMillis).map(start => Window(start, start + sizeMillis))

  /** Why these windows cannot be used, if they cannot. */
  private[xkafka] def problem: Option[String] =
    def whole(duration: FiniteDuration) = duration.toNanos % 1000000L == 0L
    if size.length <= 0 then Some(s"the size must be positive, was $size")
    else if advance.length <= 0 then Some(s"the advance must be positive, was $advance")
    else if advance > size then Some(s"the advance must not exceed the size, was $advance for a size of $size")
    else if grace.length < 0 then Some(s"the grace must not be negative, was $grace")
    else if !whole(size) || !whole(advance) || !whole(grace) then Some("the size, advance and grace must be whole milliseconds")
    else None

object TimeWindows:
  /** Windows of `size`, one after another, each taking records up to `grace` late. */
  def tumbling(size: FiniteDuration, grace: FiniteDuration): TimeWindows = TimeWindows(size, size, grace)

  /** Windows of `size`, one after another, each taking no late record. */
  def tumblingWithNoGrace(size: FiniteDuration): TimeWindows = TimeWindows(size, size, FiniteDuration(0, size.unit))

  /** Windows of `size` starting every `advance`, each taking records up to `grace` late. */
  def hopping(size: FiniteDuration, advance: FiniteDuration, grace: FiniteDuration): TimeWindows = TimeWindows(size, advance, grace)

  /** Windows of `size` starting every `advance`, each taking no late record. */
  def hoppingWithNoGrace(size: FiniteDuration, advance: FiniteDuration): TimeWindows = TimeWindows(size, advance, FiniteDuration(0, size.unit))
