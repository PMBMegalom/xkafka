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
package streams

import java.nio.ByteBuffer

import scala.concurrent.duration.FiniteDuration

import cats.MonadThrow
import cats.syntax.all.*
import fs2.Chunk

/** A span of time in epoch milliseconds from `start`, included, to `end`: excluded for a time window, included for a sliding window and a session, as
  * in Java Kafka Streams.
  */
final case class Window(start: Long, end: Long)

/** A key and the window its value covers. */
final case class Windowed[K](key: K, window: Window)

object Windowed:
  /** A windowed key as Java's `TimeWindowedSerializer` writes it: the key as `key` writes it, then its window's start as eight big-endian bytes.
    * Reading it back takes the windows' `size`, from which the window's end follows, as Java's deserializer is told the size.
    */
  def timeSerde[F[_], K](key: Serde[F, K], size: FiniteDuration)(using F: MonadThrow[F]): Serde[F, Windowed[K]] =
    val serializer =
      windowedSerializer(key.serializer): (topic, headers, windowed) =>
        key.serializer.serialize(topic, headers, windowed.key).flatMap:
          case Some(written) => F.pure(Some(written ++ Chunk.array(ByteBuffer.allocate(8).putLong(windowed.window.start).array)))
          case None => F.raiseError(new IllegalArgumentException(s"a windowed key for topic '${topic.value}' has a key its serializer wrote as none"))
    val deserializer =
      Deserializer.instance[F, Windowed[K]]: (topic, headers, bytes) =>
        bytes match
          case Some(read) if read.size >= 8 =>
            val start = ByteBuffer.wrap(read.drop(read.size - 8).toArray).getLong
            val end   = start + size.toMillis
            // Java truncates an end past the largest timestamp to it.
            key.deserializer.deserialize(topic, headers, Some(read.take(read.size - 8)))
              .map(Windowed(_, Window(start, if end < 0 then Long.MaxValue else end)))
          case Some(read) => F.raiseError(new IllegalArgumentException(s"a windowed key is at least 8 bytes, but a record of topic '${topic
                .value}' held ${read.size}"))
          case None => F.raiseError(new IllegalArgumentException(s"a record of topic '${topic.value}' has no windowed key"))
    Serde(serializer, deserializer)

  /** A session's key as Java's `SessionWindowedSerializer` writes it: the key as `key` writes it, then the session's end and its start, each as eight
    * big-endian bytes.
    */
  def sessionSerde[F[_], K](key: Serde[F, K])(using F: MonadThrow[F]): Serde[F, Windowed[K]] =
    val serializer =
      windowedSerializer(key.serializer): (topic, headers, windowed) =>
        key.serializer.serialize(topic, headers, windowed.key).flatMap:
          case Some(written) => F
              .pure(Some(written ++ Chunk.array(ByteBuffer.allocate(16).putLong(windowed.window.end).putLong(windowed.window.start).array)))
          case None => F.raiseError(new IllegalArgumentException(s"a session key for topic '${topic.value}' has a key its serializer wrote as none"))
    val deserializer =
      Deserializer.instance[F, Windowed[K]]: (topic, headers, bytes) =>
        bytes match
          case Some(read) if read.size >= 16 =>
            val times = ByteBuffer.wrap(read.drop(read.size - 16).toArray)
            val end   = times.getLong
            key.deserializer.deserialize(topic, headers, Some(read.take(read.size - 16))).map(Windowed(_, Window(times.getLong, end)))
          case Some(read) => F.raiseError(new IllegalArgumentException(s"a session key is at least 16 bytes, but a record of topic '${topic
                .value}' held ${read.size}"))
          case None => F.raiseError(new IllegalArgumentException(s"a record of topic '${topic.value}' has no session key"))
    Serde(serializer, deserializer)

  /** A windowed key's serializer, writing it as `write` does, whose records are partitioned by the key alone as `key` writes it, as Java's
    * `WindowedStreamPartitioner` partitions them, so that every window of a key goes to one partition.
    */
  private def windowedSerializer[F[_], K](
      key: Serializer[F, K]
  )(write: (Topic, Headers, Windowed[K]) => F[Option[Chunk[Byte]]]): Serializer[F, Windowed[K]] =
    new Serializer[F, Windowed[K]]:
      override def serialize(topic: Topic, headers: Headers, value: Windowed[K]): F[Option[Chunk[Byte]]] = write(topic, headers, value)
      override private[xkafka] def partitionedBy(topic: Topic, headers: Headers, value: Windowed[K]): Option[F[Option[Chunk[Byte]]]] =
        Some(key.serialize(topic, headers, value.key))

/** Windows of a fixed `size`, starting every `advance`: tumbling when the advance is the size, so each record falls in one window, and hopping when
  * it is shorter, so windows overlap. A record later than its window's end by more than `grace` no longer counts.
  */
final case class TimeWindows private (size: FiniteDuration, advance: FiniteDuration, grace: FiniteDuration):
  private[xkafka] def sizeMillis: Long    = size.toMillis
  private[xkafka] def advanceMillis: Long = advance.toMillis
  private[xkafka] def graceMillis: Long   = grace.toMillis

  /** How long a window is kept by default, and at least: its size and its grace, as Java keeps it. */
  private[xkafka] def retentionMillis: Long = sizeMillis + graceMillis

  /** What the least retention is made of, for the reason a shorter one is refused. */
  private[xkafka] def retentionRule: String = "the windows' size and grace"

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

/** Windows covering records at most `timeDifference` apart, start and end included, created only where records are, as Java Kafka Streams' sliding
  * windows: each record makes the window that ends at it and, where later records are within the time difference, the window that starts just after
  * it. A record later than its window's end by more than `grace` no longer counts.
  */
final case class SlidingWindows(timeDifference: FiniteDuration, grace: FiniteDuration):
  private[xkafka] def differenceMillis: Long = timeDifference.toMillis
  private[xkafka] def graceMillis: Long      = grace.toMillis

  /** How long a window is kept by default, and at least: twice the time difference and the grace, which creating a window can reach back over. */
  private[xkafka] def retentionMillis: Long = 2 * differenceMillis + graceMillis

  /** What the least retention is made of, for the reason a shorter one is refused. */
  private[xkafka] def retentionRule: String = "twice the windows' time difference and their grace"

  /** Why these windows cannot be used, if they cannot. */
  private[xkafka] def problem: Option[String] =
    def whole(duration: FiniteDuration) = duration.toNanos % 1000000L == 0L
    if timeDifference.length < 0 then Some(s"the time difference must not be negative, was $timeDifference")
    else if grace.length < 0 then Some(s"the grace must not be negative, was $grace")
    else if !whole(timeDifference) || !whole(grace) then Some("the time difference and grace must be whole milliseconds")
    else None

object SlidingWindows:
  /** Sliding windows taking no late record. */
  def withNoGrace(timeDifference: FiniteDuration): SlidingWindows = SlidingWindows(timeDifference, FiniteDuration(0, timeDifference.unit))

/** Sessions of each key's activity, as Java Kafka Streams' session windows: a record starts a session, or joins those of its key that end at most
  * `inactivityGap` before it or start at most that long after it, merging them into one. A record later than its session's end by more than the gap
  * and `grace` no longer counts.
  */
final case class SessionWindows(inactivityGap: FiniteDuration, grace: FiniteDuration):
  private[xkafka] def gapMillis: Long   = inactivityGap.toMillis
  private[xkafka] def graceMillis: Long = grace.toMillis

  /** How long a session is kept after its end by default, and at least: the inactivity gap and the grace. */
  private[xkafka] def retentionMillis: Long = gapMillis + graceMillis

  /** What the least retention is made of, for the reason a shorter one is refused. */
  private[xkafka] def retentionRule: String = "the sessions' inactivity gap and grace"

  /** Why these sessions cannot be used, if they cannot. */
  private[xkafka] def problem: Option[String] =
    def whole(duration: FiniteDuration) = duration.toNanos % 1000000L == 0L
    if inactivityGap.length < 0 then Some(s"the inactivity gap must not be negative, was $inactivityGap")
    else if grace.length < 0 then Some(s"the grace must not be negative, was $grace")
    else if !whole(inactivityGap) || !whole(grace) then Some("the inactivity gap and grace must be whole milliseconds")
    else None

object SessionWindows:
  /** Sessions taking no late record. */
  def withNoGrace(inactivityGap: FiniteDuration): SessionWindows = SessionWindows(inactivityGap, FiniteDuration(0, inactivityGap.unit))

/** The windows a stream-stream join matches records in, as Java Kafka Streams' join windows: a record joins each record of the other stream with the
  * same key whose time is at most `before` earlier than its own or at most `after` later. A record later than the stream time by more than `grace`
  * past its window no longer counts.
  */
final case class JoinWindows private (before: FiniteDuration, after: FiniteDuration, grace: FiniteDuration):
  private[xkafka] def beforeMillis: Long = before.toMillis
  private[xkafka] def afterMillis: Long  = after.toMillis
  private[xkafka] def graceMillis: Long  = grace.toMillis

  /** How far apart the earliest and latest records a record can join are, as Java's `size`. */
  private[xkafka] def sizeMillis: Long = beforeMillis + afterMillis

  /** How long a join's stores keep a record: the windows' size and their grace. */
  private[xkafka] def retentionMillis: Long = sizeMillis + graceMillis

  /** The same windows, joining records at most `before` earlier, as Java's `before`. */
  def withBefore(before: FiniteDuration): JoinWindows = copy(before = before)

  /** The same windows, joining records at most `after` later, as Java's `after`. */
  def withAfter(after: FiniteDuration): JoinWindows = copy(after = after)

  /** Why these windows cannot be used, if they cannot. */
  private[xkafka] def problem: Option[String] =
    def whole(duration: FiniteDuration) = duration.toNanos % 1000000L == 0L
    if !whole(before) || !whole(after) || !whole(grace) then Some("the time differences and grace must be whole milliseconds")
    else if sizeMillis < 0 then Some(s"the time before and after must not add up to less than nothing, were $before and $after")
    else if grace.length < 0 then Some(s"the grace must not be negative, was $grace")
    else None

object JoinWindows:
  /** Windows joining records at most `timeDifference` apart, either way, as Java's `ofTimeDifferenceAndGrace`. */
  def apply(timeDifference: FiniteDuration, grace: FiniteDuration): JoinWindows = new JoinWindows(timeDifference, timeDifference, grace)

  /** Windows joining records at most `timeDifference` apart, either way, taking no late record, as Java's `ofTimeDifferenceWithNoGrace`. */
  def withNoGrace(timeDifference: FiniteDuration): JoinWindows = apply(timeDifference, FiniteDuration(0, timeDifference.unit))
