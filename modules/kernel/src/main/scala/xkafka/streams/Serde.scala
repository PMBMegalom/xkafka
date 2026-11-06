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

import cats.{ApplicativeThrow, MonadThrow}
import cats.arrow.FunctionK
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.Chunk

/** How a streams program writes and reads one type, since a streams application both writes and reads its topics. */
final case class Serde[F[_], A](serializer: Serializer[F, A], deserializer: Deserializer[F, A]):
  def mapK[G[_]](fk: FunctionK[F, G]): Serde[G, A] = Serde(serializer.mapK(fk), deserializer.mapK(fk))

  /** Values that may be absent, written as no value at all, as a tombstone. */
  def option(using F: cats.Applicative[F]): Serde[F, Option[A]] = Serde(serializer.option, deserializer.option)

object Serde:
  given [A]: FunctorK[[F[_]] =>> Serde[F, A]] with
    override def mapK[F[_], G[_]](serde: Serde[F, A])(fk: FunctionK[F, G]): Serde[G, A] = serde.mapK(fk)

  /** Bytes as they are. */
  def bytes[F[_]: ApplicativeThrow]: Serde[F, Chunk[Byte]] = Serde(Serializer.bytes[F], Deserializer.bytes[F])

  /** Text as UTF-8, as Java's `Serdes.String()` writes it. */
  def utf8[F[_]: ApplicativeThrow]: Serde[F, String] = Serde(Serializer.utf8[F], Deserializer.utf8[F])

  /** A number as eight big-endian bytes, as Java's `Serdes.Long()` writes it. */
  def long[F[_]](using F: ApplicativeThrow[F]): Serde[F, Long] =
    val serializer   = Serializer.bytes[F].contramap[Long](value => Chunk.array(ByteBuffer.allocate(8).putLong(value).array))
    val deserializer =
      Deserializer.instance[F, Long]: (topic, _, value) =>
        value match
          case Some(read) if read.size == 8 => F.pure(ByteBuffer.wrap(read.toArray).getLong)
          case Some(read)                   => F
              .raiseError(new IllegalArgumentException(s"a long is 8 bytes, but a record of topic '${topic.value}' held ${read.size}"))
          case None => F.raiseError(new Deserializer.NullValueException(topic))
    Serde(serializer, deserializer)

  /** A change to a group of a table's values, as Java's `ChangedSerializer` writes it: a value alone followed by the byte 0 where it is leaving and 1
    * where it is joining, and both, as the joining value's length as a zigzag varint and then the two values, followed by 2. A value its serializer
    * writes as none counts as absent, as Java drops a null value before this topic, and a change with neither is written as none. Reading it also
    * takes the 3, 4 and 5 that Java's `ChangedDeserializer` takes, which each have a byte saying if the change is the latest before their last.
    */
  private[xkafka] def tableChange[F[_], V](value: Serde[F, V])(using F: ApplicativeThrow[F]): Serde[F, TableChange[V]] =
    val serializer =
      Serializer.instance[F, TableChange[V]]: (topic, headers, change) =>
        val written = (side: Option[V]) => side.flatTraverse(value.serializer.serialize(topic, headers, _))
        (written(change.current), written(change.old)).mapN:
          case (Some(current), Some(old)) => Some(zigzag(current.size) ++ current ++ old ++ Chunk.singleton(2.toByte))
          case (Some(current), None)      => Some(current ++ Chunk.singleton(1.toByte))
          case (None, Some(old))          => Some(old ++ Chunk.singleton(0.toByte))
          case (None, None)               => None
    val deserializer =
      Deserializer.instance[F, TableChange[V]]: (topic, headers, bytes) =>
        def read(side: Chunk[Byte])       = value.deserializer.deserialize(topic, headers, Some(side))
        def leaving(old: Chunk[Byte])     = read(old).map(left => TableChange[V](None, Some(left)))
        def joining(current: Chunk[Byte]) = read(current).map(joined => TableChange[V](Some(joined), None))
        def both(values: Chunk[Byte])     =
          unzigzag(values).filter((length, used) => length >= 0 && length <= values.size - used) match
            case Some((length, used)) =>
              val rest = values.drop(used)
              (read(rest.take(length)), read(rest.drop(length))).mapN((joined, left) => TableChange(Some(joined), Some(left)))
            case None => F
                .raiseError[TableChange[V]](new IllegalArgumentException(s"a record of topic '${topic.value}' has no valid length of a table change"))
        bytes match
          case None       => F.raiseError(new Deserializer.NullValueException(topic))
          case Some(read) => read.last.fold(F.raiseError[TableChange[V]](new IllegalArgumentException(s"a record of topic '${topic
                .value}' is empty, which no table change is"))):
              case 0                   => leaving(read.dropRight(1))
              case 1                   => joining(read.dropRight(1))
              case 2                   => both(read.dropRight(1))
              case 3 if read.size >= 2 => leaving(read.dropRight(2))
              case 4 if read.size >= 2 => joining(read.dropRight(2))
              case 5 if read.size >= 2 => both(read.dropRight(2))
              case flag => F.raiseError(new IllegalArgumentException(s"a record of topic '${topic.value}' ends in $flag, which ends no table change"))
    Serde(serializer, deserializer)

  /** A length as a zigzag varint, as Java's `ByteUtils.writeVarint`. */
  private def zigzag(value: Int): Chunk[Byte] =
    def unsigned(left: Int): List[Byte] = if (left & ~0x7f) == 0 then List(left.toByte) else ((left & 0x7f) | 0x80).toByte :: unsigned(left >>> 7)
    Chunk.from(unsigned((value << 1) ^ (value >> 31)))

  /** The number at the start of `bytes` read as a zigzag varint, as Java's `ByteUtils.readVarint`, with how many bytes it takes up, if it is one. */
  private def unzigzag(bytes: Chunk[Byte]): Option[(Int, Int)] =
    def from(at: Int, sofar: Int): Option[(Int, Int)] =
      if at >= bytes.size || at >= 5 then None
      else
        val byte  = bytes(at)
        val value = sofar | ((byte & 0x7f) << (7 * at))
        if (byte & 0x80) == 0 then Some(((value >>> 1) ^ -(value & 1), at + 1)) else from(at + 1, value)
    from(0, 0)

  /** A windowed key as Java's `TimeWindowedSerializer` writes it: the key as `key` writes it, then its window's start as eight big-endian bytes.
    * Reading it back takes the windows' `size`, from which the window's end follows, as Java's deserializer is told the size.
    */
  def timeWindowed[F[_], K](key: Serde[F, K], size: FiniteDuration)(using F: MonadThrow[F]): Serde[F, Windowed[K]] =
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
  def sessionWindowed[F[_], K](key: Serde[F, K])(using F: MonadThrow[F]): Serde[F, Windowed[K]] =
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
