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
package interop

import java.nio.ByteBuffer
import java.time.Instant

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

import fs2.Chunk
import org.apache.kafka.common.serialization.Serde as JavaSerde
import org.apache.kafka.streams.KafkaStreams as JavaKafkaStreams
import org.apache.kafka.streams.errors.{InvalidStateStoreException, StreamsNotStartedException, UnknownStateStoreException}
import org.apache.kafka.streams.kstream.Windowed as JavaWindowed
import org.apache.kafka.streams.query.{FailureReason, KeyQuery, Query as JavaQuery, RangeQuery, StateQueryRequest, WindowKeyQuery, WindowRangeQuery}
import org.apache.kafka.streams.state.{KeyValueIterator, WindowStoreIterator}
import internal.streams.{Answered, Asked, QueryWire}

/** Lets a Java Kafka Streams instance answer the queries xkafka members of its application forward to it, for the stores registered here, through
  * Java's own interactive queries. Mount [[answer]] on the route the instance's `application.server` names.
  */
final class JavaQueries private (streams: JavaKafkaStreams, application: String, stores: Map[String, JavaQueries.Kept]):
  /** These queries, answering also for the key-value store `store`, its keys and values written with `key` and `value`. */
  def keyValue[K, V](store: String, key: JavaSerde[K], value: JavaSerde[V]): JavaQueries = registered(store, JavaQueries.KeyValue(key, value))

  /** These queries, answering also for the window store `store`, its keys and values written with `key` and `value`. */
  def window[K, V](store: String, key: JavaSerde[K], value: JavaSerde[V]): JavaQueries = registered(store, JavaQueries.Windows(key, value))

  /** These queries, answering also for the session store `store`, its keys and values written with `key` and `value`. */
  def session[K, V](store: String, key: JavaSerde[K], value: JavaSerde[V]): JavaQueries = registered(store, JavaQueries.Sessions(key, value))

  /** The answer to a query another member sent, as the bytes to send back to it; bytes that are not an xkafka query are refused. */
  def answer(request: Array[Byte]): Array[Byte] =
    val asked = QueryWire.asked(Chunk.array(request)).getOrElse(throw new IllegalArgumentException("the request is not an xkafka query"))
    val store = asked.request.storeName
    val found =
      stores.get(store).flatMap(_.run(new JavaQueries.Run(streams, store, s"$application-$store-changelog", asked)))
        .getOrElse(Answered.Refused(QueryProblem.UnknownStore(store)))
    QueryWire.encode(found).toArray

  private def registered(store: String, kept: JavaQueries.Kept) = new JavaQueries(streams, application, stores.updated(store, kept))

object JavaQueries:
  /** Queries answered by `streams`, an instance of the application `application`, for no store until some are registered. */
  def of(streams: JavaKafkaStreams, application: String): JavaQueries = new JavaQueries(streams, application, Map.empty)

  /** A query for one store, with the instance that runs it, and the topic its keys and values are written for. */
  private final class Run(val streams: JavaKafkaStreams, val store: String, val topic: String, val asked: Asked):
    /** What Java's query finds in each partition asked of, read by `read`; a partition this instance does not hold, or is not ready to read from,
      * answers as rebalancing.
      */
    def ran[R](query: JavaQuery[R])(read: R => Vector[(Chunk[Byte], Chunk[Byte])]): Answered =
      val request  = StateQueryRequest.inStore(store).withQuery(query).withPartitions(asked.partitions.map(Int.box).toSet.asJava)
      val required = if asked.allowStandby then request else request.requireActive()
      Try(streams.query(required)) match
        case Failure(_: UnknownStateStoreException)                                 => Answered.Refused(QueryProblem.UnknownStore(store))
        case Failure(_: StreamsNotStartedException | _: InvalidStateStoreException) => Answered.Refused(QueryProblem.Rebalancing)
        case Failure(other)                                                         => throw other
        case Success(result)                                                        =>
          val each = asked.partitions.map(partition => Option(result.getPartitionResults.get(partition)))
          if each.exists(_.forall(_.isFailure)) then
            if each.flatten.exists(_.getFailureReason == FailureReason.DOES_NOT_EXIST) then Answered.Refused(QueryProblem.UnknownStore(store))
            else Answered.Refused(QueryProblem.Rebalancing)
          else Answered.Found(each.flatten.toVector.flatMap(found => read(found.getResult)))

    def bytes[A](serde: JavaSerde[A], value: A): Chunk[Byte] = Chunk.array(serde.serializer.serialize(topic, value))

    def read[A](serde: JavaSerde[A], value: Chunk[Byte]): A = serde.deserializer.deserialize(topic, value.toArray)

  private def long(value: Long): Chunk[Byte] = Chunk.array(ByteBuffer.allocate(8).putLong(value).array)

  /** A store registered with its serdes, which runs the queries of its kind and answers none of another. */
  private sealed trait Kept:
    def run(on: Run): Option[Answered]

  private final case class KeyValue[K, V](key: JavaSerde[K], value: JavaSerde[V]) extends Kept:
    def run(on: Run): Option[Answered] =
      def entries(found: KeyValueIterator[K, V]) =
        Using.resource(found)(_.asScala.map(kv => on.bytes(key, kv.key) -> on.bytes(value, kv.value)).toVector)
      on.asked.request match
        case QueryRequest.Get(_, bytes) =>
          Some(on.ran(KeyQuery.withKey[K, V](on.read(key, bytes)))(found => Option(found).toVector.map(held => bytes -> on.bytes(value, held))))
        case QueryRequest.Range(_, from, to) =>
          val (lower, upper) = (from.map(on.read(key, _)), to.map(on.read(key, _)))
          val query          =
            (lower, upper) match
              case (Some(first), Some(last)) => RangeQuery.withRange[K, V](first, last)
              case (Some(first), None)       => RangeQuery.withLowerBound[K, V](first)
              case (None, Some(last))        => RangeQuery.withUpperBound[K, V](last)
              case (None, None)              => RangeQuery.withNoBounds[K, V]()
          Some(on.ran(query)(entries))
        case _ => None

  // A window store's key is the key, its window's start in eight bytes and a sequence number in four, which xkafka's stores keep at 0.
  private final case class Windows[K, V](key: JavaSerde[K], value: JavaSerde[V]) extends Kept:
    def run(on: Run): Option[Answered] =
      on.asked.request match
        case QueryRequest.Windows(_, Some(bytes), from, to) =>
          val query = WindowKeyQuery.withKeyAndWindowStartRange[K, V](on.read(key, bytes), Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))
          Some(on.ran(query)((found: WindowStoreIterator[V]) =>
            Using
              .resource(found)(_.asScala.map(kv => (bytes ++ long(kv.key) ++ Chunk.array(new Array[Byte](4))) -> on.bytes(value, kv.value)).toVector)
          ))
        case QueryRequest.Windows(_, None, from, to) =>
          val query = WindowRangeQuery.withWindowStartRange[K, V](Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))
          Some(on.ran(query)((found: KeyValueIterator[JavaWindowed[K], V]) =>
            Using.resource(found)(
              _.asScala
                .map(kv => (on.bytes(key, kv.key.key) ++ long(kv.key.window.start) ++ Chunk.array(new Array[Byte](4))) -> on.bytes(value, kv.value))
                .toVector
            )
          ))
        case _ => None

  // A session store's key is the key, then its session's end and its start, each in eight bytes.
  private final case class Sessions[K, V](key: JavaSerde[K], value: JavaSerde[V]) extends Kept:
    def run(on: Run): Option[Answered] =
      on.asked.request match
        case QueryRequest.Sessions(_, bytes) => Some(
            on.ran(WindowRangeQuery.withKey[K, V](on.read(key, bytes)))((found: KeyValueIterator[JavaWindowed[K], V]) =>
              Using.resource(found)(
                _.asScala.map(kv => (bytes ++ long(kv.key.window.end) ++ long(kv.key.window.start)) -> on.bytes(value, kv.value)).toVector
              )
            )
          )
        case _ => None
