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
import java.util.concurrent.ExecutionException

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

import fs2.Chunk
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.serialization.Serde as JavaSerde
import org.apache.kafka.streams.{KafkaStreams as JavaKafkaStreams, KeyValue as JavaKeyValue, Topology as JavaTopology, TopologyDescription}
import org.apache.kafka.streams.errors.{InvalidStateStoreException, StreamsNotStartedException, UnknownStateStoreException}
import org.apache.kafka.streams.kstream.Windowed as JavaWindowed
import org.apache.kafka.streams.kstream.internals.{SessionWindow, TimeWindow}
import org.apache.kafka.streams.query.{FailureReason, KeyQuery, Query as JavaQuery, RangeQuery, StateQueryRequest, WindowKeyQuery, WindowRangeQuery}
import org.apache.kafka.streams.state.{HostInfo, KeyValueIterator, ValueAndTimestamp, WindowStoreIterator}
import internal.streams.{Answered, Asked, Murmur2, QueryWire}

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

  /** The value a window query found: the DSL's window stores keep each value with its timestamp and give back both, other stores the value alone. */
  private def plain[V](held: V): V =
    held match
      case stamped: ValueAndTimestamp[V @unchecked] => stamped.value
      case value                                    => value

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
            Using.resource(found)(
              _.asScala.map(kv => (bytes ++ long(kv.key) ++ Chunk.array(new Array[Byte](4))) -> on.bytes(value, plain(kv.value))).toVector
            )
          ))
        case QueryRequest.Windows(_, None, from, to) =>
          val query = WindowRangeQuery.withWindowStartRange[K, V](Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))
          Some(on.ran(query)((found: KeyValueIterator[JavaWindowed[K], V]) =>
            Using.resource(found)(
              _.asScala.map(kv =>
                (on.bytes(key, kv.key.key) ++ long(kv.key.window.start) ++ Chunk.array(new Array[Byte](4))) -> on.bytes(value, plain(kv.value))
              ).toVector
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

/** Why a query a Java member asked through [[JavaQuerying]] could not be answered. */
final class QueryProblemException(val problem: QueryProblem) extends RuntimeException(problem.toString)

/** Lets a Java Kafka Streams instance ask the members of its application for what their stores hold, xkafka and Java members alike, wherever each key
  * is held: Java's own metadata says which member holds each partition, and `forward` sends it the query, to the route where it answers, as an xkafka
  * member's `answer` and [[JavaQueries]] do. Every query goes to the member holding what it needs, this one included. A query that cannot be answered
  * throws a [[QueryProblemException]].
  */
final class JavaQuerying private (
    streams: JavaKafkaStreams,
    topology: TopologyDescription,
    application: String,
    partitions: String => Int,
    forward: java.util.function.BiFunction[HostInfo, Array[Byte], Array[Byte]]
):
  /** What the key-value store `store` holds for `key`, if anything. */
  def get[K, V](store: String, key: K, keySerde: JavaSerde[K], valueSerde: JavaSerde[V]): java.util.Optional[V] =
    val bytes = keyBytes(store, keySerde, key)
    keyed(store, key, keySerde, QueryRequest.Get(store, bytes)).headOption
      .fold(java.util.Optional.empty[V]())((_, v) => java.util.Optional.of(read(store, valueSerde, v)))

  /** The keys of the key-value store `store` from `from` to `to`, both included, with their values, in the order of their serialized bytes. */
  def range[K, V](store: String, from: K, to: K, keySerde: JavaSerde[K], valueSerde: JavaSerde[V]): java.util.List[JavaKeyValue[K, V]] =
    everywhere(store, QueryRequest.Range(store, Some(keyBytes(store, keySerde, from)), Some(keyBytes(store, keySerde, to))))
      .map((k, v) => JavaKeyValue.pair(read(store, keySerde, k), read(store, valueSerde, v))).asJava

  /** Every key of the key-value store `store` with its value, in the order of their serialized bytes. */
  def all[K, V](store: String, keySerde: JavaSerde[K], valueSerde: JavaSerde[V]): java.util.List[JavaKeyValue[K, V]] =
    everywhere(store, QueryRequest.Range(store, None, None)).map((k, v) => JavaKeyValue.pair(read(store, keySerde, k), read(store, valueSerde, v)))
      .asJava

  /** The windows of `key` in the window store `store` starting from `from` to `to`, both included, each by its start in epoch milliseconds, earliest
    * first.
    */
  def fetch[K, V](
      store: String,
      key: K,
      from: Instant,
      to: Instant,
      keySerde: JavaSerde[K],
      valueSerde: JavaSerde[V]
  ): java.util.List[JavaKeyValue[java.lang.Long, V]] =
    val bytes = keyBytes(store, keySerde, key)
    // A window store's key is the key, then the window's start in eight bytes and a sequence number in four.
    keyed(store, key, keySerde, QueryRequest.Windows(store, Some(bytes), from.toEpochMilli, to.toEpochMilli))
      .map((stored, v) => (ByteBuffer.wrap(stored.drop(stored.size - 12).take(8).toArray).getLong, v)).sortBy(_._1)
      .map((start, v) => JavaKeyValue.pair(java.lang.Long.valueOf(start), read(store, valueSerde, v))).asJava

  /** The windows of every key in the window store `store` starting from `from` to `to`, both included, from every member, in order of their starts
    * and then of their keys' bytes. Java's metadata does not say how long the store's windows are, so `windowSize` gives each window its end, which
    * is its start and that size: give the size the store's windows have, or the ends are wrong.
    */
  def fetchAll[K, V](
      store: String,
      from: Instant,
      to: Instant,
      windowSize: java.time.Duration,
      keySerde: JavaSerde[K],
      valueSerde: JavaSerde[V]
  ): java.util.List[JavaKeyValue[JavaWindowed[K], V]] =
    // A window store's key is the key, then the window's start in eight bytes and a sequence number in four.
    everywhere(store, QueryRequest.Windows(store, None, from.toEpochMilli, to.toEpochMilli)).map: (stored, v) =>
      val start = ByteBuffer.wrap(stored.drop(stored.size - 12).take(8).toArray).getLong
      (start, stored.take(stored.size - 12), v)
    .sortBy((start, key, _) => (start, key))(using Ordering.Tuple2(using Ordering.Long, QueryRequest.unsigned)).map: (start, key, v) =>
      JavaKeyValue.pair(new JavaWindowed(read(store, keySerde, key), new TimeWindow(start, start + windowSize.toMillis)), read(store, valueSerde, v))
    .asJava

  /** The sessions of `key` in the session store `store`, the earliest ending first. */
  def sessions[K, V](store: String, key: K, keySerde: JavaSerde[K], valueSerde: JavaSerde[V]): java.util.List[JavaKeyValue[JavaWindowed[K], V]] =
    val bytes = keyBytes(store, keySerde, key)
    // A session store's key is the key, then the session's end and its start, each in eight bytes.
    keyed(store, key, keySerde, QueryRequest.Sessions(store, bytes)).map: (stored, v) =>
      val end   = ByteBuffer.wrap(stored.drop(stored.size - 16).take(8).toArray).getLong
      val start = ByteBuffer.wrap(stored.drop(stored.size - 8).toArray).getLong
      JavaKeyValue.pair(new JavaWindowed(key, new SessionWindow(start, end)), read(store, valueSerde, v))
    .sortBy(found => (found.key.window.end, found.key.window.start)).asJava

  private def topicOf(store: String) = s"$application-$store-changelog"

  /** What Java's metadata says, where the instance can say: one not running yet, or not running any more, is moving. */
  private def settled[A](metadata: => A): A =
    Try(metadata) match
      case Success(said)                                                                                     => said
      case Failure(_: StreamsNotStartedException | _: InvalidStateStoreException | _: IllegalStateException) =>
        throw new QueryProblemException(QueryProblem.Rebalancing)
      case Failure(other) => throw other

  private def keyBytes[K](store: String, serde: JavaSerde[K], key: K): Chunk[Byte] = Chunk.array(serde.serializer.serialize(topicOf(store), key))

  private def read[A](store: String, serde: JavaSerde[A], bytes: Chunk[Byte]): A = serde.deserializer.deserialize(topicOf(store), bytes.toArray)

  /** The entries the member holding `key` finds for `request`. The key's partition is reckoned from the topic's own count of them: Java's metadata
    * counts only the partitions some member holds, which miss one while it moves (KAFKA-20887).
    */
  private def keyed[K](store: String, key: K, serde: JavaSerde[K], request: QueryRequest): Vector[(Chunk[Byte], Chunk[Byte])] =
    val holders        = holdersOf(store)
    val (topic, count) = sourcesOf(store).toList.map(topic => topic -> partitions(topic)).maxBy(_._2)
    val partition      = Murmur2.partition(Chunk.array(serde.serializer.serialize(topic, key)), count)
    JavaQuerying.holderOf(partition, holders)
      .fold(throw new QueryProblemException(QueryProblem.Rebalancing))(at => asked(at, Asked(request, List(partition), false)))

  /** The entries every member holding part of `store` finds for `request`, each asked for its partitions of the topic the store is filled from. */
  private def everywhere(store: String, request: QueryRequest): Vector[(Chunk[Byte], Chunk[Byte])] =
    val holders = holdersOf(store)
    if !JavaQuerying.covers(sourcesOf(store).toList.map(partitions).max, holders) then throw new QueryProblemException(QueryProblem.Rebalancing)
    holders.toVector.flatMap((at, partitions) => asked(at, Asked(request, partitions, false))).sortBy(_._1)(using QueryRequest.unsigned)

  /** The members Java's metadata says hold part of `store`, each with the partitions it holds as an active task. */
  private def holdersOf(store: String): List[(HostInfo, List[Int])] =
    val topics = sourcesOf(store)
    settled(streams.streamsMetadataForStore(store)).asScala.toList
      .map(at => at.hostInfo -> at.topicPartitions.asScala.filter(held => topics(held.topic)).map(_.partition).toList.sorted.distinct)
      .filter((at, held) => held.nonEmpty && at.port >= 0)

  /** The topics the sub-topology keeping `store` reads. */
  private def sourcesOf(store: String): Set[String] =
    topology.subtopologies.asScala.find(_.nodes.asScala.exists {
      case processor: TopologyDescription.Processor => processor.stores.contains(store)
      case _                                        => false
    }).fold(throw new QueryProblemException(QueryProblem.UnknownStore(store))): subtopology =>
      subtopology.nodes.asScala.collect { case source: TopologyDescription.Source => source.topicSet.asScala }.flatten.toSet

  private def asked(at: HostInfo, query: Asked): Vector[(Chunk[Byte], Chunk[Byte])] =
    val member = Endpoint.from(at.host, at.port).getOrElse(throw new QueryProblemException(QueryProblem.Rebalancing))
    val reply  =
      Try(forward.apply(at, QueryWire.encode(query).toArray)) match
        case Success(bytes)   => bytes
        case Failure(failure) =>
          throw new QueryProblemException(QueryProblem.Unreachable(member, Option(failure.getMessage).getOrElse(failure.toString)))
    QueryWire.answered(Chunk.array(reply)) match
      case Some(Answered.Found(entries))   => entries
      case Some(Answered.Refused(problem)) => throw new QueryProblemException(problem)
      case None                            => throw new QueryProblemException(QueryProblem.NotXkafka(member))

object JavaQuerying:
  /** Queries asked by `streams`, an instance of `application` running `topology`, sent to other members through `forward`. `admin` tells how many
    * partitions each topic a store is filled from has, asked once for each.
    */
  def of(
      streams: JavaKafkaStreams,
      topology: JavaTopology,
      application: String,
      admin: Admin,
      forward: java.util.function.BiFunction[HostInfo, Array[Byte], Array[Byte]]
  ): JavaQuerying = new JavaQuerying(streams, topology.describe(), application, counting(admin), forward)

  /** How many partitions each topic has, as `admin` describes it, kept once known. */
  private def counting(admin: Admin): String => Int =
    val known = TrieMap.empty[String, Int]
    topic =>
      known.getOrElseUpdate(
        topic,
        Try(admin.describeTopics(java.util.List.of(topic)).allTopicNames.get.get(topic).partitions.size).fold(
          {
            case failed: ExecutionException => throw failed.getCause
            case other                      => throw other
          },
          identity
        )
      )

  /** The member holding `partition`, of `holders` each with the partitions it holds. */
  private[interop] def holderOf(partition: Int, holders: List[(HostInfo, List[Int])]): Option[HostInfo] =
    holders.collectFirst { case (at, held) if held.contains(partition) => at }

  /** Whether `holders` hold each of `count` partitions, once each: while one moves, no member holds it. */
  private[interop] def covers(count: Int, holders: List[(HostInfo, List[Int])]): Boolean =
    val covered = holders.flatMap(_._2)
    covered.size == count && covered.toSet == (0 until count).toSet
