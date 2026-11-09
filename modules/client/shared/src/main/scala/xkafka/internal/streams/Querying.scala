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
package streams

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import cats.data.StateT
import cats.effect.{Concurrent, Ref}
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.Signal
import xkafka.streams.{Answering, Endpoint, Forward, Query, QueryProblem, QueryRequest, StreamsPhase, Subtopology}

/** A query as one member asks another: what it asks of the store, of which partitions, and whether a standby copy may answer. */
private[xkafka] final case class Asked(request: QueryRequest, partitions: List[Int], allowStandby: Boolean)

/** What a member answers to a query another forwarded: the entries found, or why it could not answer. */
private[xkafka] enum Answered derives CanEqual:
  case Found(entries: Vector[(Chunk[Byte], Chunk[Byte])])
  case Refused(problem: QueryProblem)

/** The bytes queries and their answers travel between members as: a format of xkafka's own, which a reply in any other does not match. */
private[xkafka] object QueryWire:
  private val Asking    = Chunk.array("XKQ1".getBytes(StandardCharsets.US_ASCII))
  private val Answering = Chunk.array("XKA1".getBytes(StandardCharsets.US_ASCII))

  private def long(value: Long): Chunk[Byte]                    = Chunk.array(ByteBuffer.allocate(8).putLong(value).array)
  private def int(value: Int): Chunk[Byte]                      = Chunk.array(ByteBuffer.allocate(4).putInt(value).array)
  private def sized(bytes: Chunk[Byte]): Chunk[Byte]            = int(bytes.size) ++ bytes
  private def text(value: String): Chunk[Byte]                  = sized(Chunk.array(value.getBytes(StandardCharsets.UTF_8)))
  private def optional(bytes: Option[Chunk[Byte]]): Chunk[Byte] =
    bytes.fold(Chunk.singleton(0.toByte))(present => Chunk.singleton(1.toByte) ++ sized(present))

  def encode(asked: Asked): Chunk[Byte] =
    val request =
      asked.request match
        case QueryRequest.Get(store, key)               => Chunk.singleton(1.toByte) ++ text(store) ++ sized(key)
        case QueryRequest.Range(store, from, to)        => Chunk.singleton(2.toByte) ++ text(store) ++ optional(from) ++ optional(to)
        case QueryRequest.Windows(store, key, from, to) => Chunk.singleton(3.toByte) ++ text(store) ++ optional(key) ++ long(from) ++ long(to)
        case QueryRequest.Sessions(store, key)          => Chunk.singleton(4.toByte) ++ text(store) ++ sized(key)
    Asking ++ request ++ int(asked.partitions.size) ++ Chunk.from(asked.partitions).flatMap(int) ++
      Chunk.singleton((if asked.allowStandby then 1 else 0).toByte)

  def encode(answered: Answered): Chunk[Byte] =
    answered match
      case Answered.Found(entries) => Answering ++ Chunk.singleton(0.toByte) ++ int(entries.size) ++ Chunk.from(entries)
          .flatMap((k, v) => sized(k) ++ sized(v))
      case Answered.Refused(problem) => problem match
          case QueryProblem.UnknownStore(store) => Answering ++ Chunk.singleton(2.toByte) ++ text(store)
          case _                                => Answering ++ Chunk.singleton(1.toByte)

  /** A reading of the bytes left, which fails where they end too soon. */
  private type Read[A] = StateT[Option, Chunk[Byte], A]

  private def take(count: Int): Read[Chunk[Byte]] =
    StateT(bytes => Option.when(count >= 0 && count <= bytes.size)((bytes.drop(count), bytes.take(count))))

  private val byte: Read[Byte]                 = take(1).map(_(0))
  private val number: Read[Int]                = take(4).map(read => ByteBuffer.wrap(read.toArray).getInt)
  private val wide: Read[Long]                 = take(8).map(read => ByteBuffer.wrap(read.toArray).getLong)
  private val bytes: Read[Chunk[Byte]]         = number.flatMap(take)
  private val word: Read[String]               = bytes.map(read => new String(read.toArray, StandardCharsets.UTF_8))
  private val maybe: Read[Option[Chunk[Byte]]] =
    byte.flatMap:
      case 0 => StateT.pure(None)
      case 1 => bytes.map(Some(_))
      case _ => StateT.liftF(None)

  private def many[A](each: Read[A]): Read[List[A]] = number.flatMap(count => if count < 0 then StateT.liftF(None) else each.replicateA(count))

  private def magic(expected: Chunk[Byte]): Read[Unit] = take(expected.size).flatMap(read => StateT.liftF(Option.when(read == expected)(())))

  /** What `read` reads from the whole of `bytes`, if it reads them all. */
  private def whole[A](bytes: Chunk[Byte], read: Read[A]): Option[A] = read.run(bytes).collect { case (left, value) if left.isEmpty => value }

  def asked(bytes: Chunk[Byte]): Option[Asked] =
    val request: Read[QueryRequest] =
      (byte, word).tupled.flatMap:
        case (1, store) => this.bytes.map(QueryRequest.Get(store, _))
        case (2, store) => (maybe, maybe).mapN(QueryRequest.Range(store, _, _))
        case (3, store) => (maybe, wide, wide).mapN(QueryRequest.Windows(store, _, _, _))
        case (4, store) => this.bytes.map(QueryRequest.Sessions(store, _))
        case _          => StateT.liftF(None)
    whole(bytes, magic(Asking) *> (request, many(number), byte).mapN((asked, partitions, standby) => Asked(asked, partitions, standby == 1)))

  def answered(bytes: Chunk[Byte]): Option[Answered] =
    val answer: Read[Answered] =
      byte.flatMap:
        case 0 => many((this.bytes, this.bytes).tupled).map(entries => Answered.Found(entries.toVector))
        case 1 => StateT.pure(Answered.Refused(QueryProblem.Rebalancing))
        case 2 => word.map(store => Answered.Refused(QueryProblem.UnknownStore(store)))
        case _ => StateT.liftF(None)
    whole(bytes, magic(Answering) *> answer)

/** Answers queries from this member's stores, and asks the members holding the rest through `forward`, finding them from what the group last said of
  * where each member's tasks are.
  */
private[xkafka] final class Querying[F[_]](
    application: String,
    subtopologies: Map[String, Subtopology[F]],
    globals: Map[String, TaskStore[F]],
    holding: Ref[F, Map[TaskId, Map[String, TaskStore[F]]]],
    member: Signal[F, Member],
    phase: Signal[F, StreamsPhase],
    partitions: Topic => F[Int],
    own: Option[Endpoint],
    forward: Forward[F]
)(using F: Concurrent[F]):
  /** The sub-topology keeping each store that can be queried, with the topic its partitions are numbered by and its kind. Join stores and suppression
    * buffers cannot be queried.
    */
  private val kept: Map[String, (String, String, Querying.Kind, Option[Long])] =
    subtopologies.toList.flatMap: (id, subtopology) =>
      subtopology.origins.headOption.toList.flatMap: source =>
        subtopology.kept.filter(store => !store.buffer && !store.duplicates).map: store =>
          val kind =
            if store.session then Querying.Kind.Session else if store.retention.nonEmpty then Querying.Kind.Window else Querying.Kind.KeyValue
          store.store.value -> (id, Task.topicOf(application, source), kind, store.windowSize)
    .toMap

  /** The store `request` names, where the program has one of the kind it asks of. */
  private def keptFor(request: QueryRequest): Option[(String, String)] =
    kept.get(request.storeName).collect { case (subtopology, topic, kind, _) if kind == Querying.kindOf(request) => (subtopology, topic) }

  /** The global table's store `request` names, which holds keys and values only. */
  private def globalFor(request: QueryRequest): Option[TaskStore[F]] =
    globals.get(request.storeName).filter(_ => Querying.kindOf(request) == Querying.Kind.KeyValue)

  def query[A](query: Query[F, A], allowStandby: Boolean): F[Either[QueryProblem, A]] =
    val store = query.store.value
    topicNamed(if globals.contains(store) then store else s"$application-$store-changelog").flatMap: topic =>
      query.request(topic).flatMap: request =>
        val found =
          globalFor(request) match
            case Some(global) => read(global, request).map(_.asRight[QueryProblem])
            case None         => keptFor(request)
                .fold(F.pure(QueryProblem.UnknownStore(store).asLeft[Vector[(Chunk[Byte], Chunk[Byte])]]))(routed(request, allowStandby, _))
        found.flatMap(_.traverse(query.answer(Answering(topic, kept.get(store).flatMap(_._4)), _)))

  def answer(bytes: Chunk[Byte]): F[Chunk[Byte]] =
    QueryWire.asked(bytes).fold(F.raiseError[Chunk[Byte]](new IllegalArgumentException("the request is not an xkafka query"))): asked =>
      val store = asked.request.storeName
      val found =
        globalFor(asked.request) match
          case Some(global) => read(global, asked.request).map(Answered.Found(_))
          case None         => keptFor(asked.request).fold(F.pure(Answered.Refused(QueryProblem.UnknownStore(store)))): (subtopology, topic) =>
              misrouted(asked, topic).ifM(
                F.pure(Answered.Refused(QueryProblem.Rebalancing)),
                locally(subtopology, store, asked.partitions, asked.request, asked.allowStandby)
                  .map(_.fold(Answered.Refused(QueryProblem.Rebalancing))(Answered.Found(_)))
              )
      found.map(QueryWire.encode)

  /** The entries `request` finds in `store`: what a key holds, or the keys of a range, `to` included. */
  private def read(store: TaskStore[F], request: QueryRequest): F[Vector[(Chunk[Byte], Chunk[Byte])]] =
    request match
      case QueryRequest.Get(_, key)        => store.get(key).map(_.toVector.map(held => key -> held.value))
      case QueryRequest.Range(_, from, to) => store.range(from, to.map(_ ++ Chunk.singleton(0.toByte))).map(_.map((key, held) => key -> held.value))
      // A window's key is the key and twelve bytes, its start and a sequence number; a session's is the key and sixteen, its end and start.
      case QueryRequest.Windows(_, Some(key), from, to) => store.prefixed(key).map(_.collect {
          case (stored, held) if stored.size == key.size + 12 && between(stored, from, to) => stored -> held.value
        })
      case QueryRequest.Windows(_, None, from, to) => store.timed(from, to).map(_.map((stored, held) => stored -> held.value))
      case QueryRequest.Sessions(_, key)           => store.prefixed(key).map(_.collect {
          case (stored, held) if stored.size == key.size + 16 => stored -> held.value
        })

  /** Whether the window a window store's key names starts from `from` to `to`, both included. */
  private def between(stored: Chunk[Byte], from: Long, to: Long): Boolean =
    val start = WindowStore.startOf(stored)
    start >= from && start <= to

  /** What this member's stores find for `request` in `wanted` partitions, if it holds every one of them, running, as an active task or, where
    * `allowStandby` says, a standby.
    */
  private def locally(
      subtopology: String,
      store: String,
      wanted: List[Int],
      request: QueryRequest,
      allowStandby: Boolean
  ): F[Option[Vector[(Chunk[Byte], Chunk[Byte])]]] =
    (phase.get, member.get, holding.get).tupled.flatMap: (now, joined, held) =>
      val tasks  = joined.owned.active ++ (if allowStandby then joined.owned.standby else Set.empty)
      val stores =
        wanted.traverse(partition =>
          Option.when(tasks(TaskId(subtopology, partition)))(()).flatMap(_ => held.get(TaskId(subtopology, partition)).flatMap(_.get(store)))
        )
      stores.filter(_ => now == StreamsPhase.Running)
        .traverse(_.traverse(read(_, request)).map(_.flatten.toVector.sortBy(_._1)(using StoreContents.keys)))

  /** Whether `asked` names a key the partitions it asks of do not hold. A member reckoning partitions from the group's metadata while a task moves
    * can miscount them (KAFKA-20887), and the partition it then asks of answers as if the key were not there.
    */
  private def misrouted(asked: Asked, topic: String): F[Boolean] =
    Querying.keyOf(asked.request)
      .fold(F.pure(false))(key => topicNamed(topic).flatMap(partitions).map(count => !asked.partitions.contains(Murmur2.partition(key, count))))

  /** The entries `request` finds in a store `kept` names, read here where this member holds them, and asked of the members holding the rest. */
  private def routed(
      request: QueryRequest,
      allowStandby: Boolean,
      kept: (String, String)
  ): F[Either[QueryProblem, Vector[(Chunk[Byte], Chunk[Byte])]]] =
    val (subtopology, topic) = kept
    topicNamed(topic).flatMap(partitions).flatMap: count =>
      val wanted = Querying.keyOf(request).fold((0 until count).toList)(key => List(Murmur2.partition(key, count)))
      (member.get, holding.get).tupled.flatMap: (joined, held) =>
        val here  = joined.owned.active ++ (if allowStandby then joined.owned.standby else Set.empty)
        val local =
          wanted.filter(partition =>
            here(TaskId(subtopology, partition)) && held.get(TaskId(subtopology, partition)).exists(_.contains(request.storeName))
          )
        val elsewhere = (wanted.toSet -- local).toList.sorted
        val owners    =
          elsewhere.map: partition =>
            partition -> joined.endpoints.find(holds(_, topic, partition, allowStandby)).map(at => Endpoint.from(at.endpoint.host, at.endpoint.port))
        if owners.exists((_, at) => at.forall(_.isLeft) || at.flatMap(_.toOption) == own) then F.pure(QueryProblem.Rebalancing.asLeft)
        else
          val byOwner = owners.collect { case (partition, Some(Right(at))) => at -> partition }.groupMap(_._1)(_._2).toList
          val mine    =
            if local.isEmpty then F.pure(Vector.empty[(Chunk[Byte], Chunk[Byte])].asRight[QueryProblem])
            else locally(subtopology, request.storeName, local, request, allowStandby).map(_.toRight(QueryProblem.Rebalancing))
          (mine, byOwner.traverse((at, owned) => asked(at, Asked(request, owned, allowStandby)))).tupled.map: (here, there) =>
            (here :: there).sequence.map(_.flatten.toVector.sortBy(_._1)(using StoreContents.keys))

  private def holds(at: xkafka.internal.protocol.StreamsGroup.EndpointPartitions, topic: String, partition: Int, allowStandby: Boolean): Boolean =
    (at.active ++ (if allowStandby then at.standby else Nil)).exists(held => held.topic == topic && held.partitions.contains(partition))

  /** The member at `at`'s answer to `query`. */
  private def asked(at: Endpoint, query: Asked): F[Either[QueryProblem, Vector[(Chunk[Byte], Chunk[Byte])]]] =
    forward(at, QueryWire.encode(query)).attempt.map:
      case Left(failure) => QueryProblem.Unreachable(at, Option(failure.getMessage).getOrElse(failure.toString)).asLeft
      case Right(bytes)  => QueryWire.answered(bytes) match
          case Some(Answered.Found(entries))   => entries.asRight
          case Some(Answered.Refused(problem)) => problem.asLeft
          case None                            => QueryProblem.NotXkafka(at).asLeft

  private def topicNamed(name: String): F[Topic] = Topic.from(name).leftMap(error => new KafkaException.InvalidValue(error)).liftTo[F]

private[xkafka] object Querying:
  /** The kinds of store a query asks of. */
  enum Kind derives CanEqual:
    case KeyValue, Window, Session

  /** The key `request` asks for, where it asks for one, which only the partition the key hashes to holds. */
  def keyOf(request: QueryRequest): Option[Chunk[Byte]] =
    request match
      case QueryRequest.Get(_, key)                 => Some(key)
      case QueryRequest.Windows(_, Some(key), _, _) => Some(key)
      case QueryRequest.Sessions(_, key)            => Some(key)
      case _                                        => None

  def kindOf(request: QueryRequest): Kind =
    request match
      case _: QueryRequest.Get | _: QueryRequest.Range => Kind.KeyValue
      case _: QueryRequest.Windows                     => Kind.Window
      case _: QueryRequest.Sessions                    => Kind.Session
