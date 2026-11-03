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

import scala.concurrent.duration.FiniteDuration

import cats.{ApplicativeThrow, Show}
import cats.arrow.FunctionK
import cats.data.{Ior, NonEmptyList, NonEmptyVector}

/** The name of a state store, from which its changelog topic `<application id>-<name>-changelog` is named. */
final case class Store(name: String)

/** The name of a repartitioning, from which its topic `<application id>-<name>-repartition` is named. */
final case class Repartition(name: String)

/** Why a topology could not be built. */
enum TopologyError derives CanEqual:
  /** A store or repartition name the topic it names could not carry. */
  case InvalidName(name: String, reason: String)

  /** Two stores with one name, whose changelogs would be one topic. */
  case DuplicateStore(name: String)

  /** Two repartitionings with one name, whose topics would be one. */
  case DuplicateRepartition(name: String)

  /** Two pipelines reading one topic, which Java refuses too. */
  case DuplicateSource(topic: Topic)

  /** Records grouped by a key changed since they were read, which needs a repartitioning that `groupByKey` was given no name for. */
  case UnnamedRepartition

  /** Windows a store's records cannot be windowed by, such as windows of no size. */
  case InvalidWindows(store: String, reason: String)

  /** A suppression that cannot be used, such as one with a negative time to wait. */
  case InvalidSuppression(name: String, reason: String)

  /** Records joined with a table by a key changed since they were read, which needs a repartitioning that `join` was given no name for. */
  case UnnamedJoinRepartition

  /** A windowed table joined with a stream or a table, which xkafka does not join yet. */
  case WindowedJoin

  /** A table or stream joined with itself, or with one read from the same topics, which xkafka does not join. */
  case JoinedWithItself

  /** The result of joining tables suppressed without a store, which gives the suppression no serde to write its values with. */
  case SuppressedUnstoredJoin(name: String)

  def message: String =
    this match
      case InvalidName(name, reason)  => s"'$name' cannot name a store or repartitioning: $reason"
      case DuplicateStore(name)       => s"two stores are named '$name'"
      case DuplicateRepartition(name) => s"two repartitionings are named '$name'"
      case DuplicateSource(topic)     => s"two pipelines read topic '${topic.value}'"
      case UnnamedRepartition => "records whose key changed are grouped by key without a repartitioning name; pass a Repartition to groupByKey"
      case InvalidWindows(store, reason)    => s"the windows of store '$store' cannot be used: $reason"
      case InvalidSuppression(name, reason) => s"the suppression '$name' cannot be used: $reason"
      case UnnamedJoinRepartition => "records whose key changed are joined with a table without a repartitioning name; pass a Repartition to join"
      case WindowedJoin           => "a windowed table cannot be joined yet"
      case JoinedWithItself       => "a table or stream is joined with itself, or with one read from the same topics"
      case SuppressedUnstoredJoin(name) =>
        s"the suppression '$name' holds the result of joining tables, which needs a serde for its values; pass a Store and a Serde to the join"

object TopologyError:
  given Show[TopologyError] = Show.show(_.message)

  /** Carries every reason a topology could not be built into `F`. */
  extension [A](result: Either[NonEmptyList[TopologyError], A])
    def liftTo[F[_]](using F: ApplicativeThrow[F]): F[A] = F.fromEither(result.left.map(InvalidTopology(_)))

/** A topology that could not be built, with every reason it could not. */
final class InvalidTopology(val errors: NonEmptyList[TopologyError]) extends RuntimeException(errors.toList.map(_.message).mkString("; "))

/** What a pipeline does to a record keyed `K` with a value `V`, giving records keyed `K2` with values `V2`. */
private[xkafka] enum Step[F[_], K, V, K2, V2]:
  case Filter[F[_], K, V](keep: (K, V) => Boolean)                       extends Step[F, K, V, K, V]
  case Map[F[_], K, V, K2, V2](change: (K, V) => (K2, V2))               extends Step[F, K, V, K2, V2]
  case MapValues[F[_], K, V, V2](change: V => V2)                        extends Step[F, K, V, K, V2]
  case FlatMap[F[_], K, V, K2, V2](change: (K, V) => Iterable[(K2, V2)]) extends Step[F, K, V, K2, V2]
  case Peek[F[_], K, V](effect: (K, V) => F[Unit])                       extends Step[F, K, V, K, V]
  case EvalMapValues[F[_], K, V, V2](change: V => F[V2])                 extends Step[F, K, V, K, V2]
  case EvalFilter[F[_], K, V](keep: (K, V) => F[Boolean])                extends Step[F, K, V, K, V]
  case EvalMap[F[_], K, V, K2, V2](change: (K, V) => F[(K2, V2)])        extends Step[F, K, V, K2, V2]

  /** Keeps each key's latest value in `store`, as Java's table read from a topic does: none deletes the key, and a record without a key is skipped.
    * Each change goes on with the value it replaced.
    */
  case TableSource[F[_], K, V](store: Store, key: Serde[F, K], value: Serde[F, V]) extends Step[F, Option[K], Option[V], K, Option[V]]

  /** Folds each key's records into `store`: a key's first record becomes `start`, and each later one is added with `add`. Each update goes on with
    * the value before it, or `initial`, which is none for a reduction, as Java passes on the old value of a table's change.
    */
  case Aggregate[F[_], K, V, A](store: Store, key: Serde[F, K], value: Serde[F, A], initial: Option[A], start: (K, V) => A, add: (K, V, A) => A)
      extends Step[F, K, V, K, A]

  /** Folds each key's records into `store`, once for each of `windows` a record falls in, as for `Aggregate`, keeping each window for `retention`
    * milliseconds after its start, passing on each update, or with `emitOnClose` only each window's final value once it has closed.
    */
  case WindowedAggregate[F[_], K, V, A](
      store: Store,
      windows: TimeWindows,
      retention: Long,
      emitOnClose: Boolean,
      key: Serde[F, K],
      value: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ) extends Step[F, K, V, Windowed[K], A]

  /** Folds each key's records into `store` in sliding `windows`, as Java's sliding window aggregation does, as for `WindowedAggregate`. */
  case SlidingAggregate[F[_], K, V, A](
      store: Store,
      windows: SlidingWindows,
      retention: Long,
      emitOnClose: Boolean,
      key: Serde[F, K],
      value: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ) extends Step[F, K, V, Windowed[K], A]

  /** Folds each key's records into sessions in `store`, as Java's session window aggregation does: the sessions a record joins are folded from
    * `initial`, or the first of them where there is none, with `merge`, then the record is added, or starts the value where nothing came before. A
    * session merged away goes on as none.
    */
  case SessionAggregate[F[_], K, V, A](
      store: Store,
      windows: SessionWindows,
      retention: Long,
      emitOnClose: Boolean,
      key: Serde[F, K],
      value: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A,
      merge: (K, A, A) => A
  ) extends Step[F, K, V, Windowed[K], Option[A]]

  /** Holds back the table's changes in the buffer `<name>-store`, as Java's `suppress`, the table's keys and values written as `key` and `value` say,
    * each until `release` lets it go on.
    */
  case Suppress[F[_], K, V](suppressed: Suppressed, key: TableKey[F, K], value: TableValue[F, V], release: Release[K]) extends Step[F, K, V, K, V]

  /** Each record with what `join` makes of it and its key's value in the table `view` reads, or none if the table holds no value for the key, as
    * Java's stream-table join reads the table when the record comes; a record `join` makes nothing of is dropped.
    */
  case Join[F[_], K, V, TV, R](view: TableView[F, K, TV], join: (V, Option[TV]) => Option[R]) extends Step[F, K, V, K, R]

  /** Each change to the table, as a record. */
  case ToStream[F[_], K, V]() extends Step[F, K, V, K, V]

  def mapK[G[_]](fk: FunctionK[F, G]): Step[G, K, V, K2, V2] =
    this match
      case Filter(keep)                                      => Filter(keep)
      case Map(change)                                       => Map(change)
      case MapValues(change)                                 => MapValues(change)
      case FlatMap(change)                                   => FlatMap(change)
      case Peek(effect)                                      => Peek((key, value) => fk(effect(key, value)))
      case EvalMapValues(change)                             => EvalMapValues(value => fk(change(value)))
      case EvalFilter(keep)                                  => EvalFilter((key, value) => fk(keep(key, value)))
      case EvalMap(change)                                   => EvalMap((key, value) => fk(change(key, value)))
      case Aggregate(store, key, value, initial, start, add) => Aggregate(store, key.mapK(fk), value.mapK(fk), initial, start, add)
      case TableSource(store, key, value)                    => TableSource(store, key.mapK(fk), value.mapK(fk))
      case WindowedAggregate(store, windows, retention, emitOnClose, key, value, initial, start, add) =>
        WindowedAggregate(store, windows, retention, emitOnClose, key.mapK(fk), value.mapK(fk), initial, start, add)
      case SlidingAggregate(store, windows, retention, emitOnClose, key, value, initial, start, add) =>
        SlidingAggregate(store, windows, retention, emitOnClose, key.mapK(fk), value.mapK(fk), initial, start, add)
      case SessionAggregate(store, windows, retention, emitOnClose, key, value, initial, start, add, merge) =>
        SessionAggregate(store, windows, retention, emitOnClose, key.mapK(fk), value.mapK(fk), initial, start, add, merge)
      case Suppress(suppressed, key, value, release) => Suppress(suppressed, key.mapK(fk), value.mapK(fk), release)
      case Join(view, join)                          => Join(view.mapK(fk), join)
      case ToStream()                                => ToStream()

/** The steps between a segment's origin, whose records are keyed `K` with values `V`, and its last step, which gives `K2` and `V2`. */
/** When a suppression lets a held change to a key go on. */
private[xkafka] enum Release[K]:
  /** Once the window the key names, which `end` gives the end of, has closed: `grace` after its end, the windows' grace, and for sessions also their
    * gap, as Java waits.
    */
  case WindowCloses(end: K => Long, grace: Long)

  /** Once `waited` milliseconds have passed since the first change held, by the records' own time. */
  case TimeLimit(waited: Long)

private[xkafka] enum Chain[F[_], K, V, K2, V2]:
  case Start[F[_], K, V]()                                                                           extends Chain[F, K, V, K, V]
  case Then[F[_], K, V, K1, V1, K2, V2](init: Chain[F, K, V, K1, V1], last: Step[F, K1, V1, K2, V2]) extends Chain[F, K, V, K2, V2]

  def andThen[K3, V3](step: Step[F, K2, V2, K3, V3]): Chain[F, K, V, K3, V3] = Then(this, step)

  def mapK[G[_]](fk: FunctionK[F, G]): Chain[G, K, V, K2, V2] =
    this match
      case Start()          => Start()
      case Then(init, last) => Then(init.mapK(fk), last.mapK(fk))

  /** The stores of its steps, in the order the steps come. */
  def stores: List[Store] =
    this match
      case Start()          => Nil
      case Then(init, last) => init.stores ++
          (last match
            case Step.Aggregate(store, _, _, _, _, _)                    => List(store)
            case Step.TableSource(store, _, _)                           => List(store)
            case Step.WindowedAggregate(store, _, _, _, _, _, _, _, _)   => List(store)
            case Step.SlidingAggregate(store, _, _, _, _, _, _, _, _)    => List(store)
            case Step.SessionAggregate(store, _, _, _, _, _, _, _, _, _) => List(store)
            case Step.Suppress(suppressed, _, _, _)                      => List(Store(s"${suppressed.name}-store"))
            case _                                                       => Nil)

  /** The names of its steps' suppression buffers. */
  def buffers: Set[String] =
    this match
      case Start()          => Set.empty
      case Then(init, last) => init.buffers ++
          (last match
            case Step.Suppress(suppressed, _, _, _) => Set(s"${suppressed.name}-store")
            case _                                  => Set.empty)

  /** The names of its steps' session stores. */
  def sessionStores: Set[String] =
    this match
      case Start()          => Set.empty
      case Then(init, last) => init.sessionStores ++
          (last match
            case Step.SessionAggregate(store, _, _, _, _, _, _, _, _, _) => Set(store.name)
            case _                                                       => Set.empty)

  /** How long each window store of its steps keeps a window, in milliseconds, by store name. */
  def retentions: Map[String, Long] =
    this match
      case Start()          => Map.empty
      case Then(init, last) => init.retentions ++
          (last match
            case Step.WindowedAggregate(store, _, retention, _, _, _, _, _, _)   => Map(store.name -> retention)
            case Step.SlidingAggregate(store, _, retention, _, _, _, _, _, _)    => Map(store.name -> retention)
            case Step.SessionAggregate(store, _, retention, _, _, _, _, _, _, _) => Map(store.name -> retention)
            case _                                                               => Map.empty)

/** A topic a segment reads, by name: an input topic, or the repartition topic `repartition` names. */
private[xkafka] enum Source derives CanEqual:
  case Input(topic: Topic)
  case Repartitioned(repartition: Repartition)

/** Where a segment reads from: an input topic, the repartition topic the segment before it writes, or two tables joined, each change to either going
  * on as a change to their join, as Java's table-table join merges what its two sides make.
  */
private[xkafka] enum Origin[F[_], K, V]:
  case Input(topic: Topic, key: Serde[F, K], value: Serde[F, V])
  case Repartitioned(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])

  /** The tables `left` and `right` joined by `joiner`, the result kept in `store` where one is given, written with its serdes. */
  case Joined[F[_], K, A, B, R, K1, V1, C1, K2, V2, C2](
      left: Side[F, K, A, K1, V1, C1],
      right: Side[F, K, B, K2, V2, C2],
      joiner: Joiner[A, B, R],
      store: Option[(Store, Serde[F, K], Serde[F, R])]
  ) extends Origin[F, K, Option[R]]

  /** The streams `left` and `right` joined by `joiner` within `windows`, as Java's stream-stream join: each record kept in a window store of its side
    * and joined with the other side's records in its window, its stores named from `store` as [[StreamJoin]] names them, and written with `key`,
    * `value` and `otherValue`.
    */
  case StreamsJoined[F[_], K, A, B, R, K1, V1, K2, V2](
      left: Flow[F, K, A, K1, V1],
      right: Flow[F, K, B, K2, V2],
      windows: JoinWindows,
      joiner: Joiner[A, B, R],
      store: Store,
      key: Serde[F, K],
      value: Serde[F, A],
      otherValue: Serde[F, B]
  ) extends Origin[F, K, R]

  def mapK[G[_]](fk: FunctionK[F, G]): Origin[G, K, V] =
    this match
      case Input(topic, key, value)               => Input(topic, key.mapK(fk), value.mapK(fk))
      case Repartitioned(repartition, key, value) => Repartitioned(repartition, key.mapK(fk), value.mapK(fk))
      case Joined(left, right, joiner, store)     =>
        Joined(left.mapK(fk), right.mapK(fk), joiner, store.map((named, key, value) => (named, key.mapK(fk), value.mapK(fk))))
      case StreamsJoined(left, right, windows, joiner, store, key, value, otherValue) =>
        StreamsJoined(left.mapK(fk), right.mapK(fk), windows, joiner, store, key.mapK(fk), value.mapK(fk), otherValue.mapK(fk))

  /** The topics it reads, through the tables it joins. */
  def sources: List[Source] =
    this match
      case Input(topic, _, _)                           => List(Source.Input(topic))
      case Repartitioned(repartition, _, _)             => List(Source.Repartitioned(repartition))
      case Joined(left, right, _, _)                    => left.origin.sources ++ right.origin.sources
      case StreamsJoined(left, right, _, _, _, _, _, _) => left.origin.sources ++ right.origin.sources

  /** The stores of the tables it joins, and those its joins keep their results in. */
  def stores: List[Store] =
    this match
      case Joined(left, right, _, store) => left.origin.stores ++ left.chain.stores ++ right.origin.stores ++ right.chain.stores ++ store.map(_._1)
      case StreamsJoined(left, right, _, joiner, store, _, _, _) =>
        val named = StreamJoin.names(store, joiner)
        left.origin.stores ++ left.chain.stores ++ right.origin.stores ++ right.chain.stores ++ (List(named.left, named.right) ++ named.shared)
          .map(Store(_))
      case _ => Nil

  /** The names of the suppression buffers of the tables it joins. */
  def buffers: Set[String] =
    this match
      case Joined(left, right, _, _)                    => left.origin.buffers ++ left.chain.buffers ++ right.origin.buffers ++ right.chain.buffers
      case StreamsJoined(left, right, _, _, _, _, _, _) => left.origin.buffers ++ left.chain.buffers ++ right.origin.buffers ++ right.chain.buffers
      case _                                            => Set.empty

  /** The names of the session stores of the tables it joins. */
  def sessionStores: Set[String] =
    this match
      case Joined(left, right, _, _) => left.origin.sessionStores ++ left.chain.sessionStores ++ right.origin.sessionStores ++
          right.chain.sessionStores
      case StreamsJoined(left, right, _, _, _, _, _, _) => left.origin.sessionStores ++ left.chain.sessionStores ++ right.origin.sessionStores ++
          right.chain.sessionStores
      case _ => Set.empty

  /** The window stores of its stream-stream joins, and of those it reads through, which keep every record. */
  def duplicates: Set[String] =
    this match
      case Joined(left, right, _, _)                             => left.origin.duplicates ++ right.origin.duplicates
      case StreamsJoined(left, right, _, joiner, store, _, _, _) =>
        val named = StreamJoin.names(store, joiner)
        left.origin.duplicates ++ right.origin.duplicates + named.left + named.right
      case _ => Set.empty

  /** How long each window store of the tables it joins keeps a window, in milliseconds, by store name. */
  def retentions: Map[String, Long] =
    this match
      case Joined(left, right, _, _) => left.origin.retentions ++ left.chain.retentions ++ right.origin.retentions ++ right.chain.retentions
      case StreamsJoined(left, right, windows, joiner, store, _, _, _) =>
        val named = StreamJoin.names(store, joiner)
        left.origin.retentions ++ left.chain.retentions ++ right.origin.retentions ++ right.chain.retentions ++
          Map(named.left -> windows.retentionMillis, named.right -> windows.retentionMillis)
      case _ => Map.empty

/** One side of a table-table join: the table's changes, a `C` each, read from `origin` through `chain`, `present` giving each change's value, or none
  * for a delete, and where the other side reads the table's value for a key, or none for a windowed table.
  */
private[xkafka] final case class Side[F[_], K, A, K0, V0, C](
    origin: Origin[F, K0, V0],
    chain: Chain[F, K0, V0, K, C],
    present: C => Option[A],
    view: Option[TableView[F, K, A]]
):
  def mapK[G[_]](fk: FunctionK[F, G]): Side[G, K, A, K0, V0, C] = Side(origin.mapK(fk), chain.mapK(fk), present, view.map(_.mapK(fk)))

/** One side of a stream-stream join: its records, read from `origin` through `chain`. */
private[xkafka] final case class Flow[F[_], K, A, K0, V0](origin: Origin[F, K0, V0], chain: Chain[F, K0, V0, K, A]):
  def mapK[G[_]](fk: FunctionK[F, G]): Flow[G, K, A, K0, V0] = Flow(origin.mapK(fk), chain.mapK(fk))

/** The stores of a stream-stream join, named as Java names them from the name `withStoreName` gives it: a window store for each side's records, and
  * for a left or outer join a store of the records that matched nothing yet, shared by both sides.
  */
private[xkafka] final case class StreamJoin(left: String, right: String, shared: Option[String])

private[xkafka] object StreamJoin:
  /** Java's suffixes: `-outer-this-join` for the left side's store where the right's unmatched records go on, `-outer-other-join` for the right's
    * where the left's do, and the shared store `-left-shared-join` for a left join and `-outer-shared-join` for an outer join.
    */
  def names[A, B, R](store: Store, joiner: Joiner[A, B, R]): StreamJoin =
    val (leftOuter, rightOuter) = joiner.outer
    StreamJoin(
      if rightOuter then s"${store.name}-outer-this-join-store" else s"${store.name}-this-join-store",
      if leftOuter then s"${store.name}-outer-other-join-store" else s"${store.name}-other-join-store",
      Option.when(leftOuter)(if rightOuter then s"${store.name}-outer-shared-join-store" else s"${store.name}-left-shared-join-store")
    )

/** How a join makes its result from the values of its sides, as Java's inner, left and outer joins call their joiners: an inner join with both, a
  * left join with the left and the right if any, and an outer join with either or both.
  */
private[xkafka] enum Joiner[A, B, R]:
  case Inner(join: (A, B) => R)
  case Left(join: (A, Option[B]) => R)
  case Outer(join: Ior[A, B] => R)

  /** Whether the joiner takes the left's and the right's values without the other side's: the left's in a left join, either in an outer join. Java's
    * table-table join needs the old values of those sides' changes, and its stream-stream join holds back their records that match nothing.
    */
  def outer: (Boolean, Boolean) =
    this match
      case Inner(_) => (false, false)
      case Left(_)  => (true, false)
      case Outer(_) => (true, true)

  /** What it makes of a left and a right value that match. */
  def both(a: A, b: B): R =
    this match
      case Inner(join) => join(a, b)
      case Left(join)  => join(a, Some(b))
      case Outer(join) => join(Ior.Both(a, b))

  /** What it makes of a left value that matches nothing, where it takes one alone. */
  def leftAlone(a: A): Option[R] =
    this match
      case Inner(_)    => None
      case Left(join)  => Some(join(a, None))
      case Outer(join) => Some(join(Ior.Left(a)))

  /** What it makes of a right value that matches nothing, where it takes one alone. */
  def rightAlone(b: B): Option[R] =
    this match
      case Outer(join) => Some(join(Ior.Right(b)))
      case _           => None

/** Where a segment writes to: the repartition topic the next segment reads, or the pipeline's output topic. */
private[xkafka] enum End[F[_], K, V]:
  case Repartitioning(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])
  case Output(topic: Topic, key: Serde[F, K], value: Serde[F, V])

  /** Nowhere: a segment that runs beside another, only to keep its stores, as a table read by a join is kept. */
  case Stored()

  def mapK[G[_]](fk: FunctionK[F, G]): End[G, K, V] =
    this match
      case Stored()                                => Stored()
      case Repartitioning(repartition, key, value) => Repartitioning(repartition, key.mapK(fk), value.mapK(fk))
      case Output(topic, key, value)               => Output(topic, key.mapK(fk), value.mapK(fk))

/** The records between two topics, which become one sub-topology, with the segments that run beside it in that sub-topology, each reading a topic of
  * its own and sharing its stores, as the table a join reads does, and the topics its joins copartition, as Java's join groups its sources.
  */
private[xkafka] sealed trait Segment[F[_]]:
  /** The key and value its origin reads. */
  type K
  type V

  /** The key and value its chain writes. */
  type K2
  type V2

  def origin: Origin[F, K, V]
  def chain: Chain[F, K, V, K2, V2]
  def end: End[F, K2, V2]
  def siblings: List[Segment[F]]
  def copartitioned: List[Source]

  /** The joins it holds, by the topics they read, whose sub-topologies Java numbers by which of their inputs its program builds first. */
  def buildOrdered: List[List[Source]]

  def mapK[G[_]](fk: FunctionK[F, G]): Segment[G] =
    Segment(origin.mapK(fk), chain.mapK(fk), end.mapK(fk), siblings.map(_.mapK(fk)), copartitioned, buildOrdered)

  /** It and the segments beside it. */
  def all: List[Segment[F]] = this :: siblings.flatMap(_.all)

  /** The topics the sub-topology reads, its own first. */
  def origins: List[Source] = all.flatMap(_.origin.sources)

  /** The stores of the sub-topology. */
  def stores: List[Store] = all.flatMap(segment => segment.origin.stores ++ segment.chain.stores)

  /** The names of the sub-topology's suppression buffers. */
  def buffers: Set[String] = all.flatMap(segment => segment.origin.buffers ++ segment.chain.buffers).toSet

  /** The names of the sub-topology's session stores. */
  def sessionStores: Set[String] = all.flatMap(segment => segment.origin.sessionStores ++ segment.chain.sessionStores).toSet

  /** How long each window store of the sub-topology keeps a window, in milliseconds, by store name. */
  def retentions: Map[String, Long] = all.flatMap(segment => segment.origin.retentions ++ segment.chain.retentions).toMap

  /** The window stores of the sub-topology's stream-stream joins, which keep every record. */
  def duplicates: Set[String] = all.flatMap(_.origin.duplicates).toSet

private[xkafka] object Segment:
  def apply[F[_], K0, V0, K20, V20](
      origin: Origin[F, K0, V0],
      chain: Chain[F, K0, V0, K20, V20],
      end: End[F, K20, V20],
      siblings: List[Segment[F]] = Nil,
      copartitioned: List[Source] = Nil,
      buildOrdered: List[List[Source]] = Nil
  ): Segment[F] = Of(origin, chain, end, siblings, copartitioned, buildOrdered)

  private final case class Of[F[_], K0, V0, K20, V20](
      origin: Origin[F, K0, V0],
      chain: Chain[F, K0, V0, K20, V20],
      end: End[F, K20, V20],
      siblings: List[Segment[F]],
      copartitioned: List[Source],
      buildOrdered: List[List[Source]]
  ) extends Segment[F]:
    type K  = K0
    type V  = V0
    type K2 = K20
    type V2 = V20

/** A segment still being added to, writing `K2` and `V2` so far, with the segments that run beside it and the topics its joins copartition. */
private[xkafka] sealed trait Open[F[_], K2, V2]:
  /** The key and value its origin reads. */
  type K
  type V

  def origin: Origin[F, K, V]
  def chain: Chain[F, K, V, K2, V2]
  def siblings: List[Segment[F]]
  def copartitioned: List[Source]

  /** The segments numbered after it once it closes, which a join it reads as it is made before it, as [[Joining]] orders them. */
  def later: Vector[Segment[F]]

  /** The joins it holds, by the topics they read, whose sub-topologies Java numbers by which of their inputs its program builds first. */
  def buildOrdered: List[List[Source]]

  def andThen[K3, V3](step: Step[F, K2, V2, K3, V3]): Open[F, K3, V3] =
    Open(origin, chain.andThen(step), siblings, copartitioned, later, buildOrdered)

  /** It closed by `end`, and the segments numbered after it. */
  def close(end: End[F, K2, V2]): NonEmptyVector[Segment[F]] = NonEmptyVector(beside(end), later)

  /** It closed by `end` as a segment beside another, which takes the segments numbered after it. */
  def beside(end: End[F, K2, V2]): Segment[F] = Segment(origin, chain, end, siblings, copartitioned, buildOrdered)

  /** The same segment, holding the join `joined` orders, with `siblings` beside it and its joins copartitioning `copartitioned`. */
  def joining(siblings: List[Segment[F]], copartitioned: List[Source], joined: Joining.Ordered[F]): Open[F, K2, V2] =
    Open(origin, chain, siblings, copartitioned, joined.after, (buildOrdered ++ joined.buildOrdered).distinct)

  /** The topics it and the segments beside it read. */
  def origins: List[Source] = origin.sources ++ siblings.flatMap(_.origins)

private[xkafka] object Open:
  def apply[F[_], K0, V0, K2, V2](
      origin: Origin[F, K0, V0],
      chain: Chain[F, K0, V0, K2, V2],
      siblings: List[Segment[F]] = Nil,
      copartitioned: List[Source] = Nil,
      later: Vector[Segment[F]] = Vector.empty[Segment[F]],
      buildOrdered: List[List[Source]] = Nil
  ): Open[F, K2, V2] = Of(origin, chain, siblings, copartitioned, later, buildOrdered)

  private final case class Of[F[_], K0, V0, K2, V2](
      origin: Origin[F, K0, V0],
      chain: Chain[F, K0, V0, K2, V2],
      siblings: List[Segment[F]],
      copartitioned: List[Source],
      later: Vector[Segment[F]],
      buildOrdered: List[List[Source]]
  ) extends Open[F, K2, V2]:
    type K = K0
    type V = V0

/** Where the segments of a join's two inputs go, so that their sub-topologies are numbered as Java numbers them: by the first node of each that it
  * writes, which it writes in about the order the program makes them, the receiver of the join first. The join's sub-topology takes its place from
  * the receiver where the join reads it as it is, since it holds the receiver's source, and otherwise from the other side; a side the join
  * repartitions comes before it, since Java makes that repartition topic's source only when it makes the join.
  */
private[xkafka] object Joining:
  /** One side of a join: the segments made before the part of it the join reads and those numbered after it, or, where the join repartitions the
    * side, every segment it made; and the joins it holds whose numbering depends on how Java's program builds them.
    */
  final case class Part[F[_]](before: Vector[Segment[F]], after: Vector[Segment[F]], repartitioned: Boolean, buildOrdered: List[List[Source]])

  /** Where a join's segments go: those numbered before its own, those after, and the joins whose numbering depends on how Java's program builds them,
    * this one included where building its other input first would number them otherwise.
    */
  final case class Ordered[F[_]](before: Vector[Segment[F]], after: Vector[Segment[F]], buildOrdered: List[List[Source]])

  /** A side the join reads as it is, through `open`, after the segments `closed`. */
  def read[F[_], K, V](closed: Vector[Segment[F]], open: Open[F, K, V]): Part[F] = Part(closed, open.later, false, open.buildOrdered)

  /** A side the join repartitions, every segment of it, the last writing to the repartition topic. */
  def repartitioned[F[_]](segments: Vector[Segment[F]]): Part[F] = Part(segments, Vector.empty, true, Nil)

  /** The segments of a join reading `sources` whose receiver Java's program builds before its other input, and whether building the other first would
    * number them otherwise.
    */
  def ordered[F[_]](mine: Part[F], theirs: Part[F], sources: List[Source]): Ordered[F] =
    def first(built: Part[F], next: Part[F]) =
      if !built.repartitioned then (built.before, built.after ++ next.before ++ next.after) else (built.before ++ next.before, next.after)
    val (before, after)                                    = first(mine, theirs)
    val (other, behind)                                    = first(theirs, mine)
    def same(x: Vector[Segment[F]], y: Vector[Segment[F]]) = x.corresponds(y)(_ eq _)
    val unsure                                             = Option.unless(same(before, other) && same(after, behind))(sources).toList
    Ordered(before, after, (mine.buildOrdered ++ theirs.buildOrdered ++ unsure).distinct)

final class KStream[F[_], K, V] private[xkafka] (
    private[xkafka] val closed: Vector[Segment[F]],
    private[xkafka] val open: Open[F, K, V],
    private[xkafka] val keyChanged: Boolean,
    private[xkafka] val problems: Vector[TopologyError]
):
  private[xkafka] def step[K2, V2](next: Step[F, K, V, K2, V2], changesKey: Boolean): KStream[F, K2, V2] =
    new KStream(closed, open.andThen(next), keyChanged || changesKey, problems)

  def filter(keep: (K, V) => Boolean): KStream[F, K, V] = step(Step.Filter(keep), changesKey = false)

  def map[K2, V2](change: (K, V) => (K2, V2)): KStream[F, K2, V2] = step(Step.Map(change), changesKey = true)

  def mapValues[V2](change: V => V2): KStream[F, K, V2] = step(Step.MapValues(change), changesKey = false)

  def flatMap[K2, V2](change: (K, V) => Iterable[(K2, V2)]): KStream[F, K2, V2] = step(Step.FlatMap(change), changesKey = true)

  def selectKey[K2](change: (K, V) => K2): KStream[F, K2, V] = step(Step.Map((k, v) => (change(k, v), v)), changesKey = true)

  /** Runs `effect` for each record and passes the record on. The effect runs again for records replayed after a crash or a fencing. */
  def peek(effect: (K, V) => F[Unit]): KStream[F, K, V] = step(Step.Peek(effect), changesKey = false)

  /** Replaces each value with what `change` gives. Its output is written once under exactly-once processing, but the effect runs again for records
    * replayed after a crash or a fencing.
    */
  def evalMapValues[V2](change: V => F[V2]): KStream[F, K, V2] = step(Step.EvalMapValues(change), changesKey = false)

  /** Keeps the records `keep` gives true for. The effect runs again for records replayed after a crash or a fencing. */
  def evalFilter(keep: (K, V) => F[Boolean]): KStream[F, K, V] = step(Step.EvalFilter(keep), changesKey = false)

  /** Replaces each record with what `change` gives. Its output is written once under exactly-once processing, but the effect runs again for records
    * replayed after a crash or a fencing.
    */
  def evalMap[K2, V2](change: (K, V) => F[(K2, V2)]): KStream[F, K2, V2] = step(Step.EvalMap(change), changesKey = true)

  /** Groups records by a new key, through the repartition topic `repartition` names. */
  def groupBy[K2](repartition: Repartition, key: Serde[F, K2], value: Serde[F, V])(change: (K, V) => K2): KGroupedStream[F, K2, V] =
    KGroupedStream(selectKey(change), Some(repartition), key, value)

  /** Groups records by their key, as long as it has not changed since they were read. */
  def groupByKey(key: Serde[F, K], value: Serde[F, V]): KGroupedStream[F, K, V] = KGroupedStream(this, None, key, value)

  /** Groups records by their key, through the repartition topic `repartition` names if the key has changed since they were read. */
  def groupByKey(repartition: Repartition, key: Serde[F, K], value: Serde[F, V]): KGroupedStream[F, K, V] =
    KGroupedStream(this, Some(repartition), key, value)

  /** Each record with what `join` makes of it and its key's value in `table`, or none, read as Java's stream-table join reads it when the record
    * comes; the table runs in the same sub-topology, so its topic and the stream's are copartitioned. A key changed since the records were read goes
    * through the repartition topic `repartition` names first.
    */
  private def joined[TV, C[_], R](
      table: KTable[F, K, TV, C],
      repartition: Option[(Repartition, Serde[F, K], Serde[F, V])],
      join: (V, Option[TV]) => Option[R]
  ): KStream[F, K, R] =
    val (rows, view, sources) = table.joined
    val (base, mine, found)   =
      if !keyChanged then (open, Joining.read(closed, open), Vector.empty)
      else
        repartition match
          case Some((named, key, value)) => (
              Open(Origin.Repartitioned(named, key, value), Chain.Start[F, K, V]()),
              Joining.repartitioned(closed ++ open.close(End.Repartitioning(named, key, value)).toVector),
              Vector.empty
            )
          case None => (open, Joining.read(closed, open), Vector(TopologyError.UnnamedJoinRepartition))
    val windowed = Option.when(view.isEmpty)(TopologyError.WindowedJoin).toVector
    // A windowed table has no view, and its join is refused, so the step that stands in for it never runs.
    val stepped = base.andThen(view.fold[Step[F, K, V, K, R]](Step.FlatMap((_, _) => Nil))(Step.Join(_, join)))
    val beside  = rows.open.beside(End.Stored())
    val grouped = (stepped.copartitioned ++ stepped.origins ++ sources).distinct
    val joined  = Joining.ordered(mine, Joining.read(rows.closed, rows.open), grouped)
    new KStream(joined.before, stepped.joining(stepped.siblings :+ beside, grouped, joined), false, problems ++ rows.problems ++ found ++ windowed)

  /** Each record joined with its key's value in `table`, as Java's `join` of a stream with a table: a record whose key the table holds no value for
    * is dropped. The records' key must not have changed since they were read, so that the table and they are partitioned alike.
    */
  def join[TV, C[_], R](table: KTable[F, K, TV, C])(join: (V, TV) => R): KStream[F, K, R] = joined(table, None, (v, tv) => tv.map(join(v, _)))

  /** As `join`, after the repartition topic `repartition` names where the records' key has changed since they were read. */
  def join[TV, C[_], R](table: KTable[F, K, TV, C], repartition: Repartition, key: Serde[F, K], value: Serde[F, V])(
      join: (V, TV) => R
  ): KStream[F, K, R] = joined(table, Some((repartition, key, value)), (v, tv) => tv.map(join(v, _)))

  /** Each record joined with its key's value in `table`, or none where the table holds no value for it, as Java's `leftJoin` of a stream with a
    * table.
    */
  def leftJoin[TV, C[_], R](table: KTable[F, K, TV, C])(join: (V, Option[TV]) => R): KStream[F, K, R] =
    joined(table, None, (v, tv) => Some(join(v, tv)))

  /** As `leftJoin`, after the repartition topic `repartition` names where the records' key has changed since they were read. */
  def leftJoin[TV, C[_], R](table: KTable[F, K, TV, C], repartition: Repartition, key: Serde[F, K], value: Serde[F, V])(
      join: (V, Option[TV]) => R
  ): KStream[F, K, R] = joined(table, Some((repartition, key, value)), (v, tv) => Some(join(v, tv)))

  /** The topics the stream reads, through every segment that makes it. */
  private def read: Set[Source] = (closed.flatMap(_.origins) ++ open.origins).toSet

  /** The stream as one side of a stream-stream join, with the segments closed before it: as it is, or where its key has changed, read from the
    * repartition topic `repartition` names, which the stream is written to with `key` and `value`, as Java repartitions each side of the join.
    */
  private def joinSide(repartition: Repartition, key: Serde[F, K], value: Serde[F, V]): (Joining.Part[F], Open[F, K, V]) =
    if !keyChanged then (Joining.read(closed, open), open)
    else
      (
        Joining.repartitioned(closed ++ open.close(End.Repartitioning(repartition, key, value)).toVector),
        Open(Origin.Repartitioned(repartition, key, value), Chain.Start[F, K, V]())
      )

  /** The stream joined with `other` by `joiner` within `windows`, as Java's stream-stream joins join them: in one sub-topology, their topics
    * copartitioned, each side repartitioned through `<store>-left` or `<store>-right` where its key has changed.
    */
  private def windowJoined[VO, R](
      other: KStream[F, K, VO],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO],
      joiner: Joiner[V, VO, R]
  ): KStream[F, K, R] =
    val problems = this.problems ++ other.problems ++ windows.problem.map(TopologyError.InvalidWindows(store.name, _))
    if (read & other.read).nonEmpty then
      // The join is refused, so the step that stands in for it never runs.
      val nothing = step(Step.FlatMap[F, K, V, K, R]((_, _) => Nil), changesKey = false)
      new KStream(nothing.closed, nothing.open, false, problems :+ TopologyError.JoinedWithItself)
    else
      val (mine, left)    = joinSide(Repartition(s"${store.name}-left"), key, value)
      val (theirs, right) = other.joinSide(Repartition(s"${store.name}-right"), key, otherValue)
      val origin          =
        Origin.StreamsJoined(Flow(left.origin, left.chain), Flow(right.origin, right.chain), windows, joiner, store, key, value, otherValue)
      val groups = (left.copartitioned ++ right.copartitioned ++ left.origins ++ right.origins).distinct
      val joined = Joining.ordered(mine, theirs, groups)
      new KStream(joined.before, Open(origin, Chain.Start[F, K, R]()).joining(left.siblings ++ right.siblings, groups, joined), false, problems)

  /** Each record joined with each record of `other` with the same key in its window, as Java's `join` of two streams: a record with no match in
    * `other` goes on no further. The stores `<store>-this-join-store` and `<store>-other-join-store` keep each side's records for the windows' size
    * and grace, written with `key`, `value` and `otherValue`, which also write a side whose key has changed to the repartition topic `<store>-left`
    * or `<store>-right` names. The result's time is the later of the two records'.
    */
  def join[VO, R](other: KStream[F, K, VO], windows: JoinWindows, store: Store, key: Serde[F, K], value: Serde[F, V], otherValue: Serde[F, VO])(
      join: (V, VO) => R
  ): KStream[F, K, R] = windowJoined(other, windows, store, key, value, otherValue, Joiner.Inner(join))

  /** As `join`, but a record with no match in `other` also goes on, with none for it, once its window has closed, as Java's `leftJoin` of two streams
    * holds it in the store `<store>-left-shared-join-store` until then. The store of `other`'s records is `<store>-outer-other-join-store`.
    */
  def leftJoin[VO, R](other: KStream[F, K, VO], windows: JoinWindows, store: Store, key: Serde[F, K], value: Serde[F, V], otherValue: Serde[F, VO])(
      join: (V, Option[VO]) => R
  ): KStream[F, K, R] = windowJoined(other, windows, store, key, value, otherValue, Joiner.Left(join))

  /** As `join`, but a record of either stream with no match in the other also goes on alone once its window has closed, as Java's `outerJoin` of two
    * streams holds it in the store `<store>-outer-shared-join-store` until then. The sides' stores are `<store>-outer-this-join-store` and
    * `<store>-outer-other-join-store`.
    */
  def outerJoin[VO, R](other: KStream[F, K, VO], windows: JoinWindows, store: Store, key: Serde[F, K], value: Serde[F, V], otherValue: Serde[F, VO])(
      join: Ior[V, VO] => R
  ): KStream[F, K, R] = windowJoined(other, windows, store, key, value, otherValue, Joiner.Outer(join))

  /** Writes every record to `topic`. */
  def to(topic: Topic, key: Serde[F, K], value: Serde[F, V]): Pipeline[F] =
    val written = open.close(End.Output(topic, key, value))
    Pipeline(NonEmptyList(written.head, written.tail.toList).prependList(closed.toList), problems)

object KStream:
  /** The records of `topic`. */
  def apply[F[_], K, V](topic: Topic, key: Serde[F, K], value: Serde[F, V]): KStream[F, K, V] =
    new KStream(Vector.empty, Open(Origin.Input(topic, key, value), Chain.Start()), false, Vector.empty)

/** Records grouped by key, ready to be folded into a store. */
final class KGroupedStream[F[_], K, V] private[xkafka] (
    stream: KStream[F, K, V],
    repartition: Option[Repartition],
    key: Serde[F, K],
    value: Serde[F, V]
):
  private def aggregated[A](
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): KTable[F, K, A, KTable.Updated] =
    KTable.updating(
      folded(Step.Aggregate(store, key, serde, initial, start, add), Vector.empty),
      TableKey.Plain(key),
      TableValue.Plain(serde),
      None,
      Some(TableView.Stored(store, key, serde))
    )

  /** The stream with `fold` as its last step, after a repartition topic if the key has changed. */
  private[xkafka] def folded[K2, A](fold: Step[F, K, V, K2, A], found: Vector[TopologyError]): KStream[F, K2, A] =
    if !stream.keyChanged then
      val folding = stream.step(fold, changesKey = false)
      new KStream(folding.closed, folding.open, false, folding.problems ++ found)
    else
      // The records go through a repartition topic first, so the store is in the segment that reads it.
      val named    = repartition.getOrElse(Repartition(""))
      val problems = (if repartition.isEmpty then stream.problems :+ TopologyError.UnnamedRepartition else stream.problems) ++ found
      val closed   = stream.closed ++ stream.open.close(End.Repartitioning(named, key, value)).toVector
      new KStream(closed, Open(Origin.Repartitioned(named, key, value), Chain.Start[F, K, V]().andThen(fold)), false, problems)

  /** Counts each key's records in `store`. */
  def count(store: Store)(using F: ApplicativeThrow[F]): KTable[F, K, Long, KTable.Updated] =
    aggregated(store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in `store`, starting from its first. */
  def reduce(store: Store)(combine: (V, V) => V): KTable[F, K, V, KTable.Updated] =
    aggregated(store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values into `store`, starting from `initial`. */
  def aggregate[A](store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A): KTable[F, K, A, KTable.Updated] =
    aggregated(store, serde, Some(initial), (k, v) => add(k, v, initial), add)

  /** The records split by key and by each of `windows` they fall in, ready to be folded into a window store. */
  def windowedBy(windows: TimeWindows): TimeWindowedKStream[F, K, V] = TimeWindowedKStream(this, windows, None, false, key, value)

  /** The records split by key into sessions of activity, ready to be folded into a session store. */
  def windowedBy(windows: SessionWindows): SessionWindowedKStream[F, K, V] = SessionWindowedKStream(this, windows, None, false, key, value)

  /** The records split by key and by each sliding window they fall in, ready to be folded into a window store. */
  def windowedBy(windows: SlidingWindows): TimeWindowedKStream[F, K, V] = TimeWindowedKStream(this, windows, None, false, key, value)

/** Records grouped by key and window, ready to be folded into a window store. Each update to a window goes downstream as it happens. A record later
  * than its window's end by more than the windows' grace is dropped, and a window is kept for its size and grace after its start.
  */
final class TimeWindowedKStream[F[_], K, V] private[xkafka] (
    grouped: KGroupedStream[F, K, V],
    windows: TimeWindows | SlidingWindows,
    retention: Option[FiniteDuration],
    emitOnClose: Boolean,
    key: Serde[F, K],
    value: Serde[F, V]
):
  private def aggregated[A](
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): KTable[F, Windowed[K], A, KTable.Updated] =
    val (least, rule, problem) =
      windows match
        case time: TimeWindows       => (time.retentionMillis, time.retentionRule, time.problem)
        case sliding: SlidingWindows => (sliding.retentionMillis, sliding.retentionRule, sliding.problem)
    val kept =
      problem.orElse:
        retention.flatMap: asked =>
          if asked.toNanos % 1000000L != 0L then Some("the retention must be whole milliseconds")
          else Option.when(asked.toMillis < least)(s"the retention must be at least $rule, ${least} ms, was $asked")
    val problems = kept.map(TopologyError.InvalidWindows(store.name, _)).toVector
    val keep     = retention.fold(least)(_.toMillis)
    val fold     =
      windows match
        case time: TimeWindows       => Step.WindowedAggregate(store, time, keep, emitOnClose, key, serde, initial, start, add)
        case sliding: SlidingWindows => Step.SlidingAggregate(store, sliding, keep, emitOnClose, key, serde, initial, start, add)
    val (size, grace) =
      windows match
        case time: TimeWindows       => (time.sizeMillis, time.graceMillis)
        case sliding: SlidingWindows => (sliding.differenceMillis, sliding.graceMillis)
    KTable.updating(grouped.folded(fold, problems), TableKey.TimeWindowed(key, size), TableValue.Plain(serde), Some(grace))

  /** Keeps each window in the store for `retention` after its start, which must be at least the windows' size and grace, their default. A window that
    * emits once it closes may expire before it is emitted when the retention is no longer than that, as in Java Kafka Streams, so keep it longer
    * there.
    */
  def withRetention(retention: FiniteDuration): TimeWindowedKStream[F, K, V] =
    TimeWindowedKStream(grouped, windows, Some(retention), emitOnClose, key, value)

  /** Passes on only each window's final value, once the window has closed, in place of every update. Closed windows are looked for as records arrive,
    * at most once a second, as Java Kafka Streams looks for them, so a window's value goes downstream with the first record after it closes once that
    * second has passed.
    */
  def emitOnWindowClose: TimeWindowedKStream[F, K, V] = TimeWindowedKStream(grouped, windows, retention, true, key, value)

  /** Counts each key's records in each window in `store`. */
  def count(store: Store)(using F: ApplicativeThrow[F]): KTable[F, Windowed[K], Long, KTable.Updated] =
    aggregated(store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in each window in `store`, starting from the window's first. */
  def reduce(store: Store)(combine: (V, V) => V): KTable[F, Windowed[K], V, KTable.Updated] =
    aggregated(store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values in each window into `store`, starting each window from `initial`. */
  def aggregate[A](store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A): KTable[F, Windowed[K], A, KTable.Updated] =
    aggregated(store, serde, Some(initial), (k, v) => add(k, v, initial), add)

/** Records grouped by key into sessions, ready to be folded into a session store. Each session's value goes downstream as it changes, and a session
  * merged into another goes on as none, as Java sends a session merged away as a tombstone. A record later than its session's end by more than the
  * inactivity gap and grace is dropped, and a session is kept for the gap and grace after its end.
  */
final class SessionWindowedKStream[F[_], K, V] private[xkafka] (
    grouped: KGroupedStream[F, K, V],
    windows: SessionWindows,
    retention: Option[FiniteDuration],
    emitOnClose: Boolean,
    key: Serde[F, K],
    value: Serde[F, V]
):
  private def aggregated[A](store: Store, serde: Serde[F, A], initial: Option[A], start: (K, V) => A, add: (K, V, A) => A, merge: (K, A, A) => A) =
    val least = windows.retentionMillis
    val kept  =
      windows.problem.orElse:
        retention.flatMap: asked =>
          if asked.toNanos % 1000000L != 0L then Some("the retention must be whole milliseconds")
          else Option.when(asked.toMillis < least)(s"the retention must be at least ${windows.retentionRule}, ${least} ms, was $asked")
    val problems = kept.map(TopologyError.InvalidWindows(store.name, _)).toVector
    val fold     = Step.SessionAggregate(store, windows, retention.fold(least)(_.toMillis), emitOnClose, key, serde, initial, start, add, merge)
    KTable.deleting(
      grouped.folded(fold, problems),
      TableKey.SessionWindowed(key),
      TableValue.Optional(serde),
      Some(windows.graceMillis + windows.gapMillis)
    )

  /** Counts each key's records in each session in `store`. */
  def count(store: Store)(using F: ApplicativeThrow[F]): KTable[F, Windowed[K], Long, Option] =
    aggregated(store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1, (_, a, b) => a + b)

  /** Combines each key's values in each session in `store`, starting from the session's first, and merging sessions with `combine` too. */
  def reduce(store: Store)(combine: (V, V) => V): KTable[F, Windowed[K], V, Option] =
    aggregated(store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v), (_, acc, session) => combine(acc, session))

  /** Folds each key's values in each session into `store` from `initial`, merging two sessions' values with `merge` where a record joins them. */
  def aggregate[A](store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A)(merge: (K, A, A) => A): KTable[F, Windowed[K], A, Option] =
    aggregated(store, serde, Some(initial), (k, v) => add(k, v, initial), add, merge)

  /** Keeps each session in the store for `retention` after its end, which must be at least the inactivity gap and grace, their default. */
  def withRetention(retention: FiniteDuration): SessionWindowedKStream[F, K, V] =
    SessionWindowedKStream(grouped, windows, Some(retention), emitOnClose, key, value)

  /** Passes on only each session's final value once it has closed, as for time windows. */
  def emitOnWindowClose: SessionWindowedKStream[F, K, V] = SessionWindowedKStream(grouped, windows, retention, true, key, value)

/** How a table's keys are written in a suppression's buffer: as the table's key serde writes them, or, for a windowed table, as Java's windowed
  * serdes write a windowed key.
  */
private[xkafka] enum TableKey[F[_], K]:
  case Plain[F[_], K](key: Serde[F, K])                    extends TableKey[F, K]
  case TimeWindowed[F[_], K](key: Serde[F, K], size: Long) extends TableKey[F, Windowed[K]]
  case SessionWindowed[F[_], K](key: Serde[F, K])          extends TableKey[F, Windowed[K]]

  def mapK[G[_]](fk: FunctionK[F, G]): TableKey[G, K] =
    this match
      case Plain(key)              => Plain(key.mapK(fk))
      case TimeWindowed(key, size) => TimeWindowed(key.mapK(fk), size)
      case SessionWindowed(key)    => SessionWindowed(key.mapK(fk))

/** How a table's values are written in a suppression's buffer: as the table's value serde writes them, or, for a session table, with none written as
  * a tombstone.
  */
private[xkafka] enum TableValue[F[_], V]:
  case Plain[F[_], V](value: Serde[F, V])    extends TableValue[F, V]
  case Optional[F[_], A](value: Serde[F, A]) extends TableValue[F, Option[A]]

  def mapK[G[_]](fk: FunctionK[F, G]): TableValue[G, V] =
    this match
      case Plain(value)    => Plain(value.mapK(fk))
      case Optional(value) => Optional(value.mapK(fk))

/** Where a join reads a table's value for a key, as Java's value getter does: the store holding the table, its keys and values written as `key` and
  * `value` write them; for a suppressed table, the buffer first, a key it holds showing the value it had before the change held, and otherwise the
  * table's own view; or for the result of joining tables kept in no store, what `joiner` makes of the values its sides' views read.
  */
private[xkafka] enum TableView[F[_], K, V]:
  case Stored(store: Store, key: Serde[F, K], value: Serde[F, V])
  case Held(buffer: String, parent: TableView[F, K, V])
  case Joined[F[_], K, A, B, R](left: TableView[F, K, A], right: TableView[F, K, B], joiner: Joiner[A, B, R]) extends TableView[F, K, R]

  def mapK[G[_]](fk: FunctionK[F, G]): TableView[G, K, V] =
    this match
      case Stored(store, key, value)   => Stored(store, key.mapK(fk), value.mapK(fk))
      case Held(buffer, parent)        => Held(buffer, parent.mapK(fk))
      case Joined(left, right, joiner) => Joined(left.mapK(fk), right.mapK(fk), joiner)

  /** The serde its table's keys are written with. */
  def keys: Serde[F, K] =
    this match
      case Stored(_, key, _)  => key
      case Held(_, parent)    => parent.keys
      case Joined(left, _, _) => left.keys

/** The latest value of each key, kept in a store. Each change to it is a `C[V]`: the new value, for a table that only ever updates its keys
  * ([[KTable.Updated]]), or for a table that can also delete them, the new value or none for a delete (`Option`).
  */
final class KTable[F[_], K, V, C[_]] private[xkafka] (
    stream: KStream[F, K, C[V]],
    key: TableKey[F, K],
    value: Option[TableValue[F, C[V]]],
    grace: Option[Long],
    view: Option[TableView[F, K, V]],
    present: C[V] => Option[V],
    sourcedFrom: Option[List[Source]] = None
):
  /** The topics a join copartitions for the table: those it reads, or for a suppressed table none, as Java takes its sources to be the suppression,
    * which reads no topic, and for the result of joining tables those its sides give.
    */
  private def sources: List[Source] = sourcedFrom.getOrElse(stream.open.origins)

  /** The table's records, where a join reads its values, or none for a windowed table, and the topics a join copartitions for it. */
  private[xkafka] def joined: (KStream[F, K, C[V]], Option[TableView[F, K, V]], List[Source]) = (stream, view, sources)

  /** Each change to the table, as a record, keyed as the table is; for a table that can delete keys, a delete is none, which `Serde.option` writes as
    * a tombstone, as Java writes its null.
    */
  def toStream: KStream[F, K, C[V]] = new KStream(stream.closed, stream.open.andThen(Step.ToStream()), false, stream.problems)

  private def suppressed(suppressed: Suppressed, release: Release[K]): KTable[F, K, V, C] =
    val unstored = Option.when(value.isEmpty)(TopologyError.SuppressedUnstoredJoin(suppressed.name))
    val problems = stream.problems ++ suppressed.problem.map(TopologyError.InvalidSuppression(suppressed.name, _)) ++ unstored
    // A join's result kept in no store has no serde to hold its values with, and its suppression is refused, so the step that stands in never runs.
    val holding = value.fold[Step[F, K, C[V], K, C[V]]](Step.FlatMap((_, _) => Nil))(Step.Suppress(suppressed, key, _, release))
    val held    = stream.step(holding, changesKey = false)
    new KTable(
      new KStream(held.closed, held.open, stream.keyChanged, problems),
      key,
      value,
      grace,
      view.map(TableView.Held(s"${suppressed.name}-store", _)),
      present,
      Some(Nil)
    )

  /** Holds back each key's changes until `timeToWait` has passed since the first held, by the records' own time, then passes on the latest. */
  def suppress(until: Suppressed.UntilTimeLimit): KTable[F, K, V, C] = suppressed(until, Release.TimeLimit(until.timeToWait.toMillis))

  /** Holds back each window's changes until the window has closed, its grace over, then passes on its final result; for sessions, a session merged
    * away goes on no more.
    */
  def suppress[W](until: Suppressed.UntilWindowCloses)(using windowed: K <:< Windowed[W]): KTable[F, K, V, C] =
    suppressed(until, Release.WindowCloses(windowed(_).window.end, grace.getOrElse(0L)))

  /** The table as one side of a join. */
  private def side(open: Open[F, K, C[V]]): Side[F, K, V, open.K, open.V, C[V]] = Side(open.origin, open.chain, present, view)

  /** The topics the table reads, through every segment that makes it. */
  private def read: Set[Source] = (stream.closed.flatMap(_.origins) ++ stream.open.origins).toSet

  /** The table joined with `other` by `joiner`, as Java's table-table joins join them: in one sub-topology, their topics copartitioned, and each
    * change to either joined with the other's value for its key. The result is kept in the store `store` names where one is given.
    */
  private def joinedWith[VO, C2[_], R](
      other: KTable[F, K, VO, C2],
      joiner: Joiner[V, VO, R],
      store: Option[(Store, Serde[F, R])]
  ): KTable[F, K, R, Option] =
    val (theirs, otherView, otherSources) = other.joined
    val problems                          = stream.problems ++ theirs.problems
    (view, otherView) match
      case (Some(mine), Some(their)) if (read & other.read).isEmpty =>
        val origin = Origin.Joined(side(stream.open), other.side(theirs.open), joiner, store.map((named, serde) => (named, mine.keys, serde)))
        val groups = (stream.open.copartitioned ++ theirs.open.copartitioned ++ sources ++ otherSources).distinct
        val joined = Joining.ordered(Joining.read(stream.closed, stream.open), Joining.read(theirs.closed, theirs.open), groups)
        val open   = Open(origin, Chain.Start[F, K, Option[R]]()).joining(stream.open.siblings ++ theirs.open.siblings, groups, joined)
        val values =
          store.fold[TableView[F, K, R]](TableView.Joined(mine, their, joiner))((named, serde) => TableView.Stored(named, mine.keys, serde))
        new KTable[F, K, R, Option](
          new KStream(joined.before, open, false, problems),
          key,
          store.map((_, serde) => TableValue.Optional(serde)),
          None,
          Some(values),
          identity,
          Some((sources ++ otherSources).distinct)
        )
      case _ =>
        val refused = if view.isEmpty || otherView.isEmpty then TopologyError.WindowedJoin else TopologyError.JoinedWithItself
        // The join is refused, so the step that stands in for it never runs.
        val nothing = stream.step(Step.FlatMap[F, K, C[V], K, Option[R]]((_, _) => Nil), changesKey = false)
        new KTable[F, K, R, Option](new KStream(nothing.closed, nothing.open, false, problems :+ refused), key, None, None, None, identity)

  /** The table joined with `other`, as Java's `join` of two tables: each key both hold a value for has what `join` makes of the two, and a change to
    * either side changes it. A delete on either side deletes the key from the result, as Java passes on a delete even for a key the result never
    * held. The result is kept in no store; the result of a join that is suppressed needs one, given with the other `join`.
    */
  def join[VO, C2[_], R](other: KTable[F, K, VO, C2])(join: (V, VO) => R): KTable[F, K, R, Option] = joinedWith(other, Joiner.Inner(join), None)

  /** As `join`, the result kept in `store`, its values written with `serde`, as Java's join given a `Materialized` store. */
  def join[VO, C2[_], R](other: KTable[F, K, VO, C2], store: Store, serde: Serde[F, R])(join: (V, VO) => R): KTable[F, K, R, Option] =
    joinedWith(other, Joiner.Inner(join), Some((store, serde)))

  /** The table joined with `other`, as Java's `leftJoin` of two tables: each key this table holds a value for has what `join` makes of it and
    * `other`'s value for the key, if any.
    */
  def leftJoin[VO, C2[_], R](other: KTable[F, K, VO, C2])(join: (V, Option[VO]) => R): KTable[F, K, R, Option] =
    joinedWith(other, Joiner.Left(join), None)

  /** As `leftJoin`, the result kept in `store`, its values written with `serde`. */
  def leftJoin[VO, C2[_], R](other: KTable[F, K, VO, C2], store: Store, serde: Serde[F, R])(join: (V, Option[VO]) => R): KTable[F, K, R, Option] =
    joinedWith(other, Joiner.Left(join), Some((store, serde)))

  /** The table joined with `other`, as Java's `outerJoin` of two tables: each key either table holds a value for has what `join` makes of this
    * table's value, `other`'s, or both.
    */
  def outerJoin[VO, C2[_], R](other: KTable[F, K, VO, C2])(join: Ior[V, VO] => R): KTable[F, K, R, Option] =
    joinedWith(other, Joiner.Outer(join), None)

  /** As `outerJoin`, the result kept in `store`, its values written with `serde`. */
  def outerJoin[VO, C2[_], R](other: KTable[F, K, VO, C2], store: Store, serde: Serde[F, R])(join: Ior[V, VO] => R): KTable[F, K, R, Option] =
    joinedWith(other, Joiner.Outer(join), Some((store, serde)))

object KTable:
  /** A change to a table that only ever updates its keys: the new value. */
  type Updated[A] = A

  /** The records of `topic` as a table, kept in `store`, as Java's `StreamsBuilder.table` reads it: each record's value replaces its key's, and a
    * record without a value deletes the key. A record without a key is skipped.
    */
  def apply[F[_], K, V](topic: Topic, key: Serde[F, K], value: Serde[F, V], store: Store)(using F: cats.Applicative[F]): KTable[F, K, V, Option] =
    deleting(
      KStream(topic, key.option, value.option).step(Step.TableSource(store, key, value), changesKey = false),
      TableKey.Plain(key),
      TableValue.Optional(value),
      None,
      Some(TableView.Stored(store, key, value))
    )

  private[xkafka] def updating[F[_], K, V](
      stream: KStream[F, K, V],
      key: TableKey[F, K],
      value: TableValue[F, V],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): KTable[F, K, V, Updated] = new KTable[F, K, V, Updated](stream, key, Some(value), grace, view, Some(_))

  private[xkafka] def deleting[F[_], K, V](
      stream: KStream[F, K, Option[V]],
      key: TableKey[F, K],
      value: TableValue[F, Option[V]],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): KTable[F, K, V, Option] = new KTable[F, K, V, Option](stream, key, Some(value), grace, view, identity)

/** A pipeline from an input topic to an output topic. */
final class Pipeline[F[_]] private[xkafka] (
    private[xkafka] val segments: NonEmptyList[Segment[F]],
    private[xkafka] val problems: Vector[TopologyError]
):
  def mapK[G[_]](fk: FunctionK[F, G]): Pipeline[G] = new Pipeline(segments.map(_.mapK(fk)), problems)

/** A streams program: its pipelines, in the order a Java program building the same topology makes them, which numbers their sub-topologies. */
final class Topology[F[_]] private (private[xkafka] val pipelines: NonEmptyList[Pipeline[F]]):
  /** The same program with its effects in `G`, as its serdes and effectful steps are mapped. */
  def mapK[G[_]](fk: FunctionK[F, G]): Topology[G] = new Topology(pipelines.map(_.mapK(fk)))

object Topology:
  /** The longest store or repartition name, as Java allows. */
  val MaxNameLength = 249

  def of[F[_]](first: Pipeline[F], rest: Pipeline[F]*): Either[NonEmptyList[TopologyError], Topology[F]] =
    val pipelines    = NonEmptyList(first, rest.toList)
    val segments     = pipelines.toList.flatMap(_.segments.toList).flatMap(_.all)
    val stores       = segments.flatMap(segment => segment.origin.stores ++ segment.chain.stores).map(_.name)
    val repartitions = segments.flatMap(_.origin.sources).collect { case Source.Repartitioned(named) if named.name.nonEmpty => named.name }
    val inputs       = segments.flatMap(_.origin.sources).collect { case Source.Input(topic) => topic }
    def twice[A](values: List[A]) = values.groupBy(identity).collect { case (value, all) if all.size > 1 => value }.toList
    val problems                  =
      pipelines.toList.flatMap(_.problems) ++ (stores ++ repartitions).flatMap(invalid) ++
        twice(stores).sorted.map(TopologyError.DuplicateStore(_)) ++ twice(repartitions).sorted.map(TopologyError.DuplicateRepartition(_)) ++
        twice(inputs).sortBy(_.value).map(TopologyError.DuplicateSource(_))
    NonEmptyList.fromList(problems.distinct).toLeft(new Topology(pipelines))

  private def invalid(name: String): Option[TopologyError] =
    def legal(c: Char) =
      (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
    if name.isEmpty then Some(TopologyError.InvalidName(name, "it is empty"))
    else if name.length > MaxNameLength then Some(TopologyError.InvalidName(name, s"it is longer than $MaxNameLength characters"))
    else if !name.forall(legal) then Some(TopologyError.InvalidName(name, "it may contain only [a-zA-Z0-9._-]"))
    else None
