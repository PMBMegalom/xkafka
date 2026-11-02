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
import cats.data.NonEmptyList

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

  /** A windowed table joined with a stream, which xkafka does not join yet. */
  case WindowedJoin

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
      case WindowedJoin           => "a windowed table cannot be joined with a stream yet"

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
    * for windowed tables waiting out `grace` after each window's end: the windows' grace, and for sessions also their gap, as Java waits.
    */
  case Suppress[F[_], K, V](suppressed: Suppressed, key: TableKey[F, K], value: TableValue[F, V], grace: Option[Long]) extends Step[F, K, V, K, V]

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
      case Suppress(suppressed, key, value, grace) => Suppress(suppressed, key.mapK(fk), value.mapK(fk), grace)
      case Join(view, join)                        => Join(view.mapK(fk), join)
      case ToStream()                              => ToStream()

/** The steps between a segment's origin, whose records are keyed `K` with values `V`, and its last step, which gives `K2` and `V2`. */
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

/** Where a segment reads from: an input topic, or the repartition topic the segment before it writes. */
private[xkafka] enum Origin[F[_], K, V]:
  case Input(topic: Topic, key: Serde[F, K], value: Serde[F, V])
  case Repartitioned(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])

  def mapK[G[_]](fk: FunctionK[F, G]): Origin[G, K, V] =
    this match
      case Input(topic, key, value)               => Input(topic, key.mapK(fk), value.mapK(fk))
      case Repartitioned(repartition, key, value) => Repartitioned(repartition, key.mapK(fk), value.mapK(fk))

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
private[xkafka] final case class Segment[F[_], K, V, K2, V2](
    origin: Origin[F, K, V],
    chain: Chain[F, K, V, K2, V2],
    end: End[F, K2, V2],
    siblings: List[Segment[F, ?, ?, ?, ?]] = Nil,
    copartitioned: List[Origin[F, ?, ?]] = Nil
):
  def mapK[G[_]](fk: FunctionK[F, G]): Segment[G, K, V, K2, V2] =
    Segment(origin.mapK(fk), chain.mapK(fk), end.mapK(fk), siblings.map(_.mapK(fk)), copartitioned.map(_.mapK(fk)))

  /** It and the segments beside it. */
  def all: List[Segment[F, ?, ?, ?, ?]] = this :: siblings.flatMap(_.all)

  /** The topics the sub-topology reads, its own first. */
  def origins: List[Origin[F, ?, ?]] = all.map(_.origin)

  /** The stores of the sub-topology. */
  def stores: List[Store] = all.flatMap(_.chain.stores)

  /** The names of the sub-topology's suppression buffers. */
  def buffers: Set[String] = all.flatMap(_.chain.buffers).toSet

  /** The names of the sub-topology's session stores. */
  def sessionStores: Set[String] = all.flatMap(_.chain.sessionStores).toSet

  /** How long each window store of the sub-topology keeps a window, in milliseconds, by store name. */
  def retentions: Map[String, Long] = all.flatMap(_.chain.retentions).toMap

/** A segment still being added to, with the segments that run beside it and the topics its joins copartition. */
private[xkafka] final case class Open[F[_], K, V, K2, V2](
    origin: Origin[F, K, V],
    chain: Chain[F, K, V, K2, V2],
    siblings: List[Segment[F, ?, ?, ?, ?]] = Nil,
    copartitioned: List[Origin[F, ?, ?]] = Nil
):
  def andThen[K3, V3](step: Step[F, K2, V2, K3, V3]): Open[F, K, V, K3, V3] = Open(origin, chain.andThen(step), siblings, copartitioned)
  def close(end: End[F, K2, V2]): Segment[F, K, V, K2, V2]                  = Segment(origin, chain, end, siblings, copartitioned)

  /** The topics it and the segments beside it read. */
  def origins: List[Origin[F, ?, ?]] = origin :: siblings.flatMap(_.origins)

/** A stream of keyed records, read from a topic and changed step by step. */
final class KStream[F[_], K, V] private[xkafka] (
    private[xkafka] val closed: Vector[Segment[F, ?, ?, ?, ?]],
    private[xkafka] val open: Open[F, ?, ?, K, V],
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
    val (rows, view, sourced) = table.joined
    val (base, before, found) =
      if !keyChanged then (open, closed, Vector.empty)
      else
        repartition match
          case Some((named, key, value)) => (
              Open(Origin.Repartitioned(named, key, value), Chain.Start[F, K, V]()),
              closed :+ open.close(End.Repartitioning(named, key, value)),
              Vector.empty
            )
          case None => (open, closed, Vector(TopologyError.UnnamedJoinRepartition))
    val windowed = Option.when(view.isEmpty)(TopologyError.WindowedJoin).toVector
    // A windowed table has no view, and its join is refused, so the step that stands in for it never runs.
    val joining = base.andThen(view.fold[Step[F, K, V, K, R]](Step.FlatMap((_, _) => Nil))(Step.Join(_, join)))
    val beside  = rows.open.close(End.Stored())
    val grouped = (joining.copartitioned ++ joining.origins ++ (if sourced then rows.open.origins else Nil)).distinct
    new KStream(
      before ++ rows.closed,
      joining.copy(siblings = joining.siblings :+ beside, copartitioned = grouped),
      false,
      problems ++ rows.problems ++ found ++ windowed
    )

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

  /** Writes every record to `topic`. */
  def to(topic: Topic, key: Serde[F, K], value: Serde[F, V]): Pipeline[F] =
    Pipeline(NonEmptyList.fromListUnsafe((closed :+ open.close(End.Output(topic, key, value))).toList), problems)

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
      val closed   = stream.closed :+ stream.open.close(End.Repartitioning(named, key, value))
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
  * `value` write them, or for a suppressed table, the buffer first, a key it holds showing the value it had before the change held, and otherwise the
  * table's own view.
  */
private[xkafka] enum TableView[F[_], K, V]:
  case Stored(store: Store, key: Serde[F, K], value: Serde[F, V])
  case Held(buffer: String, parent: TableView[F, K, V])

  def mapK[G[_]](fk: FunctionK[F, G]): TableView[G, K, V] =
    this match
      case Stored(store, key, value) => Stored(store, key.mapK(fk), value.mapK(fk))
      case Held(buffer, parent)      => Held(buffer, parent.mapK(fk))

/** The latest value of each key, kept in a store. Each change to it is a `C[V]`: the new value, for a table that only ever updates its keys
  * ([[KTable.Updated]]), or for a table that can also delete them, the new value or none for a delete (`Option`).
  */
final class KTable[F[_], K, V, C[_]] private[xkafka] (
    stream: KStream[F, K, C[V]],
    key: TableKey[F, K],
    value: TableValue[F, C[V]],
    grace: Option[Long],
    view: Option[TableView[F, K, V]],
    sourced: Boolean = true
):
  /** The table's records, where a join reads its values, or none for a windowed table, and whether a join copartitions the topics the table reads,
    * which Java does for any table but a suppressed one, whose sources it takes to be the suppression, which reads no topic.
    */
  private[xkafka] def joined: (KStream[F, K, C[V]], Option[TableView[F, K, V]], Boolean) = (stream, view, sourced)

  /** Each change to the table, as a record, keyed as the table is; for a table that can delete keys, a delete is none, which `Serde.option` writes as
    * a tombstone, as Java writes its null.
    */
  def toStream: KStream[F, K, C[V]] = new KStream(stream.closed, stream.open.andThen(Step.ToStream()), false, stream.problems)

  private def suppressed(suppressed: Suppressed, waited: Option[Long]): KTable[F, K, V, C] =
    val problems = stream.problems ++ suppressed.problem.map(TopologyError.InvalidSuppression(suppressed.name, _))
    val held     = stream.step(Step.Suppress(suppressed, key, value, waited), changesKey = false)
    new KTable(
      new KStream(held.closed, held.open, stream.keyChanged, problems),
      key,
      value,
      grace,
      view.map(TableView.Held(s"${suppressed.name}-store", _)),
      sourced = false
    )

  /** Holds back each key's changes until `timeToWait` has passed since the first held, by the records' own time, then passes on the latest. */
  def suppress(until: Suppressed.UntilTimeLimit): KTable[F, K, V, C] = suppressed(until, None)

  /** Holds back each window's changes until the window has closed, its grace over, then passes on its final result; for sessions, a session merged
    * away goes on no more.
    */
  def suppress(until: Suppressed.UntilWindowCloses)(using K <:< Windowed[?]): KTable[F, K, V, C] = suppressed(until, grace)

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
  ): KTable[F, K, V, Updated] = new KTable[F, K, V, Updated](stream, key, value, grace, view)

  private[xkafka] def deleting[F[_], K, V](
      stream: KStream[F, K, Option[V]],
      key: TableKey[F, K],
      value: TableValue[F, Option[V]],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): KTable[F, K, V, Option] = new KTable[F, K, V, Option](stream, key, value, grace, view)

/** A pipeline from an input topic to an output topic. */
final class Pipeline[F[_]] private[xkafka] (
    private[xkafka] val segments: NonEmptyList[Segment[F, ?, ?, ?, ?]],
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
    val pipelines                 = NonEmptyList(first, rest.toList)
    val segments                  = pipelines.toList.flatMap(_.segments.toList).flatMap(_.all)
    val stores                    = segments.flatMap(_.chain.stores.map(_.name))
    val repartitions              = segments.map(_.origin).collect { case Origin.Repartitioned(named, _, _) if named.name.nonEmpty => named.name }
    val inputs                    = segments.map(_.origin).collect { case Origin.Input(topic, _, _) => topic }
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
