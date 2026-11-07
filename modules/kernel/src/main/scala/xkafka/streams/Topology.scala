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

import cats.{Applicative, ApplicativeThrow, MonadThrow, Show}
import cats.arrow.FunctionK
import cats.data.{Ior, NonEmptyList}
import cats.effect.std.Console
import cats.syntax.all.*
import cats.tagless.InvariantK
import fs2.Chunk

/** Why a topology could not be built. */
enum TopologyError derives CanEqual:
  /** A store, repartition or step name that the topic it names could not carry, or that Java refuses as a name. */
  case InvalidName(name: String, reason: String)

  /** Two steps with one name, which Java refuses too. */
  case DuplicateName(name: String)

  /** Two stores with one name, whose changelogs would be one topic. */
  case DuplicateStore(name: String)

  /** Two repartitionings with one name, whose topics would be one. */
  case DuplicateRepartition(name: String)

  /** One topic read twice, which Java refuses too. A stream's steps are written where it is used, so a stream used twice reads its topic twice. */
  case DuplicateSource(topic: Topic)

  /** A repartition named, by `groupByKey` or by a join given serdes, or made by a flow that does not know the key of what it is sent, for records
    * that are all keyed as they were read and need none.
    */
  case UnusedRepartition(name: String)

  /** Windows a store's records cannot be windowed by, such as windows of no size. */
  case InvalidWindows(store: String, reason: String)

  /** A suppression that cannot be used, such as one with a negative time to wait. */
  case InvalidSuppression(name: String, reason: String)

  /** A punctuator that cannot be used, such as one with an interval below a millisecond. */
  case InvalidPunctuation(step: String, reason: String)

  /** A window or session store that cannot be used, such as one whose retention is less than its window size. */
  case InvalidStore(store: String, reason: String)

  /** A windowed table joined with a stream or a table, which xkafka does not join yet, or a foreign-key join's result kept in no store joined. */
  case WindowedJoin

  /** The result of the foreign-key join `join`, kept in no store, read by a step that needs the value each change replaces. */
  case UnkeptForeignKeyJoin(join: String)

  /** A join reading a table that nothing sends records to: one made from a flow, whose flow goes nowhere. `step` is the step that makes the table. */
  case UnfedTable(step: String)

  /** A table suppressed whose values have no serde to hold them with: the result of a table join or `mapValues` given no store. */
  case SuppressedWithoutSerde(name: String)

  def message: String =
    this match
      case InvalidName(name, reason)  => s"'$name' cannot name a store, repartitioning or step: $reason"
      case DuplicateName(name)        => s"two steps are named '$name'"
      case DuplicateStore(name)       => s"two stores are named '$name'"
      case DuplicateRepartition(name) => s"two repartitionings are named '$name'"
      case DuplicateSource(topic)     =>
        s"topic '${topic.value}' is read twice; to send a stream down several paths, use into or fork, and to read it again, make one stream of it"
      case UnusedRepartition(name) =>
        s"the records repartitioned as '$name' are keyed as they were read and need no repartition; group them with groupByKey(key, value), join them without serdes, or start the flow with Flow.asRead"
      case InvalidWindows(store, reason)    => s"the windows of store '$store' cannot be used: $reason"
      case InvalidSuppression(name, reason) => s"the suppression '$name' cannot be used: $reason"
      case InvalidPunctuation(step, reason) => s"a punctuator of step '$step' cannot be used: $reason"
      case InvalidStore(store, reason)      => s"the store '$store' cannot be used: $reason"
      case WindowedJoin                     => "a windowed table, or a foreign-key join's result kept in no store, cannot be joined"
      case UnkeptForeignKeyJoin(join)       => s"the result of the foreign-key join '$join' must be kept in a store for what reads it"
      case UnfedTable(step) => s"a join reads the table made in step '$step', but no records reach that step; send them to it with into or fork"
      case SuppressedWithoutSerde(name) =>
        s"the suppression '$name' holds a table whose values have no serde; pass a Store and a Serde to the join or mapValues that makes it"

object TopologyError:
  given Show[TopologyError] = Show.show(_.message)

  /** Carries every reason a topology could not be built into `F`. */
  extension [A](result: Either[NonEmptyList[TopologyError], A])
    def liftTo[F[_]](using F: ApplicativeThrow[F]): F[A] = F.fromEither(result.left.map(InvalidTopology(_)))

/** A topology that could not be built, with every reason it could not. */
final class InvalidTopology(val errors: NonEmptyList[TopologyError]) extends RuntimeException(errors.toList.map(_.message).mkString("; "))

/** What a pipeline does to a record keyed `K` with a value `V`, giving records keyed `K2` with values `V2`. */
private[xkafka] enum Step[F[_], K, V, K2, V2]:
  case Filter[F[_], K, V](keep: (K, V) => Boolean)                                                     extends Step[F, K, V, K, V]
  case Map[F[_], K, V, K2, V2](change: (K, V) => (K2, V2))                                             extends Step[F, K, V, K2, V2]
  case MapValues[F[_], K, V, V2](change: V => V2)                                                      extends Step[F, K, V, K, V2]
  case FlatMap[F[_], K, V, K2, V2](change: (K, V) => Iterable[(K2, V2)])                               extends Step[F, K, V, K2, V2]
  case FlatMapValues[F[_], K, V, V2](change: V => Iterable[V2])                                        extends Step[F, K, V, K, V2]
  case Peek[F[_], K, V](effect: (K, V) => F[Unit])                                                     extends Step[F, K, V, K, V]
  case EvalMapValues[F[_], K, V, V2](change: V => F[V2])                                               extends Step[F, K, V, K, V2]
  case EvalFilter[F[_], K, V](keep: (K, V) => F[Boolean])                                              extends Step[F, K, V, K, V]
  case EvalMap[F[_], K, V, K2, V2](change: (K, V) => F[(K2, V2)])                                      extends Step[F, K, V, K2, V2]
  case RecordFilter[F[_], K, V](keep: Record[K, V] => Boolean)                                         extends Step[F, K, V, K, V]
  case RecordPeek[F[_], K, V](effect: Record[K, V] => F[Unit])                                         extends Step[F, K, V, K, V]
  case EvalRecordFilter[F[_], K, V](keep: Record[K, V] => F[Boolean])                                  extends Step[F, K, V, K, V]
  case RecordMap[F[_], K, V, K2, V2](change: Record[K, V] => Iterable[Record[K2, V2]])                 extends Step[F, K, V, K2, V2]
  case EvalRecordMap[F[_], K, V, A, K2, V2](change: Record[K, V] => F[A], result: A => Record[K2, V2]) extends Step[F, K, V, K2, V2]

  /** Keeps each key's latest value in `store`, as Java's table read from a topic does: none deletes the key, and a record without a key is skipped.
    * Each change goes on with the value it replaced.
    */
  case TableSource[F[_], K, V](store: Store, key: Serde[F, K], value: Serde[F, V]) extends Step[F, Option[K], Option[V], K, Option[V]]

  /** Keeps each key's latest value in `store`, as Java's table made from a stream does: none deletes the key, and a key its serializer writes as none
    * is skipped. Each change goes on with the value it replaced.
    */
  case ToTable[F[_], K, I, V](lift: I => Option[V], store: Store, key: Serde[F, K], value: Serde[F, V]) extends Step[F, K, I, K, Option[V]]

  /** Each change to a table, its value kept where `keep` says and otherwise none, as Java's table filter makes it, kept in the store given, if any. A
    * change that had no value and still has none goes no further.
    */
  case TableFilter[F[_], K, C, V](keep: (K, V) => Boolean, present: C => Option[V], store: Option[(Store, TableKey[F, K], Serde[F, V])])
      extends Step[F, K, C, K, Option[V]]

  /** Each change to a table, its value changed by `mapped`, as Java's table `mapValues` makes it, kept in the store given, if any. */
  case TableMapValues[F[_], K, CA, CB, B](
      mapped: CA => CB,
      present: CB => Option[B],
      pure: B => CB,
      store: Option[(Store, TableKey[F, K], Serde[F, B])]
  ) extends Step[F, K, CA, K, CB]

  /** Folds each key's records into `store`: a key's first record becomes `start`, and each later one is added with `add`. Each update goes on with
    * the value before it, or `initial`, which is none for a reduction, as Java passes on the old value of a table's change.
    */
  case Aggregate[F[_], K, V, A](store: Store, key: Serde[F, K], value: Serde[F, A], initial: Option[A], start: (K, V) => A, add: (K, V, A) => A)
      extends Step[F, K, V, K, A]

  /** Each change to a table as what it changes in the groups `select` puts its values in, as Java's table `groupBy` makes it: one change where the
    * value stays in its group, and otherwise the old value leaving its group, then the new one joining its own.
    */
  case TableGroupBy[F[_], K, C, K2, V2](grouped: (K, C, Option[C]) => Vector[(K2, V2)]) extends Step[F, K, C, K2, V2]

  /** Folds each key's changes into `store`, as Java's grouped table aggregation does: the value leaving is taken out with `subtract` and the value
    * joining put in with `add`, or for a key holding nothing, with `start`. A value leaving a key holding nothing leaves it holding nothing, which
    * goes on as a delete, as Java's does.
    */
  case TableAggregate[F[_], K, V, A](
      store: Store,
      key: Serde[F, K],
      value: Serde[F, A],
      start: (K, V) => A,
      add: (K, V, A) => A,
      subtract: (K, V, A) => A
  ) extends Step[F, K, TableChange[V], K, Option[A]]

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

  /** Each record with what `join` makes of it and the value `table` holds for the key `select` gives, or none if it holds none, as Java's join of a
    * stream with a global table reads it when the record comes; a record `join` makes nothing of is dropped.
    */
  case GlobalJoin[F[_], K, V, GK, GV, R](table: GlobalKTable[F, GK, GV], select: (K, V) => GK, join: (V, Option[GV]) => Option[R])
      extends Step[F, K, V, K, R]

  /** The fold `step` as one side of a cogroup, numbered `side`: each folds into one store, which the first keeps, and has a stream time of its own,
    * as each of Java's processors does.
    */
  case Cogrouped[F[_], K, V, K2, V2](side: Int, step: Step[F, K, V, K2, V2]) extends Step[F, K, V, K2, V2]

  /** Each record with what `processor` makes of it, reading and writing the stores it declares, under a new key or, if `keepsKey`, under its own. */
  case Process[F[_], K, V, K2, V2](processor: Processor[F, K, V, K2, V2], keepsKey: Boolean) extends Step[F, K, V, K2, V2]

  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Step[G, K, V, K2, V2] =
    this match
      case Filter(keep)                                            => Filter(keep)
      case Map(change)                                             => Map(change)
      case MapValues(change)                                       => MapValues(change)
      case FlatMap(change)                                         => FlatMap(change)
      case FlatMapValues(change)                                   => FlatMapValues(change)
      case Peek(effect)                                            => Peek((key, value) => fk(effect(key, value)))
      case EvalMapValues(change)                                   => EvalMapValues(value => fk(change(value)))
      case EvalFilter(keep)                                        => EvalFilter((key, value) => fk(keep(key, value)))
      case EvalMap(change)                                         => EvalMap((key, value) => fk(change(key, value)))
      case RecordFilter(keep)                                      => RecordFilter(keep)
      case RecordPeek(effect)                                      => RecordPeek(record => fk(effect(record)))
      case EvalRecordFilter(keep)                                  => EvalRecordFilter(record => fk(keep(record)))
      case RecordMap(change)                                       => RecordMap(change)
      case EvalRecordMap(change, result)                           => EvalRecordMap(record => fk(change(record)), result)
      case Aggregate(store, key, value, initial, start, add)       => Aggregate(store, key.mapK(fk), value.mapK(fk), initial, start, add)
      case TableGroupBy(grouped)                                   => TableGroupBy(grouped)
      case TableAggregate(store, key, value, start, add, subtract) => TableAggregate(store, key.mapK(fk), value.mapK(fk), start, add, subtract)
      case TableSource(store, key, value)                          => TableSource(store, key.mapK(fk), value.mapK(fk))
      case ToTable(lift, store, key, value)                        => ToTable(lift, store, key.mapK(fk), value.mapK(fk))
      case TableFilter(keep, present, store) => TableFilter(keep, present, store.map((named, key, value) => (named, key.mapK(fk), value.mapK(fk))))
      case TableMapValues(mapped, present, pure, store) =>
        TableMapValues(mapped, present, pure, store.map((named, key, value) => (named, key.mapK(fk), value.mapK(fk))))
      case WindowedAggregate(store, windows, retention, emitOnClose, key, value, initial, start, add) =>
        WindowedAggregate(store, windows, retention, emitOnClose, key.mapK(fk), value.mapK(fk), initial, start, add)
      case SlidingAggregate(store, windows, retention, emitOnClose, key, value, initial, start, add) =>
        SlidingAggregate(store, windows, retention, emitOnClose, key.mapK(fk), value.mapK(fk), initial, start, add)
      case SessionAggregate(store, windows, retention, emitOnClose, key, value, initial, start, add, merge) =>
        SessionAggregate(store, windows, retention, emitOnClose, key.mapK(fk), value.mapK(fk), initial, start, add, merge)
      case Suppress(suppressed, key, value, release) => Suppress(suppressed, key.mapK(fk), value.mapK(fk), release)
      case Join(view, join)                          => Join(view.mapK(fk), join)
      case ToStream()                                => ToStream()
      case GlobalJoin(table, select, join)           => GlobalJoin(table.mapK(fk), select, join)
      case Cogrouped(side, step)                     => Cogrouped(side, step.imapK(fk)(gk))
      case Process(processor, keepsKey)              => Process(processor.imapK(fk)(gk), keepsKey)

  /** The stores it keeps. */
  def kept: List[KeptStore] =
    this match
      case Aggregate(store, _, _, _, _, _)                            => List(KeptStore(store))
      case TableAggregate(store, _, _, _, _, _)                       => List(KeptStore(store))
      case TableSource(store, _, _)                                   => List(KeptStore(store))
      case ToTable(_, store, _, _)                                    => List(KeptStore(store))
      case TableFilter(_, _, store)                                   => store.toList.map((stored, _, _) => KeptStore(stored))
      case TableMapValues(_, _, _, store)                             => store.toList.map((stored, _, _) => KeptStore(stored))
      case WindowedAggregate(store, _, retention, _, _, _, _, _, _)   => List(KeptStore(store, Some(retention)))
      case SlidingAggregate(store, _, retention, _, _, _, _, _, _)    => List(KeptStore(store, Some(retention)))
      case SessionAggregate(store, _, retention, _, _, _, _, _, _, _) => List(KeptStore(store, Some(retention), session = true))
      case Suppress(suppressed, _, _, _)                              => List(KeptStore(Store(s"${suppressed.name}-store"), buffer = true))
      case Cogrouped(side, step)                                      => if side == 0 then step.kept else Nil
      case Process(processor, _)                                      => processor.kept
      case _                                                          => Nil

  /** Why its punctuators cannot be used, if any cannot. */
  def punctuationProblems: List[String] =
    this match
      case Process(processor, _) => processor.schedules.flatMap(_.problem)
      case _                     => Nil

  /** The global table it reads, if any. */
  def global: Option[GlobalStore[F]] =
    this match
      case GlobalJoin(table, _, _) => Some(table)
      case _                       => None

  /** Whether it may give its records a new key. */
  def rekeys: Boolean =
    this match
      case Map(_) | FlatMap(_) | EvalMap(_) | TableGroupBy(_) => true
      case Process(_, keepsKey)                               => !keepsKey
      case _                                                  => false

  /** Whether it asks for the old values of the changes it reads, given whether what reads it asks: a suppression, a filter and a table's `groupBy`
    * always do, as Java's ask, and a `mapValues` kept in no store only for what reads it.
    */
  def asks(after: Boolean): Boolean =
    this match
      case Suppress(_, _, _, _)          => true
      case TableFilter(_, _, _)          => true
      case TableGroupBy(_)               => true
      case TableMapValues(_, _, _, None) => after
      case _                             => false

/** A change to a group of a table's values: the value joining it and the value leaving it, as Java writes a `Change` to the repartition topic of a
  * table's `groupBy`.
  */
private[xkafka] final case class TableChange[V](current: Option[V], old: Option[V])

private[xkafka] object TableChange:
  /** What a change to a table from `old` to `current` changes in the groups `select` puts values in: `select` makes the new pair first, and a pair
    * leaving a group goes before the pair joining another, as Java's.
    */
  def grouped[K, V, K2, V2](select: (K, V) => (K2, V2))(key: K, current: Option[V], old: Option[V]): Vector[(K2, TableChange[V2])] =
    val joining = current.map(select(key, _))
    val leaving = old.map(select(key, _))
    (leaving, joining) match
      case (Some((from, left)), Some((to, joined))) if from.equals(to) => Vector(to -> TableChange(Some(joined), Some(left)))
      case _ => leaving.map((from, left) => from -> TableChange(None, Some(left))).toVector ++
          joining.map((to, joined) => to -> TableChange(Some(joined), None))

/** When a suppression lets a held change to a key go on. */
private[xkafka] enum Release[K]:
  /** Once the window the key names, which `end` gives the end of, has closed: `grace` after its end, the windows' grace, and for sessions also their
    * gap, as Java waits.
    */
  case WindowCloses(end: K => Long, grace: Long)

  /** Once `waited` milliseconds have passed since the first change held, by the records' own time. */
  case TimeLimit(waited: Long)

/** A topic a sub-topology reads, by name: an input topic, or the repartition topic `repartition` names. */
private[xkafka] enum Source derives CanEqual:
  case Input(topic: Topic)
  case Repartitioned(repartition: Repartition)

  /** A topic a foreign-key join's two sides send each other records through, by its name without the application's id. */
  case Exchanged(topic: String)

/** A topic's records as they come into a sub-topology: from an input topic, or from the repartition topic an earlier sub-topology writes. */
private[xkafka] enum Origin[F[_], K, V]:
  case Input(named: Named, topics: NonEmptyList[Topic], key: Serde[F, K], value: Serde[F, V])
  case Repartitioned(repartition: Repartition, key: Serde[F, K], value: Serde[F, V], aggregate: Option[Named] = None)

  /** A topic a foreign-key join's two sides send each other records through, read as the bytes they are, by the source node `node`. */
  case Exchanged[F[_]](topic: String, node: String) extends Origin[F, Option[Chunk[Byte]], Option[Chunk[Byte]]]

  def mapK[G[_]](fk: FunctionK[F, G]): Origin[G, K, V] =
    this match
      case Input(named, topics, key, value)                  => Input(named, topics, key.mapK(fk), value.mapK(fk))
      case Repartitioned(repartition, key, value, aggregate) => Repartitioned(repartition, key.mapK(fk), value.mapK(fk), aggregate)
      case Exchanged(topic, node)                            => Exchanged(topic, node)

  /** The topics it reads, as one source. */
  def sources: List[Source] =
    this match
      case Input(_, topics, _, _)              => topics.toList.distinct.map(Source.Input(_))
      case Repartitioned(repartition, _, _, _) => List(Source.Repartitioned(repartition))
      case Exchanged(topic, _)                 => List(Source.Exchanged(topic))

  /** The name the program gives its step, where it gives one. */
  def names: List[Named] =
    this match
      case Input(named, _, _, _)             => List(named)
      case Repartitioned(_, _, _, aggregate) => aggregate.map(named => Named(s"${named.value}-source")).toList
      case Exchanged(_, _)                   => Nil

/** A store a sub-topology keeps: how long it keeps a window, if it keeps windows, whether it holds a session's values, a suppression's held changes,
  * or as a stream-stream join's window store does, every record, and where processors may share it, what says it is the same store.
  */
private[xkafka] final case class KeptStore(
    store: Store,
    retention: Option[Long] = None,
    session: Boolean = false,
    buffer: Boolean = false,
    duplicates: Boolean = false,
    shared: Option[AnyRef] = None,
    problems: List[String] = Nil
)

/** One side of a table-table join: `present` gives each of its changes' value, or none for a delete, and `view` where the other side reads the
  * table's value for a key, or none for a windowed table.
  */
private[xkafka] final case class Side[F[_], K, A, C](present: C => Option[A], view: Option[TableView[F, K, A]]):
  def mapK[G[_]](fk: FunctionK[F, G]): Side[G, K, A, C] = Side(present, view.map(_.mapK(fk)))

/** Two tables joined by `joiner` in the step `named`, each change to either going on to `next` as a change to their join, as Java's table-table join
  * merges what its two sides make, the result kept in `store` where one is given, written with its serdes. `id` is the join, whatever effect it is
  * mapped to.
  */
private[xkafka] final class TablesJoined[F[_], K, A, B, R, C1, C2](
    val named: Named,
    val left: Side[F, K, A, C1],
    val right: Side[F, K, B, C2],
    val joiner: Joiner[A, B, R],
    val store: Option[(Store, Serde[F, K], Serde[F, R])],
    val next: Consumer[F, K, Option[R]],
    val id: AnyRef = new Object
):
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): TablesJoined[G, K, A, B, R, C1, C2] =
    TablesJoined(
      named,
      left.mapK(fk),
      right.mapK(fk),
      joiner,
      store.map((stored, key, value) => (stored, key.mapK(fk), value.mapK(fk))),
      next.imapK(fk)(gk),
      id
    )

  /** Its store, if any. */
  def kept: List[KeptStore] = store.toList.map((stored, _, _) => KeptStore(stored))

/** A table joined with the table `right` by a key `foreignKey` takes from each of its rows, as Java's foreign-key join: each side in its own
  * sub-topology, sending the other records through two topics. `joiner` makes each result, or none to delete it, and the result is kept in `store`
  * where one is given. `keepsUnmatched` says a row whose key finds nothing still has a result. `id` is the join, whatever effect it is mapped to.
  */
private[xkafka] final class ForeignKeyJoined[F[_], K, V, KO, VO, R, C1, C2](
    val named: Named,
    val left: Side[F, K, V, C1],
    val right: Side[F, KO, VO, C2],
    val keys: Serde[F, K],
    val values: Serde[F, V],
    val otherKeys: Serde[F, KO],
    val otherValues: Serde[F, VO],
    val foreignKey: (K, V) => Option[KO],
    val keepsUnmatched: Boolean,
    val joiner: (V, Option[VO]) => Option[R],
    val store: Option[(Store, Serde[F, R])],
    val next: Consumer[F, K, Option[R]],
    val id: AnyRef = new Object
):
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): ForeignKeyJoined[G, K, V, KO, VO, R, C1, C2] =
    ForeignKeyJoined(
      named,
      left.mapK(fk),
      right.mapK(fk),
      keys.mapK(fk),
      values.mapK(fk),
      otherKeys.mapK(fk),
      otherValues.mapK(fk),
      foreignKey,
      keepsUnmatched,
      joiner,
      store.map((stored, value) => (stored, value.mapK(fk))),
      next.imapK(fk)(gk),
      id
    )

  /** The topic each subscription goes through, read beside the other table. */
  def subscriptions: String = s"${named.value}-subscription-registration-topic"

  /** The topic each answer to a subscription goes through, read beside this table. */
  def responses: String = s"${named.value}-subscription-response-topic"

  /** The store of each row's subscription, kept beside the other table. */
  def subscribed: Store = Store(s"${named.value}-subscription-store")

  def kept: List[KeptStore] = KeptStore(subscribed) :: store.toList.map((stored, _) => KeptStore(stored))

/** Two streams joined by `joiner` within `windows`, as Java's stream-stream join: each record kept in a window store of its side and joined with the
  * other side's records in its window, what they make going on to `next`, its stores named from `store` as [[StreamJoin]] names them, and written
  * with `key`, `value` and `otherValue`. `id` is the join, whatever effect it is mapped to.
  */
private[xkafka] final class StreamsJoined[F[_], K, A, B, R](
    val named: Named,
    val windows: JoinWindows,
    val joiner: Joiner[A, B, R],
    val store: Store,
    val key: Serde[F, K],
    val value: Serde[F, A],
    val otherValue: Serde[F, B],
    val next: Consumer[F, K, R],
    val id: AnyRef = new Object
):
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): StreamsJoined[G, K, A, B, R] =
    StreamsJoined(named, windows, joiner, store, key.mapK(fk), value.mapK(fk), otherValue.mapK(fk), next.imapK(fk)(gk), id)

  /** Its stores: a window store for each side, keeping each for the windows' retention, and the store they share, if any. */
  def kept: List[KeptStore] =
    val named = StreamJoin.names(store, joiner)
    List(named.left, named.right).map(name => KeptStore(Store(name), Some(windows.retentionMillis), duplicates = true)) ++
      named.shared.map(name => KeptStore(Store(name)))

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
      if rightOuter then s"${store.value}-outer-this-join-store" else s"${store.value}-this-join-store",
      if leftOuter then s"${store.value}-outer-other-join-store" else s"${store.value}-other-join-store",
      Option.when(leftOuter)(if rightOuter then s"${store.value}-outer-shared-join-store" else s"${store.value}-left-shared-join-store")
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

/** One way out of a split: the records `keep` gives true for that no earlier branch took, which `next` reads, in the node `named`. */
private[xkafka] final case class SplitBranch[F[_], K, V](named: Named, keep: (K, V) => Boolean, next: Consumer[F, K, V]):
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): SplitBranch[G, K, V] = SplitBranch(named, keep, next.imapK(fk)(gk))

/** What happens to records keyed `K` with values `V`, in the direction they run: each node holds what reads what it gives, and one node can be read
  * from several, so a node is itself and equals nothing else. A join's two sides each hold one object for the join, which what follows the join hangs
  * from.
  */
private[xkafka] enum Consumer[F[_], K, V]:
  /** `step`, then `next` for each record it gives. */
  case Then[F[_], K, V, K2, V2](named: Named, step: Step[F, K, V, K2, V2], next: Consumer[F, K2, V2]) extends Consumer[F, K, V]

  /** Writes each record to the program's output topic `topic`. */
  case Output(named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V])

  /** Writes each record to the repartition topic `repartition` names, whose records `next` reads, in another sub-topology. A join that repartitions a
    * side says so with `atJoin`, since Java makes that topic's source only when it makes the join, which decides how sub-topologies are numbered. It
    * is refused where no record reaching it has a new key, unless `always` says the program asked for the topic itself. A grouped table's `aggregate`
    * names the topic's sink and source after itself, as Java does, and writes no record without a value.
    */
  case Repartitioning(
      repartition: Repartition,
      key: Serde[F, K],
      value: Serde[F, V],
      atJoin: Boolean,
      always: Boolean,
      next: Consumer[F, K, V],
      aggregate: Option[Named] = None
  )

  /** `next`, in the sub-topology of the table made in the node named `last`, as the table a stream-table join reads runs beside the stream. A table
    * read from a topic comes with `table`, which reads it if nothing else in the program does.
    */
  case Beside(table: Option[Pipeline[F]], last: String, next: Consumer[F, K, V])

  /** Each of `children` in turn for each record, as Java forwards a record to a node's successors. */
  case Fan(children: List[Consumer[F, K, V]])

  /** A node of its own that `next` reads, the node Java's `merge` makes. */
  case Merged(named: Named, next: Consumer[F, K, V])

  /** The node a cogroup's sides meet in, the one Java names `<named>-cogroup-merge`, which `next` reads. Java takes that node for the source of what
    * follows, which so has no topics to copartition. The topics meeting here are copartitioned where `together` says so, as several sides need.
    */
  case Cogroup(named: Named, together: Boolean, next: Consumer[F, K, V])

  /** `next`, for a cogroup of one side, whose topics Java copartitions as they are before any repartition topic. */
  case Copartitioned(next: Consumer[F, K, V])

  /** The first of `branches` that takes a record, and none if none does, as Java's `split` makes. */
  case Splitting(named: Named, branches: List[SplitBranch[F, K, V]])

  /** The left side of tables joined, whose changes are `C1`s. */
  case LeftTable[F[_], K, A, B, R, C1, C2](join: TablesJoined[F, K, A, B, R, C1, C2]) extends Consumer[F, K, C1]

  /** The right side of tables joined, whose changes are `C2`s. */
  case RightTable[F[_], K, A, B, R, C1, C2](join: TablesJoined[F, K, A, B, R, C1, C2]) extends Consumer[F, K, C2]

  /** The left side of streams joined. */
  case LeftStream[F[_], K, A, B, R](join: StreamsJoined[F, K, A, B, R]) extends Consumer[F, K, A]

  /** The right side of streams joined. */
  case RightStream[F[_], K, A, B, R](join: StreamsJoined[F, K, A, B, R]) extends Consumer[F, K, B]

  /** The left table of a foreign-key join, each change to it sending subscriptions to the other table's side. */
  case ForeignLeft[F[_], K, V, KO, VO, R, C1, C2](join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2]) extends Consumer[F, K, C1]

  /** The other table of a foreign-key join, each change to it answering the subscriptions to its key. */
  case ForeignRight[F[_], K, V, KO, VO, R, C1, C2](join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2]) extends Consumer[F, KO, C2]

  /** A foreign-key join's subscriptions as they reach the other table's side, which keeps them and answers. */
  case ForeignSubscribed[F[_], K, V, KO, VO, R, C1, C2](join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2])
      extends Consumer[F, Option[Chunk[Byte]], Option[Chunk[Byte]]]

  /** A foreign-key join's answers as they reach the left table's side, which makes the results. */
  case ForeignAnswered[F[_], K, V, KO, VO, R, C1, C2](join: ForeignKeyJoined[F, K, V, KO, VO, R, C1, C2])
      extends Consumer[F, Option[Chunk[Byte]], Option[Chunk[Byte]]]

  /** `next`, in a program `Topology.of` refuses for `problem`. */
  case Refused(problem: TopologyError, next: Consumer[F, K, V])

  /** Nothing further: the end of a table a join reads, or of a step that only acts on its records, as `foreach` does. */
  case Done()

  /** `origin` with its effects in `F`, mapped as far as it is read, since mapping every path through nodes read from several would take as long as
    * there are paths, and all it maps to is one node.
    */
  case Mapped[E[_], F[_], K, V](origin: Consumer[E, K, V], fk: FunctionK[E, F], gk: FunctionK[F, E]) extends Consumer[F, K, V]

  override def equals(other: Any): Boolean =
    (this, other) match
      case (Mapped(origin, fk, gk), Mapped(otherOrigin, otherFk, otherGk)) => (origin eq otherOrigin) && (fk eq otherFk) && (gk eq otherGk)
      case (_, node: AnyRef)                                               => this eq node
      case _                                                               => false

  override def hashCode: Int =
    this match
      case Mapped(origin, fk, _) => origin.hashCode * 31 + System.identityHashCode(fk)
      case _                     => System.identityHashCode(this)

  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Consumer[G, K, V] = Mapped(this, fk, gk)

  /** The node itself, with a mapped node's own steps and serdes mapped and what reads it still to be. */
  lazy val expanded: Consumer[F, K, V] =
    this match
      case Mapped(origin, fk, gk) => origin.mappedBy(fk)(gk)
      case _                      => this

  private def mappedBy[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Consumer[G, K, V] =
    expanded match
      case Then(named, step, next)                                              => Then(named, step.imapK(fk)(gk), next.imapK(fk)(gk))
      case Output(named, topic, key, value)                                     => Output(named, topic, key.mapK(fk), value.mapK(fk))
      case Repartitioning(repartition, key, value, at, always, next, aggregate) =>
        Repartitioning(repartition, key.mapK(fk), value.mapK(fk), at, always, next.imapK(fk)(gk), aggregate)
      case Beside(table, last, next)      => Beside(table.map(_.imapK(fk)(gk)), last, next.imapK(fk)(gk))
      case Fan(children)                  => Fan(children.map(_.imapK(fk)(gk)))
      case Merged(named, next)            => Merged(named, next.imapK(fk)(gk))
      case Cogroup(named, together, next) => Cogroup(named, together, next.imapK(fk)(gk))
      case Copartitioned(next)            => Copartitioned(next.imapK(fk)(gk))
      case Splitting(named, branches)     => Splitting(named, branches.map(_.imapK(fk)(gk)))
      case LeftTable(join)                => LeftTable(join.imapK(fk)(gk))
      case RightTable(join)               => RightTable(join.imapK(fk)(gk))
      case LeftStream(join)               => LeftStream(join.imapK(fk)(gk))
      case RightStream(join)              => RightStream(join.imapK(fk)(gk))
      case ForeignLeft(join)              => ForeignLeft(join.imapK(fk)(gk))
      case ForeignRight(join)             => ForeignRight(join.imapK(fk)(gk))
      case ForeignSubscribed(join)        => ForeignSubscribed(join.imapK(fk)(gk))
      case ForeignAnswered(join)          => ForeignAnswered(join.imapK(fk)(gk))
      case Refused(problem, next)         => Refused(problem, next.imapK(fk)(gk))
      case Done()                         => Done()
      case mapped @ Mapped(_, _, _)       => mapped.expanded.mappedBy(fk)(gk)

  /** Whether what reads a table's changes here asks for their old values: a side of a table join does where its kind of join reads them, or what
    * reads the join asks, a fan where any of its children does, and nothing else after a table does unless a step of it does. Each node works it out
    * once, however many paths lead to it.
    */
  lazy val requests: Boolean =
    this match
      case Then(_, step, next)  => step.asks(next.requests)
      case LeftTable(join)      => join.joiner.outer._1 || join.next.requests
      case RightTable(join)     => join.joiner.outer._2 || join.next.requests
      case ForeignLeft(_)       => true
      case Fan(children)        => children.exists(_.requests)
      case Mapped(origin, _, _) => origin.requests
      case _                    => false

private[xkafka] object Consumer:
  /** All of `children`, each once, as Java connects a node to a successor once. */
  def fan[F[_], K, V](children: List[Consumer[F, K, V]]): Consumer[F, K, V] =
    children.distinct match
      case List(only) => only
      case all        => Fan(all)

  /** `next`, in a program `Topology.of` refuses for each of `problems`. */
  def refusing[F[_], K, V](problems: Iterable[TopologyError], next: Consumer[F, K, V]): Consumer[F, K, V] = problems.foldRight(next)(Refused(_, _))

/** A topic's records going into what reads them. */
private[xkafka] sealed trait Entry[F[_]]:
  /** The key and value it reads. */
  type K
  type V

  def origin: Origin[F, K, V]
  def consumer: Consumer[F, K, V]

  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Entry[G] = Entry(origin.mapK(fk), consumer.imapK(fk)(gk))

private[xkafka] object Entry:
  def apply[F[_], K0, V0](origin: Origin[F, K0, V0], consumer: Consumer[F, K0, V0]): Entry[F] = Of(origin, consumer)

  private final case class Of[F[_], K0, V0](origin: Origin[F, K0, V0], consumer: Consumer[F, K0, V0]) extends Entry[F]:
    type K = K0
    type V = V0

/** The topics and steps that make one sub-topology: the topics it reads, its own first, each going into what reads it; the groups of topics its joins
  * copartition, as Java groups their sources; the joins and merges it holds, by the topics they read, whose sub-topologies Java numbers by which of
  * their inputs its program builds first; the cogroups it holds, by the topics they read, that Java cannot start, since it copartitions what they
  * read from before a repartition topic, in other sub-topologies; the stores it keeps and the repartition topics it writes; and its nodes as Java's
  * builder makes them.
  */
/** A processor with punctuators, and what reads the records they return. */
private[xkafka] trait Punctuating[F[_]]:
  type Key
  type Value

  def named: String
  def stores: List[String]
  def schedules: List[Punctuation]
  def punctuators(access: Map[String, StoreAccess[F]]): List[Punctuator[F, Key, Value]]
  def next: Consumer[F, Key, Value]

private[xkafka] object Punctuating:
  def apply[F[_], K, V, K2, V2](name: String, processor: Processor[F, K, V, K2, V2], reader: Consumer[F, K2, V2]): Punctuating[F] =
    new Punctuating[F]:
      type Key   = K2
      type Value = V2
      val named: String                                                                 = name
      val stores: List[String]                                                          = processor.kept.map(_.store.value)
      val schedules: List[Punctuation]                                                  = processor.schedules
      def punctuators(access: Map[String, StoreAccess[F]]): List[Punctuator[F, K2, V2]] = processor.handling(access).punctuators
      val next: Consumer[F, K2, V2]                                                     = reader

private[xkafka] final case class Subtopology[F[_]](
    entries: List[Entry[F]],
    copartitioned: List[List[Source]],
    buildOrdered: List[List[Source]],
    unstartable: List[List[Source]],
    kept: List[KeptStore],
    repartitions: List[Source],
    nodes: List[Describing.Made]
):
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Subtopology[G] =
    Subtopology(entries.map(_.imapK(fk)(gk)), copartitioned, buildOrdered, unstartable, kept, repartitions, nodes)

  /** The topics it reads, its own first. */
  def origins: List[Source] = entries.flatMap(_.origin.sources)

  /** The processors in it with punctuators, in the order their records reach them, each node walked once however many paths lead through it. */
  lazy val punctuating: List[Punctuating[F]] =
    def found[K, V](consumer: Consumer[F, K, V], seen: Set[AnyRef]): (List[Punctuating[F]], Set[AnyRef]) =
      val marked                                       = seen + consumer
      def all[K2, V2](next: List[Consumer[F, K2, V2]]) =
        next.foldLeft((List.empty[Punctuating[F]], marked)):
          case ((so, inner), child) =>
            val (more, after) = found(child, inner)
            (so ++ more, after)
      if seen(consumer) then (Nil, seen)
      else
        consumer.expanded match
          case Consumer.Then(named, step, next) =>
            val (more, after) = found(next, marked)
            step match
              case Step.Process(processor, _) if processor.schedules.nonEmpty => (Punctuating(named.value, processor, next) :: more, after)
              case _                                                          => (more, after)
          case Consumer.Beside(_, _, next)     => found(next, marked)
          case Consumer.Fan(children)          => all(children)
          case Consumer.Merged(_, next)        => found(next, marked)
          case Consumer.Cogroup(_, _, next)    => found(next, marked)
          case Consumer.Copartitioned(next)    => found(next, marked)
          case Consumer.Splitting(_, branches) => all(branches.map(_.next))
          case Consumer.Refused(_, next)       => found(next, marked)
          case Consumer.LeftTable(join)        => found(join.next, marked)
          case Consumer.RightTable(join)       => found(join.next, marked)
          case Consumer.LeftStream(join)       => found(join.next, marked)
          case Consumer.RightStream(join)      => found(join.next, marked)
          case Consumer.ForeignAnswered(join)  => found(join.next, marked)
          case _                               => (Nil, marked)
    entries.foldLeft((List.empty[Punctuating[F]], Set.empty[AnyRef])):
      case ((so, seen), entry) =>
        val (more, after) = found(entry.consumer, seen)
        (so ++ more, after)
    ._1

  /** Its stores. */
  def stores: List[Store] = kept.map(_.store)

  /** The names of its suppression buffers. */
  def buffers: Set[String] = kept.filter(_.buffer).map(_.store.value).toSet

  /** The names of its session stores. */
  def sessionStores: Set[String] = kept.filter(_.session).map(_.store.value).toSet

  /** How long each of its window stores keeps a window, in milliseconds, by store name. */
  def retentions: Map[String, Long] = kept.flatMap(held => held.retention.map(held.store.value -> _)).toMap

  /** The window stores of its stream-stream joins, which keep every record. */
  def duplicates: Set[String] = kept.filter(_.duplicates).map(_.store.value).toSet

/** Records read from a topic, changed step by step. Its [[Keying]] says whether the records are still keyed as they were read, which decides how they
  * can be grouped or joined by key: records that may have a new key go through a repartition topic first, which `groupByKey` and `join` then take a
  * name or serdes for. A stream whose type does not say is taken to have changed its key.
  */
sealed trait KStream[F[_], K, V]:
  /** Whether the records are keyed as they were read ([[KeyState.AsRead]]) or may have a new key ([[KeyState.Changed]]). */
  type Keying <: KeyState

  /** What ending the stream gives: a [[Pipeline]] for a stream read from a topic, which `Topology.of` takes, and an [[Into]] for a [[Flow]], which a
    * stream's `into` takes.
    */
  type Out

  /** A stream after a step that leaves each record in the partition of its key. */
  type Kept[K2, V2] = KStream.Flowing[F, K2, V2, Keying, Out]

  /** A stream after a step that may give records a new key. */
  type Rekeyed[K2, V2] = KStream.Flowing[F, K2, V2, KeyState.Changed, Out]

  /** A stream after a join or a repartition, whose records are keyed as they were read from what they were last read from. */
  type Settled[K2, V2] = KStream.Flowing[F, K2, V2, KeyState.AsRead, Out]

  /** A path starting from this stream's records, in the state they are in, which is what `flow` gives and `fork` passes on. */
  type Branch = KStream.Flowing[F, K, V, Keying, Into[F, K, V, Keying]]

  /** What this stream's records going into `next` makes. */
  private[xkafka] def feed(next: Consumer[F, K, V]): Out

  /** The name of the node its records come out of, as Java names it, or empty for a flow with no steps yet. */
  private[xkafka] def lastNode: String
  private[xkafka] def keyChanged: Boolean
  private[xkafka] def ending: Ending[F, Out]

  /** The stream with `step` as its last step, which leaves each record in the partition of its key, such as one that keeps the key, or windows it. */
  private[xkafka] def kept[K2, V2](named: Named, step: Step[F, K, V, K2, V2]): Kept[K2, V2] =
    KStream.of(next => feed(Consumer.Then(named, step, next)), named.value, keyChanged, ending)

  /** The stream with `step` as its last step, which may give records a new key. */
  private def rekeyed[K2, V2](named: Named, step: Step[F, K, V, K2, V2]): Rekeyed[K2, V2] =
    KStream.of(next => feed(Consumer.Then(named, step, next)), named.value, true, ending)

  /** A path starting from these records, inheriting their key and value types and whether their key may have changed, to send them to with `into`. */
  def flow: Branch = KStream.of(new Into(_), lastNode, keyChanged, Ending.flow)

  /** Sends each record to each of the flows given, as many as there are, in turn. A [[Flow]] or this stream's `flow` makes them. */
  def into[C](to: C)(using takes: KeyState.Takes[F, K, V, C], fits: KeyState.KeyFits[Keying, C]): Out = feed(Consumer.fan(takes.consumers(to)))

  /** Sends each record down each of the paths given, in turn, written where the stream branches. Each path starts from this stream's records. */
  def fork(first: Branch => Into[F, K, V, Keying], rest: (Branch => Into[F, K, V, Keying])*): Out =
    feed(Consumer.fan((first :: rest.toList).map(_(flow).consumer)))

  /** Sends each record down each of the paths given, in turn, and what they make goes on as one stream, in the node `named`, whose key may have
    * changed if any path changed it.
    */
  def forkMerge[K2, V2, P <: KeyState](named: Named)(
      first: Branch => KStream.Reaching[F, K2, V2, P, Into[F, K, V, Keying]],
      rest: (Branch => KStream.Reaching[F, K2, V2, P, Into[F, K, V, Keying]])*
  ): KStream.Flowing[F, K2, V2, P, Out] =
    val paths = (first :: rest.toList).map(_(flow))
    KStream.of(
      next =>
        val merged = Consumer.Merged(named, next)
        feed(Consumer.fan(paths.map(_.feed(merged).consumer)))
      ,
      named.value,
      paths.exists(_.keyChanged),
      ending
    )

  /** This stream's records and those of `other` as one stream, in the node `named`, as Java's `merge` makes it, whose key may have changed if
    * either's has. They run in one sub-topology.
    */
  def merge[P <: KeyState](named: Named, other: KStream.Reaching[F, K, V, P, Out]): KStream.Flowing[F, K, V, P | Keying, Out] =
    KStream.of(
      next =>
        val merged = Consumer.Merged(named, next)
        ending.both(feed(merged), other.feed(merged))
      ,
      named.value,
      keyChanged || other.keyChanged,
      ending
    )

  /** A node of its own that any number of flows can send records to with `into`, as `merge` makes for two streams, for a path that goes to several
    * merges, where `merge` and `forkMerge` cannot say so.
    */
  def merged(named: Named): Kept[K, V] = KStream.of(next => feed(Consumer.Merged(named, next)), named.value, keyChanged, ending)

  /** Sends each record down the first branch that takes it, as Java's `split` does, each branch's name added to `named` to name its node. */
  def split(named: Named): Split[F, K, V, Keying, Out] = new Split(named, this, Vector.empty)

  /** Each record with what `join` makes of it and the value of the key `select` gives in `table`, in the step `named`, as Java's join of a stream
    * with a global table: a record whose key `table` holds no value for is dropped. The records stay in their partitions, and need not be
    * copartitioned with `table`, which every member reads whole.
    */
  def join[GK, GV, R](named: Named, table: GlobalKTable[F, GK, GV])(select: (K, V) => GK)(join: (V, GV) => R): Kept[K, R] =
    kept(named, Step.GlobalJoin(table, select, (value, found) => found.map(join(value, _))))

  /** As `join` with a global table, a record whose key `table` holds no value for going on with none. */
  def leftJoin[GK, GV, R](named: Named, table: GlobalKTable[F, GK, GV])(select: (K, V) => GK)(join: (V, Option[GV]) => R): Kept[K, R] =
    kept(named, Step.GlobalJoin(table, select, (value, found) => Some(join(value, found))))

  /** Keeps the records `keep` gives true for. */
  def filter(named: Named)(keep: (K, V) => Boolean): Kept[K, V] = kept(named, Step.Filter(keep))

  /** Drops the records `drop` gives true for. */
  def filterNot(named: Named)(drop: (K, V) => Boolean): Kept[K, V] = kept(named, Step.Filter((key, value) => !drop(key, value)))

  /** Replaces each record with what `change` gives. */
  def map[K2, V2](named: Named)(change: (K, V) => (K2, V2)): Rekeyed[K2, V2] = rekeyed(named, Step.Map(change))

  /** Replaces each value with what `change` gives. */
  def mapValues[V2](named: Named)(change: V => V2): Kept[K, V2] = kept(named, Step.MapValues(change))

  /** Replaces each record with the records `change` gives, none or more. */
  def flatMap[K2, V2](named: Named)(change: (K, V) => Iterable[(K2, V2)]): Rekeyed[K2, V2] = rekeyed(named, Step.FlatMap(change))

  /** Replaces each value with the values `change` gives, none or more, each under the record's key. */
  def flatMapValues[V2](named: Named)(change: V => Iterable[V2]): Kept[K, V2] = kept(named, Step.FlatMapValues(change))

  /** Gives each record the key `change` gives. */
  def selectKey[K2](named: Named)(change: (K, V) => K2): Rekeyed[K2, V] = rekeyed(named, Step.Map((k, v) => (change(k, v), v)))

  /** Runs `effect` for each record and passes the record on. The effect runs again for records replayed after a crash or a fencing. */
  def peek(named: Named)(effect: (K, V) => F[Unit]): Kept[K, V] = kept(named, Step.Peek(effect))

  /** Runs `effect` for each record, and ends the pipeline there, as Java's `foreach` does. The effect runs again for records replayed after a crash
    * or a fencing.
    */
  def foreach(named: Named)(effect: (K, V) => F[Unit]): Out = feed(Consumer.Then(named, Step.Peek(effect), Consumer.Done()))

  /** Prints each record as `[label]: key, value`, labelled with the name of the step before, in the step `named`, and ends the pipeline there, as
    * Java's `print` to standard output does. A flow with no step before is labelled with `named`.
    */
  def print(named: Named)(using Console[F]): Out = print(named, if lastNode.isEmpty then named.value else lastNode)

  /** Prints each record as `[label]: key, value`, in the step `named`, and ends the pipeline there. */
  def print(named: Named, label: String)(using console: Console[F]): Out = foreach(named)((key, value) => console.println(s"[$label]: $key, $value"))

  /** Replaces each value with what `change` gives. Its output is written once under exactly-once processing, but the effect runs again for records
    * replayed after a crash or a fencing.
    */
  def evalMapValues[V2](named: Named)(change: V => F[V2]): Kept[K, V2] = kept(named, Step.EvalMapValues(change))

  /** Replaces each record with what `processor` gives for it, under any key, reading and writing the stores it declares. Its results and store writes
    * are committed together, but other effects in its handler run again for records replayed after a crash or a fencing.
    */
  def process[K2, V2](named: Named, processor: Processor[F, K, V, K2, V2]): Rekeyed[K2, V2] =
    rekeyed(named, Step.Process(processor, keepsKey = false))

  /** Replaces each value with what `processor` gives for its record, under the same key, reading and writing the stores it declares. Its results and
    * store writes are committed together, but other effects in its handler run again for records replayed after a crash or a fencing.
    */
  def processValues[V2](named: Named, processor: FixedKeyProcessor[F, K, V, V2]): Kept[K, V2] =
    kept(named, Step.Process(processor.processor, keepsKey = true))

  /** Keeps the records `keep` gives true for. The effect runs again for records replayed after a crash or a fencing. */
  def evalFilter(named: Named)(keep: (K, V) => F[Boolean]): Kept[K, V] = kept(named, Step.EvalFilter(keep))

  /** Replaces each record with what `change` gives. Its output is written once under exactly-once processing, but the effect runs again for records
    * replayed after a crash or a fencing.
    */
  def evalMap[K2, V2](named: Named)(change: (K, V) => F[(K2, V2)]): Rekeyed[K2, V2] = rekeyed(named, Step.EvalMap(change))

  /** Keeps the records `keep` gives true for, given each whole record with its time and headers. */
  def filterRecord(named: Named)(keep: Record[K, V] => Boolean): Kept[K, V] = kept(named, Step.RecordFilter(keep))

  /** Replaces each record with what `change` gives, which can change anything about it. */
  def mapRecord[K2, V2](named: Named)(change: Record[K, V] => Record[K2, V2]): Rekeyed[K2, V2] =
    rekeyed(named, Step.RecordMap(record => List(change(record))))

  /** Replaces each record with what `change` gives, which can change its value, time and headers but not its key. */
  def mapRecordValue[V2](named: Named)(change: FixedKeyRecord[K, V] => FixedKeyRecord[K, V2]): Kept[K, V2] =
    kept(named, Step.RecordMap(record => List(change(new FixedKeyRecord(record)).record)))

  /** Replaces each record with the records `change` gives, in order, each of which can change anything about it. */
  def flatMapRecord[K2, V2](named: Named)(change: Record[K, V] => Iterable[Record[K2, V2]]): Rekeyed[K2, V2] = rekeyed(named, Step.RecordMap(change))

  /** Replaces each record with the records `change` gives, in order, each of which can change its value, time and headers but not its key. */
  def flatMapRecordValue[V2](named: Named)(change: FixedKeyRecord[K, V] => Iterable[FixedKeyRecord[K, V2]]): Kept[K, V2] =
    kept(named, Step.RecordMap(record => change(new FixedKeyRecord(record)).map(_.record)))

  /** Runs `effect` for each whole record, leaving it as it is. The effect runs again for records replayed after a crash or a fencing. */
  def peekRecord(named: Named)(effect: Record[K, V] => F[Unit]): Kept[K, V] = kept(named, Step.RecordPeek(effect))

  /** As `filterRecord`, running an effect, which runs again for records replayed after a crash or a fencing. */
  def evalFilterRecord(named: Named)(keep: Record[K, V] => F[Boolean]): Kept[K, V] = kept(named, Step.EvalRecordFilter(keep))

  /** As `mapRecord`, running an effect. Its output is written once under exactly-once processing, but the effect runs again for records replayed
    * after a crash or a fencing.
    */
  def evalMapRecord[K2, V2](named: Named)(change: Record[K, V] => F[Record[K2, V2]]): Rekeyed[K2, V2] =
    rekeyed(named, Step.EvalRecordMap(change, identity))

  /** As `mapRecordValue`, running an effect. Its output is written once under exactly-once processing, but the effect runs again for records replayed
    * after a crash or a fencing.
    */
  def evalMapRecordValue[V2](named: Named)(change: FixedKeyRecord[K, V] => F[FixedKeyRecord[K, V2]]): Kept[K, V2] =
    kept(named, Step.EvalRecordMap(record => change(new FixedKeyRecord(record)), _.record))

  /** Groups records by a new key, through the repartition topic `repartition` names, which also names the step that gives them the key, as Java's
    * `Grouped` names both.
    */
  def groupBy[K2](repartition: Repartition, key: Serde[F, K2], value: Serde[F, V])(change: (K, V) => K2): KGroupedStream[F, K2, V, Out] =
    KGroupedStream(selectKey(Named(repartition.value))(change), Some(repartition), key, value)

  /** Groups records keyed as they were read by their key. */
  def groupByKey(key: Serde[F, K], value: Serde[F, V])(using KeyState.Unchanged[Keying]): KGroupedStream[F, K, V, Out] =
    KGroupedStream(this, None, key, value)

  /** Groups records that may have a new key by it, through the repartition topic `repartition` names. A stream whose type does not say, and whose key
    * has not changed, needs none, and `Topology.of` refuses the name.
    */
  def groupByKey(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])(using
      KeyState.MayHaveChanged[Keying]
  ): KGroupedStream[F, K, V, Out] = KGroupedStream(this, Some(repartition), key, value)

  /** Each record with what `join` makes of it and its key's value in `table`, or none, read as Java's stream-table join reads it when the record
    * comes; the table runs in the same sub-topology, so its topic and the stream's are copartitioned. Records go through a repartition topic first
    * where `through` gives the serdes to write them with, which Java names after the join, as its `Joined` name names both.
    */
  private def joined[TV, R](
      named: Named,
      table: KTable[F, K, TV],
      through: Option[(Serde[F, K], Serde[F, V])],
      join: (V, Option[TV]) => Option[R]
  ): Settled[K, R] =
    val windowed = Option.when(table.view.isEmpty)(TopologyError.WindowedJoin)
    // A windowed table has no view, and its join is refused, so the step that stands in for it never runs.
    val step                                                = table.view.fold[Step[F, K, V, K, R]](Step.FlatMap((_, _) => Nil))(Step.Join(_, join))
    def reading(next: Consumer[F, K, R]): Consumer[F, K, V] =
      Consumer.refusing(windowed, Consumer.Beside(table.origin, table.stream.lastNode, Consumer.Then(named, step, next)))
    val feeding: Consumer[F, K, R] => Out =
      through match
        case None               => next => feed(reading(next))
        case Some((key, value)) => next => feed(Consumer.Repartitioning(Repartition(named.value), key, value, true, false, reading(next)))
    KStream.of(feeding, named.value, false, ending)

  /** Each record joined with its key's value in `table`, as Java's `join` of a stream with a table: a record whose key the table holds no value for
    * is dropped. The records are keyed as they were read, so that the table and they are partitioned alike.
    */
  def join[TV, R](named: Named, table: KTable[F, K, TV])(join: (V, TV) => R)(using KeyState.Unchanged[Keying]): Settled[K, R] =
    joined(named, table, None, (v, tv) => tv.map(join(v, _)))

  /** As `join`, for records that may have a new key, after the repartition topic `named` names, written with `key` and `value`. */
  def join[TV, R](named: Named, table: KTable[F, K, TV], key: Serde[F, K], value: Serde[F, V])(join: (V, TV) => R)(using
      KeyState.MayHaveChanged[Keying]
  ): Settled[K, R] = joined(named, table, Some((key, value)), (v, tv) => tv.map(join(v, _)))

  /** Each record joined with its key's value in `table`, or none where the table holds no value for it, as Java's `leftJoin` of a stream with a
    * table. The records are keyed as they were read.
    */
  def leftJoin[TV, R](named: Named, table: KTable[F, K, TV])(join: (V, Option[TV]) => R)(using KeyState.Unchanged[Keying]): Settled[K, R] =
    joined(named, table, None, (v, tv) => Some(join(v, tv)))

  /** As `leftJoin`, for records that may have a new key, after the repartition topic `named` names, written with `key` and `value`. */
  def leftJoin[TV, R](named: Named, table: KTable[F, K, TV], key: Serde[F, K], value: Serde[F, V])(join: (V, Option[TV]) => R)(using
      KeyState.MayHaveChanged[Keying]
  ): Settled[K, R] = joined(named, table, Some((key, value)), (v, tv) => Some(join(v, tv)))

  /** The stream as one side of a stream-stream join, read by `side`: as it is, or where its key has changed, from the repartition topic `repartition`
    * names, which the stream is written to with `key` and `value`, as Java repartitions each side of the join.
    */
  private def joinSide(repartition: Repartition, key: Serde[F, K], value: Serde[F, V], side: Consumer[F, K, V]): Out =
    if keyChanged then feed(Consumer.Repartitioning(repartition, key, value, true, false, side)) else feed(side)

  /** The stream joined with `other` by `joiner` within `windows`, as Java's stream-stream joins join them: in one sub-topology, their topics
    * copartitioned, each side repartitioned through `<store>-left` or `<store>-right` where its key has changed.
    */
  private def windowJoined[VO, R](
      named: Named,
      other: KStream.Ends[F, K, VO, Out],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO],
      joiner: Joiner[V, VO, R]
  ): Settled[K, R] =
    val invalid = windows.problem.map(TopologyError.InvalidWindows(store.value, _))
    // Java names the repartition topics after the join, as `StreamJoined.withName` names both, and the stores after the store's name.
    val feeding: Consumer[F, K, R] => Out =
      next =>
        val join = StreamsJoined(named, windows, joiner, store, key, value, otherValue, Consumer.refusing(invalid, next))
        ending.both(
          joinSide(Repartition(s"${named.value}-left"), key, value, Consumer.LeftStream(join)),
          other.joinSide(Repartition(s"${named.value}-right"), key, otherValue, Consumer.RightStream(join))
        )
    KStream.of(feeding, s"${named.value}-merge", false, ending)

  /** Each record joined with each record of `other` with the same key in its window, as Java's `join` of two streams: a record with no match in
    * `other` goes on no further. The stores `<store>-this-join-store` and `<store>-other-join-store` keep each side's records for the windows' size
    * and grace, written with `key`, `value` and `otherValue`, which also write a side whose key has changed to the repartition topic `<named>-left`
    * or `<named>-right` names. The result's time is the later of the two records'.
    */
  def join[VO, R](
      named: Named,
      other: KStream.Ends[F, K, VO, Out],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO]
  )(join: (V, VO) => R): Settled[K, R] = windowJoined(named, other, windows, store, key, value, otherValue, Joiner.Inner(join))

  /** As `join`, but a record with no match in `other` also goes on, with none for it, once its window has closed, as Java's `leftJoin` of two streams
    * holds it in the store `<store>-left-shared-join-store` until then. The store of `other`'s records is `<store>-outer-other-join-store`.
    */
  def leftJoin[VO, R](
      named: Named,
      other: KStream.Ends[F, K, VO, Out],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO]
  )(join: (V, Option[VO]) => R): Settled[K, R] = windowJoined(named, other, windows, store, key, value, otherValue, Joiner.Left(join))

  /** As `join`, but a record of either stream with no match in the other also goes on alone once its window has closed, as Java's `outerJoin` of two
    * streams holds it in the store `<store>-outer-shared-join-store` until then. The sides' stores are `<store>-outer-this-join-store` and
    * `<store>-outer-other-join-store`.
    */
  def outerJoin[VO, R](
      named: Named,
      other: KStream.Ends[F, K, VO, Out],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO]
  )(join: Ior[V, VO] => R): Settled[K, R] = windowJoined(named, other, windows, store, key, value, otherValue, Joiner.Outer(join))

  /** Sends every record through the repartition topic `repartition` names, written with `key` and `value`, whether or not its key has changed, as
    * Java's `repartition` does. Afterwards the records are keyed as they were read from that topic.
    */
  def repartition(repartition: Repartition, key: Serde[F, K], value: Serde[F, V]): Settled[K, V] = through(repartition, key, value, true)

  private def through(repartition: Repartition, key: Serde[F, K], value: Serde[F, V], always: Boolean): Settled[K, V] =
    KStream.of(
      next => feed(Consumer.Repartitioning(repartition, key, value, false, always, next)),
      s"${repartition.value}-repartition-source",
      false,
      ending
    )

  /** The latest value of each key as a table in `store`, in the step `named`, as Java's `toTable` makes it. Records that may have a new key go
    * through a repartition topic first, which `named` names and `key` and `written` write.
    */
  private def tabled[A](
      named: Named,
      store: Store,
      key: Serde[F, K],
      written: Serde[F, V],
      value: Serde[F, A],
      lift: V => Option[A]
  ): KTable.Flowing[F, K, A, Option, Out] =
    val read: KStream.Ends[F, K, V, Out] = if keyChanged then through(Repartition(named.value), key, written, false) else this
    KTable
      .deleting(read.kept(named, Step.ToTable(lift, store, key, value)), TableKey.Plain(key), value, None, Some(TableView.Stored(store, key, value)))

  /** The latest value of each key as a table kept in `store`, in the step `named`, written with `key` and `value`, as Java's `toTable` makes it.
    * Records whose key may have changed go through a repartition topic that `named` names first.
    */
  def toTable(named: Named, store: Store, key: Serde[F, K], value: Serde[F, V]): KTable.Flowing[F, K, V, Option, Out] =
    tabled(named, store, key, value, value, Some(_))

  /** As `toTable`, for optional values, of which none deletes the key, as Java's null does. */
  def toTable[A](named: Named, store: Store, key: Serde[F, K], value: Serde[F, A])(using
      optional: V =:= Option[A],
      F: Applicative[F]
  ): KTable.Flowing[F, K, A, Option, Out] =
    tabled(named, store, key, optional.flip.substituteCo[[X] =>> Serde[F, X]](value.option), value, optional(_))

  /** Writes every record to `topic`, in the step `named`. */
  def to(named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V]): Out = feed(Consumer.Output(named, topic, key, value))

object KStream:
  /** A stream read from a topic whose [[KStream.Keying]] is `P`, for annotating one without losing it. */
  type Of[F[_], K, V, P <: KeyState] = Flowing[F, K, V, P, Pipeline[F]]

  /** A stream whose [[KStream.Keying]] is `P` and whose [[KStream.Out]] is `O`. */
  type Flowing[F[_], K, V, P <: KeyState, O] = KStream[F, K, V] { type Keying = P; type Out = O }

  /** A stream whose [[KStream.Keying]] is `P` or says more, and whose [[KStream.Out]] is `O`. */
  type Reaching[F[_], K, V, P <: KeyState, O] = KStream[F, K, V] { type Keying <: P; type Out = O }

  /** A stream whose [[KStream.Out]] is `O`, whatever the state of its key. */
  type Ends[F[_], K, V, O] = KStream[F, K, V] { type Out = O }

  /** A stream read from a topic whose type does not say how it is keyed, which is taken to have changed its key. */
  type Unkeyed[F[_], K, V] = Ends[F, K, V, Pipeline[F]]

  /** The records of `topic`, read in the step `named`, as Java's `Consumed` names it. */
  def apply[F[_], K, V](named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V]): Of[F, K, V, KeyState.AsRead] =
    apply(named, NonEmptyList.one(topic), key, value)

  /** The records of each of `topics` as one stream, read in the one step `named`, as Java's `stream` given several topics reads them. A topic listed
    * twice is read once.
    */
  def apply[F[_], K, V](named: Named, topics: NonEmptyList[Topic], key: Serde[F, K], value: Serde[F, V]): Of[F, K, V, KeyState.AsRead] =
    of(next => Pipeline(List(Entry(Origin.Input(named, topics, key, value), next))), named.value, false, Ending.source)

  private[xkafka] def of[F[_], K, V, P <: KeyState, O](
      feed: Consumer[F, K, V] => O,
      lastNode: String,
      keyChanged: Boolean,
      ending: Ending[F, O]
  ): Flowing[F, K, V, P, O] = Built(feed, lastNode, keyChanged, ending)

  private final class Built[F[_], K, V, P <: KeyState, O](
      feeding: Consumer[F, K, V] => O,
      private[xkafka] val lastNode: String,
      private[xkafka] val keyChanged: Boolean,
      private[xkafka] val ending: Ending[F, O]
  ) extends KStream[F, K, V]:
    type Keying = P
    type Out    = O

    private[xkafka] def feed(next: Consumer[F, K, V]): O = feeding(next)

/** What can be done with what ending a stream gives. */
private[xkafka] sealed trait Ending[F[_], O]:
  /** The pipeline it is, if it reads topics of its own. */
  def pipeline(out: O): Option[Pipeline[F]]

  /** The two together, as the sides of a join or a merge. */
  def both(first: O, second: O): O

private[xkafka] object Ending:
  def source[F[_]]: Ending[F, Pipeline[F]] =
    new Ending[F, Pipeline[F]]:
      def pipeline(out: Pipeline[F]): Option[Pipeline[F]]            = Some(out)
      def both(first: Pipeline[F], second: Pipeline[F]): Pipeline[F] = first ++ second

  def flow[F[_], K, V, P <: KeyState]: Ending[F, Into[F, K, V, P]] =
    new Ending[F, Into[F, K, V, P]]:
      def pipeline(out: Into[F, K, V, P]): Option[Pipeline[F]]                      = None
      def both(first: Into[F, K, V, P], second: Into[F, K, V, P]): Into[F, K, V, P] = new Into(Consumer.fan(List(first.consumer, second.consumer)))

/** Somewhere to send records keyed `K` with values `V` whose key is in the state `P`, or says more: a flow that has been written to its end, which a
  * stream's `into` takes.
  */
final class Into[F[_], K, V, -P <: KeyState] private[xkafka] (private[xkafka] val consumer: Consumer[F, K, V])

/** A path with no source, which any number of streams send records to with `into`. */
object Flow:
  /** A path for records whose key may have changed: its `groupByKey` takes a repartition, and any stream can send to it. `Topology.of` refuses a
    * repartition it makes for records that all arrive keyed as they were read.
    */
  def apply[F[_], K, V]: KStream.Flowing[F, K, V, KeyState, Into[F, K, V, KeyState]] = KStream.of(new Into(_), "", true, Ending.flow)

  /** A path for records keyed as they were read, which needs no repartition to group or join them, and which only such a stream can send to. */
  def asRead[F[_], K, V]: KStream.Flowing[F, K, V, KeyState.AsRead, Into[F, K, V, KeyState.AsRead]] = KStream.of(new Into(_), "", false, Ending.flow)

/** A stream being split, as Java's `split` splits it, whose branches are written in the order they are tried: each record goes down the first that
  * takes it.
  */
final class Split[F[_], K, V, P <: KeyState, O] private[xkafka] (
    named: Named,
    stream: KStream[F, K, V] { type Keying = P; type Out = O },
    branches: Vector[SplitBranch[F, K, V]]
):
  private def path(name: Named, keep: (K, V) => Boolean, follow: KStream.Flowing[F, K, V, P, Into[F, K, V, P]] => Into[F, K, V, P]) =
    SplitBranch(Named(s"${named.value}${name.value}"), keep, follow(stream.flow).consumer)

  /** The records `keep` gives true for that no earlier branch took go down `follow`, in a node of the split's name with `name` added. */
  def branch(name: Named)(keep: (K, V) => Boolean)(follow: KStream.Flowing[F, K, V, P, Into[F, K, V, P]] => Into[F, K, V, P]): Split[F, K, V, P, O] =
    new Split(named, stream, branches :+ path(name, keep, follow))

  /** The records no branch took go down `follow`, and the split ends. */
  def defaultBranch(name: Named)(follow: KStream.Flowing[F, K, V, P, Into[F, K, V, P]] => Into[F, K, V, P]): O =
    stream.feed(Consumer.Splitting(named, (branches :+ path(name, (_, _) => true, follow)).toList))

  /** The records no branch took go no further, and the split ends. */
  def noDefaultBranch: O = stream.feed(Consumer.Splitting(named, branches.toList))

/** Records grouped by key, ready to be folded into a store. */
final class KGroupedStream[F[_], K, V, O] private[xkafka] (
    private[xkafka] val stream: KStream.Ends[F, K, V, O],
    through: Option[Repartition],
    key: Serde[F, K],
    value: Serde[F, V]
):
  private def aggregated[A](
      named: Named,
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): KTable.Flowing[F, K, A, KTable.Updated, O] =
    KTable.updating(
      folded(named, Step.Aggregate(store, key, serde, initial, start, add), None),
      TableKey.Plain(key),
      serde,
      None,
      Some(TableView.Stored(store, key, serde))
    )

  /** The stream with `fold` as its last step, after the repartition topic `through` names, if any. */
  private[xkafka] def folded[K2, A](named: Named, fold: Step[F, K, V, K2, A], found: Option[TopologyError]): KStream.Ends[F, K2, A, O] =
    val feeding: Consumer[F, K2, A] => O =
      through match
        case None => next => stream.feed(Consumer.refusing(found, Consumer.Then(named, fold, next)))
        // The records go through a repartition topic first, so the store is in the sub-topology that reads it.
        case Some(repartition) => next =>
            stream.feed(Consumer.Repartitioning(repartition, key, value, false, false, Consumer.refusing(found, Consumer.Then(named, fold, next))))
    KStream.of[F, K2, A, KeyState.AsRead, O](feeding, named.value, false, stream.ending)

  /** This group as a side of a cogroup, whose values `add` folds in. Java makes a repartition topic's source only when it makes the aggregate. */
  private[xkafka] def side[A](add: (K, V, A) => A): CogroupSide[F, K, A, O] =
    new CogroupSide[F, K, A, O]:
      def fed[K2, R](named: Named, number: Int, alone: Boolean, fold: CogroupFold[F, K, A, K2, R], merge: Consumer[F, K2, R]): O =
        val folding = Consumer.Then(named, Step.Cogrouped(number, fold[V](add)), merge)
        val read    = through.fold[Consumer[F, K, V]](folding)(Consumer.Repartitioning(_, key, value, true, false, folding))
        stream.feed(if alone then Consumer.Copartitioned(read) else read)

  /** These records folded along with those of other groups, as Java's `cogroup` does: `add` folds this group's values, and each further group's
    * `cogroup` gives its own, all into one store.
    */
  def cogroup[A](add: (K, V, A) => A): CogroupedKStream[F, K, A, O] = CogroupedKStream(Vector(side(add)), key, stream.ending)

  /** Counts each key's records in `store`, in the step `named`. */
  def count(named: Named, store: Store)(using F: ApplicativeThrow[F]): KTable.Flowing[F, K, Long, KTable.Updated, O] =
    aggregated(named, store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in `store`, starting from its first, in the step `named`. */
  def reduce(named: Named, store: Store)(combine: (V, V) => V): KTable.Flowing[F, K, V, KTable.Updated, O] =
    aggregated(named, store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values into `store`, starting from `initial`, in the step `named`. */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A): KTable.Flowing[F, K, A, KTable.Updated, O] =
    aggregated(named, store, serde, Some(initial), (k, v) => add(k, v, initial), add)

  /** The records split by key and by each of `windows` they fall in, ready to be folded into a window store. */
  def windowedBy(windows: TimeWindows): TimeWindowedKStream[F, K, V, O] = TimeWindowedKStream(this, windows, None, false, key, value)

  /** The records split by key into sessions of activity, ready to be folded into a session store. */
  def windowedBy(windows: SessionWindows): SessionWindowedKStream[F, K, V, O] = SessionWindowedKStream(this, windows, None, false, key, value)

  /** The records split by key and by each sliding window they fall in, ready to be folded into a window store. */
  def windowedBy(windows: SlidingWindows): TimeWindowedKStream[F, K, V, O] = TimeWindowedKStream(this, windows, None, false, key, value)

/** A table's values in new groups, ready to be folded into a store. A value is added to its group when it appears and subtracted when it is replaced
  * or deleted. A group that loses its last value keeps what the fold leaves, as Java's does, and what `count`, `reduce` and `aggregate` make is a
  * table that can delete keys, since a value leaving a group holding nothing, as the 0 a stream's `count` starts from does, goes on as a delete.
  */
final class KGroupedTable[F[_], K, V, O] private[xkafka] (
    stream: KStream.Ends[F, K, TableChange[V], O],
    repartition: Repartition,
    key: Serde[F, K],
    value: Serde[F, V]
)(using F: ApplicativeThrow[F]):
  private def aggregated[A](
      named: Named,
      store: Store,
      serde: Serde[F, A],
      start: (K, V) => A,
      add: (K, V, A) => A,
      subtract: (K, V, A) => A
  ): KTable.Flowing[F, K, A, Option, O] =
    val fold = Step.TableAggregate(store, key, serde, start, add, subtract)
    KTable.deleting(
      KStream.of[F, K, Option[A], KeyState.AsRead, O](
        next =>
          stream
            .feed(Consumer.Repartitioning(repartition, key, Serde.tableChange(value), false, true, Consumer.Then(named, fold, next), Some(named))),
        named.value,
        false,
        stream.ending
      ),
      TableKey.Plain(key),
      serde,
      None,
      Some(TableView.Stored(store, key, serde))
    )

  /** Counts the values in each group in `store`, in the step `named`. */
  def count(named: Named, store: Store): KTable.Flowing[F, K, Long, Option, O] =
    aggregated(named, store, Serde.long[F], (_, _) => 1L, (_, _, n) => n + 1, (_, _, n) => n - 1)

  /** Combines the values in each group in `store`, in the step `named`, starting from the first: `add` puts a value in and `subtract` takes one out.
    */
  def reduce(named: Named, store: Store)(add: (V, V) => V)(subtract: (V, V) => V): KTable.Flowing[F, K, V, Option, O] =
    aggregated(named, store, value, (_, v) => v, (_, v, acc) => add(acc, v), (_, v, acc) => subtract(acc, v))

  /** Folds the values in each group into `store`, in the step `named`, starting from `initial`: `add` puts a value in and `subtract` takes one out.
    */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A)(
      subtract: (K, V, A) => A
  ): KTable.Flowing[F, K, A, Option, O] = aggregated(named, store, serde, (k, v) => add(k, v, initial), add, subtract)

/** Records grouped by key and window, ready to be folded into a window store. Each update to a window goes downstream as it happens. A record later
  * than its window's end by more than the windows' grace is dropped, and a window is kept for its size and grace after its start.
  */
final class TimeWindowedKStream[F[_], K, V, O] private[xkafka] (
    grouped: KGroupedStream[F, K, V, O],
    windows: TimeWindows | SlidingWindows,
    retention: Option[FiniteDuration],
    emitOnClose: Boolean,
    key: Serde[F, K],
    value: Serde[F, V]
):
  private def aggregated[A](
      named: Named,
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): KTable.Flowing[F, Windowed[K], A, KTable.Updated, O] =
    val (problems, keep) = TimeWindowedKStream.kept(windows, retention, store)
    val fold             = TimeWindowedKStream.fold(windows, store, keep, emitOnClose, key, serde, initial, start, add)
    val (size, grace)    = TimeWindowedKStream.sized(windows)
    KTable.updating(grouped.folded(named, fold, problems), TableKey.TimeWindowed(key, size), serde, Some(grace))

  /** Keeps each window in the store for `retention` after its start, which must be at least the windows' size and grace, their default. A window that
    * emits once it closes may expire before it is emitted when the retention is no longer than that, as in Java Kafka Streams, so keep it longer
    * there.
    */
  def withRetention(retention: FiniteDuration): TimeWindowedKStream[F, K, V, O] =
    TimeWindowedKStream(grouped, windows, Some(retention), emitOnClose, key, value)

  /** Passes on only each window's final value, once the window has closed, in place of every update. Closed windows are looked for as records arrive,
    * at most once a second, as Java Kafka Streams looks for them, so a window's value goes downstream with the first record after it closes once that
    * second has passed.
    */
  def emitOnWindowClose: TimeWindowedKStream[F, K, V, O] = TimeWindowedKStream(grouped, windows, retention, true, key, value)

  /** Counts each key's records in each window in `store`, in the step `named`. */
  def count(named: Named, store: Store)(using F: ApplicativeThrow[F]): KTable.Flowing[F, Windowed[K], Long, KTable.Updated, O] =
    aggregated(named, store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in each window in `store`, starting from the window's first, in the step `named`. */
  def reduce(named: Named, store: Store)(combine: (V, V) => V): KTable.Flowing[F, Windowed[K], V, KTable.Updated, O] =
    aggregated(named, store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values in each window into `store`, starting each window from `initial`, in the step `named`. */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(
      add: (K, V, A) => A
  ): KTable.Flowing[F, Windowed[K], A, KTable.Updated, O] = aggregated(named, store, serde, Some(initial), (k, v) => add(k, v, initial), add)

private[xkafka] object TimeWindowedKStream:
  /** What is wrong with `windows` kept for `retention` in `store`, if anything, and how long they are kept, in milliseconds. */
  def kept(windows: TimeWindows | SlidingWindows, retention: Option[FiniteDuration], store: Store): (Option[TopologyError], Long) =
    val (least, rule, problem) =
      windows match
        case time: TimeWindows       => (time.retentionMillis, time.retentionRule, time.problem)
        case sliding: SlidingWindows => (sliding.retentionMillis, sliding.retentionRule, sliding.problem)
    val found =
      problem.orElse:
        retention.flatMap: asked =>
          if asked.toNanos % 1000000L != 0L then Some("the retention must be whole milliseconds")
          else Option.when(asked.toMillis < least)(s"the retention must be at least $rule, ${least} ms, was $asked")
    (found.map(TopologyError.InvalidWindows(store.value, _)), retention.fold(least)(_.toMillis))

  /** The step folding each key's records in `windows` into `store`. */
  def fold[F[_], K, V, A](
      windows: TimeWindows | SlidingWindows,
      store: Store,
      keep: Long,
      emitOnClose: Boolean,
      key: Serde[F, K],
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): Step[F, K, V, Windowed[K], A] =
    windows match
      case time: TimeWindows       => Step.WindowedAggregate(store, time, keep, emitOnClose, key, serde, initial, start, add)
      case sliding: SlidingWindows => Step.SlidingAggregate(store, sliding, keep, emitOnClose, key, serde, initial, start, add)

  /** The size a windowed key is written with, and the grace. */
  def sized(windows: TimeWindows | SlidingWindows): (Long, Long) =
    windows match
      case time: TimeWindows       => (time.sizeMillis, time.graceMillis)
      case sliding: SlidingWindows => (sliding.differenceMillis, sliding.graceMillis)

/** The fold a cogroup makes of a side's values: given what folds a value in, for values of any type, the step that does it. */
private[xkafka] type CogroupFold[F[_], K, A, K2, R] = [V] => ((K, V, A) => A) => Step[F, K, V, K2, R]

/** A group going into a cogroup, whose values only it knows the type of. */
private[xkafka] trait CogroupSide[F[_], K, A, O]:
  /** The group's records folded by `fold` in the step `named`, the side numbered `number`, the only side where `alone` says so, then going into
    * `merge`.
    */
  def fed[K2, R](named: Named, number: Int, alone: Boolean, fold: CogroupFold[F, K, A, K2, R], merge: Consumer[F, K2, R]): O

/** Groups of records with one key type, of any value types, ready to be folded into one store, as Java's `cogroup` makes them: each record is folded
  * by the function of the group it came from, in the group's own step, and each change to the store goes on as one table's. The groups' topics are
  * processed together, so they must have the same number of partitions.
  */
final class CogroupedKStream[F[_], K, A, O] private[xkafka] (sides: Vector[CogroupSide[F, K, A, O]], key: Serde[F, K], ending: Ending[F, O]):
  /** The records of `other` folded in too, by `add`. */
  def cogroup[V](other: KGroupedStream[F, K, V, O])(add: (K, V, A) => A): CogroupedKStream[F, K, A, O] =
    CogroupedKStream(sides :+ other.side(add), key, ending)

  /** The stream of what `fold` makes of each side's records, which meet in the node `<named>-cogroup-merge`, each side's step being
    * `<named>-cogroup-agg-<number>`, as Java names them.
    */
  private[xkafka] def folded[K2, R](named: Named, fold: CogroupFold[F, K, A, K2, R], problems: Option[TopologyError]): KStream.Ends[F, K2, R, O] =
    val merge = Named(s"${named.value}-cogroup-merge")
    KStream.of[F, K2, R, KeyState.AsRead, O](
      next =>
        val met = Consumer.Cogroup(merge, sides.size > 1, Consumer.refusing(problems, next))
        sides.zipWithIndex.map((side, number) => side.fed(Named(s"${named.value}-cogroup-agg-$number"), number, sides.size == 1, fold, met))
          .reduceLeft(ending.both)
      ,
      merge.value,
      false,
      ending
    )

  /** Folds each key's values into `store`, starting from `initial`, in the steps `named` names. */
  def aggregate(named: Named, store: Store, serde: Serde[F, A])(initial: A): KTable.Flowing[F, K, A, KTable.Updated, O] =
    KTable.updating(
      folded[K, A](named, [V] => (add: (K, V, A) => A) => Step.Aggregate(store, key, serde, Some(initial), (k, v) => add(k, v, initial), add), None),
      TableKey.Plain(key),
      serde,
      None,
      Some(TableView.Stored(store, key, serde))
    )

  /** The records split by key and by each of `windows` they fall in, ready to be folded into a window store. */
  def windowedBy(windows: TimeWindows): TimeWindowedCogroupedKStream[F, K, A, O] = TimeWindowedCogroupedKStream(this, windows, None, key)

  /** The records split by key and by each sliding window they fall in, ready to be folded into a window store. */
  def windowedBy(windows: SlidingWindows): TimeWindowedCogroupedKStream[F, K, A, O] = TimeWindowedCogroupedKStream(this, windows, None, key)

  /** The records split by key into sessions of activity, ready to be folded into a session store. */
  def windowedBy(windows: SessionWindows): SessionWindowedCogroupedKStream[F, K, A, O] = SessionWindowedCogroupedKStream(this, windows, None, key)

/** Groups of records split by key and window, ready to be folded into one window store. Each update to a window goes downstream as it happens. A
  * record is dropped when its window has closed by the latest time read from its own group, as in Java.
  */
final class TimeWindowedCogroupedKStream[F[_], K, A, O] private[xkafka] (
    cogrouped: CogroupedKStream[F, K, A, O],
    windows: TimeWindows | SlidingWindows,
    retention: Option[FiniteDuration],
    key: Serde[F, K]
):
  /** Keeps each window in the store for `retention` after its start, which must be at least the windows' size and grace, their default. */
  def withRetention(retention: FiniteDuration): TimeWindowedCogroupedKStream[F, K, A, O] =
    TimeWindowedCogroupedKStream(cogrouped, windows, Some(retention), key)

  /** Folds each key's values in each window into `store`, starting each window from `initial`, in the steps `named` names. */
  def aggregate(named: Named, store: Store, serde: Serde[F, A])(initial: A): KTable.Flowing[F, Windowed[K], A, KTable.Updated, O] =
    val (problems, keep) = TimeWindowedKStream.kept(windows, retention, store)
    val (size, grace)    = TimeWindowedKStream.sized(windows)
    val folding          =
      cogrouped.folded[Windowed[K], A](
        named,
        [V] =>
          (add: (K, V, A) => A) =>
            TimeWindowedKStream.fold(windows, store, keep, false, key, serde, Some(initial), (k: K, v: V) => add(k, v, initial), add),
        problems
      )
    KTable.updating(folding, TableKey.TimeWindowed(key, size), serde, Some(grace))

/** Groups of records split by key into sessions, ready to be folded into one session store, as for [[SessionWindowedKStream]], a record being dropped
  * when its session has closed by the latest time read from its own group, as in Java.
  */
final class SessionWindowedCogroupedKStream[F[_], K, A, O] private[xkafka] (
    cogrouped: CogroupedKStream[F, K, A, O],
    windows: SessionWindows,
    retention: Option[FiniteDuration],
    key: Serde[F, K]
):
  /** Keeps each session in the store for `retention` after its end, which must be at least the inactivity gap and grace, their default. */
  def withRetention(retention: FiniteDuration): SessionWindowedCogroupedKStream[F, K, A, O] =
    SessionWindowedCogroupedKStream(cogrouped, windows, Some(retention), key)

  /** Folds each key's values in each session into `store` from `initial`, merging two sessions' values with `merge` where a record joins them, in the
    * steps `named` names.
    */
  def aggregate(named: Named, store: Store, serde: Serde[F, A])(initial: A)(merge: (K, A, A) => A): KTable.Flowing[F, Windowed[K], A, Option, O] =
    val (problems, keep) = SessionWindowedKStream.kept(windows, retention, store)
    val folding          =
      cogrouped.folded[Windowed[K], Option[A]](
        named,
        [V] =>
          (add: (K, V, A) => A) =>
            Step.SessionAggregate(store, windows, keep, false, key, serde, Some(initial), (k: K, v: V) => add(k, v, initial), add, merge),
        problems
      )
    KTable.deleting(folding, TableKey.SessionWindowed(key), serde, Some(windows.graceMillis + windows.gapMillis))

/** Records grouped by key into sessions, ready to be folded into a session store. Each session's value goes downstream as it changes, and a session
  * merged into another goes on as none, as Java sends a session merged away as a tombstone. A record later than its session's end by more than the
  * inactivity gap and grace is dropped, and a session is kept for the gap and grace after its end.
  */
final class SessionWindowedKStream[F[_], K, V, O] private[xkafka] (
    grouped: KGroupedStream[F, K, V, O],
    windows: SessionWindows,
    retention: Option[FiniteDuration],
    emitOnClose: Boolean,
    key: Serde[F, K],
    value: Serde[F, V]
):
  private def aggregated[A](
      named: Named,
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A,
      merge: (K, A, A) => A
  ): KTable.Flowing[F, Windowed[K], A, Option, O] =
    val (problems, keep) = SessionWindowedKStream.kept(windows, retention, store)
    val fold             = Step.SessionAggregate(store, windows, keep, emitOnClose, key, serde, initial, start, add, merge)
    KTable.deleting(grouped.folded(named, fold, problems), TableKey.SessionWindowed(key), serde, Some(windows.graceMillis + windows.gapMillis))

  /** Counts each key's records in each session in `store`, in the step `named`. */
  def count(named: Named, store: Store)(using F: ApplicativeThrow[F]): KTable.Flowing[F, Windowed[K], Long, Option, O] =
    aggregated(named, store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1, (_, a, b) => a + b)

  /** Combines each key's values in each session in `store`, starting from the session's first, and merging sessions with `combine` too, in the step
    * `named`.
    */
  def reduce(named: Named, store: Store)(combine: (V, V) => V): KTable.Flowing[F, Windowed[K], V, Option, O] =
    aggregated(named, store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v), (_, acc, session) => combine(acc, session))

  /** Folds each key's values in each session into `store` from `initial`, merging two sessions' values with `merge` where a record joins them, in the
    * step `named`.
    */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A)(
      merge: (K, A, A) => A
  ): KTable.Flowing[F, Windowed[K], A, Option, O] = aggregated(named, store, serde, Some(initial), (k, v) => add(k, v, initial), add, merge)

  /** Keeps each session in the store for `retention` after its end, which must be at least the inactivity gap and grace, their default. */
  def withRetention(retention: FiniteDuration): SessionWindowedKStream[F, K, V, O] =
    SessionWindowedKStream(grouped, windows, Some(retention), emitOnClose, key, value)

  /** Passes on only each session's final value once it has closed, as for time windows. */
  def emitOnWindowClose: SessionWindowedKStream[F, K, V, O] = SessionWindowedKStream(grouped, windows, retention, true, key, value)

private[xkafka] object SessionWindowedKStream:
  /** What is wrong with `windows` kept for `retention` in `store`, if anything, and how long they are kept, in milliseconds. */
  def kept(windows: SessionWindows, retention: Option[FiniteDuration], store: Store): (Option[TopologyError], Long) =
    val least = windows.retentionMillis
    val found =
      windows.problem.orElse:
        retention.flatMap: asked =>
          if asked.toNanos % 1000000L != 0L then Some("the retention must be whole milliseconds")
          else Option.when(asked.toMillis < least)(s"the retention must be at least ${windows.retentionRule}, ${least} ms, was $asked")
    (found.map(TopologyError.InvalidWindows(store.value, _)), retention.fold(least)(_.toMillis))

/** A global table as the engine keeps it, whose key and value types only it knows. */
private[xkafka] sealed trait GlobalStore[F[_]]:
  /** What is the same however the table is mapped, by which the program's joins of one table are told apart from those of another. */
  private[xkafka] def identity: AnyRef
  private[xkafka] def named: Named
  private[xkafka] def topic: Topic
  private[xkafka] def store: Store

  /** What a record read from the topic does to the store once the table is up to date, as Java's global task reads it and its table source keeps it:
    * the key and the value to keep, or none to delete the key, written as the table's serdes write them, or nothing for a record without a key. A
    * record the table cannot read fails.
    */
  private[xkafka] def change(key: Option[Chunk[Byte]], value: Option[Chunk[Byte]], headers: Headers)(using
      MonadThrow[F]
  ): F[Option[(Chunk[Byte], Option[Chunk[Byte]])]]

  private[xkafka] def mapK[G[_]](fk: FunctionK[F, G]): GlobalStore[G]

/** A topic's latest value for each key, kept whole by every member of the application, as Java's `GlobalKTable`. A stream joins it by a key taken
  * from each record, so the topic need not be copartitioned with anything. Records are read from it apart from the application's tasks, and no
  * changelog backs it: a member reads the topic to its end as it starts, then follows it.
  */
final class GlobalKTable[F[_], K, V] private (
    private[xkafka] val identity: AnyRef,
    private[xkafka] val named: Named,
    private[xkafka] val topic: Topic,
    private[xkafka] val key: Serde[F, K],
    private[xkafka] val value: Serde[F, V],
    private[xkafka] val store: Store
) extends GlobalStore[F]:
  private[xkafka] def change(key: Option[Chunk[Byte]], value: Option[Chunk[Byte]], headers: Headers)(using
      MonadThrow[F]
  ): F[Option[(Chunk[Byte], Option[Chunk[Byte]])]] =
    key.flatTraverse: bytes =>
      this.key.deserializer.deserialize(topic, headers, Some(bytes)).flatMap(this.key.serializer.serialize(topic, headers, _)).flatMap:
        _.traverse: written =>
          value.flatTraverse(bytes =>
            this.value.deserializer.deserialize(topic, headers, Some(bytes)).flatMap(this.value.serializer.serialize(topic, headers, _))
          ).map(written -> _)

  private[xkafka] def mapK[G[_]](fk: FunctionK[F, G]): GlobalKTable[G, K, V] =
    new GlobalKTable(identity, named, topic, key.mapK(fk), value.mapK(fk), store)

object GlobalKTable:
  /** The table of `topic`, kept in `store`, in the nodes `<named>-source` and `named`, as Java names them. */
  def apply[F[_], K, V](named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V], store: Store): GlobalKTable[F, K, V] =
    new GlobalKTable(new Object, named, topic, key, value, store)

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

  /** A filter kept in no store, as Java's value getter filters what its parent's finds. */
  case Filtered(parent: TableView[F, K, V], keep: (K, V) => Boolean)

  /** A `mapValues` kept in no store, as Java's value getter maps what its parent's finds. */
  case Mapped[F[_], K, A, B](parent: TableView[F, K, A], change: A => B) extends TableView[F, K, B]

  def mapK[G[_]](fk: FunctionK[F, G]): TableView[G, K, V] =
    this match
      case Stored(store, key, value)   => Stored(store, key.mapK(fk), value.mapK(fk))
      case Held(buffer, parent)        => Held(buffer, parent.mapK(fk))
      case Joined(left, right, joiner) => Joined(left.mapK(fk), right.mapK(fk), joiner)
      case Filtered(parent, keep)      => Filtered(parent.mapK(fk), keep)
      case Mapped(parent, change)      => Mapped(parent.mapK(fk), change)

  /** The serde its table's keys are written with. */
  def keys: Serde[F, K] =
    this match
      case Stored(_, key, _)   => key
      case Held(_, parent)     => parent.keys
      case Joined(left, _, _)  => left.keys
      case Filtered(parent, _) => parent.keys
      case Mapped(parent, _)   => parent.keys

/** What a table's change holds: for a table that only ever updates its keys the new value, or for one that can delete them, the new value or none. */
private[xkafka] sealed trait ChangeKind[C[_]]:
  def present[A](change: C[A]): Option[A]
  def pure[A](value: A): C[A]
  def map[A, B](change: C[A])(f: A => B): C[B]

  /** How a suppression's buffer writes changes of values `value` writes. */
  def written[F[_], A](value: Serde[F, A]): TableValue[F, C[A]]

private[xkafka] object ChangeKind:
  val Updating: ChangeKind[KTable.Updated] =
    new ChangeKind[KTable.Updated]:
      def present[A](change: A): Option[A]                       = Some(change)
      def pure[A](value: A): A                                   = value
      def map[A, B](change: A)(f: A => B): B                     = f(change)
      def written[F[_], A](value: Serde[F, A]): TableValue[F, A] = TableValue.Plain(value)

  val Deleting: ChangeKind[Option] =
    new ChangeKind[Option]:
      def present[A](change: Option[A]): Option[A]                       = change
      def pure[A](value: A): Option[A]                                   = Some(value)
      def map[A, B](change: Option[A])(f: A => B): Option[B]             = change.map(f)
      def written[F[_], A](value: Serde[F, A]): TableValue[F, Option[A]] = TableValue.Optional(value)

/** The latest value of each key, kept in a store. Each change to it is a [[Change]] of its value. */
sealed trait KTable[F[_], K, V]:
  /** What a change to the table is: the new value, for a table that only ever updates its keys ([[KTable.Updated]]), or for a table that can also
    * delete them, the new value or none for a delete (`Option`).
    */
  type Change[A]

  /** What ending the table gives, as for [[KStream.Out]]. */
  type Out

  /** The table with values `V2`, whose changes are the same kind. */
  type Same[V2] = KTable.Flowing[F, K, V2, Change, Out]

  /** The table with values `V2`, which can delete its keys. */
  type Deleting[V2] = KTable.Flowing[F, K, V2, Option, Out]

  /** A path starting from the table's changes, which is what `flow` gives and `fork` passes on. */
  type Branch = KTable.Flowing[F, K, V, Change, Into[F, K, Change[V], KeyState.AsRead]]

  private[xkafka] def stream: KStream.Ends[F, K, Change[V], Out]
  private[xkafka] def key: TableKey[F, K]
  private[xkafka] def values: Option[Serde[F, V]]
  private[xkafka] def grace: Option[Long]
  private[xkafka] def view: Option[TableView[F, K, V]]
  private[xkafka] def kind: ChangeKind[Change]

  /** The table on its own, for a join to read, if it reads topics of its own. */
  private[xkafka] lazy val origin: Option[Pipeline[F]] = stream.ending.pipeline(stream.feed(Consumer.Done()))

  /** The table's changes going nowhere further, for a table only kept to be joined with. */
  def consumer: Out = stream.feed(Consumer.Done())

  /** A path starting from the table's changes, to send them to with `into`. */
  def flow: Branch =
    KTable.of[F, K, V, Change, Into[F, K, Change[V], KeyState.AsRead]](
      KStream.of[F, K, Change[V], KeyState.AsRead, Into[F, K, Change[V], KeyState.AsRead]](new Into(_), stream.lastNode, false, Ending.flow),
      key,
      values,
      grace,
      view,
      kind
    )

  /** Sends each change to each of the flows given, as many as there are, in turn. */
  def into[C](to: C)(using takes: KeyState.Takes[F, K, Change[V], C], fits: KeyState.KeyFits[KeyState.AsRead, C]): Out =
    stream.feed(Consumer.fan(takes.consumers(to)))

  /** Sends each change down each of the paths given, in turn, written where the table branches. Each path starts from this table's changes. */
  def fork(first: Branch => Into[F, K, Change[V], KeyState.AsRead], rest: (Branch => Into[F, K, Change[V], KeyState.AsRead])*): Out =
    stream.feed(Consumer.fan((first :: rest.toList).map(_(flow).consumer)))

  /** How a suppression's buffer writes the table's changes, where the table has a serde for its values. */
  private def value: Option[TableValue[F, Change[V]]] = values.map(kind.written(_))

  /** Each change to the table, as a record, keyed as the table is; for a table that can delete keys, a delete is none, which `Serde.option` writes as
    * a tombstone, as Java writes its null.
    */
  def toStream(named: Named): KStream.Flowing[F, K, Change[V], KeyState.AsRead, Out] =
    KStream.of(next => stream.feed(Consumer.Then(named, Step.ToStream(), next)), named.value, false, stream.ending)

  private def suppressed(suppressed: Suppressed, release: Release[K]): Same[V] =
    val unstored = Option.when(value.isEmpty)(TopologyError.SuppressedWithoutSerde(suppressed.name))
    val problems = suppressed.problem.map(TopologyError.InvalidSuppression(suppressed.name, _)).toList ++ unstored
    // A join's result kept in no store has no serde to hold its values with, and its suppression is refused, so the step that stands in never runs.
    val holding = value.fold[Step[F, K, Change[V], K, Change[V]]](Step.FlatMap((_, _) => Nil))(Step.Suppress(suppressed, key, _, release))
    // Java names the suppression's processor as its `Suppressed` names it.
    val named = Named(suppressed.name)
    KTable.of[F, K, V, Change, Out](
      KStream.of[F, K, Change[V], KeyState.AsRead, Out](
        next => stream.feed(Consumer.refusing(problems, Consumer.Then(named, holding, next))),
        named.value,
        stream.keyChanged,
        stream.ending
      ),
      key,
      values,
      grace,
      view.map(TableView.Held(s"${suppressed.name}-store", _)),
      kind
    )

  /** Holds back each key's changes until `timeToWait` has passed since the first held, by the records' own time, then passes on the latest. */
  def suppress(until: Suppressed.UntilTimeLimit): Same[V] = suppressed(until, Release.TimeLimit(until.timeToWait.toMillis))

  /** Holds back each window's changes until the window has closed, its grace over, then passes on its final result; for sessions, a session merged
    * away goes on no more.
    */
  def suppress[W](until: Suppressed.UntilWindowCloses)(using windowed: K <:< Windowed[W]): Same[V] =
    suppressed(until, Release.WindowCloses(windowed(_).window.end, grace.getOrElse(0L)))

  /** The table's changes with `step` as their last step. */
  private def after[K2, A](named: Named, step: Step[F, K, Change[V], K2, A]): KStream.Ends[F, K2, A, Out] =
    KStream.of[F, K2, A, KeyState.AsRead, Out](next => stream.feed(Consumer.Then(named, step, next)), named.value, false, stream.ending)

  /** Puts the table's values in new groups, ready to be folded into a store, as Java's `KTable.groupBy`: `select` gives each value its group's key
    * and what goes in the group, the step being named after `repartition`. Each change goes through the repartition topic
    * `<application id>-<repartition>-repartition`.
    */
  def groupBy[K2, V2](repartition: Repartition, key: Serde[F, K2], value: Serde[F, V2])(select: (K, V) => (K2, V2))(using
      F: ApplicativeThrow[F]
  ): KGroupedTable[F, K2, V2, Out] =
    val grouped: (K, Change[V], Option[Change[V]]) => Vector[(K2, TableChange[V2])] =
      (at, current, old) => TableChange.grouped(select)(at, kind.present(current), old.flatMap(kind.present))
    KGroupedTable(after(Named(repartition.value), Step.TableGroupBy(grouped)), repartition, key, value)

  /** The table's values `keep` keeps, each other value becoming a delete, as Java's table filter makes them. */
  private def filtered(named: Named, keep: (K, V) => Boolean, store: Option[(Store, Serde[F, V])]): Deleting[V] =
    KTable.of[F, K, V, Option, Out](
      after(named, Step.TableFilter[F, K, Change[V], V](keep, kind.present(_), store.map((stored, serde) => (stored, key, serde)))),
      key,
      store.map(_._2).orElse(values),
      grace,
      store.fold(view.map(TableView.Filtered(_, keep)))((stored, serde) => view.map(parent => TableView.Stored(stored, parent.keys, serde))),
      ChangeKind.Deleting
    )

  /** The table's values changed by `change`, as Java's table `mapValues` makes them. */
  private def mapped[V2](named: Named, change: V => V2, store: Option[(Store, Serde[F, V2])]): Same[V2] =
    KTable.of[F, K, V2, Change, Out](
      after(
        named,
        Step.TableMapValues[F, K, Change[V], Change[V2], V2](
          kind.map(_)(change),
          kind.present(_),
          kind.pure(_),
          store.map((stored, serde) => (stored, key, serde))
        )
      ),
      key,
      store.map(_._2),
      grace,
      store.fold(view.map(TableView.Mapped(_, change)))((stored, serde) => view.map(parent => TableView.Stored(stored, parent.keys, serde))),
      kind
    )

  /** The table with only the values `keep` gives true for, as Java's table `filter` makes it: each other change becomes a delete, kept in no store,
    * its values read through this table's.
    */
  def filter(named: Named)(keep: (K, V) => Boolean): Deleting[V] = filtered(named, keep, None)

  /** As `filter`, the result kept in `store`, its values written with `serde`, as Java's filter given a `Materialized` store. */
  def filter(named: Named, store: Store, serde: Serde[F, V])(keep: (K, V) => Boolean): Deleting[V] = filtered(named, keep, Some((store, serde)))

  /** The table without the values `drop` gives true for, as Java's table `filterNot` makes it. */
  def filterNot(named: Named)(drop: (K, V) => Boolean): Deleting[V] = filtered(named, (key, value) => !drop(key, value), None)

  /** As `filterNot`, the result kept in `store`, its values written with `serde`. */
  def filterNot(named: Named, store: Store, serde: Serde[F, V])(drop: (K, V) => Boolean): Deleting[V] =
    filtered(named, (key, value) => !drop(key, value), Some((store, serde)))

  /** The table with each value changed by `change`, as Java's table `mapValues` makes it, kept in no store, its values read through this table's. A
    * suppression of it needs a serde for its values, given with the other `mapValues`.
    */
  def mapValues[V2](named: Named)(change: V => V2): Same[V2] = mapped(named, change, None)

  /** As `mapValues`, the result kept in `store`, its values written with `serde`, as Java's `mapValues` given a `Materialized` store. */
  def mapValues[V2](named: Named, store: Store, serde: Serde[F, V2])(change: V => V2): Same[V2] = mapped(named, change, Some((store, serde)))

  /** The table as one side of a join. */
  private def side: Side[F, K, V, Change[V]] = Side(kind.present(_), view)

  /** The table joined with `other`, by a key `foreignKey` takes from each row, as Java's foreign-key joins join them. */
  private def onForeignKey[KO, VO, R](
      named: Named,
      other: KTable.Ends[F, KO, VO, Out],
      value: Serde[F, V],
      otherKey: Serde[F, KO],
      otherValue: Serde[F, VO],
      foreignKey: (K, V) => Option[KO],
      keepsUnmatched: Boolean,
      joiner: (V, Option[VO]) => Option[R],
      store: Option[(Store, Serde[F, R])]
  ): Deleting[R] =
    (view, other.view) match
      case (Some(mine), Some(_)) =>
        val feeding: Consumer[F, K, Option[R]] => Out =
          next =>
            val join =
              ForeignKeyJoined(named, side, other.side, mine.keys, value, otherKey, otherValue, foreignKey, keepsUnmatched, joiner, store, next)
            stream.ending.both(stream.feed(Consumer.ForeignLeft(join)), other.stream.feed(Consumer.ForeignRight(join)))
        KTable.of[F, K, R, Option, Out](
          KStream.of[F, K, Option[R], KeyState.AsRead, Out](feeding, s"${named.value}-result", false, stream.ending),
          key,
          store.map(_._2),
          None,
          store.map((stored, serde) => TableView.Stored(stored, mine.keys, serde)),
          ChangeKind.Deleting
        )
      case _ =>
        // The join is refused, so the step that stands in for it never runs.
        val nothing: Consumer[F, K, Option[R]] => Out =
          next =>
            stream.feed(
              Consumer.Refused(TopologyError.WindowedJoin, Consumer.Then(named, Step.FlatMap[F, K, Change[V], K, Option[R]]((_, _) => Nil), next))
            )
        KTable.of[F, K, R, Option, Out](
          KStream.of[F, K, Option[R], KeyState.AsRead, Out](nothing, named.value, false, stream.ending),
          key,
          None,
          None,
          None,
          ChangeKind.Deleting
        )

  /** Keeps a result for each row whose foreign key finds a row of `other`, as Java's foreign-key `join`, updated when either table changes. Each
    * table can have its own number of partitions. The join adds the topics `<application id>-<named>-subscription-registration-topic` and
    * `<application id>-<named>-subscription-response-topic`, and the store `<named>-subscription-store`.
    */
  def joinOnForeignKey[KO, VO, R](
      named: Named,
      other: KTable.Ends[F, KO, VO, Out],
      value: Serde[F, V],
      otherKey: Serde[F, KO],
      otherValue: Serde[F, VO]
  )(foreignKey: (K, V) => Option[KO])(join: (V, VO) => R): Deleting[R] =
    onForeignKey(named, other, value, otherKey, otherValue, foreignKey, false, (row, found) => found.map(join(row, _)), None)

  /** As `joinOnForeignKey`, the result kept in the store `store` names. */
  def joinOnForeignKey[KO, VO, R](
      named: Named,
      other: KTable.Ends[F, KO, VO, Out],
      value: Serde[F, V],
      otherKey: Serde[F, KO],
      otherValue: Serde[F, VO],
      store: Store,
      serde: Serde[F, R]
  )(foreignKey: (K, V) => Option[KO])(join: (V, VO) => R): Deleting[R] =
    onForeignKey(named, other, value, otherKey, otherValue, foreignKey, false, (row, found) => found.map(join(row, _)), Some((store, serde)))

  /** As `joinOnForeignKey`, keeping a result for every row, with none where its foreign key finds nothing, as Java's foreign-key `leftJoin`. */
  def leftJoinOnForeignKey[KO, VO, R](
      named: Named,
      other: KTable.Ends[F, KO, VO, Out],
      value: Serde[F, V],
      otherKey: Serde[F, KO],
      otherValue: Serde[F, VO]
  )(foreignKey: (K, V) => Option[KO])(join: (V, Option[VO]) => R): Deleting[R] =
    onForeignKey(named, other, value, otherKey, otherValue, foreignKey, true, (row, found) => Some(join(row, found)), None)

  /** As `leftJoinOnForeignKey`, the result kept in the store `store` names. */
  def leftJoinOnForeignKey[KO, VO, R](
      named: Named,
      other: KTable.Ends[F, KO, VO, Out],
      value: Serde[F, V],
      otherKey: Serde[F, KO],
      otherValue: Serde[F, VO],
      store: Store,
      serde: Serde[F, R]
  )(foreignKey: (K, V) => Option[KO])(join: (V, Option[VO]) => R): Deleting[R] =
    onForeignKey(named, other, value, otherKey, otherValue, foreignKey, true, (row, found) => Some(join(row, found)), Some((store, serde)))

  /** The table joined with `other` by `joiner`, as Java's table-table joins join them: in one sub-topology, their topics copartitioned, and each
    * change to either joined with the other's value for its key. The result is kept in the store `store` names where one is given.
    */
  private def joinedWith[VO, R](
      named: Named,
      other: KTable.Ends[F, K, VO, Out],
      joiner: Joiner[V, VO, R],
      store: Option[(Store, Serde[F, R])]
  ): Deleting[R] =
    (view, other.view) match
      case (Some(mine), Some(their)) =>
        val feeding: Consumer[F, K, Option[R]] => Out =
          next =>
            val keys = store.map((stored, serde) => (stored, mine.keys, serde))
            val join = TablesJoined(named, side, other.side, joiner, keys, next)
            stream.ending.both(stream.feed(Consumer.LeftTable(join)), other.stream.feed(Consumer.RightTable(join)))
        val values =
          store.fold[TableView[F, K, R]](TableView.Joined(mine, their, joiner))((stored, serde) => TableView.Stored(stored, mine.keys, serde))
        KTable.of[F, K, R, Option, Out](
          KStream.of[F, K, Option[R], KeyState.AsRead, Out](feeding, named.value, false, stream.ending),
          key,
          store.map(_._2),
          None,
          Some(values),
          ChangeKind.Deleting
        )
      case _ =>
        // The join is refused, so the step that stands in for it never runs.
        val nothing: Consumer[F, K, Option[R]] => Out =
          next =>
            stream.feed(
              Consumer.Refused(TopologyError.WindowedJoin, Consumer.Then(named, Step.FlatMap[F, K, Change[V], K, Option[R]]((_, _) => Nil), next))
            )
        KTable.of[F, K, R, Option, Out](
          KStream.of[F, K, Option[R], KeyState.AsRead, Out](nothing, named.value, false, stream.ending),
          key,
          None,
          None,
          None,
          ChangeKind.Deleting
        )

  /** The table joined with `other`, as Java's `join` of two tables: each key both hold a value for has what `join` makes of the two, and a change to
    * either side changes it. A delete on either side deletes the key from the result, as Java passes on a delete even for a key the result never
    * held. The result is kept in no store; the result of a join that is suppressed needs one, given with the other `join`.
    */
  def join[VO, R](named: Named, other: KTable.Ends[F, K, VO, Out])(join: (V, VO) => R): Deleting[R] =
    joinedWith(named, other, Joiner.Inner(join), None)

  /** As `join`, the result kept in `store`, its values written with `serde`, as Java's join given a `Materialized` store. */
  def join[VO, R](named: Named, other: KTable.Ends[F, K, VO, Out], store: Store, serde: Serde[F, R])(join: (V, VO) => R): Deleting[R] =
    joinedWith(named, other, Joiner.Inner(join), Some((store, serde)))

  /** The table joined with `other`, as Java's `leftJoin` of two tables: each key this table holds a value for has what `join` makes of it and
    * `other`'s value for the key, if any.
    */
  def leftJoin[VO, R](named: Named, other: KTable.Ends[F, K, VO, Out])(join: (V, Option[VO]) => R): Deleting[R] =
    joinedWith(named, other, Joiner.Left(join), None)

  /** As `leftJoin`, the result kept in `store`, its values written with `serde`. */
  def leftJoin[VO, R](named: Named, other: KTable.Ends[F, K, VO, Out], store: Store, serde: Serde[F, R])(join: (V, Option[VO]) => R): Deleting[R] =
    joinedWith(named, other, Joiner.Left(join), Some((store, serde)))

  /** The table joined with `other`, as Java's `outerJoin` of two tables: each key either table holds a value for has what `join` makes of this
    * table's value, `other`'s, or both.
    */
  def outerJoin[VO, R](named: Named, other: KTable.Ends[F, K, VO, Out])(join: Ior[V, VO] => R): Deleting[R] =
    joinedWith(named, other, Joiner.Outer(join), None)

  /** As `outerJoin`, the result kept in `store`, its values written with `serde`. */
  def outerJoin[VO, R](named: Named, other: KTable.Ends[F, K, VO, Out], store: Store, serde: Serde[F, R])(join: Ior[V, VO] => R): Deleting[R] =
    joinedWith(named, other, Joiner.Outer(join), Some((store, serde)))

object KTable:
  /** A change to a table that only ever updates its keys: the new value. */
  type Updated[A] = A

  /** A table read from topics whose [[KTable.Change]] is `C`, for annotating one without losing it. */
  type Of[F[_], K, V, C[_]] = Flowing[F, K, V, C, Pipeline[F]]

  /** A table whose [[KTable.Change]] is `C` and whose [[KTable.Out]] is `O`. */
  type Flowing[F[_], K, V, C[_], O] = KTable[F, K, V] { type Change[A] = C[A]; type Out = O }

  /** A table whose [[KTable.Out]] is `O`, whatever its changes are. */
  type Ends[F[_], K, V, O] = KTable[F, K, V] { type Out = O }

  /** The records of `topic` as a table, kept in `store`, as Java's `StreamsBuilder.table` reads it: each record's value replaces its key's, and a
    * record without a value deletes the key. A record without a key is skipped. Java names the table's source `<named>-source` and the step that
    * keeps it `named`, as its `Consumed` name names both.
    */
  def apply[F[_], K, V](named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V], store: Store)(using
      F: cats.Applicative[F]
  ): Of[F, K, V, Option] =
    val read = KStream(Named(s"${named.value}-source"), topic, key.option, value.option)
    deleting(read.kept(named, Step.TableSource(store, key, value)), TableKey.Plain(key), value, None, Some(TableView.Stored(store, key, value)))

  private[xkafka] def updating[F[_], K, V, O](
      stream: KStream.Ends[F, K, V, O],
      key: TableKey[F, K],
      value: Serde[F, V],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): Flowing[F, K, V, Updated, O] = of[F, K, V, Updated, O](stream, key, Some(value), grace, view, ChangeKind.Updating)

  private[xkafka] def deleting[F[_], K, V, O](
      stream: KStream.Ends[F, K, Option[V], O],
      key: TableKey[F, K],
      value: Serde[F, V],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): Flowing[F, K, V, Option, O] = of[F, K, V, Option, O](stream, key, Some(value), grace, view, ChangeKind.Deleting)

  private[xkafka] def of[F[_], K, V, C[_], O](
      stream: KStream.Ends[F, K, C[V], O],
      key: TableKey[F, K],
      values: Option[Serde[F, V]],
      grace: Option[Long],
      view: Option[TableView[F, K, V]],
      kind: ChangeKind[C]
  ): Flowing[F, K, V, C, O] = Built(stream, key, values, grace, view, kind)

  private final class Built[F[_], K, V, C[_], O](
      private[xkafka] val stream: KStream.Ends[F, K, C[V], O],
      private[xkafka] val key: TableKey[F, K],
      private[xkafka] val values: Option[Serde[F, V]],
      private[xkafka] val grace: Option[Long],
      private[xkafka] val view: Option[TableView[F, K, V]],
      private[xkafka] val kind: ChangeKind[C]
  ) extends KTable[F, K, V]:
    type Change[A] = C[A]
    type Out       = O

/** A pipeline from an input topic to an output topic. */
final class Pipeline[F[_]] private[xkafka] (private[xkafka] val entries: List[Entry[F]]):
  /** The same pipeline with its effects in `G`. A processor's stores run in `G` and its handlers in `F`, so this needs both directions. */
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Pipeline[G] = new Pipeline(entries.map(_.imapK(fk)(gk)))

  /** This pipeline's records and those of `other`, which reads the same join. */
  private[xkafka] def ++(other: Pipeline[F]): Pipeline[F] = new Pipeline(entries ++ other.entries)

object Pipeline:
  given InvariantK[Pipeline] with
    override def imapK[F[_], G[_]](pipeline: Pipeline[F])(fk: FunctionK[F, G])(gk: FunctionK[G, F]): Pipeline[G] = pipeline.imapK(fk)(gk)

/** A streams program: its sub-topologies, numbered as a Java program building the same topology numbers them. */
final class Topology[F[_]] private (
    private[xkafka] val numbered: List[(Int, Subtopology[F])],
    private[xkafka] val globals: List[(Int, GlobalStore[F])]
):
  /** Its sub-topologies, without the numbers Java gives them, which its global tables also take. */
  private[xkafka] def subtopologies: List[Subtopology[F]] = numbered.map(_._2)

  /** The same program with its effects in `G`, as its serdes and effectful steps are mapped. A processor's stores run in `G` and its handlers in `F`,
    * so this needs both directions.
    */
  def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Topology[G] =
    new Topology(numbered.map((id, subtopology) => id -> subtopology.imapK(fk)(gk)), globals.map((id, global) => id -> global.mapK(fk)))

  /** What the program runs: its topics, steps, stores and repartitions, which `toString` writes as Java Kafka Streams' `Topology.describe` does. */
  def describe: TopologyDescription = Describing(this, identity)

object Topology:
  given InvariantK[Topology] with
    override def imapK[F[_], G[_]](topology: Topology[F])(fk: FunctionK[F, G])(gk: FunctionK[G, F]): Topology[G] = topology.imapK(fk)(gk)

  /** The longest store or repartition name, as Java allows. */
  val MaxNameLength = 249

  def of[F[_]](first: Pipeline[F], rest: Pipeline[F]*): Either[NonEmptyList[TopologyError], Topology[F]] =
    val wired        = Wiring(first :: rest.toList)
    val stores       = wired.subtopologies.flatMap(_.stores).map(_.value) ++ wired.globals.map(_._2.store.value)
    val repartitions = wired.subtopologies.flatMap(_.origins).collect { case Source.Repartitioned(named) if named.value.nonEmpty => named.value }
    def twice[A](values: List[A]) = values.groupBy(identity).collect { case (value, all) if all.size > 1 => value }.toList
    val problems                  =
      wired.problems ++ (stores ++ repartitions ++ wired.steps).flatMap(invalid) ++ twice(wired.steps).sorted.map(TopologyError.DuplicateName(_)) ++
        twice(stores).sorted.map(TopologyError.DuplicateStore(_)) ++ twice(repartitions).sorted.map(TopologyError.DuplicateRepartition(_)) ++
        twice(wired.inputs).sortBy(_.value).map(TopologyError.DuplicateSource(_))
    NonEmptyList.fromList(problems.distinct).toLeft(new Topology(wired.numbered, wired.globals))

  private def invalid(name: String): Option[TopologyError] =
    def legal(c: Char) =
      (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
    if name.isEmpty then Some(TopologyError.InvalidName(name, "it is empty"))
    else if name == "." || name == ".." then Some(TopologyError.InvalidName(name, "it may not be . or .."))
    else if name.length > MaxNameLength then Some(TopologyError.InvalidName(name, s"it is longer than $MaxNameLength characters"))
    else if !name.forall(legal) then Some(TopologyError.InvalidName(name, "it may contain only [a-zA-Z0-9._-]"))
    else None
