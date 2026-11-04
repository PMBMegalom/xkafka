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

import cats.{Applicative, ApplicativeThrow, Show}
import cats.arrow.FunctionK
import cats.data.{Ior, NonEmptyList}
import cats.effect.std.Console

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

  /** Two pipelines reading one topic, which Java refuses too. */
  case DuplicateSource(topic: Topic)

  /** A repartition named, by `groupByKey` or by the join `name` given serdes, for records of a stream whose type does not say they are keyed as they
    * were read but which are, and need none.
    */
  case UnusedRepartition(name: String)

  /** Windows a store's records cannot be windowed by, such as windows of no size. */
  case InvalidWindows(store: String, reason: String)

  /** A suppression that cannot be used, such as one with a negative time to wait. */
  case InvalidSuppression(name: String, reason: String)

  /** A windowed table joined with a stream or a table, which xkafka does not join yet. */
  case WindowedJoin

  /** A table or stream joined with itself, or with one read from the same topics, which xkafka does not join. */
  case JoinedWithItself

  /** A table suppressed whose values have no serde to hold them with: the result of a table join or `mapValues` given no store. */
  case SuppressedWithoutSerde(name: String)

  def message: String =
    this match
      case InvalidName(name, reason)  => s"'$name' cannot name a store, repartitioning or step: $reason"
      case DuplicateName(name)        => s"two steps are named '$name'"
      case DuplicateStore(name)       => s"two stores are named '$name'"
      case DuplicateRepartition(name) => s"two repartitionings are named '$name'"
      case DuplicateSource(topic)     => s"two pipelines read topic '${topic.value}'"
      case UnusedRepartition(name)    =>
        s"the records repartitioned as '$name' are keyed as they were read and need no repartition; group them with groupByKey(key, value), or join them without serdes"
      case InvalidWindows(store, reason)    => s"the windows of store '$store' cannot be used: $reason"
      case InvalidSuppression(name, reason) => s"the suppression '$name' cannot be used: $reason"
      case WindowedJoin                     => "a windowed table cannot be joined yet"
      case JoinedWithItself                 => "a table or stream is joined with itself, or with one read from the same topics"
      case SuppressedWithoutSerde(name)     =>
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
      case ToTable(lift, store, key, value)                  => ToTable(lift, store, key.mapK(fk), value.mapK(fk))
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

  /** The stores it keeps. */
  def kept: List[KeptStore] =
    this match
      case Aggregate(store, _, _, _, _, _)                            => List(KeptStore(store))
      case TableSource(store, _, _)                                   => List(KeptStore(store))
      case ToTable(_, store, _, _)                                    => List(KeptStore(store))
      case TableFilter(_, _, store)                                   => store.toList.map((stored, _, _) => KeptStore(stored))
      case TableMapValues(_, _, _, store)                             => store.toList.map((stored, _, _) => KeptStore(stored))
      case WindowedAggregate(store, _, retention, _, _, _, _, _, _)   => List(KeptStore(store, Some(retention)))
      case SlidingAggregate(store, _, retention, _, _, _, _, _, _)    => List(KeptStore(store, Some(retention)))
      case SessionAggregate(store, _, retention, _, _, _, _, _, _, _) => List(KeptStore(store, Some(retention), session = true))
      case Suppress(suppressed, _, _, _)                              => List(KeptStore(Store(s"${suppressed.name}-store"), buffer = true))
      case _                                                          => Nil

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

/** A topic's records as they come into a sub-topology: from an input topic, or from the repartition topic an earlier sub-topology writes. */
private[xkafka] enum Origin[F[_], K, V]:
  case Input(named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V])
  case Repartitioned(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])

  def mapK[G[_]](fk: FunctionK[F, G]): Origin[G, K, V] =
    this match
      case Input(named, topic, key, value)        => Input(named, topic, key.mapK(fk), value.mapK(fk))
      case Repartitioned(repartition, key, value) => Repartitioned(repartition, key.mapK(fk), value.mapK(fk))

  /** The topic it reads. */
  def source: Source =
    this match
      case Input(_, topic, _, _)            => Source.Input(topic)
      case Repartitioned(repartition, _, _) => Source.Repartitioned(repartition)

  /** The name the program gives its step, where it gives one. */
  def names: List[Named] =
    this match
      case Input(named, _, _, _)  => List(named)
      case Repartitioned(_, _, _) => Nil

/** A store a sub-topology keeps: how long it keeps a window, if it keeps windows, and whether it holds a session's values, a suppression's held
  * changes, or as a stream-stream join's window store does, every record.
  */
private[xkafka] final case class KeptStore(
    store: Store,
    retention: Option[Long] = None,
    session: Boolean = false,
    buffer: Boolean = false,
    duplicates: Boolean = false
)

/** One side of a table-table join: `present` gives each of its changes' value, or none for a delete, and `view` where the other side reads the
  * table's value for a key, or none for a windowed table.
  */
private[xkafka] final case class Side[F[_], K, A, C](present: C => Option[A], view: Option[TableView[F, K, A]]):
  def mapK[G[_]](fk: FunctionK[F, G]): Side[G, K, A, C] = Side(present, view.map(_.mapK(fk)))

/** Two tables joined by `joiner` in the step `named`, each change to either going on to `next` as a change to their join, as Java's table-table join
  * merges what its two sides make, the result kept in `store` where one is given, written with its serdes.
  */
private[xkafka] final class TablesJoined[F[_], K, A, B, R, C1, C2](
    val named: Named,
    val left: Side[F, K, A, C1],
    val right: Side[F, K, B, C2],
    val joiner: Joiner[A, B, R],
    val store: Option[(Store, Serde[F, K], Serde[F, R])],
    val next: Consumer[F, K, Option[R]]
):
  def mapK[G[_]](fk: FunctionK[F, G]): TablesJoined[G, K, A, B, R, C1, C2] =
    TablesJoined(
      named,
      left.mapK(fk),
      right.mapK(fk),
      joiner,
      store.map((stored, key, value) => (stored, key.mapK(fk), value.mapK(fk))),
      next.mapK(fk)
    )

/** Two streams joined by `joiner` within `windows`, as Java's stream-stream join: each record kept in a window store of its side and joined with the
  * other side's records in its window, what they make going on to `next`, its stores named from `store` as [[StreamJoin]] names them, and written
  * with `key`, `value` and `otherValue`.
  */
private[xkafka] final class StreamsJoined[F[_], K, A, B, R](
    val named: Named,
    val windows: JoinWindows,
    val joiner: Joiner[A, B, R],
    val store: Store,
    val key: Serde[F, K],
    val value: Serde[F, A],
    val otherValue: Serde[F, B],
    val next: Consumer[F, K, R]
):
  def mapK[G[_]](fk: FunctionK[F, G]): StreamsJoined[G, K, A, B, R] =
    StreamsJoined(named, windows, joiner, store, key.mapK(fk), value.mapK(fk), otherValue.mapK(fk), next.mapK(fk))

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

/** What happens to records keyed `K` with values `V`, in the direction they run: each node holds what reads what it gives. A join's two sides each
  * hold one object for the join, which what follows the join hangs from.
  */
private[xkafka] enum Consumer[F[_], K, V]:
  /** `step`, then `next` for each record it gives. */
  case Then[F[_], K, V, K2, V2](named: Named, step: Step[F, K, V, K2, V2], next: Consumer[F, K2, V2]) extends Consumer[F, K, V]

  /** Writes each record to the program's output topic `topic`. */
  case Output(named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V])

  /** Writes each record to the repartition topic `repartition` names, whose records `next` reads, in another sub-topology. A join that repartitions a
    * side says so with `atJoin`, since Java makes that topic's source only when it makes the join, which decides how sub-topologies are numbered.
    */
  case Repartitioning(repartition: Repartition, key: Serde[F, K], value: Serde[F, V], atJoin: Boolean, next: Consumer[F, K, V])

  /** `next`, in a sub-topology holding the table `table` keeps, as the table a stream-table join reads runs beside the stream. */
  case Beside(table: Pipeline[F], next: Consumer[F, K, V])

  /** The left side of tables joined, whose changes are `C1`s. */
  case LeftTable[F[_], K, A, B, R, C1, C2](join: TablesJoined[F, K, A, B, R, C1, C2]) extends Consumer[F, K, C1]

  /** The right side of tables joined, whose changes are `C2`s. */
  case RightTable[F[_], K, A, B, R, C1, C2](join: TablesJoined[F, K, A, B, R, C1, C2]) extends Consumer[F, K, C2]

  /** The left side of streams joined. */
  case LeftStream[F[_], K, A, B, R](join: StreamsJoined[F, K, A, B, R]) extends Consumer[F, K, A]

  /** The right side of streams joined. */
  case RightStream[F[_], K, A, B, R](join: StreamsJoined[F, K, A, B, R]) extends Consumer[F, K, B]

  /** Nothing further: the end of a table a join reads, or of a step that only acts on its records, as `foreach` does. */
  case Done()

  def mapK[G[_]](fk: FunctionK[F, G]): Consumer[G, K, V] =
    this match
      case Then(named, step, next)                           => Then(named, step.mapK(fk), next.mapK(fk))
      case Output(named, topic, key, value)                  => Output(named, topic, key.mapK(fk), value.mapK(fk))
      case Repartitioning(repartition, key, value, at, next) => Repartitioning(repartition, key.mapK(fk), value.mapK(fk), at, next.mapK(fk))
      case Beside(table, next)                               => Beside(table.mapK(fk), next.mapK(fk))
      case LeftTable(join)                                   => LeftTable(join.mapK(fk))
      case RightTable(join)                                  => RightTable(join.mapK(fk))
      case LeftStream(join)                                  => LeftStream(join.mapK(fk))
      case RightStream(join)                                 => RightStream(join.mapK(fk))
      case Done()                                            => Done()

  /** The names the program gives its steps, up to the next topic. A join's are found from its right side, which its left side's records come before.
    */
  def names: List[Named] =
    this match
      case Then(named, _, next)   => named :: next.names
      case Output(named, _, _, _) => List(named)
      case Beside(_, next)        => next.names
      case RightTable(join)       => join.named :: join.next.names
      case RightStream(join)      => join.named :: join.next.names
      case _                      => Nil

  /** The stores it keeps, up to the next topic, in the order the steps come. */
  def kept: List[KeptStore] =
    this match
      case Then(_, step, next) => step.kept ++ next.kept
      case Beside(_, next)     => next.kept
      case RightTable(join)    => join.store.toList.map((stored, _, _) => KeptStore(stored)) ++ join.next.kept
      case RightStream(join)   => join.kept ++ join.next.kept
      case _                   => Nil

  /** The repartition topics it writes, up to the next topic. */
  def repartitions: List[Repartition] =
    this match
      case Then(_, _, next)                        => next.repartitions
      case Repartitioning(repartition, _, _, _, _) => List(repartition)
      case Beside(_, next)                         => next.repartitions
      case RightTable(join)                        => join.next.repartitions
      case RightStream(join)                       => join.next.repartitions
      case _                                       => Nil

  /** The topics read from here on: the repartition topics it writes, and the topics of the tables it reads. */
  def reads: Set[Source] =
    this match
      case Then(_, _, next)                           => next.reads
      case Repartitioning(repartition, _, _, _, next) => next.reads + Source.Repartitioned(repartition)
      case Beside(table, next)                        => table.reads ++ next.reads
      case LeftTable(join)                            => join.next.reads
      case RightTable(join)                           => join.next.reads
      case LeftStream(join)                           => join.next.reads
      case RightStream(join)                          => join.next.reads
      case _                                          => Set.empty

/** A topic's records going into what reads them. */
private[xkafka] sealed trait Entry[F[_]]:
  /** The key and value it reads. */
  type K
  type V

  def origin: Origin[F, K, V]
  def consumer: Consumer[F, K, V]

  def mapK[G[_]](fk: FunctionK[F, G]): Entry[G] = Entry(origin.mapK(fk), consumer.mapK(fk))

  /** The topics read from here on, this one first. */
  def reads: Set[Source] = consumer.reads + origin.source

private[xkafka] object Entry:
  def apply[F[_], K0, V0](origin: Origin[F, K0, V0], consumer: Consumer[F, K0, V0]): Entry[F] = Of(origin, consumer)

  private final case class Of[F[_], K0, V0](origin: Origin[F, K0, V0], consumer: Consumer[F, K0, V0]) extends Entry[F]:
    type K = K0
    type V = V0

/** The topics and steps that make one sub-topology: the topics it reads, its own first, each going into what reads it; the groups of topics its joins
  * copartition, as Java groups their sources; and the joins it holds, by the topics they read, whose sub-topologies Java numbers by which of their
  * inputs its program builds first.
  */
private[xkafka] final case class Subtopology[F[_]](entries: List[Entry[F]], copartitioned: List[List[Source]], buildOrdered: List[List[Source]]):
  def mapK[G[_]](fk: FunctionK[F, G]): Subtopology[G] = Subtopology(entries.map(_.mapK(fk)), copartitioned, buildOrdered)

  /** The topics it reads, its own first. */
  def origins: List[Source] = entries.map(_.origin.source)

  private def kept: List[KeptStore] = entries.flatMap(_.consumer.kept)

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

  /** The repartition topics it writes. */
  def repartitions: List[Repartition] = entries.flatMap(_.consumer.repartitions)

/** Records read from a topic, changed step by step. Its [[Keying]] says whether the records are still keyed as they were read, which decides how they
  * can be grouped or joined by key: records that may have a new key go through a repartition topic first, which `groupByKey` and `join` then take a
  * name or serdes for. A stream whose type does not say is taken to have changed its key.
  */
sealed trait KStream[F[_], K, V]:
  /** Whether the records are keyed as they were read ([[KeyState.AsRead]]) or may have a new key ([[KeyState.Changed]]). */
  type Keying <: KeyState

  /** The pipeline these records go through, with `next` reading them. */
  private[xkafka] def feed(next: Consumer[F, K, V]): Pipeline[F]

  /** The name of the node its records come out of, as Java names it. */
  private[xkafka] def lastNode: String
  private[xkafka] def keyChanged: Boolean
  private[xkafka] def problems: Vector[TopologyError]

  /** The stream with `step` as its last step, which leaves each record in the partition of its key, such as one that keeps the key, or windows it. */
  private[xkafka] def kept[K2, V2](named: Named, step: Step[F, K, V, K2, V2]): KStream.Of[F, K2, V2, Keying] =
    KStream.of(next => feed(Consumer.Then(named, step, next)), named.value, keyChanged, problems)

  /** The stream with `step` as its last step, which may give records a new key. */
  private def rekeyed[K2, V2](named: Named, step: Step[F, K, V, K2, V2]): KStream.Of[F, K2, V2, KeyState.Changed] =
    KStream.of(next => feed(Consumer.Then(named, step, next)), named.value, true, problems)

  /** Keeps the records `keep` gives true for. */
  def filter(named: Named)(keep: (K, V) => Boolean): KStream.Of[F, K, V, Keying] = kept(named, Step.Filter(keep))

  /** Drops the records `drop` gives true for. */
  def filterNot(named: Named)(drop: (K, V) => Boolean): KStream.Of[F, K, V, Keying] = kept(named, Step.Filter((key, value) => !drop(key, value)))

  /** Replaces each record with what `change` gives. */
  def map[K2, V2](named: Named)(change: (K, V) => (K2, V2)): KStream.Of[F, K2, V2, KeyState.Changed] = rekeyed(named, Step.Map(change))

  /** Replaces each value with what `change` gives. */
  def mapValues[V2](named: Named)(change: V => V2): KStream.Of[F, K, V2, Keying] = kept(named, Step.MapValues(change))

  /** Replaces each record with the records `change` gives, none or more. */
  def flatMap[K2, V2](named: Named)(change: (K, V) => Iterable[(K2, V2)]): KStream.Of[F, K2, V2, KeyState.Changed] =
    rekeyed(named, Step.FlatMap(change))

  /** Replaces each value with the values `change` gives, none or more, each under the record's key. */
  def flatMapValues[V2](named: Named)(change: V => Iterable[V2]): KStream.Of[F, K, V2, Keying] =
    kept(named, Step.FlatMap((key, value) => change(value).map(key -> _)))

  /** Gives each record the key `change` gives. */
  def selectKey[K2](named: Named)(change: (K, V) => K2): KStream.Of[F, K2, V, KeyState.Changed] =
    rekeyed(named, Step.Map((k, v) => (change(k, v), v)))

  /** Runs `effect` for each record and passes the record on. The effect runs again for records replayed after a crash or a fencing. */
  def peek(named: Named)(effect: (K, V) => F[Unit]): KStream.Of[F, K, V, Keying] = kept(named, Step.Peek(effect))

  /** Runs `effect` for each record, and ends the pipeline there, as Java's `foreach` does. The effect runs again for records replayed after a crash
    * or a fencing.
    */
  def foreach(named: Named)(effect: (K, V) => F[Unit]): Pipeline[F] = ended(Consumer.Then(named, Step.Peek(effect), Consumer.Done()))

  /** Prints each record as `[label]: key, value`, labelled with the name of the step before, in the step `named`, and ends the pipeline there, as
    * Java's `print` to standard output does.
    */
  def print(named: Named)(using Console[F]): Pipeline[F] = print(named, lastNode)

  /** Prints each record as `[label]: key, value`, in the step `named`, and ends the pipeline there. */
  def print(named: Named, label: String)(using console: Console[F]): Pipeline[F] =
    foreach(named)((key, value) => console.println(s"[$label]: $key, $value"))

  /** Replaces each value with what `change` gives. Its output is written once under exactly-once processing, but the effect runs again for records
    * replayed after a crash or a fencing.
    */
  def evalMapValues[V2](named: Named)(change: V => F[V2]): KStream.Of[F, K, V2, Keying] = kept(named, Step.EvalMapValues(change))

  /** Keeps the records `keep` gives true for. The effect runs again for records replayed after a crash or a fencing. */
  def evalFilter(named: Named)(keep: (K, V) => F[Boolean]): KStream.Of[F, K, V, Keying] = kept(named, Step.EvalFilter(keep))

  /** Replaces each record with what `change` gives. Its output is written once under exactly-once processing, but the effect runs again for records
    * replayed after a crash or a fencing.
    */
  def evalMap[K2, V2](named: Named)(change: (K, V) => F[(K2, V2)]): KStream.Of[F, K2, V2, KeyState.Changed] = rekeyed(named, Step.EvalMap(change))

  /** Groups records by a new key, through the repartition topic `repartition` names, which also names the step that gives them the key, as Java's
    * `Grouped` names both.
    */
  def groupBy[K2](repartition: Repartition, key: Serde[F, K2], value: Serde[F, V])(change: (K, V) => K2): KGroupedStream[F, K2, V] =
    KGroupedStream(selectKey(Named(repartition.value))(change), Some(repartition), key, value)

  /** Groups records keyed as they were read by their key. */
  def groupByKey(key: Serde[F, K], value: Serde[F, V])(using KeyState.Unchanged[Keying]): KGroupedStream[F, K, V] =
    KGroupedStream(this, None, key, value)

  /** Groups records that may have a new key by it, through the repartition topic `repartition` names. A stream whose type does not say, and whose key
    * has not changed, needs none, and `Topology.of` refuses the name.
    */
  def groupByKey(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])(using KeyState.MayHaveChanged[Keying]): KGroupedStream[F, K, V] =
    if keyChanged then KGroupedStream(this, Some(repartition), key, value)
    else KGroupedStream(KStream.of(feed, lastNode, false, problems :+ TopologyError.UnusedRepartition(repartition.value)), None, key, value)

  /** Each record with what `join` makes of it and its key's value in `table`, or none, read as Java's stream-table join reads it when the record
    * comes; the table runs in the same sub-topology, so its topic and the stream's are copartitioned. Records go through a repartition topic first
    * where `through` gives the serdes to write them with, which Java names after the join, as its `Joined` name names both.
    */
  private def joined[TV, R](
      named: Named,
      table: KTable[F, K, TV],
      through: Option[(Serde[F, K], Serde[F, V])],
      found: Vector[TopologyError],
      join: (V, Option[TV]) => Option[R]
  ): KStream.Of[F, K, R, KeyState.AsRead] =
    val windowed = Option.when(table.view.isEmpty)(TopologyError.WindowedJoin).toVector
    // A windowed table has no view, and its join is refused, so the step that stands in for it never runs.
    val step                                                = table.view.fold[Step[F, K, V, K, R]](Step.FlatMap((_, _) => Nil))(Step.Join(_, join))
    def reading(next: Consumer[F, K, R]): Consumer[F, K, V] = Consumer.Beside(table.stream.feed(Consumer.Done()), Consumer.Then(named, step, next))
    val feeding: Consumer[F, K, R] => Pipeline[F]           =
      through match
        case None               => next => feed(reading(next))
        case Some((key, value)) => next => feed(Consumer.Repartitioning(Repartition(named.value), key, value, true, reading(next)))
    KStream.of(feeding, named.value, false, problems ++ table.stream.problems ++ found ++ windowed)

  /** The serdes a join given `key` and `value` repartitions the records with, where their key has changed, or the refusal of a join `named` that
    * names a repartition the records do not need.
    */
  private def joinThrough(named: Named, key: Serde[F, K], value: Serde[F, V]): (Option[(Serde[F, K], Serde[F, V])], Vector[TopologyError]) =
    if keyChanged then (Some((key, value)), Vector.empty) else (None, Vector(TopologyError.UnusedRepartition(named.value)))

  /** Each record joined with its key's value in `table`, as Java's `join` of a stream with a table: a record whose key the table holds no value for
    * is dropped. The records are keyed as they were read, so that the table and they are partitioned alike.
    */
  def join[TV, R](named: Named, table: KTable[F, K, TV])(join: (V, TV) => R)(using KeyState.Unchanged[Keying]): KStream.Of[F, K, R, KeyState.AsRead] =
    joined(named, table, None, Vector.empty, (v, tv) => tv.map(join(v, _)))

  /** As `join`, for records that may have a new key, after the repartition topic `named` names, written with `key` and `value`. */
  def join[TV, R](named: Named, table: KTable[F, K, TV], key: Serde[F, K], value: Serde[F, V])(join: (V, TV) => R)(using
      KeyState.MayHaveChanged[Keying]
  ): KStream.Of[F, K, R, KeyState.AsRead] =
    val (through, found) = joinThrough(named, key, value)
    joined(named, table, through, found, (v, tv) => tv.map(join(v, _)))

  /** Each record joined with its key's value in `table`, or none where the table holds no value for it, as Java's `leftJoin` of a stream with a
    * table. The records are keyed as they were read.
    */
  def leftJoin[TV, R](named: Named, table: KTable[F, K, TV])(join: (V, Option[TV]) => R)(using
      KeyState.Unchanged[Keying]
  ): KStream.Of[F, K, R, KeyState.AsRead] = joined(named, table, None, Vector.empty, (v, tv) => Some(join(v, tv)))

  /** As `leftJoin`, for records that may have a new key, after the repartition topic `named` names, written with `key` and `value`. */
  def leftJoin[TV, R](named: Named, table: KTable[F, K, TV], key: Serde[F, K], value: Serde[F, V])(join: (V, Option[TV]) => R)(using
      KeyState.MayHaveChanged[Keying]
  ): KStream.Of[F, K, R, KeyState.AsRead] =
    val (through, found) = joinThrough(named, key, value)
    joined(named, table, through, found, (v, tv) => Some(join(v, tv)))

  /** The topics the stream reads, through everything that makes it. */
  private def read: Set[Source] = feed(Consumer.Done()).reads

  /** The stream as one side of a stream-stream join, read by `side`: as it is, or where its key has changed, from the repartition topic `repartition`
    * names, which the stream is written to with `key` and `value`, as Java repartitions each side of the join.
    */
  private def joinSide(repartition: Repartition, key: Serde[F, K], value: Serde[F, V], side: Consumer[F, K, V]): Pipeline[F] =
    if keyChanged then feed(Consumer.Repartitioning(repartition, key, value, true, side)) else feed(side)

  /** The stream joined with `other` by `joiner` within `windows`, as Java's stream-stream joins join them: in one sub-topology, their topics
    * copartitioned, each side repartitioned through `<store>-left` or `<store>-right` where its key has changed.
    */
  private def windowJoined[VO, R](
      named: Named,
      other: KStream[F, K, VO],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO],
      joiner: Joiner[V, VO, R]
  ): KStream.Of[F, K, R, KeyState.AsRead] =
    val problems = this.problems ++ other.problems ++ windows.problem.map(TopologyError.InvalidWindows(store.value, _))
    if (read & other.read).nonEmpty then
      // The join is refused, so the step that stands in for it never runs.
      val nothing = kept(named, Step.FlatMap[F, K, V, K, R]((_, _) => Nil))
      KStream.of(nothing.feed, nothing.lastNode, false, problems :+ TopologyError.JoinedWithItself)
    else
      // Java names the repartition topics after the join, as `StreamJoined.withName` names both, and the stores after the store's name.
      val feeding: Consumer[F, K, R] => Pipeline[F] =
        next =>
          val join = StreamsJoined(named, windows, joiner, store, key, value, otherValue, next)
          joinSide(Repartition(s"${named.value}-left"), key, value, Consumer.LeftStream(join)) ++
            other.joinSide(Repartition(s"${named.value}-right"), key, otherValue, Consumer.RightStream(join))
      KStream.of(feeding, s"${named.value}-merge", false, problems)

  /** Each record joined with each record of `other` with the same key in its window, as Java's `join` of two streams: a record with no match in
    * `other` goes on no further. The stores `<store>-this-join-store` and `<store>-other-join-store` keep each side's records for the windows' size
    * and grace, written with `key`, `value` and `otherValue`, which also write a side whose key has changed to the repartition topic `<named>-left`
    * or `<named>-right` names. The result's time is the later of the two records'.
    */
  def join[VO, R](
      named: Named,
      other: KStream[F, K, VO],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO]
  )(join: (V, VO) => R): KStream.Of[F, K, R, KeyState.AsRead] = windowJoined(named, other, windows, store, key, value, otherValue, Joiner.Inner(join))

  /** As `join`, but a record with no match in `other` also goes on, with none for it, once its window has closed, as Java's `leftJoin` of two streams
    * holds it in the store `<store>-left-shared-join-store` until then. The store of `other`'s records is `<store>-outer-other-join-store`.
    */
  def leftJoin[VO, R](
      named: Named,
      other: KStream[F, K, VO],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO]
  )(join: (V, Option[VO]) => R): KStream.Of[F, K, R, KeyState.AsRead] =
    windowJoined(named, other, windows, store, key, value, otherValue, Joiner.Left(join))

  /** As `join`, but a record of either stream with no match in the other also goes on alone once its window has closed, as Java's `outerJoin` of two
    * streams holds it in the store `<store>-outer-shared-join-store` until then. The sides' stores are `<store>-outer-this-join-store` and
    * `<store>-outer-other-join-store`.
    */
  def outerJoin[VO, R](
      named: Named,
      other: KStream[F, K, VO],
      windows: JoinWindows,
      store: Store,
      key: Serde[F, K],
      value: Serde[F, V],
      otherValue: Serde[F, VO]
  )(join: Ior[V, VO] => R): KStream.Of[F, K, R, KeyState.AsRead] =
    windowJoined(named, other, windows, store, key, value, otherValue, Joiner.Outer(join))

  /** Sends every record through the repartition topic `repartition` names, written with `key` and `value`, whether or not its key has changed, as
    * Java's `repartition` does. Afterwards the records are keyed as they were read from that topic.
    */
  def repartition(repartition: Repartition, key: Serde[F, K], value: Serde[F, V]): KStream.Of[F, K, V, KeyState.AsRead] =
    KStream
      .of(next => feed(Consumer.Repartitioning(repartition, key, value, false, next)), s"${repartition.value}-repartition-source", false, problems)

  /** The latest value of each key as a table in `store`, in the step `named`, as Java's `toTable` makes it. Records that may have a new key go
    * through a repartition topic first, which `named` names and `key` and `through` write.
    */
  private def tabled[A](named: Named, store: Store, key: Serde[F, K], through: Serde[F, V], value: Serde[F, A], lift: V => Option[A]) =
    val read: KStream[F, K, V] = if keyChanged then repartition(Repartition(named.value), key, through) else this
    KTable
      .deleting(read.kept(named, Step.ToTable(lift, store, key, value)), TableKey.Plain(key), value, None, Some(TableView.Stored(store, key, value)))

  /** The latest value of each key as a table kept in `store`, in the step `named`, written with `key` and `value`, as Java's `toTable` makes it.
    * Records whose key may have changed go through a repartition topic that `named` names first.
    */
  def toTable(named: Named, store: Store, key: Serde[F, K], value: Serde[F, V]): KTable.Of[F, K, V, Option] =
    tabled(named, store, key, value, value, Some(_))

  /** As `toTable`, for optional values, of which none deletes the key, as Java's null does. */
  def toTable[A](named: Named, store: Store, key: Serde[F, K], value: Serde[F, A])(using
      optional: V =:= Option[A],
      F: Applicative[F]
  ): KTable.Of[F, K, A, Option] = tabled(named, store, key, optional.flip.substituteCo[[X] =>> Serde[F, X]](value.option), value, optional(_))

  /** The pipeline ending with `end`. */
  private def ended(end: Consumer[F, K, V]): Pipeline[F] = Pipeline(feed(end).entries, problems)

  /** Writes every record to `topic`, in the step `named`. */
  def to(named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V]): Pipeline[F] = ended(Consumer.Output(named, topic, key, value))

object KStream:
  /** A stream whose [[KStream.Keying]] is `P`, for annotating one without losing it. */
  type Of[F[_], K, V, P <: KeyState] = KStream[F, K, V] { type Keying = P }

  /** The records of `topic`, read in the step `named`, as Java's `Consumed` names it. */
  def apply[F[_], K, V](named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V]): Of[F, K, V, KeyState.AsRead] =
    of(next => Pipeline(List(Entry(Origin.Input(named, topic, key, value), next)), Vector.empty), named.value, false, Vector.empty)

  private[xkafka] def of[F[_], K, V, P <: KeyState](
      feed: Consumer[F, K, V] => Pipeline[F],
      lastNode: String,
      keyChanged: Boolean,
      problems: Vector[TopologyError]
  ): Of[F, K, V, P] = Built(feed, lastNode, keyChanged, problems)

  private final class Built[F[_], K, V, P <: KeyState](
      feeding: Consumer[F, K, V] => Pipeline[F],
      private[xkafka] val lastNode: String,
      private[xkafka] val keyChanged: Boolean,
      private[xkafka] val problems: Vector[TopologyError]
  ) extends KStream[F, K, V]:
    type Keying = P

    private[xkafka] def feed(next: Consumer[F, K, V]): Pipeline[F] = feeding(next)

/** Records grouped by key, ready to be folded into a store. */
final class KGroupedStream[F[_], K, V] private[xkafka] (stream: KStream[F, K, V], through: Option[Repartition], key: Serde[F, K], value: Serde[F, V]):
  private def aggregated[A](
      named: Named,
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): KTable.Of[F, K, A, KTable.Updated] =
    KTable.updating(
      folded(named, Step.Aggregate(store, key, serde, initial, start, add), Vector.empty),
      TableKey.Plain(key),
      serde,
      None,
      Some(TableView.Stored(store, key, serde))
    )

  /** The stream with `fold` as its last step, after the repartition topic `through` names, if any. */
  private[xkafka] def folded[K2, A](named: Named, fold: Step[F, K, V, K2, A], found: Vector[TopologyError]): KStream[F, K2, A] =
    val feeding: Consumer[F, K2, A] => Pipeline[F] =
      through match
        case None => next => stream.feed(Consumer.Then(named, fold, next))
        // The records go through a repartition topic first, so the store is in the sub-topology that reads it.
        case Some(repartition) => next => stream.feed(Consumer.Repartitioning(repartition, key, value, false, Consumer.Then(named, fold, next)))
    KStream.of(feeding, named.value, false, stream.problems ++ found)

  /** Counts each key's records in `store`, in the step `named`. */
  def count(named: Named, store: Store)(using F: ApplicativeThrow[F]): KTable.Of[F, K, Long, KTable.Updated] =
    aggregated(named, store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in `store`, starting from its first, in the step `named`. */
  def reduce(named: Named, store: Store)(combine: (V, V) => V): KTable.Of[F, K, V, KTable.Updated] =
    aggregated(named, store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values into `store`, starting from `initial`, in the step `named`. */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A): KTable.Of[F, K, A, KTable.Updated] =
    aggregated(named, store, serde, Some(initial), (k, v) => add(k, v, initial), add)

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
      named: Named,
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A
  ): KTable.Of[F, Windowed[K], A, KTable.Updated] =
    val (least, rule, problem) =
      windows match
        case time: TimeWindows       => (time.retentionMillis, time.retentionRule, time.problem)
        case sliding: SlidingWindows => (sliding.retentionMillis, sliding.retentionRule, sliding.problem)
    val kept =
      problem.orElse:
        retention.flatMap: asked =>
          if asked.toNanos % 1000000L != 0L then Some("the retention must be whole milliseconds")
          else Option.when(asked.toMillis < least)(s"the retention must be at least $rule, ${least} ms, was $asked")
    val problems = kept.map(TopologyError.InvalidWindows(store.value, _)).toVector
    val keep     = retention.fold(least)(_.toMillis)
    val fold     =
      windows match
        case time: TimeWindows       => Step.WindowedAggregate(store, time, keep, emitOnClose, key, serde, initial, start, add)
        case sliding: SlidingWindows => Step.SlidingAggregate(store, sliding, keep, emitOnClose, key, serde, initial, start, add)
    val (size, grace) =
      windows match
        case time: TimeWindows       => (time.sizeMillis, time.graceMillis)
        case sliding: SlidingWindows => (sliding.differenceMillis, sliding.graceMillis)
    KTable.updating(grouped.folded(named, fold, problems), TableKey.TimeWindowed(key, size), serde, Some(grace))

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

  /** Counts each key's records in each window in `store`, in the step `named`. */
  def count(named: Named, store: Store)(using F: ApplicativeThrow[F]): KTable.Of[F, Windowed[K], Long, KTable.Updated] =
    aggregated(named, store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in each window in `store`, starting from the window's first, in the step `named`. */
  def reduce(named: Named, store: Store)(combine: (V, V) => V): KTable.Of[F, Windowed[K], V, KTable.Updated] =
    aggregated(named, store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values in each window into `store`, starting each window from `initial`, in the step `named`. */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A): KTable.Of[F, Windowed[K], A, KTable.Updated] =
    aggregated(named, store, serde, Some(initial), (k, v) => add(k, v, initial), add)

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
  private def aggregated[A](
      named: Named,
      store: Store,
      serde: Serde[F, A],
      initial: Option[A],
      start: (K, V) => A,
      add: (K, V, A) => A,
      merge: (K, A, A) => A
  ) =
    val least = windows.retentionMillis
    val kept  =
      windows.problem.orElse:
        retention.flatMap: asked =>
          if asked.toNanos % 1000000L != 0L then Some("the retention must be whole milliseconds")
          else Option.when(asked.toMillis < least)(s"the retention must be at least ${windows.retentionRule}, ${least} ms, was $asked")
    val problems = kept.map(TopologyError.InvalidWindows(store.value, _)).toVector
    val fold     = Step.SessionAggregate(store, windows, retention.fold(least)(_.toMillis), emitOnClose, key, serde, initial, start, add, merge)
    KTable.deleting(grouped.folded(named, fold, problems), TableKey.SessionWindowed(key), serde, Some(windows.graceMillis + windows.gapMillis))

  /** Counts each key's records in each session in `store`, in the step `named`. */
  def count(named: Named, store: Store)(using F: ApplicativeThrow[F]): KTable.Of[F, Windowed[K], Long, Option] =
    aggregated(named, store, Serde.long[F], Some(0L), (_, _) => 1L, (_, _, n) => n + 1, (_, a, b) => a + b)

  /** Combines each key's values in each session in `store`, starting from the session's first, and merging sessions with `combine` too, in the step
    * `named`.
    */
  def reduce(named: Named, store: Store)(combine: (V, V) => V): KTable.Of[F, Windowed[K], V, Option] =
    aggregated(named, store, value, None, (_, v) => v, (_, v, acc) => combine(acc, v), (_, acc, session) => combine(acc, session))

  /** Folds each key's values in each session into `store` from `initial`, merging two sessions' values with `merge` where a record joins them, in the
    * step `named`.
    */
  def aggregate[A](named: Named, store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A)(
      merge: (K, A, A) => A
  ): KTable.Of[F, Windowed[K], A, Option] = aggregated(named, store, serde, Some(initial), (k, v) => add(k, v, initial), add, merge)

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

  private[xkafka] def stream: KStream[F, K, Change[V]]
  private[xkafka] def key: TableKey[F, K]
  private[xkafka] def values: Option[Serde[F, V]]
  private[xkafka] def grace: Option[Long]
  private[xkafka] def view: Option[TableView[F, K, V]]
  private[xkafka] def kind: ChangeKind[Change]

  /** How a suppression's buffer writes the table's changes, where the table has a serde for its values. */
  private def value: Option[TableValue[F, Change[V]]] = values.map(kind.written(_))

  /** Each change to the table, as a record, keyed as the table is; for a table that can delete keys, a delete is none, which `Serde.option` writes as
    * a tombstone, as Java writes its null.
    */
  def toStream(named: Named): KStream.Of[F, K, Change[V], KeyState.AsRead] =
    KStream.of(next => stream.feed(Consumer.Then(named, Step.ToStream(), next)), named.value, false, stream.problems)

  private def suppressed(suppressed: Suppressed, release: Release[K]): KTable.Of[F, K, V, Change] =
    val unstored = Option.when(value.isEmpty)(TopologyError.SuppressedWithoutSerde(suppressed.name))
    val problems = stream.problems ++ suppressed.problem.map(TopologyError.InvalidSuppression(suppressed.name, _)) ++ unstored
    // A join's result kept in no store has no serde to hold its values with, and its suppression is refused, so the step that stands in never runs.
    val holding = value.fold[Step[F, K, Change[V], K, Change[V]]](Step.FlatMap((_, _) => Nil))(Step.Suppress(suppressed, key, _, release))
    // Java names the suppression's processor as its `Suppressed` names it.
    val held = stream.kept(Named(suppressed.name), holding)
    KTable.of[F, K, V, Change](
      KStream.of(held.feed, held.lastNode, stream.keyChanged, problems),
      key,
      values,
      grace,
      view.map(TableView.Held(s"${suppressed.name}-store", _)),
      kind
    )

  /** Holds back each key's changes until `timeToWait` has passed since the first held, by the records' own time, then passes on the latest. */
  def suppress(until: Suppressed.UntilTimeLimit): KTable.Of[F, K, V, Change] = suppressed(until, Release.TimeLimit(until.timeToWait.toMillis))

  /** Holds back each window's changes until the window has closed, its grace over, then passes on its final result; for sessions, a session merged
    * away goes on no more.
    */
  def suppress[W](until: Suppressed.UntilWindowCloses)(using windowed: K <:< Windowed[W]): KTable.Of[F, K, V, Change] =
    suppressed(until, Release.WindowCloses(windowed(_).window.end, grace.getOrElse(0L)))

  /** The table's values `keep` keeps, each other value becoming a delete, as Java's table filter makes them. */
  private def filtered(named: Named, keep: (K, V) => Boolean, store: Option[(Store, Serde[F, V])]): KTable.Of[F, K, V, Option] =
    KTable.of[F, K, V, Option](
      stream.kept(named, Step.TableFilter[F, K, Change[V], V](keep, kind.present(_), store.map((stored, serde) => (stored, key, serde)))),
      key,
      store.map(_._2).orElse(values),
      grace,
      store.fold(view.map(TableView.Filtered(_, keep)))((stored, serde) => view.map(parent => TableView.Stored(stored, parent.keys, serde))),
      ChangeKind.Deleting
    )

  /** The table's values changed by `change`, as Java's table `mapValues` makes them. */
  private def mapped[V2](named: Named, change: V => V2, store: Option[(Store, Serde[F, V2])]): KTable.Of[F, K, V2, Change] =
    KTable.of[F, K, V2, Change](
      stream.kept(
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
  def filter(named: Named)(keep: (K, V) => Boolean): KTable.Of[F, K, V, Option] = filtered(named, keep, None)

  /** As `filter`, the result kept in `store`, its values written with `serde`, as Java's filter given a `Materialized` store. */
  def filter(named: Named, store: Store, serde: Serde[F, V])(keep: (K, V) => Boolean): KTable.Of[F, K, V, Option] =
    filtered(named, keep, Some((store, serde)))

  /** The table without the values `drop` gives true for, as Java's table `filterNot` makes it. */
  def filterNot(named: Named)(drop: (K, V) => Boolean): KTable.Of[F, K, V, Option] = filtered(named, (key, value) => !drop(key, value), None)

  /** As `filterNot`, the result kept in `store`, its values written with `serde`. */
  def filterNot(named: Named, store: Store, serde: Serde[F, V])(drop: (K, V) => Boolean): KTable.Of[F, K, V, Option] =
    filtered(named, (key, value) => !drop(key, value), Some((store, serde)))

  /** The table with each value changed by `change`, as Java's table `mapValues` makes it, kept in no store, its values read through this table's. A
    * suppression of it needs a serde for its values, given with the other `mapValues`.
    */
  def mapValues[V2](named: Named)(change: V => V2): KTable.Of[F, K, V2, Change] = mapped(named, change, None)

  /** As `mapValues`, the result kept in `store`, its values written with `serde`, as Java's `mapValues` given a `Materialized` store. */
  def mapValues[V2](named: Named, store: Store, serde: Serde[F, V2])(change: V => V2): KTable.Of[F, K, V2, Change] =
    mapped(named, change, Some((store, serde)))

  /** The table as one side of a join. */
  private def side: Side[F, K, V, Change[V]] = Side(kind.present(_), view)

  /** The topics the table reads, through everything that makes it. */
  private def read: Set[Source] = stream.feed(Consumer.Done()).reads

  /** The table joined with `other` by `joiner`, as Java's table-table joins join them: in one sub-topology, their topics copartitioned, and each
    * change to either joined with the other's value for its key. The result is kept in the store `store` names where one is given.
    */
  private def joinedWith[VO, R](
      named: Named,
      other: KTable[F, K, VO],
      joiner: Joiner[V, VO, R],
      store: Option[(Store, Serde[F, R])]
  ): KTable.Of[F, K, R, Option] =
    val problems = stream.problems ++ other.stream.problems
    (view, other.view) match
      case (Some(mine), Some(their)) if (read & other.read).isEmpty =>
        val feeding: Consumer[F, K, Option[R]] => Pipeline[F] =
          next =>
            val keys = store.map((stored, serde) => (stored, mine.keys, serde))
            val join = TablesJoined(named, side, other.side, joiner, keys, next)
            stream.feed(Consumer.LeftTable(join)) ++ other.stream.feed(Consumer.RightTable(join))
        val values =
          store.fold[TableView[F, K, R]](TableView.Joined(mine, their, joiner))((stored, serde) => TableView.Stored(stored, mine.keys, serde))
        KTable.of[F, K, R, Option](KStream.of(feeding, named.value, false, problems), key, store.map(_._2), None, Some(values), ChangeKind.Deleting)
      case _ =>
        val refused = if view.isEmpty || other.view.isEmpty then TopologyError.WindowedJoin else TopologyError.JoinedWithItself
        // The join is refused, so the step that stands in for it never runs.
        val nothing = stream.kept(named, Step.FlatMap[F, K, Change[V], K, Option[R]]((_, _) => Nil))
        KTable.of[F, K, R, Option](KStream.of(nothing.feed, nothing.lastNode, false, problems :+ refused), key, None, None, None, ChangeKind.Deleting)

  /** The table joined with `other`, as Java's `join` of two tables: each key both hold a value for has what `join` makes of the two, and a change to
    * either side changes it. A delete on either side deletes the key from the result, as Java passes on a delete even for a key the result never
    * held. The result is kept in no store; the result of a join that is suppressed needs one, given with the other `join`.
    */
  def join[VO, R](named: Named, other: KTable[F, K, VO])(join: (V, VO) => R): KTable.Of[F, K, R, Option] =
    joinedWith(named, other, Joiner.Inner(join), None)

  /** As `join`, the result kept in `store`, its values written with `serde`, as Java's join given a `Materialized` store. */
  def join[VO, R](named: Named, other: KTable[F, K, VO], store: Store, serde: Serde[F, R])(join: (V, VO) => R): KTable.Of[F, K, R, Option] =
    joinedWith(named, other, Joiner.Inner(join), Some((store, serde)))

  /** The table joined with `other`, as Java's `leftJoin` of two tables: each key this table holds a value for has what `join` makes of it and
    * `other`'s value for the key, if any.
    */
  def leftJoin[VO, R](named: Named, other: KTable[F, K, VO])(join: (V, Option[VO]) => R): KTable.Of[F, K, R, Option] =
    joinedWith(named, other, Joiner.Left(join), None)

  /** As `leftJoin`, the result kept in `store`, its values written with `serde`. */
  def leftJoin[VO, R](named: Named, other: KTable[F, K, VO], store: Store, serde: Serde[F, R])(
      join: (V, Option[VO]) => R
  ): KTable.Of[F, K, R, Option] = joinedWith(named, other, Joiner.Left(join), Some((store, serde)))

  /** The table joined with `other`, as Java's `outerJoin` of two tables: each key either table holds a value for has what `join` makes of this
    * table's value, `other`'s, or both.
    */
  def outerJoin[VO, R](named: Named, other: KTable[F, K, VO])(join: Ior[V, VO] => R): KTable.Of[F, K, R, Option] =
    joinedWith(named, other, Joiner.Outer(join), None)

  /** As `outerJoin`, the result kept in `store`, its values written with `serde`. */
  def outerJoin[VO, R](named: Named, other: KTable[F, K, VO], store: Store, serde: Serde[F, R])(join: Ior[V, VO] => R): KTable.Of[F, K, R, Option] =
    joinedWith(named, other, Joiner.Outer(join), Some((store, serde)))

object KTable:
  /** A change to a table that only ever updates its keys: the new value. */
  type Updated[A] = A

  /** A table whose [[KTable.Change]] is `C`, for annotating one without losing it. */
  type Of[F[_], K, V, C[_]] = KTable[F, K, V] { type Change[A] = C[A] }

  /** The records of `topic` as a table, kept in `store`, as Java's `StreamsBuilder.table` reads it: each record's value replaces its key's, and a
    * record without a value deletes the key. A record without a key is skipped. Java names the table's source `<named>-source` and the step that
    * keeps it `named`, as its `Consumed` name names both.
    */
  def apply[F[_], K, V](named: Named, topic: Topic, key: Serde[F, K], value: Serde[F, V], store: Store)(using
      F: cats.Applicative[F]
  ): Of[F, K, V, Option] =
    deleting(
      KStream(Named(s"${named.value}-source"), topic, key.option, value.option).kept(named, Step.TableSource(store, key, value)),
      TableKey.Plain(key),
      value,
      None,
      Some(TableView.Stored(store, key, value))
    )

  private[xkafka] def updating[F[_], K, V](
      stream: KStream[F, K, V],
      key: TableKey[F, K],
      value: Serde[F, V],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): Of[F, K, V, Updated] = of[F, K, V, Updated](stream, key, Some(value), grace, view, ChangeKind.Updating)

  private[xkafka] def deleting[F[_], K, V](
      stream: KStream[F, K, Option[V]],
      key: TableKey[F, K],
      value: Serde[F, V],
      grace: Option[Long],
      view: Option[TableView[F, K, V]] = None
  ): Of[F, K, V, Option] = of[F, K, V, Option](stream, key, Some(value), grace, view, ChangeKind.Deleting)

  private[xkafka] def of[F[_], K, V, C[_]](
      stream: KStream[F, K, C[V]],
      key: TableKey[F, K],
      values: Option[Serde[F, V]],
      grace: Option[Long],
      view: Option[TableView[F, K, V]],
      kind: ChangeKind[C]
  ): Of[F, K, V, C] = Built(stream, key, values, grace, view, kind)

  private final class Built[F[_], K, V, C[_]](
      private[xkafka] val stream: KStream[F, K, C[V]],
      private[xkafka] val key: TableKey[F, K],
      private[xkafka] val values: Option[Serde[F, V]],
      private[xkafka] val grace: Option[Long],
      private[xkafka] val view: Option[TableView[F, K, V]],
      private[xkafka] val kind: ChangeKind[C]
  ) extends KTable[F, K, V]:
    type Change[A] = C[A]

/** A pipeline from an input topic to an output topic. */
final class Pipeline[F[_]] private[xkafka] (private[xkafka] val entries: List[Entry[F]], private[xkafka] val problems: Vector[TopologyError]):
  def mapK[G[_]](fk: FunctionK[F, G]): Pipeline[G] = new Pipeline(entries.map(_.mapK(fk)), problems)

  /** This pipeline's records and those of `other`, which reads the same join. */
  private[xkafka] def ++(other: Pipeline[F]): Pipeline[F] = new Pipeline(entries ++ other.entries, problems ++ other.problems)

  /** The topics it reads. */
  private[xkafka] def reads: Set[Source] = entries.flatMap(_.reads).toSet

/** A streams program: its sub-topologies, numbered as a Java program building the same topology numbers them. */
final class Topology[F[_]] private (private[xkafka] val subtopologies: List[Subtopology[F]]):
  /** The same program with its effects in `G`, as its serdes and effectful steps are mapped. */
  def mapK[G[_]](fk: FunctionK[F, G]): Topology[G] = new Topology(subtopologies.map(_.mapK(fk)))

  /** What the program runs: its topics, steps, stores and repartitions, which `toString` writes as Java Kafka Streams' `Topology.describe` does. */
  def describe: TopologyDescription = Describing(this, repartition => s"${repartition.value}-repartition")

object Topology:
  /** The longest store or repartition name, as Java allows. */
  val MaxNameLength = 249

  def of[F[_]](first: Pipeline[F], rest: Pipeline[F]*): Either[NonEmptyList[TopologyError], Topology[F]] =
    val pipelines                 = first :: rest.toList
    val subtopologies             = Wiring(pipelines)
    val entries                   = subtopologies.flatMap(_.entries)
    val stores                    = subtopologies.flatMap(_.stores).map(_.value)
    val steps                     = entries.flatMap(entry => entry.origin.names ++ entry.consumer.names).map(_.value)
    val repartitions              = entries.map(_.origin.source).collect { case Source.Repartitioned(named) if named.value.nonEmpty => named.value }
    val inputs                    = entries.map(_.origin.source).collect { case Source.Input(topic) => topic }
    def twice[A](values: List[A]) = values.groupBy(identity).collect { case (value, all) if all.size > 1 => value }.toList
    val problems                  =
      pipelines.flatMap(_.problems) ++ (stores ++ repartitions ++ steps).flatMap(invalid) ++
        twice(steps).sorted.map(TopologyError.DuplicateName(_)) ++ twice(stores).sorted.map(TopologyError.DuplicateStore(_)) ++ twice(repartitions)
          .sorted.map(TopologyError.DuplicateRepartition(_)) ++ twice(inputs).sortBy(_.value).map(TopologyError.DuplicateSource(_))
    NonEmptyList.fromList(problems.distinct).toLeft(new Topology(subtopologies))

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
