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

import cats.{ApplicativeThrow, Show}
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

  def message: String =
    this match
      case InvalidName(name, reason)  => s"'$name' cannot name a store or repartitioning: $reason"
      case DuplicateStore(name)       => s"two stores are named '$name'"
      case DuplicateRepartition(name) => s"two repartitionings are named '$name'"
      case DuplicateSource(topic)     => s"two pipelines read topic '${topic.value}'"
      case UnnamedRepartition => "records whose key changed are grouped by key without a repartitioning name; pass a Repartition to groupByKey"

object TopologyError:
  given Show[TopologyError] = Show.show(_.message)

/** What a pipeline does to a record keyed `K` with a value `V`, giving records keyed `K2` with values `V2`. */
private[xkafka] enum Step[F[_], K, V, K2, V2]:
  case Filter[F[_], K, V](keep: (K, V) => Boolean)                       extends Step[F, K, V, K, V]
  case Map[F[_], K, V, K2, V2](change: (K, V) => (K2, V2))               extends Step[F, K, V, K2, V2]
  case MapValues[F[_], K, V, V2](change: V => V2)                        extends Step[F, K, V, K, V2]
  case FlatMap[F[_], K, V, K2, V2](change: (K, V) => Iterable[(K2, V2)]) extends Step[F, K, V, K2, V2]
  case Peek[F[_], K, V](effect: (K, V) => F[Unit])                       extends Step[F, K, V, K, V]
  case EvalMapValues[F[_], K, V, V2](change: V => F[V2])                 extends Step[F, K, V, K, V2]

  /** Folds each key's records into `store`: a key's first record becomes `start`, and each later one is added with `add`. */
  case Aggregate[F[_], K, V, A](store: Store, key: Serde[F, K], value: Serde[F, A], start: (K, V) => A, add: (K, V, A) => A)
      extends Step[F, K, V, K, A]

  /** Each change to the table, as a record. */
  case ToStream[F[_], K, V]() extends Step[F, K, V, K, V]

/** The steps between a segment's origin, whose records are keyed `K` with values `V`, and its last step, which gives `K2` and `V2`. */
private[xkafka] enum Chain[F[_], K, V, K2, V2]:
  case Start[F[_], K, V]()                                                                           extends Chain[F, K, V, K, V]
  case Then[F[_], K, V, K1, V1, K2, V2](init: Chain[F, K, V, K1, V1], last: Step[F, K1, V1, K2, V2]) extends Chain[F, K, V, K2, V2]

  def andThen[K3, V3](step: Step[F, K2, V2, K3, V3]): Chain[F, K, V, K3, V3] = Then(this, step)

  /** The stores of its steps, in the order the steps come. */
  def stores: List[Store] =
    this match
      case Start()          => Nil
      case Then(init, last) => init.stores ++
          (last match
            case Step.Aggregate(store, _, _, _, _) => List(store)
            case _                                 => Nil)

/** Where a segment reads from: an input topic, or the repartition topic the segment before it writes. */
private[xkafka] enum Origin[F[_], K, V]:
  case Input(topic: Topic, key: Serde[F, K], value: Serde[F, V])
  case Repartitioned(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])

/** Where a segment writes to: the repartition topic the next segment reads, or the pipeline's output topic. */
private[xkafka] enum End[F[_], K, V]:
  case Repartitioning(repartition: Repartition, key: Serde[F, K], value: Serde[F, V])
  case Output(topic: Topic, key: Serde[F, K], value: Serde[F, V])

/** The records between two topics, which become one sub-topology. */
private[xkafka] final case class Segment[F[_], K, V, K2, V2](origin: Origin[F, K, V], chain: Chain[F, K, V, K2, V2], end: End[F, K2, V2])

/** A segment still being added to. */
private[xkafka] final case class Open[F[_], K, V, K2, V2](origin: Origin[F, K, V], chain: Chain[F, K, V, K2, V2]):
  def andThen[K3, V3](step: Step[F, K2, V2, K3, V3]): Open[F, K, V, K3, V3] = Open(origin, chain.andThen(step))
  def close(end: End[F, K2, V2]): Segment[F, K, V, K2, V2]                  = Segment(origin, chain, end)

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

  /** Groups records by a new key, through the repartition topic `repartition` names. */
  def groupBy[K2](repartition: Repartition, key: Serde[F, K2], value: Serde[F, V])(change: (K, V) => K2): KGroupedStream[F, K2, V] =
    KGroupedStream(selectKey(change), Some(repartition), key, value)

  /** Groups records by their key, as long as it has not changed since they were read. */
  def groupByKey(key: Serde[F, K], value: Serde[F, V]): KGroupedStream[F, K, V] = KGroupedStream(this, None, key, value)

  /** Groups records by their key, through the repartition topic `repartition` names if the key has changed since they were read. */
  def groupByKey(repartition: Repartition, key: Serde[F, K], value: Serde[F, V]): KGroupedStream[F, K, V] =
    KGroupedStream(this, Some(repartition), key, value)

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
  private def aggregated[A](store: Store, serde: Serde[F, A], start: (K, V) => A, add: (K, V, A) => A): KTable[F, K, A] =
    val fold = Step.Aggregate(store, key, serde, start, add)
    if !stream.keyChanged then KTable(stream.step(fold, changesKey = false))
    else
      // The records go through a repartition topic first, so the store is in the segment that reads it.
      val named    = repartition.getOrElse(Repartition(""))
      val problems = if repartition.isEmpty then stream.problems :+ TopologyError.UnnamedRepartition else stream.problems
      val closed   = stream.closed :+ stream.open.close(End.Repartitioning(named, key, value))
      KTable(new KStream(closed, Open(Origin.Repartitioned(named, key, value), Chain.Start[F, K, V]().andThen(fold)), false, problems))

  /** Counts each key's records in `store`. */
  def count(store: Store)(using F: ApplicativeThrow[F]): KTable[F, K, Long] = aggregated(store, Serde.long[F], (_, _) => 1L, (_, _, n) => n + 1)

  /** Combines each key's values in `store`, starting from its first. */
  def reduce(store: Store)(combine: (V, V) => V): KTable[F, K, V] = aggregated(store, value, (_, v) => v, (_, v, acc) => combine(acc, v))

  /** Folds each key's values into `store`, starting from `initial`. */
  def aggregate[A](store: Store, serde: Serde[F, A])(initial: A)(add: (K, V, A) => A): KTable[F, K, A] =
    aggregated(store, serde, (k, v) => add(k, v, initial), add)

/** The latest value of each key, kept in a store. */
final class KTable[F[_], K, V] private[xkafka] (stream: KStream[F, K, V]):
  /** Each change to the table, as a record, keyed as the table is. */
  def toStream: KStream[F, K, V] = new KStream(stream.closed, stream.open.andThen(Step.ToStream()), false, stream.problems)

/** A pipeline from an input topic to an output topic. */
final class Pipeline[F[_]] private[xkafka] (
    private[xkafka] val segments: NonEmptyList[Segment[F, ?, ?, ?, ?]],
    private[xkafka] val problems: Vector[TopologyError]
)

/** A streams program: its pipelines, in the order a Java program building the same topology makes them, which numbers their sub-topologies. */
final class Topology[F[_]] private (private[xkafka] val pipelines: NonEmptyList[Pipeline[F]])

object Topology:
  /** The longest store or repartition name, as Java allows. */
  val MaxNameLength = 249

  def of[F[_]](first: Pipeline[F], rest: Pipeline[F]*): Either[NonEmptyList[TopologyError], Topology[F]] =
    val pipelines                 = NonEmptyList(first, rest.toList)
    val segments                  = pipelines.toList.flatMap(_.segments.toList)
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
