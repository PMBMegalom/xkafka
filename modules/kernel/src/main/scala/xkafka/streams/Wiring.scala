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

/** Walks a program's graph from its topics, finding its sub-topologies and numbering them as Java numbers them: by the first node of each that it
  * writes, which it writes in about the order the program makes them, the receiver of a join first. A join's sub-topology takes its place from the
  * receiver where the join reads it as it is, since it holds the receiver's source, and otherwise from the other side; a side the join repartitions
  * comes before it, since Java makes that repartition topic's source only when it makes the join.
  */
private[xkafka] object Wiring:
  /** A sub-topology as far as the walk has followed it: the topics it reads, its own and those of the tables beside it; the topics each of its joins
    * copartitions; the topics a join copartitions for these records, which Java's streams and tables pass on from step to step, the two sides'
    * together after a join, and none after a suppression, which Java takes for their source; whether sub-topologies are numbered before it and after
    * it; the joins whose numbering depends on how Java's program builds them; and the topics it reads whose sources Java has not made yet.
    */
  private final case class Reading[F[_]](
      own: Vector[Entry[F]],
      beside: Vector[Entry[F]],
      copartitioned: List[List[Source]],
      sources: List[Source],
      before: Boolean,
      after: Boolean,
      buildOrdered: List[List[Source]],
      unwritten: List[Source]
  ):
    def origins: List[Source] = (own ++ beside).map(_.origin.source).toList

    /** It after `step`. */
    def through[K, V, K2, V2](step: Step[F, K, V, K2, V2]): Reading[F] =
      step match
        case Step.Suppress(_, _, _, _) => copy(sources = Nil)
        case _                         => this

    def entries: Vector[Entry[F]] = own ++ beside

    /** It and `other` as the one sub-topology a join makes of them, keeping `beside` beside it. Whichever comes first in the program is taken to be
      * built first; the join says so where building the other first would number them otherwise.
      */
    def joined(other: Reading[F], own: Vector[Entry[F]], beside: Vector[Entry[F]]): Reading[F] =
      val all    = (sources ++ other.sources).distinct
      val joined = copartitioned ++ other.copartitioned :+ all
      val unsure = before || other.before || (after && other.after)
      Reading(
        own,
        beside,
        joined,
        all,
        before,
        after || other.before || other.after,
        (buildOrdered ++ other.buildOrdered ++ Option.when(unsure)((joined.flatten).distinct)).distinct,
        Nil
      )

  private object Reading:
    def of[F[_]](entry: Entry[F]): Reading[F] = Reading(Vector(entry), Vector.empty, Nil, List(entry.origin.source), false, false, Nil, Nil)

  /** How far the walk has got: the topics whose sources it has written, in order; each join with the side that reached it first; the sub-topologies
    * it has finished; and what reached the end of a table it is reading.
    */
  private final case class Walk[F[_]](
      written: Vector[Source],
      waiting: Map[AnyRef, Reading[F]],
      closed: Vector[Reading[F]],
      ended: Option[Reading[F]]
  ):
    def writing(sources: List[Source]): Walk[F] = copy(written = written ++ sources)

    def closing(reading: Reading[F]): Walk[F] = copy(closed = closed :+ reading)

  /** The sub-topologies of `pipelines`, in the order Java numbers them. */
  def apply[F[_]](pipelines: List[Pipeline[F]]): List[Subtopology[F]] =
    val walked =
      pipelines.flatMap(_.entries).foldLeft(Walk[F](Vector.empty, Map.empty, Vector.empty, None)): (walk, entry) =>
        val after = entered(entry, walk)
        after.ended.fold(after)(ended => after.closing(ended).copy(ended = None))
    walked.closed.sortBy(_.origins.map(walked.written.indexOf(_)).min).toList
      .map(reading => Subtopology(reading.entries.toList, groups(reading.copartitioned), reading.buildOrdered))

  /** The copartition groups each join's topics make, as Java makes them: a join's topics go in the group of the first it finds already in one, and
    * all of them then belong to that group; a group that has lost all its topics to another is gone.
    */
  private def groups(joins: List[List[Source]]): List[List[Source]] =
    val (all, owners) =
      joins.foldLeft((Vector.empty[List[Source]], Map.empty[Source, Int])):
        case ((all, owners), topics) =>
          val at = topics.flatMap(owners.get).headOption.getOrElse(all.size)
          (if at == all.size then all :+ topics else all.updated(at, (all(at) ++ topics).distinct), owners ++ topics.map(_ -> at))
    all.zipWithIndex.collect { case (group, at) if owners.values.exists(_ == at) => group }.toList

  private def entered[F[_]](entry: Entry[F], walk: Walk[F]): Walk[F] =
    followed(entry.consumer, Reading.of(entry), walk.writing(List(entry.origin.source)))

  private def followed[F[_], K, V](consumer: Consumer[F, K, V], at: Reading[F], walk: Walk[F]): Walk[F] =
    consumer match
      case Consumer.Then(_, step, next)                                   => followed(next, at.through(step), walk)
      case Consumer.Output(_, _, _, _)                                    => walk.closing(at)
      case Consumer.Done()                                                => walk.copy(ended = Some(at))
      case Consumer.Repartitioning(repartition, key, value, atJoin, next) =>
        val read   = Reading.of(Entry(Origin.Repartitioned(repartition, key, value), next)).copy(before = true)
        val source = List[Source](Source.Repartitioned(repartition))
        if atJoin then followed(next, read.copy(unwritten = source), walk.closing(at)) else followed(next, read, walk.closing(at).writing(source))
      case Consumer.Beside(table, next) =>
        val pulled = table.entries.foldLeft(walk)((walking, entry) => entered(entry, walking))
        pulled.ended.fold(followed(next, at, pulled)): rows =>
          followed(next, at.joined(rows, at.own, at.beside ++ rows.entries), pulled.copy(ended = None).writing(at.unwritten))
      case Consumer.LeftTable(join)   => reached(join, true, at, walk)(followed(join.next, _, _))
      case Consumer.RightTable(join)  => reached(join, false, at, walk)(followed(join.next, _, _))
      case Consumer.LeftStream(join)  => reached(join, true, at, walk)(followed(join.next, _, _))
      case Consumer.RightStream(join) => reached(join, false, at, walk)(followed(join.next, _, _))

  /** A side reaching `join`, its left where `left` says: it waits if the other has not reached it yet, and otherwise the join makes one sub-topology
    * of the two, which `next` goes on from, the sources of both that were waiting for the join now written.
    */
  private def reached[F[_]](join: AnyRef, left: Boolean, at: Reading[F], walk: Walk[F])(next: (Reading[F], Walk[F]) => Walk[F]): Walk[F] =
    walk.waiting.get(join) match
      case None        => walk.copy(waiting = walk.waiting.updated(join, at))
      case Some(other) =>
        val (mine, theirs) = if left then (at, other) else (other, at)
        next(
          mine.joined(theirs, mine.own ++ theirs.own, mine.beside ++ theirs.beside),
          walk.copy(waiting = walk.waiting - join).writing(other.unwritten ++ at.unwritten)
        )
