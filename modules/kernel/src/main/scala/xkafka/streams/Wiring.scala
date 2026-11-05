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
  * writes, which it writes in about the order the program makes them, the receiver of a join first. A node comes once the last of what reads into it
  * has come, what it sends to in the order the program gives them. A join's sub-topology takes its place from the receiver where the join reads it as
  * it is, since it holds the receiver's source, and otherwise from the other side; a side the join repartitions comes before it, since Java makes
  * that repartition topic's source only when it makes the join. A table a join reads is made at the join, if nothing else in the program reads its
  * topic.
  */
private[xkafka] object Wiring:
  /** A program as the walk found it: its sub-topologies in the order Java numbers them, what is wrong with it, its steps' names, and each topic read.
    */
  final case class Wired[F[_]](subtopologies: List[Subtopology[F]], problems: List[TopologyError], steps: List[String], inputs: List[Topic])

  /** Where a node's records have come from, as far as the walk has followed them: a topic of its sub-topology; the node they came out of; the topics
    * a join copartitions for these records, which Java's streams and tables pass on from step to step, the sides' or parents' together after a join
    * or a merge, and none after a suppression, which Java takes for their source; every topic joined for them so far; whether sub-topologies are
    * numbered before and after theirs; whether any of them may have a new key; the topics they read whose sources Java has not made yet; and whether
    * they run beside what reads a table rather than as a program's own.
    */
  private final case class Path(
      home: Source,
      last: List[String],
      sources: List[Source],
      joined: List[Source],
      before: Boolean,
      after: Boolean,
      rekeyed: Boolean,
      unwritten: List[Source],
      pulled: Boolean
  ):
    /** It after `step`. */
    def through[F[_], K, V, K2, V2](step: Step[F, K, V, K2, V2]): Path =
      step match
        case Step.Suppress(_, _, _, _) => copy(sources = Nil, rekeyed = rekeyed || step.rekeys)
        case _                         => copy(rekeyed = rekeyed || step.rekeys)

    /** The topics of it and `other` together. */
    def topics(other: Path): List[Source] = (sources ++ other.sources).distinct

    /** It and `other` as what comes out of the node where they meet. Whichever comes first in the program is taken to be built first. */
    def met(other: Path): Path =
      val all = topics(other)
      Path(
        home,
        (last ++ other.last).distinct,
        all,
        (joined ++ other.joined ++ all).distinct,
        before,
        after || other.before || other.after,
        rekeyed || other.rekeyed,
        unwritten ++ other.unwritten,
        pulled && other.pulled
      )

    /** Whether Java could number their sub-topologies otherwise, by building the other first. */
    def unsure(other: Path): Boolean = before || other.before || (after && other.after)

  private object Path:
    def of(sources: List[Source], node: String, pulled: Boolean): Path =
      Path(sources.head, List(node), sources, Nil, false, false, false, Nil, pulled)

  /** The stores and topics of the program's own pipelines. */
  private final case class Own(stores: Set[String], sources: Set[Source])

  /** What the graph holds before the walk: how many edges reach each node, the joins seen, the stores a program's own topics keep, and what its table
    * joins bring.
    */
  private final case class Graph[F[_]](degrees: Map[AnyRef, Int], joins: Set[AnyRef], stores: Set[String], tables: Vector[Pipeline[F]]):
    def keeping(kept: List[KeptStore]): Graph[F] = copy(stores = stores ++ kept.map(_.store.value))

  private def linked[F[_], K, V](consumer: Consumer[F, K, V], graph: Graph[F]): Graph[F] =
    val seen  = graph.degrees.contains(consumer)
    val added = graph.copy(degrees = graph.degrees.updated(consumer, graph.degrees.getOrElse(consumer, 0) + 1))
    def onward[K2, V2](id: AnyRef, kept: List[KeptStore], next: Consumer[F, K2, V2]): Graph[F] =
      if added.joins(id) then added else linked(next, added.copy(joins = added.joins + id).keeping(kept))
    if seen then added
    else
      consumer.expanded match
        case Consumer.Then(_, step, next)                    => linked(next, added.keeping(step.kept))
        case Consumer.Repartitioning(_, _, _, _, _, next, _) => linked(next, added)
        case Consumer.Beside(table, _, next)                 => linked(next, added.copy(tables = added.tables ++ table))
        case Consumer.Fan(children)                          => children.foldLeft(added)((graph, child) => linked(child, graph))
        case Consumer.Merged(_, next)                        => linked(next, added)
        case Consumer.Splitting(_, branches)                 => branches.foldLeft(added)((graph, branch) => linked(branch.next, graph))
        case Consumer.Refused(_, next)                       => linked(next, added)
        case Consumer.LeftTable(join)                        => onward(join.id, join.kept, join.next)
        case Consumer.RightTable(join)                       => onward(join.id, join.kept, join.next)
        case Consumer.LeftStream(join)                       => onward(join.id, join.kept, join.next)
        case Consumer.RightStream(join)                      => onward(join.id, join.kept, join.next)
        case _                                               => added

  /** Whether `entry` only reads into a store the program's own topics keep from the same topic, as the table a join reads does where the program also
    * uses that table.
    */
  private def onlyKeeps[F[_]](entry: Entry[F], own: Own): Boolean =
    entry.consumer.expanded match
      case Consumer.Then(_, Step.TableSource(store, _, _), next) => own.stores(store.value) && entry.origin.sources.forall(own.sources) &&
        (next.expanded match
          case Consumer.Done() => true
          case _               => false)
      case _ => false

  /** The graph with the entries the tables it brings read, and what those bring. */
  private def pulled[F[_]](graph: Graph[F], own: Own, reached: Set[(List[Source], AnyRef)]): Graph[F] =
    graph.tables.headOption match
      case None        => graph
      case Some(table) =>
        val wanted = table.entries.filterNot(entry => reached(identified(entry)) || onlyKeeps(entry, own))
        pulled(
          wanted.foldLeft(graph.copy(tables = graph.tables.tail))((linking, entry) => linked(entry.consumer, linking)),
          own,
          reached ++ wanted.map(identified)
        )

  /** What makes an entry the one it is: several topics can go into one node, and one table is brought by each join that reads it. */
  private def identified[F[_]](entry: Entry[F]): (List[Source], AnyRef) = (entry.origin.sources, entry.consumer)

  /** How far the walk has got: the topics whose sources it has written, in order; what has reached each node with several parents that has yet to
    * come; each join with the side that reached it first; where each named node's records have come from, and what is waiting for a node to come;
    * which topics share a sub-topology; the entries it has entered, and all each sub-topology is found to hold.
    */
  private final case class Walk[F[_]](
      degrees: Map[AnyRef, Int],
      own: Own,
      written: Vector[Source],
      arrived: Map[AnyRef, Vector[Path]],
      sides: Map[AnyRef, Path],
      came: Map[String, Path],
      readers: Map[String, Vector[(Path, Walk[F]) => Walk[F]]],
      shared: Map[Source, Source],
      entered: Set[(List[Source], AnyRef)],
      found: Vector[(Entry[F], Boolean)],
      nodes: Vector[(Source, Describing.Made)],
      kept: Vector[(Source, KeptStore)],
      repartitions: Vector[(Source, Repartition)],
      joins: Vector[List[Source]],
      unsure: Vector[List[Source]],
      problems: Vector[TopologyError],
      steps: Vector[String]
  ):
    def writing(sources: List[Source]): Walk[F] = copy(written = written ++ sources)

    /** The topic standing for the sub-topology of `source`. */
    @scala.annotation.tailrec
    def homeOf(source: Source): Source =
      shared.get(source) match
        case Some(other) => homeOf(other)
        case None        => source

    def united(first: Source, second: Source): Walk[F] =
      val (one, other) = (homeOf(first), homeOf(second))
      if one == other then this else copy(shared = shared.updated(other, one))

    /** The node made, in the step `named` if the program names one. */
    def making(at: Path, node: Describing.Made, named: Boolean = true): Walk[F] =
      copy(nodes = nodes :+ (at.home, node), steps = if named then steps :+ node.name else steps)

    def keeping(at: Path, stores: List[KeptStore]): Walk[F] = copy(kept = kept ++ stores.map(at.home -> _))

    /** `first` and `second` meeting in one node, noting where that decides which Java numbers first. */
    def meeting(first: Path, second: Path): (Path, Walk[F]) =
      val all   = first.topics(second)
      val noted =
        if homeOf(first.home) != homeOf(second.home) && first.unsure(second) && (first.joined ++ second.joined ++ all).nonEmpty then
          copy(unsure = unsure :+ (first.joined ++ second.joined ++ all).distinct)
        else this
      (first.met(second), noted.united(first.home, second.home))

    /** `first` and `second` joined, which is one node to them, whose topics Java copartitions. */
    def joining(first: Path, second: Path): (Path, Walk[F]) =
      val (met, noted) = meeting(first, second)
      (met.copy(unwritten = Nil, rekeyed = false), noted.copy(joins = noted.joins :+ met.sources).writing(met.unwritten))

    /** The node `name` having come, with the paths its records take, and all that waited for it going on. */
    def coming(name: String, at: Path): Walk[F] =
      val after = copy(came = came.updated(name, at), readers = readers - name)
      readers.getOrElse(name, Vector.empty).foldLeft(after)((walk, reader) => reader(at, walk))

  /** The sub-topologies of `pipelines`, in the order Java numbers them. */
  def apply[F[_]](pipelines: List[Pipeline[F]]): Wired[F] =
    val own       = pipelines.flatMap(_.entries)
    val distinct  = own.distinctBy(identified)
    val first     = distinct.foldLeft(Graph[F](Map.empty, Set.empty, Set.empty, Vector.empty))((graph, entry) => linked(entry.consumer, graph))
    val ownership = Own(first.stores, distinct.flatMap(_.origin.sources).toSet)
    val graph     = pulled(first, ownership, distinct.map(identified).toSet)
    val start     =
      Walk[F](
        graph.degrees,
        ownership,
        Vector.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Set.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty
      )
    val walked                                          = own.foldLeft(start)((walk, entry) => entered(entry, false, walk))
    val unfed                                           = walked.readers.keys.toList.sorted.map(TopologyError.UnfedTable(_))
    def of[A](found: Vector[(Source, A)], home: Source) = found.collect { case (source, value) if walked.homeOf(source) == home => value }.toList
    val inputs = (own ++ walked.found.map(_._1).filterNot(entry => own.exists(identified(_) == identified(entry)))).flatMap(_.origin.sources)
      .collect { case Source.Input(topic) => topic }
    val subtopologies =
      walked.found.flatMap(_._1.origin.sources).map(walked.homeOf).distinct
        .sortBy(home => walked.found.flatMap(_._1.origin.sources).filter(walked.homeOf(_) == home).map(walked.written.indexOf(_)).min).toList.map:
          home =>
            val (beside, entries) = walked.found.filter((entry, _) => walked.homeOf(entry.origin.sources.head) == home).toList.partition(_._2)
            val inside            = (topics: List[Source]) => topics.headOption.exists(walked.homeOf(_) == home)
            Subtopology(
              (entries ++ beside).map(_._1),
              groups(walked.joins.toList.filter(inside)),
              walked.unsure.toList.filter(inside).distinct,
              of(walked.kept, home),
              of(walked.repartitions, home),
              of(walked.nodes, home)
            )
    Wired(subtopologies, walked.problems.toList ++ unfed, walked.steps.toList, inputs)

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

  private def entered[F[_]](entry: Entry[F], pulled: Boolean, walk: Walk[F]): Walk[F] =
    if walk.entered(identified(entry)) || (pulled && onlyKeeps(entry, walk.own)) then walk
    else
      val sources      = entry.origin.sources
      val (node, name) = Describing.source(entry.origin)
      val noted        = walk.copy(entered = walk.entered + identified(entry), found = walk.found :+ (entry, pulled)).writing(sources)
      val at           = Path.of(sources, name, pulled)
      val together     = sources.tail.foldLeft(noted)(_.united(sources.head, _))
      val named        = together.copy(nodes = together.nodes :+ (sources.head, node), steps = together.steps ++ entry.origin.names.map(_.value))
      arrive(entry.consumer, at, named.coming(name, at))

  /** One edge reaching `consumer`, which comes once all have. */
  private def arrive[F[_], K, V](consumer: Consumer[F, K, V], at: Path, walk: Walk[F]): Walk[F] =
    val arrived = walk.arrived.getOrElse(consumer, Vector.empty) :+ at
    if arrived.size < walk.degrees.getOrElse(consumer, 1) then walk.copy(arrived = walk.arrived.updated(consumer, arrived))
    else
      val (together, met) =
        arrived.tail.foldLeft((arrived.head, walk.copy(arrived = walk.arrived - consumer))):
          case ((first, walking), second) => walking.meeting(first, second)
      followed(consumer.expanded, together, met)

  private def followed[F[_], K, V](consumer: Consumer[F, K, V], at: Path, walk: Walk[F]): Walk[F] =
    consumer match
      case Consumer.Then(named, step, next) =>
        val after = at.through(step).copy(last = List(named.value))
        val made  = walk.making(at, Describing.Made(named.value, Describing.Kind.Processor(Describing.kept(step)), at.last)).keeping(at, step.kept)
        arrive(next, after, made.coming(named.value, after))
      case Consumer.Output(named, topic, _, _) => walk.making(at, Describing.Made(named.value, Describing.Kind.Writes(Source.Input(topic)), at.last))
      case Consumer.Done()                     => walk
      case Consumer.Repartitioning(repartition, key, value, atJoin, always, next, aggregate) =>
        val read         = Entry(Origin.Repartitioned(repartition, key, value, aggregate), next)
        val source       = Source.Repartitioned(repartition)
        val (node, name) = Describing.source(read.origin)
        val filter       = s"${repartition.value}-repartition-filter"
        val sink         = s"${repartition.value}-repartition-sink"
        val written      =
          aggregate match
            case Some(named) => walk.making(at, Describing.Made(s"${named.value}-sink", Describing.Kind.Writes(source), at.last))
            case None        => walk.making(at, Describing.Made(filter, Describing.Kind.Processor(Nil), at.last), named = false)
                .making(at, Describing.Made(sink, Describing.Kind.Writes(source), List(filter)), named = false)
        val closed =
          written.copy(
            repartitions = walk.repartitions :+ (at.home, repartition),
            found = walk.found :+ (read, at.pulled),
            entered = walk.entered + identified(read),
            steps = written.steps ++ read.origin.names.map(_.value)
          )
        val after   = Path.of(List(source), name, at.pulled).copy(before = true, unwritten = if atJoin then List(source) else Nil)
        val refused =
          if always || at.rekeyed then closed else closed.copy(problems = closed.problems :+ TopologyError.UnusedRepartition(repartition.value))
        val placed = (if atJoin then refused else refused.writing(List(source))).copy(nodes = refused.nodes :+ (source, node))
        arrive(next, after, placed.coming(name, after))
      case Consumer.Beside(table, last, next) =>
        val pulled = table.fold(walk)(_.entries.foldLeft(walk)((walking, entry) => entered(entry, true, walking)))
        def reading(rows: Path, reader: Walk[F]): Walk[F] =
          val (joined, met) = reader.joining(at, rows)
          arrive(next, joined.copy(last = at.last), met)
        pulled.came.get(last) match
          case Some(rows) => reading(rows, pulled)
          case None       => pulled.copy(readers = pulled.readers.updated(last, pulled.readers.getOrElse(last, Vector.empty) :+ reading))
      case Consumer.Fan(children) => children.foldLeft(walk): (walking, child) =>
          arrive(child, if walking.written.size > walk.written.size then at.copy(after = true) else at, walking)
      case Consumer.Merged(named, next) =>
        val after = at.copy(last = List(named.value))
        arrive(next, after, walk.making(at, Describing.Made(named.value, Describing.Kind.Processor(Nil), at.last)).coming(named.value, after))
      case Consumer.Splitting(named, branches) =>
        val made  = walk.making(at, Describing.Made(named.value, Describing.Kind.Processor(Nil), at.last))
        val after =
          branches.foldLeft(made): (walking, branch) =>
            val taken = walking.making(at, Describing.Made(branch.named.value, Describing.Kind.Processor(Nil), List(named.value)))
            arrive(branch.next, at.copy(last = List(branch.named.value), after = at.after || walking.written.size > walk.written.size), taken)
        after
      case Consumer.Refused(problem, next) => arrive(next, at, walk.copy(problems = walk.problems :+ problem))
      case Consumer.LeftTable(join)        => reached(join.id, true, at, walk)(tablesJoined(join, _, _, _))
      case Consumer.RightTable(join)       => reached(join.id, false, at, walk)(tablesJoined(join, _, _, _))
      case Consumer.LeftStream(join)       => reached(join.id, true, at, walk)(streamsJoined(join, _, _, _))
      case Consumer.RightStream(join)      => reached(join.id, false, at, walk)(streamsJoined(join, _, _, _))
      case mapped @ Consumer.Mapped(_, _)  => followed(mapped.expanded, at, walk)

  /** A side reaching the join `id`, its left where `left` says: it waits if the other has not reached it yet, and otherwise `next` goes on from the
    * two, left first.
    */
  private def reached[F[_]](id: AnyRef, left: Boolean, at: Path, walk: Walk[F])(next: (Path, Path, Walk[F]) => Walk[F]): Walk[F] =
    walk.sides.get(id) match
      case None        => walk.copy(sides = walk.sides.updated(id, at))
      case Some(other) =>
        val (mine, theirs) = if left then (at, other) else (other, at)
        next(mine, theirs, walk.copy(sides = walk.sides - id))

  /** Tables joined: each side joins its changes with the other side's values, read through the other side's stores, and the merge passes on what
    * either makes.
    */
  private def tablesJoined[F[_], K, A, B, R, C1, C2](join: TablesJoined[F, K, A, B, R, C1, C2], left: Path, right: Path, walk: Walk[F]): Walk[F] =
    import Describing.{Kind, Made}
    val named         = join.named.value
    val (joined, met) = walk.joining(left, right)
    val after         = joined.copy(last = List(named))
    val joinThis      = Made(s"$named-join-this", Kind.Processor(join.right.view.fold(Nil)(Describing.stores)), left.last)
    val joinOther     = Made(s"$named-join-other", Kind.Processor(join.left.view.fold(Nil)(Describing.stores)), right.last)
    val merge         = Made(named, Kind.Processor(join.store.toList.map(_._1.value)), List(joinThis.name, joinOther.name))
    val made = met.making(after, joinThis, named = false).making(after, joinOther, named = false).making(after, merge).keeping(after, join.kept)
    arrive(join.next, after, made.coming(named, after))

  /** Streams joined: each side's records are kept in its window store, then joined with the other side's, and the merge passes on what either makes.
    */
  private def streamsJoined[F[_], K, A, B, R](join: StreamsJoined[F, K, A, B, R], left: Path, right: Path, walk: Walk[F]): Walk[F] =
    import Describing.{Kind, Made}
    val named                   = join.named.value
    val held                    = StreamJoin.names(join.store, join.joiner)
    val (leftOuter, rightOuter) = join.joiner.outer
    val (joined, met)           = walk.joining(left, right)
    val after                   = joined.copy(last = List(s"$named-merge"))
    val thisWindowed            = Made(s"$named-this-windowed", Kind.Processor(List(held.left)), left.last)
    val otherWindowed           = Made(s"$named-other-windowed", Kind.Processor(List(held.right)), right.last)
    val thisJoin                =
      Made(s"$named-${if rightOuter then "outer-" else ""}this-join", Kind.Processor(held.right :: held.shared.toList), List(thisWindowed.name))
    val otherJoin =
      Made(s"$named-${if leftOuter then "outer-" else ""}other-join", Kind.Processor(held.left :: held.shared.toList), List(otherWindowed.name))
    val merge = Made(s"$named-merge", Kind.Processor(Nil), List(thisJoin.name, otherJoin.name))
    val made  =
      List(thisWindowed, otherWindowed, thisJoin, otherJoin, merge).foldLeft(met)(_.making(after, _, named = false)).copy(steps = met.steps :+ named)
        .keeping(after, join.kept)
    arrive(join.next, after, made.coming(s"$named-merge", after))
