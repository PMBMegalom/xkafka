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

import scala.annotation.tailrec

/** How Java's topology builder makes a program's nodes, and how its `describe` lists them. */
private[xkafka] object Describing:
  /** What a node is, as Java's builder makes it. */
  enum Kind:
    /** Reads `topic`. */
    case Reads(topic: Source)

    /** Keeps or reads `stores`, in the order Java connects them. */
    case Processor(stores: List[String])

    /** Writes `topic`. */
    case Writes(topic: Source)

  /** A node as Java's builder makes it: its name, what it is, and the nodes it reads from, in the order the builder is given them. */
  final case class Made(name: String, kind: Kind, predecessors: List[String])

  /** The program's description, each repartition topic named as `topic` names it. */
  def apply[F[_]](topology: Topology[F], topic: Repartition => String): TopologyDescription =
    def named(source: Source): String =
      source match
        case Source.Input(read)            => read.value
        case Source.Repartitioned(written) => topic(written)
    TopologyDescription(topology.subtopologies.zipWithIndex.map((subtopology, id) => described(id, subtopology.nodes.toVector, named)))

  /** The node a topic's source makes, and its name. */
  def source[F[_], K, V](origin: Origin[F, K, V]): (Made, String) =
    origin match
      case Origin.Input(named, read, _, _)         => (Made(named.value, Kind.Reads(origin.source), Nil), named.value)
      case Origin.Repartitioned(repartition, _, _) =>
        val name = s"${repartition.value}-repartition-source"
        (Made(name, Kind.Reads(origin.source), Nil), name)

  /** The stores a step's processor keeps, or reads for a join. */
  def kept[F[_], K, V, K2, V2](step: Step[F, K, V, K2, V2]): List[String] =
    step match
      case Step.Aggregate(store, _, _, _, _, _)                    => List(store.value)
      case Step.TableSource(store, _, _)                           => List(store.value)
      case Step.ToTable(_, store, _, _)                            => List(store.value)
      case Step.TableFilter(_, _, store)                           => store.toList.map(_._1.value)
      case Step.TableMapValues(_, _, _, store)                     => store.toList.map(_._1.value)
      case Step.WindowedAggregate(store, _, _, _, _, _, _, _, _)   => List(store.value)
      case Step.SlidingAggregate(store, _, _, _, _, _, _, _, _)    => List(store.value)
      case Step.SessionAggregate(store, _, _, _, _, _, _, _, _, _) => List(store.value)
      case Step.Suppress(suppressed, _, _, _)                      => List(s"${suppressed.name}-store")
      case Step.Join(view, _)                                      => stores(view)
      case _                                                       => Nil

  /** The stores Java reads a table's values through, in the order its value getter names them: a suppression's buffer before its table's, and for
    * tables joined without a store, both sides'. Java gathers those in a `HashSet` of their own, whose order a processor's set of them keeps for two
    * names in one bucket, so they are taken in the order they come.
    */
  def stores[F[_], K, V](view: TableView[F, K, V]): List[String] =
    view match
      case TableView.Stored(store, _, _)    => List(store.value)
      case TableView.Held(buffer, parent)   => buffer :: stores(parent)
      case TableView.Joined(left, right, _) => (stores(left) ++ stores(right)).distinct
      case TableView.Filtered(parent, _)    => stores(parent)
      case TableView.Mapped(parent, _)      => stores(parent)

  /** A sub-topology as Java describes it: each node, in the order Java's `HashMap` of them iterates, connected to its predecessors and they to it in
    * `TreeSet`s ordered by the size of the nodes' subtrees at the time, larger first, then by name, each connection growing the subtrees above it;
    * then the nodes listed in that order, once every size is known.
    */
  private def described(id: Int, made: Vector[Made], topic: Source => String): TopologyDescription.Subtopology =
    val byName = made.map(node => node.name -> node).toMap
    val graph  =
      JavaHashSet.ordered(made.map(_.name), (_: String).hashCode)
        .foldLeft(Graph.empty)((graph, name) => byName(name).predecessors.foldLeft(graph)(_.connect(name, _)))
    val nodes =
      made.sortBy(node => (-graph.size(node.name), node.name)).toList.map: node =>
        val successors   = graph.successors(node.name)
        val predecessors = graph.predecessors(node.name)
        node.kind match
          case Kind.Reads(read)       => TopologyDescription.Node.Source(node.name, List(topic(read)), successors)
          case Kind.Processor(stores) => TopologyDescription.Node
              .Processor(node.name, JavaHashSet.ordered(stores, (_: String).hashCode).toList, successors, predecessors)
          case Kind.Writes(written) => TopologyDescription.Node.Sink(node.name, topic(written), predecessors)
    TopologyDescription.Subtopology(id, nodes)

  /** Java's nodes as they are being connected: the size of each one's subtree, counting a node once for each path to it, and each one's successors
    * and predecessors, each in a `TreeSet` that orders a node as it compares when it is added.
    */
  private final case class Graph(sizes: Map[String, Int], before: Map[String, JavaTreeSet], after: Map[String, JavaTreeSet]):
    def size(name: String): Int = sizes.getOrElse(name, 1)

    def successors(name: String): List[String]   = after.get(name).fold(Nil)(_.ordered)
    def predecessors(name: String): List[String] = before.get(name).fold(Nil)(_.ordered)

    /** As Java's `NodeComparator`: the node with the larger subtree first, then by name. */
    private def compare(x: String, y: String): Int = if x == y then 0 else if size(x) != size(y) then size(y) - size(x) else x.compareTo(y)

    /** `node` connected to `predecessor`, and the subtrees above `predecessor` grown by `node`'s. */
    def connect(node: String, predecessor: String): Graph =
      val linked =
        Graph(
          sizes,
          before.updated(node, before.getOrElse(node, JavaTreeSet.empty).inserted(predecessor, compare)),
          after.updated(predecessor, after.getOrElse(predecessor, JavaTreeSet.empty).inserted(node, compare))
        )
      linked.grown(predecessor, size(node))

    private def grown(name: String, delta: Int): Graph =
      predecessors(name).foldLeft(copy(sizes = sizes.updated(name, size(name) + delta)))(_.grown(_, delta))

  private object Graph:
    val empty: Graph = Graph(Map.empty, Map.empty, Map.empty)

/** Names in the order a `java.util.TreeSet` holding them iterates, each having been added as Java's `TreeMap` adds it: found a place by comparing it
  * with the names on the way down the tree, with the comparator as it is at that moment, then the tree rebalanced as Java's red-black tree is.
  */
private[xkafka] final case class JavaTreeSet(entries: Vector[JavaTreeSet.Entry], root: Option[Int]):
  import JavaTreeSet.Entry

  /** The names in order. */
  def ordered: List[String] =
    def from(at: Option[Int]): List[String] = at.fold(Nil)(i => from(entries(i).left) ++ (entries(i).key :: from(entries(i).right)))
    from(root)

  /** With `key` added where `compare` places it, unless it compares equal to a name it holds. */
  def inserted(key: String, compare: (String, String) => Int): JavaTreeSet =
    @tailrec
    def place(at: Int): Option[(Int, Boolean)] =
      val c = compare(key, entries(at).key)
      if c == 0 then None
      else
        val next = if c < 0 then entries(at).left else entries(at).right
        next match
          case Some(child) => place(child)
          case None        => Some((at, c < 0))
    root match
      case None    => JavaTreeSet(Vector(Entry(key, None, None, None, true)), Some(0))
      case Some(r) => place(r).fold(this): (parent, left) =>
          val id    = entries.size
          val added = JavaTreeSet(entries :+ Entry(key, None, None, Some(parent), false), root)
          added.set(parent)(entry => if left then entry.copy(left = Some(id)) else entry.copy(right = Some(id))).fixed(Some(id))

  private def set(at: Int)(change: Entry => Entry): JavaTreeSet = copy(entries = entries.updated(at, change(entries(at))))

  private def parentOf(x: Option[Int]): Option[Int]                  = x.flatMap(entries(_).parent)
  private def leftOf(x: Option[Int]): Option[Int]                    = x.flatMap(entries(_).left)
  private def rightOf(x: Option[Int]): Option[Int]                   = x.flatMap(entries(_).right)
  private def black(x: Option[Int]): Boolean                         = x.forall(entries(_).black)
  private def painted(x: Option[Int], isBlack: Boolean): JavaTreeSet = x.fold(this)(set(_)(_.copy(black = isBlack)))

  /** As `TreeMap.fixAfterInsertion`, from the red entry `x`. */
  @tailrec
  private def fixed(x: Option[Int]): JavaTreeSet =
    if x.isEmpty || x == root || black(parentOf(x)) then painted(root, true)
    else
      val grand = parentOf(parentOf(x))
      if parentOf(x) == leftOf(grand) then
        val uncle = rightOf(grand)
        if !black(uncle) then painted(parentOf(x), true).painted(uncle, true).painted(grand, false).fixed(grand)
        else
          val (turned, at) = if x == rightOf(parentOf(x)) then (rotatedLeft(parentOf(x)), parentOf(x)) else (this, x)
          val top          = turned.parentOf(turned.parentOf(at))
          turned.painted(turned.parentOf(at), true).painted(top, false).rotatedRight(top).fixed(at)
      else
        val uncle = leftOf(grand)
        if !black(uncle) then painted(parentOf(x), true).painted(uncle, true).painted(grand, false).fixed(grand)
        else
          val (turned, at) = if x == leftOf(parentOf(x)) then (rotatedRight(parentOf(x)), parentOf(x)) else (this, x)
          val top          = turned.parentOf(turned.parentOf(at))
          turned.painted(turned.parentOf(at), true).painted(top, false).rotatedLeft(top).fixed(at)

  /** As `TreeMap.rotateLeft`. */
  private def rotatedLeft(p: Option[Int]): JavaTreeSet =
    p.fold(this): at =>
      val r      = entries(at).right.get
      val moved  = entries(r).left
      val up     = entries(at).parent
      val linked = set(at)(_.copy(right = moved)).relinked(moved, Some(at)).set(r)(_.copy(parent = up))
      val rooted =
        up match
          case None                                          => linked.copy(root = Some(r))
          case Some(u) if linked.entries(u).left == Some(at) => linked.set(u)(_.copy(left = Some(r)))
          case Some(u)                                       => linked.set(u)(_.copy(right = Some(r)))
      rooted.set(r)(_.copy(left = Some(at))).set(at)(_.copy(parent = Some(r)))

  /** As `TreeMap.rotateRight`. */
  private def rotatedRight(p: Option[Int]): JavaTreeSet =
    p.fold(this): at =>
      val l      = entries(at).left.get
      val moved  = entries(l).right
      val up     = entries(at).parent
      val linked = set(at)(_.copy(left = moved)).relinked(moved, Some(at)).set(l)(_.copy(parent = up))
      val rooted =
        up match
          case None                                           => linked.copy(root = Some(l))
          case Some(u) if linked.entries(u).right == Some(at) => linked.set(u)(_.copy(right = Some(l)))
          case Some(u)                                        => linked.set(u)(_.copy(left = Some(l)))
      rooted.set(l)(_.copy(right = Some(at))).set(at)(_.copy(parent = Some(l)))

  private def relinked(child: Option[Int], parent: Option[Int]): JavaTreeSet = child.fold(this)(set(_)(_.copy(parent = parent)))

private[xkafka] object JavaTreeSet:
  final case class Entry(key: String, left: Option[Int], right: Option[Int], parent: Option[Int], black: Boolean)

  val empty: JavaTreeSet = JavaTreeSet(Vector.empty, None)
