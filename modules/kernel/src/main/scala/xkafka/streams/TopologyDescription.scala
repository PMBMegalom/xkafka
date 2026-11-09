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

import cats.Show

/** What a program runs: its sub-topologies and global tables, and their nodes. `toString` writes the text that Java Kafka Streams'
  * `Topology.describe` writes for the same program.
  */
final case class TopologyDescription(subtopologies: List[TopologyDescription.Subtopology], globalStores: List[TopologyDescription.GlobalStore] = Nil):
  override def toString: String = TopologyDescription.text(this)

object TopologyDescription:
  /** A part of the program that runs as tasks of its own, and its nodes. */
  final case class Subtopology(id: Int, nodes: List[Node])

  /** A global table, numbered with the sub-topologies although it makes no tasks: the node that reads its topic and the node that keeps its store. */
  final case class GlobalStore(id: Int, source: Node.Source, processor: Node.Processor)

  /** A node of a sub-topology, with the nodes it passes records to and gets them from, by name. */
  enum Node:
    /** Reads `topics`. A repartition topic is shown without the application's id. */
    case Source(name: String, topics: List[String], successors: List[String])

    /** Processes records, keeping or reading `stores`. */
    case Processor(name: String, stores: List[String], successors: List[String], predecessors: List[String])

    /** Writes `topic`. */
    case Sink(name: String, topic: String, predecessors: List[String])

    def name: String

  given Show[TopologyDescription] = Show.show(text)

  private def names(nodes: List[String]): String = if nodes.isEmpty then "none" else nodes.mkString(", ")

  private def line(node: Node): String =
    node match
      case Node.Source(name, topics, successors) => s"Source: $name (topics: ${topics.mkString("[", ", ", "]")})\n      --> ${names(successors)}"
      case Node.Processor(name, stores, successors, predecessors) =>
        s"Processor: $name (stores: ${stores.mkString("[", ", ", "]")})\n      --> ${names(successors)}\n      <-- ${names(predecessors)}"
      case Node.Sink(name, topic, predecessors) => s"Sink: $name (topic: $topic)\n      <-- ${names(predecessors)}"

  /** Java's text: a header line, then each sub-topology and global table by number with its nodes, each sub-topology ending with an empty line. */
  private def text(description: TopologyDescription): String =
    val subtopologies =
      description.subtopologies.map(s => s.id -> (s"  Sub-topology: ${s.id}\n" + s.nodes.map(n => s"    ${line(n)}\n").mkString + "\n"))
    val stores =
      description.globalStores.map: g =>
        g.id -> (s"  Sub-topology: ${g.id} for global store (will not generate tasks)\n    ${line(g.source)}\n    ${line(g.processor)}\n")
    "Topologies:\n " + (subtopologies ++ stores).sortBy(_._1).map(_._2).mkString
