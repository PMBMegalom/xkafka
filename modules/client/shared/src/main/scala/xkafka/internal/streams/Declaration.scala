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

import protocol.StreamsGroup
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{End, Source, Topology}

/** The topology a member declares to the coordinator for a program, as Java Streams declares the same program: sub-topologies numbered in the order
  * their segments are made, and every list sorted as Java's client sorts it before sending.
  */
private[xkafka] object Declaration:
  /** The configs Java gives a key-value store's changelog topic. */
  val ChangelogConfigs: List[(String, String)] = List("cleanup.policy" -> "compact", "message.timestamp.type" -> "CreateTime")

  /** The configs Java gives a repartition topic. */
  val RepartitionConfigs: List[(String, String)] =
    List("cleanup.policy" -> "delete", "message.timestamp.type" -> "CreateTime", "retention.ms" -> "-1", "segment.bytes" -> "52428800")

  /** Java's `replication.factor` default, which leaves the choice to the broker. */
  val BrokerReplication: Short = -1

  /** A store's changelog topic, configured as Java configures a key-value store's, or a window store's where the store keeps windows for `retention`,
    * and for a window store of a stream-stream join, which keeps every record, with only `cleanup.policy=delete`, as Java's join forces it.
    */
  private def changelog(
      application: String,
      store: String,
      retention: Option[Long],
      duplicates: Boolean,
      replicationFactor: Short
  ): StreamsGroup.TopicInfo =
    val configs = retention.fold(ChangelogConfigs)(WindowStore.changelogConfigs)
    val kept    = if duplicates then configs.map((name, value) => name -> (if name == "cleanup.policy" then "delete" else value)) else configs
    StreamsGroup.TopicInfo(s"$application-$store-changelog", 0, replicationFactor, kept)

  /** The topics of each copartition group whose order Java takes from its own names for the program's sources, which xkafka does not know: those of a
    * group in which two topic names share a bucket of Java's set.
    */
  def unordered[F[_]](application: String, topology: Topology[F]): List[List[String]] =
    topology.pipelines.toList.flatMap(_.segments.toList).map(_.copartitioned.map(Task.topicOf(application, _)))
      .filter(names => names.foldLeft(JavaHashSet.empty((name: String) => name.hashCode))(_ + _).crowded)

  def apply[F[_]](application: String, topology: Topology[F], replicationFactor: Short = BrokerReplication): StreamsGroup.Topology =
    def repartitionTopic(name: String) = s"$application-$name-repartition"
    val segments                       = topology.pipelines.toList.flatMap(_.segments.toList)
    val subtopologies                  =
      segments.zipWithIndex.map: (segment, id) =>
        val sources      = segment.origins.collect { case Source.Input(topic) => topic.value }.sorted
        val repartitions = segment.origins.collect { case Source.Repartitioned(named) => repartitionTopic(named.value) }.sorted
        // A join puts the topics it reads in one copartition group, which Java keeps as a HashSet of their names, and sends as indices into the
        // sorted lists of the sub-topology's topics, in the order that set iterates.
        val copartitioned =
          Option.when(segment.copartitioned.nonEmpty):
            val names = JavaHashSet.ordered(segment.copartitioned.map(Task.topicOf(application, _)), (name: String) => name.hashCode)
            StreamsGroup.CopartitionGroup(
              names.flatMap(name => Option(sources.indexOf(name)).filter(_ >= 0)).map(_.toShort).toList,
              Nil,
              names.flatMap(name => Option(repartitions.indexOf(name)).filter(_ >= 0)).map(_.toShort).toList
            )
        StreamsGroup.Subtopology(
          id = id.toString,
          sourceTopics = sources,
          sourceTopicRegex = Nil,
          stateChangelogTopics =
            segment.stores.map(store =>
              changelog(application, store.value, segment.retentions.get(store.value), segment.duplicates(store.value), replicationFactor)
            ).sortBy(_.name),
          repartitionSinkTopics =
            segment.end match
              case End.Repartitioning(named, _, _) => List(repartitionTopic(named.value))
              case _                               => Nil,
          repartitionSourceTopics = repartitions.map(StreamsGroup.TopicInfo(_, 0, replicationFactor, RepartitionConfigs)),
          copartitionGroups = copartitioned.toList
        )
    StreamsGroup.Topology(0, subtopologies.sortBy(_.id))
