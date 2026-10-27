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
import xkafka.streams.{End, Origin, Topology}

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

  def apply[F[_]](application: String, topology: Topology[F], replicationFactor: Short = BrokerReplication): StreamsGroup.Topology =
    def repartitionTopic(name: String) = s"$application-$name-repartition"
    val segments                       = topology.pipelines.toList.flatMap(_.segments.toList)
    val subtopologies                  =
      segments.zipWithIndex.map: (segment, id) =>
        StreamsGroup.Subtopology(
          id = id.toString,
          sourceTopics =
            segment.origin match
              case Origin.Input(topic, _, _) => List(topic.value)
              case _                         => Nil,
          sourceTopicRegex = Nil,
          stateChangelogTopics =
            segment.chain.stores.map(store => s"$application-${store.name}-changelog").sorted
              .map(StreamsGroup.TopicInfo(_, 0, replicationFactor, ChangelogConfigs)),
          repartitionSinkTopics =
            segment.end match
              case End.Repartitioning(named, _, _) => List(repartitionTopic(named.name))
              case _                               => Nil,
          repartitionSourceTopics =
            segment.origin match
              case Origin.Repartitioned(named, _, _) =>
                List(StreamsGroup.TopicInfo(repartitionTopic(named.name), 0, replicationFactor, RepartitionConfigs))
              case _ => Nil,
          copartitionGroups = Nil
        )
    StreamsGroup.Topology(0, subtopologies.sortBy(_.id))
