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

import java.util.{Optional, Properties, UUID}
import java.util.function.Supplier

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, Topology as JavaTopology}
import org.apache.kafka.streams.processor.internals.{InternalTopologyBuilder, StreamThread, TopologyMetadata}

/** A Java Streams program as its stream thread prepares it to join a group, for comparing xkafka's with it. */
private[xkafka] object JavaStreams:
  /** The program `build` makes, for the application `application`, and what its stream thread sends the group's coordinator for it. */
  def prepared(application: String)(build: StreamsBuilder => Unit): (JavaTopology, StreamsRebalanceData) =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, application)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    val builder = new StreamsBuilder
    build(builder)
    val topology = builder.build(properties)
    val field    = classOf[JavaTopology].getDeclaredField("internalTopologyBuilder")
    field.setAccessible(true)
    val config   = StreamsConfig(properties)
    val metadata = TopologyMetadata(field.get(topology).asInstanceOf[InternalTopologyBuilder], config)
    metadata.buildAndRewriteTopology()
    val init = classOf[StreamThread].getDeclaredMethods.find(_.getName == "initStreamsRebalanceData").get
    init.setAccessible(true)
    val none: Supplier[java.util.Map[StreamsRebalanceData.TaskId, java.lang.Long]] = () => java.util.Map.of()
    (topology, init.invoke(null, UUID.randomUUID, config, Optional.empty, Optional.empty, metadata, none, none).asInstanceOf[StreamsRebalanceData])

  /** The description the Java program pushes, as `StreamsGroupTopologyDescriptionUpdate` carries it. */
  def pushed(application: String)(build: StreamsBuilder => Unit): StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription =
    prepared(application)(build)._2.wireTopologyDescription()
