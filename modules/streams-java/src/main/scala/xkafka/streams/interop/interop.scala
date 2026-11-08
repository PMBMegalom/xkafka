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
package interop

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

import org.apache.kafka.streams.processor.StreamPartitioner
import xkafka.interop.toJava

extension (companion: Partitioner.type)
  /** A Java `StreamPartitioner` as a partitioner, given each record's topic and headers as Java's is. Where it chooses none, the record goes where
    * Kafka's default puts it, as in Java; where it chooses several partitions or none at all, the application stops, as xkafka sends each record to
    * one partition.
    */
  def fromJava[K, V](java: StreamPartitioner[K, V]): Partitioner[K, V] =
    Partitioner.orDefault: (topic, key, value, headers, count) =>
      java.partitions(topic.value, key, value, headers.toJava, count).toScala.map(_.asScala.toList) match
        case None               => None
        case Some(List(chosen)) => Some(chosen.intValue)
        case Some(chosen) => throw new IllegalArgumentException(s"the Java partitioner for the topic '${topic.value}' chose the partitions ${chosen
              .sorted.mkString("[", ", ", "]")}, where xkafka sends each record to one partition")
