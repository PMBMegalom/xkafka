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

import scala.jdk.CollectionConverters.*

import munit.FunSuite
import org.apache.kafka.streams.processor.internals.{ProcessorMetadata, TopicPartitionMetadata}

/** The partition time in committed offsets' metadata, against Java Streams' own `TopicPartitionMetadata`. */
final class PartitionTimeGoldenSuite extends FunSuite:
  private val times = List(0L, 1L, 42L, 1700000000000L, Long.MaxValue)

  test("the engine writes what Java writes for a partition time with no processor metadata"):
    times.foreach(time => assertEquals(PartitionTime.encode(time), TopicPartitionMetadata(time, ProcessorMetadata()).encode()))

  test("the engine reads the time from what Java writes, with or without processor metadata"):
    val metadata = ProcessorMetadata(Map[String, java.lang.Long]("store" -> 7L, "other" -> 9L).asJava)
    times.foreach: time =>
      assertEquals(PartitionTime.decode(TopicPartitionMetadata(time, ProcessorMetadata()).encode()), Some(time))
      assertEquals(PartitionTime.decode(TopicPartitionMetadata(time, metadata).encode()), Some(time))

  test("Java reads the time from what the engine writes"):
    times.foreach(time => assertEquals(TopicPartitionMetadata.decode(PartitionTime.encode(time)).partitionTime, time))
