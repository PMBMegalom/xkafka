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

import java.util.Optional

import scala.jdk.CollectionConverters.*

import org.apache.kafka.common.header.Headers as JavaHeaders
import org.apache.kafka.streams.processor.StreamPartitioner
import munit.FunSuite

/** A Java partitioner made into ours chooses what Java's chooses, given what Java's is given. */
final class PartitionerFromJavaSuite extends FunSuite:
  private val topic   = Topic.from("orders").orFail
  private val headers = Headers.empty.append("region", "eu")

  private def partitioner(answer: (String, String, String, JavaHeaders, Int) => Optional[java.util.Set[Integer]]) =
    new StreamPartitioner[String, String]:
      override def partitions(topic: String, key: String, value: String, count: Int): Optional[java.util.Set[Integer]] =
        answer(topic, key, value, null, count)
      override def partitions(topic: String, key: String, value: String, passed: JavaHeaders, count: Int): Optional[java.util.Set[Integer]] =
        answer(topic, key, value, passed, count)

  private def one(n: Int): Optional[java.util.Set[Integer]] = Optional.of(Set(Integer.valueOf(n)).asJava)

  test("a Java partitioner's one partition is chosen, from the topic, key, value, headers and partition count it is given"):
    val java =
      partitioner: (topic, key, value, passed, count) =>
        val region = Option(passed.lastHeader("region")).map(header => String(header.value)).getOrElse("")
        one((topic.length + key.length + value.length + region.length) % count)
    assertEquals(Partitioner.fromJava(java).chosen(topic, "k", "value", headers, 4), Some((6 + 1 + 5 + 2) % 4))

  test("a Java partitioner that chooses none leaves the record to Kafka's default"):
    val java = partitioner((_, _, _, _, _) => Optional.empty())
    assertEquals(Partitioner.fromJava(java).chosen(topic, "k", "v", headers, 4), None)

  test("a Java partitioner that chooses several partitions, or none at all, stops the application, saying which"):
    val several = partitioner((_, _, _, _, _) => Optional.of(Set(Integer.valueOf(2), Integer.valueOf(0)).asJava))
    val nothing = partitioner((_, _, _, _, _) => Optional.of(java.util.Collections.emptySet[Integer]))
    val failed  = intercept[IllegalArgumentException](Partitioner.fromJava(several).chosen(topic, "k", "v", headers, 4))
    assert(failed.getMessage.contains("[0, 2]") && failed.getMessage.contains("'orders'"), failed.getMessage)
    val dropped = intercept[IllegalArgumentException](Partitioner.fromJava(nothing).chosen(topic, "k", "v", headers, 4))
    assert(dropped.getMessage.contains("[]"), dropped.getMessage)
