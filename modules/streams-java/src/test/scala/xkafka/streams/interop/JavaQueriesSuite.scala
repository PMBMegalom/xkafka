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

import java.nio.file.Files
import java.util.Properties

import fs2.Chunk
import munit.FunSuite
import org.apache.kafka.common.serialization.Serdes as JavaSerdes
import org.apache.kafka.streams.{KafkaStreams as JavaKafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.Materialized
import internal.streams.{Answered, Asked, QueryWire}

/** What a Java member answers before it runs, and for stores it was not given, without a broker. */
final class JavaQueriesSuite extends FunSuite:
  private def topology() =
    val builder = new StreamsBuilder()
    builder.stream[String, String]("in").groupByKey.count(Materialized.as("counts"))
    builder.build()

  private def notStarted(): JavaKafkaStreams =
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("xkafka-java-queries").toString)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JavaSerdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JavaSerdes.String.getClass)
    new JavaKafkaStreams(topology(), properties)

  private def asked(request: QueryRequest) = QueryWire.encode(Asked(request, List(0), false)).toArray

  private def answered(bytes: Array[Byte]) = QueryWire.answered(Chunk.array(bytes))

  test("a store given but not running yet answers as rebalancing, and one not given, or of another kind, as unknown"):
    val streams = notStarted()
    try
      val handler = JavaQueries.of(streams, "app").keyValue("counts", JavaSerdes.String, JavaSerdes.Long)
      val key     = Chunk.array("k".getBytes)
      assertEquals(answered(handler.answer(asked(QueryRequest.Get("counts", key)))), Some(Answered.Refused(QueryProblem.Rebalancing)))
      assertEquals(answered(handler.answer(asked(QueryRequest.Get("other", key)))), Some(Answered.Refused(QueryProblem.UnknownStore("other"))))
      assertEquals(answered(handler.answer(asked(QueryRequest.Sessions("counts", key)))), Some(Answered.Refused(QueryProblem.UnknownStore("counts"))))
    finally streams.close()

  test("bytes that are not an xkafka query are refused"):
    val streams = notStarted()
    try
      val failure = intercept[IllegalArgumentException](JavaQueries.of(streams, "app").answer("GET /".getBytes))
      assertEquals(failure.getMessage, "the request is not an xkafka query")
    finally streams.close()

  test("a Java member asking before it runs is told the store is moving, and asking of a store its program lacks is told the store is unknown"):
    val streams = notStarted()
    try
      val asking               = JavaQuerying.of(streams, topology(), "app", (_, _) => throw new AssertionError("nothing is asked of other members"))
      def problem(ask: => Any) = intercept[QueryProblemException](ask).problem
      assertEquals(problem(asking.get("counts", "k", JavaSerdes.String, JavaSerdes.Long)), QueryProblem.Rebalancing)
      assertEquals(problem(asking.all("counts", JavaSerdes.String, JavaSerdes.Long)), QueryProblem.Rebalancing)
      assertEquals(problem(asking.all("other", JavaSerdes.String, JavaSerdes.Long)), QueryProblem.UnknownStore("other"))
    finally streams.close()
