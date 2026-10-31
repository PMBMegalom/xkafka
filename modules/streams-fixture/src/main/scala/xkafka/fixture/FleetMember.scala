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

package xkafka.fixture

import java.nio.file.Files
import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{Grouped, Materialized, Produced}

/** One Java Streams member of a mixed fleet, in a process of its own so that it can be killed outright: it counts the records of
  * `<application>-input` by their value's first letter, through the repartition topic `<application>-by-letter-repartition`, into the store
  * `counts`, writing each new count to `<application>-output`, as the fleet tests' xkafka members do. It prints `running` once Java Streams says it
  * runs, and closes on a SIGTERM, within 30 seconds.
  *
  * Arguments: bootstrap server, application id, `at_least_once` or `exactly_once_v2`.
  */
object FleetMember:
  def main(arguments: Array[String]): Unit =
    val Array(bootstrap, application, guarantee) = arguments
    val builder                                   = StreamsBuilder()
    builder.stream[String, String](s"$application-input").groupBy((_, value) => value.take(1), Grouped.as("by-letter"))
      .count(Materialized.as("counts")).toStream.to(s"$application-output", Produced.`with`(Serdes.String, Serdes.Long))
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, application)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams")
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, guarantee)
    properties.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("xkafka-fleet-member").toString)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    // Java's record cache would hold back changes the tests count; xkafka writes every change.
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    val streams = KafkaStreams(builder.build(), properties)
    streams.setStateListener: (now, _) =>
      if now == KafkaStreams.State.RUNNING then
        println("running")
        System.out.flush()
    // Bounded, so a member that cannot close cleanly still ends.
    sys.addShutdownHook { val _ = streams.close(java.time.Duration.ofSeconds(30)) }
    streams.start()
    Thread.currentThread.join()
