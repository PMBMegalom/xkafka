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
import java.time.Duration
import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{
  Grouped, JoinWindows, Materialized, Produced, SessionWindows, SlidingWindows, StreamJoined, TableJoined, TimeWindows, WindowedSerdes
}
import org.apache.kafka.streams.state.KeyValueStore

/** One Java Streams member of a mixed fleet, in a process of its own so that it can be killed outright, running the program the fleet tests' xkafka
  * members run. With `letters`, it counts the records of `<application>-input` by their value's first letter, through the repartition topic
  * `<application>-by-letter-repartition`, into the store `counts`, writing each new count to `<application>-output`. With `windows`, it counts each
  * key's records of `<application>-input-tumbling` in tumbling windows of 5 s into `tumbling`, of `<application>-input-sliding` in sliding windows of
  * 2.5 s into `sliding`, and of `<application>-input-sessions` in sessions with a gap of 1.5 s into `sessions`, each with an hour of grace, writing
  * each change to `<application>-output-<store>`. With `joins`, it joins each record of `<application>-input-orders` with its key's value in the
  * table `users` read from `<application>-input-users`, writing `<order>:<user>` to `<application>-output`. With `tables`, it left joins the table
  * `plans` read from `<application>-input-plans` with the table `users`, into the store `accounts`, writing `<plan>:<user>`, or `<plan>:-` for a key
  * without a user, to `<application>-output`. With `pairs`, it joins each record of the stream `<application>-input-orders` with each record of the
  * stream `<application>-input-users` with its key at most an hour apart, with an hour of grace, its stores named from `pairs`, writing
  * `<order>:<user>` to `<application>-output`. It prints `running` once Java Streams says
  * it runs, and closes on a SIGTERM, within 30 seconds.
  *
  * Arguments: bootstrap server, application id, `at_least_once` or `exactly_once_v2`, `letters`, `windows`, `joins`, `tables` or `pairs`.
  */
object FleetMember:
  def main(arguments: Array[String]): Unit =
    val Array(bootstrap, application, guarantee, program) = arguments
    val builder                                            = StreamsBuilder()
    program match
      case "letters" =>
        builder.stream[String, String](s"$application-input").groupBy((_, value) => value.take(1), Grouped.as("by-letter"))
          .count(Materialized.as("counts")).toStream.to(s"$application-output", Produced.`with`(Serdes.String, Serdes.Long))
      case "windows" =>
        val hour = Duration.ofHours(1)
        builder.stream[String, String](s"$application-input-tumbling").groupByKey.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(5), hour))
          .count(Materialized.as("tumbling")).toStream
          .to(s"$application-output-tumbling", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 5000L), Serdes.Long))
        builder.stream[String, String](s"$application-input-sliding").groupByKey
          .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(2500), hour)).count(Materialized.as("sliding")).toStream
          .to(s"$application-output-sliding", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 2500L), Serdes.Long))
        builder.stream[String, String](s"$application-input-sessions").groupByKey
          .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMillis(1500), hour)).count(Materialized.as("sessions")).toStream
          .to(s"$application-output-sessions", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long))
      case "joins" =>
        val users = builder.table[String, String](s"$application-input-users", Materialized.as("users"))
        builder.stream[String, String](s"$application-input-orders").join(users, (order: String, user: String) => s"$order:$user")
          .to(s"$application-output", Produced.`with`(Serdes.String, Serdes.String))
      case "foreign-keys" =>
        // The table of orders is built first, which is how xkafka numbers the sub-topologies around the join.
        val orders = builder.table[String, String](s"$application-input-orders", Materialized.as("orders"))
        val users  = builder.table[String, String](s"$application-input-users", Materialized.as("users"))
        orders.join[String, String, String](
          users,
          (order: String) => order.dropWhile(_ != ':').drop(1),
          (order: String, user: String) => s"${order.takeWhile(_ != ':')}:$user",
          TableJoined.as[String, String]("fk")
        ).toStream.to(s"$application-output", Produced.`with`(Serdes.String, Serdes.String))
      case "globals" =>
        // Built after the stream that joins it, which is how xkafka numbers the sub-topologies around it.
        val orders = builder.stream[String, String](s"$application-input-orders")
        orders.join(
          builder.globalTable[String, String](s"$application-input-users", Materialized.as("users")),
          (key: String, _: String) => key,
          (order: String, user: String) => s"$order:$user"
        ).to(s"$application-output", Produced.`with`(Serdes.String, Serdes.String))
      case "tables" =>
        val users    = builder.table[String, String](s"$application-input-users", Materialized.as("users"))
        val accounts = Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("accounts").withValueSerde(Serdes.String)
        builder.table[String, String](s"$application-input-plans", Materialized.as("plans"))
          .leftJoin(users, (plan: String, user: String) => s"$plan:${Option(user).getOrElse("-")}", accounts).toStream
          .to(s"$application-output", Produced.`with`(Serdes.String, Serdes.String))
      case "pairs" =>
        val hour   = Duration.ofHours(1)
        val joined = StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
        builder.stream[String, String](s"$application-input-orders").join(
          builder.stream[String, String](s"$application-input-users"),
          (order: String, user: String) => s"$order:$user",
          JoinWindows.ofTimeDifferenceAndGrace(hour, hour),
          joined
        ).to(s"$application-output", Produced.`with`(Serdes.String, Serdes.String))
      case other => throw IllegalArgumentException(s"no fleet program $other")
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
    streams.setStateListener: (now, before) =>
      if now == KafkaStreams.State.RUNNING then println("running")
      // Any other change is news for whoever reads a failed test's output, without which a member that never runs says nothing.
      else println(s"state $before -> $now")
      System.out.flush()
    streams.setUncaughtExceptionHandler: error =>
      println(s"failed: $error")
      error.printStackTrace(System.out)
      System.out.flush()
      org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT
    // Bounded, so a member that cannot close cleanly still ends.
    sys.addShutdownHook { val _ = streams.close(java.time.Duration.ofSeconds(30)) }
    streams.start()
    Thread.currentThread.join()
