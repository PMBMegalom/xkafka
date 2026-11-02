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

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

/** Java members of the fleet tests of one backend, each a process of its own running [[FleetMember]], started and stopped as the tests ask.
  *
  * Commands, one per record of `xkafka-fleet-<backend>-control`: `start <id> <application> <guarantee> <program>`, where the program is one
  * [[FleetMember]] runs, `stop <id>` (a SIGTERM, on which Java
  * Streams closes) and `kill <id>` (a SIGKILL). Each member's news goes to `xkafka-fleet-<backend>-status` as `<id> running` once it runs and
  * `<id> ended` once its process has ended.
  */
final class Fleet(broker: String, backend: String):
  private val control = s"xkafka-fleet-$backend-control"
  private val status  = s"xkafka-fleet-$backend-status"
  private val members = ConcurrentHashMap[String, Process]()

  private val admin = Admin.create(Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> broker).asJava)
  Try(admin.createTopics(List(NewTopic(control, 1, 1.toShort), NewTopic(status, 1, 1.toShort)).asJava).all.get)
  admin.close()

  private val producer =
    KafkaProducer[String, String](
      Map[String, AnyRef](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker).asJava,
      StringSerializer(),
      StringSerializer()
    )

  private def report(news: String): Unit =
    val _ = producer.send(ProducerRecord(status, "fleet", news)).get

  private def daemon(body: => Unit): Unit =
    val thread = Thread(() => body)
    thread.setDaemon(true)
    thread.start()

  daemon:
      val consumer = KafkaConsumer[String, String](Map[String, AnyRef](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker).asJava, StringDeserializer(), StringDeserializer())
      consumer.assign(List(TopicPartition(control, 0)).asJava)
      consumer.seekToBeginning(List(TopicPartition(control, 0)).asJava)
      while true do consumer.poll(Duration.ofMillis(500)).asScala.foreach(record => carryOut(record.value.split(' ').toList))

  private def carryOut(command: List[String]): Unit =
    command match
      case List("start", id, application, guarantee, program) => start(id, application, guarantee, program)
      case List("stop", id)                                   => Option(members.get(id)).foreach(_.destroy())
      case List("kill", id)                                   => Option(members.get(id)).foreach(_.destroyForcibly())
      case other                                              => System.err.println(s"fleet $backend: unknown command $other")

  private def start(id: String, application: String, guarantee: String, program: String): Unit =
    val java    = ProcessHandle.current.info.command.orElse("java")
    val process =
      ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "xkafka.fixture.FleetMember", broker, application, guarantee, program)
        .redirectErrorStream(true).start()
    members.put(id, process)
    daemon:
      val lines = BufferedReader(InputStreamReader(process.getInputStream))
      // Stopping a process closes its output, which a read still under way then sees as an error, not as its end.
      try
        Iterator.continually(lines.readLine()).takeWhile(_ != null).foreach: line =>
          if line == "running" then report(s"$id running") else println(s"fleet $backend $id: $line")
      catch case _: IOException => ()
      val _ = process.waitFor()
      members.remove(id)
      report(s"$id ended")

  def close(): Unit =
    members.values.asScala.foreach(_.destroyForcibly())
    producer.close()

object Fleet:
  /** The broker release the fleet tests run on. */
  val Release = "4.4.0"
