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
package librdkafka

final class LibrdkafkaAnswersSuite extends munit.FunSuite:
  test("listing groups leaves out a group with an empty name, which no ConsumerGroup can name"):
    assertEquals(
      LibrdkafkaAnswers.listed(Vector("" -> 5, "workers" -> 3)),
      Vector(ConsumerGroup.from("workers").toOption.get -> ConsumerGroupState.Stable)
    )

  test("a transactional failure is retriable as its code says, whatever librdkafka's own flag"):
    val timedOut = Failures.classified("timed out", -185, fatal = 0, transactionAbortRequired = 1)
    val fenced   = Failures.classified("fenced", -144, fatal = 1, transactionAbortRequired = -1)
    assertEquals(
      (timedOut.code, timedOut.retriable, timedOut.fatal, timedOut.transactionAbortRequired),
      (Some(ErrorCode.RequestTimedOut), Some(true), Some(false), Some(true))
    )
    assertEquals(
      (fenced.code, fenced.retriable, fenced.fatal, fenced.transactionAbortRequired),
      (Some(ErrorCode.Fenced), Some(false), Some(true), None)
    )

  test("a topic id reads as Kafka prints it, in URL-safe base64 without padding, where librdkafka prints + and /"):
    assertEquals(LibrdkafkaAnswers.topicId((0x0123456789abcdefL, 0xfedcba9876543210L)).value, "ASNFZ4mrze_-3LqYdlQyEA")
    assertEquals(LibrdkafkaAnswers.topicId((0xfbffbffbffbffbffL, 0xfeffeffeffeffeffL)).value, "-_-_-_-_-__-_-_-_-_-_w")

  test("describing topics fails as a whole where every topic's answer says the cluster was not reached, and answers each topic otherwise"):
    val topics                = Vector(Topic.from("first").toOption.get, Topic.from("second").toOption.get)
    def answered(codes: Int*) =
      topics.zip(codes).map((topic, code) => DescribedTopic(topic.value, code, s"code $code", (0L, 0L), false, Vector.empty))
    val unreached                      = intercept[KafkaException.BackendFailure](LibrdkafkaAnswers.topics(topics, answered(-185, -185)))
    val Vector(timedOut, unknownTopic) = LibrdkafkaAnswers.topics(topics, answered(-185, 3))
    assertEquals(unreached.code, Some(ErrorCode.RequestTimedOut))
    assertEquals(
      timedOut.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
      Some(ErrorCode.RequestTimedOut)
    )
    assertEquals(
      unknownTopic.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
      Some(ErrorCode.UnknownTopicOrPartition)
    )

  test("changing configurations fails as a whole where every topic's answer says the cluster was not reached, and answers each topic otherwise"):
    val topics                    = Vector(Topic.from("first").toOption.get, Topic.from("second").toOption.get)
    def answered(codes: Int*)     = topics.zip(codes).map((topic, code) => ConfigResource(topic.value, code, s"code $code", Vector.empty))
    val unreached                 = intercept[KafkaException.BackendFailure](LibrdkafkaAnswers.changed(topics, answered(-185, -185)))
    val Vector(timedOut, changed) = LibrdkafkaAnswers.changed(topics, answered(-185, 0))
    assertEquals(unreached.code, Some(ErrorCode.RequestTimedOut))
    assertEquals(
      timedOut.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
      Some(ErrorCode.RequestTimedOut)
    )
    assertEquals(changed, Right(()))

  test("a partition its preferred replica already leads answers as elected, beside one the cluster does not have"):
    val partitions          = Vector(0, 1).map(index => TopicPartition(Topic.from("events").toOption.get, Partition.from(index).toOption.get))
    val Vector(led, absent) = LibrdkafkaAnswers.elected(partitions, Coded.Answered(Vector(84, 3)), code => s"code $code")
    assertEquals(led, Right(()))
    assertEquals(
      absent.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
      Some(ErrorCode.UnknownTopicOrPartition)
    )
