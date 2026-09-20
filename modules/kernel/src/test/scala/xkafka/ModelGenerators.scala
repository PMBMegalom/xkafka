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

import org.scalacheck.{Arbitrary, Cogen, Gen}

/** Generators for the portable model types, shared by the law and property suites. */
trait ModelGenerators:
  private val topicCharacters = Gen.oneOf(Gen.alphaNumChar, Gen.oneOf('.', '_', '-'))

  private val topicNames = Gen.chooseNum(1, 12).flatMap(Gen.stringOfN(_, topicCharacters)).suchThat(value => value != "." && value != "..")

  given Arbitrary[Topic] = Arbitrary(topicNames.map(Topic.from(_).toOption.get))
  given Cogen[Topic]     = Cogen[String].contramap(_.value)

  given Arbitrary[TopicPattern] = Arbitrary(Gen.oneOf("events-.*", "a|b", ".*").map(TopicPattern.from(_).toOption.get))
  given Cogen[TopicPattern]     = Cogen[String].contramap(_.value)

  given Arbitrary[Partition] = Arbitrary(Gen.chooseNum(0, 8).map(Partition.from(_).toOption.get))
  given Cogen[Partition]     = Cogen[Int].contramap(_.value)

  given Arbitrary[Offset] = Arbitrary(Gen.chooseNum(0L, Long.MaxValue).map(Offset.from(_).toOption.get))
  given Cogen[Offset]     = Cogen[Long].contramap(_.value)

  given Arbitrary[ConsumerGroup] = Arbitrary(Gen.chooseNum(1, 12).flatMap(Gen.stringOfN(_, Gen.alphaNumChar)).map(ConsumerGroup.from(_).toOption.get))
  given Cogen[ConsumerGroup]     = Cogen[String].contramap(_.value)

  given Arbitrary[Timestamp] = Arbitrary(Arbitrary.arbitrary[Long].map(Timestamp.fromEpochMillis))
  given Cogen[Timestamp]     = Cogen[Long].contramap(_.epochMillis)

  given Arbitrary[TopicPartition] =
    Arbitrary(for topic <- Arbitrary.arbitrary[Topic]; partition <- Arbitrary.arbitrary[Partition] yield TopicPartition(topic, partition))
  given Cogen[TopicPartition] = Cogen[(String, Int)].contramap(value => (value.topic.value, value.partition.value))

  /** A small partition space, so that generated batches actually collide on a topic-partition. */
  val collidingTopicPartitions: Gen[TopicPartition] =
    for
      topic     <- Gen.oneOf("alpha", "beta").map(Topic.from(_).toOption.get)
      partition <- Gen.oneOf(0, 1).map(Partition.from(_).toOption.get)
    yield TopicPartition(topic, partition)

  val smallOffsets: Gen[Offset] = Gen.chooseNum(0L, 20L).map(Offset.from(_).toOption.get)
