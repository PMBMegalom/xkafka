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

import cats.Order
import cats.syntax.all.*
import fs2.Chunk
import munit.FunSuite

final class ModelSuite extends FunSuite:
  test("topic rejects an empty name"):
    assertEquals(Topic.from(""), Left(ValidationError.EmptyTopic))

  test("topic applies the broker naming rules"):
    assertEquals(Topic.from("."), Left(ValidationError.ReservedTopicName(".")))
    assertEquals(Topic.from(".."), Left(ValidationError.ReservedTopicName("..")))
    assertEquals(Topic.from("a b"), Left(ValidationError.InvalidTopicCharacters("a b")))
    assertEquals(Topic.from("a/b"), Left(ValidationError.InvalidTopicCharacters("a/b")))
    assertEquals(Topic.from("a" * 250), Left(ValidationError.TopicTooLong(250)))
    assertEquals(Topic.from("a" * 249).map(_.value), Right("a" * 249))
    assertEquals(Topic.from("events.v1_final-2").map(_.value), Right("events.v1_final-2"))
    assertEquals(Topic.from("...").map(_.value), Right("..."))

  test("consumer group rejects a blank name"):
    assertEquals(ConsumerGroup.from("   "), Left(ValidationError.EmptyConsumerGroup))

  test("model types order and render portably"):
    val first  = TopicPartition(Topic.from("a").toOption.get, Partition.from(1).toOption.get)
    val second = TopicPartition(Topic.from("a").toOption.get, Partition.from(10).toOption.get)
    val third  = TopicPartition(Topic.from("b").toOption.get, Partition.from(0).toOption.get)

    assertEquals(List(third, second, first).sorted(Order[TopicPartition].toOrdering), List(first, second, third))
    assertEquals(third.show, "b-0")
    assertEquals(Offset.from(7L).toOption.get.show, "7")
    assertEquals(ValidationError.OffsetOverflow.show, "offset cannot be advanced past Long.MaxValue")

  test("topic pattern rejects an empty expression"):
    assertEquals(TopicPattern.from(""), Left(ValidationError.EmptyTopicPattern))

  test("topic pattern is anchored for backend subscriptions"):
    val pattern = TopicPattern.from("events-.*").toOption.get

    assertEquals(pattern.anchored, "^(events-.*)$")

  test("partition rejects a negative value"):
    assertEquals(Partition.from(-1), Left(ValidationError.NegativePartition(-1)))

  test("offset next detects overflow"):
    val offset = Offset.from(Long.MaxValue).toOption.get

    assertEquals(offset.next, Left(ValidationError.OffsetOverflow))

  test("headers preserve duplicates and insertion order"):
    val first   = Header("trace", Some(Chunk.array(Array[Byte](1))))
    val second  = Header("other", None)
    val third   = Header("trace", Some(Chunk.array(Array[Byte](2))))
    val headers = Headers(first, second, third)

    assertEquals(headers.values, Vector(first, second, third))
    assertEquals(headers.getAll("trace").map(_.map(_.toList)), Vector(Some(List[Byte](1)), Some(List[Byte](2))))
