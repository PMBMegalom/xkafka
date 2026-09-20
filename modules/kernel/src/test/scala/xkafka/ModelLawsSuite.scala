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

import cats.kernel.laws.discipline.OrderTests
import munit.DisciplineSuite
import org.scalacheck.Prop.forAll

final class ModelLawsSuite extends DisciplineSuite, ModelGenerators:
  checkAll("Order[Topic]", OrderTests[Topic].order)
  checkAll("Order[TopicPattern]", OrderTests[TopicPattern].order)
  checkAll("Order[Partition]", OrderTests[Partition].order)
  checkAll("Order[Offset]", OrderTests[Offset].order)
  checkAll("Order[ConsumerGroup]", OrderTests[ConsumerGroup].order)
  checkAll("Order[Timestamp]", OrderTests[Timestamp].order)
  checkAll("Order[TopicPartition]", OrderTests[TopicPartition].order)

  property("a validated topic round-trips through its accessor"):
    forAll: (topic: Topic) =>
      assertEquals(Topic.from(topic.value).map(_.value), Right(topic.value))

  property("offset.next is the successor when it succeeds"):
    forAll: (offset: Offset) =>
      offset.next match
        case Right(next)                          => assertEquals(next.value, offset.value + 1L)
        case Left(ValidationError.OffsetOverflow) => assertEquals(offset.value, Long.MaxValue)
        case Left(other)                          => fail(s"unexpected error: $other")
