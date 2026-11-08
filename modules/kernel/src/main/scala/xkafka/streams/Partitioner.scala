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

/** Chooses the partition a record is written to, from its key, value and headers and how many partitions the topic has. It must give one of them,
  * from 0 up to one less than that number; any other number stops the application.
  */
final class Partitioner[-K, -V] private[xkafka] (private[xkafka] val chosen: (Topic, K, V, Headers, Int) => Option[Int])

object Partitioner:
  def apply[K, V](choose: (K, V, Headers, Int) => Int): Partitioner[K, V] =
    new Partitioner((_, key, value, headers, count) => Some(choose(key, value, headers, count)))

  /** A partitioner that goes by the key alone, as the partitioner of a table's topic must, so that a key always lands in the same partition. */
  def byKey[K](choose: (K, Int) => Int): Partitioner[K, Any] = new Partitioner((_, key, _, _, count) => Some(choose(key, count)))

  /** A partitioner that is also told the topic, and can leave a record to Kafka's default partitioning by giving none, as Java's can. */
  private[xkafka] def orDefault[K, V](choose: (Topic, K, V, Headers, Int) => Option[Int]): Partitioner[K, V] = new Partitioner(choose)
