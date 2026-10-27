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
package streams

import scodec.bits.ByteVector

/** The partition time a task keeps in its committed offsets' metadata, the largest record timestamp it has processed on the partition, written as
  * Java Streams writes it (`TopicPartitionMetadata`): base64 of a version byte, the time as eight big-endian bytes, then processor metadata.
  */
private[xkafka] object PartitionTime:
  private val Version: Byte      = 2
  private val FirstVersion: Byte = 1

  /** The metadata for `time`, with no processor metadata: only windowed aggregations that emit final results write any. */
  def encode(time: Long): String = (ByteVector(Version) ++ ByteVector.fromLong(time)).toBase64

  /** The time in metadata Java or this engine wrote, or none where there is none or it cannot be read, which Java also takes as an unknown time. A
    * version 2 entry's processor metadata, after the time, is skipped.
    */
  def decode(metadata: String): Option[Long] =
    ByteVector.fromBase64(metadata).filter(_.size >= 9).flatMap: bytes =>
      Option.when(bytes(0) == Version || bytes(0) == FirstVersion)(bytes.slice(1, 9).toLong()).filter(_ >= 0)
