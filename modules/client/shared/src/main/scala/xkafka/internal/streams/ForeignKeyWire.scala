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

import java.nio.ByteBuffer

import fs2.Chunk

/** What a foreign-key join's two sides send each other, and how its store keys subscriptions, in the bytes Java writes. */
private[xkafka] object ForeignKeyWire:
  type Hash = (Long, Long)

  val DeleteNoPropagate: Byte      = 0
  val DeleteAndPropagate: Byte     = 1
  val PropagateNullIfMissing: Byte = 2
  val PropagateOnlyIfFound: Byte   = 3

  private val SubscriptionVersion: Byte = 1
  private val ResponseVersion: Byte     = 0
  private val NoHash: Int               = 0x80

  private def hashed(hash: Option[Hash]): Array[Byte] =
    hash.fold(Array.emptyByteArray)((first, second) => ByteBuffer.allocate(16).putLong(first).putLong(second).array)

  private def versioned(version: Byte, hash: Option[Hash]): Byte = if hash.isDefined then version else (version | NoHash).toByte

  /** A left table's row asking the other table for the row its foreign key names: the hash of its value, what to do, its key, and its partition. */
  final case class Subscription(hash: Option[Hash], instruction: Byte, primaryKey: Chunk[Byte], primaryPartition: Int):
    def encoded: Chunk[Byte] =
      Chunk.array(
        Array(versioned(SubscriptionVersion, hash), instruction) ++ hashed(hash) ++ primaryKey.toArray ++ ByteBuffer.allocate(4)
          .putInt(primaryPartition).array
      )

  object Subscription:
    /** The subscription `bytes` holds, in the only version with a partition, which Java has written since Kafka 3.4 unless told to write an older
      * one.
      */
    def decoded(bytes: Chunk[Byte]): Option[Subscription] =
      val data = bytes.toArray
      if data.length < 6 || (data(0) & 0x7f) != SubscriptionVersion then None
      else
        val hasHash = (data(0) & NoHash) == 0
        val start   = if hasHash then 18 else 2
        Option.when(data.length >= start + 4):
          val buffer = ByteBuffer.wrap(data)
          val hash   = Option.when(hasHash)((buffer.getLong(2), buffer.getLong(10)))
          Subscription(hash, data(1), Chunk.array(data.slice(start, data.length - 4)), buffer.getInt(data.length - 4))

  /** The other table answering a subscription: the hash the subscription came with, and the row found. An empty value reads as none, as in Java. */
  final case class Response(hash: Option[Hash], foreign: Option[Chunk[Byte]]):
    def encoded: Chunk[Byte] = Chunk.array(Array(versioned(ResponseVersion, hash)) ++ hashed(hash) ++ foreign.fold(Array.emptyByteArray)(_.toArray))

  object Response:
    def decoded(bytes: Chunk[Byte]): Option[Response] =
      val data = bytes.toArray
      if data.isEmpty || (data(0) & 0x7f) != ResponseVersion then None
      else
        val hasHash = (data(0) & NoHash) == 0
        val start   = if hasHash then 17 else 1
        Option.when(data.length >= start):
          val buffer = ByteBuffer.wrap(data)
          val hash   = Option.when(hasHash)((buffer.getLong(1), buffer.getLong(9)))
          Response(hash, Option.when(data.length > start)(Chunk.array(data.drop(start))))

  /** What every subscription to the foreign key `key` has its store key start with. */
  def prefixOf(key: Chunk[Byte]): Chunk[Byte] = Chunk.array(ByteBuffer.allocate(4).putInt(key.size).array ++ key.toArray)

  /** A subscription's key in the store. */
  def combinedOf(key: Chunk[Byte], primary: Chunk[Byte]): Chunk[Byte] = prefixOf(key) ++ primary

  /** The left table's key in a store key under the foreign key `key`. */
  def primaryOf(combined: Chunk[Byte], key: Chunk[Byte]): Chunk[Byte] = combined.drop(4 + key.size)
