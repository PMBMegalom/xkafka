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

package xkafka.internal.librdkafka

import scala.scalanative.unsafe.*

@link("rdkafka") @extern
private[xkafka] object Bindings:
  def xkafka_version_str(): CString = extern

  def xkafka_producer_new(
      brokers: CString,
      clientId: CString,
      propertyNames: Ptr[CString],
      propertyValues: Ptr[CString],
      propertyCount: CSize,
      error: CString,
      errorSize: CSize
  ): CVoidPtr = extern

  def xkafka_producer_destroy(producer: CVoidPtr): Unit = extern

  def xkafka_headers_new(count: CSize): CVoidPtr = extern

  def xkafka_headers_destroy(headers: CVoidPtr): Unit = extern

  def xkafka_headers_add(headers: CVoidPtr, name: CString, value: CVoidPtr, valueSize: CSize, error: CString, errorSize: CSize): CInt = extern

  def xkafka_producer_send(
      producer: CVoidPtr,
      topic: CString,
      partition: CInt,
      timestamp: CLongLong,
      key: CVoidPtr,
      keySize: CSize,
      value: CVoidPtr,
      valueSize: CSize,
      headers: CVoidPtr,
      resultPartition: Ptr[CInt],
      resultOffset: Ptr[CLongLong],
      resultTimestamp: Ptr[CLongLong],
      error: CString,
      errorSize: CSize
  ): CInt = extern

  def xkafka_consumer_new(
      brokers: CString,
      clientId: CString,
      groupId: CString,
      autoOffsetReset: CString,
      propertyNames: Ptr[CString],
      propertyValues: Ptr[CString],
      propertyCount: CSize,
      error: CString,
      errorSize: CSize
  ): CVoidPtr = extern

  def xkafka_consumer_destroy(consumer: CVoidPtr): Unit = extern

  def xkafka_subscription_new(count: CSize): CVoidPtr = extern

  def xkafka_subscription_destroy(subscription: CVoidPtr): Unit = extern

  def xkafka_subscription_add(subscription: CVoidPtr, topic: CString): Unit = extern

  def xkafka_consumer_subscribe(consumer: CVoidPtr, subscription: CVoidPtr, error: CString, errorSize: CSize): CInt = extern

  def xkafka_consumer_poll(consumer: CVoidPtr, timeoutMs: CInt, status: Ptr[CInt], error: CString, errorSize: CSize): CVoidPtr = extern

  def xkafka_message_destroy(message: CVoidPtr): Unit = extern

  def xkafka_message_topic(message: CVoidPtr): CString = extern

  def xkafka_message_partition(message: CVoidPtr): CInt = extern

  def xkafka_message_offset(message: CVoidPtr): CLongLong = extern

  def xkafka_message_timestamp(message: CVoidPtr): CLongLong = extern

  def xkafka_message_key(message: CVoidPtr): CVoidPtr = extern

  def xkafka_message_key_size(message: CVoidPtr): CSize = extern

  def xkafka_message_value(message: CVoidPtr): CVoidPtr = extern

  def xkafka_message_value_size(message: CVoidPtr): CSize = extern

  def xkafka_message_header_count(message: CVoidPtr): CSize = extern

  def xkafka_message_header_at(
      message: CVoidPtr,
      index: CSize,
      name: Ptr[CString],
      value: Ptr[Ptr[Byte]],
      valueSize: Ptr[CSize],
      hasValue: Ptr[CInt]
  ): CInt = extern

  def xkafka_consumer_assignment(consumer: CVoidPtr, error: CString, errorSize: CSize): CVoidPtr = extern

  def xkafka_assignment_destroy(assignment: CVoidPtr): Unit = extern

  def xkafka_assignment_count(assignment: CVoidPtr): CSize = extern

  def xkafka_assignment_topic_at(assignment: CVoidPtr, index: CSize): CString = extern

  def xkafka_assignment_partition_at(assignment: CVoidPtr, index: CSize): CInt = extern

  def xkafka_consumer_committed(
      consumer: CVoidPtr,
      topics: Ptr[CString],
      partitions: Ptr[CInt],
      count: CSize,
      offsets: Ptr[CLongLong],
      error: CString,
      errorSize: CSize
  ): CInt = extern

  def xkafka_consumer_watermark_offsets(
      consumer: CVoidPtr,
      topic: CString,
      partition: CInt,
      low: Ptr[CLongLong],
      high: Ptr[CLongLong],
      error: CString,
      errorSize: CSize
  ): CInt = extern

  def xkafka_consumer_offsets_for_times(
      consumer: CVoidPtr,
      topics: Ptr[CString],
      partitions: Ptr[CInt],
      timestamps: Ptr[CLongLong],
      count: CSize,
      offsets: Ptr[CLongLong],
      error: CString,
      errorSize: CSize
  ): CInt = extern

  def xkafka_consumer_metadata(consumer: CVoidPtr, topic: CString, error: CString, errorSize: CSize): CVoidPtr = extern

  def xkafka_metadata_destroy(metadata: CVoidPtr): Unit = extern

  def xkafka_metadata_topic_count(metadata: CVoidPtr): CSize = extern

  def xkafka_metadata_topic_at(metadata: CVoidPtr, topicIndex: CSize): CString = extern

  def xkafka_metadata_partition_count_at(metadata: CVoidPtr, topicIndex: CSize): CSize = extern

  def xkafka_metadata_partition_at(metadata: CVoidPtr, topicIndex: CSize, partitionIndex: CSize): CInt = extern

  def xkafka_consumer_seek(consumer: CVoidPtr, topic: CString, partition: CInt, offset: CLongLong, error: CString, errorSize: CSize): CInt = extern

  def xkafka_consumer_commit(
      consumer: CVoidPtr,
      topics: Ptr[CString],
      partitions: Ptr[CInt],
      offsets: Ptr[CLongLong],
      count: CSize,
      error: CString,
      errorSize: CSize
  ): CInt = extern
