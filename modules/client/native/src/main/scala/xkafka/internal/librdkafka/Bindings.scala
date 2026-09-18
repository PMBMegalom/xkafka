package xkafka.internal.librdkafka

import scala.scalanative.unsafe.*

@link("rdkafka")
@extern
private[xkafka] object Bindings:
  def xkafka_version_str(): CString = extern

  def xkafka_producer_new(
      brokers: CString,
      clientId: CString,
      error: CString,
      errorSize: CSize
  ): CVoidPtr = extern

  def xkafka_producer_destroy(producer: CVoidPtr): Unit = extern

  def xkafka_headers_new(count: CSize): CVoidPtr = extern

  def xkafka_headers_destroy(headers: CVoidPtr): Unit = extern

  def xkafka_headers_add(
      headers: CVoidPtr,
      name: CString,
      value: CVoidPtr,
      valueSize: CSize,
      error: CString,
      errorSize: CSize
  ): CInt = extern

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
      error: CString,
      errorSize: CSize
  ): CVoidPtr = extern

  def xkafka_consumer_destroy(consumer: CVoidPtr): Unit = extern

  def xkafka_subscription_new(count: CSize): CVoidPtr = extern

  def xkafka_subscription_destroy(subscription: CVoidPtr): Unit = extern

  def xkafka_subscription_add(subscription: CVoidPtr, topic: CString): Unit =
    extern

  def xkafka_consumer_subscribe(
      consumer: CVoidPtr,
      subscription: CVoidPtr,
      error: CString,
      errorSize: CSize
  ): CInt = extern

  def xkafka_consumer_poll(
      consumer: CVoidPtr,
      timeoutMs: CInt,
      status: Ptr[CInt],
      error: CString,
      errorSize: CSize
  ): CVoidPtr = extern

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

  def xkafka_consumer_commit(
      consumer: CVoidPtr,
      topic: CString,
      partition: CInt,
      offset: CLongLong,
      error: CString,
      errorSize: CSize
  ): CInt = extern
