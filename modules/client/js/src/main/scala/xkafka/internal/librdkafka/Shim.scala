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

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

import cats.syntax.all.*
import xkafka.KafkaException

/** The C shim's functions, declared to koffi from their prototypes.
  *
  * Handles of every kind are `void *`. Every out-parameter is a Node buffer, which reaches C as a plain pointer and stays where it is, so a call
  * running on another thread can still write into it. Each `async` function starts the shim function it is named after on a thread of its own and
  * reports through `done` when it has finished.
  */
private[librdkafka] final class Shim(library: KoffiLibrary):
  private def f(prototype: String): js.Dynamic = library.func(prototype)

  private val Error          = "char *error, size_t error_size, int32_t *error_code"
  private val Classified     = s"$Error, int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort"
  private val Done           = s"${Shim.DoneType} *done, int64_t token"
  private val Properties     = "const char **property_names, const char **property_values, size_t property_count"
  private val PartitionNames = "const char **topics, const int32_t *partitions, size_t count"
  private val Starts         =
    "const char **start_topics, const int32_t *start_partitions, const int64_t *start_offsets, const int32_t *start_leader_epochs, size_t start_count"

  val versionStr  = f("const char *xkafka_version_str(void)")
  val shimId      = f("const char *xkafka_shim_id(void)")
  val errorString = f("const char *xkafka_error_string(int32_t code)")

  val producerNew     = f(s"void *xkafka_producer_new(const char *brokers, const char *client_id, $Properties, $Error)")
  val producerDestroy = f(s"int xkafka_async_producer_destroy(void *producer, int timeout_ms, $Done)")
  val consumerNew     =
    f(
      s"void *xkafka_consumer_new(const char *brokers, const char *client_id, const char *group_id, const char *auto_offset_reset, $Properties, $Error)"
    )
  val consumerDestroy = f(s"int xkafka_async_consumer_destroy(void *consumer, $Done)")
  val adminNew        = f(s"void *xkafka_admin_new(const char *brokers, const char *client_id, $Properties, $Error)")
  val adminDestroy    = f(s"int xkafka_async_admin_destroy(void *client, $Done)")

  val headersNew     = f("void *xkafka_headers_new(size_t count)")
  val headersDestroy = f("void xkafka_headers_destroy(void *headers)")
  val headersAdd     = f(s"int xkafka_headers_add(void *headers, const char *name, const void *value, size_t value_size, $Error)")

  val batchNew = f("void *xkafka_batch_new(size_t capacity)")
  val batchAdd =
    f(
      "int xkafka_batch_add(void *producer, void *batch, const char *topic, int32_t partition, int64_t timestamp, const void *key, size_t key_size, " +
        s"const void *value, size_t value_size, void *headers, $Error)"
    )
  val producerPoll     = f(s"int xkafka_async_producer_poll(void *producer, int timeout_ms, $Done)")
  val producerFlush    = f(s"int xkafka_async_producer_flush(void *producer, int timeout_ms, $Done)")
  val batchPending     = f("size_t xkafka_batch_pending(void *batch)")
  val batchErrorAt     = f(s"int xkafka_batch_error_at(void *producer, void *batch, size_t index, $Error)")
  val batchCount       = f("size_t xkafka_batch_count(void *batch)")
  val batchPartitionAt = f("int32_t xkafka_batch_partition_at(void *batch, size_t index)")
  val batchOffsetAt    = f("int64_t xkafka_batch_offset_at(void *batch, size_t index)")
  val batchTimestampAt = f("int64_t xkafka_batch_timestamp_at(void *batch, size_t index)")
  val batchFree        = f("void xkafka_batch_free(void *batch)")

  val initTransactions  = f(s"int xkafka_async_producer_init_transactions(void *producer, int timeout_ms, $Classified, int32_t *result, $Done)")
  val beginTransaction  = f(s"int xkafka_producer_begin_transaction(void *producer, $Classified)")
  val commitTransaction = f(s"int xkafka_async_producer_commit_transaction(void *producer, int timeout_ms, $Classified, int32_t *result, $Done)")
  val abortTransaction  = f(s"int xkafka_async_producer_abort_transaction(void *producer, int timeout_ms, $Classified, int32_t *result, $Done)")
  val sendOffsets       =
    f(
      "int xkafka_async_producer_send_offsets(void *producer, void *metadata, const char **topics, const int32_t *partitions, const int64_t *offsets, " +
        s"const int32_t *leader_epochs, const char **offset_metadata, size_t count, int timeout_ms, $Classified, int32_t *result, $Done)"
    )
  val groupMetadata        = f("void *xkafka_consumer_group_metadata(void *consumer)")
  val groupMetadataDestroy = f("void xkafka_consumer_group_metadata_destroy(void *metadata)")

  val takeRebalance       = f("void *xkafka_consumer_take_rebalance(void *consumer)")
  val rebalanceKind       = f("int xkafka_rebalance_kind(void *event)")
  val rebalancePartitions = f("void *xkafka_rebalance_partitions(void *event)")
  val cooperative         = f("int xkafka_consumer_cooperative(void *consumer)")
  val applyRebalance      = f(s"void xkafka_consumer_apply_rebalance(void *consumer, void *event, $Starts)")

  val subscriptionNew     = f("void *xkafka_subscription_new(size_t count)")
  val subscriptionDestroy = f("void xkafka_subscription_destroy(void *subscription)")
  val subscriptionAdd     = f("void xkafka_subscription_add(void *subscription, const char *topic)")
  val subscribe           = f(s"int xkafka_consumer_subscribe(void *consumer, void *subscription, $Error)")
  val assign              = f(s"int xkafka_consumer_assign(void *consumer, $PartitionNames, $Starts, $Error)")
  val pause               = f(s"int xkafka_consumer_pause(void *consumer, $PartitionNames, $Error)")
  val resume              = f(s"int xkafka_consumer_resume(void *consumer, $PartitionNames, $Error)")

  val poll             = f(s"int xkafka_async_consumer_poll(void *consumer, int timeout_ms, int *status, $Error, void **result, $Done)")
  val messageDestroy   = f("void xkafka_message_destroy(void *message)")
  val messageTopic     = f("const char *xkafka_message_topic(void *message)")
  val messagePartition = f("int32_t xkafka_message_partition(void *message)")
  val messageOffset    = f("int64_t xkafka_message_offset(void *message)")
  val messageTimestamp = f("int64_t xkafka_message_timestamp(void *message)")
  val messageTimeType  = f("int32_t xkafka_message_timestamp_type(void *message)")
  val messageEpoch     = f("int32_t xkafka_message_leader_epoch(void *message)")
  val messageKey       = f("void *xkafka_message_key(void *message)")
  val messageKeySize   = f("size_t xkafka_message_key_size(void *message)")
  val messageValue     = f("void *xkafka_message_value(void *message)")
  val messageValueSize = f("size_t xkafka_message_value_size(void *message)")
  val messageHeaderCnt = f("size_t xkafka_message_header_count(void *message)")
  val messageHeaderAt  =
    f(
      "int xkafka_message_header_at(void *message, size_t index, _Out_ const char **name, _Out_ void **value, _Out_ size_t *value_size, _Out_ int *has_value)"
    )

  val assignment            = f(s"void *xkafka_consumer_assignment(void *consumer, $Error)")
  val assignmentDestroy     = f("void xkafka_assignment_destroy(void *assignment)")
  val assignmentCount       = f("size_t xkafka_assignment_count(void *assignment)")
  val assignmentTopicAt     = f("const char *xkafka_assignment_topic_at(void *assignment, size_t index)")
  val assignmentPartitionAt = f("int32_t xkafka_assignment_partition_at(void *assignment, size_t index)")

  val commit =
    f(
      "int xkafka_async_consumer_commit(void *consumer, const char **topics, const int32_t *partitions, const int64_t *offsets, " +
        s"const int32_t *leader_epochs, const char **metadata, size_t count, int timeout_ms, $Error, int32_t *result, $Done)"
    )
  val committed           = f(s"int xkafka_async_consumer_committed(void *consumer, $PartitionNames, int timeout_ms, $Error, void **result, $Done)")
  val committedOffsetAt   = f("int64_t xkafka_committed_offset_at(void *committed, size_t index)")
  val committedEpochAt    = f("int32_t xkafka_committed_leader_epoch_at(void *committed, size_t index)")
  val committedMetadataAt = f("void *xkafka_committed_metadata_at(void *committed, size_t index, _Out_ size_t *size)")
  val committedDestroy    = f("void xkafka_committed_destroy(void *committed)")
  val position            = f(s"int xkafka_consumer_position(void *consumer, $PartitionNames, int64_t *offsets, $Error)")
  val watermarks          =
    f(
      "int xkafka_async_consumer_watermark_offsets(void *consumer, const char *topic, int32_t partition, int64_t *low, int64_t *high, " +
        s"int timeout_ms, $Error, int32_t *result, $Done)"
    )
  val offsetsForTimes =
    f(
      "int xkafka_async_consumer_offsets_for_times(void *consumer, const char **topics, const int32_t *partitions, const int64_t *timestamps, " +
        s"size_t count, int64_t *offsets, int timeout_ms, $Error, int32_t *result, $Done)"
    )
  val seek =
    f(
      s"int xkafka_async_consumer_seek(void *consumer, const char *topic, int32_t partition, int64_t offset, int timeout_ms, $Error, int32_t *result, $Done)"
    )

  val metadata               = f(s"int xkafka_async_consumer_metadata(void *client, const char *topic, int timeout_ms, $Error, void **result, $Done)")
  val metadataDestroy        = f("void xkafka_metadata_destroy(void *metadata)")
  val metadataTopicCount     = f("size_t xkafka_metadata_topic_count(void *metadata)")
  val metadataTopicAt        = f("const char *xkafka_metadata_topic_at(void *metadata, size_t topic_index)")
  val metadataPartitionCount = f("size_t xkafka_metadata_partition_count_at(void *metadata, size_t topic_index)")
  val metadataPartitionAt    = f("int32_t xkafka_metadata_partition_at(void *metadata, size_t topic_index, size_t partition_index)")

  val createTopics =
    f(
      "int xkafka_async_admin_create_topics(void *client, const char **names, const int32_t *partitions, const int32_t *replication, " +
        s"const char **config_names, const char **config_values, const size_t *config_counts, size_t count, int timeout_ms, $Error, int32_t *result, $Done)"
    )
  val deleteTopics =
    f(s"int xkafka_async_admin_delete_topics(void *client, const char **names, size_t count, int timeout_ms, $Error, int32_t *result, $Done)")
  val createPartitions =
    f(s"int xkafka_async_admin_create_partitions(void *client, const char *topic, int32_t count, int timeout_ms, $Error, int32_t *result, $Done)")
  val deleteRecords =
    f(
      "int xkafka_async_admin_delete_records(void *client, const char **topics, const int32_t *partitions, const int64_t *offsets, size_t count, " +
        s"int timeout_ms, int32_t *codes, int64_t *low_watermarks, $Error, int32_t *result, $Done)"
    )

  val describeConfigs =
    f(s"int xkafka_async_admin_describe_topic_configs(void *client, const char **names, size_t count, int timeout_ms, $Error, void **result, $Done)")
  val configsResourceCount  = f("size_t xkafka_configs_resource_count(void *configs)")
  val configsResourceName   = f("const char *xkafka_configs_resource_name(void *configs, size_t resource_index)")
  val configsResourceError  = f("int32_t xkafka_configs_resource_error(void *configs, size_t resource_index)")
  val configsResourceString = f("const char *xkafka_configs_resource_error_string(void *configs, size_t resource_index)")
  val configsEntryCount     = f("size_t xkafka_configs_entry_count(void *configs, size_t resource_index)")
  val configsEntryName      = f("const char *xkafka_configs_entry_name(void *configs, size_t resource_index, size_t entry_index)")
  val configsEntryValue     = f("const char *xkafka_configs_entry_value(void *configs, size_t resource_index, size_t entry_index)")
  val configsEntrySource    = f("int32_t xkafka_configs_entry_source(void *configs, size_t resource_index, size_t entry_index)")
  val configsEntrySensitive = f("int32_t xkafka_configs_entry_is_sensitive(void *configs, size_t resource_index, size_t entry_index)")
  val configsEntryReadOnly  = f("int32_t xkafka_configs_entry_is_read_only(void *configs, size_t resource_index, size_t entry_index)")
  val configsDestroy        = f("void xkafka_configs_destroy(void *configs)")

  val describeCluster    = f(s"int xkafka_async_admin_describe_cluster(void *client, int timeout_ms, $Error, void **result, $Done)")
  val clusterId          = f("const char *xkafka_cluster_id(void *cluster)")
  val clusterController  = f("const void *xkafka_cluster_controller(void *cluster)")
  val clusterBrokerCount = f("size_t xkafka_cluster_broker_count(void *cluster)")
  val clusterBrokerAt    = f("const void *xkafka_cluster_broker_at(void *cluster, size_t index)")
  val clusterDestroy     = f("void xkafka_cluster_destroy(void *cluster)")
  val nodeId             = f("int32_t xkafka_node_id(const void *node)")
  val nodeHost           = f("const char *xkafka_node_host(const void *node)")
  val nodePort           = f("int32_t xkafka_node_port(const void *node)")
  val nodeRack           = f("const char *xkafka_node_rack(const void *node)")

  val describeTopics =
    f(s"int xkafka_async_admin_describe_topics(void *client, const char **names, size_t count, int timeout_ms, $Error, void **result, $Done)")
  val topicsCount          = f("size_t xkafka_topics_count(void *topics)")
  val topicName            = f("const char *xkafka_topic_name(void *topics, size_t topic)")
  val topicError           = f("int32_t xkafka_topic_error(void *topics, size_t topic)")
  val topicErrorString     = f("const char *xkafka_topic_error_string(void *topics, size_t topic)")
  val topicIdMost          = f("int64_t xkafka_topic_id_most(void *topics, size_t topic)")
  val topicIdLeast         = f("int64_t xkafka_topic_id_least(void *topics, size_t topic)")
  val topicInternal        = f("int xkafka_topic_internal(void *topics, size_t topic)")
  val topicPartitionCount  = f("size_t xkafka_topic_partition_count(void *topics, size_t topic)")
  val topicPartitionAt     = f("int32_t xkafka_topic_partition_at(void *topics, size_t topic, size_t partition)")
  val topicPartitionLeader = f("int32_t xkafka_topic_partition_leader(void *topics, size_t topic, size_t partition)")
  val topicReplicaCount    = f("size_t xkafka_topic_replica_count(void *topics, size_t topic, size_t partition)")
  val topicReplicaAt       = f("int32_t xkafka_topic_replica_at(void *topics, size_t topic, size_t partition, size_t replica)")
  val topicInSyncCount     = f("size_t xkafka_topic_in_sync_count(void *topics, size_t topic, size_t partition)")
  val topicInSyncAt        = f("int32_t xkafka_topic_in_sync_at(void *topics, size_t topic, size_t partition, size_t replica)")
  val topicsDestroy        = f("void xkafka_topics_destroy(void *topics)")

  val listGroups    = f(s"int xkafka_async_admin_list_groups(void *client, int timeout_ms, $Error, void **result, $Done)")
  val groupsCount   = f("size_t xkafka_groups_count(void *groups)")
  val groupsIdAt    = f("const char *xkafka_groups_id_at(void *groups, size_t index)")
  val groupsStateAt = f("int32_t xkafka_groups_state_at(void *groups, size_t index)")
  val groupsDestroy = f("void xkafka_groups_destroy(void *groups)")

  val describeGroups =
    f(s"int xkafka_async_admin_describe_groups(void *client, const char **groups, size_t count, int timeout_ms, $Error, void **result, $Done)")
  val descriptionsCount          = f("size_t xkafka_descriptions_count(void *descriptions)")
  val descriptionGroup           = f("const char *xkafka_description_group(void *descriptions, size_t group)")
  val descriptionError           = f("int32_t xkafka_description_error(void *descriptions, size_t group)")
  val descriptionErrorString     = f("const char *xkafka_description_error_string(void *descriptions, size_t group)")
  val descriptionSimple          = f("int xkafka_description_simple(void *descriptions, size_t group)")
  val descriptionState           = f("int32_t xkafka_description_state(void *descriptions, size_t group)")
  val descriptionAssignor        = f("const char *xkafka_description_assignor(void *descriptions, size_t group)")
  val descriptionCoordinatorId   = f("int32_t xkafka_description_coordinator_id(void *descriptions, size_t group)")
  val descriptionCoordinatorHost = f("const char *xkafka_description_coordinator_host(void *descriptions, size_t group)")
  val descriptionCoordinatorPort = f("int32_t xkafka_description_coordinator_port(void *descriptions, size_t group)")
  val descriptionCoordinatorRack = f("const char *xkafka_description_coordinator_rack(void *descriptions, size_t group)")
  val descriptionMemberCount     = f("size_t xkafka_description_member_count(void *descriptions, size_t group)")
  val memberId                   = f("const char *xkafka_member_id(void *descriptions, size_t group, size_t member)")
  val memberInstanceId           = f("const char *xkafka_member_instance_id(void *descriptions, size_t group, size_t member)")
  val memberClientId             = f("const char *xkafka_member_client_id(void *descriptions, size_t group, size_t member)")
  val memberHost                 = f("const char *xkafka_member_host(void *descriptions, size_t group, size_t member)")
  val memberPartitionCount       = f("size_t xkafka_member_partition_count(void *descriptions, size_t group, size_t member)")
  val memberTopicAt              = f("const char *xkafka_member_topic_at(void *descriptions, size_t group, size_t member, size_t index)")
  val memberPartitionAt          = f("int32_t xkafka_member_partition_at(void *descriptions, size_t group, size_t member, size_t index)")
  val descriptionsDestroy        = f("void xkafka_descriptions_destroy(void *descriptions)")

  val listGroupOffsets =
    f(s"int xkafka_async_admin_list_group_offsets(void *client, const char *group, $PartitionNames, int timeout_ms, $Error, void **result, $Done)")
  val alterGroupOffsets =
    f(
      "int xkafka_async_admin_alter_group_offsets(void *client, const char *group, const char **topics, const int32_t *partitions, " +
        "const int64_t *offsets, const int32_t *leader_epochs, const char **metadata, size_t count, int timeout_ms, int32_t *codes, " +
        s"$Error, int32_t *result, $Done)"
    )
  val deleteGroupOffsets =
    f(
      s"int xkafka_async_admin_delete_group_offsets(void *client, const char *group, $PartitionNames, int timeout_ms, int32_t *codes, $Error, " +
        s"int32_t *result, $Done)"
    )
  val deleteGroups =
    f(
      s"int xkafka_async_admin_delete_groups(void *client, const char **groups, size_t count, int timeout_ms, int32_t *codes, $Error, " +
        s"int32_t *result, $Done)"
    )
  val alterTopicConfigs =
    f(
      "int xkafka_async_admin_alter_topic_configs(void *client, const char **topics, const char **names, const char **values, " +
        s"const int32_t *operations, size_t count, int timeout_ms, $Error, void **result, $Done)"
    )
  val electPreferredLeaders =
    f(
      s"int xkafka_async_admin_elect_preferred_leaders(void *client, $PartitionNames, int timeout_ms, int32_t *codes, $Error, int32_t *result, $Done)"
    )

private[librdkafka] object Shim:
  /** The callback type every `async` function reports through. */
  val DoneType = "xkafka_done_t"

  /** The shim, loaded once for the whole process, or why it could not be.
    *
    * The library must report the identity this build was made with, since the functions it offers are declared from that build's shim.
    */
  lazy val loaded: Either[Throwable, Shim] =
    for
      path <-
        Option(PrebuiltLibrary.library)
          .toRight(new KafkaException.BackendFailure(Option(PrebuiltLibrary.error).getOrElse("@xkafka/librdkafka found no library")))
      shim <-
        Either.catchNonFatal { Koffi.proto(s"void $DoneType(int64_t token)"): Unit; new Shim(Koffi.load(path)) }
          .leftMap(error => new KafkaException.BackendFailure(s"could not load the xkafka librdkafka library from $path: ${error.getMessage}"))
      reported = shim.shimId().asInstanceOf[String]
      _ <-
        Either.cond(
          reported == LibraryIdentity.shim,
          (),
          new KafkaException.BackendFailure(s"the xkafka librdkafka library at $path is $reported, but this xkafka needs ${LibraryIdentity.shim}")
        )
    yield shim

/** The prebuilt library @xkafka/librdkafka found for this platform, or why it found none. */
@js.native
@JSImport("@xkafka/librdkafka", JSImport.Default)
private[librdkafka] object PrebuiltLibrary extends js.Object:
  val library: String | Null = js.native
  val error: String | Null   = js.native

/** The calls running on threads of their own, by token, and the one callback they all report through.
  *
  * koffi runs the callback on the event loop. A call waiting on its own thread does not keep Node running by itself, so a timer does while any call
  * is outstanding.
  */
private[librdkafka] object Completions:
  private val waiting   = scala.collection.mutable.LongMap.empty[() => Unit]
  private val tokens    = new java.util.concurrent.atomic.AtomicLong(0L)
  private val keepAlive = new java.util.concurrent.atomic.AtomicReference[Option[scala.scalajs.js.timers.SetIntervalHandle]](None)

  /** The registered callback, passed to every `async` function. */
  lazy val done: js.Any =
    val report: js.Function1[js.Any, Unit] =
      token => complete(if js.typeOf(token) == "bigint" then token.toString.toLong else token.asInstanceOf[Double].toLong)
    Koffi.register(report, s"${Shim.DoneType} *")

  /** A token for a call about to start, which runs `onDone` once the call reports. */
  def expect(onDone: () => Unit): Long =
    val token = tokens.incrementAndGet()
    waiting.update(token, onDone)
    if keepAlive.get.isEmpty then keepAlive.set(Some(scala.scalajs.js.timers.setInterval(1.0e9)(())))
    token

  /** Drops a call that never started. */
  def forget(token: Long): Unit =
    waiting.remove(token): Unit
    settle()

  private def complete(token: Long): Unit =
    val onDone = waiting.remove(token)
    settle()
    onDone.foreach(_())

  private def settle(): Unit =
    if waiting.isEmpty then
      keepAlive.get.foreach(scala.scalajs.js.timers.clearInterval)
      keepAlive.set(None)
