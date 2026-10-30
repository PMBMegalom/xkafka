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
package internal.protocol

import cats.syntax.all.*
import scodec.bits.ByteVector
import Wire.*

/** A request the group connection makes, with the versions this codec writes it in.
  *
  * Each request is written the same way in every version listed for it, which is what lets the connection take the highest one the broker also reads.
  * Answers are read as the version they answer.
  */
private[xkafka] enum Api(val key: Short, val lowest: Short, val highest: Short, val flexible: Boolean) derives CanEqual:
  case OffsetCommit          extends Api(8, 9, 9, true)
  case OffsetFetch           extends Api(9, 9, 9, true)
  case FindCoordinator       extends Api(10, 4, 6, true)
  case SaslHandshake         extends Api(17, 1, 1, false)
  case ApiVersions           extends Api(18, 3, 4, true)
  case SaslAuthenticate      extends Api(36, 2, 2, true)
  case StreamsGroupHeartbeat extends Api(88, 0, 1, true)

private[xkafka] object Api:
  /** The highest version of each request that both this codec and the broker read, leaving out the requests they share no version of. */
  def negotiate(broker: List[ApiVersion]): Map[Api, Short] = Api.values.toList.flatMap(api => chosen(api, broker).map(api -> _)).toMap

  private def chosen(api: Api, broker: List[ApiVersion]): Option[Short] =
    broker.find(_.key == api.key).flatMap: offered =>
      val version = math.min(offered.max, api.highest).toShort
      Option.when(version >= offered.min && version >= api.lowest)(version)

private[xkafka] final case class ApiVersion(key: Short, min: Short, max: Short)

/** A request and how to read the broker's answer to it. */
private[xkafka] sealed trait Request[R]:
  def api: Api
  def body: ByteVector
  def response(version: Short): Read[R]

private[xkafka] final case class ApiVersionsRequest(clientSoftwareName: String, clientSoftwareVersion: String) extends Request[ApiVersionsResponse]:
  val api: Api                                            = Api.ApiVersions
  def body: ByteVector                                    = compactString(clientSoftwareName) ++ compactString(clientSoftwareVersion) ++ noTags
  def response(version: Short): Read[ApiVersionsResponse] =
    // A broker that does not read the version asked for answers in version 0, which says so and lists what it reads.
    readInt16.flatMap: errorCode =>
      if errorCode == UnsupportedVersion then
        readArray((readInt16, readInt16, readInt16).mapN(ApiVersion.apply)).map(ApiVersionsResponse(errorCode, _))
      else
        (readCompactArray((readInt16, readInt16, readInt16).mapN(ApiVersion.apply) <* skipTags) <* readInt32 <* skipTags)
          .map(ApiVersionsResponse(errorCode, _))

  private val UnsupportedVersion: Short = 35

private[xkafka] final case class ApiVersionsResponse(errorCode: Short, apis: List[ApiVersion])

private[xkafka] final case class FindCoordinatorRequest(keyType: Byte, keys: List[String]) extends Request[List[Coordinator]]:
  val api: Api                                          = Api.FindCoordinator
  def body: ByteVector                                  = int8(keyType.toInt) ++ compactArray(keys)(compactString) ++ noTags
  def response(version: Short): Read[List[Coordinator]] =
    val coordinator =
      (readCompactString, readInt32, readCompactString, readInt32, readInt16, readCompactNullableString).mapN(Coordinator.apply) <* skipTags
    readInt32 *> readCompactArray(coordinator) <* skipTags

private[xkafka] object FindCoordinatorRequest:
  val GroupKey: Byte = 0

private[xkafka] final case class Coordinator(key: String, nodeId: Int, host: String, port: Int, errorCode: Short, errorMessage: Option[String])

private[xkafka] final case class SaslHandshakeRequest(mechanism: String) extends Request[SaslHandshakeResponse]:
  val api: Api                                              = Api.SaslHandshake
  def body: ByteVector                                      = string(mechanism)
  def response(version: Short): Read[SaslHandshakeResponse] = (readInt16, readArray(readString)).mapN(SaslHandshakeResponse.apply)

private[xkafka] final case class SaslHandshakeResponse(errorCode: Short, mechanisms: List[String])

private[xkafka] final case class SaslAuthenticateRequest(authBytes: ByteVector) extends Request[SaslAuthenticateResponse]:
  val api: Api                                                 = Api.SaslAuthenticate
  def body: ByteVector                                         = compactBytes(authBytes) ++ noTags
  def response(version: Short): Read[SaslAuthenticateResponse] =
    (readInt16, readCompactNullableString, readCompactBytes, readInt64).mapN(SaslAuthenticateResponse.apply) <* skipTags

private[xkafka] final case class SaslAuthenticateResponse(
    errorCode: Short,
    errorMessage: Option[String],
    authBytes: ByteVector,
    sessionLifetimeMs: Long
)

/** A member's heartbeat to the coordinator of a streams group (KIP-1071), which joins, reports the tasks it owns, and leaves.
  *
  * Fields left empty are the ones that did not change since the last heartbeat.
  */
private[xkafka] final case class StreamsGroupHeartbeatRequest(
    groupId: String,
    memberId: String,
    memberEpoch: Int,
    endpointInformationEpoch: Int,
    instanceId: Option[String],
    rackId: Option[String],
    rebalanceTimeoutMs: Int,
    topology: Option[StreamsGroup.Topology],
    activeTasks: Option[List[StreamsGroup.Tasks]],
    standbyTasks: Option[List[StreamsGroup.Tasks]],
    warmupTasks: Option[List[StreamsGroup.Tasks]],
    processId: Option[String],
    userEndpoint: Option[StreamsGroup.Endpoint],
    clientTags: Option[List[(String, String)]],
    taskOffsets: Option[List[StreamsGroup.TaskOffset]],
    taskEndOffsets: Option[List[StreamsGroup.TaskOffset]],
    shutdownApplication: Boolean
) extends Request[StreamsGroupHeartbeatResponse]:
  import StreamsGroup.*
  val api: Api         = Api.StreamsGroupHeartbeat
  def body: ByteVector =
    compactString(groupId) ++ compactString(memberId) ++ int32(memberEpoch) ++ int32(endpointInformationEpoch) ++ compactNullableString(instanceId) ++
      compactNullableString(rackId) ++ int32(rebalanceTimeoutMs) ++ nullableStruct(topology)(writeTopology) ++
      compactNullableArray(activeTasks)(writeTasks) ++ compactNullableArray(standbyTasks)(writeTasks) ++
      compactNullableArray(warmupTasks)(writeTasks) ++ compactNullableString(processId) ++ nullableStruct(userEndpoint)(writeEndpoint) ++
      compactNullableArray(clientTags)(writeKeyValue) ++ compactNullableArray(taskOffsets)(writeTaskOffset) ++
      compactNullableArray(taskEndOffsets)(writeTaskOffset) ++ boolean(shutdownApplication) ++ noTags

  /** Version 1 replaces the 32-bit recovery lag of version 0 with a 64-bit one, and adds whether the broker wants the topology described. */
  def response(version: Short): Read[StreamsGroupHeartbeatResponse] =
    for
      throttleTimeMs           <- readInt32
      errorCode                <- readInt16
      errorMessage             <- readCompactNullableString
      memberId                 <- readCompactString
      memberEpoch              <- readInt32
      heartbeatIntervalMs      <- readInt32
      legacyRecoveryLag        <- if version == 0 then readInt32.map(_.toLong.some) else none[Long].pure[Read]
      taskOffsetIntervalMs     <- readInt32
      recoveryLag              <- legacyRecoveryLag.fold(readInt64)(_.pure[Read])
      statuses                 <- readCompactNullableArray((readInt8, readCompactString).mapN(Status.apply) <* skipTags)
      activeTasks              <- readCompactNullableArray(readTasks)
      standbyTasks             <- readCompactNullableArray(readTasks)
      warmupTasks              <- readCompactNullableArray(readTasks)
      describeTopology         <- if version == 0 then false.pure[Read] else readBoolean
      endpointInformationEpoch <- readInt32
      partitionsByEndpoint     <- readCompactNullableArray(readEndpointPartitions)
      _                        <- skipTags
    yield StreamsGroupHeartbeatResponse(
      throttleTimeMs,
      errorCode,
      errorMessage,
      memberId,
      memberEpoch,
      heartbeatIntervalMs,
      recoveryLag,
      taskOffsetIntervalMs,
      statuses,
      activeTasks,
      standbyTasks,
      warmupTasks,
      describeTopology,
      endpointInformationEpoch,
      partitionsByEndpoint
    )

/** The coordinator's answer to a heartbeat. Fields left empty are the ones that did not change since the last answer. */
private[xkafka] final case class StreamsGroupHeartbeatResponse(
    throttleTimeMs: Int,
    errorCode: Short,
    errorMessage: Option[String],
    memberId: String,
    memberEpoch: Int,
    heartbeatIntervalMs: Int,
    acceptableRecoveryLag: Long,
    taskOffsetIntervalMs: Int,
    statuses: Option[List[StreamsGroup.Status]],
    activeTasks: Option[List[StreamsGroup.Tasks]],
    standbyTasks: Option[List[StreamsGroup.Tasks]],
    warmupTasks: Option[List[StreamsGroup.Tasks]],
    topologyDescriptionRequired: Boolean,
    endpointInformationEpoch: Int,
    partitionsByUserEndpoint: Option[List[StreamsGroup.EndpointPartitions]]
)

/** Offsets a member commits for its group under its id and epoch (KIP-848), which the coordinator takes only from a member at its current epoch. */
private[xkafka] final case class OffsetCommitRequest(
    groupId: String,
    memberId: String,
    memberEpoch: Int,
    instanceId: Option[String],
    topics: List[OffsetCommitRequest.Topic]
) extends Request[List[OffsetCommitRequest.Answer]]:
  import OffsetCommitRequest.*
  val api: Api         = Api.OffsetCommit
  def body: ByteVector =
    compactString(groupId) ++ int32(memberEpoch) ++ compactString(memberId) ++ compactNullableString(instanceId) ++
      compactArray(topics)(writeTopic) ++ noTags

  def response(version: Short): Read[List[Answer]] =
    val partition = (readInt32, readInt16).tupled <* skipTags
    val topic     = (readCompactString, readCompactArray(partition)).tupled <* skipTags
    readInt32 *> readCompactArray(topic).map(_.flatMap((name, partitions) => partitions.map((index, code) => Answer(name, index, code)))) <* skipTags

private[xkafka] object OffsetCommitRequest:
  final case class Topic(name: String, partitions: List[Partition])

  /** The next offset to read in a partition, with the leader epoch of the record before it, or -1, and the metadata kept beside it. */
  final case class Partition(index: Int, offset: Long, leaderEpoch: Int, metadata: Option[String])

  /** The coordinator's answer for one partition: 0, or the error that kept its offset from being stored. */
  final case class Answer(topic: String, partition: Int, errorCode: Short)

  private def writeTopic(topic: Topic): ByteVector = compactString(topic.name) ++ compactArray(topic.partitions)(writePartition) ++ noTags

  private def writePartition(partition: Partition): ByteVector =
    int32(partition.index) ++ int64(partition.offset) ++ int32(partition.leaderEpoch) ++ compactNullableString(partition.metadata) ++ noTags

/** The offsets a group has stored for the partitions asked about, read by a member under its id and epoch (KIP-848). With `requireStable`, a
  * partition whose offsets a transaction has yet to settle answers an error to try again with.
  */
private[xkafka] final case class OffsetFetchRequest(
    groupId: String,
    memberId: Option[String],
    memberEpoch: Int,
    topics: List[OffsetFetchRequest.Topic],
    requireStable: Boolean
) extends Request[OffsetFetchRequest.Answer]:
  import OffsetFetchRequest.*
  val api: Api         = Api.OffsetFetch
  def body: ByteVector =
    val group = compactString(groupId) ++ compactNullableString(memberId) ++ int32(memberEpoch) ++ compactArray(topics)(writeTopic) ++ noTags
    unsignedVarint(2) ++ group ++ boolean(requireStable) ++ noTags

  def response(version: Short): Read[Answer] =
    val partition = (readInt32, readInt64, readInt32, readCompactNullableString, readInt16).tupled <* skipTags
    val topic     =
      (readCompactString, readCompactArray(partition)).mapN((name, partitions) =>
        partitions.map((index, offset, leaderEpoch, metadata, code) => Stored(name, index, offset, leaderEpoch, metadata, code))
      ) <* skipTags
    val group = (readCompactString *> readCompactArray(topic).map(_.flatten), readInt16).mapN(Answer.apply) <* skipTags
    val one   =
      readCompactArray(group).flatMap:
        case List(answer) => answer.pure[Read]
        case other        => fail[Answer](s"an answer for ${other.size} groups came for one")
    readInt32 *> one <* skipTags

private[xkafka] object OffsetFetchRequest:
  final case class Topic(name: String, partitions: List[Int])

  /** What the group stored for one partition: offset -1 when nothing is, and the partition's own error, or 0. */
  final case class Stored(topic: String, partition: Int, offset: Long, leaderEpoch: Int, metadata: Option[String], errorCode: Short)

  /** The answer for the group: an error for the group as a whole, or 0, and each partition's own. */
  final case class Answer(partitions: List[Stored], errorCode: Short)

  private def writeTopic(topic: Topic): ByteVector = compactString(topic.name) ++ compactArray(topic.partitions)(int32) ++ noTags

/** The structures a streams group's heartbeat carries. */
private[xkafka] object StreamsGroup:
  final case class Topology(epoch: Int, subtopologies: List[Subtopology])

  final case class Subtopology(
      id: String,
      sourceTopics: List[String],
      sourceTopicRegex: List[String],
      stateChangelogTopics: List[TopicInfo],
      repartitionSinkTopics: List[String],
      repartitionSourceTopics: List[TopicInfo],
      copartitionGroups: List[CopartitionGroup]
  )

  /** An internal topic the broker creates, with 0 partitions or replicas leaving the choice to it. */
  final case class TopicInfo(name: String, partitions: Int, replicationFactor: Short, configs: List[(String, String)])

  /** Topics that must have as many partitions as each other, as indexes into their sub-topology's lists. */
  final case class CopartitionGroup(sourceTopics: List[Short], sourceTopicRegex: List[Short], repartitionSourceTopics: List[Short])

  final case class Endpoint(host: String, port: Int)

  /** The tasks of one sub-topology, one for each partition. */
  final case class Tasks(subtopologyId: String, partitions: List[Int])

  final case class TaskOffset(subtopologyId: String, partition: Int, offset: Long)

  /** Something the coordinator reports about the group, such as missing topics, by its code. */
  final case class Status(code: Byte, detail: String)

  final case class TopicPartitions(topic: String, partitions: List[Int])

  /** The partitions of the tasks behind one member's endpoint, for interactive queries. */
  final case class EndpointPartitions(endpoint: Endpoint, active: List[TopicPartitions], standby: List[TopicPartitions])

  private[protocol] def writeTopology(topology: Topology): ByteVector =
    int32(topology.epoch) ++ compactArray(topology.subtopologies)(writeSubtopology) ++ noTags

  private def writeSubtopology(subtopology: Subtopology): ByteVector =
    compactString(subtopology.id) ++ compactArray(subtopology.sourceTopics)(compactString) ++
      compactArray(subtopology.sourceTopicRegex)(compactString) ++ compactArray(subtopology.stateChangelogTopics)(writeTopicInfo) ++
      compactArray(subtopology.repartitionSinkTopics)(compactString) ++ compactArray(subtopology.repartitionSourceTopics)(writeTopicInfo) ++
      compactArray(subtopology.copartitionGroups)(writeCopartitionGroup) ++ noTags

  private def writeTopicInfo(topic: TopicInfo): ByteVector =
    compactString(topic.name) ++ int32(topic.partitions) ++ int16(topic.replicationFactor.toInt) ++ compactArray(topic.configs)(writeKeyValue) ++
      noTags

  private def writeCopartitionGroup(group: CopartitionGroup): ByteVector =
    val indexes = (items: List[Short]) => compactArray(items)(index => int16(index.toInt))
    indexes(group.sourceTopics) ++ indexes(group.sourceTopicRegex) ++ indexes(group.repartitionSourceTopics) ++ noTags

  private[protocol] def writeKeyValue(pair: (String, String)): ByteVector = compactString(pair._1) ++ compactString(pair._2) ++ noTags

  private[protocol] def writeEndpoint(endpoint: Endpoint): ByteVector = compactString(endpoint.host) ++ int16(endpoint.port) ++ noTags

  private[protocol] def writeTasks(tasks: Tasks): ByteVector = compactString(tasks.subtopologyId) ++ compactArray(tasks.partitions)(int32) ++ noTags

  private[protocol] def writeTaskOffset(offset: TaskOffset): ByteVector =
    compactString(offset.subtopologyId) ++ int32(offset.partition) ++ int64(offset.offset) ++ noTags

  private[protocol] val readTasks: Read[Tasks] = (readCompactString, readCompactArray(readInt32)).mapN(Tasks.apply) <* skipTags

  private val readEndpoint: Read[Endpoint] = (readCompactString, readUint16).mapN(Endpoint.apply) <* skipTags

  private val readTopicPartitions: Read[TopicPartitions] = (readCompactString, readCompactArray(readInt32)).mapN(TopicPartitions.apply) <* skipTags

  private[protocol] val readEndpointPartitions: Read[EndpointPartitions] =
    (readEndpoint, readCompactArray(readTopicPartitions), readCompactArray(readTopicPartitions)).mapN(EndpointPartitions.apply) <* skipTags

/** Requests as they go on the wire, and the answers as they come back. */
private[xkafka] object Frame:
  /** The request with its header, after its size. */
  def request(request: Request[?], version: Short, correlationId: Int, clientId: String): ByteVector =
    val header  = int16(request.api.key.toInt) ++ int16(version.toInt) ++ int32(correlationId) ++ nullableString(Some(clientId))
    val message = header ++ (if request.api.flexible then noTags else ByteVector.empty) ++ request.body
    int32(message.size.toInt) ++ message

  /** Reads the answer to a request sent in `version` from what followed its size. */
  def response[R](request: Request[R], version: Short, correlationId: Int)(bytes: ByteVector): Result[R] =
    // The answer to ApiVersions keeps the first header, so that a broker can answer a version it does not read.
    val tags   = if request.api.flexible && request.api != Api.ApiVersions then skipTags else ().pure[Read]
    val header =
      readInt32.flatMap(found => if found == correlationId then tags else fail(s"an answer to request $found came for request $correlationId"))
    readAll(header *> request.response(version))(bytes)
