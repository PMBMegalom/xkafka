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
package librdkafka

import cats.data.NonEmptyList
import cats.syntax.all.*

/** What deleting records answered. */
private[xkafka] enum DeletedRecords:
  /** One failure for the whole call, with librdkafka's code for it. */
  case Refused(failure: KafkaException.BackendFailure, code: Int)

  /** A code and low watermark for each requested partition, in request order; `Failures.NoAnswer` where the result left one out. */
  case Answered(results: Vector[(Int, Long)])

/** One described topic: its error code and text, and its entries when the code is 0. */
private[xkafka] final case class ConfigResource(name: String, error: Int, errorString: String, entries: Vector[ConfigEntry])

private[xkafka] final case class ConfigEntry(name: String, value: Option[String], source: Int, sensitive: Boolean, readOnly: Boolean)

/** What a call that answers each requested item reported. */
private[xkafka] enum Coded:
  /** One failure for the whole call, or for the group it names. */
  case Refused(failure: KafkaException.BackendFailure)

  /** A code for each requested item, in request order; `Failures.NoAnswer` where the result left one out. */
  case Answered(codes: Vector[Int])

/** One described group: its error code and text, and what it holds when the code is 0. */
private[xkafka] final case class DescribedGroup(
    group: String,
    error: Int,
    errorString: String,
    simple: Boolean,
    state: Int,
    assignor: Option[String],
    coordinator: Option[Node],
    members: List[DescribedMember]
)

/** A broker as librdkafka reports it. */
private[xkafka] final case class Node(id: Int, host: String, port: Int, rack: Option[String])

/** What describing the cluster answered, each part `None` where librdkafka reported none. */
private[xkafka] final case class DescribedCluster(id: Option[String], controller: Option[Node], brokers: Vector[Node])

/** One described topic: its error code and text, and what it holds when the code is 0. `id` is the topic id's most and least significant halves. */
private[xkafka] final case class DescribedTopic(
    name: String,
    error: Int,
    errorString: String,
    id: (Long, Long),
    internal: Boolean,
    partitions: Vector[DescribedPartition]
)

/** A partition's replicas by broker id; a negative `leader` is none. */
private[xkafka] final case class DescribedPartition(partition: Int, leader: Int, replicas: Vector[Int], inSync: Vector[Int])

private[xkafka] final case class DescribedMember(
    memberId: String,
    instanceId: Option[String],
    clientId: String,
    host: String,
    assignment: Vector[(String, Int)]
)

/** How the shim's failures become exceptions, the same on every platform. */
private[xkafka] object Failures:
  /** What the shim leaves in place of a code for a partition its result does not mention. */
  val NoAnswer: Int = Int.MinValue

  /** A failure the shim reported with a librdkafka code. */
  def coded(message: String, code: Int): KafkaException.BackendFailure =
    val classified = ErrorCode.fromLibrdkafka(code)
    new KafkaException.BackendFailure(message, Some(classified), retriable = Some(classified.retriable))

  /** A transactional failure, whose flags are -1 when librdkafka did not say. Whether it is retriable comes from its code, as on the JVM, so the same
    * failure is retriable on every platform.
    */
  def classified(message: String, code: Int, fatal: Int, transactionAbortRequired: Int): KafkaException.BackendFailure =
    val classified = ErrorCode.fromLibrdkafka(code)
    new KafkaException.BackendFailure(message, Some(classified), Some(classified.retriable), flag(fatal), flag(transactionAbortRequired))

  def plain(detail: String): KafkaException.BackendFailure = new KafkaException.BackendFailure(detail)

  private def flag(value: Int): Option[Boolean] = Option.when(value >= 0)(value != 0)

/** The portable answers for what the shim reports about deleting records and describing configurations. */
private[librdkafka] object LibrdkafkaAnswers:
  // _NOENT, which librdkafka reports when deleting records finds no leader for any requested partition.
  private val NoLeadersFound = -156

  /** librdkafka fails the whole call where no requested partition exists, and answers each one where only some do. */
  def deleted(cuts: Vector[(TopicPartition, Offset)], answer: DeletedRecords, errorString: Int => String): Vector[Either[KafkaException, Offset]] =
    answer match
      case DeletedRecords.Refused(failure, NoLeadersFound) =>
        val unknown =
          new KafkaException.BackendFailure(
            failure.getMessage,
            Some(ErrorCode.UnknownTopicOrPartition),
            Some(ErrorCode.UnknownTopicOrPartition.retriable)
          )
        cuts.map(_ => Left(unknown))
      case DeletedRecords.Refused(failure, _) => throw failure
      case DeletedRecords.Answered(results)   => cuts.zip(results).map:
          case ((topicPartition, _), (code, low)) => code match
              case Failures.NoAnswer => throw new KafkaException.InvalidBackendResponse(s"no answer for partition ${topicPartition.show}")
              case 0                 => Right(Offset.from(low).fold(error => throw invalid("low watermark", low, error), identity))
              case code              =>
                val classified = ErrorCode.fromLibrdkafka(code)
                Left(new KafkaException.BackendFailure(errorString(code), Some(classified), Some(classified.retriable)))

  /** Each requested topic's own answer, which librdkafka reports by name. */
  def configurations(requested: Vector[Topic], resources: Vector[ConfigResource]): Vector[Either[KafkaException, TopicConfiguration]] =
    val answers = resources.map(resource => resource.name -> resource).toMap
    requested.map: topic =>
      answers.get(topic.value) match
        case None           => throw new KafkaException.InvalidBackendResponse(s"no answer for topic ${topic.value}")
        case Some(resource) => configuration(resource)

  /** One entry for each property changed, in request order: its topic, its name, its value where it has one, and librdkafka's
    * `rd_kafka_AlterConfigOpType_t` for the change.
    */
  def configurationChanges(changes: Vector[(Topic, NonEmptyList[(String, ConfigurationChange)])]): Vector[(String, String, Option[String], Int)] =
    changes.flatMap: (topic, properties) =>
      properties.toList.map: (name, change) =>
        change match
          case ConfigurationChange.Set(value)      => (topic.value, name, Some(value), 0) // SET
          case ConfigurationChange.Unset           => (topic.value, name, None, 1)        // DELETE
          case ConfigurationChange.Append(value)   => (topic.value, name, Some(value), 2) // APPEND
          case ConfigurationChange.Subtract(value) => (topic.value, name, Some(value), 3) // SUBTRACT

  /** Each requested topic's own answer, which librdkafka reports by name, and failures that say no topic got an answer fail the call. */
  def changed(requested: Vector[Topic], resources: Vector[ConfigResource]): Vector[Either[KafkaException, Unit]] =
    val answers = resources.map(resource => resource.name -> resource).toMap
    val each    =
      requested.map: topic =>
        answers.get(topic.value) match
          case None                                  => throw new KafkaException.InvalidBackendResponse(s"no answer for topic ${topic.value}")
          case Some(resource) if resource.error == 0 => Right(())
          case Some(resource)                        => Left(Failures.coded(resource.errorString, resource.error))
    unlessAllUnreached(each)

  /** Each partition's answer, where one already led by its preferred replica answers as elected, since what was asked for holds. */
  def elected(requested: Vector[TopicPartition], answer: Coded, errorString: Int => String): Vector[Either[KafkaException, Unit]] =
    val settled =
      answer match
        case Coded.Answered(codes) => Coded.Answered(codes.map(code => if code == ElectionNotNeeded then 0 else code))
        case refused               => refused
    eachCoded(requested, settled, _.show, errorString)

  // ELECTION_NOT_NEEDED, which the broker answers for a partition its preferred replica already leads.
  private val ElectionNotNeeded = 84

  private def unlessAllUnreached[A](answers: Vector[Either[KafkaException, A]]): Vector[Either[KafkaException, A]] =
    val failures = answers.collect { case Left(failure: KafkaException.BackendFailure) => failure }
    failures.headOption.filter(_ => failures.size == answers.size && failures.forall(unreached)).fold(answers)(throw _)

  private def configuration(resource: ConfigResource): Either[KafkaException, TopicConfiguration] =
    resource.error match
      case 0 =>
        val entries =
          resource.entries.map: entry =>
            entry.name -> ConfigurationEntry.reported(entry.value, entry.sensitive, ConfigurationSource.fromLibrdkafka(entry.source), entry.readOnly)
        Right(TopicConfiguration(entries.toMap))
      case code =>
        val classified = ErrorCode.fromLibrdkafka(code)
        Left(new KafkaException.BackendFailure(resource.errorString, Some(classified), Some(classified.retriable)))

  private def invalid(field: String, value: Any, error: ValidationError): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error")

  /** The groups whose names a `ConsumerGroup` can hold, with their states. */
  def listed(groups: Vector[(String, Int)]): Vector[(ConsumerGroup, ConsumerGroupState)] =
    groups.flatMap((name, state) => ConsumerGroup.from(name).toOption.map(_ -> ConsumerGroupState.fromLibrdkafka(state)))

  /** Each requested group's own answer, which librdkafka reports by name. A group it describes as dead does not exist, as the Java client reports it,
    * and failures that say no group got an answer fail the call.
    */
  def described(requested: Vector[ConsumerGroup], groups: Vector[DescribedGroup]): Vector[Either[KafkaException, ConsumerGroupDescription]] =
    val answers  = groups.map(group => group.group -> group).toMap
    val failures = groups.map(group => Failures.coded(group.errorString, group.error))
    if failures.nonEmpty && failures.forall(unreached) then throw failures.head
    requested.map: group =>
      answers.get(group.value) match
        case None                              => throw new KafkaException.InvalidBackendResponse(s"no answer for group ${group.value}")
        case Some(answer) if answer.error != 0 => Left(Failures.coded(answer.errorString, answer.error))
        case Some(answer) if answer.state == 4 => Left(Failures.coded(s"Group ${group.value} not found.", 69))
        case Some(answer)                      => Right(ConsumerGroupDescription(
            ConsumerGroupState.fromLibrdkafka(answer.state),
            answer.simple,
            answer.assignor.filter(_.nonEmpty),
            answer.coordinator.map(broker),
            answer.members.map: member =>
              GroupMember(member.memberId, member.instanceId, member.clientId, member.host, member.assignment.map(topicPartition).toSet)
          ))

  def cluster(described: DescribedCluster): ClusterDescription =
    ClusterDescription(
      described.id.getOrElse(throw new KafkaException.InvalidBackendResponse("the cluster reported no id")),
      described.controller.map(broker),
      NonEmptyList.fromList(described.brokers.toList.map(broker)).map(_.toNes)
        .getOrElse(throw new KafkaException.InvalidBackendResponse("the cluster reported no brokers"))
    )

  /** Each requested topic's own answer, which librdkafka reports by name, and failures that say no topic got an answer fail the call. */
  def topics(requested: Vector[Topic], described: Vector[DescribedTopic]): Vector[Either[KafkaException, TopicDescription]] =
    val answers  = described.map(topic => topic.name -> topic).toMap
    val failures = described.map(topic => Failures.coded(topic.errorString, topic.error))
    if failures.nonEmpty && failures.forall(unreached) then throw failures.head
    requested.map: topic =>
      answers.get(topic.value) match
        case None                              => throw new KafkaException.InvalidBackendResponse(s"no answer for topic ${topic.value}")
        case Some(answer) if answer.error != 0 => Left(Failures.coded(answer.errorString, answer.error))
        case Some(answer)                      => Right(description(answer))

  private def description(topic: DescribedTopic): TopicDescription =
    def partition(described: DescribedPartition): (Partition, PartitionDescription) =
      val replicas =
        NonEmptyList.fromList(described.replicas.toList.map(brokerId))
          .getOrElse(throw new KafkaException.InvalidBackendResponse(s"partition ${described.partition} of ${topic.name} has no replicas"))
      Partition.from(described.partition).fold(error => throw invalid("partition", described.partition, error), identity) ->
        PartitionDescription(Option.when(described.leader >= 0)(brokerId(described.leader)), replicas, described.inSync.toList.map(brokerId))
    TopicDescription(
      topicId(topic.id),
      topic.internal,
      NonEmptyList.fromList(topic.partitions.toList.map(partition)).map(_.toNem)
        .getOrElse(throw new KafkaException.InvalidBackendResponse(s"topic ${topic.name} has no partitions"))
    )

  /** Encodes the id as Kafka's `Uuid.toString` does: its bytes from the most significant, in URL-safe base64 without padding. */
  def topicId(halves: (Long, Long)): TopicId =
    val bytes = java.nio.ByteBuffer.allocate(16).putLong(halves._1).putLong(halves._2).array
    TopicId(java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes))

  private def broker(node: Node): Broker = Broker(brokerId(node.id), node.host, node.port, node.rack)

  private def brokerId(value: Int): BrokerId = BrokerId.from(value).fold(error => throw invalid("broker id", value, error), identity)

  /** The offsets a group committed, as librdkafka lists them by topic and partition. */
  def committed(entries: Vector[(String, Int, driver.StoredOffset)]): Vector[(TopicPartition, driver.StoredOffset)] =
    entries.map((topic, partition, stored) => topicPartition((topic, partition)) -> stored)

  /** Each requested item's answer. A failure that says the request got no answer fails the call; any other failure for the whole call, or for the
    * group it names, is every item's answer, as the Java client gives it.
    */
  def eachCoded[K](requested: Vector[K], answer: Coded, name: K => String, errorString: Int => String): Vector[Either[KafkaException, Unit]] =
    answer match
      case Coded.Refused(failure) if unreached(failure) => throw failure
      case Coded.Refused(failure)                       => requested.map(_ => Left(failure))
      case Coded.Answered(codes) if codes.nonEmpty && codes.forall(code => unreached(Failures.coded(errorString(code), code))) =>
        throw Failures.coded(errorString(codes.head), codes.head)
      case Coded.Answered(codes) => requested.zip(codes).map:
          case (key, Failures.NoAnswer) => throw new KafkaException.InvalidBackendResponse(s"no answer for ${name(key)}")
          case (_, 0)                   => Right(())
          case (_, code)                => Left(Failures.coded(errorString(code), code))

  private def unreached(failure: KafkaException.BackendFailure): Boolean =
    failure.code.exists(code => code == ErrorCode.RequestTimedOut || code == ErrorCode.NetworkException || code == ErrorCode.BrokerNotAvailable)

  private def topicPartition(value: (String, Int)): TopicPartition =
    TopicPartition(
      Topic.from(value._1).fold(error => throw invalid("topic", value._1, error), identity),
      Partition.from(value._2).fold(error => throw invalid("partition", value._2, error), identity)
    )
