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
package interop

import java.time.{Duration, Instant}
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.apache.kafka.common.{Metric, MetricName}
import org.apache.kafka.common.metrics.{Metrics, Sensor}
import org.apache.kafka.common.serialization.Serde as JavaSerde
import org.apache.kafka.streams.StreamsMetrics
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType, Punctuator, StateStore, StreamPartitioner, TaskId}
import org.apache.kafka.streams.processor.api.{
  FixedKeyProcessor as JavaFixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord as JavaFixedKeyRecord, InternalFixedKeyRecordFactory,
  Processor as JavaProcessor, ProcessorContext, Record as JavaRecord, RecordMetadata
}
import xkafka.interop.toJava

/** A Java processor's instance in one task, as its context sees it: the call it is in, the records it sent on in that call, its punctuators, and the
  * views of its stores, each waiting on the store through `dispatcher`, which lives as long as the instance.
  */
private final class Session[F[_], K2, V2](task: HostedTask, stores: List[JavaStore[F]], dispatcher: Dispatcher[F])(using F: Async[F]):
  private val current     = new AtomicReference[Option[HostedCall[F]]](None)
  private val sent        = new AtomicReference(Vector.empty[Record[K2, V2]])
  private val numbers     = new AtomicInteger(0)
  private val punctuators = new ConcurrentHashMap[Int, (HostedSchedule, Punctuator)]
  private val metrics     = new Metrics()

  private val run =
    new Runner[F]:
      def apply[A](effect: F[A]): A = dispatcher.unsafeRunSync(effect)

  private def call: HostedCall[F] = current.get.getOrElse(throw new IllegalStateException("a Java processor used its context outside a call"))

  private val views: Map[String, StateStore] =
    stores.map(store =>
      store.name ->
        store.view(() => call.stores.getOrElse(store.name, throw new IllegalStateException(s"the store '${store.name}' is not open")), run)
    ).toMap

  /** What `body` does, run in `call`, on a thread that may wait, with the records it sent on. */
  def calling[A](during: HostedCall[F])(body: => A): F[(A, Vector[Record[K2, V2]])] =
    F.blocking:
      current.set(Some(during))
      try
        val done = body
        (done, sent.getAndSet(Vector.empty))
      finally current.set(None)

  def send(record: Record[K2, V2]): Unit = sent.updateAndGet(_ :+ record): Unit

  def store(name: String): StateStore =
    views.getOrElse(name, throw new IllegalArgumentException(s"the Java processor asked for the store '$name', which it was not given"))

  def schedule(start: Option[Instant], interval: Duration, kind: PunctuationType, punctuator: Punctuator): Cancellable =
    if interval.toMillis < 1 then throw new IllegalArgumentException(s"a punctuator's interval must be at least a millisecond, was $interval")
    val every  = FiniteDuration(interval.toMillis, MILLISECONDS)
    val number = numbers.incrementAndGet
    val wanted = if kind == PunctuationType.WALL_CLOCK_TIME then Punctuation.wallClock(every) else Punctuation.streamTime(every)
    punctuators.put(number, (HostedSchedule(wanted, start.map(_.toEpochMilli)), punctuator)): Unit
    () => punctuators.remove(number): Unit

  def schedules: Map[Int, HostedSchedule] = punctuators.asScala.view.mapValues(_._1).toMap

  def punctuator(number: Int): Option[Punctuator] = Option(punctuators.get(number)).map(_._2)

  def taskId: TaskId                         = new TaskId(task.subtopology, task.partition)
  def application: String                    = task.application
  def streamTime: Long                       = call.streamTime
  def wallClock: Long                        = call.wallClock
  def configs: java.util.Map[String, Object] = Map[String, Object]("application.id" -> task.application).asJava

  def metadata: Optional[RecordMetadata] =
    call.origin.map: origin =>
      new RecordMetadata:
        def topic(): String  = origin.topic.value
        def partition(): Int = origin.partition.value
        def offset(): Long   = origin.offset.value
    .toJava

  /** A metrics registry of the instance's own, for a processor that adds sensors; nothing reads what it records. */
  val streamsMetrics: StreamsMetrics =
    new StreamsMetrics:
      def metrics(): java.util.Map[MetricName, ? <: Metric] = Session.this.metrics.metrics()
      def addLatencyRateTotalSensor(scope: String, entity: String, operation: String, level: Sensor.RecordingLevel, tags: String*): Sensor =
        Session.this.metrics.sensor(s"$scope.$entity.$operation", level)
      def addRateTotalSensor(scope: String, entity: String, operation: String, level: Sensor.RecordingLevel, tags: String*): Sensor =
        Session.this.metrics.sensor(s"$scope.$entity.$operation", level)
      def addSensor(name: String, level: Sensor.RecordingLevel): Sensor                   = Session.this.metrics.sensor(name, level)
      def addSensor(name: String, level: Sensor.RecordingLevel, parents: Sensor*): Sensor = Session.this.metrics.sensor(name, level, parents*)
      def removeSensor(sensor: Sensor): Unit                                              = Session.this.metrics.removeSensor(sensor.name)

  def closeMetrics(): Unit = metrics.close()

/** What every context gives a Java processor that does not depend on what it sends on. */
private trait Contexts[F[_], K2, V2]:
  protected def session: Session[F, K2, V2]

  def applicationId(): String                    = session.application
  def taskId(): TaskId                           = session.taskId
  def recordMetadata(): Optional[RecordMetadata] = session.metadata
  def keySerde(): JavaSerde[?]                   = throw new UnsupportedOperationException("xkafka has no default serdes to give a Java processor")
  def valueSerde(): JavaSerde[?]                 = throw new UnsupportedOperationException("xkafka has no default serdes to give a Java processor")
  def stateDir(): java.io.File  = throw new UnsupportedOperationException("xkafka keeps a Java processor's stores in memory, with no directory")
  def metrics(): StreamsMetrics = session.streamsMetrics
  def schedule(interval: Duration, kind: PunctuationType, punctuator: Punctuator): Cancellable = session.schedule(None, interval, kind, punctuator)
  def schedule(start: Instant, interval: Duration, kind: PunctuationType, punctuator: Punctuator): Cancellable =
    session.schedule(Some(start), interval, kind, punctuator)
  // Commits come at the commit interval, as they would after Java's request too; asking for one sooner changes nothing.
  def commit(): Unit                                                      = ()
  def appConfigs(): java.util.Map[String, Object]                         = session.configs
  def appConfigsWithPrefix(prefix: String): java.util.Map[String, Object] = session.configs.asScala.filter(_._1.startsWith(prefix)).asJava
  def currentSystemTimeMs(): Long                                         = session.wallClock
  def currentStreamTimeMs(): Long                                         = session.streamTime

  def getStateStore[S <: StateStore](name: String): S =
    // Java's own context casts to what its caller asks for in the same way; a processor asking for the wrong kind of store fails where it uses it.
    session.store(name).asInstanceOf[S]

private object Records:
  def toJava[K, V](record: Record[K, V]): JavaRecord[K, V] =
    new JavaRecord(record.key, record.value, record.timestamp.epochMillis, record.headers.toJava)

  def fromJava[K, V](record: JavaRecord[? <: K, ? <: V]): Record[K, V] =
    Record(record.key, record.value, Timestamp.fromEpochMillis(record.timestamp), xkafka.interop.fromJava(Headers)(record.headers))

/** Starts each task's instance of a Java processor, keeps it by the task, runs records and punctuators through it, and closes it. */
private final class Hosted[F[_], K, V, K2, V2, J](
    make: () => J,
    stores: List[JavaStore[F]],
    init: (J, Session[F, K2, V2]) => Unit,
    process: (J, Record[K, V]) => Unit,
    close: J => Unit
)(using F: Async[F])
    extends Hosting[F, K, V, K2, V2]:
  private val instances = new ConcurrentHashMap[AnyRef, (J, Session[F, K2, V2], F[Unit])]

  private def instance(task: HostedTask): F[(J, Session[F, K2, V2], F[Unit])] =
    F.delay(Option(instances.get(task.token)))
      .flatMap(_.liftTo[F](new IllegalStateException("a Java processor's instance was used before it started")))

  def start(task: HostedTask, call: HostedCall[F]): F[Unit] =
    Dispatcher.sequential[F].allocated.flatMap: (dispatcher, release) =>
      val session = new Session[F, K2, V2](task, stores, dispatcher)
      session.calling(call):
        val made = make()
        init(made, session)
        made
      .flatMap: (made, sent) =>
        F.raiseWhen(sent.nonEmpty)(new UnsupportedOperationException("a Java processor sent records on as it started")) *>
          F.delay(instances.put(task.token, (made, session, release))).void
      .onError(_ => release)

  def process(task: HostedTask, record: Record[K, V], call: HostedCall[F]): F[List[Record[K2, V2]]] =
    instance(task).flatMap((made, session, _) => session.calling(call)(process(made, record)).map(_._2.toList))

  def punctuate(task: HostedTask, number: Int, at: Timestamp, call: HostedCall[F]): F[List[Record[K2, V2]]] =
    instance(task).flatMap: (_, session, _) =>
      session.calling(call)(session.punctuator(number).foreach(_.punctuate(at.epochMillis))).map(_._2.toList)

  def schedules(task: HostedTask): F[Map[Int, HostedSchedule]] = instance(task).map(_._2.schedules)

  def close(task: HostedTask): F[Unit] =
    F.delay(Option(instances.remove(task.token))).flatMap:
      _.traverse_ : (made, session, release) =>
        F.blocking(
          try close(made)
          finally session.closeMetrics()
        ).guarantee(release)

/** Makes processors from Java's for the effect `F`, given the record types from what is passed. */
final class ProcessorFromJava[F[_]] private[interop] ():
  /** A Java `Processor` as a processor: each task makes its own with `make`, and it reads `stores` as Java stores by their names. */
  def apply[K, V, K2, V2](make: () => JavaProcessor[K, V, K2, V2], stores: JavaStore[F]*)(using F: Async[F]): Processor[F, K, V, K2, V2] =
    val hosting =
      Hosted[F, K, V, K2, V2, JavaProcessor[K, V, K2, V2]](
        make,
        stores.toList,
        (made, session) => made.init(Contexts.plain(session)),
        (made, record) => made.process(Records.toJava(record)),
        _.close()
      )
    Processor.hosted(stores.toList.map(_.kept), stores.map(store => store.name -> store.idle).toMap, hosting)

/** Makes processors keeping each record's key from Java's for the effect `F`, given the record types from what is passed. */
final class FixedKeyProcessorFromJava[F[_]] private[interop] ():
  /** A Java `FixedKeyProcessor` as one: each task makes its own with `make`, and it reads `stores` as Java stores by their names. */
  def apply[K, V, V2](make: () => JavaFixedKeyProcessor[K, V, V2], stores: JavaStore[F]*)(using F: Async[F]): FixedKeyProcessor[F, K, V, V2] =
    val hosting =
      Hosted[F, K, V, K, V2, JavaFixedKeyProcessor[K, V, V2]](
        make,
        stores.toList,
        (made, session) => made.init(Contexts.fixed(session)),
        (made, record) => made.process(InternalFixedKeyRecordFactory.create(Records.toJava(record))),
        _.close()
      )
    FixedKeyProcessor.hosted(Processor.hosted(stores.toList.map(_.kept), stores.map(store => store.name -> store.idle).toMap, hosting))

private object Contexts:
  def plain[F[_], K2, V2](held: Session[F, K2, V2]): ProcessorContext[K2, V2] =
    new ProcessorContext[K2, V2] with Contexts[F, K2, V2]:
      protected val session: Session[F, K2, V2]                                        = held
      def forward[K <: K2, V <: V2](record: JavaRecord[K, V]): Unit                    = session.send(Records.fromJava(record))
      def forward[K <: K2, V <: V2](record: JavaRecord[K, V], childName: String): Unit =
        throw new UnsupportedOperationException(s"xkafka sends a Java processor's records on to its one next step, not to '$childName'")

  def fixed[F[_], K, V2](held: Session[F, K, V2]): FixedKeyProcessorContext[K, V2] =
    new FixedKeyProcessorContext[K, V2] with Contexts[F, K, V2]:
      protected val session: Session[F, K, V2]                               = held
      def forward[K1 <: K, V <: V2](record: JavaFixedKeyRecord[K1, V]): Unit =
        session.send(Record(record.key, record.value, Timestamp.fromEpochMillis(record.timestamp), xkafka.interop.fromJava(Headers)(record.headers)))
      def forward[K1 <: K, V <: V2](record: JavaFixedKeyRecord[K1, V], childName: String): Unit =
        throw new UnsupportedOperationException(s"xkafka sends a Java processor's records on to its one next step, not to '$childName'")

extension (companion: Processor.type)
  /** A processor from a Java one, as in `Processor.fromJava[IO](() => new MyProcessor(), JavaStore.keyValue(...))`. */
  def fromJava[F[_]]: ProcessorFromJava[F] = ProcessorFromJava[F]()

extension (companion: FixedKeyProcessor.type)
  /** A processor keeping each record's key from a Java one, as in `FixedKeyProcessor.fromJava[IO](() => new MyProcessor())`. */
  def fromJava[F[_]]: FixedKeyProcessorFromJava[F] = FixedKeyProcessorFromJava[F]()

extension (companion: Partitioner.type)
  /** A Java `StreamPartitioner` as a partitioner, given each record's topic and headers as Java's is. Where it chooses none, the record goes where
    * Kafka's default puts it, as in Java; where it chooses several partitions or none at all, the application stops, as xkafka sends each record to
    * one partition.
    */
  def fromJava[K, V](java: StreamPartitioner[K, V]): Partitioner[K, V] =
    Partitioner.orDefault: (topic, key, value, headers, count) =>
      java.partitions(topic.value, key, value, headers.toJava, count).toScala.map(_.asScala.toList) match
        case None               => None
        case Some(List(chosen)) => Some(chosen.intValue)
        case Some(chosen) => throw new IllegalArgumentException(s"the Java partitioner for the topic '${topic.value}' chose the partitions ${chosen
              .sorted.mkString("[", ", ", "]")}, where xkafka sends each record to one partition")
