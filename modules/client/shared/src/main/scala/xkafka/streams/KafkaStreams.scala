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

import cats.Eq
import cats.arrow.FunctionK
import fs2.Stream

/** A streams application running as one member of its group, until the resource that gave it is released. */
trait KafkaStreams[F[_]]:
  self =>

  /** Waits while the application runs, failing with the error that stops it. Releasing the resource stops the application without failing it. */
  def run: F[Unit]

  /** The application's state, first as it is and then each time it changes. */
  def state: Stream[F, StreamsState]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaStreams[G] =
    new KafkaStreams[G]:
      def run: G[Unit]                   = fk(self.run)
      def state: Stream[G, StreamsState] = self.state.translate(fk)

/** Where a streams application is, and what the group's coordinator reports wrong with the application as a whole. */
final case class StreamsState(phase: StreamsPhase, problems: List[StreamsProblem])

object StreamsState:
  given Eq[StreamsState] = Eq.fromUniversalEquals

enum StreamsPhase derives CanEqual:
  /** Waiting for the group to give it tasks. */
  case Joining

  /** Refilling the stores of tasks it was given from their changelogs. */
  case Restoring

  /** Processing the tasks it was given, if any. */
  case Running

  /** Its tasks were taken away without being given back, as when it stopped heartbeating for too long; it is joining again. */
  case Lost

/** Something the group's coordinator reports keeps the application from running fully. Each comes with the coordinator's own description. */
enum StreamsProblem derives CanEqual:
  /** The application declared another topology than the group runs. */
  case StaleTopology(detail: String)

  /** Some of the topics the application reads do not exist. */
  case MissingSourceTopics(detail: String)

  /** Topics that must have as many partitions as each other do not. */
  case IncorrectlyPartitionedTopics(detail: String)

  /** The coordinator is creating the application's internal topics. */
  case MissingInternalTopics(detail: String)

  /** A member asked the whole application to shut down. */
  case ShutdownRequested(detail: String)

  /** The coordinator waits before assigning tasks, as it does once a group first forms. */
  case AssignmentDelayed(detail: String)

  /** Client tags that standby placement across racks needs are missing. */
  case MissingClientTags(detail: String)

  /** The application may not read or write some of its topics. */
  case TopicAuthorizationFailed(detail: String)

  /** A report this release does not know, by its code. */
  case Other(code: Int, detail: String)
