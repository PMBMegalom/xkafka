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

import scala.concurrent.duration.FiniteDuration

/** Where an application keeps its stores on disk, as Java's state directory: each task's stores under a directory named by the task, the global
  * tables' under `global`, and the process id the application runs as, kept between runs so that the group can give a member back the tasks whose
  * stores it holds. Only one instance of the application uses it at a time.
  */
private[xkafka] trait StateDirectory[F[_]]:
  /** Opens the store `store` of `owner`, a task's id or [[StateDirectory.Global]], finding keys by the time `time` reads if given. A store that holds
    * what its changelog had without saying how far, which no store this writes does, is emptied first, to be filled again from the start.
    */
  def open(owner: String, store: String, time: Option[TaskStore.TimeOf]): F[TaskStore[F]]

  /** The process id an earlier run kept, or else `fresh`, kept from now on. */
  def processId(fresh: String): F[String]

  /** Marks `owner`'s stores as given up now, from when their directory is removed once the cleanup delay has passed, with where each store, by name,
    * holds its changelog up to.
    */
  def released(owner: String, positions: Map[String, Long]): F[Unit]

  /** Where the stores of each task given up here, by its id, hold their changelogs up to, by store, as `released` last recorded. */
  def held: F[Map[String, Map[String, Long]]]

  /** Removes the directories of tasks other than those `held` names, which nothing has marked for `delay`, as Java's cleaner removes them. */
  def cleaned(held: Set[String], delay: FiniteDuration): F[Unit]

private[xkafka] object StateDirectory:
  /** The owner of the global tables' stores. */
  val Global: String = "global"
