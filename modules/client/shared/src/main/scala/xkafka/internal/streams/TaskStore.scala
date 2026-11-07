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

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk

/** One store of a task, by serialized key, in the order of [[StoreContents.keys]]. A task reads it through what it has written since its last commit,
  * and writes to it only once that commit has succeeded, all of a commit's changes at once, so that a store never holds what the group has not
  * committed.
  */
private[xkafka] trait TaskStore[F[_]]:
  def get(key: Chunk[Byte]): F[Option[Stored]]

  /** The keys starting with `prefix` and what they hold, in order. */
  def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Stored)]]

  def all: F[StoreContents]

  /** Applies `changes` as one: a key with nothing to hold is removed. */
  def write(changes: StoreChanges): F[Unit]

private[xkafka] object TaskStore:
  /** Opens the store of a name for a task, holding what its changelog restored. */
  type Open[F[_]] = (String, StoreContents) => F[TaskStore[F]]

  /** Opens each store in memory. */
  def memory[F[_]: Concurrent]: Open[F] = (_, held) => inMemory(held)

  /** A store in memory, holding `held` to start with. */
  def inMemory[F[_]](held: StoreContents)(using F: Concurrent[F]): F[TaskStore[F]] =
    F.ref(held).map: contents =>
      new TaskStore[F]:
        def get(key: Chunk[Byte]): F[Option[Stored]]                        = contents.get.map(_.get(key))
        def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Stored)]] = contents.get.map(_.prefixed(prefix))
        def all: F[StoreContents]                                           = contents.get
        def write(changes: StoreChanges): F[Unit]                           = contents.update(_.after(changes))
