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
import scala.annotation.implicitNotFound
import scala.util.NotGiven

/** Whether a stream's records are keyed as they were read, each in the partition of its key, or may have been given a new key since. A stream's
  * [[KStream.Keying]] says which, and decides how it is grouped or joined by key.
  */
sealed trait KeyState

object KeyState:
  /** Records keyed as they were read, which can be grouped or joined by key where they are. */
  sealed trait AsRead extends KeyState

  /** Records `map`, `flatMap`, `selectKey` or `evalMap` may have given a new key, which go through a repartition topic before they are grouped or
    * joined by key.
    */
  sealed trait Changed extends KeyState

  /** Evidence that records of the key state `P` are keyed as they were read. */
  @implicitNotFound(
    "the records' key may have changed since they were read, so they go through a repartition topic first: name it with groupByKey(Repartition(...), key, value), or pass join the serdes to write it with"
  )
  sealed trait Unchanged[P]

  object Unchanged:
    given Unchanged[AsRead] = new Unchanged[AsRead] {}

  /** Evidence that records of the key state `P` may not be keyed as they were read: those of a stream known to have changed its key, or of one whose
    * type does not say.
    */
  @implicitNotFound(
    "the records are keyed as they were read, so they need no repartition topic: group them with groupByKey(key, value), or join them without serdes"
  )
  sealed trait MayHaveChanged[P]

  object MayHaveChanged:
    given [P](using NotGiven[Unchanged[P]]): MayHaveChanged[P] = new MayHaveChanged[P] {}
