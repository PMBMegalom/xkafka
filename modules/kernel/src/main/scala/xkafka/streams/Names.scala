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

import cats.Show

/** The name of a state store, from which its changelog topic `<application id>-<name>-changelog` is named. */
opaque type Store = String

object Store:
  def apply(name: String): Store = name

  extension (store: Store) def value: String = store

  given CanEqual[Store, Store] = CanEqual.derived
  given Show[Store]            = Show.show(_.value)

/** The name of a repartitioning, from which its topic `<application id>-<name>-repartition` is named. */
opaque type Repartition = String

object Repartition:
  def apply(name: String): Repartition = name

  extension (repartition: Repartition) def value: String = repartition

  given CanEqual[Repartition, Repartition] = CanEqual.derived
  given Show[Repartition]                  = Show.show(_.value)

/** The name of a step of a program, as Java's `Named` names a processor, from which the program's description names the step and Java names the nodes
  * it makes for it.
  */
opaque type Named = String

object Named:
  def apply(name: String): Named = name

  extension (named: Named) def value: String = named

  given CanEqual[Named, Named] = CanEqual.derived
  given Show[Named]            = Show.show(_.value)
