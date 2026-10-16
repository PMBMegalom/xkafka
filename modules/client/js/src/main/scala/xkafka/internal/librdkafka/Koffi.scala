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
import scala.scalajs.js.typedarray.ArrayBuffer

/** The parts of koffi the bindings use. Functions are declared from C prototypes and called dynamically, so they are left untyped here. */
@js.native
@JSImport("koffi", JSImport.Default)
private[librdkafka] object Koffi extends js.Object:
  def load(path: String): KoffiLibrary = js.native

  /** Declares a function type from its C prototype, so later prototypes can name it. */
  def proto(prototype: String): js.Any = js.native

  /** A pointer C can call `function` through, of the given function pointer type. */
  def register(function: js.Function, pointerType: String): js.Any = js.native

  /** The value at `pointer`, read as `type`. A pointer reads back as its address. */
  def decode(pointer: js.Any, `type`: String): js.Any = js.native

  /** A view of native memory without copying it, valid only while the memory is. */
  def view(pointer: js.Any, length: Double): ArrayBuffer = js.native

@js.native
private[librdkafka] trait KoffiLibrary extends js.Object:
  /** A function from its C prototype, which runs on the calling thread. */
  def func(prototype: String): js.Dynamic = js.native
