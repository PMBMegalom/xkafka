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

import scala.concurrent.duration.*
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

import cats.effect.IO
import munit.CatsEffectSuite

@js.native
@JSImport("v8", JSImport.Namespace)
private object V8 extends js.Object:
  def setFlagsFromString(flags: String): Unit = js.native

@js.native
@JSImport("vm", JSImport.Namespace)
private object Vm extends js.Object:
  def runInNewContext(code: String): js.Function0[Unit] = js.native

final class CompletionsSuite extends CatsEffectSuite:
  /** Node's own collector, which a test may only run once it is told to expose it. */
  private lazy val collect: js.Function0[Unit] =
    V8.setFlagsFromString("--expose_gc")
    Vm.runInNewContext("gc")

  /** A weak reference to a buffer a call is given, which nothing else refers to. */
  private def started(): (js.Dynamic, Long) =
    val buffer = Buffers.alloc(1 << 20)
    val token  = Completions.expect(Seq(buffer))(() => ())
    (js.Dynamic.newInstance(js.Dynamic.global.WeakRef)(buffer), token)

  // A weak reference keeps its target until the job that made it ends, so each collection runs in a later one.
  private def collected(reference: js.Dynamic): IO[Boolean] = IO.sleep(10.millis) *> IO(collect()) *> IO(js.isUndefined(reference.deref()))

  test("the arguments of a call are kept until it reports, and may be collected once it has"):
    val (reference, token) = started()
    for
      whileRunning <- collected(reference)
      _            <- IO(Completions.forget(token))
      afterwards   <- collected(reference)
    yield
      assert(!whileRunning, "a running call's argument was collected")
      assert(afterwards, "an argument was still kept after its call ended")
