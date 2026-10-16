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
import scala.scalajs.js.annotation.JSGlobal

/** A Node buffer, which koffi passes to C as a pointer to its bytes. Every platform xkafka ships for is little-endian. */
@js.native
private[librdkafka] trait Buffer extends js.Object:
  def writeInt32LE(value: Int, offset: Int): Int               = js.native
  def readInt32LE(offset: Int): Int                            = js.native
  def writeBigInt64LE(value: js.BigInt, offset: Int): Int      = js.native
  def writeBigUInt64LE(value: js.BigInt, offset: Int): Int     = js.native
  def readBigInt64LE(offset: Int): js.BigInt                   = js.native
  def indexOf(value: Int): Int                                 = js.native
  def toString(encoding: String, start: Int, end: Int): String = js.native

@js.native
@JSGlobal("Buffer")
private[librdkafka] object Buffers extends js.Object:
  /** A zero-filled buffer whose memory lives outside the JavaScript heap, so it stays put while a worker thread writes to it. */
  def alloc(size: Int): Buffer = js.native
