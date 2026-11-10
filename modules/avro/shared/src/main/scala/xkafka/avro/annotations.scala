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

package xkafka.avro

import scala.annotation.StaticAnnotation

/** The name a derived schema gives a record, enum or field in place of its Scala name, as avro4s's annotation of this name does. */
final class AvroName(val name: String) extends StaticAnnotation

/** The namespace a derived schema puts a record or enum in, in place of its Scala package. */
final class AvroNamespace(val namespace: String) extends StaticAnnotation

/** The documentation a derived schema gives a record, enum or field. */
final class AvroDoc(val doc: String) extends StaticAnnotation

/** Another name a record, enum or field was known by, so records written under that name are read; one annotation for each name. */
final class AvroAlias(val alias: String) extends StaticAnnotation

/** Leaves a field's Scala default out of its schema, so a reader requires the field to have been written. */
final class AvroNoDefault extends StaticAnnotation
