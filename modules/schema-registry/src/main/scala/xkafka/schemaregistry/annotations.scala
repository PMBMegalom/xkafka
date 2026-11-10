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

package xkafka.schemaregistry

import scala.annotation.StaticAnnotation

/** The name a derived schema gives a type or field in place of its Scala name, in every schema language. */
final class SchemaName(val name: String) extends StaticAnnotation

/** The namespace a derived schema puts a type in, in place of its Scala package. */
final class SchemaNamespace(val namespace: String) extends StaticAnnotation

/** The documentation a derived schema gives a type or field. */
final class SchemaDoc(val doc: String) extends StaticAnnotation

/** Another name a type or field was known by, so records written under that name are read; one annotation for each name. */
final class SchemaAlias(val alias: String) extends StaticAnnotation

/** Leaves a field's Scala default out of its schema, so a reader requires the field to have been written. */
final class SchemaNoDefault extends StaticAnnotation
