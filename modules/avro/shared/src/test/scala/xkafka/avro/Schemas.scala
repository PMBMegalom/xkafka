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

/** Schemas covering every kind of Avro type, as `.avsc` JSON. */
object Schemas:
  val everything: String =
    """{"type":"record","name":"Order","namespace":"com.example","doc":"An order","fields":[
      |{"name":"nothing","type":"null"},
      |{"name":"paid","type":"boolean"},
      |{"name":"count","type":"int"},
      |{"name":"id","type":"long","doc":"Its id","aliases":["number"]},
      |{"name":"ratio","type":"float"},
      |{"name":"total","type":"double"},
      |{"name":"raw","type":"bytes","default":"ÿ\u0000a"},
      |{"name":"note","type":"string","default":"none"},
      |{"name":"status","type":{"type":"enum","name":"Status","symbols":["OPEN","PAID","SHIPPED"],"default":"OPEN"}},
      |{"name":"hash","type":{"type":"fixed","name":"Hash","namespace":"com.example.crypto","size":4}},
      |{"name":"previous","type":["null","com.example.crypto.Hash"],"default":null},
      |{"name":"lines","type":{"type":"array","items":{"type":"record","name":"Line","fields":[{"name":"sku","type":"string"},{"name":"quantity","type":"int","default":1}]}}},
      |{"name":"labels","type":{"type":"map","values":["null","long","string"]}},
      |{"name":"first","type":["null","Line"],"default":null},
      |{"name":"choice","type":["int","string","Status",{"type":"array","items":"long"}]},
      |{"name":"placed","type":{"type":"long","logicalType":"timestamp-millis"}},
      |{"name":"day","type":{"type":"int","logicalType":"date"}},
      |{"name":"key","type":{"type":"string","logicalType":"uuid"}},
      |{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
      |{"name":"tax","type":{"type":"fixed","name":"Tax","size":8,"logicalType":"decimal","precision":12,"scale":3}},
      |{"name":"local","type":{"type":"long","logicalType":"local-timestamp-micros"}},
      |{"name":"other","type":{"type":"record","name":"Other","namespace":"org.elsewhere","fields":[{"name":"line","type":"com.example.Line"}]}}
      |]}""".stripMargin

  /** A record that holds itself. */
  val list: String =
    """{"type":"record","name":"Node","fields":[{"name":"value","type":"int"},{"name":"next","type":["null","Node"],"default":null}]}"""

  val primitives: List[String] = List("\"null\"", "\"boolean\"", "\"int\"", "\"long\"", "\"float\"", "\"double\"", "\"bytes\"", "\"string\"")

  val all: List[String] = everything :: list :: primitives
