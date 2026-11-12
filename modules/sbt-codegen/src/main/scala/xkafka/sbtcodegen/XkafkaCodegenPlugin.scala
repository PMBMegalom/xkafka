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

package xkafka.sbtcodegen

import sbt._
import sbt.Keys._

/** Generates Scala classes from the Avro schemas in `src/main/avro` and the JSON Schema files in `src/main/json-schema` while a project compiles,
  * with xkafka-codegen run in a JVM of its own.
  */
object XkafkaCodegenPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  override def requires: Plugins = plugins.JvmPlugin

  object autoImport {
    val xkafkaAvroSources = settingKey[Seq[File]]("Directories whose .avsc and .avdl files, at any depth, Scala classes are generated from")

    val xkafkaJsonSchemaSources = settingKey[Seq[File]]("Directories whose .json files, at any depth, Scala classes are generated from")

    val xkafkaJsonSchemaPackage =
      settingKey[Option[String]]("The package of the classes of JSON Schema files whose titles name none, such as com.example.Order does")

    val xkafkaJsonSchemaBigDecimals = settingKey[Boolean]("Whether JSON Schema's numbers are BigDecimal, rather than Double")

    val xkafkaCodegenVersion = settingKey[String]("The version of xkafka-codegen that generates the classes")

    // The task keeps track itself of when its classes need generating again, so sbt 2 does not cache it.
    @transient
    val xkafkaGenerate = taskKey[Seq[File]]("Generates Scala classes from the schemas in xkafkaAvroSources and xkafkaJsonSchemaSources")
  }

  import autoImport._

  override def projectSettings: Seq[Setting[?]] =
    Seq(xkafkaCodegenVersion := PluginVersion.value, xkafkaJsonSchemaPackage := None, xkafkaJsonSchemaBigDecimals := false) ++ inConfig(Compile)(Seq(
      xkafkaAvroSources       := Seq(sourceDirectory.value / "avro"),
      xkafkaJsonSchemaSources := Seq(sourceDirectory.value / "json-schema"),
      xkafkaGenerate          := generate.value,
      sourceGenerators += xkafkaGenerate.taskValue
    ))

  private def generate: Def.Initialize[Task[Seq[File]]] =
    Def.task {
      val log     = streams.value.log
      val avro    = xkafkaAvroSources.value.flatMap(directory => (directory ** ("*.avsc" || "*.avdl")).get())
      val json    = xkafkaJsonSchemaSources.value.flatMap(directory => (directory ** "*.json").get())
      val schemas = (avro ++ json).distinct.sorted
      val output  = sourceManaged.value / "xkafka-codegen"
      val version = xkafkaCodegenVersion.value
      val cache   = streams.value.cacheDirectory / s"xkafka-codegen-$version"
      val options =
        xkafkaJsonSchemaPackage.value.toSeq.flatMap(name => Seq("--json-package", name)) ++
          (if (xkafkaJsonSchemaBigDecimals.value) Seq("--json-big-decimals") else Nil)
      val resolve = dependencyResolution.value
      val java    = javaHome.value
      // The options are kept in a file of their own, which the classes are generated again where it changes, as where a schema does.
      val settings = cache / "options"
      if (schemas.isEmpty) {
        IO.delete(output)
        Nil
      } else {
        // The classes are generated again when any schema or option changes, or the generated classes are gone.
        IO.write(settings, options.mkString("\n"))
        val run =
          FileFunction.cached(cache / "inputs", FilesInfo.hash, FilesInfo.exists) { _ =>
            IO.delete(output)
            val tool =
              resolve.retrieve("io.github.pmbmegalom" % "xkafka-codegen_3" % version, None, cache / "tool", log) match {
                case Left(unresolved) => throw unresolved.resolveException
                case Right(files)     => files
              }
            val fork      = ForkOptions().withJavaHome(java).withOutputStrategy(OutputStrategy.LoggedOutput(log))
            val arguments = Seq("-cp", Path.makeString(tool), "xkafka.codegen.Main") ++ options ++ Seq(output.getPath) ++ schemas.map(_.getPath)
            if (Fork.java(fork, arguments) != 0)
              throw new sbt.internal.util.MessageOnlyException("xkafka-codegen could not generate classes from the schemas, for the reasons above")
            (output ** "*.scala").get().toSet
          }
        run(schemas.toSet + settings).toSeq.sorted
      }
    }
}
