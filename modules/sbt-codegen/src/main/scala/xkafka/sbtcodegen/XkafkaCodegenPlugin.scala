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

/** Generates Scala classes from the Avro schemas in `src/main/avro` while a project compiles, with xkafka-codegen run in a JVM of its own. */
object XkafkaCodegenPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  override def requires: Plugins = plugins.JvmPlugin

  object autoImport {
    val xkafkaAvroSources = settingKey[Seq[File]]("Directories whose .avsc and .avdl files, at any depth, Scala classes are generated from")

    val xkafkaCodegenVersion = settingKey[String]("The version of xkafka-codegen that generates the classes")

    // The task keeps track itself of when its classes need generating again, so sbt 2 does not cache it.
    @transient
    val xkafkaAvroGenerate = taskKey[Seq[File]]("Generates Scala classes from the Avro schemas in xkafkaAvroSources")
  }

  import autoImport._

  override def projectSettings: Seq[Setting[?]] =
    Seq(xkafkaCodegenVersion := PluginVersion.value) ++ inConfig(Compile)(Seq(
      xkafkaAvroSources  := Seq(sourceDirectory.value / "avro"),
      xkafkaAvroGenerate := generate.value,
      sourceGenerators += xkafkaAvroGenerate.taskValue
    ))

  private def generate: Def.Initialize[Task[Seq[File]]] =
    Def.task {
      val log     = streams.value.log
      val schemas = xkafkaAvroSources.value.flatMap(directory => (directory ** ("*.avsc" || "*.avdl")).get()).sorted
      val output  = sourceManaged.value / "xkafka-codegen"
      val version = xkafkaCodegenVersion.value
      val cache   = streams.value.cacheDirectory / s"xkafka-codegen-$version"
      val resolve = dependencyResolution.value
      val java    = javaHome.value
      if (schemas.isEmpty) {
        IO.delete(output)
        Nil
      } else {
        // The schemas are generated again when any of them changes, or the generated classes are gone.
        val run =
          FileFunction.cached(cache / "inputs", FilesInfo.hash, FilesInfo.exists) { _ =>
            IO.delete(output)
            val tool =
              resolve.retrieve("io.github.pmbmegalom" % "xkafka-codegen_3" % version, None, cache / "tool", log) match {
                case Left(unresolved) => throw unresolved.resolveException
                case Right(files)     => files
              }
            val options   = ForkOptions().withJavaHome(java).withOutputStrategy(OutputStrategy.LoggedOutput(log))
            val arguments = Seq("-cp", Path.makeString(tool), "xkafka.codegen.Main", output.getPath) ++ schemas.map(_.getPath)
            if (Fork.java(options, arguments) != 0) throw new sbt.internal.util.MessageOnlyException(
              "xkafka-codegen could not generate classes from the Avro schemas, for the reasons above"
            )
            (output ** "*.scala").get().toSet
          }
        run(schemas.toSet).toSeq.sorted
      }
    }
}
