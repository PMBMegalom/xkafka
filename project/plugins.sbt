// addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.4.0")
// addSbtPlugin("org.portable-scala" % "sbt-scala-native-crossproject" % "1.4.0")
addSbtPlugin("org.scala-js"     % "sbt-scalajs"         % "1.22.0")
addSbtPlugin("org.scala-native" % "sbt-scala-native"    % "0.5.12")
addSbtPlugin("ch.epfl.scala"    % "sbt-scalajs-bundler" % "0.21.1")
addSbtPlugin("org.typelevel"    % "sbt-typelevel"       % "0.8.7")
addSbtPlugin("org.typelevel"    % "sbt-typelevel-site"  % "0.8.7")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"       % "0.14.0")
addSbtPlugin("com.thesamet"     % "sbt-protoc"          % "1.0.8")

// ScalaPB generates the Scala messages the ScalaPB bridge's tests read and write.
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.21"

// smithy4s generates the Scala types the smithy4s bridge's tests read and write, from Smithy files.
addSbtPlugin("com.disneystreaming.smithy4s" % "smithy4s-sbt-codegen" % "0.19.13")

// avrohugger generates the case classes the avrohugger tests derive codecs for, from Avro files, as an application's build would.
addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.18.0")
