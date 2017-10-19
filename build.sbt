name := "spark-2-streaming"
 
version := "1.0"

val sparkVersion = "2.0.2"
val connectorVersion = "2.0.2"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.first
}

scalaVersion := "2.11.8"

// still want to be able to run in sbt
// https://github.com/sbt/sbt-assembly#-provided-configuration
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")


// https://github.com/JetBrains/intellij-scala/wiki/%5BSBT%5D-How-to-use-provided-libraries-in-run-configurations
lazy val intellijRunner = project.in(file("intellijRunner")).dependsOn(RootProject(file("."))).settings(
  scalaVersion := "2.11.8",
  libraryDependencies ++= sparkDependencies.map(_ % "compile") ++ otherDependencies
).disablePlugins(sbtassembly.AssemblyPlugin)

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % connectorVersion // assumes running in DSE; move to `otherDependencies` if OSS
)

lazy val otherDependencies = Seq(
  "com.typesafe" % "config" % "1.3.1",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion
)

libraryDependencies ++= sparkDependencies.map(_ % "provided") ++ otherDependencies // for assembly plugin - see project/assembly.sbt
