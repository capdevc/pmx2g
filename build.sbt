name := "PMX2G"

organization := "PYA Analytics"

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers ++= Seq("cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.0.M5b" % "test" withSources() withJavadoc(),
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-graphx" % "1.3.0" % "provided" withSources() withJavadoc(),
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided" withJavadoc(),
  "com.github.scopt" %% "scopt" % "3.2.0"
)


initialCommands := "import .pmx2g._"

