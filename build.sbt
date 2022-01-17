name := "SparkSchemaDemo"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"

autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.swaydb" %% "swaydb" % "0.16.2",
  "com.github.cb372" %% "scalacache-guava" % "0.26.0",
  "com.google.guava" % "guava" % "12.0",
  "com.github.cb372" %% "scalacache-core" % "0.28.0"

)

libraryDependencies ++= sparkDependencies
