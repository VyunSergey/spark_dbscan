name := "spark_dbscan"

organization := "org.alitouka"

version := "0.0.4"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.2.18" % Test

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / test := {}
