name := "spark_dbscan"

organization := "org.alitouka"

version := "0.0.4"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "4.1.0",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
  "org.scalactic" %% "scalactic" % "3.2.18",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / test := {}
