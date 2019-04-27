name := "S3SparkDataLoader"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.test.loader",
  scalaVersion := "2.12.8",
  mainClass in Compile := Some("loaders.DataLoader")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)

assemblyMergeStrategy in assembly ~= { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}