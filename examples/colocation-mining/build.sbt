import sbt.Keys.{libraryDependencies, version}


lazy val root = (project in file(".")).
  settings(
    name := "sedona-analysis",

    version := "0.1.0",

    scalaVersion := "2.12.11",

    organization := "org.apache.sedona",

    publishMavenStyle := true
  )

val SparkVersion = "3.0.1"

val SparkCompatibleVersion = "3.0"

val HadoopVersion = "2.7.2"

val SedonaVersion = "1.0.0-incubator-SNAPSHOT"

val ScalaCompatibleVersion = "2.12"

val dependencyScope = "compile"

val geotoolsVersion = "24.0"

logLevel := Level.Warn

logLevel in assembly := Level.Error

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion % dependencyScope,
  "org.apache.sedona" % "sedona-core".concat("_").concat(ScalaCompatibleVersion) % SedonaVersion,
  "org.apache.sedona" % "sedona-sql-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion ,
  "org.apache.sedona" % "sedona-viz-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion,
  "org.locationtech.jts"% "jts-core"% "1.18.0" % "compile",
  "org.geotools" % "gt-main" % geotoolsVersion % "compile",
  "org.geotools" % "gt-referencing" % geotoolsVersion % "compile",
  "org.geotools" % "gt-epsg-hsql" % geotoolsVersion % "compile"
)

assemblyMergeStrategy in assembly := {
  case PathList("org.apache.sedona", "sedona-core", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case path if path.endsWith(".SF") => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers +=
  "Apache Software Foundation OSS Snapshots" at "https://repository.apache.org/content/groups/snapshots"

resolvers +=
  "Open Source Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/release/"

resolvers +=
  "Java.net repository" at "https://download.java.net/maven/2"