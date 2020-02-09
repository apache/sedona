name := "geo_wrapper"

version := "0.3.0"

scalaVersion := "2.11.8"

val SparkVersion = "2.4.0"

val SparkCompatibleVersion = "2.3"

val GeoSparkVersion = "1.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz_".concat(SparkCompatibleVersion) % GeoSparkVersion
)
