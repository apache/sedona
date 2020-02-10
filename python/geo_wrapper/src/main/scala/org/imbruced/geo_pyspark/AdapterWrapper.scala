package org.imbruced.geo_pyspark

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter

import scala.collection.JavaConversions._

object AdapterWrapper {
  def toSpatialRdd(dataFrame: DataFrame, fieldNames: java.util.ArrayList[String]): SpatialRDD[Geometry] = {
    Adapter.toSpatialRdd(dataFrame, fieldNames.toList)
  }

  def toDf[T <:Geometry](spatialRDD: SpatialRDD[T], fieldNames: java.util.ArrayList[String], sparkSession: SparkSession): DataFrame = {
    Adapter.toDf(spatialRDD, fieldNames.toList, sparkSession)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry],
           leftFieldnames: java.util.ArrayList[String],
           rightFieldNames: java.util.ArrayList[String],
           sparkSession: SparkSession): DataFrame = {
    Adapter.toDf(spatialPairRDD, leftFieldnames.toList, rightFieldNames.toList, sparkSession)
  }
}
