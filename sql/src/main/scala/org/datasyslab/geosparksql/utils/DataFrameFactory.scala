package org.datasyslab.geosparksql.utils
import com.vividsolutions.jts.geom.Geometry

import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import scala.collection.JavaConversions._

object DataFrameFactory {
  def geometryDFFromShapeFile(spark: org.apache.spark.sql.SparkSession, folderPath: String): org.apache.spark.sql.DataFrame ={
    val srdd = new SpatialRDD[Geometry]
    srdd.rawSpatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, folderPath)
    val df = Adapter.toDf(srdd, spark)
    val fieldNames = ShapefileReader.readFieldNames(spark.sparkContext, folderPath)
    if(fieldNames != null)
      df.toDF(fieldNames: _*)
    else df
  }
}
