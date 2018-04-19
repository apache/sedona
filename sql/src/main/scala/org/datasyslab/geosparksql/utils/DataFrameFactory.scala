package org.datasyslab.geosparksql.utils
import com.vividsolutions.jts.geom.Geometry

import org.apache.hadoop.fs.{FileSystem, Path}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import scala.collection.JavaConversions._

object DataFrameFactory {
  def geometryDfFromShapeFile(spark: org.apache.spark.sql.SparkSession, path: String): org.apache.spark.sql.DataFrame ={
    val srdd = new SpatialRDD[Geometry]
    srdd.rawSpatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, path)
    val df = Adapter.toDf(srdd, spark)
    val inputPath = new Path(path)
    val fileSys = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val dbfs = fileSys.listStatus(inputPath).filter(i => i.getPath.getName.toLowerCase.contains("dbf"))
    if(dbfs.length>0) {
      val inputStream = fileSys.open(dbfs(0).getPath)
      val dbfParser = new DbfParseUtil
      dbfParser.parseFileHead(inputStream)
      val x = dbfParser.getFieldDescriptors
      val op = "rddshape" +: x.map(x => x.getFieldName).toSeq
      df.toDF(op: _*)
    } else df
  }
}
