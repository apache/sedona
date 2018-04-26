/*
 * FILE: Adapter.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geosparksql.utils


import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.geosparksql.GeometryWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD

object Adapter {

  def toJavaRdd(dataFrame: DataFrame): JavaRDD[Geometry] = {
    return toRdd(dataFrame).toJavaRDD()
  }

  def toRdd(dataFrame: DataFrame): RDD[Geometry] = {
    return dataFrame.rdd.map[Geometry](f => f.get(0).asInstanceOf[Geometry])
  }

  def toSpatialRdd(dataFrame: DataFrame): SpatialRDD[Geometry] =
  {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = toRdd(dataFrame).toJavaRDD()
    spatialRDD
  }

  def toSpatialRdd(dataFrame: DataFrame, fieldNames: List[String]): SpatialRDD[Geometry] =
  {
    var spatialRDD = toSpatialRdd(dataFrame)
    import scala.collection.JavaConversions._
    spatialRDD.fieldNames = fieldNames
    spatialRDD
  }

  def toDf(spatialRDD: SpatialRDD[Geometry], fieldNames: List[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](f => Row.fromSeq(f.toString.split("\t").toSeq))
    var fieldArray = new Array[StructField](fieldNames.size+1)
    fieldArray(0) = StructField("geometry", StringType)
    for (i <- 1 to fieldArray.length - 1) fieldArray(i) = StructField(fieldNames(i-1), StringType)
    val schema = StructType(fieldArray)
    return sparkSession.createDataFrame(rowRdd, schema)
  }

  def toDf(spatialRDD: SpatialRDD[Geometry], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](f => Row.fromSeq(f.toString.split("\t").toSeq))
    var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
    fieldArray(0) = StructField("geometry", StringType)
    if (spatialRDD.fieldNames!=null)
    {
      for (i <- 1 to fieldArray.length - 1) fieldArray(i) = StructField(spatialRDD.fieldNames.get(i-1), StringType)
    }
    else {
      for (i <- 1 to fieldArray.length - 1) fieldArray(i) = StructField("_c" + i, StringType)
    }
    val schema = StructType(fieldArray)
    return sparkSession.createDataFrame(rowRdd, schema)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map[Row](f => {
      val seq1 = f._1.toString.split("\t").toSeq
      val seq2 = f._2.toString.split("\t").toSeq
      val result = seq1 ++ seq2
      Row.fromSeq(result)
    })
    val leftgeomlength = spatialPairRDD.rdd.take(1)(0)._1.toString.split("\t").length

    var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
    for (i <- 0 to fieldArray.length - 1) fieldArray(i) = StructField("_c" + i, StringType)
    fieldArray(0) = StructField("leftgeometry", StringType)
    fieldArray(leftgeomlength) = StructField("rightgeometry", StringType)
    val schema = StructType(fieldArray)
    return sparkSession.createDataFrame(rowRdd, schema)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], leftFieldnames: List[String], rightFieldNames: List[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map[Row](f => {
      val seq1 = f._1.toString.split("\t").toSeq
      val seq2 = f._2.toString.split("\t").toSeq
      val result = seq1 ++ seq2
      Row.fromSeq(result)
    })
    val leftgeomlength = spatialPairRDD.rdd.take(1)(0)._1.toString.split("\t").length
    val rightgeomlength = spatialPairRDD.rdd.take(1)(0)._2.toString.split("\t").length
    val leftgeometryName = List("leftgeometry")
    val rightgeometryName = List("rightgeometry")
    var fullFieldNames = leftgeometryName ++ leftFieldnames
    fullFieldNames = fullFieldNames ++ rightgeometryName
    fullFieldNames = fullFieldNames ++ rightFieldNames
    var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
    for (i <- 0 to fieldArray.length - 1) fieldArray(i) = StructField(fullFieldNames(i), StringType)
    val schema = StructType(fieldArray)
    return sparkSession.createDataFrame(rowRdd, schema)
  }

  /*
   * Since UserDefinedType is hidden from users. We cannot directly return spatialRDD to spatialDf.
   * Let's wait for Spark side's change
   */
  /*
  def toSpatialDf(spatialRDD: SpatialRDD[Geometry], sparkSession: SparkSession): DataFrame =
  {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](f =>
      {
        var seq = Seq(new GeometryWrapper(f))
        var otherFields = f.getUserData.asInstanceOf[String].split("\t").toSeq
        seq :+ otherFields
        Row.fromSeq(seq)
      }
      )
    var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
    fieldArray(0) = StructField("rddshape", ArrayType(ByteType, false))
    for (i <- 1 to fieldArray.length-1) fieldArray(i) = StructField("_c"+i, StringType)
    val schema = StructType(fieldArray)
    return sparkSession.createDataFrame(rowRdd, schema)
  }
  */
}
