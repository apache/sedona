/*
 * FILE: Adapter.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    return dataFrame.rdd.map[Geometry](f =>
      {
        var geometry = f.get(0).asInstanceOf[Geometry]
        var userData = ""
        // Add all attributes into geometry user data
        for (i <- 1 to f.size-1) if (i==1) {userData+=f.get(i)} else {userData+="\t"+f.get(i)}
        geometry.setUserData(userData)
        geometry
      })
  }

  @deprecated( "use toSpatialRdd and append geometry column's name", "1.2" )
  def toSpatialRdd(dataFrame: DataFrame): SpatialRDD[Geometry] =
  {
    toSpatialRdd(dataFrame, "geometry")
  }

  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName:String): SpatialRDD[Geometry] =
  {
    // Delete the field that have geometry name
    if (dataFrame.schema.size==1) {
      toSpatialRdd(dataFrame, List[String]())
    }
    else toSpatialRdd(dataFrame, dataFrame.schema.toList.map(f=>f.name.toString()).filter(p => !p.equalsIgnoreCase(geometryFieldName)))
  }

  def toSpatialRdd(dataFrame: DataFrame, fieldNames: List[String]): SpatialRDD[Geometry] =
  {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = toRdd(dataFrame).toJavaRDD()
    import scala.collection.JavaConversions._
    if (fieldNames.size!=0) spatialRDD.fieldNames = fieldNames
    else spatialRDD.fieldNames = null
    spatialRDD
  }

  def toDf[T <:Geometry](spatialRDD: SpatialRDD[T], fieldNames: List[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](f => Row.fromSeq(f.toString.split("\t",-1).toSeq))
    if (fieldNames!=null && fieldNames.nonEmpty)
    {
      var fieldArray = new Array[StructField](fieldNames.size+1)
      fieldArray(0) = StructField("geometry", StringType)
      for (i <- 1 to fieldArray.length - 1) fieldArray(i) = StructField(fieldNames(i-1), StringType)
      val schema = StructType(fieldArray)
      return sparkSession.createDataFrame(rowRdd, schema)
    }
    else {
      var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
      fieldArray(0) = StructField("geometry", StringType)
      for (i <- 1 to fieldArray.length - 1) fieldArray(i) = StructField("_c" + i, StringType)
      val schema = StructType(fieldArray)
      return sparkSession.createDataFrame(rowRdd, schema)
    }
  }

  def toDf[T <:Geometry](spatialRDD: SpatialRDD[T], sparkSession: SparkSession): DataFrame = {
    import scala.collection.JavaConverters._
    if (spatialRDD.fieldNames!=null) return toDf(spatialRDD, spatialRDD.fieldNames.asScala.toList, sparkSession)
    return toDf(spatialRDD, null, sparkSession);
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
