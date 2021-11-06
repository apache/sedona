/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql.utils

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.Geometry

object Adapter {

  /**
    * Convert a Spatial DF to a Spatial RDD. The geometry column can be at any place in the DF
    *
    * @param dataFrame
    * @param geometryFieldName
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName: String): SpatialRDD[Geometry] = {
    // Delete the field that have geometry
    if (dataFrame.schema.size == 1) {
      toSpatialRdd(dataFrame, 0, List[String]())
    }
    else {
      val fieldList = dataFrame.schema.toList.map(f => f.name)
      toSpatialRdd(dataFrame, geometryFieldName, fieldList.filter(p => !p.equalsIgnoreCase(geometryFieldName)))
    }
  }

  /**
    * Convert a Spatial DF to a Spatial RDD with a list of user-supplied col names (except geom col). The geometry column can be at any place in the DF.
    *
    * @param dataFrame
    * @param geometryFieldName
    * @param fieldNames
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName: String, fieldNames: Seq[String]): SpatialRDD[Geometry] = {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = toRdd(dataFrame, geometryFieldName).toJavaRDD()
    import scala.jdk.CollectionConverters._
    if (fieldNames != null && fieldNames.nonEmpty) spatialRDD.fieldNames = fieldNames.asJava
    else spatialRDD.fieldNames = null
    spatialRDD
  }

  private def toRdd(dataFrame: DataFrame, geometryFieldName: String): RDD[Geometry] = {
    val fieldList = dataFrame.schema.toList.map(f => f.name)
    val geomColId = fieldList.indexOf(geometryFieldName)
    assert(geomColId >= 0)
    toRdd(dataFrame, geomColId)
  }

  /**
    * Convert a Spatial DF to a Spatial RDD with a list of user-supplied col names (except geom col). The geometry column can be at any place in the DF.
    *
    * @param dataFrame
    * @param geometryColId
    * @param fieldNames
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryColId: Int, fieldNames: Seq[String]): SpatialRDD[Geometry] = {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = toRdd(dataFrame, geometryColId).toJavaRDD()
    import scala.jdk.CollectionConverters._
    if (fieldNames.nonEmpty) spatialRDD.fieldNames = fieldNames.asJava
    else spatialRDD.fieldNames = null
    spatialRDD
  }

  /**
    * Convert a Spatial DF to a Spatial RDD. The geometry column can be at any place in the DF
    *
    * @param dataFrame
    * @param geometryColId
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryColId: Int): SpatialRDD[Geometry] = {
    // Delete the field that have geometry
    if (dataFrame.schema.size == 1) {
      toSpatialRdd(dataFrame, 0, List[String]())
    }
    else {
      val fieldList = dataFrame.schema.toList.map(f => f.name)
      val geometryFieldName = fieldList(geometryColId)
      toSpatialRdd(dataFrame, geometryColId, fieldList.filter(p => !p.equalsIgnoreCase(geometryFieldName)))
    }
  }

  def toDf[T <: Geometry](spatialRDD: SpatialRDD[T], sparkSession: SparkSession): DataFrame = {
    import scala.jdk.CollectionConverters._
    if (spatialRDD.fieldNames != null) return toDf(spatialRDD, spatialRDD.fieldNames.asScala.toList, sparkSession)
    toDf(spatialRDD, null, sparkSession);
  }

  def toDf[T <: Geometry](spatialRDD: SpatialRDD[T], fieldNames: Seq[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](geom => {
      val userData = geom.getUserData
      val geomWithoutUserData = geom.copy
      geomWithoutUserData.setUserData(null)
      if (userData != null)
        Row.fromSeq(geomWithoutUserData +: userData.asInstanceOf[String].split("\t", -1))
      else
        Row.fromSeq(Seq(geom))
    })
    var cols:Seq[StructField] = Seq(StructField("geometry", GeometryUDT))
    if (fieldNames != null && fieldNames.nonEmpty) {
      cols = cols ++ fieldNames.map(f => StructField(f, StringType))
    }
    val schema = StructType(cols)
    sparkSession.createDataFrame(rowRdd, schema)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], sparkSession: SparkSession): DataFrame = {
    toDf(spatialPairRDD, null, null, sparkSession)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], leftFieldnames: Seq[String], rightFieldNames: Seq[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map(f => {
      val left = getGeomAndFields(f._1, leftFieldnames)
      val right = getGeomAndFields(f._2, rightFieldNames)
      Row.fromSeq(left._1 ++ left._2 ++ right._1 ++ right._2)
    })
    var cols:Seq[StructField] = Seq(StructField("leftgeometry", GeometryUDT))
    if (leftFieldnames != null && leftFieldnames.nonEmpty) cols = cols ++ leftFieldnames.map(fName => StructField(fName, StringType))
    cols = cols ++ Seq(StructField("rightgeometry", GeometryUDT))
    if (rightFieldNames != null && rightFieldNames.nonEmpty) cols = cols ++ rightFieldNames.map(fName => StructField(fName, StringType))
    val schema = StructType(cols)
    sparkSession.createDataFrame(rowRdd, schema)
  }

  private def toRdd(dataFrame: DataFrame, geometryColId: Int): RDD[Geometry] = {
    dataFrame.rdd.map[Geometry](f => {
      var geometry = f.get(geometryColId).asInstanceOf[Geometry]
      var fieldSize = f.size
      var userData: String = null
      if (fieldSize > 1) {
        userData = ""
        // Add all attributes into geometry user data
        for (i <- 0 until geometryColId) userData += f.get(i) + "\t"
        for (i <- geometryColId + 1 until f.size) userData += f.get(i) + "\t"
        userData = userData.dropRight(1)
      }
      geometry.setUserData(userData)
      geometry
    })
  }

  private def getGeomAndFields(geom: Geometry, fieldNames: Seq[String]): (Seq[Geometry], Seq[String]) = {
    if (fieldNames != null && fieldNames.nonEmpty) {
      val userData = "" + geom.getUserData.asInstanceOf[String]
      val fields = userData.split("\t")
      val geomWithoutUserData = geom.copy
      geomWithoutUserData.setUserData(null)
      (Seq(geomWithoutUserData), fields)
    }
    else (Seq(geom), Seq())
  }
}
