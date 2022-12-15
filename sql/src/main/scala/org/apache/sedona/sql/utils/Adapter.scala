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

import java.sql.{Date, Timestamp}

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
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
    toDf(spatialRDD = spatialRDD, fieldNames = null, sparkSession = sparkSession);
  }

  def toDf[T <: Geometry](spatialRDD: SpatialRDD[T], fieldNames: Seq[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](geom => {
      val stringRow = extractUserData(geom)
      Row.fromSeq(stringRow)
    })
    var cols:Seq[StructField] = Seq(StructField("geometry", GeometryUDT))
    if (fieldNames != null && fieldNames.nonEmpty) {
      cols = cols ++ fieldNames.map(f => StructField(f, StringType))
    }
    val schema = StructType(cols)
    sparkSession.createDataFrame(rowRdd, schema)
  }

  /**
    * Convert a spatial RDD to DataFrame with a given schema.
    *
    * @param spatialRDD Spatial RDD
    * @param schema Desired schema
    * @param sparkSession Spark Session
    * @tparam T Geometry
    * @return DataFrame with the specified schema
    */
  def toDf[T <: Geometry](spatialRDD: SpatialRDD[T], schema: StructType, sparkSession: SparkSession): DataFrame = {
    val rdd = spatialRDD.rawSpatialRDD.rdd.map[Row](geom => {
      val stringRow = extractUserData(geom)
      castRowToSchema(stringRow = stringRow, schema = schema)
    })

    sparkSession.sqlContext.createDataFrame(rdd, schema)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], sparkSession: SparkSession): DataFrame = {
    toDf(spatialPairRDD, null, null, sparkSession)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], leftFieldnames: Seq[String], rightFieldNames: Seq[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map(f => {
      val stringRow = extractUserData(row = f, leftFieldnames = leftFieldnames, rightFieldnames = rightFieldNames)
      Row.fromSeq(stringRow)
    })
    var cols:Seq[StructField] = Seq(StructField("leftgeometry", GeometryUDT))
    if (leftFieldnames != null && leftFieldnames.nonEmpty) cols = cols ++ leftFieldnames.map(fName => StructField(fName, StringType))
    cols = cols ++ Seq(StructField("rightgeometry", GeometryUDT))
    if (rightFieldNames != null && rightFieldNames.nonEmpty) cols = cols ++ rightFieldNames.map(fName => StructField(fName, StringType))
    val schema = StructType(cols)
    sparkSession.createDataFrame(rowRdd, schema)
  }

  /**
    * Convert a JavaPairRDD to DataFrame with a specified schema.
    *
    * The schema is expected to follow the format:
    *   [left geometry, (left user data columns), right geometry, (right user data columns)]
    *
    * Note that the schema _must_ provide columns for left and right user data, even if they do not exist in the
    * original data.
    *
    * Also note that some complex types, such as `MapType`, and schemas as strings are not supported.
    *
    * @param spatialPairRDD
    * @param schema Desired output schema
    * @param sparkSession Spark session
    * @return Spatial pair RDD as a DataFrame with the desired schema
    */
  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], schema: StructType, sparkSession: SparkSession): DataFrame = {
    val rdd = spatialPairRDD.rdd.map(f => {
      // Extract user data from geometries
      // Use an empty seq to grab all col names -- we will rename to fit the schema anyways
      val stringRow = extractUserData(row = f, leftFieldnames = Seq(""), rightFieldnames = Seq(""))

      // Convert data types
      castRowToSchema(stringRow = stringRow, schema = schema)
    })

    sparkSession.sqlContext.createDataFrame(rdd, schema)
  }

  /**
    * Extract user data from a geometry.
    *
    * @param geom Geometry
    * @return Sequence of Geometry object and its user data as Strings
    */
  private def extractUserData(geom: Geometry): Seq[Any] = {
    val userData = geom.getUserData
    val geomWithoutUserData = geom.copy
    geomWithoutUserData.setUserData(null)

    if (userData != null)
      geomWithoutUserData +: userData.asInstanceOf[String].split("\t", -1)
    else
      Seq(geom)
  }

  /**
    * Extract user data from left and right geometries.
    *
    * @param row Pair of Geometry objects
    * @param leftFieldnames Left geometry's field names to pull. If null or an empty sequence, user data is not extracted.
    * @param rightFieldnames Right geometry's field names to pull. If null or an empty sequence, user data is not extracted.
    * @return Sequence of Geometry objects and their user data as Strings
    */
  private def extractUserData(row: (Geometry, Geometry), leftFieldnames: Seq[String], rightFieldnames: Seq[String]): Seq[Any] = {
    val left = getGeomAndFields(row._1, leftFieldnames)
    val right = getGeomAndFields(row._2, rightFieldnames)

    val leftGeom = left._1
    val leftUserData = left._2
    val rightGeom = right._1
    val rightUserData = right._2

    leftGeom ++ leftUserData ++ rightGeom ++ rightUserData
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

  /**
    * Generate a Row with desired schema from a sequence of Geometry and String objects.
    *
    * @param stringRow Sequence of Geometry objects and String user data
    * @param schema Desired output schema
    * @return Row with the specified schema
    */
  private def castRowToSchema(stringRow: Seq[Any], schema: StructType): Row = {
    val parsedRow = stringRow.zipWithIndex.map { case (value, idx) =>
      val desiredDataType = schema(idx).dataType
      // Don't convert geometry data, only user data
      if (desiredDataType == GeometryUDT) value else parseString(value.toString, desiredDataType)
    }

    Row.fromSeq(parsedRow)
  }

  /**
    * Parse a string to another data type based on the desired schema.
    *
    * @param data Data stored as string
    * @param desiredType Desired SparkSQL data type
    * @return Parsed value, or in the case of a struct column, an array of parsed values
    */
  private def parseString(data: String, desiredType: DataType): Any = {
    // Spark needs to know how to serialize null values, so we have to provide the relevant class
    if (data == "null") {
      return desiredType match {
        case _: ByteType => null.asInstanceOf[Byte]
        case _: ShortType => null.asInstanceOf[Short]
        case _: IntegerType => null.asInstanceOf[Integer]
        case _: LongType => null.asInstanceOf[Long]
        case _: FloatType => null.asInstanceOf[Float]
        case _: DoubleType => null.asInstanceOf[Double]
        case _: DateType => null.asInstanceOf[Date]
        case _: TimestampType => null.asInstanceOf[Timestamp]
        case _: BooleanType => null.asInstanceOf[Boolean]
        case _: StringType => null.asInstanceOf[String]
        case _: BinaryType => null.asInstanceOf[Array[Byte]]
        case _: StructType => null.asInstanceOf[StructType]
      }
    }

    desiredType match {
      case _: ByteType => data.toByte
      case _: ShortType => data.toShort
      case _: IntegerType => data.toInt
      case _: LongType => data.toLong
      case _: FloatType => data.toFloat
      case _: DoubleType => data.toDouble
      case _: DateType => Date.valueOf(data)
      case _: TimestampType => Timestamp.valueOf(data)
      case _: BooleanType => data.toBoolean
      case _: StringType => data
      case _: StructType =>
        val desiredStructSchema = desiredType.asInstanceOf[StructType]
        new GenericRowWithSchema(parseStruct(data, desiredStructSchema), desiredStructSchema)
    }
  }

  /**
    * Parse the string representation of a struct into an array of values.
    *
    * @param data Struct as a string (format: "[value1,value2,...]")
    * @param desiredType Desired schema for the struct column
    * @return Array of parsed values
    */
  private def parseStruct(data: String, desiredType: StructType): Array[Any] = {
    // Structs are stored as "[value1,value2,...]"
    // Recurse into the struct and parse
    val csv = data.slice(1, data.length - 1)  // Remove brackets
    csv.split(",").zipWithIndex.map { case (element, idx) =>
      val nestedField = desiredType.slice(idx, idx + 1).head
      val nestedDataType = nestedField.dataType
      parseString(element, nestedDataType)
    }
  }
}
