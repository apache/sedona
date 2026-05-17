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
package org.apache.spark.sql.sedona_sql.io.geojson

import org.apache.sedona.common.Constructors.geomFromText
import org.apache.sedona.common.Functions.asGeoJson
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object GeoJSONUtils {

  def updateGeometrySchema(schema: StructType, datatype: DataType): StructType = {
    // If this struct already has a geometry field, only update that field and stop.
    val hasGeometry = hasGeometryField(schema)
    if (hasGeometry) {
      StructType(schema.fields.map {
        case StructField("geometry", _, nullable, metadata) =>
          StructField("geometry", datatype, nullable, metadata)
        case other =>
          other
      })
    } else {
      // Otherwise keep searching deeper for the first geometry field
      StructType(schema.fields.map {
        case StructField(name, st: StructType, nullable, metadata) =>
          StructField(name, updateGeometrySchema(st, datatype), nullable, metadata)

        case StructField(name, ArrayType(elem: StructType, containsNull), nullable, metadata) =>
          val updatedElem = updateGeometrySchema(elem, datatype)
          StructField(name, ArrayType(updatedElem, containsNull), nullable, metadata)

        case other => other
      })
    }
  }

  def geoJsonToGeometry(geoJson: String): Array[Byte] = {
    val geometry = geomFromText(geoJson, FileDataSplitter.GEOJSON)
    GeometryUDT.serialize(geometry)
  }

  def geometryToGeoJson(geometryBinary: Array[Byte]): String = {
    val geometry = GeometryUDT.deserialize(geometryBinary)
    asGeoJson(geometry)
  }

  def handleArray(
      row: InternalRow,
      index: Int,
      elementType: DataType,
      toGeometry: Boolean): ArrayData = {
    val arrayData = row.getArray(index)
    if (arrayData == null || arrayData.numElements() == 0)
      return new GenericArrayData(Seq.empty[Any])

    elementType match {
      case structType: StructType =>
        val convertedArray = (0 until arrayData.numElements()).map { i =>
          if (!arrayData.isNullAt(i)) {
            val innerRow = arrayData.getStruct(i, structType.fields.length)
            if (toGeometry) {
              convertGeoJsonToGeometry(innerRow, structType)
            } else {
              convertGeometryToGeoJson(innerRow, structType)
            }
          } else {
            null
          }
        }
        new GenericArrayData(convertedArray)
      case _ => arrayData
    }
  }

  def convertGeometryToGeoJson(row: InternalRow, schema: StructType): InternalRow = {
    val newValues = new Array[Any](schema.fields.length)

    schema.fields.zipWithIndex.foreach {
      case (StructField("geometry", _: GeometryUDT, _, _), index) =>
        val geometryBinary = row.getBinary(index)
        newValues(index) = UTF8String.fromString(geometryToGeoJson(geometryBinary))
      case (StructField(_, structType: StructType, _, _), index) =>
        val nestedRow = row.getStruct(index, structType.fields.length)
        newValues(index) = convertGeometryToGeoJson(nestedRow, structType)
      case (StructField(_, arrayType: ArrayType, _, _), index) =>
        newValues(index) = handleArray(row, index, arrayType.elementType, false)
      case (_, index) =>
        newValues(index) = row.get(index, schema.fields(index).dataType)
    }

    InternalRow.fromSeq(newValues)
  }

  private def hasGeometryField(st: StructType): Boolean =
    st.fields.exists(_.name == "geometry")

  def convertGeoJsonToGeometry(row: InternalRow, schema: StructType): InternalRow = {
    val newValues = new Array[Any](schema.fields.length)

    // This struct is the geometry level if it has a geometry field at this level
    val geometryLevel = hasGeometryField(schema)

    schema.fields.zipWithIndex.foreach {

      // Convert geometry ONLY at the first geometry level
      case (StructField("geometry", StringType, _, _), index) if geometryLevel =>
        newValues(index) =
          if (row.isNullAt(index)) null
          else geoJsonToGeometry(row.getString(index))

      // If we've reached the geometry level, do NOT recurse further
      case (sf @ StructField(_, _: StructType, _, _), index) if geometryLevel =>
        newValues(index) =
          if (row.isNullAt(index)) null
          else row.get(index, sf.dataType)

      case (sf @ StructField(_, _: ArrayType, _, _), index) if geometryLevel =>
        newValues(index) =
          if (row.isNullAt(index)) null
          else row.get(index, sf.dataType)

      // Otherwise, recurse until the first geometry level is reached
      case (StructField(_, structType: StructType, _, _), index) =>
        newValues(index) =
          if (row.isNullAt(index)) null
          else {
            val nestedRow = row.getStruct(index, structType.fields.length)
            convertGeoJsonToGeometry(nestedRow, structType)
          }

      case (StructField(_, arrayType: ArrayType, _, _), index) =>
        newValues(index) =
          if (row.isNullAt(index)) null
          else handleArray(row, index, arrayType.elementType, toGeometry = true)

      // Primitives
      case (_, index) =>
        newValues(index) =
          if (row.isNullAt(index)) null
          else row.get(index, schema.fields(index).dataType)
    }

    InternalRow.fromSeq(newValues)
  }

}
