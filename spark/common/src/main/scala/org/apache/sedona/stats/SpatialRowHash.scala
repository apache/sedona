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
package org.apache.sedona.stats

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/** Row identity for stats algorithms; native spatial JSON includes WKB and its separate SRID. */
private[stats] object SpatialRowHash {
  def apply(schema: StructType): Column = {
    val row = if (SpatialTypeSupport.usesNativeTypes) {
      struct(schema.fields.map { field =>
        col(s"`${field.name.replace("`", "``")}`").as(field.name)
      }: _*)
    } else struct("*")
    hash(row, schema)
  }

  def hash(value: Column, dataType: DataType): Column =
    sha2(to_json(hashableColumn(value, dataType)), 256)

  private def containsNativeSpatial(dataType: DataType): Boolean =
    SpatialTypeSupport.isNativeSpatial(dataType) || (dataType match {
      case StructType(fields) => fields.exists(field => containsNativeSpatial(field.dataType))
      case ArrayType(elementType, _) => containsNativeSpatial(elementType)
      case MapType(keyType, valueType, _) =>
        containsNativeSpatial(keyType) || containsNativeSpatial(valueType)
      case _ => false
    })

  private[stats] def hashableColumn(value: Column, dataType: DataType): Column = {
    if (!containsNativeSpatial(dataType)) value
    else if (SpatialTypeSupport.isNativeSpatial(dataType)) {
      when(value.isNull, lit(null)).otherwise(
        struct(
          call_function("ST_AsBinary", value).as("wkb"),
          call_function("ST_SRID", value).as("srid")))
    } else
      dataType match {
        case StructType(fields) =>
          when(value.isNull, lit(null)).otherwise(struct(fields.map { field =>
            hashableColumn(value.getField(field.name), field.dataType).as(field.name)
          }: _*))
        case ArrayType(elementType, _) =>
          transform(value, element => hashableColumn(element, elementType))
        case MapType(keyType, valueType, _) =>
          // Entries retain spatial keys without coercing binary values to JSON object strings.
          transform(
            map_entries(value),
            entry =>
              struct(
                hashableColumn(entry.getField("key"), keyType).as("key"),
                hashableColumn(entry.getField("value"), valueType).as("value")))
      }
  }
}
