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
package org.apache.spark.sql.sedona_sql.types

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, SpecializedGetters}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.Geometry

/** Version-specific boundary between Sedona expressions and Spark SQL spatial values. */
object SpatialTypeSupport {
  val usesNativeTypes: Boolean = false
  def unwrapGeometryInput(expression: Expression): Expression = expression
  def geometryType: DataType = GeometryUDT()
  def geometryType(srid: Int): DataType = geometryType
  def isNativeSpatial(dataType: DataType): Boolean = false
  def isGeometry(dataType: DataType): Boolean = dataType.isInstanceOf[GeometryUDT]
  def serializeGeometry(value: Geometry): Any =
    if (value == null) null else GeometryUDT.serialize(value)
  def serializeGeometry(value: Geometry, dataType: DataType): Any = serializeGeometry(value)
  def deserializeGeometry(value: Any, dataType: DataType): Geometry =
    if (value == null) null else GeometryUDT.deserialize(value)
  def readGeometry(row: SpecializedGetters, ordinal: Int, dataType: DataType): Geometry =
    if (row.isNullAt(ordinal)) null else deserializeGeometry(row.get(ordinal, dataType), dataType)
  def externalGeometry(value: Geometry): Any = value
  def fromExternalGeometry(value: Any): Geometry = value.asInstanceOf[Geometry]
  def adaptFunction(builder: FunctionBuilder): FunctionBuilder = builder
}
