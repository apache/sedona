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

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.Geometry

/** Version-selected geometry boundary used by the DataFrame/RDD adapters. */
object SpatialTypeSupport {
  def geometryType: DataType = GeometryUDT()
  def isGeometry(dataType: DataType): Boolean = dataType.isInstanceOf[GeometryUDT]
  def externalGeometry(value: Geometry): Any = value
  def externalGeometry(value: Geometry, dataType: DataType): Any = value
  def fromExternalGeometry(value: Any): Geometry = value.asInstanceOf[Geometry]
  def readGeometry(row: SpecializedGetters, ordinal: Int, dataType: DataType): Geometry =
    if (row.isNullAt(ordinal)) null else GeometryUDT.deserialize(row.get(ordinal, dataType))
}
