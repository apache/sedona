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
package org.apache.sedona.sql

import org.apache.sedona.common.S2Geography.{Geography, WKBGeography}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.locationtech.jts.geom.Geometry

/** Spatial result access for semantic tests across external SQL representations. */
object SpatialTestValues {
  def geometry(value: Any): Geometry = SpatialTypeSupport.fromExternalGeometry(value)

  def geometry(row: Row, ordinal: Int): Geometry = geometry(row.get(ordinal))

  def geometry(row: Row, field: String): Geometry = geometry(row, row.fieldIndex(field))

  def geography(value: Any): Geography = value match {
    case null => null
    case geography: Geography => geography
    case native if native.getClass.getName == "org.apache.spark.sql.types.Geography" =>
      // The public native value class is absent from older Spark compile profiles.
      val wkb = native.getClass.getMethod("getBytes").invoke(native).asInstanceOf[Array[Byte]]
      val srid = native.getClass.getMethod("getSrid").invoke(native).asInstanceOf[Int]
      WKBGeography.fromWKB(wkb, srid)
    case other =>
      throw new IllegalArgumentException(s"Expected an external geography, got $other")
  }

  def geography(row: Row, ordinal: Int): Geography = geography(row.get(ordinal))

  def geography(row: Row, field: String): Geography = geography(row, row.fieldIndex(field))
}
