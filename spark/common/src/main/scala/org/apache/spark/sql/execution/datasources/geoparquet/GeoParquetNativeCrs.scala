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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.json4s.{JNull, JValue}

/** Native types need a representable column CRS; explicit legacy UDT reads remain permissive. */
private[geoparquet] object GeoParquetNativeCrs {
  def readSrid(column: String, crs: Option[JValue]): Int = {
    val srid = GeoParquetMetaData.extractSridFromCrs(crs)
    if (srid == 0 && crs.exists(_ != JNull)) {
      throw new IllegalArgumentException(
        s"GeoParquet column '$column' has a CRS that cannot be represented by a Spark native SRID")
    }
    srid
  }

  def requireSrid(column: String, expected: Int, actual: Int): Unit = {
    if (expected != actual) {
      throw new IllegalArgumentException(
        s"GeoParquet column '$column' requires SRID $expected but contains SRID $actual")
    }
  }

  def writeCrs(column: String, srid: Int, explicitCrs: Option[Option[JValue]]): Option[JValue] = {
    explicitCrs match {
      case Some(crs) =>
        requireSrid(column, srid, readSrid(column, crs))
        crs
      case None if srid == 0 => Some(JNull)
      case None if srid == GeoParquetMetaData.DEFAULT_SRID => None
      case None =>
        Some(GeoParquetMetaData.sridToProjJson(srid).getOrElse {
          throw new IllegalArgumentException(
            s"Cannot derive GeoParquet CRS metadata for column '$column' with SRID $srid; " +
              "provide matching geoparquet.crs metadata")
        })
    }
  }
}
