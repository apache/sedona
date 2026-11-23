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

import org.json4s.{JNothing, JNull, JValue}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.jvalue2monadic

/** Native types need a representable column CRS; explicit legacy UDT reads remain permissive. */
private[geoparquet] object GeoParquetNativeCrs {
  def readSrid(column: String, crs: Option[JValue]): Int = {
    val srid = GeoParquetMetaData.extractSridFromCrs(crs)
    if (srid == 0 && crs.exists(_ != JNull)) rejectUnrepresentable(column)
    srid
  }

  def rejectUnrepresentable(column: String): Nothing =
    throw new IllegalArgumentException(
      s"GeoParquet column '$column' has a CRS that cannot be represented by a Spark native SRID")

  def rejectUnrepresentableGeography(column: String): Nothing =
    throw new GeoParquetColumnMismatchException(
      s"GeoParquet column '$column' has a CRS that Spark's native geography type cannot " +
        "represent. To read the column, give the reader a schema with Sedona's own geography " +
        "type for it")

  /**
   * The SRID of the CRS of a GEOGRAPHY column for a geography of Sedona's own geography type with
   * `srid`. A geography without an SRID is in OGC:CRS84, as a geography that Sedona constructs
   * without one is where Spark has a native geography type.
   */
  def geographySrid(srid: Int): Int = if (srid == 0) GeoParquetMetaData.DEFAULT_SRID else srid

  def requireSrid(column: String, expected: Int, actual: Int): Unit = {
    if (expected != actual) {
      throw new IllegalArgumentException(
        s"GeoParquet column '$column' requires SRID $expected but contains SRID $actual")
    }
  }

  /** As [[requireSrid]], for a column of a file that a read compares with its schema. */
  def requireColumnSrid(column: String, expected: Int, actual: Int): Unit = {
    if (expected != actual) {
      throw new GeoParquetColumnMismatchException(
        s"GeoParquet column '$column' requires SRID $expected but contains SRID $actual")
    }
  }

  def writeCrs(column: String, srid: Int, explicitCrs: Option[Option[JValue]]): Option[JValue] = {
    explicitCrs match {
      case Some(crs) =>
        requireSrid(column, srid, readSrid(column, crs))
        // The SRID of the column stands for the current meaning of its code. A code can mean
        // another CRS in another version of its registry, and there is no way to compare them.
        // The check is on the identifier that gave the SRID, which is id or an entry of ids.
        crs
          .flatMap(GeoParquetMetaData.sridIdentifier)
          .map(_._1 \ "version")
          .filter(_ != JNothing)
          .foreach { version =>
            throw new IllegalArgumentException(
              s"The CRS given for GeoParquet column '$column' takes SRID $srid from an " +
                s"identifier that names version ${compact(render(version))} of its registry. " +
                "The SRID of the column stands for what the code means today, and that " +
                "version can mean another CRS. Check which CRS the coordinates of the column " +
                s"are in. If it is the CRS that SRID $srid stands for today, give the CRS " +
                "without the version or leave the CRS to the SRID of the column. If it is " +
                "not, the SRID of the column is wrong")
          }
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
