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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}

/**
 * Picks the row groups of a Parquet file that a [[GeoParquetSpatialFilter]] has to read, from the
 * geospatial statistics of the row groups. The Parquet library has no filter on these statistics.
 */
object GeoParquetRowGroupFilter {
  // Sedona also compiles against Parquet libraries without geospatial statistics
  private val statistics =
    Try(classOf[ColumnChunkMetaData].getMethod("getGeospatialStatistics")).toOption

  /** Whether the Parquet library on the classpath reads the geospatial statistics of a file. */
  val supported: Boolean = statistics.isDefined

  /**
   * The row groups of `rowGroups` that may have a row that `filter` keeps, in their order. A row
   * group that the filter can tell nothing about is one of them.
   */
  def select(rowGroups: Seq[BlockMetaData], filter: GeoParquetSpatialFilter): Seq[BlockMetaData] =
    if (!supported || !filter.usesRowGroupStatistics) rowGroups
    else
      rowGroups.filter { rowGroup =>
        val boxes = mutable.Map.empty[String, Option[Array[Double]]]
        filter.evaluateRowGroup(column =>
          boxes.getOrElseUpdate(column, geographyBox(rowGroup, column)))
      }

  /**
   * The bounding box that `rowGroup` has for the geographies of `column`: the western, eastern,
   * southern and northern bound in degrees. None for a column that is not a GEOGRAPHY column with
   * spherical edges at the root of the file, whose box is of another kind, and without a box of
   * longitudes and latitudes.
   */
  private def geographyBox(rowGroup: BlockMetaData, column: String): Option[Array[Double]] =
    for {
      // A column at the root, whose name may have a dot
      chunk <- rowGroup.getColumns.asScala.find(_.getPath == ColumnPath.get(column))
      described <- ParquetGeospatialTypes.describe(chunk.getPrimitiveType)
      if described.isGeography
      if described.edges.forall(_ == ParquetGeospatialTypes.DEFAULT_EDGES)
      found <- Try(Option(statistics.get.invoke(chunk))).toOption.flatten
      box <- Try(
        Option(found.getClass.getMethod("getBoundingBox").invoke(found))).toOption.flatten
      bounds <- Try(Array("getXMin", "getXMax", "getYMin", "getYMax").map(name =>
        box.getClass.getMethod(name).invoke(box).asInstanceOf[Double])).toOption
      // The western bound is above the eastern bound across the antimeridian. Other bounds that
      // are not in order, or not a longitude or a latitude, are not a box of the sphere.
      if bounds.forall(bound => !bound.isNaN && !bound.isInfinity)
      if bounds(0) >= -180 && bounds(0) <= 180 && bounds(1) >= -180 && bounds(1) <= 180
      if bounds(2) >= -90 && bounds(3) <= 90 && bounds(2) <= bounds(3)
    } yield bounds
}
