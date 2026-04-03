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
package org.apache.spark.sql.sedona_sql.io.sedonainfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Trait for extracting metadata from raster files. Implementations handle format-specific logic
 * (e.g., GeoTIFF, NetCDF, etc.) and return a common [[RasterFileMetadata]] structure.
 */
trait RasterFileMetadataExtractor {

  /** Short driver name (e.g., "GTiff", "NetCDF"). */
  def driver: String

  /**
   * Extract metadata from the file at the given path. Implementations must not decode pixel data
   * — only headers/metadata should be read.
   */
  def extract(path: Path, fileSize: Long, configuration: Configuration): RasterFileMetadata

  /** Returns true if this extractor can handle the given file path (by extension). */
  def canHandle(path: Path): Boolean
}

/**
 * Common metadata structure returned by all format-specific extractors.
 */
case class RasterFileMetadata(
    path: String,
    driver: String,
    fileSize: Long,
    width: Int,
    height: Int,
    numBands: Int,
    srid: Int,
    crs: String,
    geoTransform: GeoTransformMetadata,
    cornerCoordinates: CornerCoordinatesMetadata,
    bands: Seq[BandMetadata],
    overviews: Seq[OverviewMetadata],
    metadata: Map[String, String],
    isTiled: Boolean,
    compression: String)

case class GeoTransformMetadata(
    upperLeftX: Double,
    upperLeftY: Double,
    scaleX: Double,
    scaleY: Double,
    skewX: Double,
    skewY: Double)

case class CornerCoordinatesMetadata(minX: Double, minY: Double, maxX: Double, maxY: Double)

case class BandMetadata(
    band: Int,
    dataType: String,
    colorInterpretation: String,
    noDataValue: java.lang.Double,
    blockWidth: Int,
    blockHeight: Int,
    description: String,
    unit: String)

case class OverviewMetadata(level: Int, width: Int, height: Int)
