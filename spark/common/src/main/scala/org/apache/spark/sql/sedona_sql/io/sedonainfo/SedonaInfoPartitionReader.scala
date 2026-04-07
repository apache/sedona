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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.net.URI

/**
 * Reads raster file metadata by delegating to format-specific [[RasterFileMetadataExtractor]]
 * implementations. Produces one [[InternalRow]] per file matching the readDataSchema.
 */
class SedonaInfoPartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    readDataSchema: StructType)
    extends PartitionReader[InternalRow] {

  private var currentFileIndex = 0
  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (currentFileIndex < partitionedFiles.length) {
      currentRow = readFileMetadata(partitionedFiles(currentFileIndex))
      currentFileIndex += 1
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}

  private def readFileMetadata(partition: PartitionedFile): InternalRow = {
    val path = new Path(new URI(partition.filePath.toString()))
    val extractor = SedonaInfoPartitionReader.findExtractor(path)
    val requiredFields = readDataSchema.fieldNames.toSet
    val meta = extractor.extract(path, partition.fileSize, configuration, requiredFields)
    SedonaInfoPartitionReader.toInternalRow(meta, readDataSchema)
  }
}

object SedonaInfoPartitionReader {

  /** Registered metadata extractors. Add new format extractors here. */
  private val extractors: Seq[RasterFileMetadataExtractor] = Seq(GeoTiffMetadataExtractor)

  def findExtractor(path: Path): RasterFileMetadataExtractor = {
    extractors
      .find(_.canHandle(path))
      .getOrElse(
        throw new UnsupportedOperationException(
          s"No metadata extractor found for file: ${path.getName}. " +
            s"Supported formats: ${extractors.map(_.driver).mkString(", ")}"))
  }

  def toInternalRow(meta: RasterFileMetadata, readDataSchema: StructType): InternalRow = {
    val gt = meta.geoTransform
    val geoTransformRow = new GenericInternalRow(
      Array[Any](gt.upperLeftX, gt.upperLeftY, gt.scaleX, gt.scaleY, gt.skewX, gt.skewY))

    val cc = meta.cornerCoordinates
    val cornerCoordinatesRow =
      new GenericInternalRow(Array[Any](cc.minX, cc.minY, cc.maxX, cc.maxY))

    lazy val bandsArray: ArrayData = {
      val bands = meta.bands.map { b =>
        new GenericInternalRow(
          Array[Any](
            b.band,
            if (b.dataType != null) UTF8String.fromString(b.dataType) else null,
            if (b.colorInterpretation != null) UTF8String.fromString(b.colorInterpretation)
            else null,
            if (b.noDataValue != null) b.noDataValue.doubleValue() else null,
            b.blockWidth,
            b.blockHeight,
            if (b.description != null) UTF8String.fromString(b.description) else null,
            if (b.unit != null) UTF8String.fromString(b.unit) else null))
      }.toArray
      new GenericArrayData(bands)
    }

    lazy val overviewsArray: ArrayData = {
      val overviews = meta.overviews.map { o =>
        new GenericInternalRow(Array[Any](o.level, o.width, o.height))
      }.toArray
      new GenericArrayData(overviews)
    }

    lazy val metadataMap: Any = {
      if (meta.metadata.isEmpty) null
      else {
        val entries = meta.metadata.toSeq
        org.apache.spark.sql.catalyst.util.ArrayBasedMapData(
          entries.map { case (k, _) => UTF8String.fromString(k) }.toArray,
          entries.map { case (_, v) => UTF8String.fromString(v) }.toArray)
      }
    }

    val fields = readDataSchema.fieldNames.map {
      case "path" => UTF8String.fromString(meta.path)
      case "driver" => UTF8String.fromString(meta.driver)
      case "fileSize" => meta.fileSize: Any
      case "width" => meta.width: Any
      case "height" => meta.height: Any
      case "numBands" => meta.numBands: Any
      case "srid" => meta.srid: Any
      case "crs" =>
        if (meta.crs != null) UTF8String.fromString(meta.crs) else null
      case "geoTransform" => geoTransformRow
      case "cornerCoordinates" => cornerCoordinatesRow
      case "bands" => bandsArray
      case "overviews" => overviewsArray
      case "metadata" => metadataMap
      case "isTiled" => meta.isTiled: Any
      case "compression" =>
        if (meta.compression != null) UTF8String.fromString(meta.compression) else null
      case other =>
        throw new IllegalArgumentException(s"Unsupported field name: $other")
    }

    new GenericInternalRow(fields)
  }
}
