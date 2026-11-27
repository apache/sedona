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

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath, FileMetaData}
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.execution.datasources.geoparquet.internal.ParquetReadSupport
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

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
   *
   * @param columnOf
   *   the column of the file for a column of the filter, as [[geographyColumn]] finds it. None
   *   keeps every row group for that column.
   */
  def select(
      rowGroups: Seq[BlockMetaData],
      filter: GeoParquetSpatialFilter,
      columnOf: String => Option[String] = column => Some(column)): Seq[BlockMetaData] =
    if (!supported || !filter.usesRowGroupStatistics) rowGroups
    else {
      val columns = mutable.Map.empty[String, Option[String]]
      rowGroups.filter { rowGroup =>
        val boxes = mutable.Map.empty[String, Option[Array[Double]]]
        filter.evaluateRowGroup(name =>
          boxes.getOrElseUpdate(
            name,
            columns.getOrElseUpdate(name, columnOf(name)).flatMap(geographyBox(rowGroup, _))))
      }
    }

  /**
   * The column of a file that a read takes the geographies of `field` from, for a filter on
   * `field` that skips row groups by the statistics of that column. It is the column that the
   * reader of the file finds for the field: by the field id or by the name of the field, in
   * whichever case, as `conf` says. None for a field that the reader finds no column for, or more
   * than one.
   *
   * @param field
   *   a column of the schema of the read
   */
  def geographyColumn(
      field: StructField,
      fileMetaData: FileMetaData,
      conf: Configuration): Option[String] =
    Try(columns(field, fileMetaData.getSchema, conf)).toOption.collect { case Seq(column) =>
      column.getName
    }

  /**
   * Whether a scan may reject the column of the file for `field` at a value of it, and not before
   * it reads one. A scan compares the SRID of some columns with the SRID that the type of the
   * field names at their first value only, because a file without a value in a column fits any
   * SRID. A row group that a filter skips must not keep a scan from that value, so no row group
   * is skipped for a read with such a field.
   *
   * It is so for a field of a native Spark type, which names an SRID, and
   *   - a GEOGRAPHY column of another SRID
   *   - a GEOMETRY column that the file states no CRS for
   *   - a binary column that is read as geographies, each of which states its SRID
   *   - a geometry or a geography inside of a struct, an array or a map, which is not looked into
   *
   * @param field
   *   a column of the schema of the read
   * @param parameters
   *   the options of the read
   */
  def rejectsAtValue(
      field: StructField,
      fileMetaData: FileMetaData,
      conf: Configuration,
      parameters: Map[String, String]): Boolean = {
    val dataType = field.dataType
    def native(inside: DataType): Boolean = inside match {
      case array: ArrayType => native(array.elementType)
      case map: MapType => native(map.keyType) || native(map.valueType)
      case struct: StructType => struct.exists(inner => native(inner.dataType))
      case other => SpatialTypeSupport.isNativeSpatial(other)
    }
    def converter =
      new GeoParquetToSparkSchemaConverter(fileMetaData.getKeyValueMetaData, conf, parameters)
    if (!native(dataType)) false
    else
      // Whatever cannot be told of the column is left to a scan
      Try(columns(field, fileMetaData.getSchema, conf) match {
        // No value is read for a field that the file has no column for
        case Seq() => false
        case Seq(column) if SpatialTypeSupport.isGeography(dataType) =>
          ParquetGeospatialTypes.describe(column) match {
            case Some(described) if described.isGeography =>
              val stored = converter.geographySrid(column, native = true, column = true)
              SpatialTypeSupport.geographySrid(dataType).exists(_ != stored)
            // A GEOMETRY column, which a scan rejects before it reads a value
            case Some(_) => false
            case None => true
          }
        case Seq(column) if SpatialTypeSupport.isGeometry(dataType) =>
          column.isPrimitive && SpatialTypeSupport.geometrySrid(dataType).exists(_ != 0) &&
          converter.geometrySrid(column, native = true, column = true) == 0
        case _ => true
      }).getOrElse(true)
  }

  /** The columns of a file that the reader of the file finds for `field`. */
  private def columns(field: StructField, fileSchema: MessageType, conf: Configuration) =
    ParquetReadSupport
      .getRequestedSchema(
        fileSchema,
        GeoParquetSchemaConverter.physicalSchema(StructType(Seq(field))),
        conf,
        enableVectorizedReader = false)
      .getFields
      .asScala
      .map(found => fileSchema.getType(fileSchema.getFieldIndex(found.getName)))
      .toSeq

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
