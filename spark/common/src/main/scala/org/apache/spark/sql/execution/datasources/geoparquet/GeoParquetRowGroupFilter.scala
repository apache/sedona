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
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.execution.datasources.geoparquet.internal.{ParquetReadSupport, PortableSQLConf}
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.locationtech.jts.geom.Envelope

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
   *   the column of the file for a column of the filter, as [[fileColumn]] finds it. None keeps
   *   every row group for that column.
   */
  def select(
      rowGroups: Seq[BlockMetaData],
      filter: GeoParquetSpatialFilter,
      columnOf: String => Option[String] = column => Some(column)): Seq[BlockMetaData] =
    if (!supported || !filter.usesRowGroupStatistics) rowGroups
    else {
      val columns = mutable.Map.empty[String, Option[String]]
      rowGroups.filter { rowGroup =>
        val geographies = mutable.Map.empty[String, Option[Array[Double]]]
        val geometries = mutable.Map.empty[String, Option[Envelope]]
        def column(name: String) = columns.getOrElseUpdate(name, columnOf(name))
        filter.evaluateRowGroup(new GeoParquetSpatialFilter.RowGroupBoxes {
          override def geography(name: String): Option[Array[Double]] =
            geographies.getOrElseUpdate(name, column(name).flatMap(geographyBox(rowGroup, _)))
          override def geometry(name: String): Option[Envelope] =
            geometries.getOrElseUpdate(name, column(name).flatMap(geometryBox(rowGroup, _)))
        })
      }
    }

  /**
   * The column of a file that a read takes the values of `field` from, for a filter on `field`
   * that skips row groups by the statistics of that column. It is the column that the reader of
   * the file finds for the field: by the field id or by the name of the field, in whichever case,
   * as `conf` says. None for a field that the reader finds no column for, or more than one, and
   * for a column that the read does not give the values of to the field, see [[named]].
   *
   * @param field
   *   a column of the schema of the read
   * @param read
   *   the columns of the schema of the read
   */
  def fileColumn(
      field: StructField,
      fileMetaData: FileMetaData,
      conf: Configuration,
      read: Seq[StructField] = Nil): Option[String] =
    Try {
      val fileSchema = fileMetaData.getSchema
      columns(field, fileSchema, conf) match {
        case Seq(column) if named(column, field, read, fileSchema, conf) => Some(column.getName)
        case _ => None
      }
    }.toOption.flatten

  /**
   * Whether a read of the columns `read` gives the values of `column`, the column of a file that
   * the reader finds for `field`, to that field and those of no other column. The reader finds a
   * column by its field id when the session reads by field ids, and [[GeoParquetRowConverter]]
   * gives the values of a column to the field of its name, in whichever case if the session says
   * so. A column of another name than its field is therefore one whose values the field does not
   * get, and so is a column next to another one of that name that the read takes for another
   * field. The statistics of such a column say nothing of the values of the field.
   */
  private def named(
      column: Type,
      field: StructField,
      read: Seq[StructField],
      fileSchema: MessageType,
      conf: Configuration): Boolean = {
    val caseSensitive = conf.getBoolean(
      PortableSQLConf.CASE_SENSITIVE.key,
      PortableSQLConf.CASE_SENSITIVE.defaultValue.get)
    def ofField(name: String) =
      if (caseSensitive) name == field.name else name.equalsIgnoreCase(field.name)
    ofField(column.getName) && read
      .flatMap(columns(_, fileSchema, conf))
      .map(_.getName)
      .filter(ofField)
      .forall(_ == column.getName)
  }

  /**
   * Whether a scan may reject a value that `rowGroups` have for `field`. A scan compares the SRID
   * of some columns with the SRID that the type of the field names at their first value only,
   * because a file without a value in a column fits any SRID. A file or a row group that a filter
   * leaves out must not keep a scan from that value, so nothing of `rowGroups` is left out for a
   * read with such a field.
   *
   * It is so for a field of a native Spark type, which names an SRID, with a value in
   *   - a GEOGRAPHY column of another SRID
   *   - a GEOMETRY column that the file states no CRS for
   *   - a binary column that is read as geographies, each of which states its SRID. A column of
   *     text is one of them: it is stored as binary, and no geography is in it
   *
   * A field with a struct, an array or a map is compared as a whole: each geography of the file
   * in it with each geography of its type, and not only with the one that it is read as.
   *
   * @param field
   *   a column of the schema of the read
   * @param rowGroups
   *   the row groups of the read, which are looked at for a column that a scan may reject only
   * @param parameters
   *   the options of the read
   * @param read
   *   the columns of the schema of the read
   */
  def rejectsValue(
      field: StructField,
      fileMetaData: FileMetaData,
      rowGroups: => Seq[BlockMetaData],
      conf: Configuration,
      parameters: Map[String, String],
      read: Seq[StructField] = Nil): Boolean =
    // Whatever cannot be told of the column is left to a scan
    Try(rejected(field, fileMetaData, conf, parameters, read)).toOption.flatten.forall {
      columns =>
        columns.nonEmpty && rowGroups.exists(rowGroup => columns.exists(hasValue(rowGroup, _)))
    }

  /**
   * The columns of a file whose values a scan may reject for `field`, by their paths. None for a
   * field that the reader finds more than one column for, or a column of another name, which is
   * not the column that the field gets its values from.
   */
  private def rejected(
      field: StructField,
      fileMetaData: FileMetaData,
      conf: Configuration,
      parameters: Map[String, String],
      read: Seq[StructField]): Option[Seq[ColumnPath]] = {
    def native(dataType: DataType): Seq[DataType] = dataType match {
      case array: ArrayType => native(array.elementType)
      case map: MapType => native(map.keyType) ++ native(map.valueType)
      case struct: StructType => struct.flatMap(inner => native(inner.dataType))
      case other => Seq(other).filter(SpatialTypeSupport.isNativeSpatial)
    }
    val types = native(field.dataType)
    val fileSchema = fileMetaData.getSchema
    if (types.isEmpty) Some(Nil)
    else
      columns(field, fileSchema, conf) match {
        // No value is read for a field that the file has no column for
        case Seq() => Some(Nil)
        case Seq(column) if named(column, field, read, fileSchema, conf) =>
          val converter =
            new GeoParquetToSparkSchemaConverter(
              fileMetaData.getKeyValueMetaData,
              conf,
              parameters)
          val geographies = types.filter(SpatialTypeSupport.isGeography)
          val srids = geographies.flatMap(SpatialTypeSupport.geographySrid)
          val geometryWithSrid = types.exists(dataType =>
            SpatialTypeSupport.isGeometry(dataType) &&
              SpatialTypeSupport.geometrySrid(dataType).exists(_ != 0))
          // The GeoParquet metadata names the columns of a file and no field within them
          val top = column.isPrimitive
          def withoutCrs(leaf: PrimitiveType) = geometryWithSrid &&
            converter.geometrySrid(leaf, native = true, column = top) == 0
          Some(leaves(column, Seq(column.getName)).collect {
            case (path, leaf)
                if fileSchema.containsPath(path.toArray) && (
                  ParquetGeospatialTypes.describe(leaf) match {
                    case Some(described) if described.isGeography =>
                      val stored = converter.geographySrid(leaf, native = true, column = top)
                      srids.exists(_ != stored)
                    case Some(_) => withoutCrs(leaf)
                    case None =>
                      // A column is read as the spatial values that the field is of, whatever
                      // the file says is in its bytes. A field within a column may be one of
                      // text next to the spatial one, which is read as text.
                      leaf.getPrimitiveTypeName == PrimitiveTypeName.BINARY &&
                      (top || leaf.getLogicalTypeAnnotation == null) &&
                      (geographies.nonEmpty || withoutCrs(leaf))
                  }
                ) =>
              ColumnPath.get(path: _*)
          })
        case _ => None
      }
  }

  private def leaves(field: Type, path: Seq[String]): Seq[(Seq[String], PrimitiveType)] =
    if (field.isPrimitive) Seq(path -> field.asPrimitiveType)
    else
      field.asGroupType.getFields.asScala.toSeq.flatMap(inner =>
        leaves(inner, path :+ inner.getName))

  /** Whether `rowGroup` has a value in `column`, as far as its statistics tell. */
  private def hasValue(rowGroup: BlockMetaData, column: ColumnPath): Boolean =
    rowGroup.getColumns.asScala.find(_.getPath == column).forall { chunk =>
      val statistics = chunk.getStatistics
      statistics == null || !statistics.isNumNullsSet ||
      statistics.getNumNulls < chunk.getValueCount
    }

  /**
   * The columns of a file that the reader of the file finds for `field`, with the fields that the
   * read takes of them.
   */
  private def columns(field: StructField, fileSchema: MessageType, conf: Configuration) =
    ParquetReadSupport
      .getRequestedSchema(
        fileSchema,
        GeoParquetSchemaConverter.physicalSchema(StructType(Seq(field))),
        conf,
        enableVectorizedReader = false)
      .getFields
      .asScala
      .toSeq
      // A field that the file does not have is one of them when the read takes whole columns
      .filter(found => fileSchema.containsField(found.getName))

  /**
   * The bounding box that `rowGroup` has for the geographies of `column`: the western, eastern,
   * southern and northern bound in degrees. None for a column that is not a GEOGRAPHY column with
   * spherical edges at the root of the file, whose box is of another kind, and without a box of
   * longitudes and latitudes.
   */
  private def geographyBox(rowGroup: BlockMetaData, column: String): Option[Array[Double]] =
    for {
      bounds <- bounds(
        rowGroup,
        column,
        described =>
          described.isGeography &&
            described.edges.forall(_ == ParquetGeospatialTypes.DEFAULT_EDGES))
      // The western bound is above the eastern bound across the antimeridian. Other bounds that
      // are not in order, or not a longitude or a latitude, are not a box of the sphere.
      if bounds(0) >= -180 && bounds(0) <= 180 && bounds(1) >= -180 && bounds(1) <= 180
      if bounds(2) >= -90 && bounds(3) <= 90 && bounds(2) <= bounds(3)
    } yield bounds

  /**
   * The bounding box that `rowGroup` has for the geometries of `column`, in X and Y. None for a
   * column that is not a GEOMETRY column at the root of the file, and without a box whose bounds
   * are in order, as that of a row group without a geometry is.
   */
  private def geometryBox(rowGroup: BlockMetaData, column: String): Option[Envelope] =
    for {
      bounds <- bounds(rowGroup, column, described => !described.isGeography)
      if bounds(0) <= bounds(1) && bounds(2) <= bounds(3)
    } yield new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))

  /**
   * The bounds of X and of Y that the geospatial statistics of `rowGroup` have for `column`, a
   * column at the root of the file of a type that `ofType` takes: the lower and the upper bound
   * of X, and those of Y. None when one of them is no number.
   */
  private def bounds(
      rowGroup: BlockMetaData,
      column: String,
      ofType: ParquetGeospatialColumn => Boolean): Option[Array[Double]] =
    for {
      // A column at the root, whose name may have a dot
      chunk <- rowGroup.getColumns.asScala.find(_.getPath == ColumnPath.get(column))
      described <- ParquetGeospatialTypes.describe(chunk.getPrimitiveType)
      if ofType(described)
      found <- Try(Option(statistics.get.invoke(chunk))).toOption.flatten
      box <- Try(
        Option(found.getClass.getMethod("getBoundingBox").invoke(found))).toOption.flatten
      bounds <- Try(Array("getXMin", "getXMax", "getYMin", "getYMax").map(name =>
        box.getClass.getMethod(name).invoke(box).asInstanceOf[Double])).toOption
      if bounds.forall(bound => !bound.isNaN && !bound.isInfinity)
    } yield bounds
}
