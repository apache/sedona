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

import java.lang.reflect.Modifier
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sedona_sql.types.GeographyBounds

/**
 * The geospatial statistics of the row groups of the geography columns of one file: the bounding
 * box and the type codes of the geographies of each row group.
 *
 * The Parquet library collects these statistics for a GEOMETRY column and has no algorithm for a
 * GEOGRAPHY column, whose edges are on the sphere, and it drops statistics that a writer gives it
 * for one. Sedona therefore collects them while it writes the values, and puts them into the row
 * groups that the Parquet library has finished, before that library writes the footer of the file
 * from those row groups. See [[GeographyRowGroupStatistics.rowGroupsOf]] for how it gets at them.
 *
 * @param columns
 *   the names of the geography columns, by their ordinal in the schema, null for other columns
 * @param rowGroups
 *   the row groups that the Parquet library has finished, which it writes the footer from
 */
private[geoparquet] class GeographyRowGroupStatistics(
    columns: Array[String],
    rowGroups: java.util.List[BlockMetaData]) {
  import GeographyRowGroupStatistics.Column

  private val ordinals = columns.indices.filter(columns(_) != null).toArray
  private val bounds = columns.map(name => if (name == null) null else new GeographyBounds)
  private val types = columns.map(name => if (name == null) null else mutable.BitSet.empty)
  private var rows = 0L
  private val finished = mutable.ArrayBuffer.empty[(Long, Map[ColumnPath, Column])]

  /**
   * Adds a geography of the column at `ordinal`, which `wkb` holds as ISO WKB from `offset` on.
   */
  def add(ordinal: Int, wkb: Array[Byte], offset: Int, typeCode: Int): Unit = {
    bounds(ordinal).add(wkb, offset)
    types(ordinal) += typeCode
  }

  def endRow(): Unit = rows += 1

  /** Ends the row group of the rows since the last call. Without such rows there is none. */
  def endRowGroup(): Unit = if (rows > 0) {
    val found = ordinals.map { ordinal =>
      val column = Column(Option(bounds(ordinal).box()), types(ordinal).toSet)
      bounds(ordinal).reset()
      types(ordinal).clear()
      ColumnPath.get(columns(ordinal)) -> column
    }.toMap
    finished += rows -> found
    rows = 0
  }

  /** Ends the last row group and gives the row groups of the file their statistics. */
  def write(): Unit = {
    endRowGroup()
    GeographyRowGroupStatistics.write(rowGroups, finished.toSeq)
  }
}

object GeographyRowGroupStatistics extends Logging {

  /**
   * The statistics of a column in one row group: the western, eastern, southern and northern
   * bound of its geographies, None without a coordinate, and their ISO type codes.
   */
  case class Column(box: Option[Array[Double]], types: Set[Int])

  /** The key of the Parquet library that turns the statistics of the columns on and off. */
  private val STATISTICS_ENABLED = "parquet.column.statistics.enabled"

  /**
   * Whether a write with `configuration` collects the statistics of `column`, a column at the
   * root of the schema. It follows the switch of the Parquet library for the statistics of that
   * column, as that library reads it.
   */
  def enabled(configuration: Configuration, column: String): Boolean = library.isDefined && {
    // The key of one column has # and the path of the column after it, with a dot between the
    // fields of the path. The path of a column at the root is its name, and a name with a dot
    // is the path of a nested field, so such a column has no key of its own.
    val key = STATISTICS_ENABLED + "#" + column
    val ofColumn = !column.contains(".") && configuration.get(key) != null
    // A value that is no boolean is the default of the Parquet library, which is true
    configuration.getBoolean(if (ofColumn) key else STATISTICS_ENABLED, true)
  }

  /**
   * The classes of the geospatial statistics, which parquet-java has since 1.16.0. Sedona also
   * compiles against older versions, so they are reached by reflection.
   */
  private class Library {
    private def load(name: String): Class[_] =
      Class.forName(
        "org.apache.parquet.column.statistics.geospatial." + name,
        false,
        classOf[ColumnChunkMetaData].getClassLoader)

    private val boxClass = load("BoundingBox")
    private val typesClass = load("GeospatialTypes")
    private val statisticsClass = load("GeospatialStatistics")
    private val newBox = boxClass.getConstructor(Seq.fill(8)(java.lang.Double.TYPE): _*)
    private val newTypes = typesClass.getConstructor(classOf[java.util.Set[_]])
    private val newStatistics = statisticsClass.getConstructor(boxClass, typesClass)
    private val sizeStatistics = classOf[ColumnChunkMetaData].getMethod("getSizeStatistics")
    private val newChunk = classOf[ColumnChunkMetaData].getMethods
      .find { method =>
        method.getName == "get" && Modifier.isStatic(method.getModifiers) &&
        method.getParameterCount == 13 && method.getParameterTypes.last == statisticsClass
      }
      .getOrElse(throw new NoSuchMethodException("ColumnChunkMetaData.get"))

    /** A copy of `chunk` with the statistics of `column`. */
    def withStatistics(chunk: ColumnChunkMetaData, column: Column): ColumnChunkMetaData = {
      val nan = Double.box(Double.NaN)
      val box = column.box.map { case Array(west, east, south, north) =>
        // The box has no bounds of Z and M
        newBox.newInstance(
          Double.box(west),
          Double.box(east),
          Double.box(south),
          Double.box(north),
          nan,
          nan,
          nan,
          nan)
      }
      val types = newTypes.newInstance(column.types.map(Int.box).asJava)
      val statistics =
        newStatistics.newInstance(box.orNull.asInstanceOf[AnyRef], types.asInstanceOf[AnyRef])
      newChunk
        .invoke(
          null,
          chunk.getPath,
          chunk.getPrimitiveType,
          chunk.getCodec,
          chunk.getEncodingStats,
          chunk.getEncodings,
          chunk.getStatistics,
          Long.box(chunk.getFirstDataPageOffset),
          Long.box(chunk.getDictionaryPageOffset),
          Long.box(chunk.getValueCount),
          Long.box(chunk.getTotalSize),
          Long.box(chunk.getTotalUncompressedSize),
          sizeStatistics.invoke(chunk),
          statistics.asInstanceOf[AnyRef])
        .asInstanceOf[ColumnChunkMetaData]
    }
  }

  private lazy val library: Option[Library] = Try(new Library).toOption

  private val warned = new AtomicBoolean(false)

  private def warn(reason: String, error: Throwable = null): Unit = {
    if (warned.compareAndSet(false, true)) {
      val message = s"Writing GeoParquet files without the row group statistics of their " +
        s"geography columns: $reason. The files are complete without them."
      if (error == null) logWarning(message) else logWarning(message, error)
    }
  }

  private def field(target: AnyRef, name: String): AnyRef = {
    var owner: Class[_] = target.getClass
    while (owner != null) {
      try {
        val found = owner.getDeclaredField(name)
        found.setAccessible(true)
        return found.get(target)
      } catch {
        case _: NoSuchFieldException => owner = owner.getSuperclass
      }
    }
    throw new NoSuchFieldException(s"${target.getClass.getName}.$name")
  }

  /**
   * The row groups that the Parquet library has finished for the file of `recordWriter`, a writer
   * of `ParquetOutputFormat`. The library adds a row group to this list when it flushes one, and
   * writes the footer from the list when the file is closed.
   *
   * The library has no public way to the list, so this reads three private fields of it, which
   * have these names in parquet-java 1.16.0 and 1.17.0. None when they are not there, and for an
   * encrypted file, whose column chunks are of another kind. A file is then written as the
   * Parquet library writes it, without statistics for its geography columns.
   */
  def rowGroupsOf(recordWriter: AnyRef): Option[java.util.List[BlockMetaData]] = {
    Try {
      val fileWriter = field(field(recordWriter, "internalWriter"), "parquetFileWriter")
        .asInstanceOf[ParquetFileWriter]
      if (fileWriter.getEncryptor != null) None
      else {
        field(fileWriter, "blocks") match {
          case rowGroups: java.util.List[_] =>
            Some(rowGroups.asInstanceOf[java.util.List[BlockMetaData]])
          case other => throw new ClassCastException(s"The row groups are $other")
        }
      }
    } match {
      case Success(rowGroups) => rowGroups
      case Failure(error) =>
        warn("the row groups of the Parquet writer were not found", error)
        None
    }
  }

  /**
   * Replaces the row groups of `rowGroups` by copies that have the statistics of `finished`,
   * which has the number of rows and the statistics of each row group in the order of the file.
   * The row groups stay as they are if the two do not describe the same row groups.
   */
  private def write(
      rowGroups: java.util.List[BlockMetaData],
      finished: Seq[(Long, Map[ColumnPath, Column])]): Unit = {
    val existing = rowGroups.asScala.toSeq
    if (existing.map(_.getRowCount) != finished.map(_._1)) {
      warn(
        s"the Parquet writer has row groups of ${existing.map(_.getRowCount).mkString(", ")} " +
          s"rows, and the statistics are of ${finished.map(_._1).mkString(", ")} rows")
      return
    }
    Try {
      existing.zip(finished).map { case (rowGroup, (_, columns)) =>
        val copy = new BlockMetaData
        copy.setRowCount(rowGroup.getRowCount)
        copy.setTotalByteSize(rowGroup.getTotalByteSize)
        copy.setPath(rowGroup.getPath)
        copy.setOrdinal(rowGroup.getOrdinal)
        copy.setRowIndexOffset(rowGroup.getRowIndexOffset)
        rowGroup.getColumns.asScala.foreach { chunk =>
          columns.get(chunk.getPath) match {
            // A row group of nulls has nothing to state
            case Some(column) if column.box.isDefined || column.types.nonEmpty =>
              copy.addColumn(library.get.withStatistics(chunk, column))
            case _ => copy.addColumn(chunk)
          }
        }
        copy
      }
    } match {
      case Success(copies) =>
        copies.zipWithIndex.foreach { case (copy, index) => rowGroups.set(index, copy) }
      case Failure(error) => warn("they could not be added to the row groups", error)
    }
  }
}
