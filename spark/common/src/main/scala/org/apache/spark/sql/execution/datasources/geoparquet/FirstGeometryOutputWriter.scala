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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.StructType

/**
 * Writes a GeoParquet 2 file that has GeometryUDT columns without a CRS option.
 *
 * GeoParquet 2 states the CRS of a geometry column in the Parquet schema, which the Parquet
 * writer needs before the first row. A GeometryUDT column has an SRID on each geometry only. This
 * writer therefore holds rows back until every such column has shown a geometry, then opens the
 * file with those SRIDs. [[GeoParquetWriteSupport]] turns them into the CRS of the columns and
 * rejects a later geometry that has another SRID.
 *
 * The rows held back are limited to [[GeoParquetWriteSupport.GEOPARQUET_SRID_LOOKAHEAD_KEY]]
 * bytes. A column that has shown no geometry by then is written with an unknown CRS.
 *
 * @param columns
 *   the ordinals of the columns to look up, see [[GeoParquetWriteSupport.columnsWithoutCrs]]
 */
class FirstGeometryOutputWriter(
    val path: String,
    context: TaskAttemptContext,
    schema: StructType,
    columns: Seq[Int])
    extends OutputWriter {

  private val configuration = context.getConfiguration
  private val lookaheadBytes = configuration.getLong(
    GeoParquetWriteSupport.GEOPARQUET_SRID_LOOKAHEAD_KEY,
    GeoParquetWriteSupport.DEFAULT_CRS_LOOKAHEAD_BYTES)

  private val waiting = mutable.LinkedHashSet(columns: _*)
  private val srids = mutable.Map.empty[Int, Int]
  private val heldBack = mutable.ArrayBuffer.empty[InternalRow]
  private var heldBackBytes = 0L
  private var recordWriter: RecordWriter[Void, InternalRow] = _
  private var closed = false

  override def write(row: InternalRow): Unit = {
    if (recordWriter != null) {
      recordWriter.write(null, row)
    } else {
      waiting.toSeq.foreach { ordinal =>
        if (!row.isNullAt(ordinal)) {
          val geometry = SpatialTypeSupport.readGeometry(row, ordinal, schema(ordinal).dataType)
          srids.put(ordinal, geometry.getSRID)
          waiting.remove(ordinal)
        }
      }
      val bytes = row match {
        case unsafe: UnsafeRow => unsafe.getSizeInBytes.toLong
        case _ => FirstGeometryOutputWriter.ASSUMED_ROW_BYTES
      }
      if (waiting.isEmpty || heldBackBytes + bytes >= lookaheadBytes) {
        // This row ends the wait, so it is written as it is. Only the rows before it are kept
        // as copies, which keeps the rows held back under the limit however large this row is.
        open()
        recordWriter.write(null, row)
      } else {
        heldBack += row.copy()
        heldBackBytes += bytes
      }
    }
  }

  private def open(): Unit = {
    // Other files of the same task may find other SRIDs, so each file gets its own configuration
    val fileConfiguration = new Configuration(configuration)
    columns.foreach { ordinal =>
      fileConfiguration.set(
        GeoParquetWriteSupport.FIRST_GEOMETRY_SRID_KEY + ordinal,
        srids.get(ordinal).map(_.toString).getOrElse(GeoParquetWriteSupport.NO_FIRST_GEOMETRY))
    }
    recordWriter = new ParquetOutputFormat[InternalRow]()
      .getRecordWriter(fileConfiguration, new Path(path), CodecConfig.from(context).getCodec)
    try heldBack.foreach(recordWriter.write(null, _))
    catch {
      case error: Throwable =>
        // A row held back can fail, with a second SRID for example. Spark does not close a
        // writer again after its close() has failed, so the file is released here.
        release(error)
        throw error
    } finally heldBack.clear()
  }

  private def release(error: Throwable): Unit = {
    closed = true
    try recordWriter.close(context)
    catch { case other: Throwable => error.addSuppressed(other) }
  }

  override def close(): Unit = if (!closed) {
    if (recordWriter == null) open()
    closed = true
    recordWriter.close(context)
  }
}

object FirstGeometryOutputWriter {

  /** The size counted for a held back row that does not report its own size. */
  private val ASSUMED_ROW_BYTES = 1024L
}
