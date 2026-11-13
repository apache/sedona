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
 * The rows held back are limited to [[GeoParquetWriteSupport.GEOPARQUET_CRS_LOOKAHEAD_KEY]]
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
    GeoParquetWriteSupport.GEOPARQUET_CRS_LOOKAHEAD_KEY,
    GeoParquetWriteSupport.DEFAULT_CRS_LOOKAHEAD_BYTES)

  private val waiting = mutable.LinkedHashSet(columns: _*)
  private val srids = mutable.Map.empty[Int, Int]
  private val heldBack = mutable.ArrayBuffer.empty[InternalRow]
  private var heldBackBytes = 0L
  private var recordWriter: RecordWriter[Void, InternalRow] = _

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
      heldBack += row.copy()
      heldBackBytes += (row match {
        case unsafe: UnsafeRow => unsafe.getSizeInBytes.toLong
        case _ => FirstGeometryOutputWriter.ASSUMED_ROW_BYTES
      })
      if (waiting.isEmpty || heldBackBytes >= lookaheadBytes) open()
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
    heldBack.foreach(recordWriter.write(null, _))
    heldBack.clear()
  }

  override def close(): Unit = {
    if (recordWriter == null) open()
    recordWriter.close(context)
  }
}

object FirstGeometryOutputWriter {

  /** The size counted for a held back row that does not report its own size. */
  private val ASSUMED_ROW_BYTES = 1024L
}
