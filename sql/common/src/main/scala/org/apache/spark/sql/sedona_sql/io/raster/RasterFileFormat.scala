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


package org.apache.spark.sql.sedona_sql.io.raster

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.sedona.common.raster.Serde
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.geotools.gce.arcgrid.ArcGridWriter
import org.geotools.gce.geotiff.GeoTiffWriter
import org.opengis.coverage.grid.GridCoverageWriter

import java.io.IOException
import java.nio.file.Paths
import java.util.UUID

private[spark] class RasterFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = None

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    val rasterOptions = new RasterOptions(options)
    if (!isValidRasterSchema(dataSchema)) {
      throw new IllegalArgumentException("Invalid GeoTiff Schema")
    }

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new RasterFileWriter(path, rasterOptions, dataSchema, context)
      }
    }
  }

  override def shortName(): String = "raster"

  override protected def buildReader(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    throw new UnsupportedOperationException("Please use Binary data source to reading raster files")
  }

  private def isValidRasterSchema(dataSchema: StructType): Boolean = {
    var imageColExist: Boolean = false
    val fields = dataSchema.fields
    fields.foreach(field => {
      if (field.dataType.typeName.equals("raster")) {
        imageColExist = true
      }
    })
    imageColExist
  }

}

// class for writing raster images
private class RasterFileWriter(savePath: String,
                               rasterOptions: RasterOptions,
                                dataSchema: StructType,
                                context: TaskAttemptContext) extends OutputWriter {

  private val hfs = new Path(savePath).getFileSystem(context.getConfiguration)

  override def write(row: InternalRow): Unit = {
    val rowFields: InternalRow = row
    val schemaFields: StructType = dataSchema
    var imageColIndex = -1
    for (i <- schemaFields.indices) {
      if (schemaFields.fields(i).dataType.typeName.equals("raster")) {
        imageColIndex = i
      }
    }
    // Get grid coverage 2D from the row
    val rasterRaw = rowFields.getBinary(imageColIndex)
    // If the raster is null, return
    if (rasterRaw == null) return
    // If the raster is not null, deserialize it
    val gridCoverage2D = Serde.deserialize(rasterRaw)
    var writer:GridCoverageWriter = null
    var out:FSDataOutputStream = null
    if (rasterOptions.rasterFormat.equalsIgnoreCase("geotiff")) {
      // If the output path is not provided, generate a random UUID as the file name
      val fileExtension = ".tiff"
      val rasterFilePath = getRasterFilePath(fileExtension, rowFields, schemaFields, rasterOptions)
      // create the write path
      out = hfs.create(new Path(Paths.get(savePath, new Path(rasterFilePath).getName).toString))
      writer = new GeoTiffWriter(out)
    } else if (rasterOptions.rasterFormat.equalsIgnoreCase("arcgrid")) {
      val fileExtension = ".asc"
      val rasterFilePath = getRasterFilePath(fileExtension, rowFields, schemaFields, rasterOptions)
      out = hfs.create(new Path(Paths.get(savePath, new Path(rasterFilePath).getName).toString))
      writer = new ArcGridWriter(out)
    } else
      throw new IllegalArgumentException("Invalid raster format")

    // write the image to file
    try {
      writer.write(gridCoverage2D)
      writer.dispose()
      out.close()
    } catch {
      case e@(_: IllegalArgumentException | _: IOException) =>
        // TODO Auto-generated catch block
        e.printStackTrace()
    }
  }

  override def close(): Unit = {
    hfs.close()
  }

  def path(): String = {
    savePath
  }

  private def getRasterFilePath(fileExtension: String, row: InternalRow, schema: StructType, rasterOptions: RasterOptions): String = {
    // If the output path is not provided, generate a random UUID as the file name
    var rasterFilePath = UUID.randomUUID().toString
    if (rasterOptions.rasterPathField.isDefined) {
      val rasterFilePathRaw = row.getString(schema.fieldIndex(rasterOptions.rasterPathField.get))
      // If the output path field is provided, but the value is null, generate a random UUID as the file name
      if (rasterFilePathRaw != null) {
        // remove the extension if exists
        if (rasterFilePathRaw.contains(".")) rasterFilePath = rasterFilePathRaw.substring(0, rasterFilePathRaw.lastIndexOf("."))
        else rasterFilePath = rasterFilePathRaw
      }
    }
    rasterFilePath + fileExtension
  }
}
