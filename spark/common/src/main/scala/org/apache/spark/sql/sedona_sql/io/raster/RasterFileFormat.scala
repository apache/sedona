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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.OutputCommitter
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

import java.io.IOException
import java.util.UUID

private[spark] class RasterFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException(
      "Please use 'binaryFile' data source to reading raster files")
    None
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val rasterOptions = new RasterOptions(options)
    if (!isValidRasterSchema(dataSchema)) {
      throw new IllegalArgumentException("Invalid Raster DataFrame Schema")
    }
    if (rasterOptions.useDirectCommitter) {
      val conf = job.getConfiguration

      // For working with SQLHadoopMapReduceCommitProtocol, which is the default output commit protocol
      conf.set("spark.sql.sources.outputCommitterClass", classOf[DirectOutputCommitter].getName)

      // For working with org.apache.spark.internal.io.cloud.PathOutputCommitProtocol, which is the
      // output commit protocol used in cloud storage. For instance, the S3 magic committer uses
      // this output commit protocol
      conf.set(
        "mapreduce.outputcommitter.factory.class",
        classOf[DirectOutputCommitterFactory].getName)
    }

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new RasterFileWriter(path, rasterOptions, dataSchema, context)
      }
    }
  }

  override def shortName(): String = "raster"

  private def isValidRasterSchema(dataSchema: StructType): Boolean = {
    var imageColExist: Boolean = false
    val fields = dataSchema.fields
    fields.foreach(field => {
      if (field.dataType.typeName.equals("binary")) {
        imageColExist = true
      }
    })
    imageColExist
  }

}

// class for writing raster images
private class RasterFileWriter(
    savePath: String,
    rasterOptions: RasterOptions,
    dataSchema: StructType,
    context: TaskAttemptContext)
    extends OutputWriter {

  private val hfs = (new Path(savePath)).getFileSystem(context.getConfiguration)
  private val rasterFieldIndex =
    if (rasterOptions.rasterField.isEmpty) getRasterFieldIndex
    else dataSchema.fieldIndex(rasterOptions.rasterField.get)

  private def getRasterFieldIndex: Int = {
    val schemaFields: StructType = dataSchema
    var curField = -1
    for (i <- schemaFields.indices) {
      if (schemaFields.fields(i).dataType.typeName.equals("binary")) {
        curField = i
      }
    }
    curField
  }
  override def write(row: InternalRow): Unit = {
    // Get grid coverage 2D from the row
    val rasterRaw = row.getBinary(rasterFieldIndex)
    // If the raster is null, return
    if (rasterRaw == null) return
    // If the raster is not null, write it to disk
    val rasterFilePath = getRasterFilePath(row, dataSchema, rasterOptions)
    // write the image to file
    try {
      val out = hfs.create(new Path(savePath, new Path(rasterFilePath).getName))
      out.write(rasterRaw)
      out.close()
    } catch {
      case e @ (_: IOException) =>
        // TODO Auto-generated catch block
        e.printStackTrace()
    }
  }

  override def close(): Unit = {}

  def path(): String = {
    savePath
  }

  private def getRasterFilePath(
      row: InternalRow,
      schema: StructType,
      rasterOptions: RasterOptions): String = {
    // If the output path is not provided, generate a random UUID as the file name
    var rasterFilePath = UUID.randomUUID().toString
    if (rasterOptions.rasterPathField.isDefined) {
      val rasterFilePathRaw = row.getString(schema.fieldIndex(rasterOptions.rasterPathField.get))
      // If the output path field is provided, but the value is null, generate a random UUID as the file name
      if (rasterFilePathRaw != null) {
        // remove the extension if exists
        if (rasterFilePathRaw.contains("."))
          rasterFilePath = rasterFilePathRaw.substring(0, rasterFilePathRaw.lastIndexOf("."))
        else rasterFilePath = rasterFilePathRaw
      }
    }
    rasterFilePath + rasterOptions.fileExtension
  }
}

class DirectOutputCommitterFactory extends PathOutputCommitterFactory {
  override def createOutputCommitter(
      outputPath: Path,
      context: TaskAttemptContext): PathOutputCommitter =
    new DirectPathOutputCommitter(outputPath, context)
}

trait DirectOutputCommitterTrait extends OutputCommitter {
  override def setupJob(jobContext: JobContext): Unit = {
    val outputPath = FileOutputFormat.getOutputPath(jobContext)
    if (outputPath != null) {
      val fs = outputPath.getFileSystem(jobContext.getConfiguration)
      if (!fs.exists(outputPath)) {
        fs.mkdirs(outputPath)
      }
    }
  }

  override def commitJob(jobContext: JobContext): Unit = {
    val outputPath = FileOutputFormat.getOutputPath(jobContext)
    if (outputPath != null) {
      val fs = outputPath.getFileSystem(jobContext.getConfiguration)
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      if (jobContext.getConfiguration.getBoolean(
          "mapreduce.fileoutputcommitter.marksuccessfuljobs",
          true)) {
        val markerPath = new Path(outputPath, "_SUCCESS")
        fs.create(markerPath).close()
      }
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = ()
  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = false
  override def commitTask(taskContext: TaskAttemptContext): Unit = ()
  override def abortTask(taskContext: TaskAttemptContext): Unit = ()
}

class DirectOutputCommitter extends DirectOutputCommitterTrait

class DirectPathOutputCommitter(outputPath: Path, context: JobContext)
    extends PathOutputCommitter(outputPath, context)
    with DirectOutputCommitterTrait {

  override def getOutputPath: Path = outputPath
  override def getWorkPath: Path = outputPath
}
