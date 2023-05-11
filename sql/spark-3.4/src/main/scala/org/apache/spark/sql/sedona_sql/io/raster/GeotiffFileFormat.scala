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


package org.apache.spark.sql.sedona_sql.io

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.{AbstractGridFormat, GridFormatFinder}
import org.geotools.gce.geotiff.{GeoTiffFormat, GeoTiffWriteParams, GeoTiffWriter}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.locationtech.jts.geom.{Coordinate, Polygon}
import org.locationtech.jts.io.WKTReader
import org.opengis.parameter.GeneralParameterValue

import java.awt.image.DataBuffer
import java.io.IOException
import java.nio.file.Paths
import javax.imageio.ImageWriteParam
import javax.media.jai.RasterFactory

private[spark] class GeotiffFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = Some(GeotiffSchema.imageSchema)

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    val imageWriteOptions = new ImageWriteOptions(options)
    if (!isValidGeoTiffSchema(imageWriteOptions, dataSchema)) {
      throw new IllegalArgumentException("Invalid GeoTiff Schema")
    }

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new GeotiffFileWriter(path, imageWriteOptions, dataSchema, context)
      }
    }
  }

  override def shortName(): String = "geotiff"

  override protected def buildReader(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Image data source only produces a single data column named \"image\".")

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val imageSourceOptions = new ImageReadOptions(options)

    (file: PartitionedFile) => {
      val emptyUnsafeRow = new UnsafeRow(0)
      if (!imageSourceOptions.dropInvalid && requiredSchema.isEmpty) {
        Iterator(emptyUnsafeRow)
      } else {
        val path = file.toPath
        val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
        val stream = fs.open(path)
        val bytes = try {
          ByteStreams.toByteArray(stream)
        } finally {
          Closeables.close(stream, true)
        }

        val resultOpt = GeotiffSchema.decode(path.toString, bytes, imageSourceOptions)
        val filteredResult = if (imageSourceOptions.dropInvalid) {
          resultOpt.toIterator
        } else {
          Iterator(resultOpt.getOrElse(GeotiffSchema.invalidImageRow(path.toString)))
        }

        if (requiredSchema.isEmpty) {
          filteredResult.map(_ => emptyUnsafeRow)
        } else {
          val converter = RowEncoder(requiredSchema).createSerializer() // SPARK3 anchor
           filteredResult.map(row => converter(row)) // SPARK3 anchor
//          val converter = RowEncoder(requiredSchema) // SPARK2 anchor
//          filteredResult.map(row => converter.toRow(row)) // SPARK2 anchor
        }
      }
    }
  }

  private def isValidGeoTiffSchema(imageWriteOptions: ImageWriteOptions, dataSchema: StructType): Boolean = {
    val fields = dataSchema.fieldNames
    if (fields.contains(imageWriteOptions.colImage) ){
      val schemaFields = dataSchema.fields(dataSchema.fieldIndex(imageWriteOptions.colImage)).dataType.asInstanceOf[StructType]
      if (schemaFields.fieldNames.length != 6) return false
    }
    else {
      if (fields.length != 6) return false
    }
    true
  }

}

// class for writing geoTiff images
private class GeotiffFileWriter(savePath: String,
                                imageWriteOptions: ImageWriteOptions,
                                dataSchema: StructType,
                                context: TaskAttemptContext) extends OutputWriter {

  // set writing parameters
  private val DEFAULT_WRITE_PARAMS: GeoTiffWriteParams = new GeoTiffWriteParams()
  DEFAULT_WRITE_PARAMS.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
  DEFAULT_WRITE_PARAMS.setCompressionType("LZW")
  DEFAULT_WRITE_PARAMS.setCompressionQuality(0.75F)
  DEFAULT_WRITE_PARAMS.setTilingMode(ImageWriteParam.MODE_EXPLICIT)
  DEFAULT_WRITE_PARAMS.setTiling(512, 512)

  private val hfs = new Path(savePath).getFileSystem(context.getConfiguration)

  override def write(row: InternalRow): Unit = {
    // retrieving the metadata of a geotiff image
    var rowFields: InternalRow = row
    var schemaFields: StructType = dataSchema
    val fields = dataSchema.fieldNames

    if (fields.contains(imageWriteOptions.colImage)) {
      schemaFields = dataSchema.fields(dataSchema.fieldIndex(imageWriteOptions.colImage)).dataType.asInstanceOf[StructType]
      rowFields = row.getStruct(dataSchema.fieldIndex(imageWriteOptions.colImage), 6)
    }

    val tiffOrigin = rowFields.getString(schemaFields.fieldIndex(imageWriteOptions.colOrigin))
    val tiffBands = rowFields.getInt(schemaFields.fieldIndex(imageWriteOptions.colBands))
    val tiffWidth = rowFields.getInt(schemaFields.fieldIndex(imageWriteOptions.colWidth))
    val tiffHeight = rowFields.getInt(schemaFields.fieldIndex(imageWriteOptions.colHeight))
    val tiffGeometry = Row.fromSeq(rowFields.toSeq(schemaFields)).get(schemaFields.fieldIndex(imageWriteOptions.colGeometry))
    val tiffData = rowFields.getArray(schemaFields.fieldIndex(imageWriteOptions.colData)).toDoubleArray()

    // if an image is invalid, fields are -1 and data array is empty. Skip writing that image
    if (tiffBands == -1) return

    // create a writable raster object
    val raster = RasterFactory.createBandedRaster(DataBuffer.TYPE_DOUBLE, tiffWidth, tiffHeight, tiffBands, null)

    // extract the pixels of the geotiff image and write to the writable raster
    val pixelVal = Array.ofDim[Double](tiffBands)
    for (i <- 0 until tiffHeight) {
      for (j <- 0 until tiffWidth) {
        for (k <- 0 until tiffBands) {
          pixelVal(k) = tiffData(tiffHeight*tiffWidth*k + i * tiffWidth + j)
        }
        raster.setPixel(j, i, pixelVal)
      }
    }

    // CRS is decoded to user-provided option "writeToCRS", default value is "EPSG:4326"
    val crs = CRS.decode(imageWriteOptions.writeToCRS, true)

    // Extract the geometry coordinates and set the envelop of the geotiff source
    var coordinateList: Array[Coordinate] = null
    if (tiffGeometry.isInstanceOf[UTF8String]) {
      val wktReader = new WKTReader()
      val envGeom = wktReader.read(tiffGeometry.toString).asInstanceOf[Polygon]
      coordinateList = envGeom.getCoordinates()
    } else {
      val envGeom = GeometrySerializer.deserialize(tiffGeometry.asInstanceOf[Array[Byte]])
      coordinateList = envGeom.getCoordinates()
    }
    val referencedEnvelope = new ReferencedEnvelope(coordinateList(0).x, coordinateList(2).x, coordinateList(0).y, coordinateList(2).y, crs)

    // create the write path
    val writePath = Paths.get(savePath, new Path(tiffOrigin).getName).toString
    val out = hfs.create(new Path(writePath))

    val format = GridFormatFinder.findFormat(out)
    var hints: Hints = null
    if (format.isInstanceOf[GeoTiffFormat]) {
      hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
    }

    // create the writer object
    val factory = CoverageFactoryFinder.getGridCoverageFactory(hints)
    val gc = factory.create("GRID", raster, referencedEnvelope)
    val writer = new GeoTiffWriter(out, hints)

    val gtiffParams = new GeoTiffFormat().getWriteParameters
    gtiffParams.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName.toString).setValue(DEFAULT_WRITE_PARAMS)
    val wps: Array[GeneralParameterValue] = gtiffParams.values.toArray(new Array[GeneralParameterValue](1))

    // write the geotiff image to file
    try {
      writer.write(gc, wps)
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
}
