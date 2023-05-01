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

import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.geotools.coverage.grid.io.{AbstractGridFormat, GridCoverage2DReader, OverviewPolicy}
import org.geotools.coverage.grid.{GridCoordinates2D, GridCoverage2D}
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon}
import org.opengis.coverage.grid.{GridCoordinates, GridEnvelope}
import org.opengis.parameter.{GeneralParameterValue, ParameterValue}
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

import java.io.ByteArrayInputStream

object GeotiffSchema {
  val undefinedImageType = "Undefined"

  /**
   * Schema for the image column: Row(String,Geometry, Int, Int, Int, Array[Double])
   */
  val columnSchema = StructType(
    StructField("origin", StringType, true) ::
      StructField("geometry", StringType, true) ::
      StructField("height", IntegerType, false) ::
      StructField("width", IntegerType, false) ::
      StructField("nBands", IntegerType, false) ::
      StructField("data", ArrayType(DoubleType), false) :: Nil)

  val imageFields: Array[String] = columnSchema.fieldNames

  /**
   * DataFrame with a single column of images named "image" (nullable)
   */
  val imageSchema = StructType(StructField("image", columnSchema, true) :: Nil)

  /**
   * Gets the origin of the image
   *
   * @return The origin of the image
   */
  def getOrigin(row: Row): String = row.getString(0)

  /**
   * Gets the origin of the image
   *
   * @return The origin of the image
   */
  def getGeometry(row: Row): GeometryUDT = row.getAs[GeometryUDT](1)


  /**
   * Gets the height of the image
   *
   * @return The height of the image
   */
  def getHeight(row: Row): Int = row.getInt(2)

  /**
   * Gets the width of the image
   *
   * @return The width of the image
   */
  def getWidth(row: Row): Int = row.getInt(3)

  /**
   * Gets the number of channels in the image
   *
   * @return The number of bands in the image
   */
  def getNBands(row: Row): Int = row.getInt(4)


  /**
   * Gets the image data
   *
   * @return The image data
   */
  def getData(row: Row): Array[Double] = row.getAs[Array[Double]](5)

  /**
   * Default values for the invalid image
   *
   * @param origin Origin of the invalid image
   * @return Row with the default values
   */
  private[io] def invalidImageRow(origin: String): Row =
    Row(Row(origin, -1, -1, -1, Array.ofDim[Byte](0)))

  /**
   *
   * Convert a GeoTiff image into a dataframe row
   *
   *
   * @param origin Arbitrary string that identifies the image
   * @param bytes  Image bytes (for example, jpeg)
   * @return DataFrame Row or None (if the decompression fails)
   *
   */

  private[io] def decode(origin: String, bytes: Array[Byte], imageSourceOptions: ImageReadOptions): Option[Row] = {

    val policy: ParameterValue[OverviewPolicy] = AbstractGridFormat.OVERVIEW_POLICY.createValue
    policy.setValue(OverviewPolicy.IGNORE)
    val gridsize: ParameterValue[String] = AbstractGridFormat.SUGGESTED_TILE_SIZE.createValue
    val useJaiRead: ParameterValue[Boolean] = AbstractGridFormat.USE_JAI_IMAGEREAD.createValue.asInstanceOf[ParameterValue[Boolean]]
    useJaiRead.setValue(true)

    // Read Geotiff image from Byte Array
    val reader: GridCoverage2DReader = try {
      new GeoTiffReader(new ByteArrayInputStream(bytes))
    } catch {
      // Catch runtime exception because `ImageIO` may throw unexpected `RuntimeException`.
      // But do not catch the declared `IOException` (regarded as FileSystem failure)
      case _: RuntimeException => null
    }
    var coverage: GridCoverage2D = null
    if (reader == null) {
      None
    } else {
      coverage = reader.read(Array[GeneralParameterValue](policy, gridsize, useJaiRead))
    }

    // Fetch geometry from given image
    var source: CoordinateReferenceSystem = try {
      coverage.getCoordinateReferenceSystem
    }
    catch {
      case _: Exception => null
    }
    if (source == null && imageSourceOptions.readFromCRS != "") {
      source = CRS.decode(imageSourceOptions.readFromCRS, true)
    }

    val target: CoordinateReferenceSystem = if (imageSourceOptions.readToCRS != "") {
      CRS.decode(imageSourceOptions.readToCRS, true)
    } else {
      null
    }

    var targetCRS: MathTransform = null
    if (target != null) {
      if (source == null) {
        throw new IllegalArgumentException("Invalid arguments. Source coordinate reference system was not found.")
      } else {
        targetCRS = CRS.findMathTransform(source, target, imageSourceOptions.disableErrorInCRS)
      }
    }

    val gridRange2D = coverage.getGridGeometry.getGridRange
    val cords = Array(Array(gridRange2D.getLow(0), gridRange2D.getLow(1)), Array(gridRange2D.getLow(0), gridRange2D.getHigh(1)), Array(gridRange2D.getHigh(0), gridRange2D.getHigh(1)), Array(gridRange2D.getHigh(0), gridRange2D.getLow(1)))
    val polyCoordinates = new Array[Coordinate](5)
    var index = 0

    for (point <- cords) {
      val coordinate2D = new GridCoordinates2D(point(0), point(1))
      val result = coverage.getGridGeometry.gridToWorld(coordinate2D)
      polyCoordinates({
        index += 1;
        index - 1
      }) = new Coordinate(result.getOrdinate(0), result.getOrdinate(1))
    }

    polyCoordinates(index) = polyCoordinates(0)
    val factory = new GeometryFactory
    var polygon = factory.createPolygon(polyCoordinates)
    if (targetCRS != null) {
      polygon = JTS.transform(polygon, targetCRS).asInstanceOf[Polygon]
    }

    // Fetch band values from given image
    val nBands: Int = coverage.getNumSampleDimensions
    val dimensions: GridEnvelope = reader.getOriginalGridRange
    val maxDimensions: GridCoordinates = dimensions.getHigh
    val width: Int = maxDimensions.getCoordinateValue(0) + 1
    val height: Int = maxDimensions.getCoordinateValue(1) + 1
    val imageSize = height * width * nBands
    assert(imageSize < 1e9, "image is too large")
    val decoded = Array.ofDim[Double](imageSize)

    for (i <- 0 until height) {
      for (j <- 0 until width) {
        val vals: Array[Double] = new Array[Double](nBands)
        coverage.evaluate(new GridCoordinates2D(j, i), vals)
        // bands of a pixel will be put in [b0...b1...b2...]
        // Each "..." represent w * h pixels
        for (bandId <- 0 until nBands) {
          val offset = i * width + j + width * height * bandId
          decoded(offset) = vals(bandId)
        }
      }
    }
    // the internal "Row" is needed, because the image is a single DataFrame column
    Some(Row(Row(origin, polygon.toText, height, width, nBands, decoded)))
  }
}





