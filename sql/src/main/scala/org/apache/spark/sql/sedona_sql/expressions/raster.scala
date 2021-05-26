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

package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.coverage.grid.io.{AbstractGridFormat, GridCoverage2DReader, GridFormatFinder, OverviewPolicy}
import org.geotools.coverage.grid.{GridCoordinates2D, GridCoverage2D}
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.opengis.coverage.grid.{GridCoordinates, GridEnvelope}
import org.opengis.parameter.{GeneralParameterValue, ParameterValue}
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

import java.io.IOException

class GeometryOperations {

  var coverage:GridCoverage2D = null
  var source:CoordinateReferenceSystem = null
  var target:CoordinateReferenceSystem = null
  var targetCRS:MathTransform =  null

  def getDimensions(url:String):GridEnvelope = {
    val format = GridFormatFinder.findFormat(url)
    val hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
    val reader = format.getReader(url, hints)


    try coverage = reader.read(null)
    catch {
      case giveUp: IOException =>
        throw new RuntimeException(giveUp)
    }
    reader.dispose()
    source = coverage.getCoordinateReferenceSystem
    target = CRS.decode("EPSG:4326", true)
    targetCRS = CRS.findMathTransform(source, target)
    val gridRange2D = coverage.getGridGeometry.getGridRange
    gridRange2D

  }
   def readGeometry(url: String): Geometry = {
    val gridRange2D = getDimensions(url)
    val cords = Array(Array(gridRange2D.getLow(0), gridRange2D.getLow(1)), Array(gridRange2D.getLow(0), gridRange2D.getHigh(1)), Array(gridRange2D.getHigh(0), gridRange2D.getHigh(1)), Array(gridRange2D.getHigh(0), gridRange2D.getLow(1)))
    val polyCoordinates = new Array[Coordinate](5)
    var index = 0

    for (point <- cords) {
      val coordinate2D = new GridCoordinates2D(point(0), point(1))
      val result = coverage.getGridGeometry.gridToWorld(coordinate2D)
      polyCoordinates({
        index += 1; index - 1
      }) = new Coordinate(result.getOrdinate(0), result.getOrdinate(1))
    }

    polyCoordinates(index) = polyCoordinates(0)
    val factory = new GeometryFactory
    val polygon = JTS.transform(factory.createPolygon(polyCoordinates), targetCRS)

    polygon

  }
}


// Fetches polygonal coordinates from a raster image
case class ST_GeomFromGeoTiff(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomConstruction = new GeometryOperations
    val geometry = geomConstruction.readGeometry(geomString)
    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }


  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions
}


// Constructs a raster dataframe from a raster image which contains multiple columns such as Geometry, Band values etc
case class ST_GeomWithBandsFromGeoTiff(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val totalBands = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    val geomConstruction = new GeometryOperations
    val geometry = geomConstruction.readGeometry(geomString)
    val bandvalues = getBands(geomString, totalBands)
    returnValue(geometry.toGenericArrayData,bandvalues, 2)
  }

  private  def getBands(url: String, bands:Int): Array[Double] = {
    val policy: ParameterValue[OverviewPolicy] = AbstractGridFormat.OVERVIEW_POLICY.createValue
    policy.setValue(OverviewPolicy.IGNORE)

    val gridsize: ParameterValue[String] = AbstractGridFormat.SUGGESTED_TILE_SIZE.createValue

    val useJaiRead: ParameterValue[Boolean] = AbstractGridFormat.USE_JAI_IMAGEREAD.createValue.asInstanceOf[ParameterValue[Boolean]]
    useJaiRead.setValue(true)


    val reader: GridCoverage2DReader = new GeoTiffReader(url)
    val coverage: GridCoverage2D = reader.read(Array[GeneralParameterValue](policy, gridsize, useJaiRead))

    val dimensions: GridEnvelope = reader.getOriginalGridRange
    val maxDimensions: GridCoordinates = dimensions.getHigh
    val w: Int = maxDimensions.getCoordinateValue(0) + 1
    val h: Int = maxDimensions.getCoordinateValue(1) + 1
    val numCoordinates= w * h
    val numBands: Int = bands
    val bandValues = new Array[Double](w * h * numBands)

    // Put pixels band by band in the array. The first block will be band1, second block will be band2, ...
    // For each band, store pixels row by row (not column by column)
    for (i <- 0 until h) {
      for (j <- 0 until w) {
        val vals: Array[Double] = new Array[Double](numBands)
        coverage.evaluate(new GridCoordinates2D(j, i), vals)
        // bands of a pixel will be put in [b0...b1...b2...]
        // Each "..." represent w * h pixels
        for (bandId <- 0 until numBands) {
          val offset = i * w + j + numCoordinates * bandId
          bandValues(offset) = vals(bandId)
        }
      }
    }
    bandValues
  }

  // Dynamic results based on number of columns and type of structure
  private def returnValue(geometry:GenericArrayData, bands:Array[Double], count:Int): InternalRow = {

    val genData = new Array[GenericArrayData](count)
    genData(0) = geometry
    genData(1) = new GenericArrayData(bands)
    val result = InternalRow(genData.toList : _*)
    result
  }

  // Dynamic Schema generation using Number of Bands
  private def getSchema():DataType = {
    val mySchema = StructType(Array(StructField("Geometry", GeometryUDT, false),StructField("Bands", ArrayType(DoubleType))))
    mySchema
  }

  override def dataType: DataType = getSchema()

  override def children: Seq[Expression] = inputExpressions
}

// get a particular band from a results of ST_GeomWithBandsFromGeoTiff
case class RS_GetBand(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 3)
    var bandInfo:Array[Double] = null
    if(inputExpressions(0).eval(inputRow).getClass().toString() == "class org.apache.spark.sql.catalyst.expressions.UnsafeArrayData") {
      bandInfo = inputExpressions(0).eval(inputRow).asInstanceOf[UnsafeArrayData].toDoubleArray()
    }
    else {
      bandInfo = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()

    }
    val targetBand = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    val totalBands = inputExpressions(2).eval(inputRow).asInstanceOf[Int]
    val result = gettargetband(bandInfo, targetBand, totalBands)
    new GenericArrayData(result)
  }

  // fetch target band from the given array of bands
  private def gettargetband(bandinfo: Array[Double], targetband:Int, totalbands:Int): Array[Double] = {
    val sizeOfBand = bandinfo.length/totalbands
    val lowerBound = (targetband - 1)*sizeOfBand
    val upperBound = targetband*sizeOfBand
    assert(bandinfo.slice(lowerBound,upperBound).length == sizeOfBand)
    bandinfo.slice(lowerBound, upperBound)

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

