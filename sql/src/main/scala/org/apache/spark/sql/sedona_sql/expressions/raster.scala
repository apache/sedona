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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.geotools.coverage.grid.io.GridFormatFinder
import org.geotools.coverage.grid.{GridCoordinates2D, GridCoverage2D}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.opengis.coverage.grid.GridEnvelope
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

// get a particular band from a results of ST_GeomWithBandsFromGeoTiff
case class RS_GetBand(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 3)
    val bandInfo =inputExpressions(0).eval(inputRow).asInstanceOf[UnsafeArrayData].toDoubleArray()
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


