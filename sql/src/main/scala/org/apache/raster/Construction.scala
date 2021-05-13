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



package org.apache.raster

import org.geotools.coverage.grid.GridCoordinates2D
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.GridCoverage2DReader
import org.geotools.coverage.grid.io.OverviewPolicy
import org.geotools.gce.geotiff.GeoTiffReader
import org.opengis.coverage.grid.GridCoordinates
import org.opengis.coverage.grid.GridEnvelope
import org.opengis.parameter.GeneralParameterValue
import org.opengis.parameter.ParameterValue
import java.util

 class Construction(var bands:Int) {

  def getBands(url: String): util.List[util.List[Double]] = {
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
   val numBands: Int = bands

   val bandValues: util.List[util.List[Double]] = new util.ArrayList[util.List[Double]](numBands)

   for (i <- 0 until numBands) {
    bandValues.add(new util.ArrayList[Double])
   }

   for (i <- 0 until w) {
    for (j <- 0 until h) {
     val vals: Array[Double] = new Array[Double](numBands)
     coverage.evaluate(new GridCoordinates2D(i, j), vals)
     var band: Int = 0
     for (pixel <- vals) {
      bandValues.get({
       band += 1; band - 1
      }).add(pixel)
     }
    }
   }
   bandValues


  }





}
