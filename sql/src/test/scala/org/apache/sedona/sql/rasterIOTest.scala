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

package org.apache.sedona.sql

import org.locationtech.jts.geom.Geometry
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import scala.collection.mutable

class rasterIOTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  var rasterdatalocation: String = resourceFolder + "raster/"

  describe("Raster IO test") {
    it("Should Pass geotiff loading") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
      df = df.selectExpr("image.origin as origin","ST_GeomFromWkt(image.wkt) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
      assert(df.first().getAs[Geometry](1).toText == "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284," +
        " -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))")
      assert(df.first().getInt(2) == 517)
      assert(df.first().getInt(3) == 512)
      assert(df.first().getInt(5) == 1)
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("should pass RS_GetBand") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/")
      df = df.selectExpr(" image.data as data", "image.nBands as bands")
      df = df.selectExpr("RS_GetBand(data, 1, bands) as targetBand")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0).length == 512 * 517)
    }

    it("should pass RS_Base64") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/")
      df = df.selectExpr("image.origin as origin","ST_GeomFromWkt(image.wkt) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
      df = df.selectExpr("RS_GetBand(data, 1, bands) as targetBand", "width","height")
      df.createOrReplaceTempView("geotiff")
      df = sparkSession.sql("Select RS_base64(height, width, targetBand, RS_Array(height*width, 0), RS_Array(height*width, 0)) as encodedstring from geotiff")
//      printf(df.first().getAs[String](0))
    }

    it("should pass RS_HTML") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/")
      df = df.selectExpr("image.origin as origin","ST_GeomFromWkt(image.wkt) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
      df = df.selectExpr("RS_GetBand(data, 1, bands) as targetBand", "width","height")
      df.createOrReplaceTempView("geotiff")
      df = sparkSession.sql("Select RS_base64(height, width, targetBand, RS_Array(height*width, 0.0), RS_Array(height*width, 0.0)) as encodedstring from geotiff")
      df = df.selectExpr("RS_HTML(encodedstring, '300') as htmlstring" )
      assert(df.first().getAs[String](0).contains("img"))
//      printf(df.first().getAs[String](0))
    }
  }
}





