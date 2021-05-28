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

import org.scalatest.{BeforeAndAfter, GivenWhenThen}

class rasterTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {
  val rasterDataName = "test.tif"
  var rasterdatalocation: String = resourceFolder + "raster/" + rasterDataName


    it("Should Pass geotiff loading") {
    var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/")
    df = df.selectExpr("image.Geometry as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nChannels as bands")
      df.show()
    assert(df.count()==2)

    }

    it("should pass RS_GetBand") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/")
      df = df.selectExpr(" image.data as data", "image.nChannels as bands")
      df = df.selectExpr("RS_GetBand(data, 1, bands) as targetBand")
      assert(df.first().getAs[Array[Byte]](0).length == 32*32)
    }

}





