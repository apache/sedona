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

import org.apache.sedona.common.raster.RasterConstructors
import org.apache.spark.sql.functions.{col, expr, udf}
import org.geotools.coverage.grid.GridCoverage2D

/**
 * Executable copies of the Scala examples on docs/api/sql/Raster-UDF.md. When an example changes
 * there, change it here.
 */
class rasterUdfTest extends TestBaseScala {

  val makeRaster: String =
    "RS_MakeRasterForTesting(4, 'D', 'BandedSampleModel', 4, 3, 100, 100, 10, -10, 0, 0, 3857)"

  describe("Scala UDFs over rasters") {

    it("takes a raster and returns a scalar") {
      val df = sparkSession.range(1).withColumn("rast", expr(makeRaster))
      val numBands = udf((raster: GridCoverage2D) => raster.getNumSampleDimensions)
      val result = df.select(numBands(col("rast")).alias("num_bands")).first()
      assert(result.getInt(0) == 4)
    }

    it("takes a raster and returns a raster") {
      val df = sparkSession.range(1).withColumn("rast", expr(makeRaster))
      val process = udf((raster: GridCoverage2D) => raster)
      val result = df
        .select(process(col("rast")).alias("rast"))
        .selectExpr("RS_NumBands(rast) AS nb", "RS_SRID(rast) AS srid")
        .first()
      assert(result.getInt(0) == 4)
      assert(result.getInt(1) == 3857)
    }

    it("is not pinned to the input's grid, unlike the Python path") {
      // Backs the claim that a Scala UDF controls the band count, CRS, cell size and
      // extent of what it returns: 4x3 @ scale 10 in EPSG:3857 goes in, 7x5 @ scale 25
      // in EPSG:4326 comes out.
      val df = sparkSession.range(1).withColumn("rast", expr(makeRaster))
      val regrid = udf((_: GridCoverage2D) =>
        RasterConstructors.makeEmptyRaster(2, "D", 7, 5, 10.0, 20.0, 25.0, -25.0, 0, 0, 4326))
      val result = df
        .select(regrid(col("rast")).alias("out"))
        .selectExpr(
          "RS_Width(out) AS w",
          "RS_Height(out) AS h",
          "RS_NumBands(out) AS nb",
          "RS_ScaleX(out) AS sx",
          "RS_SRID(out) AS srid")
        .first()
      assert(result.getInt(0) == 7)
      assert(result.getInt(1) == 5)
      assert(result.getInt(2) == 2)
      assert(result.getDouble(3) == 25.0)
      assert(result.getInt(4) == 4326)
    }
  }
}
