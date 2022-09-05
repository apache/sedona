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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.locationtech.jts.geom.Geometry
import org.scalatest.BeforeAndAfterAll

import java.io.File

class geoparquetIOTests extends TestBaseScala with BeforeAndAfterAll {
  val geoparquetdatalocation1: String = resourceFolder + "geoparquet/example1.parquet"
  val geoparquetdatalocation2: String = resourceFolder + "geoparquet/example2.parquet"
  val geoparquetdatalocation3: String = resourceFolder + "geoparquet/example3.parquet"
  val geoparquetoutputlocation: String = resourceFolder + "geoparquet/geoparquet_output/"

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(geoparquetoutputlocation))

  describe("GeoParquet IO tests"){
    it("GEOPARQUET Test example1 i.e. naturalearth_lowers dataset's Read and Write"){
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("pop_est") == 920938)
      assert(rows.getAs[String]("continent") == "Oceania")
      assert(rows.getAs[String]("name") == "Fiji")
      assert(rows.getAs[String]("iso_a3") == "FJI")
      assert(rows.getAs[Double]("gdp_md_est") == 8374.0)
      assert(rows.getAs[Geometry]("geometry").toString == "MULTIPOLYGON (((180 -16.067132663642447, 180 -16.555216566639196, 179.36414266196414 -16.801354076946883, 178.72505936299711 -17.01204167436804, 178.59683859511713 -16.639150000000004, 179.0966093629971 -16.433984277547403, 179.4135093629971 -16.379054277547404, 180 -16.067132663642447)), ((178.12557 -17.50481, 178.3736 -17.33992, 178.71806 -17.62846, 178.55271 -18.15059, 177.93266000000003 -18.28799, 177.38146 -18.16432, 177.28504 -17.72465, 177.67087 -17.381140000000002, 178.12557 -17.50481)), ((-179.79332010904864 -16.020882256741224, -179.9173693847653 -16.501783135649397, -180 -16.555216566639196, -180 -16.067132663642447, -179.79332010904864 -16.020882256741224)))")
      df.write.format("geoparquet").mode(SaveMode.Overwrite).save(geoparquetoutputlocation + "/gp_sample1.parquet")
      val df2 = sparkSession.read.format("geoparquet").load(geoparquetoutputlocation + "/gp_sample1.parquet")
      val newrows = df2.collect()(0)
      assert(newrows.getAs[Geometry]("geometry").toString == "MULTIPOLYGON (((180 -16.067132663642447, 180 -16.555216566639196, 179.36414266196414 -16.801354076946883, 178.72505936299711 -17.01204167436804, 178.59683859511713 -16.639150000000004, 179.0966093629971 -16.433984277547403, 179.4135093629971 -16.379054277547404, 180 -16.067132663642447)), ((178.12557 -17.50481, 178.3736 -17.33992, 178.71806 -17.62846, 178.55271 -18.15059, 177.93266000000003 -18.28799, 177.38146 -18.16432, 177.28504 -17.72465, 177.67087 -17.381140000000002, 178.12557 -17.50481)), ((-179.79332010904864 -16.020882256741224, -179.9173693847653 -16.501783135649397, -180 -16.555216566639196, -180 -16.067132663642447, -179.79332010904864 -16.020882256741224)))")
    }
    it("GEOPARQUET Test example2 i.e. naturalearth_citie dataset's Read and Write"){
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation2)
      val rows = df.collect()(0)
      assert(rows.getAs[String]("name") == "Vatican City")
      assert(rows.getAs[Geometry]("geometry").toString == "POINT (12.453386544971766 41.903282179960115)")
      df.write.format("geoparquet").mode(SaveMode.Overwrite).save(geoparquetoutputlocation + "/gp_sample2.parquet")
      val df2 = sparkSession.read.format("geoparquet").load(geoparquetoutputlocation + "/gp_sample2.parquet")
      val newrows = df2.collect()(0)
      assert(newrows.getAs[String]("name") == "Vatican City")
      assert(newrows.getAs[Geometry]("geometry").toString == "POINT (12.453386544971766 41.903282179960115)")
    }
    it("GEOPARQUET Test example3 i.e. nybb dataset's Read and Write"){
      val df=sparkSession.read.format("geoparquet").load(geoparquetdatalocation3)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("BoroCode") == 5)
      assert(rows.getAs[String]("BoroName") == "Staten Island")
      assert(rows.getAs[Double]("Shape_Leng") == 330470.010332)
      assert(rows.getAs[Double]("Shape_Area") == 1.62381982381E9)
      assert(rows.getAs[Geometry]("geometry").toString.startsWith("MULTIPOLYGON (((970217.022"))
      df.write.format("geoparquet").mode(SaveMode.Overwrite).save(geoparquetoutputlocation + "/gp_sample3.parquet")
      val df2 = sparkSession.read.format("geoparquet").load(geoparquetoutputlocation + "/gp_sample3.parquet")
      val newrows = df2.collect()(0)
      assert(newrows.getAs[Geometry]("geometry").toString.startsWith("MULTIPOLYGON (((970217.022"))
    }
    it("GEOPARQUET Test example3 i.e. nybb dataset's Read and Write Options"){
      val df=sparkSession.read.format("geoparquet").option("fieldGeometry", "geometry").load(geoparquetdatalocation3)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("BoroCode") == 5)
      assert(rows.getAs[String]("BoroName") == "Staten Island")
      assert(rows.getAs[Double]("Shape_Leng") == 330470.010332)
      assert(rows.getAs[Double]("Shape_Area") == 1.62381982381E9)
      assert(rows.getAs[Geometry]("geometry").toString.startsWith("MULTIPOLYGON (((970217.022"))
      df.write.format("geoparquet").mode(SaveMode.Overwrite).save(geoparquetoutputlocation + "/gp_sample3o.parquet")
      val df2 = sparkSession.read.format("geoparquet").load(geoparquetoutputlocation + "/gp_sample3.parquet")
      val newrows = df2.collect()(0)
      assert(newrows.getAs[Geometry]("geometry").toString.startsWith("MULTIPOLYGON (((970217.022"))
    }
  }
}
