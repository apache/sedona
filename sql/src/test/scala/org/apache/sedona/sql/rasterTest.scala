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

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.locationtech.jts.geom.Geometry
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import java.io.{File, FileWriter}
import java.util
import scala.collection.mutable

class rasterTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {
  val rasterDataName = "test.tif"
  var rasterdatalocation: String = resourceFolder + "raster/" + rasterDataName
  var conf: SparkConf = null
  var sc: JavaSparkContext = null
  var hdfsURI: String = null
  var rasterfileHDFSpath: String = null
  var fs: FileSystem = null
  var hdfsCluster: MiniDFSCluster = null
  var localcsvPath: String = null
  var hdfscsvpath: String = null

  import sparkSession.implicits._



  before {

    // Set up HDFS mini-cluster configurations
    val baseDir = new File("target/hdfs").getAbsoluteFile
    FileUtil.fullyDelete(baseDir)
    val hdfsConf = new HdfsConfiguration
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(hdfsConf)
    hdfsCluster = builder.build
    fs = FileSystem.get(hdfsConf)
    hdfsURI = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/"
    localcsvPath = baseDir.getAbsolutePath + "/train.csv"
    hdfscsvpath = hdfsURI + "train.csv"
  }

  describe("Geotiff loader test") {

    it("should fetch polygonal coordinates from geotiff image") {
      rasterfileHDFSpath = hdfsURI + rasterDataName
      fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath))
      createLocalFile()
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(localcsvPath)
      df = df.selectExpr(" ST_GeomFromGeotiff(_c0) as geom")
      assert(df.count == 2 && df.first().getAs[Geometry](0).getGeometryType === "Polygon")
    }

    // Testing ST_DataframeFromRaster constructor which converts spark dataframe into geotiff dataframe in Apache Sedona
    it("should fetch polygonal coordinates and band values as a single array from geotiff image") {
      rasterfileHDFSpath = hdfsURI + rasterDataName
      fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath))
      createLocalFile()
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(localcsvPath)
      df = df.selectExpr("ST_GeomWithBandsFromGeoTiff(_c0, 4) as rasterstruct")
      df.printSchema()
      df.selectExpr()
      df = df.selectExpr("rasterstruct.geometry as geom", "rasterstruct.bands as rasterBand")
      assert(df.count()==2 && df.first().getAs[mutable.WrappedArray[Double]](1).toArray.length == (512 * 517 * 4))
      df = df.selectExpr("RS_GetBand(rasterBand, 1, 4)") // Get the black band from RGBA
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](0)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("should fetch a particular band from result of ST_GeomWithBandsFromGeoTiff") {

      var inputDf = Seq((Seq(200.0,400.0,600.0,800.0,900.0,100.0)), (Seq(200.0,500.0,800.0,300.0,200.0,100.0))).toDF("GeoTiff")
      val resultDf = Seq((Seq(600.0,800.0)), (Seq(800.0, 300.0))).toDF("GeoTiff")
      inputDf = inputDf.selectExpr("RS_GetBand(GeoTiff,2,3) as GeoTiff")
      assert(resultDf.first().getAs[mutable.WrappedArray[Double]](0) == inputDf.first().getAs[mutable.WrappedArray[Double]](0))

    }

    def createLocalFile():Unit = {

      val rows = util.Arrays.asList(util.Arrays.asList(hdfsURI + rasterDataName), util.Arrays.asList(hdfsURI + rasterDataName))
      val csvWriter = new FileWriter(localcsvPath)
      for (i <- 0 until rows.size()) {
        csvWriter.append(String.join(",", rows.get(i)))
        csvWriter.append("\n")
      }
      csvWriter.flush()
      csvWriter.close()
    }

    after {
      hdfsCluster.shutdown()
      fs.close()
    }

  }

  describe("Map algebra tests") {
    it("Should pass add two bands") {
      val data = Seq(((200.0,400.0,600.0),(Seq(200.0, 400.0, 600.0))), (Seq(200.0,500.0,800.0), Seq(100.0, 500.0, 800.0))).toDF("Band1", "Band2")
      data.createOrReplaceTempView("givenDataframe")
      data.show()
      val resultDF = sparkSession.sql("Select rs_AddBands(Band1, Band2) as sumOfBands from givenDataframe")
      resultDF.show()
      assert(resultDF.first().getAs[mutable.WrappedArray[Double]](0).toArray.deep == Array(400.0, 800.0, 1200.0))

    }
  }

}





