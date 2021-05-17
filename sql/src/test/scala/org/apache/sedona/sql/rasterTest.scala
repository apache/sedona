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
  var resourcefolder: String = System.getProperty("user.dir") + "/../core/src/test/resources/"
  var rasterdatalocation: String = resourcefolder + "raster/image.tif"
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
    print(localcsvPath)
    hdfscsvpath = hdfsURI + "train.csv"
  }

  describe("Geotiff loader test") {

    it("should fetch polygonal coordinates from geotiff image") {
      rasterfileHDFSpath = hdfsURI + "image.tif"
      fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath))
      createLocalFile()
      val df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(localcsvPath)
      df.show(false)
      df.createOrReplaceTempView("inputtable")
      val spatialDf = sparkSession.sql("select ST_GeomFromGeotiff(inputtable._c0) as countyshape from inputtable")
      spatialDf.show()
      assert(spatialDf.count == 2 && spatialDf.first().getAs[Geometry](0).getGeometryType === "Polygon")
    }

    // Testing ST_DataframeFromRaster constructor which converts spark dataframe into geotiff dataframe in Apache Sedona
    it("should fetch polygonal coordinates and band values as a single array from geotiff image") {
      rasterfileHDFSpath = hdfsURI + "image.tif"
      fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath))
      createLocalFile()
      val df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(localcsvPath)
      df.createOrReplaceTempView("inputtable")
      val spatialDf = sparkSession.sql("select ST_GeomWithBandsFromGeoTiff(inputtable._c0, 4) as rasterstruct from inputtable")
      spatialDf.show(false)
      spatialDf.printSchema()
      spatialDf.createOrReplaceTempView("sedonaframe")
      val sedonaDF = sparkSession.sql("select rasterstruct.Polygon as geom, rasterstruct.bands as rasterBand from sedonaframe")
      sedonaDF.show()
      sedonaDF.createOrReplaceTempView("sedonaDF")
      assert(sedonaDF.count()==2 && sedonaDF.first().getAs[mutable.WrappedArray[Double]](1).toArray.length==4096)

    }

    it("should fetch a particular band from result of ST_GeomWithBandsFromGeoTiff") {

      val data = Seq((Seq(200.0,400.0,600.0,800.0,900.0,100.0)), (Seq(200.0,500.0,800.0,300.0,200.0,100.0))).toDF("TotalBand")
      val resultDF = Seq((Seq(600.0,800.0)), (Seq(800.0, 300.0))).toDF("band2")
      data.createOrReplaceTempView("allBandsDF")
      val targetbandDF = sparkSession.sql("Select ST_GetBand(TotalBand,2,3) as band2 from allBandsDF")
      data.show(false)
      targetbandDF.show(false)
      assert(resultDF.first().getAs[mutable.WrappedArray[Double]](0) == targetbandDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    def createLocalFile():Unit = {

      val rows = util.Arrays.asList(util.Arrays.asList(hdfsURI + "image.tif"), util.Arrays.asList(hdfsURI + "image.tif"))
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

}





