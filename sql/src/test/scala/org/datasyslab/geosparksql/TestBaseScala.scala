/**
  * FILE: TestBaseScala
  * Copyright (c) 2015 - 2018 GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package org.datasyslab.geosparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait TestBaseScala extends FunSpec with BeforeAndAfterAll{
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab.geospark").setLevel(Level.WARN)

  val warehouseLocation = System.getProperty("user.dir") + "/target/"
  var sparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
    master("local[*]").appName("geosparksqlScalaTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport().getOrCreate()

  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

  val mixedWkbGeometryInputLocation = resourceFolder + "county_small_wkb.tsv"
  val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
  val geojsonInputLocation = resourceFolder + "testPolygon.json"
  val arealmPointInputLocation = resourceFolder + "arealm.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val unionPolygonInputLocation = resourceFolder + "testunion.csv"

  override def beforeAll(): Unit = {
    GeoSparkSQLRegistrator.registerAll(sparkSession)
  }

  override def afterAll(): Unit = {
    //GeoSparkSQLRegistrator.dropAll(spark)
    //spark.stop
  }
}
