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

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait TestBaseScala extends FunSpec with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.apache.sedona.core").setLevel(Level.WARN)

  val warehouseLocation = System.getProperty("user.dir") + "/target/"
  val sparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName).
    master("local[*]").appName("sedonasqlScalaTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    // We need to be explicit about broadcasting in tests.
    .config("sedona.join.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()

  val resourceFolder = System.getProperty("user.dir") + "/../../core/src/test/resources/"
  val mixedWkbGeometryInputLocation = resourceFolder + "county_small_wkb.tsv"
  val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
  val shapefileWithMissingsTrailingInputLocation = resourceFolder + "shapefiles/missing"
  val geojsonInputLocation = resourceFolder + "testPolygon.json"
  val arealmPointInputLocation = resourceFolder + "arealm.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPolygon1InputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope1.csv"
  val csvPolygon2InputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope2.csv"
  val csvPolygon1RandomInputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope1_random.csv"
  val csvPolygon2RandomInputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope2_random.csv"
  val overlapPolygonInputLocation = resourceFolder + "testenvelope_overlap.csv"
  val unionPolygonInputLocation = resourceFolder + "testunion.csv"
  val intersectionPolygonInputLocation = resourceFolder + "test_intersection_aggregate.tsv"
  val intersectionPolygonNoIntersectionInputLocation = resourceFolder + "test_intersection_aggregate_no_intersection.tsv"
  val csvPoint1InputLocation = resourceFolder + "equalitycheckfiles/testequals_point1.csv"
  val csvPoint2InputLocation = resourceFolder + "equalitycheckfiles/testequals_point2.csv"
  val geojsonIdInputLocation = resourceFolder + "testContainsId.json"
  val smallAreasLocation: String = resourceFolder + "small/areas.csv"
  val smallPointsLocation: String = resourceFolder + "small/points.csv"
  val spatialJoinLeftInputLocation: String = resourceFolder + "spatial-predicates-test-data.tsv"
  val spatialJoinRightInputLocation: String = resourceFolder + "spatial-join-query-window.tsv"

  override def beforeAll(): Unit = {
    SedonaSQLRegistrator.registerAll(sparkSession)
  }

  override def afterAll(): Unit = {
    //SedonaSQLRegistrator.dropAll(spark)
    //spark.stop
  }

  def loadCsv(path: String): DataFrame = {
    sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(path)
  }

  lazy val buildPointDf = loadCsv(csvPointInputLocation).selectExpr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as pointshape")
  lazy val buildPolygonDf = loadCsv(csvPolygonInputLocation).selectExpr("ST_PolygonFromEnvelope(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20)), cast(_c2 as Decimal(24,20)), cast(_c3 as Decimal(24,20))) as polygonshape")
}
