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

import com.google.common.math.DoubleMath
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import org.apache.sedona.common.Functions.{frechetDistance, hausdorffDistance}
import org.apache.sedona.common.Predicates.dWithin
import org.apache.sedona.common.sphere.{Haversine, Spheroid}
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom._
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import java.io.File

trait TestBaseScala extends FunSpec with BeforeAndAfterAll {
  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.apache.sedona.core").setLevel(Level.WARN)

  // Default Spark configurations
  def defaultSparkConfig: Map[String, String] = Map(
    "spark.sql.warehouse.dir" -> (System.getProperty("user.dir") + "/target/"),
    "sedona.join.autoBroadcastJoinThreshold" -> "-1",
    "spark.kryoserializer.buffer.max" -> "64m")

  // Method to be overridden by subclasses to provide additional configurations
  def sparkConfig: Map[String, String] = defaultSparkConfig

  // Lazy initialization of Spark session using configurations
  lazy val sparkSession: SparkSession = {
    val builder = SedonaContext
      .builder()
      .master("local[*]")
      .appName("sedonasqlScalaTest")
    sparkConfig.foreach { case (key, value) => builder.config(key, value) }
    builder.getOrCreate()
  }

  // Lazy initialization of Spark context from the Spark session
  lazy val sc: SparkContext = sparkSession.sparkContext

  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"
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
  val csvPolygon1RandomInputLocation =
    resourceFolder + "equalitycheckfiles/testequals_envelope1_random.csv"
  val csvPolygon2RandomInputLocation =
    resourceFolder + "equalitycheckfiles/testequals_envelope2_random.csv"
  val overlapPolygonInputLocation = resourceFolder + "testenvelope_overlap.csv"
  val unionPolygonInputLocation = resourceFolder + "testunion.csv"
  val intersectionPolygonInputLocation = resourceFolder + "test_intersection_aggregate.tsv"
  val intersectionPolygonNoIntersectionInputLocation =
    resourceFolder + "test_intersection_aggregate_no_intersection.tsv"
  val csvPoint1InputLocation = resourceFolder + "equalitycheckfiles/testequals_point1.csv"
  val csvPoint2InputLocation = resourceFolder + "equalitycheckfiles/testequals_point2.csv"
  val geojsonIdInputLocation = resourceFolder + "testContainsId.json"
  val smallAreasLocation: String = resourceFolder + "small/areas.csv"
  val smallPointsLocation: String = resourceFolder + "small/points.csv"
  val spatialJoinLeftInputLocation: String = resourceFolder + "spatial-predicates-test-data.tsv"
  val spatialJoinRightInputLocation: String = resourceFolder + "spatial-join-query-window.tsv"
  val rasterDataLocation: String = resourceFolder + "raster/raster_with_no_data/test5.tiff"
  val buildingDataLocation: String = resourceFolder + "813_buildings_test.csv"
  val smallRasterDataLocation: String = resourceFolder + "raster/test1.tiff"
  private val factory = new GeometryFactory()

  override def beforeAll(): Unit = {
    super.beforeAll()
    SedonaContext.create(sparkSession)
  }

  override def afterAll(): Unit = {
    // SedonaSQLRegistrator.dropAll(spark)
//    sparkSession.stop
  }

  def loadCsv(path: String): DataFrame = {
    sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(path)
  }

  def loadCsvWithHeader(path: String): DataFrame = {
    sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(path)
  }

  def loadGeoTiff(path: String): DataFrame = {
    sparkSession.read.format("binaryFile").load(path)
  }

  def generateTestData(): Seq[(Int, Double, Geometry)] = {
    val geometries = (-180 to 180 by 10).flatMap { x =>
      (-80 to 80 by 10).map { y =>
        factory.createPoint(new Coordinate(x, y))
      }
    } ++ Seq(
      factory.createPoint(new Coordinate(0, -90)),
      factory.createPoint(new Coordinate(0, 90)))

    // uniformly generate geometries.size partitions of distance between [110000,  110000 + 2000000)
    geometries.zipWithIndex.map { case (geom, idx) =>
      (idx, 110000 + 2000000 * (idx.asInstanceOf[Double] / geometries.size), geom)
    }
  }

  def createSpheroidDataFrames(): Seq[DataFrame] = {
    import sparkSession.implicits._
    val sphericalTestData1 = generateTestData()
    val sphericalTestData2 = generateTestData()
    val sphericalDf1 = sphericalTestData1.toDF("id", "dist", "geom")
    val sphericalDf2 = sphericalTestData2.toDF("id", "dist", "geom")
    Seq(sphericalDf1, sphericalDf2)
  }

  lazy val buildPointDf = loadCsv(csvPointInputLocation).selectExpr(
    "ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as pointshape")
  lazy val buildPointLonLatDf = loadCsv(csvPointInputLocation).selectExpr(
    "ST_Point(cast(_c1 as Decimal(24,20)),cast(_c0 as Decimal(24,20))) as pointshape")
  lazy val buildPolygonDf = loadCsv(csvPolygonInputLocation).selectExpr(
    "ST_PolygonFromEnvelope(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20)), cast(_c2 as Decimal(24,20)), cast(_c3 as Decimal(24,20))) as polygonshape")
  lazy val buildRasterDf =
    loadGeoTiff(rasterDataLocation).selectExpr("RS_FromGeoTiff(content) as raster")
  lazy val buildBuildingsDf =
    loadCsvWithHeader(buildingDataLocation).selectExpr("ST_GeomFromWKT(geometry) as building")
  lazy val buildSmallRasterDf =
    loadGeoTiff(smallRasterDataLocation).selectExpr("RS_FromGeoTiff(content) as raster")

  protected final val FP_TOLERANCE: Double = 1e-12
  protected final val COORDINATE_SEQUENCE_COMPARATOR: CoordinateSequenceComparator =
    new CoordinateSequenceComparator(2) {
      override protected def compareCoordinate(
          s1: CoordinateSequence,
          s2: CoordinateSequence,
          i: Int,
          dimension: Int): Int = {
        for (d <- 0 until dimension) {
          val ord1: Double = s1.getOrdinate(i, d)
          val ord2: Double = s2.getOrdinate(i, d)
          val comp: Int = DoubleMath.fuzzyCompare(ord1, ord2, FP_TOLERANCE)
          if (comp != 0) return comp
        }
        0
      }
    }

  def withConf[T](conf: Map[String, String])(f: => T): T = {
    val oldConf = conf.values.map(key => key -> sparkSession.conf.getOption(key))
    conf.foreach { case (key, value) => sparkSession.conf.set(key, value) }
    try {
      f
    } finally {
      oldConf.foreach { case (key, value) =>
        value match {
          case Some(v) => sparkSession.conf.set(key, v)
          case None => sparkSession.conf.unset(key)
        }
      }
    }
  }

  protected def bruteForceDistanceJoinCountSpheroid(sampleCount: Int, distance: Double): Int = {
    val input = buildPointLonLatDf.limit(sampleCount).collect()
    input
      .map(row => {
        val point1 = row.getAs[org.locationtech.jts.geom.Point](0)
        input
          .map(row => {
            val point2 = row.getAs[org.locationtech.jts.geom.Point](0)
            if (Spheroid.distance(point1, point2) <= distance) 1 else 0
          })
          .sum
      })
      .sum
  }

  protected def bruteForceDistanceJoinCountSphere(sampleCount: Int, distance: Double): Int = {
    val input = buildPointLonLatDf.limit(sampleCount).collect()
    input
      .map(row => {
        val point1 = row.getAs[org.locationtech.jts.geom.Point](0)
        input
          .map(row => {
            val point2 = row.getAs[org.locationtech.jts.geom.Point](0)
            if (Haversine.distance(point1, point2) <= distance) 1 else 0
          })
          .sum
      })
      .sum
  }

  protected def bruteForceDistanceJoinHausdorff(
      sampleCount: Int,
      distance: Double,
      densityFrac: Double,
      intersects: Boolean): Int = {
    val inputPolygon = buildPolygonDf.limit(sampleCount).collect()
    val inputPoint = buildPointDf.limit(sampleCount).collect()
    inputPoint
      .map(row => {
        val point = row.getAs[org.locationtech.jts.geom.Point](0)
        inputPolygon
          .map(row => {
            val polygon = row.getAs[org.locationtech.jts.geom.Polygon](0)
            if (densityFrac == 0) {
              if (intersects)
                if (hausdorffDistance(point, polygon) <= distance) 1 else 0
              else if (hausdorffDistance(point, polygon) < distance) 1
              else 0
            } else {
              if (intersects)
                if (hausdorffDistance(point, polygon, densityFrac) <= distance) 1 else 0
              else if (hausdorffDistance(point, polygon, densityFrac) < distance) 1
              else 0
            }
          })
          .sum
      })
      .sum
  }

  protected def bruteForceDistanceJoinFrechet(
      sampleCount: Int,
      distance: Double,
      intersects: Boolean): Int = {
    val inputPolygon = buildPolygonDf.limit(sampleCount).collect()
    val inputPoint = buildPointDf.limit(sampleCount).collect()
    inputPoint
      .map(row => {
        val point = row.getAs[org.locationtech.jts.geom.Point](0)
        inputPolygon
          .map(row => {
            val polygon = row.getAs[org.locationtech.jts.geom.Polygon](0)
            if (intersects)
              if (frechetDistance(point, polygon) <= distance) 1 else 0
            else if (frechetDistance(point, polygon) < distance) 1
            else 0
          })
          .sum
      })
      .sum
  }

  protected def bruteForceDWithin(sampleCount: Int, distance: Double): Int = {
    val inputPolygon = buildPolygonDf.limit(sampleCount).collect()
    val inputPoint = buildPointDf.limit(sampleCount).collect()

    inputPoint
      .map(row => {
        val point = row.getAs[org.locationtech.jts.geom.Point](0)
        inputPolygon
          .map(row => {
            val polygon = row.getAs[org.locationtech.jts.geom.Polygon](0)
            if (dWithin(point, polygon, distance, false)) 1 else 0
          })
          .sum
      })
      .sum
  }

  protected def bruteForceDWithinSphere(distance: Double): Int = {
    val Seq(sphericalDf1, sphericalDf2) = createSpheroidDataFrames()
    val sphericalDf1Collected = sphericalDf1.collect()
    val sphericalDf2Collected = sphericalDf2.collect()
    sphericalDf1Collected
      .map(row => {
        val geom1 = row.get(2).asInstanceOf[org.locationtech.jts.geom.Point]
        sphericalDf2Collected
          .map(row => {
            val geom2 = row.get(2).asInstanceOf[org.locationtech.jts.geom.Point]
            if (dWithin(geom1, geom2, distance, true)) 1 else 0
          })
          .sum
      })
      .sum
  }

  /**
   * Create a mini HDFS cluster and return the HDFS instance and the URI.
   * @return
   *   (MiniDFSCluster, HDFS URI)
   */
  def creatMiniHdfs(): (MiniDFSCluster, String) = {
    val baseDir = new File("./target/hdfs/").getAbsoluteFile
    FileUtil.fullyDelete(baseDir)
    val hdfsConf = new HdfsConfiguration
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(hdfsConf)
    val hdfsCluster = builder.build
    (hdfsCluster, "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/")
  }
}
