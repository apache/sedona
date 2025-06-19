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

import io.minio.{MakeBucketArgs, MinioClient, PutObjectArgs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{BinaryType, BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.testcontainers.containers.MinIOContainer

import java.io.FileInputStream
import java.sql.{Date, Timestamp}
import java.util.TimeZone

class GeoPackageReaderTest extends TestBaseScala with Matchers {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  import sparkSession.implicits._

  val path: String = resourceFolder + "geopackage/example.gpkg"
  val polygonsPath: String = resourceFolder + "geopackage/features.gpkg"
  val rasterPath: String = resourceFolder + "geopackage/raster.gpkg"
  val wktReader = new org.locationtech.jts.io.WKTReader()
  val wktWriter = new org.locationtech.jts.io.WKTWriter()

  val expectedFeatureSchema = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("geometry", GeometryUDT, true),
      StructField("text", StringType, true),
      StructField("real", DoubleType, true),
      StructField("boolean", BooleanType, true),
      StructField("blob", BinaryType, true),
      StructField("integer", IntegerType, true),
      StructField("text_limited", StringType, true),
      StructField("blob_limited", BinaryType, true),
      StructField("date", DateType, true),
      StructField("datetime", TimestampType, true)))

  describe("Reading GeoPackage metadata") {
    it("should read GeoPackage metadata") {
      val df = sparkSession.read
        .format("geopackage")
        .option("showMetadata", "true")
        .load(path)

      df.where("data_type = 'tiles'").show(false)

      df.count shouldEqual 34
    }
  }

  describe("Reading Vector data") {
    it("should read GeoPackage - point1") {
      val df = readFeatureData("point1")
      df.schema shouldEqual expectedFeatureSchema

      df.count() shouldEqual 4

      val firstElement = df.collectAsList().get(0).toSeq

      val expectedValues = Seq(
        1,
        wktReader.read(POINT_1),
        "BIT Systems",
        4519.866024037493,
        true,
        Array(48, 99, 57, 54, 49, 56, 55, 54, 45, 98, 102, 100, 52, 45, 52, 102, 52, 48, 45, 97,
          49, 102, 101, 45, 55, 49, 55, 101, 57, 100, 50, 98, 48, 55, 98, 101),
        3,
        "bcd5a36f-16dc-4385-87be-b40353848597",
        Array(49, 50, 53, 50, 97, 99, 98, 52, 45, 57, 54, 54, 52, 45, 52, 101, 51, 50, 45, 57, 54,
          100, 101, 45, 56, 48, 54, 101, 101, 48, 101, 101, 49, 102, 57, 48),
        Date.valueOf("2023-09-19"),
        Timestamp.valueOf("2023-09-19 11:24:15.695"))

      firstElement should contain theSameElementsAs expectedValues
    }

    it("should read GeoPackage - line1") {
      val df = readFeatureData("line1")
        .withColumn("datetime", expr("from_utc_timestamp(datetime, 'UTC')"))

      df.schema shouldEqual expectedFeatureSchema

      df.count() shouldEqual 3

      val firstElement = df.collectAsList().get(0).toSeq

      firstElement should contain theSameElementsAs Seq(
        1,
        wktReader.read(LINESTRING_1),
        "East Lockheed Drive",
        1990.5159635296877,
        false,
        Array(54, 97, 98, 100, 98, 51, 97, 56, 45, 54, 53, 101, 48, 45, 52, 55, 48, 54, 45, 56,
          50, 52, 48, 45, 51, 57, 48, 55, 99, 50, 102, 102, 57, 48, 99, 55),
        1,
        "13dd91dc-3b7d-4d8d-a0ca-b3afb8e31c3d",
        Array(57, 54, 98, 102, 56, 99, 101, 56, 45, 102, 48, 54, 49, 45, 52, 55, 99, 48, 45, 97,
          98, 48, 101, 45, 97, 99, 50, 52, 100, 98, 50, 97, 102, 50, 50, 54),
        Date.valueOf("2023-09-19"),
        Timestamp.valueOf("2023-09-19 11:24:15.716"))
    }

    it("should read GeoPackage - polygon1") {
      val df = readFeatureData("polygon1")
      df.count shouldEqual 3
      df.schema shouldEqual expectedFeatureSchema

      df.select("geometry").collectAsList().get(0).toSeq should contain theSameElementsAs Seq(
        wktReader.read(POLYGON_1))
    }

    it("should read GeoPackage - geometry1") {
      val df = readFeatureData("geometry1")
      df.count shouldEqual 10
      df.schema shouldEqual expectedFeatureSchema

      df.selectExpr("ST_ASTEXT(geometry)")
        .as[String]
        .collect() should contain theSameElementsAs Seq(
        POINT_1,
        POINT_2,
        POINT_3,
        POINT_4,
        LINESTRING_1,
        LINESTRING_2,
        LINESTRING_3,
        POLYGON_1,
        POLYGON_2,
        POLYGON_3)
    }

    it("should read polygon with envelope data") {
      val tables = Table(
        ("tableName", "expectedCount"),
        ("GB_Hex_5km_GS_CompressibleGround_v8", 4233),
        ("GB_Hex_5km_GS_Landslides_v8", 4228),
        ("GB_Hex_5km_GS_RunningSand_v8", 4233),
        ("GB_Hex_5km_GS_ShrinkSwell_v8", 4233),
        ("GB_Hex_5km_GS_SolubleRocks_v8", 4295))

      forAll(tables) { (tableName: String, expectedCount: Int) =>
        val df = sparkSession.read
          .format("geopackage")
          .option("tableName", tableName)
          .load(polygonsPath)

        df.count() shouldEqual expectedCount
      }
    }
  }

  describe("GeoPackage Raster Data Test") {
    it("should read") {
      val fractions =
        Table(
          ("tableName", "channelNumber", "expectedSum"),
          ("point1_tiles", 4, 466591.0),
          ("line1_tiles", 4, 5775976.0),
          ("polygon1_tiles", 4, 1.1269871e7),
          ("geometry1_tiles", 4, 2.6328442e7),
          ("point2_tiles", 4, 137456.0),
          ("line2_tiles", 4, 6701101.0),
          ("polygon2_tiles", 4, 5.1170714e7),
          ("geometry2_tiles", 4, 1.6699823e7),
          ("bit_systems", 1, 6.5561879e7),
          ("nga", 1, 6.8078856e7),
          ("bit_systems_wgs84", 1, 7.7276934e7),
          ("nga_pc", 1, 2.90590616e8),
          ("bit_systems_world", 1, 7.7276934e7),
          ("nga_pc_world", 1, 2.90590616e8))

      forAll(fractions) { (tableName: String, channelNumber: Int, expectedSum: Double) =>
        {
          val df = readFeatureData(tableName)
          val calculatedSum = df
            .selectExpr(s"RS_SummaryStats(tile_data, 'sum', ${channelNumber}) as stats")
            .selectExpr("sum(stats)")
            .as[Double]

          calculatedSum.collect().head shouldEqual expectedSum
        }
      }
    }

    it("should be able to read complex raster data") {
      val df = sparkSession.read
        .format("geopackage")
        .option("tableName", "AuroraAirportNoise")
        .load(rasterPath)

      df.show(5)

      val calculatedSum = df
        .selectExpr(s"RS_SummaryStats(tile_data, 'sum', ${1}) as stats")
        .selectExpr("sum(stats)")
        .as[Double]

      calculatedSum.first() shouldEqual 2.027126e7

      val df2 = sparkSession.read
        .format("geopackage")
        .option("tableName", "LiquorLicenseDensity")
        .load(rasterPath)

      val calculatedSum2 = df2
        .selectExpr(s"RS_SummaryStats(tile_data, 'sum', ${1}) as stats")
        .selectExpr("sum(stats)")
        .as[Double]

      calculatedSum2.first() shouldEqual 2.882028e7
    }

  }

  describe("Reading from S3") {
    it("should be able to read files from S3") {
      val container = new MinIOContainer("minio/minio:latest")

      container.start()

      val minioClient = createMinioClient(container)
      val makeBucketRequest = MakeBucketArgs
        .builder()
        .bucket("sedona")
        .build()

      minioClient.makeBucket(makeBucketRequest)

      adjustSparkSession(sparkSessionMinio, container)

      val inputPath: String = prepareFile("example.geopackage", path, minioClient)

      sparkSessionMinio.read
        .format("geopackage")
        .option("showMetadata", "true")
        .load(inputPath)
        .count shouldEqual 34

      val df = sparkSession.read
        .format("geopackage")
        .option("tableName", "point1")
        .load(inputPath)

      df.count shouldEqual 4

      val inputPathLarger: String = prepareFiles((1 to 300).map(_ => path).toArray, minioClient)

      val dfLarger = sparkSessionMinio.read
        .format("geopackage")
        .option("tableName", "point1")
        .load(inputPathLarger)

      dfLarger.count shouldEqual 300 * 4

      container.stop()
    }
  }

  private def readFeatureData(tableName: String): DataFrame = {
    sparkSession.read
      .format("geopackage")
      .option("tableName", tableName)
      .load(path)
  }

  private def prepareFiles(paths: Array[String], minioClient: MinioClient): String = {
    val key = "geopackage"

    paths.foreach(path => {
      val fis = new FileInputStream(path);
      putFileIntoBucket(
        "sedona",
        s"${key}/${scala.util.Random.nextInt(1000000000)}.geopackage",
        fis,
        minioClient)
    })

    s"s3a://sedona/$key"
  }

  private def prepareFile(name: String, path: String, minioClient: MinioClient): String = {
    val fis = new FileInputStream(path);
    putFileIntoBucket("sedona", name, fis, minioClient)

    s"s3a://sedona/$name"
  }

  private val POINT_1 = "POINT (-104.801918 39.720014)"
  private val POINT_2 = "POINT (-104.802987 39.717703)"
  private val POINT_3 = "POINT (-104.807496 39.714085)"
  private val POINT_4 = "POINT (-104.79948 39.714729)"
  private val LINESTRING_1 =
    "LINESTRING (-104.800614 39.720721, -104.802174 39.720726, -104.802584 39.72066, -104.803088 39.720477, -104.803474 39.720209)"
  private val LINESTRING_2 =
    "LINESTRING (-104.809612 39.718379, -104.806638 39.718372, -104.806236 39.718439, -104.805939 39.718536, -104.805654 39.718677, -104.803652 39.720095)"
  private val LINESTRING_3 =
    "LINESTRING (-104.806344 39.722425, -104.805854 39.722634, -104.805656 39.722647, -104.803749 39.722641, -104.803769 39.721849, -104.803806 39.721725, -104.804382 39.720865)"
  private val POLYGON_1 =
    "POLYGON ((-104.802246 39.720343, -104.802246 39.719753, -104.802183 39.719754, -104.802184 39.719719, -104.802138 39.719694, -104.802097 39.719691, -104.802096 39.719648, -104.801646 39.719648, -104.801644 39.719722, -104.80155 39.719723, -104.801549 39.720207, -104.801648 39.720207, -104.801648 39.720341, -104.802246 39.720343))"
  private val POLYGON_2 =
    "POLYGON ((-104.802259 39.719604, -104.80226 39.71955, -104.802281 39.719416, -104.802332 39.719372, -104.802081 39.71924, -104.802044 39.71929, -104.802027 39.719278, -104.802044 39.719229, -104.801785 39.719129, -104.801639 39.719413, -104.801649 39.719472, -104.801694 39.719524, -104.801753 39.71955, -104.80175 39.719606, -104.80194 39.719606, -104.801939 39.719555, -104.801977 39.719556, -104.801979 39.719606, -104.802259 39.719604), (-104.80213 39.71944, -104.802133 39.71949, -104.802148 39.71949, -104.80218 39.719473, -104.802187 39.719456, -104.802182 39.719439, -104.802088 39.719387, -104.802047 39.719427, -104.801858 39.719342, -104.801883 39.719294, -104.801832 39.719284, -104.801787 39.719298, -104.801763 39.719331, -104.801823 39.719352, -104.80179 39.71942, -104.801722 39.719404, -104.801715 39.719445, -104.801748 39.719484, -104.801809 39.719494, -104.801816 39.719439, -104.80213 39.71944))"
  private val POLYGON_3 =
    "POLYGON ((-104.802867 39.718122, -104.802369 39.717845, -104.802571 39.71763, -104.803066 39.717909, -104.802867 39.718122))"
}
