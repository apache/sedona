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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.geoparquet.{Covering, GeoParquetMetaData}
import org.apache.spark.sql.execution.datasources.geoparquet.internal.ParquetReadSupport
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.{ST_Point, ST_PolygonFromEnvelope}
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_Intersects
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampNTZType
import org.json4s.jackson.parseJson
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.ByteOrder
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

class geoparquetIOTests extends TestBaseScala with BeforeAndAfterAll {
  val geoparquetdatalocation1: String = resourceFolder + "geoparquet/example1.parquet"
  val geoparquetdatalocation2: String = resourceFolder + "geoparquet/example2.parquet"
  val geoparquetdatalocation3: String = resourceFolder + "geoparquet/example3.parquet"
  val geoparquetdatalocation4: String = resourceFolder + "geoparquet/example-1.0.0-beta.1.parquet"
  val geoparquetdatalocation5: String = resourceFolder + "geoparquet/example-1.1.0.parquet"
  val legacyparquetdatalocation: String =
    resourceFolder + "parquet/legacy-parquet-nested-columns.snappy.parquet"
  val geoparquetoutputlocation: String = resourceFolder + "geoparquet/geoparquet_output/"
  val overtureBBOX: String = resourceFolder + "geoparquet/overture/bbox.geoparquet"
  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(geoparquetoutputlocation))

  describe("GeoParquet IO tests") {
    it("GEOPARQUET Test example1 i.e. naturalearth_lowers dataset's Read and Write") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("pop_est") == 920938)
      assert(rows.getAs[String]("continent") == "Oceania")
      assert(rows.getAs[String]("name") == "Fiji")
      assert(rows.getAs[String]("iso_a3") == "FJI")
      assert(rows.getAs[Double]("gdp_md_est") == 8374.0)
      assert(
        rows
          .getAs[Geometry]("geometry")
          .toString == "MULTIPOLYGON (((180 -16.067132663642447, 180 -16.555216566639196, 179.36414266196414 -16.801354076946883, 178.72505936299711 -17.01204167436804, 178.59683859511713 -16.639150000000004, 179.0966093629971 -16.433984277547403, 179.4135093629971 -16.379054277547404, 180 -16.067132663642447)), ((178.12557 -17.50481, 178.3736 -17.33992, 178.71806 -17.62846, 178.55271 -18.15059, 177.93266000000003 -18.28799, 177.38146 -18.16432, 177.28504 -17.72465, 177.67087 -17.381140000000002, 178.12557 -17.50481)), ((-179.79332010904864 -16.020882256741224, -179.9173693847653 -16.501783135649397, -180 -16.555216566639196, -180 -16.067132663642447, -179.79332010904864 -16.020882256741224)))")
      df.write
        .format("geoparquet")
        .mode(SaveMode.Overwrite)
        .save(geoparquetoutputlocation + "/gp_sample1.parquet")
      val df2 = sparkSession.read
        .format("geoparquet")
        .load(geoparquetoutputlocation + "/gp_sample1.parquet")
      val newrows = df2.collect()(0)
      assert(
        newrows
          .getAs[Geometry]("geometry")
          .toString == "MULTIPOLYGON (((180 -16.067132663642447, 180 -16.555216566639196, 179.36414266196414 -16.801354076946883, 178.72505936299711 -17.01204167436804, 178.59683859511713 -16.639150000000004, 179.0966093629971 -16.433984277547403, 179.4135093629971 -16.379054277547404, 180 -16.067132663642447)), ((178.12557 -17.50481, 178.3736 -17.33992, 178.71806 -17.62846, 178.55271 -18.15059, 177.93266000000003 -18.28799, 177.38146 -18.16432, 177.28504 -17.72465, 177.67087 -17.381140000000002, 178.12557 -17.50481)), ((-179.79332010904864 -16.020882256741224, -179.9173693847653 -16.501783135649397, -180 -16.555216566639196, -180 -16.067132663642447, -179.79332010904864 -16.020882256741224)))")
      // The endianness of the WKB should be system native.
      val df2Binary = sparkSession.read.parquet(geoparquetoutputlocation + "/gp_sample1.parquet")
      df2Binary.collect().foreach { row =>
        val wkb = row.getAs[Array[Byte]]("geometry")
        wkb(0) match {
          case 0x00 => assert(ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN)
          case 0x01 => assert(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN)
        }
      }
    }
    it("GEOPARQUET Test example2 i.e. naturalearth_citie dataset's Read and Write") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation2)
      val rows = df.collect()(0)
      assert(rows.getAs[String]("name") == "Vatican City")
      assert(
        rows
          .getAs[Geometry]("geometry")
          .toString == "POINT (12.453386544971766 41.903282179960115)")
      df.write
        .format("geoparquet")
        .mode(SaveMode.Overwrite)
        .save(geoparquetoutputlocation + "/gp_sample2.parquet")
      val df2 = sparkSession.read
        .format("geoparquet")
        .load(geoparquetoutputlocation + "/gp_sample2.parquet")
      val newrows = df2.collect()(0)
      assert(newrows.getAs[String]("name") == "Vatican City")
      assert(
        newrows
          .getAs[Geometry]("geometry")
          .toString == "POINT (12.453386544971766 41.903282179960115)")
    }
    it("GEOPARQUET Test example3 i.e. nybb dataset's Read and Write") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation3)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("BoroCode") == 5)
      assert(rows.getAs[String]("BoroName") == "Staten Island")
      assert(rows.getAs[Double]("Shape_Leng") == 330470.010332)
      assert(rows.getAs[Double]("Shape_Area") == 1.62381982381e9)
      assert(rows.getAs[Geometry]("geometry").toString.startsWith("MULTIPOLYGON (((970217.022"))
      df.write
        .format("geoparquet")
        .mode(SaveMode.Overwrite)
        .save(geoparquetoutputlocation + "/gp_sample3.parquet")
      val df2 = sparkSession.read
        .format("geoparquet")
        .load(geoparquetoutputlocation + "/gp_sample3.parquet")
      val newrows = df2.collect()(0)
      assert(
        newrows.getAs[Geometry]("geometry").toString.startsWith("MULTIPOLYGON (((970217.022"))
    }
    it("GEOPARQUET Test example-1.0.0-beta.1.parquet") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation4)
      val count = df.count()
      val rows = df.collect()
      assert(rows(0).getAs[AnyRef]("geometry").isInstanceOf[Geometry])
      assert(count == rows.length)

      val geoParquetSavePath = geoparquetoutputlocation + "/gp_sample4.parquet"
      df.write.format("geoparquet").mode(SaveMode.Overwrite).save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val newRows = df2.collect()
      assert(rows.length == newRows.length)
      assert(newRows(0).getAs[AnyRef]("geometry").isInstanceOf[Geometry])
      assert(rows sameElements newRows)

      val parquetFiles =
        new File(geoParquetSavePath).listFiles().filter(_.getName.endsWith(".parquet"))
      parquetFiles.foreach { filePath =>
        val metadata = ParquetFileReader
          .open(HadoopInputFile.fromPath(new Path(filePath.getPath), new Configuration()))
          .getFooter
          .getFileMetaData
          .getKeyValueMetaData
        assert(metadata.containsKey("geo"))
        val geo = parseJson(metadata.get("geo"))
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val columnName = (geo \ "primary_column").extract[String]
        assert(columnName == "geometry")
        val geomTypes = (geo \ "columns" \ "geometry" \ "geometry_types").extract[Seq[String]]
        assert(geomTypes.nonEmpty)
        val sparkSqlRowMetadata = metadata.get(ParquetReadSupport.SPARK_METADATA_KEY)
        assert(!sparkSqlRowMetadata.contains("GeometryUDT()"))
      }
    }
    it("GEOPARQUET Test example-1.1.0.parquet") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation5)
      val count = df.count()
      val rows = df.collect()
      assert(rows(0).getAs[AnyRef]("geometry").isInstanceOf[Geometry])
      assert(count == rows.length)

      val geoParquetSavePath = geoparquetoutputlocation + "/gp_sample5.parquet"
      df.write.format("geoparquet").mode(SaveMode.Overwrite).save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val newRows = df2.collect()
      assert(rows.length == newRows.length)
      assert(newRows(0).getAs[AnyRef]("geometry").isInstanceOf[Geometry])
      assert(rows sameElements newRows)
    }

    it("GeoParquet with multiple geometry columns") {
      val wktReader = new WKTReader()
      val testData = Seq(
        Row(
          1,
          wktReader.read("POINT (1 2)"),
          wktReader.read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")),
        Row(
          2,
          wktReader.read("POINT Z(1 2 3)"),
          wktReader.read("POLYGON Z((0 0 2, 1 0 2, 1 1 2, 0 1 2, 0 0 2))")),
        Row(
          3,
          wktReader.read("MULTIPOINT (0 0, 1 1, 2 2)"),
          wktReader.read("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))")))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("g0", GeometryUDT(), nullable = false),
          StructField("g1", GeometryUDT(), nullable = false)))
      val df = sparkSession.createDataFrame(testData.asJava, schema).repartition(1)
      val geoParquetSavePath = geoparquetoutputlocation + "/multi_geoms.parquet"
      df.write.format("geoparquet").mode("overwrite").save(geoParquetSavePath)

      // Find parquet files in geoParquetSavePath directory and validate their metadata
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val version = (geo \ "version").extract[String]
        assert(version == GeoParquetMetaData.VERSION)
        val g0Types = (geo \ "columns" \ "g0" \ "geometry_types").extract[Seq[String]]
        val g1Types = (geo \ "columns" \ "g1" \ "geometry_types").extract[Seq[String]]
        assert(g0Types.sorted == Seq("Point", "Point Z", "MultiPoint").sorted)
        assert(g1Types.sorted == Seq("Polygon", "Polygon Z", "MultiLineString").sorted)
        val g0Crs = geo \ "columns" \ "g0" \ "crs"
        val g1Crs = geo \ "columns" \ "g1" \ "crs"
        assert(g0Crs == org.json4s.JNull)
        assert(g1Crs == org.json4s.JNull)
      }

      // Read GeoParquet with multiple geometry columns
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      assert(df2.schema.fields(1).dataType.isInstanceOf[GeometryUDT])
      assert(df2.schema.fields(2).dataType.isInstanceOf[GeometryUDT])
      val rows = df2.collect()
      assert(testData.length == rows.length)
      assert(rows(0).getAs[AnyRef]("g0").isInstanceOf[Geometry])
      assert(rows(0).getAs[AnyRef]("g1").isInstanceOf[Geometry])
    }

    it("GeoParquet save should work with empty dataframes") {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("g", GeometryUDT(), nullable = false)))
      val df = sparkSession.createDataFrame(Collections.emptyList[Row](), schema)
      val geoParquetSavePath = geoparquetoutputlocation + "/empty.parquet"
      df.write.format("geoparquet").mode("overwrite").save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      assert(df2.schema.fields(1).dataType.isInstanceOf[GeometryUDT])
      assert(0 == df2.count())

      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val g0Types = (geo \ "columns" \ "g" \ "geometry_types").extract[Seq[String]]
        val g0BBox = (geo \ "columns" \ "g" \ "bbox").extract[Seq[Double]]
        assert(g0Types.isEmpty)
        assert(g0BBox == Seq(0.0, 0.0, 0.0, 0.0))
      }
    }

    it("GeoParquet save should work with snake_case column names") {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("geom_column", GeometryUDT(), nullable = false)))
      val df = sparkSession.createDataFrame(Collections.emptyList[Row](), schema)
      val geoParquetSavePath = geoparquetoutputlocation + "/snake_case_column_name.parquet"
      df.write.format("geoparquet").mode("overwrite").save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val geomField = df2.schema.fields(1)
      assert(geomField.name == "geom_column")
      assert(geomField.dataType.isInstanceOf[GeometryUDT])
      assert(0 == df2.count())
    }

    it("GeoParquet save should work with camelCase column names") {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("geomColumn", GeometryUDT(), nullable = false)))
      val df = sparkSession.createDataFrame(Collections.emptyList[Row](), schema)
      val geoParquetSavePath = geoparquetoutputlocation + "/camel_case_column_name.parquet"
      df.write.format("geoparquet").mode("overwrite").save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val geomField = df2.schema.fields(1)
      assert(geomField.name == "geomColumn")
      assert(geomField.dataType.isInstanceOf[GeometryUDT])
      assert(0 == df2.count())
    }

    it("GeoParquet save should write user specified version and crs to geo metadata") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation4)
      // This CRS is taken from https://proj.org/en/9.3/specifications/projjson.html#geographiccrs
      // with slight modification.
      val projjson =
        """
          |{
          |  "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
          |  "type": "GeographicCRS",
          |  "name": "NAD83(2011)",
          |  "datum": {
          |    "type": "GeodeticReferenceFrame",
          |    "name": "NAD83 (National Spatial Reference System 2011)",
          |    "ellipsoid": {
          |      "name": "GRS 1980",
          |      "semi_major_axis": 6378137,
          |      "inverse_flattening": 298.257222101
          |    }
          |  },
          |  "coordinate_system": {
          |    "subtype": "ellipsoidal",
          |    "axis": [
          |      {
          |        "name": "Geodetic latitude",
          |        "abbreviation": "Lat",
          |        "direction": "north",
          |        "unit": "degree"
          |      },
          |      {
          |        "name": "Geodetic longitude",
          |        "abbreviation": "Lon",
          |        "direction": "east",
          |        "unit": "degree"
          |      }
          |    ]
          |  },
          |  "scope": "Horizontal component of 3D system.",
          |  "area": "Puerto Rico - onshore and offshore. United States (USA) onshore and offshore.",
          |  "bbox": {
          |    "south_latitude": 14.92,
          |    "west_longitude": 167.65,
          |    "north_latitude": 74.71,
          |    "east_longitude": -63.88
          |  },
          |  "id": {
          |    "authority": "EPSG",
          |    "code": 6318
          |  }
          |}
          |""".stripMargin
      var geoParquetSavePath = geoparquetoutputlocation + "/gp_custom_meta.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.version", "10.9.8")
        .option("geoparquet.crs", projjson)
        .mode("overwrite")
        .save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      assert(df2.count() == df.count())

      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val version = (geo \ "version").extract[String]
        val columnName = (geo \ "primary_column").extract[String]
        assert(version == "10.9.8")
        val crs = geo \ "columns" \ columnName \ "crs"
        assert(crs.isInstanceOf[org.json4s.JObject])
        assert(crs == parseJson(projjson))
      }

      // Setting crs to null explicitly
      geoParquetSavePath = geoparquetoutputlocation + "/gp_crs_null.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "null")
        .mode("overwrite")
        .save(geoParquetSavePath)
      val df3 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      assert(df3.count() == df.count())

      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val columnName = (geo \ "primary_column").extract[String]
        val crs = geo \ "columns" \ columnName \ "crs"
        assert(crs == org.json4s.JNull)
      }

      // Setting crs to "" to omit crs
      geoParquetSavePath = geoparquetoutputlocation + "/gp_crs_omit.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val columnName = (geo \ "primary_column").extract[String]
        val crs = geo \ "columns" \ columnName \ "crs"
        assert(crs == org.json4s.JNothing)
      }
    }

    it("GeoParquet save should support specifying per-column CRS") {
      val wktReader = new WKTReader()
      val testData = Seq(
        Row(
          1,
          wktReader.read("POINT (1 2)"),
          wktReader.read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("g0", GeometryUDT(), nullable = false),
          StructField("g1", GeometryUDT(), nullable = false)))
      val df = sparkSession.createDataFrame(testData.asJava, schema).repartition(1)

      val projjson0 =
        """
          |{
          |  "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
          |  "type": "GeographicCRS",
          |  "name": "NAD83(2011)",
          |  "datum": {
          |    "type": "GeodeticReferenceFrame",
          |    "name": "NAD83 (National Spatial Reference System 2011)",
          |    "ellipsoid": {
          |      "name": "GRS 1980",
          |      "semi_major_axis": 6378137,
          |      "inverse_flattening": 298.257222101
          |    }
          |  },
          |  "coordinate_system": {
          |    "subtype": "ellipsoidal",
          |    "axis": [
          |      {
          |        "name": "Geodetic latitude",
          |        "abbreviation": "Lat",
          |        "direction": "north",
          |        "unit": "degree"
          |      },
          |      {
          |        "name": "Geodetic longitude",
          |        "abbreviation": "Lon",
          |        "direction": "east",
          |        "unit": "degree"
          |      }
          |    ]
          |  },
          |  "scope": "Horizontal component of 3D system.",
          |  "area": "Puerto Rico - onshore and offshore. United States (USA) onshore and offshore.",
          |  "bbox": {
          |    "south_latitude": 14.92,
          |    "west_longitude": 167.65,
          |    "north_latitude": 74.71,
          |    "east_longitude": -63.88
          |  },
          |  "id": {
          |    "authority": "EPSG",
          |    "code": 6318
          |  }
          |}
          |""".stripMargin

      val projjson1 =
        """
          |{
          |  "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
          |  "type": "GeographicCRS",
          |  "name": "Monte Mario (Rome)",
          |  "datum": {
          |    "type": "GeodeticReferenceFrame",
          |    "name": "Monte Mario (Rome)",
          |    "ellipsoid": {
          |      "name": "International 1924",
          |      "semi_major_axis": 6378388,
          |      "inverse_flattening": 297
          |    },
          |    "prime_meridian": {
          |      "name": "Rome",
          |      "longitude": 12.4523333333333
          |    }
          |  },
          |  "coordinate_system": {
          |    "subtype": "ellipsoidal",
          |    "axis": [
          |      {
          |        "name": "Geodetic latitude",
          |        "abbreviation": "Lat",
          |        "direction": "north",
          |        "unit": "degree"
          |      },
          |      {
          |        "name": "Geodetic longitude",
          |        "abbreviation": "Lon",
          |        "direction": "east",
          |        "unit": "degree"
          |      }
          |    ]
          |  },
          |  "scope": "Geodesy, onshore minerals management.",
          |  "area": "Italy - onshore and offshore; San Marino, Vatican City State.",
          |  "bbox": {
          |    "south_latitude": 34.76,
          |    "west_longitude": 5.93,
          |    "north_latitude": 47.1,
          |    "east_longitude": 18.99
          |  },
          |  "id": {
          |    "authority": "EPSG",
          |    "code": 4806
          |  }
          |}
          |""".stripMargin

      val geoParquetSavePath = geoparquetoutputlocation + "/multi_geoms_with_custom_crs.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", projjson0)
        .option("geoparquet.crs.g1", projjson1)
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        val g0Crs = geo \ "columns" \ "g0" \ "crs"
        val g1Crs = geo \ "columns" \ "g1" \ "crs"
        assert(g0Crs == parseJson(projjson0))
        assert(g1Crs == parseJson(projjson1))
      }

      // Write without fallback CRS for g0
      df.write
        .format("geoparquet")
        .option("geoparquet.crs.g1", projjson1)
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        val g0Crs = geo \ "columns" \ "g0" \ "crs"
        val g1Crs = geo \ "columns" \ "g1" \ "crs"
        assert(g0Crs == org.json4s.JNull)
        assert(g1Crs == parseJson(projjson1))
      }

      // Fallback CRS is omitting CRS
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "")
        .option("geoparquet.crs.g1", projjson1)
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        val g0Crs = geo \ "columns" \ "g0" \ "crs"
        val g1Crs = geo \ "columns" \ "g1" \ "crs"
        assert(g0Crs == org.json4s.JNothing)
        assert(g1Crs == parseJson(projjson1))
      }

      // Write with CRS, explicitly set CRS to null for g1
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", projjson0)
        .option("geoparquet.crs.g1", "null")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        val g0Crs = geo \ "columns" \ "g0" \ "crs"
        val g1Crs = geo \ "columns" \ "g1" \ "crs"
        assert(g0Crs == parseJson(projjson0))
        assert(g1Crs == org.json4s.JNull)
      }

      // Write with CRS, explicitly omit CRS for g1
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", projjson0)
        .option("geoparquet.crs.g1", "")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        val g0Crs = geo \ "columns" \ "g0" \ "crs"
        val g1Crs = geo \ "columns" \ "g1" \ "crs"
        assert(g0Crs == parseJson(projjson0))
        assert(g1Crs == org.json4s.JNothing)
      }
    }

    it("GeoParquet read should set SRID from PROJJSON CRS with EPSG identifier") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation4)
      val projjson =
        """
          |{
          |  "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
          |  "type": "GeographicCRS",
          |  "name": "NAD83(2011)",
          |  "datum": {
          |    "type": "GeodeticReferenceFrame",
          |    "name": "NAD83 (National Spatial Reference System 2011)",
          |    "ellipsoid": {
          |      "name": "GRS 1980",
          |      "semi_major_axis": 6378137,
          |      "inverse_flattening": 298.257222101
          |    }
          |  },
          |  "coordinate_system": {
          |    "subtype": "ellipsoidal",
          |    "axis": [
          |      {
          |        "name": "Geodetic latitude",
          |        "abbreviation": "Lat",
          |        "direction": "north",
          |        "unit": "degree"
          |      },
          |      {
          |        "name": "Geodetic longitude",
          |        "abbreviation": "Lon",
          |        "direction": "east",
          |        "unit": "degree"
          |      }
          |    ]
          |  },
          |  "id": {
          |    "authority": "EPSG",
          |    "code": 6318
          |  }
          |}
          |""".stripMargin
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_srid_epsg.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", projjson)
        .mode("overwrite")
        .save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val geoms = df2.select("geometry").collect().map(_.getAs[Geometry](0))
      assert(geoms.nonEmpty)
      geoms.foreach { geom =>
        assert(geom.getSRID == 6318, s"Expected SRID 6318, got ${geom.getSRID}")
      }
    }

    it("GeoParquet read should set SRID 4326 when CRS is omitted") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation4)
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_srid_omit.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "")
        .mode("overwrite")
        .save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val geoms = df2.select("geometry").collect().map(_.getAs[Geometry](0))
      assert(geoms.nonEmpty)
      geoms.foreach { geom =>
        assert(geom.getSRID == 4326, s"Expected SRID 4326 for omitted CRS, got ${geom.getSRID}")
      }
    }

    it("GeoParquet read should set SRID 0 when CRS is null") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation4)
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_srid_null.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "null")
        .mode("overwrite")
        .save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val geoms = df2.select("geometry").collect().map(_.getAs[Geometry](0))
      assert(geoms.nonEmpty)
      geoms.foreach { geom =>
        assert(geom.getSRID == 0, s"Expected SRID 0 for null CRS, got ${geom.getSRID}")
      }
    }

    it("GeoParquet read should set SRID 0 for CRS without EPSG identifier") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation4)
      // A PROJJSON without the "id" field
      val projjsonNoId =
        """
          |{
          |  "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
          |  "type": "GeographicCRS",
          |  "name": "Unknown CRS",
          |  "datum": {
          |    "type": "GeodeticReferenceFrame",
          |    "name": "Unknown datum",
          |    "ellipsoid": {
          |      "name": "GRS 1980",
          |      "semi_major_axis": 6378137,
          |      "inverse_flattening": 298.257222101
          |    }
          |  },
          |  "coordinate_system": {
          |    "subtype": "ellipsoidal",
          |    "axis": [
          |      {
          |        "name": "Geodetic latitude",
          |        "abbreviation": "Lat",
          |        "direction": "north",
          |        "unit": "degree"
          |      },
          |      {
          |        "name": "Geodetic longitude",
          |        "abbreviation": "Lon",
          |        "direction": "east",
          |        "unit": "degree"
          |      }
          |    ]
          |  }
          |}
          |""".stripMargin
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_srid_no_id.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", projjsonNoId)
        .mode("overwrite")
        .save(geoParquetSavePath)
      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val geoms = df2.select("geometry").collect().map(_.getAs[Geometry](0))
      assert(geoms.nonEmpty)
      geoms.foreach { geom =>
        assert(geom.getSRID == 0, s"Expected SRID 0 for CRS without id, got ${geom.getSRID}")
      }
    }

    it("GeoParquet read should set per-column SRID from different CRSes") {
      val wktReader = new WKTReader()
      val testData = Seq(
        Row(
          1,
          wktReader.read("POINT (1 2)"),
          wktReader.read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("g0", GeometryUDT(), nullable = false),
          StructField("g1", GeometryUDT(), nullable = false)))
      val df = sparkSession.createDataFrame(testData.asJava, schema).repartition(1)

      // g0: EPSG:4326, g1: EPSG:32632
      val projjson4326 =
        """{"type":"GeographicCRS","name":"WGS 84","id":{"authority":"EPSG","code":4326}}"""
      val projjson32632 =
        """{"type":"ProjectedCRS","name":"WGS 84 / UTM zone 32N",""" +
          """"base_crs":{"name":"WGS 84","datum":{"type":"GeodeticReferenceFrame",""" +
          """"name":"World Geodetic System 1984",""" +
          """"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}}},""" +
          """"conversion":{"name":"UTM zone 32N","method":{"name":"Transverse Mercator"},""" +
          """"parameters":[{"name":"Latitude of natural origin","value":0},""" +
          """{"name":"Longitude of natural origin","value":9},""" +
          """{"name":"Scale factor at natural origin","value":0.9996},""" +
          """{"name":"False easting","value":500000},""" +
          """{"name":"False northing","value":0}]},""" +
          """"coordinate_system":{"subtype":"Cartesian","axis":[""" +
          """{"name":"Easting","direction":"east","unit":"metre"},""" +
          """{"name":"Northing","direction":"north","unit":"metre"}]},""" +
          """"id":{"authority":"EPSG","code":32632}}"""

      val geoParquetSavePath =
        geoparquetoutputlocation + "/gp_srid_multi_column.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", projjson4326)
        .option("geoparquet.crs.g1", projjson32632)
        .mode("overwrite")
        .save(geoParquetSavePath)

      val df2 = sparkSession.read.format("geoparquet").load(geoParquetSavePath)
      val rows = df2.collect()
      assert(rows.length == 1)
      val g0 = rows(0).getAs[Geometry](rows(0).fieldIndex("g0"))
      val g1 = rows(0).getAs[Geometry](rows(0).fieldIndex("g1"))
      assert(g0.getSRID == 4326, s"Expected g0 SRID 4326, got ${g0.getSRID}")
      assert(g1.getSRID == 32632, s"Expected g1 SRID 32632, got ${g1.getSRID}")
    }

    it("GeoParquet load should raise exception when loading plain parquet files") {
      val e = intercept[SparkException] {
        sparkSession.read.format("geoparquet").load(resourceFolder + "geoparquet/plain.parquet")
      }
      assert(e.getMessage.contains("does not contain valid geo metadata"))
    }

    it("GeoParquet load with spatial predicates") {
      val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
      val rows =
        df.where(ST_Intersects(ST_Point(35.174722, -6.552465), col("geometry"))).collect()
      assert(rows.length == 1)
      assert(rows(0).getAs[String]("name") == "Tanzania")
    }

    it("Filter push down for nested columns") {
      import sparkSession.implicits._

      // Prepare multiple GeoParquet files with bbox metadata. There should be 10 files in total, each file contains
      // 1000 records.
      val dfIds = (0 until 10000).toDF("id")
      val dfGeom = dfIds
        .withColumn(
          "bbox",
          expr("struct(id as minx, id as miny, id + 1 as maxx, id + 1 as maxy)"))
        .withColumn("geom", expr("ST_PolygonFromEnvelope(id, id, id + 1, id + 1)"))
        .withColumn("part_id", expr("CAST(id / 1000 AS INTEGER)"))
        .coalesce(1)
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_with_bbox.parquet"
      dfGeom.write
        .partitionBy("part_id")
        .format("geoparquet")
        .mode("overwrite")
        .save(geoParquetSavePath)

      val sparkListener = new SparkListener() {
        val recordsRead = new AtomicLong(0)

        def reset(): Unit = recordsRead.set(0)

        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          val recordsRead = taskEnd.taskMetrics.inputMetrics.recordsRead
          this.recordsRead.getAndAdd(recordsRead)
        }
      }

      sparkSession.sparkContext.addSparkListener(sparkListener)
      try {
        val df = sparkSession.read.format("geoparquet").load(geoParquetSavePath)

        // This should trigger filter push down to Parquet and only read one of the files. The number of records read
        // should be less than 1000.
        df.where("bbox.minx > 6000 and bbox.minx < 6600").count()
        assert(sparkListener.recordsRead.get() <= 1000)

        // Reading these files using spatial filter. This should only read two of the files.
        sparkListener.reset()
        df.where(ST_Intersects(ST_PolygonFromEnvelope(7010, 7010, 8100, 8100), col("geom")))
          .count()
        assert(sparkListener.recordsRead.get() <= 2000)
      } finally {
        sparkSession.sparkContext.removeSparkListener(sparkListener)
      }
    }

    it("Ready legacy parquet files written by Apache Sedona <= 1.3.1-incubating") {
      val df = sparkSession.read
        .format("geoparquet")
        .option("legacyMode", "true")
        .load(legacyparquetdatalocation)
      val rows = df.collect()
      assert(rows.nonEmpty)
      rows.foreach { row =>
        assert(row.getAs[AnyRef]("geom").isInstanceOf[Geometry])
        assert(row.getAs[AnyRef]("struct_geom").isInstanceOf[Row])
        val structGeom = row.getAs[Row]("struct_geom")
        assert(structGeom.getAs[AnyRef]("g0").isInstanceOf[Geometry])
        assert(structGeom.getAs[AnyRef]("g1").isInstanceOf[Geometry])
      }
    }

    it("GeoParquet supports writing covering metadata") {
      val df = sparkSession
        .range(0, 100)
        .toDF("id")
        .withColumn("id", expr("CAST(id AS DOUBLE)"))
        .withColumn("geometry", expr("ST_Point(id, id + 1)"))
        .withColumn(
          "test_cov",
          expr("struct(id AS xmin, id + 1 AS ymin, id AS xmax, id + 1 AS ymax)"))
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_with_covering_metadata.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.covering", "test_cov")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val coveringJsValue = geo \ "columns" \ "geometry" \ "covering"
        val covering = coveringJsValue.extract[Covering]
        assert(covering.bbox.xmin == Seq("test_cov", "xmin"))
        assert(covering.bbox.ymin == Seq("test_cov", "ymin"))
        assert(covering.bbox.xmax == Seq("test_cov", "xmax"))
        assert(covering.bbox.ymax == Seq("test_cov", "ymax"))
      }

      df.write
        .format("geoparquet")
        .option("geoparquet.covering.geometry", "test_cov")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val coveringJsValue = geo \ "columns" \ "geometry" \ "covering"
        val covering = coveringJsValue.extract[Covering]
        assert(covering.bbox.xmin == Seq("test_cov", "xmin"))
        assert(covering.bbox.ymin == Seq("test_cov", "ymin"))
        assert(covering.bbox.xmax == Seq("test_cov", "xmax"))
        assert(covering.bbox.ymax == Seq("test_cov", "ymax"))
      }
    }

    it("GeoParquet supports writing covering metadata for multiple columns") {
      val df = sparkSession
        .range(0, 100)
        .toDF("id")
        .withColumn("id", expr("CAST(id AS DOUBLE)"))
        .withColumn("geom1", expr("ST_Point(id, id + 1)"))
        .withColumn(
          "test_cov1",
          expr("struct(id AS xmin, id + 1 AS ymin, id AS xmax, id + 1 AS ymax)"))
        .withColumn("geom2", expr("ST_Point(10 * id, 10 * id + 1)"))
        .withColumn(
          "test_cov2",
          expr(
            "struct(10 * id AS xmin, 10 * id + 1 AS ymin, 10 * id AS xmax, 10 * id + 1 AS ymax)"))
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_with_covering_metadata.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.covering.geom1", "test_cov1")
        .option("geoparquet.covering.geom2", "test_cov2")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        Seq(("geom1", "test_cov1"), ("geom2", "test_cov2")).foreach {
          case (geomName, coveringName) =>
            val coveringJsValue = geo \ "columns" \ geomName \ "covering"
            val covering = coveringJsValue.extract[Covering]
            assert(covering.bbox.xmin == Seq(coveringName, "xmin"))
            assert(covering.bbox.ymin == Seq(coveringName, "ymin"))
            assert(covering.bbox.xmax == Seq(coveringName, "xmax"))
            assert(covering.bbox.ymax == Seq(coveringName, "ymax"))
        }
      }

      df.write
        .format("geoparquet")
        .option("geoparquet.covering.geom2", "test_cov2")
        .mode("overwrite")
        .save(geoParquetSavePath)
      validateGeoParquetMetadata(geoParquetSavePath) { geo =>
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        assert(geo \ "columns" \ "geom1" \ "covering" == org.json4s.JNothing)
        val coveringJsValue = geo \ "columns" \ "geom2" \ "covering"
        val covering = coveringJsValue.extract[Covering]
        assert(covering.bbox.xmin == Seq("test_cov2", "xmin"))
        assert(covering.bbox.ymin == Seq("test_cov2", "ymin"))
        assert(covering.bbox.xmax == Seq("test_cov2", "xmax"))
        assert(covering.bbox.ymax == Seq("test_cov2", "ymax"))
      }
    }
  }

  describe("Spark types tests") {
    it("should support timestamp_ntz") {
      // Write geoparquet files with a TimestampNTZ column
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("timestamp_ntz", TimestampNTZType, nullable = false)))
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val data = Seq(
        Row(1, LocalDateTime.parse("2024-10-04 12:34:56", formatter)),
        Row(2, LocalDateTime.parse("2024-10-04 15:30:00", formatter)))
      val df = sparkSession
        .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
        .withColumn("geom", expr("ST_Point(id, id)"))
      df.write.format("geoparquet").mode("overwrite").save(geoparquetoutputlocation)

      // Read it back
      val df2 =
        sparkSession.read.format("geoparquet").load(geoparquetoutputlocation).sort(col("id"))
      assert(df2.schema.fields(1).dataType == TimestampNTZType)
      val data1 = df.sort(col("id")).collect()
      val data2 = df2.collect()
      assert(data1 sameElements data2)
    }
  }

  describe("loading one file geoparquet and filtering") {
    it("should not fail when bbox is not available in geoparquet metadata") {
      val numberOfRecords = sparkSession.read
        .format("geoparquet")
        .load(overtureBBOX)
        .where("ST_Intersects(geometry, ST_PolygonFromEnvelope(0, 0, 1, 1))")
        .count()

      assert(numberOfRecords == 9)
    }
  }

  describe("Fix SPARK-48942 reading parquet with array of structs of UDTs workaround") {
    it("should handle array of struct with geometry UDT") {
      // This test reproduces the issue described in SPARK-48942
      // https://issues.apache.org/jira/browse/SPARK-48942
      // where reading back nested geometry from Parquet with PySpark 3.5 fails
      val testPath = geoparquetoutputlocation + "/spark_48942_test.parquet"

      // Create DataFrame with array of struct containing geometry
      val df = sparkSession.sql("""
        SELECT ARRAY(STRUCT(ST_POINT(1.0, 1.1) AS geometry)) AS nested_geom_array
      """)

      // Write to Parquet
      df.write.mode("overwrite").format("parquet").save(testPath)

      // The fix allows vectorized reading to handle UDT compatibility properly
      val readDf = sparkSession.read.format("parquet").load(testPath)

      // Verify the geometry data is correct and accessible
      val result = readDf.collect()
      assert(result.length == 1)
      val nestedArray = result(0).getSeq[Any](0)
      assert(nestedArray.length == 1)

      // Verify we can perform geometric operations on the read data
      // This tests that the nested geometry is functionally correct after the SPARK-48942 fix
      readDf.createOrReplaceTempView("nested_geom_test")
      val extractedGeom = sparkSession.sql("""
        SELECT nested_geom_array[0].geometry as extracted_geometry
        FROM nested_geom_test
      """)

      val extractedResult = extractedGeom.collect()
      assert(extractedResult.length == 1)
      assert(extractedResult(0).get(0) != null, "Extracted geometry should not be null")

      // Test that we can use the extracted geometry in spatial functions
      extractedGeom.createOrReplaceTempView("extracted_test")
      val spatialTest = sparkSession.sql("""
        SELECT ST_X(extracted_geometry) as x, ST_Y(extracted_geometry) as y
        FROM extracted_test
      """)

      val spatialResult = spatialTest.collect()
      assert(spatialResult.length == 1)
      assert(spatialResult(0).getDouble(0) == 1.0, "X coordinate should be 1.0")
      assert(spatialResult(0).getDouble(1) == 1.1, "Y coordinate should be 1.1")

      // Test that we can perform more complex spatial operations
      val spatialOperations = sparkSession.sql("""
        SELECT
          ST_AsText(extracted_geometry) as wkt,
          ST_GeometryType(extracted_geometry) as geom_type
        FROM extracted_test
      """)

      val operationResult = spatialOperations.collect()
      assert(operationResult.length == 1)
      assert(
        operationResult(0).getString(0) == "POINT (1 1.1)",
        "WKT should match original point")
      assert(operationResult(0).getString(1) == "ST_Point", "Geometry type should be ST_Point")
    }

    it("should reject nested geometry when using GeoParquet format") {
      // GeoParquet specification requires "Geometry columns MUST be at the root of the schema"
      // Therefore, nested geometry should be rejected
      val testPath = geoparquetoutputlocation + "/spark_48942_geoparquet_test.geoparquet"

      val df = sparkSession.sql("""
        SELECT ARRAY(STRUCT(ST_POINT(1.0, 1.1) AS geometry)) AS nested_geom_array
      """)

      // Writing nested geometry to GeoParquet should fail according to the specification
      assertThrows[SparkException] {
        df.write.mode("overwrite").format("geoparquet").save(testPath)
      }
    }

    it("should handle deeply nested arrays with geometry UDT") {
      // Test deeply nested arrays: array of array of struct with geometry
      val testPath = geoparquetoutputlocation + "/spark_48942_deep_nested_test.parquet"

      val df = sparkSession.sql("""
        SELECT ARRAY(
          ARRAY(STRUCT(ST_POINT(1.0, 1.1) AS geometry)),
          ARRAY(STRUCT(ST_POINT(2.0, 2.1) AS geometry))
        ) AS deeply_nested_geom_array
      """)

      // Write to Parquet
      df.write.mode("overwrite").format("parquet").save(testPath)

      // Read back and verify
      val readDf = sparkSession.read.format("parquet").load(testPath)

      val result = readDf.collect()
      assert(result.length == 1)
    }
  }

  describe("GeoParquetMetaData.extractSridFromCrs") {
    it("should return 4326 when CRS is omitted (None)") {
      assert(GeoParquetMetaData.extractSridFromCrs(None) == 4326)
    }

    it("should return 0 when CRS is null") {
      assert(GeoParquetMetaData.extractSridFromCrs(Some(org.json4s.JNull)) == 0)
    }

    it("should extract EPSG code from PROJJSON") {
      val projjson =
        parseJson("""{"type":"GeographicCRS","id":{"authority":"EPSG","code":4326}}""")
      assert(GeoParquetMetaData.extractSridFromCrs(Some(projjson)) == 4326)
    }

    it("should extract non-4326 EPSG code") {
      // Use a complete ProjectedCRS PROJJSON so proj4sedona can parse it
      val projjson = parseJson(
        """{"type":"ProjectedCRS","name":"WGS 84 / UTM zone 32N",""" +
          """"base_crs":{"name":"WGS 84","datum":{"type":"GeodeticReferenceFrame",""" +
          """"name":"World Geodetic System 1984",""" +
          """"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}}},""" +
          """"conversion":{"name":"UTM zone 32N","method":{"name":"Transverse Mercator"},""" +
          """"parameters":[{"name":"Latitude of natural origin","value":0},""" +
          """{"name":"Longitude of natural origin","value":9},""" +
          """{"name":"Scale factor at natural origin","value":0.9996},""" +
          """{"name":"False easting","value":500000},""" +
          """{"name":"False northing","value":0}]},""" +
          """"coordinate_system":{"subtype":"Cartesian","axis":[""" +
          """{"name":"Easting","direction":"east","unit":"metre"},""" +
          """{"name":"Northing","direction":"north","unit":"metre"}]},""" +
          """"id":{"authority":"EPSG","code":32632}}""")
      assert(GeoParquetMetaData.extractSridFromCrs(Some(projjson)) == 32632)
    }

    it("should return 4326 for OGC:CRS84") {
      val projjson =
        parseJson("""{"type":"GeographicCRS","id":{"authority":"OGC","code":"CRS84"}}""")
      assert(GeoParquetMetaData.extractSridFromCrs(Some(projjson)) == 4326)
    }

    it("should return 0 for CRS without id field") {
      val projjson = parseJson("""{"type":"GeographicCRS","name":"Unknown"}""")
      assert(GeoParquetMetaData.extractSridFromCrs(Some(projjson)) == 0)
    }

    it("should return 0 for CRS with non-EPSG authority") {
      val projjson =
        parseJson("""{"type":"GeographicCRS","id":{"authority":"IAU","code":49900}}""")
      assert(GeoParquetMetaData.extractSridFromCrs(Some(projjson)) == 0)
    }
  }

  def validateGeoParquetMetadata(path: String)(body: org.json4s.JValue => Unit): Unit = {
    val parquetFiles = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
    parquetFiles.foreach { filePath =>
      val metadata = ParquetFileReader
        .open(HadoopInputFile.fromPath(new Path(filePath.getPath), new Configuration()))
        .getFooter
        .getFileMetaData
        .getKeyValueMetaData
      assert(metadata.containsKey("geo"))
      val geo = parseJson(metadata.get("geo"))
      body(geo)
    }
  }
}
