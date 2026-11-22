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

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, Type, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.geoparquet.ParquetGeospatialTypes
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, IntegerType, StructField, StructType}
import org.locationtech.jts.io.{ByteOrderValues, WKBWriter, WKTReader}

import scala.collection.JavaConverters._

/**
 * GeoParquet 2.0 stores a geography in the GEOGRAPHY logical type of the Parquet format. The
 * files of these tests come from the Parquet library itself, and from Spark 4.2's own Parquet
 * writer for the fixtures under `geoparquet/native`.
 */
class GeoParquetGeographySuite extends TestBaseScala {
  private val nativeFolder = resourceFolder + "geoparquet/native/"

  private def messages(error: Throwable): String = Iterator
    .iterate(error)(_.getCause)
    .takeWhile(_ != null)
    .flatMap(cause => Option(cause.getMessage))
    .mkString("\n")

  private def withDirectory(run: File => Unit): Unit = {
    val directory = Files.createTempDirectory("geoparquet-geography").toFile
    try run(directory)
    finally FileUtils.deleteDirectory(directory)
  }

  private def wkb(wkt: String): Array[Byte] = {
    val geometry = new WKTReader().read(wkt)
    val dimensions = if (geometry.getCoordinate.getZ.isNaN) 2 else 3
    new WKBWriter(dimensions, ByteOrderValues.LITTLE_ENDIAN).write(geometry)
  }

  private def geography(name: String, crs: Option[String], edges: String): Type =
    Types.optional(BINARY).as(ParquetGeospatialTypes.geography(crs, edges)).named(name)

  private def write(file: File, schema: MessageType, keyValues: Map[String, String] = Map.empty)(
      rows: (Int => Group)*): Unit = {
    val writer = ExampleParquetWriter
      .builder(new Path(file.toURI))
      .withType(schema)
      .withExtraMetaData(keyValues.asJava)
      .build()
    try rows.zipWithIndex.foreach { case (row, id) => writer.write(row(id)) }
    finally writer.close()
  }

  /**
   * Writes a file with an `id` column and a GEOGRAPHY column `geography` that holds the
   * geographies of `wkts`, where null is a row without a value.
   */
  private def writeGeographies(
      file: File,
      crs: Option[String] = None,
      edges: String = "spherical",
      keyValues: Map[String, String] = Map.empty)(wkts: String*): Unit = {
    val schema = Types
      .buildMessage()
      .required(INT32)
      .named("id")
      .addField(geography("geography", crs, edges))
      .named("spark_schema")
    val rows = wkts.map { wkt => (id: Int) =>
      val group: Group = new SimpleGroup(schema)
      group.add("id", id)
      if (wkt != null) group.add("geography", Binary.fromConstantByteArray(wkb(wkt)))
      group
    }
    write(file, schema, keyValues)(rows: _*)
  }

  private def geometry(name: String, crs: Option[String] = None): Type =
    Types.optional(BINARY).as(ParquetGeospatialTypes.geometry(crs)).named(name)

  /**
   * Writes a file of one row: `id`, and each of `columns` with the geometry or geography of its
   * WKT.
   */
  private def writeRow(file: File, id: Int, columns: (Type, String)*): Unit = {
    val fields = Types.required(INT32).named("id") +: columns.map(_._1)
    val schema = new MessageType("spark_schema", fields.asJava)
    write(file, schema) { _ =>
      val group: Group = new SimpleGroup(schema)
      group.add("id", id)
      columns.foreach { case (field, wkt) =>
        group.add(field.getName, Binary.fromConstantByteArray(wkb(wkt)))
      }
      group
    }
  }

  /** Writes a file of one row, whose column `items` is a list of the one value of `element`. */
  private def writeList(file: File, element: Type, wkt: String): Unit = {
    val items = Types
      .optionalGroup()
      .as(LogicalTypeAnnotation.listType())
      .addField(Types.repeatedGroup().addField(element).named("list"))
      .named("items")
    val schema = Types.buildMessage().required(INT32).named("id").addField(items).named("data")
    write(file, schema) { id =>
      val group: Group = new SimpleGroup(schema)
      group.add("id", id)
      group
        .addGroup("items")
        .addGroup("list")
        .add(element.getName, Binary.fromConstantByteArray(wkb(wkt)))
      group
    }
  }

  private def read(path: String): DataFrame = sparkSession.read.format("geoparquet").load(path)

  /** The geographies of `df` as EWKT in the order of `id`, null for a row without a value. */
  private def ewkt(df: DataFrame, column: String = "geography"): Seq[String] =
    df.orderBy("id").selectExpr(s"ST_AsEWKT($column)").collect().map(_.getString(0)).toSeq

  private def assertGeography(df: DataFrame, srid: Int, column: String = "geography"): Unit = {
    val dataType = df.schema(column).dataType
    assert(SpatialTypeSupport.isGeography(dataType))
    assert(SpatialTypeSupport.isNativeGeography(dataType) == SpatialTypeSupport.usesNativeTypes)
    if (SpatialTypeSupport.usesNativeTypes) {
      assert(dataType == SpatialTypeSupport.geographyType(srid))
    }
  }

  private val projjson4269 =
    """{"type":"GeographicCRS","name":"NAD83","id":{"authority":"EPSG","code":4269}}"""

  describe("Reading Parquet GEOGRAPHY columns") {
    if (ParquetGeospatialTypes.supported) {
      Seq("spark-4.2-geography.parquet", "spark-4.2-geography-geo-metadata.parquet").foreach {
        file =>
          it(s"reads $file of Spark's own Parquet writer as geographies") {
            val df = read(nativeFolder + file)
            assertGeography(df, 4326)
            assert(
              df.selectExpr("ST_AsEWKT(geography)").collect().map(_.getString(0)).toSeq ==
                Seq.fill(3)("SRID=4326; POINT (1 2)"))
          }
      }

      it("reads points, lines, polygons and rows without a value") {
        withDirectory { directory =>
          writeGeographies(new File(directory, "part-0.parquet"))(
            "POINT (1 2)",
            null,
            "LINESTRING (0 0, 10 10)",
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "POINT Z (1 2 3)")
          val df = read(directory.getPath)
          assertGeography(df, 4326)
          assert(
            ewkt(df) == Seq(
              "SRID=4326; POINT (1 2)",
              null,
              "SRID=4326; LINESTRING (0 0, 10 10)",
              "SRID=4326; POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
              "SRID=4326; POINT (1 2)"))
          // The text of a geography has no Z coordinate. Its WKB keeps the one of the file.
          val dataType = df.schema("geography").dataType
          val value = df
            .where("id = 4")
            .select("geography")
            .queryExecution
            .executedPlan
            .executeCollect()
            .head
            .get(0, dataType)
          val stored = SpatialTypeSupport.deserializeGeography(value, dataType)
          assert(
            WKBWriter.toHex(stored.asInstanceOf[WKBGeography].getWKBBytes) ==
              "01E9030000000000000000F03F00000000000000400000000000000840")
        }
      }

      it("computes on the geographies that it reads") {
        withDirectory { directory =>
          writeGeographies(new File(directory, "part-0.parquet"))("POINT (0 0)", "POINT (0 1)")
          val distance = read(directory.getPath)
            .where("id = 1")
            .selectExpr("ST_Distance(geography, ST_GeogFromWKT('POINT (0 0)', 4326))")
            .collect()
            .head
            .getDouble(0)
          // One degree of latitude on a sphere is about 111 km
          assert(math.abs(distance - 111195) < 200)
        }
      }

      // The forms that the Parquet format allows for the CRS of a column
      Seq(
        ("no CRS", None, 4326),
        ("OGC:CRS84", Some("OGC:CRS84"), 4326),
        ("EPSG:4269", Some("EPSG:4269"), 4269),
        ("srid:4269", Some("srid:4269"), 4269),
        ("inline PROJJSON", Some(projjson4269), 4269)).foreach { case (name, crs, srid) =>
        it(s"takes SRID $srid from a column with $name") {
          withDirectory { directory =>
            writeGeographies(new File(directory, "part-0.parquet"), crs)("POINT (1 2)")
            val df = read(directory.getPath)
            assertGeography(df, srid)
            assert(ewkt(df) == Seq(s"SRID=$srid; POINT (1 2)"))
          }
        }
      }

      it("takes the SRID from PROJJSON that the CRS of the column names by its key") {
        withDirectory { directory =>
          writeGeographies(
            new File(directory, "part-0.parquet"),
            Some("projjson:nad83"),
            keyValues = Map("nad83" -> projjson4269))("POINT (1 2)")
          val df = read(directory.getPath)
          assertGeography(df, 4269)
          assert(ewkt(df) == Seq("SRID=4269; POINT (1 2)"))
        }
      }

      // Spark's native geography type takes the SRID of a geographic CRS only
      Seq("srid:0" -> 0, "EPSG:3857" -> 3857).foreach { case (crs, srid) =>
        it(s"reads a column with CRS $crs, which the native geography type rejects") {
          withDirectory { directory =>
            writeGeographies(new File(directory, "part-0.parquet"), Some(crs))("POINT (1 2)")
            val expected = Seq(if (srid == 0) "POINT (1 2)" else s"SRID=$srid; POINT (1 2)")
            if (SpatialTypeSupport.usesNativeTypes) {
              val error = intercept[Exception](read(directory.getPath).collect())
              assert(messages(error).contains("'geography'"))
              assert(messages(error).contains("native geography type cannot represent"))
              // A schema with Sedona's own geography type still reads the column
              val schema = StructType(
                Seq(StructField("id", IntegerType), StructField("geography", GeographyUDT())))
              val df =
                sparkSession.read.schema(schema).format("geoparquet").load(directory.getPath)
              assert(ewkt(df) == expected)
            } else {
              val df = read(directory.getPath)
              assertGeography(df, srid)
              assert(ewkt(df) == expected)
            }
          }
        }
      }

      Seq("vincenty", "thomas", "andoyer", "karney").foreach { edges =>
        it(s"rejects a column whose edges follow the $edges algorithm") {
          withDirectory { directory =>
            writeGeographies(new File(directory, "part-0.parquet"), edges = edges)("POINT (1 2)")
            val error = intercept[Exception](read(directory.getPath).collect())
            assert(messages(error).contains("'geography'"))
            assert(messages(error).contains(edges))
            assert(messages(error).contains("spherical"))
            // The WKB of the column is still there for a reader that asks for it
            val schema = StructType(
              Seq(StructField("id", IntegerType), StructField("geography", BinaryType)))
            val stored = sparkSession.read
              .schema(schema)
              .format("geoparquet")
              .load(directory.getPath)
              .selectExpr("hex(geography)")
              .collect()
              .map(_.getString(0))
            assert(stored.toSeq == Seq("0101000000000000000000F03F0000000000000040"))
            // A schema cannot make such a column a geography
            val asGeography = StructType(
              Seq(
                StructField("id", IntegerType),
                StructField("geography", SpatialTypeSupport.geographyType(4326))))
            val second = intercept[Exception] {
              sparkSession.read
                .schema(asGeography)
                .format("geoparquet")
                .load(directory.getPath)
                .collect()
            }
            assert(messages(second).contains(edges))
          }
        }
      }

      it("reads a column with the geography type that a schema gives it") {
        withDirectory { directory =>
          writeGeographies(new File(directory, "part-0.parquet"))("POINT (1 2)")
          def withType(dataType: DataType): DataFrame = sparkSession.read
            .schema(
              StructType(Seq(StructField("id", IntegerType), StructField("geography", dataType))))
            .format("geoparquet")
            .load(directory.getPath)
          assert(
            ewkt(withType(SpatialTypeSupport.geographyType(4326))) ==
              Seq("SRID=4326; POINT (1 2)"))
          assert(ewkt(withType(GeographyUDT())) == Seq("SRID=4326; POINT (1 2)"))
          if (SpatialTypeSupport.usesNativeTypes) {
            // The type of a native geography column names the SRID of all its values
            val error =
              intercept[Exception](withType(SpatialTypeSupport.geographyType(4269)).collect())
            assert(messages(error).contains("requires SRID 4269 but contains SRID 4326"))
          }
        }
      }

      it("reads a geography column next to a geometry column") {
        withDirectory { directory =>
          val schema = Types
            .buildMessage()
            .required(INT32)
            .named("id")
            .addField(
              Types
                .optional(BINARY)
                .as(ParquetGeospatialTypes.geometry(Some("EPSG:3857")))
                .named("geometry"))
            .addField(geography("geography", None, "spherical"))
            .named("spark_schema")
          write(new File(directory, "part-0.parquet"), schema) { id =>
            val group: Group = new SimpleGroup(schema)
            group.add("id", id)
            group.add("geometry", Binary.fromConstantByteArray(wkb("POINT (3 4)")))
            group.add("geography", Binary.fromConstantByteArray(wkb("POINT (1 2)")))
            group
          }
          val df = read(directory.getPath)
          assert(SpatialTypeSupport.isGeometry(df.schema("geometry").dataType))
          assertGeography(df, 4326)
          val row = df.selectExpr("ST_AsEWKT(geometry)", "ST_AsEWKT(geography)").collect().head
          assert(row.getString(0) == "SRID=3857;POINT (3 4)")
          assert(row.getString(1) == "SRID=4326; POINT (1 2)")
        }
      }

      it("reads a geography column that is nested in another column") {
        withDirectory { directory =>
          val holder: GroupType = Types
            .optionalGroup()
            .addField(geography("geography", None, "spherical"))
            .named("holder")
          val schema =
            Types
              .buildMessage()
              .required(INT32)
              .named("id")
              .addField(holder)
              .named("spark_schema")
          write(new File(directory, "part-0.parquet"), schema) { id =>
            val group: Group = new SimpleGroup(schema)
            group.add("id", id)
            group
              .addGroup("holder")
              .add("geography", Binary.fromConstantByteArray(wkb("POINT (1 2)")))
            group
          }
          val df = read(directory.getPath).selectExpr("id", "holder.geography AS geography")
          assertGeography(df, 4326)
          assert(ewkt(df) == Seq("SRID=4326; POINT (1 2)"))
        }
      }

      it("reads the files of a dataset that agree on the CRS of the column") {
        withDirectory { directory =>
          writeGeographies(new File(directory, "part-0.parquet"))("POINT (1 2)")
          writeGeographies(new File(directory, "part-1.parquet"), Some("OGC:CRS84"))(null)
          val df = read(directory.getPath)
          assertGeography(df, 4326)
          assert(df.where("geography IS NOT NULL").count() == 1)
          assert(df.count() == 2)
        }
      }

      for (mergeSchema <- Seq("false", "true")) {
        it(
          s"reads no value from a file without the geography column (mergeSchema=$mergeSchema)") {
          withDirectory { directory =>
            writeRow(
              new File(directory, "part-0.parquet"),
              0,
              geometry("anchor") -> "POINT (3 4)",
              geography("geography", Some("EPSG:4269"), "spherical") -> "POINT (1 2)")
            writeRow(
              new File(directory, "part-1.parquet"),
              1,
              geometry("anchor") -> "POINT (3 4)")
            val df = sparkSession.read
              .format("geoparquet")
              .option("mergeSchema", mergeSchema)
              .load(directory.getPath)
            assertGeography(df, 4269)
            assert(ewkt(df) == Seq("SRID=4269; POINT (1 2)", null))
          }
        }
      }

      it("reads no value from a file without the geography column of a nested column") {
        withDirectory { directory =>
          def holder(field: Type): GroupType =
            Types.optionalGroup().addField(field).named("holder")
          def file(name: String, holder: GroupType)(fill: Group => Unit): Unit = {
            val schema = Types
              .buildMessage()
              .required(INT32)
              .named("id")
              .addField(geometry("anchor"))
              .addField(holder)
              .named("data")
            write(new File(directory, name), schema) { _ =>
              val group: Group = new SimpleGroup(schema)
              group.add("anchor", Binary.fromConstantByteArray(wkb("POINT (3 4)")))
              fill(group)
              group
            }
          }
          file("part-0.parquet", holder(geography("geography", None, "spherical"))) { group =>
            group.add("id", 0)
            group
              .addGroup("holder")
              .add("geography", Binary.fromConstantByteArray(wkb("POINT (1 2)")))
          }
          file("part-1.parquet", holder(Types.optional(INT32).named("number"))) { group =>
            group.add("id", 1)
            group.addGroup("holder").add("number", 7)
          }
          val df = read(directory.getPath).selectExpr("id", "holder.geography AS geography")
          assertGeography(df, 4326)
          assert(ewkt(df) == Seq("SRID=4326; POINT (1 2)", null))
        }
      }

      // A reader of geometries computes on a plane, so it must not take the values of a
      // GEOGRAPHY column, and the other way around
      Seq("GEOMETRY" -> "GEOGRAPHY", "GEOGRAPHY" -> "GEOMETRY").foreach { case (first, second) =>
        it(
          s"rejects a column that is a $first column in one file and a $second column in the next") {
          withDirectory { directory =>
            def column(kind: String): Type =
              if (kind == "GEOMETRY") geometry("shape") else geography("shape", None, "spherical")
            writeRow(new File(directory, "part-0.parquet"), 0, column(first) -> "POINT (1 2)")
            writeRow(new File(directory, "part-1.parquet"), 1, column(second) -> "POINT (1 2)")
            val error = intercept[Exception](read(directory.getPath).collect())
            val message = messages(error).toLowerCase(java.util.Locale.ROOT)
            assert(message.contains("'shape'"))
            assert(message.contains("geometry"))
            assert(message.contains("geography"))
          }
        }
      }

      it("rejects a GEOGRAPHY column for a schema that makes it a geometry") {
        withDirectory { directory =>
          writeGeographies(new File(directory, "part-0.parquet"))("POINT (1 2)")
          val schema = StructType(
            Seq(
              StructField("id", IntegerType),
              StructField("geography", SpatialTypeSupport.geometryType(4326))))
          val error = intercept[Exception] {
            sparkSession.read
              .schema(schema)
              .format("geoparquet")
              .load(directory.getPath)
              .collect()
          }
          assert(messages(error).contains(
            "Column 'geography' is a Parquet GEOGRAPHY column and cannot be read as a geometry"))
        }
      }

      it("reads files that state different SRIDs for a geography column") {
        withDirectory { directory =>
          writeRow(
            new File(directory, "part-0.parquet"),
            0,
            geography("geography", None, "spherical") -> "POINT (1 2)")
          writeRow(
            new File(directory, "part-1.parquet"),
            1,
            geography("geography", Some("EPSG:4269"), "spherical") -> "POINT (3 4)")
          val both = Seq("SRID=4326; POINT (1 2)", "SRID=4269; POINT (3 4)")
          if (SpatialTypeSupport.usesNativeTypes) {
            // The type of a native geography column names one SRID. The files are compared
            // before the scan, which would otherwise leave out a file as a corrupt one.
            for (ignoreCorruptFiles <- Seq("false", "true")) {
              val error = intercept[Exception] {
                sparkSession.read
                  .format("geoparquet")
                  .option("ignoreCorruptFiles", ignoreCorruptFiles)
                  .load(directory.getPath)
              }
              assert(messages(error).contains("'geography'"))
              assert(messages(error).contains("4326"))
              assert(messages(error).contains("4269"))
            }
          } else {
            assert(ewkt(read(directory.getPath)) == both)
          }
          // The type of mixed SRIDs takes the SRID of each file
          val schema = StructType(
            Seq(
              StructField("id", IntegerType),
              StructField("geography", SpatialTypeSupport.geographyType)))
          val df = sparkSession.read.schema(schema).format("geoparquet").load(directory.getPath)
          assert(ewkt(df) == both)
        }
      }

      it("reads a list of geographies") {
        withDirectory { directory =>
          writeList(
            new File(directory, "part-0.parquet"),
            geography("element", None, "spherical"),
            "POINT (1 2)")
          val df = read(directory.getPath).selectExpr("id", "items[0] AS geography")
          assertGeography(df, 4326)
          assert(ewkt(df) == Seq("SRID=4326; POINT (1 2)"))
        }
      }

      it("reads the WKB of a list of geographies whose edges do not follow a sphere") {
        withDirectory { directory =>
          writeList(
            new File(directory, "part-0.parquet"),
            geography("element", None, "karney"),
            "POINT (1 2)")
          val schema = StructType(Seq(StructField("items", ArrayType(BinaryType))))
          val stored = sparkSession.read
            .schema(schema)
            .format("geoparquet")
            .load(directory.getPath)
            .selectExpr("hex(items[0])")
            .collect()
            .map(_.getString(0))
          assert(stored.toSeq == Seq("0101000000000000000000F03F0000000000000040"))
        }
      }

      it("reads a list of geographies of unknown CRS with Sedona's own geography type") {
        withDirectory { directory =>
          writeList(
            new File(directory, "part-0.parquet"),
            geography("element", Some("srid:0"), "spherical"),
            "POINT (1 2)")
          val schema = StructType(
            Seq(StructField("id", IntegerType), StructField("items", ArrayType(GeographyUDT()))))
          val df = sparkSession.read
            .schema(schema)
            .format("geoparquet")
            .load(directory.getPath)
            .selectExpr("id", "items[0] AS geography")
          assert(ewkt(df) == Seq("POINT (1 2)"))
        }
      }

      it("applies a geography predicate to the rows of a file that states a bounding box") {
        withDirectory { directory =>
          // The bounding box of the GeoParquet metadata is compared with the planar bounding
          // box of the geometry of a query. That must not take a file from a query on
          // geographies, whose edges leave the planar bounding box of their end points.
          val geo =
            """{"version":"2.0.0","primary_column":"geography","columns":{"geography":
              |{"encoding":"WKB","geometry_types":["Point"],"edges":"spherical",
              |"bbox":[45.0,68.2,45.0,68.2]}}}""".stripMargin.replace("\n", "")
          writeGeographies(new File(directory, "part-0.parquet"), keyValues = Map("geo" -> geo))(
            "POINT (45 68.2)")
          val df = read(directory.getPath)
          assertGeography(df, 4326)
          def count(wkt: String): Long =
            df.where(s"ST_Intersects(geography, ST_GeogFromWKT('$wkt', 4326))").count()
          assert(count("POLYGON ((44 68, 46 68, 46 69, 44 69, 44 68))") == 1)
          assert(count("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))") == 0)
          // The edges of this polygon between longitude 0 and 90 are arcs of great circles.
          // They reach latitude 67.8 and 68.6 at longitude 45, far north of their end points.
          assert(count("POLYGON ((0 60, 90 60, 90 61, 0 61, 0 60))") == 1)
        }
      }
    }
  }

  describe("Writing geography columns") {
    // Sedona's own geography type is written as it is serialized, and the schema that Spark
    // leaves in the file names the type.
    if (SpatialTypeSupport.usesNativeTypes) {
      it("rejects a column of Spark's native geography type") {
        withDirectory { directory =>
          val error = intercept[Exception] {
            sparkSession
              .sql("""SELECT ST_Point(1, 2) AS geometry,
                     |ST_GeogFromWKT('POINT (1 2)', 4326) AS geography""".stripMargin)
              .write
              .format("geoparquet")
              .save(new File(directory, "data").getPath)
          }
          assert(messages(error).contains("'geography'"))
          assert(messages(error).contains("native geography type"))
        }
      }
    } else {
      it("reads back a geography column that a GeoParquet 1 write stored") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          sparkSession
            .sql("""SELECT 1 AS id, ST_Point(1, 2) AS geometry,
                   |ST_GeogFromWKT('LINESTRING (0 0, 10 10)', 4326) AS geography""".stripMargin)
            .write
            .format("geoparquet")
            .option("geoparquet.version", "1.1.0")
            .save(path)
          val df = read(path)
          assert(df.schema("geography").dataType == GeographyUDT())
          assert(ewkt(df) == Seq("SRID=4326; LINESTRING (0 0, 10 10)"))
        }
      }
    }
  }
}
