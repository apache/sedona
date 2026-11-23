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
import java.nio.ByteBuffer
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.sedona.sql.utils.GeographySerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, Type, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.geoparquet.{ParquetGeospatialColumn, ParquetGeospatialTypes}
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, IntegerType, StructField, StructType}
import org.json4s.{JArray, JInt, JNothing, JString, JValue}
import org.json4s.jackson.JsonMethods.parse
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
  private def writeRow(file: File, id: Int, columns: (Type, String)*): Unit =
    writeValues(file, id)(columns.map { case (field, wkt) => field -> wkb(wkt) }: _*)

  /**
   * Writes a file of one row: `id`, and each of `columns` with its bytes. `sparkSchema` is the
   * schema that a write by Spark leaves in the file.
   */
  private def writeValues(file: File, id: Int, sparkSchema: Option[StructType] = None)(
      columns: (Type, Array[Byte])*): Unit = {
    val fields = Types.required(INT32).named("id") +: columns.map(_._1)
    val schema = new MessageType("spark_schema", fields.asJava)
    val keyValues =
      sparkSchema.map(value => "org.apache.spark.sql.parquet.row.metadata" -> value.json).toMap
    write(file, schema, keyValues) { _ =>
      val group: Group = new SimpleGroup(schema)
      group.add("id", id)
      columns.foreach { case (field, bytes) =>
        group.add(field.getName, Binary.fromConstantByteArray(bytes))
      }
      group
    }
  }

  /**
   * Writes a file of one row: `id`, a geometry column `anchor`, and a binary column `geography`
   * with `bytes`, which the schema that Spark leaves in the file gives Sedona's own geography
   * type. A GeoParquet 1 write stores a geography column this way.
   */
  private def writeStoredGeography(file: File, id: Int, bytes: Array[Byte]): Unit = {
    val sparkSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("anchor", BinaryType),
        StructField("geography", GeographyUDT())))
    writeValues(file, id, Some(sparkSchema))(
      geometry("anchor") -> wkb("POINT (3 4)"),
      Types.optional(BINARY).named("geography") -> bytes)
  }

  /**
   * Writes a file of one row: `id`, and a binary column `shape` that the GeoParquet 1.1 metadata
   * of the file makes a geometry column. `wkt` is None for a row without a geometry, and `crs` is
   * what the metadata states as the CRS of the column, None for a column without that key.
   */
  private def writeGeoParquet1(
      file: File,
      id: Int,
      wkt: Option[String],
      crs: Option[String] = None): Unit = {
    val schema = Types
      .buildMessage()
      .required(INT32)
      .named("id")
      .addField(Types.optional(BINARY).named("shape"))
      .named("spark_schema")
    val crsEntry = crs.map(value => s""","crs":$value""").getOrElse("")
    val geo = """{"version":"1.1.0","primary_column":"shape","columns":{"shape":""" +
      s"""{"encoding":"WKB","geometry_types":[]$crsEntry}}}"""
    write(file, schema, Map("geo" -> geo)) { _ =>
      val group: Group = new SimpleGroup(schema)
      group.add("id", id)
      wkt.foreach(value => group.add("shape", Binary.fromConstantByteArray(wkb(value))))
      group
    }
  }

  /** Writes a file of one row, whose column `holder` has the geography column `geography`. */
  private def writeNestedGeography(
      file: File,
      id: Int,
      crs: Option[String],
      wkt: String): Unit = {
    val schema = Types
      .buildMessage()
      .required(INT32)
      .named("id")
      .addField(
        Types.optionalGroup().addField(geography("geography", crs, "spherical")).named("holder"))
      .named("data")
    write(file, schema) { _ =>
      val group: Group = new SimpleGroup(schema)
      group.add("id", id)
      group.addGroup("holder").add("geography", Binary.fromConstantByteArray(wkb(wkt)))
      group
    }
  }

  private def reader(ignoreCorruptFiles: String) =
    sparkSession.read.format("geoparquet").option("ignoreCorruptFiles", ignoreCorruptFiles)

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
            // The files are sound, so a scan that ignores corrupt files rejects them as well
            for (ignoreCorruptFiles <- Seq("false", "true")) {
              val error =
                intercept[Exception](reader(ignoreCorruptFiles).load(directory.getPath).collect())
              val message = messages(error).toLowerCase(java.util.Locale.ROOT)
              assert(message.contains("'shape'"))
              assert(message.contains("geometry"))
              assert(message.contains("geography"))
            }
          }
        }
      }

      // A column of GeoParquet 1 is a binary column that the metadata of the file makes a
      // geometry column, with or without a geometry in it
      for {
        (values, wkt, crs) <- Seq(
          ("a geometry", Some("POINT (1 2)"), None),
          ("no geometry and no CRS", None, Some("null")))
        geometryFirst <- Seq(true, false)
      } {
        val order = if (geometryFirst) "geometry column first" else "GEOGRAPHY column first"
        it(
          s"rejects a geometry column of GeoParquet 1 with $values next to a GEOGRAPHY column " +
            s"($order)") {
          withDirectory { directory =>
            def name(geometry: Boolean): String =
              if (geometry == geometryFirst) "part-0.parquet" else "part-1.parquet"
            writeGeoParquet1(new File(directory, name(geometry = true)), 0, wkt, crs)
            writeRow(
              new File(directory, name(geometry = false)),
              1,
              geography("shape", None, "spherical") -> "POINT (5 6)")
            for (ignoreCorruptFiles <- Seq("false", "true")) {
              val error =
                intercept[Exception](reader(ignoreCorruptFiles).load(directory.getPath).collect())
              val message = messages(error).toLowerCase(java.util.Locale.ROOT)
              assert(message.contains("'shape'"))
              assert(message.contains("geometry"))
              assert(message.contains("geography"))
            }
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

      // A writer has to state a CRS for a GEOGRAPHY column of a file before it has seen a
      // geography, and has none to state for a file without one
      for (emptyFirst <- Seq(true, false); mergeSchema <- Seq("false", "true")) {
        val order = if (emptyFirst) "first" else "second"
        it(
          "reads a file without a geography in a column next to a file that states another " +
            s"SRID for it (the file without one $order, mergeSchema $mergeSchema)") {
          withDirectory { directory =>
            def name(empty: Boolean): String =
              if (empty == emptyFirst) "part-0.parquet" else "part-1.parquet"
            writeGeographies(new File(directory, name(empty = true)))(null)
            writeRow(
              new File(directory, name(empty = false)),
              1,
              geography("geography", Some("EPSG:4269"), "spherical") -> "POINT (1 2)")
            val df = sparkSession.read
              .format("geoparquet")
              .option("mergeSchema", mergeSchema)
              .load(directory.getPath)
            assertGeography(df, 4269)
            assert(ewkt(df) == Seq(null, "SRID=4269; POINT (1 2)"))
            if (SpatialTypeSupport.usesNativeTypes) {
              val schema = StructType(
                Seq(
                  StructField("id", IntegerType),
                  StructField("geography", SpatialTypeSupport.geographyType(4269))))
              val withSchema =
                sparkSession.read.schema(schema).format("geoparquet").load(directory.getPath)
              assert(ewkt(withSchema) == Seq(null, "SRID=4269; POINT (1 2)"))
            }
          }
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

      it("rejects a GEOGRAPHY column for a geometry schema in a file that a filter leaves out") {
        withDirectory { directory =>
          // The bounding box of the file is far from the window of the query
          val geo =
            """{"version":"2.0.0","primary_column":"geography","columns":{"geography":
              |{"encoding":"WKB","geometry_types":["Point"],"edges":"spherical",
              |"bbox":[45.0,68.2,45.0,68.2]}}}""".stripMargin.replace("\n", "")
          writeGeographies(new File(directory, "part-0.parquet"), keyValues = Map("geo" -> geo))(
            "POINT (45 68.2)")
          val schema = StructType(
            Seq(
              StructField("id", IntegerType),
              StructField("geography", SpatialTypeSupport.geometryType(4326))))
          val df = sparkSession.read.schema(schema).format("geoparquet").load(directory.getPath)
          val window = "ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)"
          val error = intercept[Exception](df.where(s"ST_Intersects(geography, $window)").count())
          assert(messages(error).contains(
            "Column 'geography' is a Parquet GEOGRAPHY column and cannot be read as a geometry"))
        }
      }

      it("does not leave out a file whose geography edges do not follow a sphere") {
        withDirectory { directory =>
          writeGeographies(new File(directory, "part-0.parquet"))("POINT (1 2)")
          writeGeographies(new File(directory, "part-1.parquet"), edges = "karney")("POINT (3 4)")
          for (ignoreCorruptFiles <- Seq("false", "true")) {
            val error =
              intercept[Exception](ewkt(reader(ignoreCorruptFiles).load(directory.getPath)))
            assert(messages(error).contains("karney"))
          }
        }
      }

      it("does not leave out a file that states another SRID for a nested geography column") {
        withDirectory { directory =>
          writeNestedGeography(new File(directory, "part-0.parquet"), 0, None, "POINT (1 2)")
          writeNestedGeography(
            new File(directory, "part-1.parquet"),
            1,
            Some("EPSG:4269"),
            "POINT (3 4)")
          def load(ignoreCorruptFiles: String): Seq[String] = ewkt(
            reader(ignoreCorruptFiles)
              .load(directory.getPath)
              .selectExpr("id", "holder.geography AS geography"))
          for (ignoreCorruptFiles <- Seq("false", "true")) {
            if (SpatialTypeSupport.usesNativeTypes) {
              // The type of the column names the SRID of the first file
              val message = messages(intercept[Exception](load(ignoreCorruptFiles)))
              assert(message.contains("'geography'"))
              assert(message.contains("4269"))
            } else {
              assert(
                load(ignoreCorruptFiles) ==
                  Seq("SRID=4326; POINT (1 2)", "SRID=4269; POINT (3 4)"))
            }
          }
        }
      }

      it("leaves out a corrupt file before it compares what the files state about a column") {
        withDirectory { directory =>
          val corrupt = new File(directory, "part-0.parquet")
          writeRow(
            corrupt,
            0,
            geometry("shape", Some("EPSG:3857")) -> "POINT (1 2)",
            geography("geography", Some("EPSG:4269"), "spherical") -> "POINT (1 2)")
          ParquetTestFiles.damageRowGroups(corrupt)
          writeRow(
            new File(directory, "part-1.parquet"),
            1,
            geometry("shape") -> "POINT (3 4)",
            geography("geography", None, "spherical") -> "POINT (3 4)")
          for (mergeSchema <- Seq("false", "true")) {
            val df = reader(ignoreCorruptFiles = "true")
              .option("mergeSchema", mergeSchema)
              .load(directory.getPath)
            if (SpatialTypeSupport.usesNativeTypes) {
              assertGeography(df, 4326)
              assert(df.schema("shape").dataType == SpatialTypeSupport.geometryType(4326))
            }
            assert(ewkt(df) == Seq("SRID=4326; POINT (3 4)"))
            assert(ewkt(df, "shape") == Seq("SRID=4326;POINT (3 4)"))
          }
        }
      }

      // A file may come with the schema of the Spark write that made it, which describes a
      // repeated field as an array: of the binary values that it stores, or of a spatial type
      Seq[(String, Option[(DataType, DataType)])](
        "no schema of Spark" -> None,
        "a schema of Spark with binary elements" -> Some((BinaryType, BinaryType)),
        "a schema of Spark with spatial elements" -> Some(
          (SpatialTypeSupport.geometryType(4326), SpatialTypeSupport.geographyType(4326))))
        .foreach { case (description, elements) =>
          it(
            s"reads repeated GEOMETRY and GEOGRAPHY fields as arrays, in a file with $description") {
            withDirectory { directory =>
              val shapes =
                Types.repeated(BINARY).as(ParquetGeospatialTypes.geometry(None)).named("shapes")
              val items = Types
                .repeated(BINARY)
                .as(ParquetGeospatialTypes.geography(None, "spherical"))
                .named("items")
              val schema = new MessageType(
                "data",
                Seq[Type](Types.required(INT32).named("id"), shapes, items).asJava)
              val keyValues = elements.map { case (shape, item) =>
                val sparkSchema = StructType(
                  Seq(
                    StructField("id", IntegerType, nullable = false),
                    StructField("shapes", ArrayType(shape)),
                    StructField("items", ArrayType(item))))
                "org.apache.spark.sql.parquet.row.metadata" -> sparkSchema.json
              }.toMap
              write(new File(directory, "part-0.parquet"), schema, keyValues) { id =>
                val group: Group = new SimpleGroup(schema)
                group.add("id", id)
                Seq("POINT (1 2)", "POINT (3 4)").foreach { wkt =>
                  group.add("shapes", Binary.fromConstantByteArray(wkb(wkt)))
                  group.add("items", Binary.fromConstantByteArray(wkb(wkt)))
                }
                group
              }
              for (mergeSchema <- Seq("false", "true")) {
                val df = sparkSession.read
                  .format("geoparquet")
                  .option("mergeSchema", mergeSchema)
                  .load(directory.getPath)
                def elementOf(column: String): DataType =
                  df.schema(column).dataType.asInstanceOf[ArrayType].elementType
                assert(SpatialTypeSupport.isGeometry(elementOf("shapes")))
                assert(SpatialTypeSupport.isGeography(elementOf("items")))
                val row = df
                  .selectExpr(
                    "size(shapes)",
                    "ST_AsEWKT(shapes[0])",
                    "ST_AsEWKT(shapes[1])",
                    "size(items)",
                    "ST_AsEWKT(items[0])",
                    "ST_AsEWKT(items[1])")
                  .head()
                assert(
                  row == Row(
                    2,
                    "SRID=4326;POINT (1 2)",
                    "SRID=4326;POINT (3 4)",
                    2,
                    "SRID=4326; POINT (1 2)",
                    "SRID=4326; POINT (3 4)"))
              }
            }
          }
        }

      // A GeoParquet 1 write stores a geography column as Sedona's own geography type is
      // serialized. Files of it may be next to the files of a GeoParquet 2 write to the same
      // path. A geography without an SRID is in OGC:CRS84 for the native geography type.
      for (storedSrid <- Seq(4326, 0); mergeSchema <- Seq("false", "true")) {
        it(
          "reads a column that one file stores as Sedona's geography type and the next as " +
            s"GEOGRAPHY (stored SRID $storedSrid, mergeSchema $mergeSchema)") {
          withDirectory { directory =>
            writeStoredGeography(
              new File(directory, "part-0.parquet"),
              0,
              GeographySerializer.serializeWkb(wkb("POINT (1 2)"), storedSrid))
            writeRow(
              new File(directory, "part-1.parquet"),
              1,
              geometry("anchor") -> "POINT (3 4)",
              geography("geography", None, "spherical") -> "POINT (5 6)")
            val df = sparkSession.read
              .format("geoparquet")
              .option("mergeSchema", mergeSchema)
              .load(directory.getPath)
            assertGeography(df, 4326)
            val stored =
              if (storedSrid == 4326 || SpatialTypeSupport.usesNativeTypes) {
                "SRID=4326; POINT (1 2)"
              } else "POINT (1 2)"
            assert(ewkt(df) == Seq(stored, "SRID=4326; POINT (5 6)"))
          }
        }
      }

      it("does not leave out a file that stores the geographies of a column in another way") {
        withDirectory { directory =>
          // Sedona 1.9 stored a geography as its SRID in big-endian order and its WKB. Sedona
          // does not read that, and a file of it is none that a scan may skip as a corrupt one.
          writeStoredGeography(
            new File(directory, "part-0.parquet"),
            0,
            ByteBuffer.allocate(Integer.BYTES).putInt(4326).array() ++ wkb("POINT (1 2)"))
          writeRow(
            new File(directory, "part-1.parquet"),
            1,
            geometry("anchor") -> "POINT (3 4)",
            geography("geography", None, "spherical") -> "POINT (5 6)")
          for (ignoreCorruptFiles <- Seq("false", "true")) {
            val error =
              intercept[Exception](ewkt(reader(ignoreCorruptFiles).load(directory.getPath)))
            if (SpatialTypeSupport.usesNativeTypes) {
              assert(messages(error).contains("Column 'geography' is stored as binary"))
            }
          }
        }
      }

      if (SpatialTypeSupport.usesNativeTypes) {
        for (binaryFirst <- Seq(true, false)) {
          val order = if (binaryFirst) "binary column first" else "GEOGRAPHY column first"
          it(s"rejects a column that is binary in one file and GEOGRAPHY in another ($order)") {
            withDirectory { directory =>
              def name(binary: Boolean): String =
                if (binary == binaryFirst) "part-0.parquet" else "part-1.parquet"
              writeRow(
                new File(directory, name(binary = true)),
                0,
                geometry("anchor") -> "POINT (3 4)",
                Types.optional(BINARY).named("geography") -> "POINT (1 2)")
              writeRow(
                new File(directory, name(binary = false)),
                1,
                geometry("anchor") -> "POINT (3 4)",
                geography("geography", None, "spherical") -> "POINT (5 6)")
              for (ignoreCorruptFiles <- Seq("false", "true")) {
                val error = intercept[Exception](
                  reader(ignoreCorruptFiles).load(directory.getPath).collect())
                assert(messages(error).contains("'geography'"))
              }
            }
          }
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

  describe("Writing geography columns to GeoParquet 2 files") {
    if (ParquetGeospatialTypes.supported) {
      def writeVersion2(df: DataFrame, path: String, options: (String, String)*): Unit =
        df.coalesce(1)
          .write
          .format("geoparquet")
          .option("geoparquet.version", "2.0.0")
          .options(options.toMap)
          .save(path)

      def writtenFile(path: String): Path = {
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.length == 1)
        new Path(files.head.getPath)
      }

      def footer(path: String): ParquetMetadata = {
        val reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(writtenFile(path), new Configuration()))
        try reader.getFooter
        finally reader.close()
      }

      def column(path: String, name: String): Option[ParquetGeospatialColumn] = {
        val schema = footer(path).getFileMetaData.getSchema
        ParquetGeospatialTypes.describe(schema.getType(schema.getFieldIndex(name)))
      }

      def geoMetadata(path: String): JValue =
        parse(footer(path).getFileMetaData.getKeyValueMetaData.get("geo"))

      /** The bytes that the file stores for `name` in its first row. */
      def stored(path: String, name: String): String = {
        val reader = ParquetReader.builder(new GroupReadSupport(), writtenFile(path)).build()
        try WKBWriter.toHex(reader.read().getBinary(name, 0).getBytes)
        finally reader.close()
      }

      val spherical = Some("spherical")
      val line = "LINESTRING (0 0, 10 10)"
      // ISO WKB of the line, in the byte order of little endian machines
      val lineWkb = WKBWriter.toHex(wkb(line))

      it("stores a geography column in the GEOGRAPHY logical type, as WKB") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql(s"""SELECT 1 AS id, ST_Point(1, 2) AS geometry,
                 |ST_GeogFromWKT('$line', 4326) AS geography""".stripMargin),
            path)
          assert(
            column(path, "geography") == Some(ParquetGeospatialColumn(true, None, spherical)))
          assert(stored(path, "geography") == lineWkb)
          val geo = geoMetadata(path)
          assert(geo \ "primary_column" == JString("geometry"))
          val metadata = geo \ "columns" \ "geography"
          assert(metadata \ "encoding" == JString("WKB"))
          assert(metadata \ "geometry_types" == JArray(List(JString("LineString"))))
          assert(metadata \ "edges" == JString("spherical"))
          // OGC:CRS84 is the CRS of a column that states none
          assert(metadata \ "crs" == JNothing)
          // The bounding box of GeoParquet metadata is planar
          assert(metadata \ "bbox" == JNothing)
          assert(metadata \ "covering" == JNothing)
          assert(geo \ "columns" \ "geometry" \ "edges" == JNothing)
          val df = read(path)
          assertGeography(df, 4326)
          assert(ewkt(df) == Seq(s"SRID=4326; $line"))
        }
      }

      it("writes a file whose only spatial column is a geography column") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line', 4326) AS geography"),
            path)
          assert(geoMetadata(path) \ "primary_column" == JString("geography"))
          val df = read(path)
          assertGeography(df, 4326)
          assert(ewkt(df) == Seq(s"SRID=4326; $line"))
        }
      }

      it("states the CRS of a geography column that is not OGC:CRS84") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line', 4269) AS geography"),
            path)
          assert(
            column(path, "geography") ==
              Some(ParquetGeospatialColumn(true, Some("EPSG:4269"), spherical)))
          val id = geoMetadata(path) \ "columns" \ "geography" \ "crs" \ "id"
          assert(id \ "authority" == JString("EPSG"))
          assert(id \ "code" == JInt(4269))
          val df = read(path)
          assertGeography(df, 4269)
          assert(ewkt(df) == Seq(s"SRID=4269; $line"))
        }
      }

      it("takes the CRS of a geography column from a CRS option") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line', 4269) AS geography"),
            path,
            "geoparquet.crs.geography" -> projjson4269)
          assert(
            column(path, "geography") ==
              Some(ParquetGeospatialColumn(true, Some("EPSG:4269"), spherical)))
          assert(geoMetadata(path) \ "columns" \ "geography" \ "crs" == parse(projjson4269))
        }
      }

      it("keeps the Z coordinate of a geography") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          // A geography that is read from WKB keeps it. One that is read from text has no Z.
          val pointZ = "01E9030000000000000000F03F00000000000000400000000000000840"
          writeVersion2(
            sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKB(unhex('$pointZ')) AS geography"),
            path)
          assert(stored(path, "geography") == pointZ)
          assert(
            geoMetadata(path) \ "columns" \ "geography" \ "geometry_types" ==
              JArray(List(JString("Point Z"))))
        }
      }

      it("writes a geography column without a value") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql(s"""SELECT id, IF(id = 0, NULL, ST_GeogFromWKT('$line', 4326))
                 |AS geography FROM range(1)""".stripMargin),
            path)
          assert(
            column(path, "geography") == Some(ParquetGeospatialColumn(true, None, spherical)))
          assert(geoMetadata(path) \ "columns" \ "geography" \ "geometry_types" == JArray(Nil))
          val df = read(path)
          assertGeography(df, 4326)
          assert(df.where("geography IS NULL").count() == 1)
        }
      }

      it("returns the column from Spark's own Parquet reader") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line', 4326) AS geography"),
            path)
          val df = sparkSession.read.parquet(path)
          if (SpatialTypeSupport.usesNativeTypes) {
            // The schema that Spark leaves in the file names its native type
            assert(df.schema("geography").dataType == SpatialTypeSupport.geographyType(4326))
            assert(ewkt(df) == Seq(s"SRID=4326; $line"))
          } else {
            assert(df.schema("geography").dataType == BinaryType)
            val hex = df.selectExpr("hex(geography)").collect().head.getString(0)
            assert(hex == lineWkb)
          }
        }
      }

      if (SpatialTypeSupport.usesNativeTypes) {
        it("rejects a native geography column whose type names no SRID") {
          withDirectory { directory =>
            val error = intercept[Exception] {
              writeVersion2(
                sparkSession.sql(s"""SELECT 1 AS id,
                     |CAST(ST_GeogFromWKT('$line', 4326) AS GEOGRAPHY(ANY)) AS geography""".stripMargin),
                new File(directory, "data").getPath)
            }
            assert(messages(error).contains("GEOGRAPHY(<srid>)"))
          }
        }
      } else {
        // Sedona's own geography type has an SRID on each value, and a GEOGRAPHY column has one
        // CRS. A geography without an SRID is in OGC:CRS84, as a geography that Sedona
        // constructs without one is on Spark 4.2.
        it("writes geographies without an SRID as OGC:CRS84") {
          withDirectory { directory =>
            val path = new File(directory, "data").getPath
            writeVersion2(
              sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line') AS geography"),
              path)
            assert(
              column(path, "geography") == Some(ParquetGeospatialColumn(true, None, spherical)))
            assert(ewkt(read(path)) == Seq(s"SRID=4326; $line"))
          }
        }

        it("rejects geographies with another SRID than the first of the file") {
          withDirectory { directory =>
            val error = intercept[Exception] {
              writeVersion2(
                sparkSession.sql(s"""SELECT id,
                     |IF(id = 0, ST_GeogFromWKT('$line', 4326), ST_GeogFromWKT('$line', 4269))
                     |AS geography FROM range(2)""".stripMargin),
                new File(directory, "data").getPath)
            }
            assert(messages(error).contains("'geography'"))
            assert(messages(error).contains("SRID 4326"))
            assert(messages(error).contains("SRID 4269"))
          }
        }

        // The GeoParquet metadata of a file names its columns, and none of the fields within
        // them
        it(
          "keeps the SRID of a geometry that is nested in a column named as a geography column") {
          withDirectory { directory =>
            val path = new File(directory, "data").getPath
            writeVersion2(
              sparkSession.sql(s"""SELECT 1 AS id, ST_GeogFromWKT('$line', 4269) AS shape,
                   |named_struct('shape', ST_Point(1, 2)) AS holder""".stripMargin),
              path)
            val df = read(path)
            assert(df.selectExpr("ST_SRID(holder.shape)").head().getInt(0) == 0)
            assert(ewkt(df, "shape") == Seq(s"SRID=4269; $line"))
          }
        }

        // The GEOGRAPHY logical type takes a geographic CRS, with longitudes from -180 to 180
        // and latitudes from -90 to 90. Sedona's own geography type takes any SRID and any
        // coordinates.
        it("rejects geographies whose SRID is the one of a projected CRS") {
          withDirectory { directory =>
            val error = intercept[Exception] {
              writeVersion2(
                sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line', 3857) AS geography"),
                new File(directory, "data").getPath)
            }
            assert(messages(error).contains("'geography'"))
            assert(messages(error).contains("geographic"))
          }
        }

        it("rejects a CRS option that gives a geography column a projected CRS") {
          withDirectory { directory =>
            val projected = """{"type":"ProjectedCRS","name":"WGS 84 / Pseudo-Mercator",""" +
              """"id":{"authority":"EPSG","code":3857}}"""
            val error = intercept[Exception] {
              writeVersion2(
                sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKT('$line', 3857) AS geography"),
                new File(directory, "data").getPath,
                "geoparquet.crs.geography" -> projected)
            }
            assert(messages(error).contains("'geography'"))
            assert(messages(error).contains("geographic"))
          }
        }

        Seq(
          "POINT (190 10)" -> "longitude 190",
          "POINT (10 100)" -> "latitude 100",
          "LINESTRING (0 0, -180.5 10)" -> "longitude -180.5",
          "POLYGON ((0 0, 10 0, 10 -91, 0 0))" -> "latitude -91",
          "GEOMETRYCOLLECTION (POINT (1 2), MULTIPOINT ((3 4), (5 95)))" -> "latitude 95")
          .foreach { case (wkt, coordinate) =>
            it(s"rejects $wkt, which is outside the bounds of a geography") {
              withDirectory { directory =>
                val error = intercept[Exception] {
                  writeVersion2(
                    sparkSession.sql(
                      s"SELECT 1 AS id, ST_GeogFromWKT('$wkt', 4326) AS geography"),
                    new File(directory, "data").getPath)
                }
                assert(messages(error).contains("'geography'"))
                assert(messages(error).contains(coordinate))
              }
            }
          }

        it("writes geographies at the bounds of longitude and latitude, and empty ones") {
          withDirectory { directory =>
            val path = new File(directory, "data").getPath
            val wkts = Seq("POINT (-180 -90)", "LINESTRING (180 90, 0 0)", "POINT EMPTY")
            val rows = wkts.zipWithIndex.map { case (wkt, id) =>
              s"SELECT $id AS id, ST_GeogFromWKT('$wkt', 4326) AS geography"
            }
            writeVersion2(sparkSession.sql(rows.mkString(" UNION ALL ")), path)
            assert(read(path).count() == 3)
          }
        }

        it("keeps a geography column that is nested in another column as it is serialized") {
          withDirectory { directory =>
            val path = new File(directory, "data").getPath
            writeVersion2(
              sparkSession.sql(s"""SELECT 1 AS id, ST_Point(1, 2) AS geometry,
                   |named_struct('geography', ST_GeogFromWKT('$line', 4326)) AS holder""".stripMargin),
              path)
            val df = read(path).selectExpr("id", "holder.geography AS geography")
            assert(df.schema("geography").dataType == GeographyUDT())
            assert(ewkt(df) == Seq(s"SRID=4326; $line"))
          }
        }
      }
    }
  }

  describe("Writing geography columns to GeoParquet 1 files") {
    // GeoParquet 1 has no GEOGRAPHY logical type. Sedona's own geography type is written as it
    // is serialized, and the schema that Spark leaves in the file names the type.
    if (SpatialTypeSupport.usesNativeTypes) {
      it("rejects a column of Spark's native geography type") {
        withDirectory { directory =>
          val error = intercept[Exception] {
            sparkSession
              .sql("""SELECT ST_Point(1, 2) AS geometry,
                     |ST_GeogFromWKT('POINT (1 2)', 4326) AS geography""".stripMargin)
              .write
              .format("geoparquet")
              .option("geoparquet.version", "1.1.0")
              .save(new File(directory, "data").getPath)
          }
          assert(messages(error).contains("'geography'"))
          assert(messages(error).contains("geoparquet.version"))
          assert(messages(error).contains("2.0.0"))
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
