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
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat, ParquetReader}
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.geoparquet.{GeoParquetFileFormat, GeoParquetMetaData, GeoParquetWriteSupport, ParquetGeospatialColumn, ParquetGeospatialTypes}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.json4s.{JArray, JDouble, JInt, JNothing, JNull, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import scala.collection.mutable
import scala.util.Try

/**
 * GeoParquet 2.0 stores geometries in Parquet's GEOMETRY and GEOGRAPHY logical types. The
 * fixtures under `geoparquet/native` were written by other tools: `example-2.0.0.parquet` is the
 * example file of the GeoParquet 2.0.0 release, the `spark-4.2-*` files come from Spark 4.2's own
 * Parquet writer, and the `crs-*` files come from parquet-java 1.17.0, one per form the Parquet
 * format allows for the CRS of a column. Each `crs-*` file holds three POINT (1 2) rows.
 * `spark-4.2-geography-geo-metadata.parquet` is Spark's geography file with GeoParquet 2.0
 * metadata added to its footer.
 */
class GeoParquetNativeTypesSuite extends TestBaseScala {
  private val nativeFolder = resourceFolder + "geoparquet/native/"

  /** Parquet libraries older than 1.16.0 have no geospatial logical types. */
  private val libraryHasGeospatialTypes = Try(
    Class.forName(
      "org.apache.parquet.schema.LogicalTypeAnnotation$GeometryLogicalTypeAnnotation")).isSuccess

  /**
   * Spark 4.2 has a registry of EPSG and ESRI coordinate reference systems. Sedona asks it about
   * authorities other than EPSG when it uses Spark's native spatial types; older Spark versions
   * know no ESRI code, so the SRID stays unknown there.
   */
  private val esriSrid = if (SpatialTypeSupport.usesNativeTypes) 102100 else 0

  private def read(file: String) =
    sparkSession.read.format("geoparquet").load(nativeFolder + file)

  private def messages(error: Throwable): String = Iterator
    .iterate(error)(_.getCause)
    .takeWhile(_ != null)
    .flatMap(cause => Option(cause.getMessage))
    .mkString("\n")

  private def withPath(run: String => Unit): Unit = {
    val directory = Files.createTempDirectory("geoparquet-native").toFile
    try run(new File(directory, "data").getAbsolutePath)
    finally FileUtils.deleteDirectory(directory)
  }

  /** The Parquet file that a write of one partition leaves in `path`. */
  private def writtenFile(path: String): Path = {
    val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
    assert(files.length == 1)
    new Path(files.head.getPath)
  }

  private def footer(path: String): ParquetMetadata = {
    val reader =
      ParquetFileReader.open(HadoopInputFile.fromPath(writtenFile(path), new Configuration()))
    try reader.getFooter
    finally reader.close()
  }

  private def geometryColumn(footer: ParquetMetadata): Option[ParquetGeospatialColumn] = {
    val schema = footer.getFileMetaData.getSchema
    ParquetGeospatialTypes.describe(schema.getType(schema.getFieldIndex("geometry")))
  }

  /** The CRS that the Parquet schema of the file in `path` states for `column`. */
  private def parquetCrs(path: String, column: String): Option[String] = {
    val schema = footer(path).getFileMetaData.getSchema
    ParquetGeospatialTypes.describe(schema.getType(schema.getFieldIndex(column))).flatMap(_.crs)
  }

  private def geoMetadata(footer: ParquetMetadata): JValue =
    parse(footer.getFileMetaData.getKeyValueMetaData.get("geo"))

  private def geometry(crs: Option[String]): Option[ParquetGeospatialColumn] =
    Some(ParquetGeospatialColumn(isGeography = false, crs, None))

  private def point(srid: Int): DataFrame = sparkSession
    .sql(s"SELECT ST_SetSRID(ST_Point(1.0, 2.0), $srid) AS geometry")
    .coalesce(1)

  private def pointWithSrid(srid: Int): Geometry = {
    val point = new GeometryFactory().createPoint(new Coordinate(1, 2))
    point.setSRID(srid)
    point
  }

  /** One partition of GeometryUDT columns, where the SRID belongs to each geometry. */
  private def udtColumns(names: Seq[String], rows: Seq[Geometry]*): DataFrame =
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(rows.map(Row.fromSeq(_)), 1),
      StructType(names.map(StructField(_, GeometryUDT()))))

  private def udtColumn(geometries: Geometry*): DataFrame =
    udtColumns(Seq("geometry"), geometries.map(Seq(_)): _*)

  private def writeVersion2(df: DataFrame, path: String, options: (String, String)*): Unit =
    df.write
      .format("geoparquet")
      .option("geoparquet.version", "2.0.0")
      .options(options.toMap)
      .save(path)

  /**
   * Writes a GeoParquet 2 file to `path` with the writer that a write task gets, the way a caller
   * does that hands it rows of its own.
   */
  private def withWriter(
      path: String,
      schema: StructType,
      options: Map[String, String] = Map.empty,
      writeSupport: Option[Class[_]] = None)(run: OutputWriter => Unit): Unit = {
    val all = options + ("geoparquet.version" -> "2.0.0")
    val job = Job.getInstance(sparkSession.sessionState.newHadoopConfWithOptions(all))
    val factory = new GeoParquetFileFormat().prepareWrite(sparkSession, job, all, schema)
    writeSupport.foreach { support =>
      job.getConfiguration.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, support.getName)
    }
    val context = new TaskAttemptContextImpl(
      job.getConfiguration,
      new TaskAttemptID("test", 0, TaskType.MAP, 0, 0))
    val writer = factory.newInstance(path, schema, context)
    try run(writer)
    catch {
      case error: Throwable =>
        Try(writer.close())
        throw error
    }
    writer.close()
  }

  private def withCaseSensitiveNames(run: => Unit): Unit = {
    val key = "spark.sql.caseSensitive"
    val before = sparkSession.conf.get(key)
    sparkSession.conf.set(key, "true")
    try run
    finally sparkSession.conf.set(key, before)
  }

  /** The SRID of each geometry read back from `path`, None for a null geometry. */
  private def sridsIn(path: String, column: String = "geometry"): Seq[Option[Int]] =
    sparkSession.read
      .format("geoparquet")
      .load(path)
      .collect()
      .map(row => Option(row.getJtsGeometry(column)).map(_.getSRID))
      .toSeq

  describe("Reading Parquet native geospatial types") {
    if (libraryHasGeospatialTypes) {
      it("reads the GeoParquet 2.0.0 example file") {
        val df = read("example-2.0.0.parquet")
        assert(SpatialTypeSupport.isGeometry(df.schema("geometry").dataType))
        val rows = df.collect()
        assert(rows.length == 5)
        val fiji = rows.find(_.getAs[String]("name") == "Fiji").get
        assert(fiji.getJtsGeometry("geometry").getGeometryType == "MultiPolygon")
        assert(fiji.getJtsGeometry("geometry").getSRID == 4326)
      }

      // The CRS forms a GeoParquet 2.0 reader must or should understand, none of these files
      // has GeoParquet metadata.
      Seq(
        "crs-absent.parquet" -> 4326,
        "crs-authority-code.parquet" -> 3857,
        "crs-projjson.parquet" -> 32610,
        "crs-srid.parquet" -> 32610,
        "crs-srid-0.parquet" -> 0,
        "crs-projjson-key.parquet" -> 32610,
        "crs-esri.parquet" -> esriSrid,
        "spark-4.2-geometry-3857.parquet" -> 3857,
        "spark-4.2-geometry-srid-0.parquet" -> 0).foreach { case (file, srid) =>
        it(s"reads $file without GeoParquet metadata as geometry with SRID $srid") {
          val df = read(file)
          assert(SpatialTypeSupport.isGeometry(df.schema("geometry").dataType))
          val geometries = df.collect().map(_.getJtsGeometry("geometry"))
          assert(geometries.length == 3)
          geometries.foreach { geometry =>
            assert(geometry.toText == "POINT (1 2)")
            assert(geometry.getSRID == srid)
          }
        }
      }

      Seq("crs-unresolvable.parquet", "crs-projjson-key-missing.parquet").foreach { file =>
        it(s"treats the CRS of $file as unknown, which native Spark types reject") {
          if (SpatialTypeSupport.usesNativeTypes) {
            val error = intercept[Exception](read(file).collect())
            assert(messages(error).contains("cannot be represented by a Spark native SRID"))
            // A schema with Sedona's own geometry type still reads the column
            val schema = StructType(Seq(StructField("geometry", GeometryUDT())))
            val geometries = sparkSession.read
              .schema(schema)
              .format("geoparquet")
              .load(nativeFolder + file)
              .collect()
              .map(_.getAs[Geometry]("geometry"))
            assert(geometries.map(_.toText).toSeq == Seq.fill(3)("POINT (1 2)"))
            assert(geometries.map(_.getSRID).toSeq == Seq(0, 0, 0))
          } else {
            val geometries = read(file).collect().map(_.getJtsGeometry("geometry"))
            assert(geometries.map(_.getSRID).toSeq == Seq(0, 0, 0))
          }
        }
      }

      it("applies a spatial filter to a file without GeoParquet metadata") {
        val df = read("crs-srid-0.parquet")
        def count(envelope: String): Long =
          df.where(s"ST_Intersects(geometry, ST_GeomFromText('$envelope'))").count()
        assert(count("POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))") == 3)
        assert(count("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))") == 0)
      }

      it("reads a GEOGRAPHY column as WKB") {
        val df = read("spark-4.2-geography.parquet")
        assert(df.schema("geography").dataType == BinaryType)
        val wkb = df.selectExpr("hex(geography)").collect().map(_.getString(0))
        assert(wkb.toSeq == Seq.fill(3)("0101000000000000000000F03F0000000000000040"))
      }

      it("reads a GEOGRAPHY column as WKB when the GeoParquet metadata lists it") {
        val df = read("spark-4.2-geography-geo-metadata.parquet")
        assert(df.schema("geography").dataType == BinaryType)
        val wkb = df.selectExpr("hex(geography)").collect().map(_.getString(0))
        assert(wkb.toSeq == Seq.fill(3)("0101000000000000000000F03F0000000000000040"))
      }

      it("reports a file with neither native types nor GeoParquet metadata") {
        val error = intercept[Exception] {
          sparkSession.read
            .format("geoparquet")
            .load(resourceFolder + "geoparquet/plain.parquet")
            .collect()
        }
        assert(messages(error).contains("does not contain valid geo metadata"))
      }
    }
  }

  describe("Writing GeoParquet 2.0") {
    if (libraryHasGeospatialTypes) {
      val webMercator = compact(render(GeoParquetMetaData.sridToProjJson(3857).get))
      Seq(
        ("the default CRS", 4326, "", None),
        ("a CRS with an EPSG code", 3857, webMercator, Some("EPSG:3857")),
        ("an unknown CRS", 0, "null", Some("srid:0"))).foreach {
        case (title, srid, crsOption, crsProperty) =>
          it(s"writes a GEOMETRY column for $title") {
            withPath { path =>
              point(srid).write
                .format("geoparquet")
                .option("geoparquet.version", "2.0.0")
                .option("geoparquet.crs", crsOption)
                .save(path)
              val written = footer(path)
              assert(geometryColumn(written) == geometry(crsProperty))
              // GeoParquet 2.0 relies on row group statistics, so no covering column is added
              assert(written.getFileMetaData.getSchema.getFieldCount == 1)

              val metadata = geoMetadata(written)
              assert(metadata \ "version" == JString("2.0.0"))
              assert(metadata \ "primary_column" == JString("geometry"))
              val column = metadata \ "columns" \ "geometry"
              assert(column \ "encoding" == JString("WKB"))
              assert(column \ "geometry_types" == JArray(List(JString("Point"))))
              assert(column \ "bbox" == JArray(List(1.0, 2.0, 1.0, 2.0).map(JDouble(_))))
              assert(column \ "covering" == JNothing)
              srid match {
                case 4326 => assert(column \ "crs" == JNothing)
                case 0 => assert(column \ "crs" == JNull)
                case _ => assert(column \ "crs" \ "id" \ "code" == JInt(srid))
              }

              val geometries = sparkSession.read
                .format("geoparquet")
                .load(path)
                .collect()
                .map(_.getJtsGeometry("geometry"))
              assert(
                geometries.map(g => (g.toText, g.getSRID)).toSeq == Seq(("POINT (1 2)", srid)))
            }
          }
      }

      it("writes the version without the spaces around it") {
        withPath { path =>
          point(4326).write
            .format("geoparquet")
            .option("geoparquet.version", " 2.0.0 ")
            .option("geoparquet.crs", "")
            .save(path)
          val written = footer(path)
          assert(geometryColumn(written) == geometry(None))
          assert(geoMetadata(written) \ "version" == JString("2.0.0"))
        }
      }

      it("writes inline PROJJSON for a CRS without an identifier") {
        // Native Spark types cannot hold such a CRS, so only a GeometryUDT column can have it.
        if (!SpatialTypeSupport.usesNativeTypes) {
          withPath { path =>
            val localGrid = """{"type":"EngineeringCRS","name":"local grid"}"""
            point(0).write
              .format("geoparquet")
              .option("geoparquet.version", "2.0.0")
              .option("geoparquet.crs", localGrid)
              .save(path)
            val written = footer(path)
            assert(geometryColumn(written) == geometry(Some(localGrid)))
            assert(geoMetadata(written) \ "columns" \ "geometry" \ "crs" == parse(localGrid))
          }
        }
      }

      Seq(4326 -> None, 3857 -> Some("EPSG:3857"), 0 -> Some("srid:0")).foreach {
        case (srid, crsProperty) =>
          it(s"takes the CRS of a GEOMETRY column from SRID $srid") {
            // The SRID is part of a native Spark type and of each geometry of a GeometryUDT
            Seq(point(srid), udtColumn(pointWithSrid(srid), pointWithSrid(srid))).foreach { df =>
              withPath { path =>
                writeVersion2(df, path)
                assert(geometryColumn(footer(path)) == geometry(crsProperty))
                assert(sridsIn(path).forall(_.contains(srid)))
              }
            }
          }
      }

      it("takes the CRS of a GeometryUDT column from its first geometry after null rows") {
        withPath { path =>
          writeVersion2(udtColumn(null, null, pointWithSrid(3857)), path)
          assert(geometryColumn(footer(path)) == geometry(Some("EPSG:3857")))
          assert(sridsIn(path) == Seq(None, None, Some(3857)))
        }
      }

      it("takes the CRS of each GeometryUDT column from its own first geometry") {
        withPath { path =>
          val df = udtColumns(
            Seq("geometry", "other"),
            Seq(pointWithSrid(4326), null),
            Seq(pointWithSrid(4326), pointWithSrid(3857)))
          writeVersion2(df, path)
          val schema = footer(path).getFileMetaData.getSchema
          def crsOf(column: String): Option[String] = ParquetGeospatialTypes
            .describe(schema.getType(schema.getFieldIndex(column)))
            .flatMap(_.crs)
          assert(crsOf("geometry").isEmpty)
          assert(crsOf("other").contains("EPSG:3857"))
          assert(sridsIn(path) == Seq(Some(4326), Some(4326)))
          assert(sridsIn(path, "other") == Seq(None, Some(3857)))
        }
      }

      it("fails when a GeometryUDT column holds a second SRID") {
        withPath { path =>
          val error = intercept[Exception] {
            writeVersion2(udtColumn(pointWithSrid(4326), pointWithSrid(3857)), path)
          }
          val message = messages(error)
          assert(message.contains("SRID 4326") && message.contains("SRID 3857"))
          assert(message.contains("geoparquet.crs"))
        }
      }

      it("writes an unknown CRS for a GeometryUDT column without geometries") {
        Seq(udtColumn(null, null), udtColumn()).foreach { df =>
          withPath { path =>
            writeVersion2(df, path)
            assert(geometryColumn(footer(path)) == geometry(Some("srid:0")))
            assert(geoMetadata(footer(path)) \ "columns" \ "geometry" \ "crs" == JNull)
          }
        }
      }

      it("stops waiting for the first geometry when the rows held back reach the limit") {
        val limit = "geoparquet.srid.lookahead.bytes" -> "1"
        withPath { path =>
          // The CRS is fixed as unknown after the first row, which geometries without an SRID fit
          writeVersion2(udtColumn(null, pointWithSrid(0)), path, limit)
          assert(geometryColumn(footer(path)) == geometry(Some("srid:0")))
          assert(sridsIn(path) == Seq(None, Some(0)))
        }
        withPath { path =>
          val error = intercept[Exception] {
            writeVersion2(udtColumn(null, pointWithSrid(3857)), path, limit)
          }
          assert(messages(error).contains("geoparquet.crs"))
        }
      }

      it("does not check the geometries of a GeometryUDT column against geoparquet.crs") {
        withPath { path =>
          writeVersion2(
            udtColumn(pointWithSrid(4326), pointWithSrid(3857)),
            path,
            "geoparquet.crs" -> "")
          assert(geometryColumn(footer(path)) == geometry(None))
        }
      }

      it("takes the CRS options whatever the case of their keys") {
        // Spark takes the keys of the options of a write without regard to case
        val df = udtColumns(Seq("Geom"), Seq(pointWithSrid(4326)))
        Seq("geoparquet.crs.geom", "GeoParquet.CRS.Geom", "GEOPARQUET.CRS").foreach { key =>
          withPath { path =>
            writeVersion2(df, path, key -> webMercator)
            assert(parquetCrs(path, "Geom").contains("EPSG:3857"), key)
          }
        }
        withPath { path =>
          df.write
            .format("geoparquet")
            .option("geoparquet.version", "1.1.0")
            .option("GEOPARQUET.CRS.GEOM", webMercator)
            .save(path)
          assert(geoMetadata(footer(path)) \ "columns" \ "Geom" \ "crs" == parse(webMercator))
        }
      }

      it("takes the version whatever the case of its key") {
        withPath { path =>
          point(3857).write.format("geoparquet").option("GeoParquet.Version", "2.0.0").save(path)
          assert(geometryColumn(footer(path)).exists(_.crs.contains("EPSG:3857")))
        }
      }

      it("gives the CRS option of a column to that column only") {
        withCaseSensitiveNames {
          val df =
            udtColumns(Seq("Geom", "geom"), Seq(pointWithSrid(4326), pointWithSrid(32610)))
          withPath { path =>
            writeVersion2(df, path, "geoparquet.crs.Geom" -> webMercator)
            assert(parquetCrs(path, "Geom").contains("EPSG:3857"))
            assert(parquetCrs(path, "geom").contains("EPSG:32610"))
          }
          withPath { path =>
            df.write
              .format("geoparquet")
              .option("geoparquet.version", "1.1.0")
              .option("geoparquet.crs.Geom", webMercator)
              .save(path)
            val columns = geoMetadata(footer(path)) \ "columns"
            assert(columns \ "Geom" \ "crs" == parse(webMercator))
            assert(columns \ "geom" \ "crs" \ "id" \ "code" == JInt(32610))
          }
        }
      }

      it("rejects a CRS option that names two columns in a case that neither has") {
        withCaseSensitiveNames {
          val df =
            udtColumns(Seq("Geom", "geom"), Seq(pointWithSrid(4326), pointWithSrid(32610)))
          withPath { path =>
            val message = messages(intercept[Exception] {
              writeVersion2(df, path, "geoparquet.crs.GEOM" -> webMercator)
            })
            assert(message.contains("geoparquet.crs.GEOM"))
            assert(message.contains("'Geom'") && message.contains("'geom'"))
          }
        }
      }

      it("prefers a CRS option of the write to the Hadoop configuration") {
        val hadoop = sparkSession.sparkContext.hadoopConfiguration
        // An empty CRS leaves the CRS of the column out of the file
        hadoop.set("geoparquet.crs.geometry", "")
        try {
          withPath { path =>
            writeVersion2(
              udtColumn(pointWithSrid(4326)),
              path,
              "geoparquet.crs.GEOMETRY" -> webMercator)
            assert(parquetCrs(path, "geometry").contains("EPSG:3857"))
          }
        } finally hadoop.unset("geoparquet.crs.geometry")
      }

      it("keeps the limit of the rows held back apart from the CRS options of the columns") {
        withPath { path =>
          val df = udtColumns(
            Seq("lookahead.bytes", "other"),
            Seq(pointWithSrid(3857), pointWithSrid(4326)))
          writeVersion2(df, path, "geoparquet.crs.lookahead.bytes" -> webMercator)
          assert(parquetCrs(path, "lookahead.bytes").contains("EPSG:3857"))
          assert(parquetCrs(path, "other").isEmpty)
        }
      }

      it("releases the file when the rows held back until the end show a second SRID") {
        // Parquet lists the writers that are open, and offers no public way to read the list
        def openWriters: Int = {
          val manager = ParquetOutputFormat.getMemoryManager
          val writers = manager.getClass.getDeclaredMethod("getWriterList")
          writers.setAccessible(true)
          writers.invoke(manager).asInstanceOf[java.util.Map[_, _]].size
        }
        withPath(path => writeVersion2(udtColumn(pointWithSrid(4326)), path))
        val before = openWriters
        withPath { path =>
          // The second column shows no geometry, so the rows are held back until the file ends
          val df = udtColumns(
            Seq("geometry", "other"),
            Seq(pointWithSrid(4326), null),
            Seq(pointWithSrid(3857), null))
          val message = messages(intercept[Exception](writeVersion2(df, path)))
          assert(message.contains("SRID 4326") && message.contains("SRID 3857"))
        }
        assert(openWriters == before)
      }

      it("writes the row that ends the wait for a geometry as it is") {
        val schema = StructType(Seq(StructField("geometry", GeometryUDT())))
        val toUnsafe = UnsafeProjection.create(schema)
        def writtenAsGiven(
            geometries: Seq[Geometry],
            options: (String, String)*): Seq[Boolean] = {
          val rows = geometries.map { geometry =>
            val value = if (geometry == null) null else GeometryUDT().serialize(geometry)
            toUnsafe(new GenericInternalRow(Array[Any](value))).copy()
          }
          RecordingWriteSupport.rows.clear()
          withPath { path =>
            withWriter(path, schema, options.toMap, Some(classOf[RecordingWriteSupport])) {
              writer => rows.foreach(writer.write)
            }
            assert(sridsIn(path).size == geometries.size)
          }
          rows.zip(RecordingWriteSupport.rows).map { case (given, written) => given eq written }
        }
        // The row that shows the last geometry waited for opens the file, so only the row before
        // it is written from a copy
        assert(
          writtenAsGiven(Seq(null, pointWithSrid(3857), pointWithSrid(3857))) ==
            Seq(false, true, true))
        // The row that reaches the limit opens the file too, however large it is
        assert(
          writtenAsGiven(Seq(null, null), "geoparquet.srid.lookahead.bytes" -> "1") ==
            Seq(true, true))
      }

      describe("with rows that are not UnsafeRows") {
        val schema =
          StructType(
            Seq(StructField("geometry", GeometryUDT()), StructField("payload", BinaryType)))
        def serialized(srid: Int): Any = GeometryUDT().serialize(pointWithSrid(srid))

        it("holds back a copy that shares no values with the row") {
          val payload = Array[Byte](1)
          val row = new GenericInternalRow(Array[Any](null, payload))
          withPath { path =>
            withWriter(path, schema) { writer =>
              writer.write(row)
              // The caller uses the row and its values again for the next one
              payload(0) = 2
              row.update(0, serialized(3857))
              writer.write(row)
            }
            val payloads = sparkSession.read
              .format("geoparquet")
              .load(path)
              .collect()
              .map(_.getAs[Array[Byte]]("payload").toSeq)
              .toSeq
            assert(payloads == Seq(Seq[Byte](1), Seq[Byte](2)))
            assert(sridsIn(path) == Seq(None, Some(3857)))
          }
        }

        it("counts the bytes of a row held back against the limit") {
          withPath { path =>
            val error = intercept[Exception] {
              withWriter(path, schema, Map("geoparquet.srid.lookahead.bytes" -> "4096")) {
                writer =>
                  // This row alone reaches the limit, so the file opens with an unknown CRS
                  writer.write(new GenericInternalRow(Array[Any](null, new Array[Byte](8192))))
                  writer.write(
                    new GenericInternalRow(Array[Any](serialized(3857), Array[Byte](1))))
              }
            }
            assert(messages(error).contains("had no geometry in the first rows"))
          }
        }
      }

      it("writes geospatial statistics for the row groups of a GEOMETRY column") {
        withPath { path =>
          sparkSession
            .sql("SELECT ST_Point(x, x + 10) AS geometry FROM VALUES (1.0), (2.0), (3.0) AS t(x)")
            .coalesce(1)
            .write
            .format("geoparquet")
            .option("geoparquet.version", "2.0.0")
            .option("geoparquet.crs", "null")
            .save(path)
          val chunk = footer(path).getBlocks.get(0).getColumns.get(0)
          val statistics =
            classOf[ColumnChunkMetaData].getMethod("getGeospatialStatistics").invoke(chunk)
          assert(statistics != null)
          val box = statistics.getClass.getMethod("getBoundingBox").invoke(statistics)
          val bounds = Seq("getXMin", "getYMin", "getXMax", "getYMax").map { name =>
            box.getClass.getMethod(name).invoke(box).asInstanceOf[Double]
          }
          assert(bounds == Seq(1.0, 11.0, 3.0, 13.0))
        }
      }

      it("writes ISO WKB for a geometry with a Z coordinate") {
        withPath { path =>
          sparkSession
            .sql("SELECT ST_PointZ(1.0, 2.0, 3.0) AS geometry")
            .coalesce(1)
            .write
            .format("geoparquet")
            .option("geoparquet.version", "2.0.0")
            .option("geoparquet.crs", "null")
            .save(path)
          val reader = ParquetReader.builder(new GroupReadSupport(), writtenFile(path)).build()
          val wkb =
            try reader.read().getBinary("geometry", 0).getBytes
            finally reader.close()
          val order = if (wkb(0) == 1) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
          assert(ByteBuffer.wrap(wkb).order(order).getInt(1) == 1001)
          val column = geoMetadata(footer(path)) \ "columns" \ "geometry"
          assert(column \ "geometry_types" == JArray(List(JString("Point Z"))))
        }
      }

      it("keeps a plain binary column and its covering column for version 1.1.0") {
        withPath { path =>
          point(4326).write.format("geoparquet").option("geoparquet.version", "1.1.0").save(path)
          val written = footer(path)
          assert(geometryColumn(written).isEmpty)
          assert(written.getFileMetaData.getSchema.containsField("geometry_bbox"))
          assert(geoMetadata(written) \ "version" == JString("1.1.0"))
        }
      }
    } else {
      it("rejects GeoParquet 2.0.0 on a Parquet library without geospatial types") {
        withPath { path =>
          val error = intercept[Exception] {
            point(4326).write
              .format("geoparquet")
              .option("geoparquet.version", "2.0.0")
              .save(path)
          }
          assert(messages(error).contains("Spark 4.1"))
        }
      }
    }
  }
}

/** Keeps the rows that the Parquet writer gets, to tell a row from a copy of it. */
class RecordingWriteSupport extends GeoParquetWriteSupport {
  override def write(row: InternalRow): Unit = {
    RecordingWriteSupport.rows += row
    super.write(row)
  }
}

object RecordingWriteSupport {
  val rows: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer.empty
}
