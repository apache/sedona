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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.geoparquet.{GeographyRowGroupStatistics, ParquetGeospatialTypes}

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * The Parquet library collects no statistics for a GEOGRAPHY column. Sedona gives the row groups
 * of a GeoParquet 2 file the bounding box and the geospatial types of their geographies itself.
 */
class GeoParquetGeographyStatisticsSuite extends TestBaseScala {

  /** The bounding box of a row group, with `xmin` above `xmax` across the antimeridian. */
  private case class Box(xmin: Double, xmax: Double, ymin: Double, ymax: Double) {
    def contains(x: Double, y: Double): Boolean = {
      val inX = if (xmin <= xmax) x >= xmin && x <= xmax else x >= xmin || x <= xmax
      inX && y >= ymin && y <= ymax
    }
  }

  /** The geospatial statistics of a column in one row group. */
  private case class RowGroup(rows: Long, box: Option[Box], types: Set[Int])

  private def withDirectory(run: File => Unit): Unit = {
    val directory = Files.createTempDirectory("geoparquet-geography-statistics").toFile
    try run(directory)
    finally FileUtils.deleteDirectory(directory)
  }

  private def writeVersion2(df: DataFrame, path: String, options: (String, String)*): Unit =
    df.coalesce(1)
      .write
      .format("geoparquet")
      .option("geoparquet.version", "2.0.0")
      .options(options.toMap)
      .save(path)

  /** The statistics that the footer of the one file under `path` has for `column`. */
  private def rowGroups(path: String, column: String = "geography"): Seq[RowGroup] = {
    val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
    assert(files.length == 1)
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(new Path(files.head.getPath), new Configuration()))
    val blocks =
      try reader.getFooter.getBlocks.asScala.toSeq
      finally reader.close()
    // Sedona also compiles against Parquet libraries without geospatial statistics
    val statistics = classOf[ColumnChunkMetaData].getMethod("getGeospatialStatistics")
    def number(box: AnyRef, name: String): Double =
      box.getClass.getMethod(name).invoke(box).asInstanceOf[Double]
    blocks.map { block =>
      val chunk = block.getColumns.asScala.find(_.getPath.toDotString == column).get
      val found = Option(statistics.invoke(chunk))
      val box = found
        .flatMap(value => Option(value.getClass.getMethod("getBoundingBox").invoke(value)))
        .filter(box => box.getClass.getMethod("isValid").invoke(box).asInstanceOf[Boolean])
        .filterNot(box => box.getClass.getMethod("isXYEmpty").invoke(box).asInstanceOf[Boolean])
        .map(box =>
          Box(
            number(box, "getXMin"),
            number(box, "getXMax"),
            number(box, "getYMin"),
            number(box, "getYMax")))
      val types = found
        .flatMap(value => Option(value.getClass.getMethod("getGeospatialTypes").invoke(value)))
        .map(types =>
          types.getClass
            .getMethod("getTypes")
            .invoke(types)
            .asInstanceOf[java.util.Set[Integer]]
            .asScala
            .map(_.intValue)
            .toSet)
        .getOrElse(Set.empty[Int])
      RowGroup(block.getRowCount, box, types)
    }
  }

  private def geographies(wkts: String*): DataFrame = {
    val rows = wkts.zipWithIndex.map { case (wkt, id) =>
      if (wkt == null) s"SELECT $id AS id, ST_GeogFromWKT(NULL, 4326) AS geography"
      else s"SELECT $id AS id, ST_GeogFromWKT('$wkt', 4326) AS geography"
    }
    sparkSession.sql(rows.mkString(" UNION ALL "))
  }

  /** The one row group of the file that holds `wkts` in its column `geography`. */
  private def rowGroupOf(wkts: String*): RowGroup = {
    var found: Option[RowGroup] = None
    withDirectory { directory =>
      val path = new File(directory, "data").getPath
      writeVersion2(geographies(wkts: _*), path)
      val all = rowGroups(path)
      assert(all.size == 1)
      found = all.headOption
    }
    found.get
  }

  private def near(actual: Double, expected: Double): Boolean =
    math.abs(actual - expected) < 1e-9

  describe("Row group statistics of geography columns in GeoParquet 2 files") {
    if (ParquetGeospatialTypes.supported) {
      it("gives a row group the bounds and the type of its points, as they are written") {
        val group = rowGroupOf("POINT (10 20)", "POINT (-30 5)", "POINT (15 -40)")
        assert(group.rows == 3)
        // The coordinates of points are in the box as they are, without a margin
        assert(group.box.contains(Box(-30, 15, -40, 20)))
        assert(group.types == Set(1))
      }

      it("reads the file back as it is read without the statistics") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(geographies("POINT (10 20)", null, "LINESTRING (0 0, 10 10)"), path)
          val df = sparkSession.read.format("geoparquet").load(path)
          val ewkt =
            df.orderBy("id").selectExpr("ST_AsEWKT(geography)").collect().map(_.getString(0))
          assert(
            ewkt.toSeq == Seq(
              "SRID=4326; POINT (10 20)",
              null,
              "SRID=4326; LINESTRING (0 0, 10 10)"))
        }
      }

      it("follows an edge to the north of its end points") {
        // The geodesic between two points on the 50th parallel that are 90 degrees apart reaches
        // atan(tan(50) / cos(45)), about 59.32 degrees north
        val box = rowGroupOf("LINESTRING (0 50, 90 50)").box.get
        val north =
          math.toDegrees(math.atan(math.tan(math.toRadians(50)) / math.cos(math.Pi / 4)))
        assert(box.ymax >= north && box.ymax < north + 1e-6)
        assert(box.ymin <= 50 && near(box.ymin, 50))
        assert(box.xmin <= 0 && near(box.xmin, 0))
        assert(box.xmax >= 90 && near(box.xmax, 90))
      }

      it("reaches the pole and all longitudes for a polygon around the pole") {
        val group = rowGroupOf("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))")
        val box = group.box.get
        assert(box.ymax == 90)
        assert(box.xmin == -180 && box.xmax == 180)
        assert(box.ymin <= 80 && near(box.ymin, 80))
        assert(group.types == Set(3))
      }

      it("writes a box across the antimeridian with xmin above xmax") {
        val line = rowGroupOf("LINESTRING (170 10, -170 20)").box.get
        assert(line.xmin > line.xmax)
        assert(line.xmin <= 170 && near(line.xmin, 170))
        assert(line.xmax >= -170 && near(line.xmax, -170))
        assert(line.contains(180, 15) && line.contains(-180, 15) && !line.contains(0, 15))

        val points = rowGroupOf("POINT (179 1)", "POINT (-179 2)").box.get
        assert(points == Box(179, -179, 1, 2))
      }

      it("has a point on the antimeridian in the box as longitude 180 and as -180") {
        Seq("POINT (180 5)", "POINT (-180 5)").foreach { wkt =>
          val box = rowGroupOf(wkt, "POINT (175 6)").box.get
          assert(box.contains(180, 5) && box.contains(-180, 5) && box.contains(175, 6))
          assert(!box.contains(0, 5))
        }
      }

      it("covers the members of a collection and of multi geographies") {
        val collection =
          rowGroupOf("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))")
        assert(collection.types == Set(7))
        assert(collection.box.get.contains(1, 2) && collection.box.get.contains(5, 6))
        assert(!collection.box.get.contains(7, 6))

        val multi = rowGroupOf("MULTIPOINT ((1 2), (-3 4))", "MULTILINESTRING ((5 6, 7 8))")
        assert(multi.types == Set(4, 5))
        assert(multi.box.get.contains(-3, 4) && multi.box.get.contains(7, 8))
      }

      it("gives each row group the bounds of its own rows") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql("""SELECT id, ST_GeogFromWKT(
                |  concat('POINT (', CAST(id * 0.25 AS DOUBLE), ' ', CAST(id * 0.125 AS DOUBLE), ')'),
                |  4326) AS geography
                |FROM range(600)""".stripMargin),
            path,
            "parquet.block.size" -> "4096",
            "parquet.page.size" -> "512")
          val groups = rowGroups(path)
          assert(groups.size > 1)
          assert(groups.map(_.rows).sum == 600)
          var first = 0L
          groups.foreach { group =>
            val last = first + group.rows - 1
            assert(
              group.box.contains(Box(first * 0.25, last * 0.25, first * 0.125, last * 0.125)))
            assert(group.types == Set(1))
            first = last + 1
          }
        }
      }

      it("writes the type and no box for empty geographies, and nothing for a row group of nulls") {
        val empty = rowGroupOf("POINT EMPTY", "LINESTRING EMPTY")
        assert(empty.box.isEmpty)
        assert(empty.types == Set(1, 2))

        val nulls = rowGroupOf(null, null)
        assert(nulls == RowGroup(2, None, Set.empty))
      }

      it("writes the type code of a geography with a Z coordinate, and its box of X and Y") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          // POINT Z (1 2 3)
          val pointZ = "01E9030000000000000000F03F00000000000000400000000000000840"
          writeVersion2(
            sparkSession.sql(s"SELECT 1 AS id, ST_GeogFromWKB(unhex('$pointZ')) AS geography"),
            path)
          assert(rowGroups(path) == Seq(RowGroup(1, Some(Box(1, 1, 2, 2)), Set(1001))))
        }
      }

      it("covers every vertex of lines and polygons, wherever they are") {
        val random = new Random(42)
        def ordinate(range: Double): Double =
          math.round((random.nextDouble() - 0.5) * 2 * range * 1e6) / 1e6
        val wkts = Seq.fill(60) {
          val lon = ordinate(180)
          val lat = ordinate(85)
          def vertex(): (Double, Double) = {
            val x = lon + ordinate(4)
            (if (x > 180) x - 360 else if (x < -180) x + 360 else x, lat + ordinate(4))
          }
          val vertices = Seq.fill(3)(vertex())
          def text(points: Seq[(Double, Double)]): String =
            points.map { case (x, y) => s"$x $y" }.mkString(", ")
          val wkt =
            if (random.nextBoolean()) s"LINESTRING (${text(vertices)})"
            else s"POLYGON ((${text(vertices :+ vertices.head)}))"
          wkt -> vertices
        }
        wkts.grouped(10).foreach { some =>
          val box = rowGroupOf(some.map(_._1): _*).box.get
          some.foreach { case (wkt, vertices) =>
            vertices.foreach { case (x, y) =>
              assert(box.contains(x, y), s"$box does not have ($x $y) of $wkt")
            }
          }
        }
      }

      it("leaves the statistics of a geometry column to the Parquet library") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            sparkSession.sql("""SELECT 1 AS id, ST_Point(1, 2) AS geometry,
                |ST_GeogFromWKT('POINT (3 4)', 4326) AS geography""".stripMargin),
            path)
          assert(rowGroups(path, "geometry") == Seq(RowGroup(1, Some(Box(1, 1, 2, 2)), Set(1))))
          assert(rowGroups(path, "geography") == Seq(RowGroup(1, Some(Box(3, 3, 4, 4)), Set(1))))
        }
      }

      it("writes no statistics for a column whose Parquet statistics are turned off") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          writeVersion2(
            geographies("POINT (10 20)"),
            path,
            "parquet.column.statistics.enabled#geography" -> "false")
          assert(rowGroups(path) == Seq(RowGroup(1, None, Set.empty)))
        }
      }

      it("finds no row groups in a writer of another kind, and writes without statistics then") {
        assert(GeographyRowGroupStatistics.rowGroupsOf(new Object).isEmpty)
      }
    }
  }
}
