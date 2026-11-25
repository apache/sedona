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
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetRecordReader}
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnPath}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.geoparquet.{GeoParquetFileFormat, GeoParquetRowGroupFilter, GeoParquetSpatialFilter, ParquetGeospatialTypes}
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter.{AndFilter, GeographyLeafFilter, OrFilter}

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * A filter on a geography column of a GeoParquet 2 file skips the row groups whose bounding box,
 * in the geospatial statistics of the row group, the geography of the query is outside of.
 */
class GeoParquetGeographyRowGroupPruningSuite extends TestBaseScala {

  private val PUSH_DOWN = "spark.sedona.geoparquet.spatialFilterPushDown"

  private def withDirectory(run: File => Unit): Unit = {
    val directory = Files.createTempDirectory("geoparquet-geography-pruning").toFile
    try run(directory)
    finally FileUtils.deleteDirectory(directory)
  }

  private def withConf(settings: (String, String)*)(run: => Unit): Unit = {
    val before = settings.map { case (key, _) => key -> sparkSession.conf.getOption(key) }
    settings.foreach { case (key, value) => sparkSession.conf.set(key, value) }
    try run
    finally
      before.foreach {
        case (key, Some(value)) => sparkSession.conf.set(key, value)
        case (key, None) => sparkSession.conf.unset(key)
      }
  }

  /** A file of small row groups, in the order of `df`. */
  private def write(df: DataFrame, path: String, options: (String, String)*): Unit =
    df.coalesce(1)
      .write
      .format("geoparquet")
      .option("geoparquet.version", "2.0.0")
      .option("parquet.block.size", "4096")
      .option("parquet.page.size", "512")
      .options(options.toMap)
      .save(path)

  private val ROWS = 3000L

  /** Points from (0 0) towards the north east, a twentieth of a degree of longitude apart. */
  private def diagonal: DataFrame =
    sparkSession.sql(s"""SELECT id, ST_GeogFromWKT(
        |  concat('POINT (', CAST(id * 0.05 AS DOUBLE), ' ', CAST(id * 0.025 AS DOUBLE), ')'),
        |  4326) AS geography
        |FROM range($ROWS)""".stripMargin)

  private def blocks(path: String): Seq[BlockMetaData] = {
    val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
    assert(files.length == 1)
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(new Path(files.head.getPath), new Configuration()))
    try reader.getFooter.getBlocks.asScala.toSeq
    finally reader.close()
  }

  private def read(path: String, condition: String): DataFrame =
    sparkSession.read.format("geoparquet").load(path).where(condition)

  private def scan(df: DataFrame): FileSourceScanExec = {
    def find(plan: SparkPlan): Option[FileSourceScanExec] = plan match {
      case adaptive: AdaptiveSparkPlanExec => find(adaptive.executedPlan)
      case scan: FileSourceScanExec => Some(scan)
      case other => other.children.flatMap(find).headOption
    }
    find(df.queryExecution.executedPlan).get
  }

  private def pushed(df: DataFrame): Option[GeoParquetSpatialFilter] =
    scan(df).relation.fileFormat.asInstanceOf[GeoParquetFileFormat].spatialFilter

  /** The rows of `df`, by their id, and the number of rows that its scan has read for them. */
  private def run(df: DataFrame): (Seq[Long], Long) = {
    val projected = df.select("id")
    val ids = projected.collect().map(_.getLong(0)).sorted.toSeq
    (ids, scan(projected).metrics("numOutputRows").value)
  }

  /** The ids that `condition` keeps when no filter is pushed down. */
  private def expected(path: String, condition: String): Seq[Long] = {
    var ids: Seq[Long] = Nil
    withConf(PUSH_DOWN -> "false") {
      val df = read(path, condition)
      assert(pushed(df).isEmpty)
      ids = run(df)._1
    }
    ids
  }

  /**
   * Reads the rows of `condition` with and without the filter pushed down, which have to be the
   * same rows, and gives the number of rows that the scan with the filter has read.
   */
  private def scanned(path: String, condition: String): Long = {
    val df = read(path, condition)
    assert(pushed(df).isDefined, condition)
    val (ids, rows) = run(df)
    assert(ids == expected(path, condition), condition)
    assert(rows >= ids.size)
    rows
  }

  private val window = "ST_GeogFromWKT('POLYGON ((50 20, 60 20, 60 35, 50 35, 50 20))', 4326)"
  private val far =
    "ST_GeogFromWKT('POLYGON ((-60 -20, -50 -20, -50 -10, -60 -10, -60 -20))', 4326)"

  describe("Row group pruning for geography columns of GeoParquet 2 files") {
    if (ParquetGeospatialTypes.supported) {
      it("reads the row groups whose box the geography of the query reaches, and no other") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val groups = blocks(path)
          assert(groups.size > 4)

          val df = read(path, s"ST_Intersects(geography, $window)")
          val filter = pushed(df).get
          val kept = GeoParquetRowGroupFilter.select(groups, filter)
          assert(kept.nonEmpty && kept.size < groups.size / 2)
          val (ids, rows) = run(df)
          // Between the longitudes 50 and 60 are the ids 1001 to 1199
          assert(ids == (1001L to 1199L))
          assert(rows == kept.map(_.getRowCount).sum)
          assert(ids == expected(path, s"ST_Intersects(geography, $window)"))
        }
      }

      it("reads no row group for a query that is outside of all of them") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          assert(scanned(path, s"ST_Intersects(geography, $far)") == 0)
        }
      }

      it("reads every row group for a query that reaches all of them") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val all =
            "ST_GeogFromWKT('POLYGON ((-10 -10, 170 -10, 170 80, -10 80, -10 -10))', 4326)"
          assert(scanned(path, s"ST_Intersects(geography, $all)") == ROWS)
        }
      }

      it("prunes for every predicate between a geography column and a geography") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val point = "ST_GeogFromWKT('POINT (50 25)', 4326)"
          Seq(
            s"ST_Intersects(geography, $window)",
            s"ST_Intersects($window, geography)",
            s"ST_Contains($window, geography)",
            s"ST_Contains(geography, $point)",
            s"ST_Within(geography, $window)",
            s"ST_Within($point, geography)",
            s"ST_Equals(geography, $point)",
            s"ST_Equals($point, geography)",
            s"ST_DWithin(geography, $point, 100000.0)",
            s"ST_DWithin($point, geography, 100000.0)").foreach { condition =>
            val rows = scanned(path, condition)
            assert(rows > 0 && rows < ROWS / 2, condition)
          }
        }
      }

      it("reads the row groups within the distance of ST_DWithin") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val point = "ST_GeogFromWKT('POINT (50 25)', 4326)"
          val near = scanned(path, s"ST_DWithin(geography, $point, 1000.0)")
          val wide = scanned(path, s"ST_DWithin(geography, $point, 2000000.0)")
          val everywhere = scanned(path, s"ST_DWithin(geography, $point, 30000000.0)")
          assert(near > 0 && near < wide && wide < everywhere && everywhere == ROWS)
          // 2000 km around (50 25) is more than 10 degrees of longitude to either side
          assert(expected(path, s"ST_DWithin(geography, $point, 2000000.0)").size > 400)
          // Nothing is within a distance below zero
          assert(read(path, s"ST_DWithin(geography, $point, -1.0)").count() == 0)
        }
      }

      it("prunes across the antimeridian") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          // Longitudes from 150 eastwards to -150, and then the same again at other latitudes
          write(
            sparkSession.sql(s"""SELECT id, ST_GeogFromWKT(concat('POINT (',
                |  CAST(CASE WHEN id % 1500 < 750 THEN 150 + (id % 1500) * 0.04
                |       ELSE -180 + (id % 1500 - 750) * 0.04 END AS DOUBLE),
                |  ' ', CAST(id * 0.02 AS DOUBLE), ')'), 4326) AS geography
                |FROM range($ROWS)""".stripMargin),
            path)
          val across =
            "ST_GeogFromWKT('POLYGON ((178 0, -178 0, -178 80, 178 80, 178 0))', 4326)"
          val rows = scanned(path, s"ST_Intersects(geography, $across)")
          assert(rows > 0 && rows < ROWS)
          // Two degrees to either side of the antimeridian, twice
          val found = expected(path, s"ST_Intersects(geography, $across)").size
          assert(found > 190 && found < 205)
          val on = "ST_GeogFromWKT('LINESTRING (180 -10, 180 80)', 4326)"
          assert(expected(path, s"ST_DWithin(geography, $on, 1.0)") == Seq(750L, 2250L))
          assert(scanned(path, s"ST_DWithin(geography, $on, 1.0)") < ROWS)
        }
      }

      it("reads the edges of the geographies of the query, not only their vertices") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          // Points on the meridian 45, from 40 to 70 degrees north
          write(
            sparkSession.sql(s"""SELECT id, ST_GeogFromWKT(
                |  concat('POINT (45 ', CAST(40 + id * 0.01 AS DOUBLE), ')'), 4326) AS geography
                |FROM range($ROWS)""".stripMargin),
            path)
          // The edge between two points on the 50th parallel goes north of 59 degrees
          val arc = "ST_GeogFromWKT('LINESTRING (0 50, 90 50)', 4326)"
          val rows = scanned(path, s"ST_DWithin(geography, $arc, 5000.0)")
          // The ids of the latitudes around 59.32 degrees
          val ids = expected(path, s"ST_DWithin(geography, $arc, 5000.0)")
          assert(ids.nonEmpty && ids.forall(id => id > 1920 && id < 1945))
          assert(rows > 0 && rows < ROWS)
        }
      }

      it("reads every row group of a file without the statistics") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path, "parquet.column.statistics.enabled" -> "false")
          assert(scanned(path, s"ST_Intersects(geography, $window)") == ROWS)
          assert(scanned(path, s"ST_Intersects(geography, $far)") == ROWS)
        }
      }

      it("reads a row group of nulls as before") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(
            sparkSession.sql(s"""SELECT id, CASE
                |  WHEN id < 1000 THEN ST_GeogFromWKT(NULL, 4326)
                |  ELSE ST_GeogFromWKT(concat('POINT (', CAST(id * 0.05 AS DOUBLE), ' 10)'), 4326)
                |  END AS geography
                |FROM range($ROWS)""".stripMargin),
            path)
          val some = "ST_GeogFromWKT('POLYGON ((120 0, 130 0, 130 20, 120 20, 120 0))', 4326)"
          val rows = scanned(path, s"ST_Intersects(geography, $some)")
          assert(expected(path, s"ST_Intersects(geography, $some)") == (2401L to 2599L))
          assert(rows < ROWS)
        }
      }

      it("reads the row groups of each split of a file that is read in several splits") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val groups = blocks(path)
          withConf(
            "spark.sql.files.maxPartitionBytes" -> "3000",
            "spark.sql.files.openCostInBytes" -> "0") {
            val df = read(path, s"ST_Intersects(geography, $window)")
            assert(df.rdd.getNumPartitions > 2)
            val kept = GeoParquetRowGroupFilter.select(groups, pushed(df).get)
            val (ids, rows) = run(df)
            assert(ids == (1001L to 1199L))
            assert(rows == kept.map(_.getRowCount).sum)
          }
        }
      }

      it("prunes with filters on other columns and with filters joined by AND and OR") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val other = "ST_GeogFromWKT('POLYGON ((100 45, 110 45, 110 60, 100 60, 100 45))', 4326)"
          val both = scanned(
            path,
            s"ST_Intersects(geography, $window) OR ST_Intersects(geography, $other)")
          val one = scanned(path, s"ST_Intersects(geography, $window)")
          assert(both > one && both < ROWS)
          assert(
            scanned(
              path,
              s"ST_Intersects(geography, $window) AND ST_Intersects(geography, $other)") == 0)
          assert(scanned(path, s"ST_Intersects(geography, $window) AND id > 1100") <= one)
          assert(
            expected(
              path,
              s"ST_Intersects(geography, $window) AND id > 1100") == (1101L to 1199L))
          // A filter that is not pushed down keeps the row groups of the other side of an OR
          assert(pushed(read(path, s"ST_Intersects(geography, $window) OR id = 5")).isEmpty)
        }
      }

      it("keeps the rows that the predicates keep, for geographies and queries anywhere") {
        withDirectory { directory =>
          val random = new Random(31)
          def wrap(longitude: Double): Double =
            if (longitude > 180) longitude - 360
            else if (longitude < -180) longitude + 360
            else longitude
          def text(points: Seq[(Double, Double)]): String =
            points.map { case (x, y) => s"${wrap(x)} $y" }.mkString("(", ", ", ")")
          // Points, lines and triangles, in the order of the cell of 30 degrees that they are in
          val geographies = Seq
            .fill(ROWS.toInt) {
              val (x, y) = (random.nextDouble() * 360 - 180, random.nextDouble() * 160 - 80)
              val (dx, dy) = (random.nextDouble() * 4 + 0.1, random.nextDouble() * 4 + 0.1)
              val wkt = random.nextInt(3) match {
                case 0 => s"POINT ${text(Seq((x, y)))}"
                case 1 => s"LINESTRING ${text(Seq((x, y), (x + dx, y + dy)))}"
                case _ => s"POLYGON (${text(Seq((x, y), (x + dx, y), (x, y + dy), (x, y)))})"
              }
              ((math.floor(x / 30), math.floor(y / 30)), wkt)
            }
            .sortBy(_._1)
            .map(_._2)
            .zipWithIndex
            .map { case (wkt, id) => Row(id.toLong, wkt) }
          val schema =
            StructType(Seq(StructField("id", LongType), StructField("wkt", StringType)))
          val path = new File(directory, "data").getPath
          write(
            sparkSession
              .createDataFrame(sparkSession.sparkContext.parallelize(geographies, 1), schema)
              .selectExpr("id", "ST_GeogFromWKT(wkt, 4326) AS geography"),
            path)

          var read = 0L
          var matched = 0L
          (1 to 40).foreach { _ =>
            val (x, y) = (random.nextDouble() * 360 - 180, random.nextDouble() * 140 - 70)
            val (dx, dy) = (random.nextDouble() * 15 + 1, random.nextDouble() * 15 + 1)
            val box = "POLYGON (" +
              text(Seq((x, y), (x + dx, y), (x + dx, y + dy), (x, y + dy), (x, y))) + ")"
            val query = random.nextInt(3) match {
              case 0 => s"ST_GeogFromWKT('POINT ${text(Seq((x, y)))}', 4326)"
              case 1 =>
                s"ST_GeogFromWKT('LINESTRING ${text(Seq((x, y), (x + dx, y + dy)))}', 4326)"
              case _ => s"ST_GeogFromWKT('$box', 4326)"
            }
            val meters = math.pow(10, random.nextDouble() * 4 + 2)
            val condition = random.nextInt(5) match {
              case 0 => s"ST_Intersects(geography, $query)"
              case 1 => s"ST_Within(geography, ST_GeogFromWKT('$box', 4326))"
              case 2 => s"ST_Contains(ST_GeogFromWKT('$box', 4326), geography)"
              case 3 => s"ST_DWithin(geography, $query, $meters)"
              case _ => s"ST_DWithin($query, geography, $meters)"
            }
            read += scanned(path, condition)
            matched += expected(path, condition).size
          }
          assert(matched > 0)
          // Less than a third of the rows is read
          assert(read < 40 * ROWS / 3)
        }
      }

      it("pushes no filter for a query without a box and for a distance that is no number") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val point = "ST_GeogFromWKT('POINT (50 25)', 4326)"
          Seq(
            "ST_Intersects(geography, ST_GeogFromWKT('POINT EMPTY', 4326))",
            s"ST_DWithin(geography, $point, CAST('NaN' AS DOUBLE))",
            s"ST_DWithin(geography, $point, id * 1.0)",
            "ST_Intersects(geography, geography)",
            s"NOT ST_Intersects(geography, $window)").foreach { condition =>
            val df = read(path, condition)
            assert(pushed(df).isEmpty, condition)
            assert(run(df)._1 == expected(path, condition), condition)
          }
        }
      }

      it("says in the plan which filter it has pushed down") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val filter = pushed(read(path, s"ST_Intersects(geography, $window)")).get
          assert(filter.isInstanceOf[GeographyLeafFilter])
          assert(filter.simpleString.startsWith("geography INTERSECTS GEOGRAPHY BOX("))
        }
      }
    }
  }

  describe("The row groups that a filter on a geography column keeps") {
    if (ParquetGeospatialTypes.supported) {
      it("keeps a row group without a box, and a column that the file does not have") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val groups = blocks(path)
          val nowhere = GeographyLeafFilter("geography", -60, -50, -20, -10)
          assert(GeoParquetRowGroupFilter.select(groups, nowhere).isEmpty)
          val missing = GeographyLeafFilter("other", -60, -50, -20, -10)
          assert(GeoParquetRowGroupFilter.select(groups, missing) == groups)
          assert(GeoParquetRowGroupFilter.select(groups, OrFilter(nowhere, missing)) == groups)
          assert(GeoParquetRowGroupFilter.select(groups, AndFilter(nowhere, missing)).isEmpty)

          val plain = new File(directory, "plain").getPath
          write(diagonal, plain, "parquet.column.statistics.enabled" -> "false")
          assert(GeoParquetRowGroupFilter.select(blocks(plain), nowhere).size == groups.size)
        }
      }

      it("reads the row groups that a split names, and the whole split when one is not found") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(diagonal, path)
          val groups = blocks(path)
          val file = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
          val whole = new FileSplit(new Path(file.getPath), 0, file.length(), Array.empty[String])
          val context = new TaskAttemptContextImpl(
            new Configuration(),
            new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0))
          def rows(starts: Long*): Long =
            GeoParquetFileFormat.openRowGroups(whole, starts.toArray, new Configuration()) {
              split =>
                val reader = new ParquetRecordReader[Group](new GroupReadSupport)
                try {
                  reader.initialize(split, context)
                  var count = 0L
                  while (reader.nextKeyValue()) count += 1
                  count
                } finally reader.close()
            }
          assert(rows(groups(1).getStartingPos) == groups(1).getRowCount)
          assert(
            rows(groups(1).getStartingPos, groups.last.getStartingPos) ==
              groups(1).getRowCount + groups.last.getRowCount)
          assert(rows(groups(1).getStartingPos + 1) == ROWS)
          assert(rows(groups(1).getStartingPos, groups(2).getStartingPos + 1) == ROWS)
        }
      }

      it("keeps the row groups of a geometry column, whose box is not one on the sphere") {
        withDirectory { directory =>
          val path = new File(directory, "data").getPath
          write(
            sparkSession.sql(
              s"SELECT id, ST_Point(id * 0.05, id * 0.025) AS geography FROM range($ROWS)"),
            path)
          val groups = blocks(path)
          val nowhere = GeographyLeafFilter("geography", -60, -50, -20, -10)
          assert(GeoParquetRowGroupFilter.select(groups, nowhere) == groups)
        }
      }
    }
  }
}
