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
import org.apache.sedona.sql.GeoParquetSpatialFilterPushDownSuite.generateTestData
import org.apache.sedona.sql.GeoParquetSpatialFilterPushDownSuite.readGeoParquetMetaDataMap
import org.apache.sedona.sql.GeoParquetSpatialFilterPushDownSuite.writeTestDataAsGeoParquet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetFileFormat
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetMetaData
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.scalatest.prop.TableDrivenPropertyChecks

import java.io.File
import java.nio.file.Files

class GeoParquetSpatialFilterPushDownSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val tempDir: String = Files.createTempDirectory("sedona_geoparquet_test_").toFile.getAbsolutePath
  val geoParquetDir: String = tempDir + "/geoparquet"
  var df: DataFrame = _
  var geoParquetDf: DataFrame = _
  var geoParquetMetaDataMap: Map[Int, Seq[GeoParquetMetaData]] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    df = generateTestData(sparkSession)
    writeTestDataAsGeoParquet(df, geoParquetDir)
    geoParquetDf = sparkSession.read.format("geoparquet").load(geoParquetDir)
    geoParquetMetaDataMap = readGeoParquetMetaDataMap(geoParquetDir)
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(tempDir))

  describe("GeoParquet spatial filter push down tests") {
    it("Push down ST_Contains") {
      testFilter("ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(1))
      testFilter("ST_Contains(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)", Seq(0))
      testFilter("ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'), geom)", Seq.empty)
      testFilter("ST_Contains(geom, ST_GeomFromText('POINT (15 -15)'))", Seq(3))
      testFilter("ST_Contains(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))", Seq(3))
      testFilter("ST_Contains(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))", Seq.empty)
    }

    it("Push down ST_Covers") {
      testFilter("ST_Covers(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(1))
      testFilter("ST_Covers(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)", Seq(0))
      testFilter("ST_Covers(ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'), geom)", Seq.empty)
      testFilter("ST_Covers(geom, ST_GeomFromText('POINT (15 -15)'))", Seq(3))
      testFilter("ST_Covers(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))", Seq(3))
      testFilter("ST_Covers(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))", Seq.empty)
    }

    it("Push down ST_Within") {
      testFilter("ST_Within(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))", Seq(1))
      testFilter("ST_Within(geom, ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'))", Seq(0))
      testFilter("ST_Within(geom, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))", Seq.empty)
      testFilter("ST_Within(ST_GeomFromText('POINT (15 -15)'), geom)", Seq(3))
      testFilter("ST_Within(ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'), geom)", Seq(3))
      testFilter("ST_Within(ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'), geom)", Seq.empty)
    }

    it("Push down ST_CoveredBy") {
      testFilter("ST_CoveredBy(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))", Seq(1))
      testFilter("ST_CoveredBy(geom, ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'))", Seq(0))
      testFilter("ST_CoveredBy(geom, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))", Seq.empty)
      testFilter("ST_CoveredBy(ST_GeomFromText('POINT (15 -15)'), geom)", Seq(3))
      testFilter("ST_CoveredBy(ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'), geom)", Seq(3))
      testFilter("ST_CoveredBy(ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'), geom)", Seq.empty)
    }

    it("Push down ST_Intersects") {
      testFilter("ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(1))
      testFilter("ST_Intersects(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)", Seq(0))
      testFilter("ST_Intersects(geom, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))", Seq.empty)
      testFilter("ST_Intersects(geom, ST_GeomFromText('POINT (15 -15)'))", Seq(3))
      testFilter("ST_Intersects(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))", Seq(3))
      testFilter("ST_Intersects(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))", Seq(3))
      testFilter("ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))'))", Seq(1, 3))
    }

    it("Push down ST_Equals") {
      testFilter("ST_Equals(geom, ST_GeomFromText('POLYGON ((-16 -16, -16 -14, -14 -14, -14 -16, -16 -16))'))", Seq(2))
      testFilter("ST_Equals(geom, ST_GeomFromText('POINT (-15 -15)'))", Seq(2))
      testFilter("ST_Equals(geom, ST_GeomFromText('POINT (-16 -16)'))", Seq(2))
      testFilter("ST_Equals(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))", Seq.empty)
    }

    forAll(Table("<", "<=")) { op =>
      it(s"Push down ST_Distance $op d") {
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 1", Seq.empty)
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 5", Seq.empty)
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (3 4)')) $op 1", Seq(1))
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 7.1", Seq(0, 1, 2, 3))
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (-5 -5)')) $op 1", Seq(2))
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')) $op 2", Seq.empty)
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')) $op 3", Seq(0, 1, 2, 3))
        testFilter(s"ST_Distance(geom, ST_GeomFromText('LINESTRING (17 17, 18 18)')) $op 1", Seq(1))
      }
    }

    it("Push down And(filters...)") {
      testFilter("ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))')) AND ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(1))
      testFilter("ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))')) AND ST_Intersects(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))", Seq(3))
    }

    it("Push down Or(filters...)") {
      testFilter("ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom) OR ST_Intersects(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)", Seq(0, 1))
      testFilter("ST_Distance(geom, ST_GeomFromText('POINT (-5 -5)')) <= 1 OR ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(1, 2))
    }

    it("Ignore negated spatial filters") {
      testFilter("NOT ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(0, 1, 2, 3))
      testFilter("ST_Contains(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))')) AND NOT ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(3))
      testFilter("ST_Contains(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))')) OR NOT ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)", Seq(0, 1, 2, 3))
    }

    it("Mixed spatial filter with other filter") {
      testFilter("id < 10 AND ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))'))", Seq(1, 3))
    }
  }

  /**
   * Test filter push down using specified query condition, and verify if the pushed down filter prunes regions as
   * expected. We'll also verify the correctness of query results.
   * @param condition SQL query condition
   * @param expectedPreservedRegions Regions that should be preserved after filter push down
   */
  private def testFilter(condition: String, expectedPreservedRegions: Seq[Int]): Unit = {
    val dfFiltered = geoParquetDf.where(condition)
    val preservedRegions = getPushedDownSpatialFilter(dfFiltered) match {
      case Some(spatialFilter) => resolvePreservedRegions(spatialFilter)
      case None => (0 until 4)
    }
    assert(expectedPreservedRegions == preservedRegions)
    val expectedResult = df.where(condition).orderBy("region", "id").select("region", "id").collect()
    val actualResult = dfFiltered.orderBy("region", "id").select("region", "id").collect()
    assert(expectedResult sameElements actualResult)
  }

  private def getPushedDownSpatialFilter(df: DataFrame): Option[GeoParquetSpatialFilter] = {
    val executedPlan = df.queryExecution.executedPlan
    val fileSourceScanExec = executedPlan.find(_.isInstanceOf[FileSourceScanExec])
    assert(fileSourceScanExec.isDefined)
    val fileFormat = fileSourceScanExec.get.asInstanceOf[FileSourceScanExec].relation.fileFormat
    assert(fileFormat.isInstanceOf[GeoParquetFileFormat])
    fileFormat.asInstanceOf[GeoParquetFileFormat].spatialFilter
  }

  private def resolvePreservedRegions(spatialFilter: GeoParquetSpatialFilter): Seq[Int] = {
    geoParquetMetaDataMap.filter { case (_, metaDataList) =>
      metaDataList.exists(metadata => spatialFilter.evaluate(metadata.columns))
    }.keys.toSeq
  }
}

object GeoParquetSpatialFilterPushDownSuite {
  case class TestDataItem(id: Int, region: Int, geom: Geometry)

  /**
   * Generate test data centered at (0, 0). The entire dataset was divided into 4 quadrants, each with a unique
   * region ID. The dataset contains 4 points and 4 polygons in each quadrant.
   * @param sparkSession SparkSession object
   * @return DataFrame containing test data
   */
  def generateTestData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val regionCenters = Seq((-10, 10), (10, 10), (-10, -10), (10, -10))
    val testData = regionCenters.zipWithIndex.flatMap { case ((x, y), i) => generateTestDataForRegion(i, x, y) }
    testData.toDF()
  }

  private def generateTestDataForRegion(region: Int, centerX: Double, centerY: Double) = {
    val factory = new GeometryFactory()
    val points = Seq(
      factory.createPoint(new Coordinate(centerX - 5, centerY + 5)),
      factory.createPoint(new Coordinate(centerX + 5, centerY + 5)),
      factory.createPoint(new Coordinate(centerX - 5, centerY - 5)),
      factory.createPoint(new Coordinate(centerX + 5, centerY - 5))
    )
    val polygons = points.map { p =>
      val envelope = p.getEnvelopeInternal
      envelope.expandBy(1)
      factory.toGeometry(envelope)
    }
    (points ++ polygons).zipWithIndex.map { case (g, i) => TestDataItem(i, region, g) }
  }

  /**
   * Write the test dataframe as GeoParquet files. Each region is written to a separate file. We'll test spatial
   * filter push down by examining which regions were preserved/pruned by evaluating the pushed down spatial filters
   * @param testData dataframe containing test data
   * @param path path to write GeoParquet files
   */
  def writeTestDataAsGeoParquet(testData: DataFrame, path: String): Unit = {
    testData.coalesce(1).write.partitionBy("region").format("geoparquet").save(path)
  }

  /**
   * Load GeoParquet metadata for each region. Note that there could be multiple files for each region, thus each
   * region ID was associated with a list of GeoParquet metadata.
   * @param path path to directory containing GeoParquet files
   * @return Map of region ID to list of GeoParquet metadata
   */
  def readGeoParquetMetaDataMap(path: String): Map[Int, Seq[GeoParquetMetaData]] = {
    (0 until 4).map { k =>
      val geoParquetMetaDataSeq = readGeoParquetMetaDataByRegion(path, k)
      k -> geoParquetMetaDataSeq
    }.toMap
  }

  private def readGeoParquetMetaDataByRegion(geoParquetSavePath: String, region: Int): Seq[GeoParquetMetaData] = {
    val parquetFiles = new File(geoParquetSavePath + s"/region=$region").listFiles().filter(_.getName.endsWith(".parquet"))
    parquetFiles.flatMap { filePath =>
      val metadata = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(filePath.getPath), new Configuration()))
        .getFooter.getFileMetaData.getKeyValueMetaData
      assert(metadata.containsKey("geo"))
      GeoParquetMetaData.parseKeyValueMetaData(metadata)
    }
  }
}
