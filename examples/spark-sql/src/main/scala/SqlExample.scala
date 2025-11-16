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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}


object SqlExample {

  val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
  val rasterdatalocation = resourceFolder + "raster/"

  /**
   * Demonstrates predicate pushdown optimization and range join queries with spatial indexing.
   * Tests ST_Contains predicate with polygon and point data, including spatial filter pushdown.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testPredicatePushdownAndRangeJonQuery(sedona: SparkSession):Unit =
  {
    val sedonaConf = new SedonaConf(sedona.conf)
    println(sedonaConf)

    val polygonCsvDf = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPolygonInputLocation)
    polygonCsvDf.createOrReplaceTempView("polygontable")
    polygonCsvDf.show()
    val polygonDf = sedona.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    polygonDf.createOrReplaceTempView("polygondf")
    polygonDf.show()

    val pointCsvDF = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    pointCsvDF.show()
    val pointDf = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    pointDf.show()

    val rangeJoinDf = sedona.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
      "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

    // Write result to GeoParquet file
    rangeJoinDf.write.format("geoparquet").mode("overwrite").save("target/test-classes/output/join.parquet")
    rangeJoinDf.explain()
    rangeJoinDf.show(3)
    assert (rangeJoinDf.count()==500)
  }

  /**
   * Demonstrates distance join query that finds all point pairs within a specified distance.
   * Uses ST_Distance predicate with distance-based join optimization.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testDistanceJoinQuery(sedona: SparkSession): Unit =
  {
    val sedonaConf = new SedonaConf(sedona.conf)
    println(sedonaConf)

    val pointCsvDF1 = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF1.createOrReplaceTempView("pointtable")
    pointCsvDF1.show()
    val pointDf1 = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
    pointDf1.createOrReplaceTempView("pointdf1")
    pointDf1.show()

    val pointCsvDF2 = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF2.createOrReplaceTempView("pointtable")
    pointCsvDF2.show()
    val pointDf2 = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
    pointDf2.createOrReplaceTempView("pointdf2")
    pointDf2.show()

    val distanceJoinDf = sedona.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
    distanceJoinDf.explain()
    distanceJoinDf.show(10)
    assert (distanceJoinDf.count()==2998)
  }

  /**
   * Demonstrates spatial aggregate function ST_Envelope_Aggr to compute bounding box of point set.
   * Validates the computed envelope matches the expected boundary.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testAggregateFunction(sedona: SparkSession): Unit =
  {
    val sedonaConf = new SedonaConf(sedona.conf)
    println(sedonaConf)

    val pointCsvDF = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    val pointDf = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    val boundary = sedona.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")
    val coordinates:Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(1.1,101.1)
    coordinates(1) = new Coordinate(1.1,1100.1)
    coordinates(2) = new Coordinate(1000.1,1100.1)
    coordinates(3) = new Coordinate(1000.1,101.1)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)
    assert(boundary.take(1)(0).get(0)==geometryFactory.createPolygon(coordinates))
  }

  /**
   * Demonstrates reading shapefiles using the modern DataFrame-based reader.
   * Shows how to load shapefile data and query geometry and attribute fields.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testShapefileConstructor(sedona: SparkSession): Unit =
  {
    // Read shapefile using the DataFrame-based reader
    val spatialDf = sedona.read.format("shapefile").load(shapefileInputLocation)
    spatialDf.createOrReplaceTempView("rawSpatialDf")

    // Select specific columns
    val resultDf = sedona.sql("""
                                       | SELECT geometry, STATEFP, COUNTYFP
                                       | FROM rawSpatialDf
                                     """.stripMargin)
    resultDf.show()
    resultDf.printSchema()
  }

  /**
   * Demonstrates raster data I/O and map algebra operations.
   * Loads GeoTIFF raster data and performs various raster operations.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testRasterIOAndMapAlgebra(sedona: SparkSession): Unit = {
    val df = sedona.read.format("binaryFile").option("dropInvalid", true).load(rasterdatalocation).selectExpr("RS_FromGeoTiff(content) as raster", "path")
    df.printSchema()
    df.show()
    df.selectExpr("RS_Metadata(raster) as metadata", "RS_GeoReference(raster) as georef", "RS_NumBands(raster) as numBands").show(false)
    df.selectExpr("RS_AddBand(raster, raster, 1) as raster_extraband").show()
  }

  /**
   * Demonstrates writing spatial DataFrame to GeoParquet format.
   * GeoParquet is a cloud-native geospatial data format based on Apache Parquet.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testGeoParquetWriter(sedona: SparkSession): Unit = {
    // Create a sample DataFrame with geometries
    val pointCsvDF = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    val pointDf = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as geometry from pointtable")

    // Write to GeoParquet format
    val geoParquetOutputPath = "target/test-classes/output/points.geoparquet"
    pointDf.write
      .format("geoparquet")
      .mode(SaveMode.Overwrite)
      .save(geoParquetOutputPath)

    println(s"GeoParquet file written to: $geoParquetOutputPath")
    pointDf.show(5)
  }

  /**
   * Demonstrates reading GeoParquet files and performing spatial operations.
   * Shows how to load GeoParquet data and apply spatial transformations.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def testGeoParquetReader(sedona: SparkSession): Unit = {
    // First, ensure we have a GeoParquet file by writing one
    testGeoParquetWriter(sedona)

    // Read GeoParquet file
    val geoParquetInputPath = "target/test-classes/output/points.geoparquet"
    val geoParquetDf = sedona.read
      .format("geoparquet")
      .load(geoParquetInputPath)

    println(s"GeoParquet file read from: $geoParquetInputPath")
    geoParquetDf.printSchema()
    geoParquetDf.show(5)

    // Perform spatial operations on the loaded data
    geoParquetDf.createOrReplaceTempView("geoparquet_points")
    val bufferedDf = sedona.sql("""
                                    | SELECT ST_Buffer(geometry, 0.1) as buffered_geometry
                                    | FROM geoparquet_points
                                  """.stripMargin)

    println("Applied spatial operations on GeoParquet data:")
    bufferedDf.show(5)
  }
}
