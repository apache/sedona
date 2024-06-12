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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sedona_sql.strategy.join.{DistanceJoinExec, RangeJoinExec}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

class predicateJoinTestScala extends TestBaseScala {

  describe("Sedona-SQL Predicate Join Test") {

    // raster-vector predicates

    it("Passed RS_Intersects in a join") {
      val sedonaConf = new SedonaConf(sparkSession.conf)
      println(sedonaConf)

      val polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .load(buildingDataLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT(geometry) as building from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      val rasterDf = sparkSession.read
        .format("binaryFile")
        .load(rasterDataLocation)
        .selectExpr("RS_FromGeoTiff(content) as raster")
      rasterDf.createOrReplaceTempView("rasterDf")
      //        assert(distanceDefaultNoIntersectsDF.queryExecution.sparkPlan.collect { case p: DistanceJoinExec => p }.size === 1)
      val rangeJoinDf = sparkSession.sql(
        "select * from polygondf, rasterDf where RS_Intersects(rasterDf.raster, polygondf.building)")
      assert(rangeJoinDf.queryExecution.sparkPlan.collect { case p: RangeJoinExec =>
        p
      }.size === 1)
      assert(rangeJoinDf.count() == 999)
    }

    it("Passed RS_Contains in a join") {
      val sedonaConf = new SedonaConf(sparkSession.conf)
      println(sedonaConf)

      val polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .load(buildingDataLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT(geometry) as building from polygontable where confidence > 0.85")
      polygonDf.createOrReplaceTempView("polygondf")

      val rasterDf = sparkSession.read
        .format("binaryFile")
        .load(rasterDataLocation)
        .selectExpr("RS_FromGeoTiff(content) as raster")
      rasterDf.createOrReplaceTempView("rasterDf")
      val rangeJoinDf = sparkSession.sql(
        "select * from rasterDf, polygondf where RS_Contains(rasterDf.raster, polygondf.building)")
      assert(rangeJoinDf.queryExecution.sparkPlan.collect { case p: RangeJoinExec =>
        p
      }.size === 1)
      assert(rangeJoinDf.count() == 210)
    }

    it("Passed RS_Within in a join") {
      val sedonaConf = new SedonaConf(sparkSession.conf)
      println(sedonaConf)

      val smallRasterDf = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test1.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      smallRasterDf.createOrReplaceTempView("smallRaster")

      val rangeJoinDf = sparkSession.sql(
        "select * from smallRaster r1, smallRaster r2 where RS_Within(r1.raster, RS_ConvexHull(r2.raster))")
      assert(rangeJoinDf.queryExecution.sparkPlan.collect { case p: RangeJoinExec =>
        p
      }.size === 1)
      assert(rangeJoinDf.count() == 1)
    }

    it("Passed ST_Contains in a join") {
      val sedonaConf = new SedonaConf(sparkSession.conf)
      println(sedonaConf)

      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Intersects in a join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Intersects(polygondf.polygonshape,pointdf.pointshape) ")
      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Touches in a join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Touches(polygondf.polygonshape,pointdf.pointshape) ")

      assert(rangeJoinDf.count() == 0)
    }

    it("Passed ST_Within in a join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Within(pointdf.pointshape, polygondf.polygonshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Overlaps in a join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var polygonCsvOverlapDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(overlapPolygonInputLocation)
      polygonCsvOverlapDf.createOrReplaceTempView("polygonoverlaptable")
      var polygonOverlapDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygonoverlaptable._c0 as Decimal(24,20)),cast(polygonoverlaptable._c1 as Decimal(24,20)), cast(polygonoverlaptable._c2 as Decimal(24,20)), cast(polygonoverlaptable._c3 as Decimal(24,20))) as polygonshape from polygonoverlaptable")
      polygonOverlapDf.createOrReplaceTempView("polygonodf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, polygonodf where ST_Overlaps(polygondf.polygonshape, polygonodf.polygonshape)")

      assert(rangeJoinDf.count() == 15)
    }

    it("Passed ST_Crosses in a join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Crosses(pointdf.pointshape, polygondf.polygonshape) ")

      assert(rangeJoinDf.count() == 0)
    }

    it("Passed ST_Covers in a join") {
      val sedonaConf = new SedonaConf(sparkSession.conf)
      println(sedonaConf)

      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Covers(polygondf.polygonshape,pointdf.pointshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_CoveredBy in a join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql(
        "select * from polygondf, pointdf where ST_CoveredBy(pointdf.pointshape, polygondf.polygonshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Intersects in a join with singleton dataframe") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
        .limit(1)
        .repartition(1)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      // Join with a singleton dataframe is essentially a range query
      val polygon = polygonDf.first().getAs[Geometry]("polygonshape")
      val rangeQueryDf = sparkSession.sql(
        s"select * from pointdf where ST_Intersects(pointdf.pointshape, ST_GeomFromWKT('${polygon.toString}'))")
      val rangeQueryCount = rangeQueryDf.count()

      // Perform spatial join and compare results
      val rangeJoinDf1 = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Intersects(pointdf.pointshape, polygondf.polygonshape)")
      val rangeJoinDf2 = sparkSession.sql(
        "select * from pointdf, polygondf where ST_Intersects(polygondf.polygonshape, pointdf.pointshape)")
      assert(rangeJoinDf1.count() == rangeQueryCount)
      assert(rangeJoinDf2.count() == rangeQueryCount)
    }

    it("Passed ST_Intersects in a join with empty dataframe") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession
        .sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable where pointtable._c0 > 10000")
        .repartition(1)
      pointDf.createOrReplaceTempView("pointdf")

      val rangeJoinDf1 = sparkSession.sql(
        "select * from polygondf, pointdf where ST_Intersects(pointdf.pointshape, polygondf.polygonshape)")
      val rangeJoinDf2 = sparkSession.sql(
        "select * from pointdf, polygondf where ST_Intersects(polygondf.polygonshape, pointdf.pointshape)")
      assert(rangeJoinDf1.count() == 0)
      assert(rangeJoinDf2.count() == 0)
    }

    it("Passed ST_Distance <= radius in a join") {
      var pointCsvDF1 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF1.createOrReplaceTempView("pointtable")
      var pointDf1 = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
      pointDf1.createOrReplaceTempView("pointdf1")

      var pointCsvDF2 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable")
      var pointDf2 = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
      pointDf2.createOrReplaceTempView("pointdf2")

      var distanceJoinDf = sparkSession.sql(
        "select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) <= 2")

      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < distance in a join") {
      var pointCsvDF1 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF1.createOrReplaceTempView("pointtable")
      var pointDf1 = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
      pointDf1.createOrReplaceTempView("pointdf1")

      var pointCsvDF2 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable")
      var pointDf2 = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
      pointDf2.createOrReplaceTempView("pointdf2")

      var distanceJoinDf = sparkSession.sql(
        "select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")

      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < distance with LineString in a join") {
      assert(
        sparkSession
          .sql("""
          |select *
          |from (select ST_LineFromText('LineString(1 1, 1 3, 3 3)') as geom) a
          |join (select ST_Point(2.0,2.0) as geom) b
          |on ST_Distance(a.geom, b.geom) < 0.1
          |""".stripMargin)
          .isEmpty)
      assert(
        sparkSession
          .sql("""
          |select *
          |from (select ST_LineFromText('LineString(1 1, 1 4)') as geom) a
          |join (select ST_Point(1.0,5.0) as geom) b
          |on ST_Distance(a.geom, b.geom) < 1.5
          |""".stripMargin)
          .count() == 1)
    }

    it("Passed ST_Contains in a range and join") {
      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
        "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

      assert(rangeJoinDf.count() == 500)
    }

    it("Passed super small data join") {
      val rawPointDf = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(
          Seq(Row(1, "40.0", "-120.0"), Row(2, "30.0", "-110.0"), Row(3, "20.0", "-100.0"))),
        StructType(
          List(
            StructField("id", IntegerType, true),
            StructField("lat", StringType, true),
            StructField("lon", StringType, true))))
      rawPointDf.createOrReplaceTempView("rawPointDf")

      val pointDF = sparkSession.sql(
        "select id, ST_Point(cast(lat as Decimal(24,20)), cast(lon as Decimal(24,20))) AS latlon_point FROM rawPointDf")
      pointDF.createOrReplaceTempView("pointDf")

      val rawPolygonDf = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(
          Seq(Row("A", 25.0, -115.0, 35.0, -105.0), Row("B", 25.0, -135.0, 35.0, -125.0))),
        StructType(
          List(
            StructField("id", StringType, true),
            StructField("latmin", DoubleType, true),
            StructField("lonmin", DoubleType, true),
            StructField("latmax", DoubleType, true),
            StructField("lonmax", DoubleType, true))))
      rawPolygonDf.createOrReplaceTempView("rawPolygonDf")

      val polygonEnvelopeDF = sparkSession.sql(
        "select id, ST_PolygonFromEnvelope(" +
          "cast(latmin as Decimal(24,20)), cast(lonmin as Decimal(24,20)), " +
          "cast(latmax as Decimal(24,20)), cast(lonmax as Decimal(24,20))) AS polygon FROM rawPolygonDf")
      polygonEnvelopeDF.createOrReplaceTempView("polygonDf")

      val withinEnvelopeDF = sparkSession.sql(
        "select * FROM pointDf, polygonDf WHERE ST_Within(pointDf.latlon_point, polygonDf.polygon)")
      assert(withinEnvelopeDF.count() == 1)
    }
    it("Passed ST_Equals in a join for ST_Point") {
      var pointCsvDf1 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPoint1InputLocation)
      pointCsvDf1.createOrReplaceTempView("pointtable1")
      var pointDf1 = sparkSession.sql(
        "select ST_Point(cast(pointtable1._c0 as Decimal(24,20)),cast(pointtable1._c1 as Decimal(24,20)) ) as pointshape1 from pointtable1")
      pointDf1.createOrReplaceTempView("pointdf1")

      var pointCsvDF2 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPoint2InputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable2")
      var pointDf2 = sparkSession.sql(
        "select ST_Point(cast(pointtable2._c0 as Decimal(24,20)),cast(pointtable2._c1 as Decimal(24,20))) as pointshape2 from pointtable2")
      pointDf2.createOrReplaceTempView("pointdf2")

      var equalJoinDf = sparkSession.sql(
        "select * from pointdf1, pointdf2 where ST_Equals(pointdf1.pointshape1,pointdf2.pointshape2) ")

      assert(equalJoinDf.count() == 100, s"Expected 100 but got ${equalJoinDf.count()}")
    }
    it("Passed ST_Equals in a join for ST_Polygon") {
      var polygonCsvDf1 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1InputLocation)
      polygonCsvDf1.createOrReplaceTempView("polygontable1")
      var polygonDf1 = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable1._c0 as Decimal(24,20)),cast(polygontable1._c1 as Decimal(24,20)), cast(polygontable1._c2 as Decimal(24,20)), cast(polygontable1._c3 as Decimal(24,20))) as polygonshape1 from polygontable1")
      polygonDf1.createOrReplaceTempView("polygondf1")

      var polygonCsvDf2 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon2InputLocation)
      polygonCsvDf2.createOrReplaceTempView("polygontable2")
      var polygonDf2 = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable2._c0 as Decimal(24,20)),cast(polygontable2._c1 as Decimal(24,20)), cast(polygontable2._c2 as Decimal(24,20)), cast(polygontable2._c3 as Decimal(24,20))) as polygonshape2 from polygontable2")
      polygonDf2.createOrReplaceTempView("polygondf2")

      var equalJoinDf = sparkSession.sql(
        "select * from polygondf1, polygondf2 where ST_Equals(polygondf1.polygonshape1,polygondf2.polygonshape2) ")

      assert(equalJoinDf.count() == 100, s"Expected 100 but got ${equalJoinDf.count()}")
    }
    it("Passed ST_Equals in a join for ST_Polygon Random Shuffle") {
      var polygonCsvDf1 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1RandomInputLocation)
      polygonCsvDf1.createOrReplaceTempView("polygontable1")
      var polygonDf1 = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable1._c0 as Decimal(24,20)),cast(polygontable1._c1 as Decimal(24,20)), cast(polygontable1._c2 as Decimal(24,20)), cast(polygontable1._c3 as Decimal(24,20))) as polygonshape1 from polygontable1")
      polygonDf1.createOrReplaceTempView("polygondf1")

      var polygonCsvDf2 = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon2RandomInputLocation)
      polygonCsvDf2.createOrReplaceTempView("polygontable2")
      var polygonDf2 = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable2._c0 as Decimal(24,20)),cast(polygontable2._c1 as Decimal(24,20)), cast(polygontable2._c2 as Decimal(24,20)), cast(polygontable2._c3 as Decimal(24,20))) as polygonshape2 from polygontable2")
      polygonDf2.createOrReplaceTempView("polygondf2")

      var equalJoinDf = sparkSession.sql(
        "select * from polygondf1, polygondf2 where ST_Equals(polygondf1.polygonshape1,polygondf2.polygonshape2) ")

      assert(equalJoinDf.count() == 100, s"Expected 100 but got ${equalJoinDf.count()}")
    }
    it("Passed ST_Equals in a join for ST_Point and ST_Polygon") {
      var pointCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPoint1InputLocation)
      pointCsvDf.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)) ) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1InputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var equalJoinDf = sparkSession.sql(
        "select * from pointdf, polygondf where ST_Equals(pointdf.pointshape,polygondf.polygonshape) ")

      assert(equalJoinDf.count() == 0, s"Expected 0 but got ${equalJoinDf.count()}")
    }

    it("Passed ST_DistanceSpheroid in a spatial join") {
      val distanceCandidates = Seq(130000, 160000, 500000)
      val sampleCount = 50
      distanceCandidates.foreach(distance => {
        val expected = bruteForceDistanceJoinCountSpheroid(sampleCount, distance)
        val pointDf1 = buildPointLonLatDf.limit(sampleCount).repartition(4)
        val pointDf2 = pointDf1
        var distanceJoinDf = pointDf1
          .alias("pointDf1")
          .join(
            pointDf2.alias("pointDf2"),
            expr(s"ST_DistanceSpheroid(pointDf1.pointshape, pointDf2.pointshape) <= $distance"))
        assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)
        assert(distanceJoinDf.count() == expected)

        distanceJoinDf = pointDf1
          .alias("pointDf1")
          .join(
            pointDf2.alias("pointDf2"),
            expr(s"ST_DistanceSpheroid(pointDf1.pointshape, pointDf2.pointshape) < $distance"))
        assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)
        assert(distanceJoinDf.count() == expected)
      })
    }

    it("Passed ST_DistanceSphere in a spatial join") {
      val distanceCandidates = Seq(130000, 160000, 500000)
      val sampleCount = 50
      distanceCandidates.foreach(distance => {
        val expected = bruteForceDistanceJoinCountSphere(sampleCount, distance)
        val pointDf1 = buildPointLonLatDf.limit(sampleCount).repartition(4)
        val pointDf2 = pointDf1
        var distanceJoinDf = pointDf1
          .alias("pointDf1")
          .join(
            pointDf2.alias("pointDf2"),
            expr(s"ST_DistanceSphere(pointDf1.pointshape, pointDf2.pointshape) <= $distance"))
        assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)
        assert(distanceJoinDf.count() == expected)

        distanceJoinDf = pointDf1
          .alias("pointDf1")
          .join(
            pointDf2.alias("pointDf2"),
            expr(s"ST_DistanceSphere(pointDf1.pointshape, pointDf2.pointshape) < $distance"))
        assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)
        assert(distanceJoinDf.count() == expected)
      })
    }

    it("Passed ST_HausdorffDistance in a spatial join") {
      val sampleCount = 100
      val distanceCandidates = Seq(1, 2, 5, 10)
      val densityFrac = 0.6
      val inputPoint = buildPointDf.limit(sampleCount).repartition(5)
      val inputPolygon = buildPolygonDf.limit(sampleCount).repartition(3)

      distanceCandidates.foreach(distance => {

        // DensityFrac specified, <= distance
        val expectedDensityIntersects =
          bruteForceDistanceJoinHausdorff(sampleCount, distance, densityFrac, true)
        val distanceDensityIntersectsDF = inputPoint
          .alias("pointDF")
          .join(
            inputPolygon.alias("polygonDF"),
            expr(
              s"ST_HausdorffDistance(pointDF.pointshape, polygonDF.polygonshape, $densityFrac) <= $distance"))
        assert(distanceDensityIntersectsDF.queryExecution.sparkPlan.collect {
          case p: DistanceJoinExec => p
        }.size === 1)
        assert(distanceDensityIntersectsDF.count() == expectedDensityIntersects)

        // DensityFrac specified, < distance
        val expectedDensityNoIntersect =
          bruteForceDistanceJoinHausdorff(sampleCount, distance, densityFrac, false)
        val distanceDensityNoIntersectDF = inputPoint
          .alias("pointDF")
          .join(
            inputPolygon.alias("polygonDF"),
            expr(
              s"ST_HausdorffDistance(pointDF.pointshape, polygonDF.polygonshape, $densityFrac) <= $distance"))
        assert(distanceDensityNoIntersectDF.queryExecution.sparkPlan.collect {
          case p: DistanceJoinExec => p
        }.size === 1)
        assert(distanceDensityNoIntersectDF.count() == expectedDensityNoIntersect)

        // DensityFrac not specified, <= distance
        val expectedDefaultIntersects =
          bruteForceDistanceJoinHausdorff(sampleCount, distance, 0.0, true)
        val distanceDefaultIntersectsDF = inputPoint
          .alias("pointDF")
          .join(
            inputPolygon.alias("polygonDF"),
            expr(
              s"ST_HausdorffDistance(pointDF.pointshape, polygonDF.polygonshape, $densityFrac) <= $distance"))
        assert(distanceDefaultIntersectsDF.queryExecution.sparkPlan.collect {
          case p: DistanceJoinExec => p
        }.size === 1)
        assert(distanceDefaultIntersectsDF.count() == expectedDefaultIntersects)

        // DensityFrac not specified, < distance
        val expectedDefaultNoIntersects =
          bruteForceDistanceJoinHausdorff(sampleCount, distance, 0.0, false)
        val distanceDefaultNoIntersectsDF = inputPoint
          .alias("pointDF")
          .join(
            inputPolygon.alias("polygonDF"),
            expr(
              s"ST_HausdorffDistance(pointDF.pointshape, polygonDF.polygonshape, $densityFrac) <= $distance"))
        assert(distanceDefaultNoIntersectsDF.queryExecution.sparkPlan.collect {
          case p: DistanceJoinExec => p
        }.size === 1)
        assert(distanceDefaultIntersectsDF.count() == expectedDefaultNoIntersects)
      })
    }

    it("Passed ST_FrechetDistance in a spatial join") {
      val sampleCount = 200
      val distanceCandidates = Seq(1, 2, 5, 10)
      val inputPoint = buildPointDf.limit(sampleCount).repartition(5)
      val inputPolygon = buildPolygonDf.limit(sampleCount).repartition(3)

      distanceCandidates.foreach(distance => {
        // <= distance
        val expectedIntersects = bruteForceDistanceJoinFrechet(sampleCount, distance, true)
        val distanceDefaultIntersectsDF = inputPoint
          .alias("pointDF")
          .join(
            inputPolygon.alias("polygonDF"),
            expr(s"ST_FrechetDistance(pointDF.pointshape, polygonDF.polygonshape) <= $distance"))
        assert(distanceDefaultIntersectsDF.queryExecution.sparkPlan.collect {
          case p: DistanceJoinExec => p
        }.size === 1)
        assert(distanceDefaultIntersectsDF.count() == expectedIntersects)

        // < distance
        val expectedNoIntersects = bruteForceDistanceJoinFrechet(sampleCount, distance, false)
        val distanceDefaultNoIntersectsDF = inputPoint
          .alias("pointDF")
          .join(
            inputPolygon.alias("polygonDF"),
            expr(s"ST_FrechetDistance(pointDF.pointshape, polygonDF.polygonshape) <= $distance"))
        assert(distanceDefaultNoIntersectsDF.queryExecution.sparkPlan.collect {
          case p: DistanceJoinExec => p
        }.size === 1)
        assert(distanceDefaultIntersectsDF.count() == expectedNoIntersects)
      })
    }

    it("Passed ST_DWithin (useSpheroid = false) in a spatial join") {
      val sampleCount = 200
      val distanceCandidates = Seq(1, 2, 5, 10)
      val inputPoint = buildPointDf.limit(sampleCount).repartition(5)
      val inputPolygon = buildPolygonDf.limit(sampleCount).repartition(3)

      distanceCandidates.foreach(distance => {
        val expected = bruteForceDWithin(sampleCount, distance)

        val dfWithinDfShortForm = inputPoint
          .alias("pointDf")
          .join(
            inputPolygon.alias("polygonDf"),
            expr(s"ST_DWithin(pointDf.pointshape, polygonDf.polygonshape, $distance)"))
        assert(dfWithinDfShortForm.count() == expected)
        assert(dfWithinDfShortForm.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)

        val dWithinDf = inputPoint
          .alias("pointDf")
          .join(
            inputPolygon.alias("polygonDf"),
            expr(s"ST_DWithin(pointDf.pointshape, polygonDf.polygonshape, $distance, false)"))

        assert(dWithinDf.count() == expected)
        assert(dWithinDf.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)
      })
    }

    it("Passed ST_DWithin useSphere long form (useSpheroid = true) in a spatial join") {
      val Seq(sphericalDf1, sphericalDf2) = createSpheroidDataFrames()
      val distanceCandidates = Seq(110000, 1055000, 2000000)

      distanceCandidates.foreach(distance => {
        val expected = bruteForceDWithinSphere(distance)
        val dWithinDf = sphericalDf1
          .alias("df1")
          .join(
            sphericalDf2.alias("df2"),
            expr(s"ST_DWithin(df1.geom, df2.geom, $distance, true)"))

        assert(dWithinDf.count() == expected)
        assert(dWithinDf.queryExecution.sparkPlan.collect { case p: DistanceJoinExec =>
          p
        }.size === 1)
      })
    }

    it("Passed ST_DWithin complex boolean expression") {
      val expected = 55
      val df_point = sparkSession.range(10).withColumn("pt", expr("ST_Point(id, id)"))
      val df_polygon = sparkSession.range(10).withColumn("poly", expr("ST_Point(id, id + 0.01)"))
      val actual = df_point
        .alias("a")
        .join(df_polygon.alias("b"), expr("ST_DWithin(pt, poly, 10000, a.`id` % 2 = 0)"))
        .count()
      assert(expected == actual)
    }
  }
}
