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

import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
import org.apache.sedona.core.formatMapper.WktReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{CircleRDD, PolygonRDD}
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers

class adapterTestScala extends TestBaseScala with Matchers with GivenWhenThen{

  import sparkSession.implicits._

  describe("Sedona-SQL Scala Adapter Test") {

    it("Read CSV point into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD, sparkSession).show(1)
    }

    it("Read CSV point at a different column id into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, 2)
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f => f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point at a different column col name into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f => f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point into a SpatialRDD by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
      //      Adapter.toDf(spatialRDD, sparkSession).show(1)
    }

    it("Read CSV point into a SpatialRDD with unique Id by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      // Use Column _c0 as the unique Id but the id can be anything in the same row
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
      //      Adapter.toDf(spatialRDD, sparkSession).show(1)
    }


    it("Read mixed WKT geometries into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
    }

    it("Read mixed WKT geometries into a SpatialRDD with uniqueId") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty, inputtable._c3, inputtable._c5 from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 3)
    }

    it("Read shapefile -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      df.show(1)
    }

    it("Read shapefileWithMissing -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileWithMissingsTrailingInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      df.show(1)
    }

    it("Read GeoJSON to DataFrame") {
      import org.apache.spark.sql.functions.{callUDF, col}
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession).withColumn("geometry", callUDF("ST_GeomFromWKT", col("geometry")))
      assert(df.columns(1) == "STATEFP")
    }

    it("Convert spatial join result to DataFrame") {
      val polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty, 'abc' as abc, 'def' as def from polygontable")
      val polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()

      val pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      val pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      val pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true)

      val joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      joinResultDf.show(1)

      val joinResultDf2 = Adapter.toDf(joinResultPairRDD, List("abc", "def"), List(), sparkSession)
      joinResultDf2.show(1)
    }

    it("Convert distance join result to DataFrame") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      var pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable")
      var polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()
      var circleRDD = new CircleRDD(polygonRDD, 0.2)

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      circleRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      var joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true)

      var joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      joinResultDf.show(1)
    }

    it("load id column Data check") {
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonIdInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      val df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.columns.length == 4)
      assert(df.count() == 1)

    }

    it("should correctly convert to geometry dataframe"){
      Given("point rdd converted from dataframe and circle rdd from data frame")
      val pointDf = sparkSession.read.format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(arealmPointInputLocation)
        .selectExpr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as arealandmark")

      val pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      val polygonDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
        .selectExpr("ST_GeomFromWKT(_c0) as usacounty")

      val polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()
      val circleRDD = new CircleRDD(polygonRDD, 0.2)

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      circleRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      val joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true)

      When("running Adapter to geometry df")
      val geometryDf = Adapter.toGeometryDf(joinResultPairRDD, sparkSession)

      Then("Count should match to given one")
      geometryDf.count shouldBe 8283

      And("columns should match to expected ones")
      geometryDf.columns should contain theSameElementsAs Seq("_c0", "_c1", "_c2", "_c3")

      And("geometries should be correctly placed")
      geometryDf.schema.map(structField => structField.dataType) should contain theSameElementsAs List(
        GeometryUDT, StringType, GeometryUDT, StringType
      )

    }

    it("should correctly map field names while converting JavaRDD[Geometry, Geometry] to dataframe via Adapter"){
      Given("Spatial RDD representing POIS and areas")
      val pointSmallRDD = WktReader.readToGeometryRDD(
        sparkSession.sparkContext, smallPointsLocation,
        1, false,
        false
      )
      val areasPolygonRDD = WktReader.readToGeometryRDD(
        sparkSession.sparkContext,
        smallAreasLocation, 1,
        false,
        false
      )

      pointSmallRDD.analyze()
      areasPolygonRDD.analyze()

      pointSmallRDD.spatialPartitioning(GridType.QUADTREE)
      areasPolygonRDD.spatialPartitioning(pointSmallRDD.getPartitioner)

      When("running spatial join query and the convert it into dataframe")
      val poisWithinAreas = JoinQuery.SpatialJoinQueryFlat(pointSmallRDD, areasPolygonRDD, false, false)
      val spatialResult = Adapter.toDf(poisWithinAreas, sparkSession)

      Then("count should match to expected one")
      spatialResult.count shouldBe 5

      And("geometry columns should contain geometry wkt")
      val leftGeometryWkt = spatialResult.select("leftgeometry").as[String].collect()
      val rightGeometryWkt = spatialResult.select("rightgeometry").as[String].collect()

      leftGeometryWkt should contain theSameElementsAs List(
        "POLYGON ((0 4, -3 3, -8 6, -6 8, -2 9, 0 4))", "POLYGON ((2 2, 2 4, 3 5, 7 5, 9 3, 8 1, 4 1, 2 2))",
        "POLYGON ((10 3, 10 6, 14 6, 14 3, 10 3))", "POLYGON ((-1 -1, -1 -3, -2 -5, -6 -8, -5 -2, -3 -2, -1 -1))",
        "POLYGON ((-1 -1, -1 -3, -2 -5, -6 -8, -5 -2, -3 -2, -1 -1))"
      )

      rightGeometryWkt should contain theSameElementsAs List(
        "POINT (-3 5)", "POINT (4 3)", "POINT (11 5)", "POINT (-1 -1)", "POINT (-4 -5)"
      )
    }

    it("should correctly map field names while converting SpatialRDD[Geometry] to dataframe via Adapter"){
      Given("Spatial RDD representing POIS")
      val pointSmallRDD = WktReader.readToGeometryRDD(
        sparkSession.sparkContext, smallPointsLocation,
        1, false,
        false
      )

      When("converting to geometry dataframe")
      val geometryDf = Adapter.toGeometryDf(pointSmallRDD, sparkSession)

      Then("dataframe count should match to expected one")
      geometryDf.count shouldBe 10

      And("schema should have correct data types")
      geometryDf.schema.map(_.dataType) shouldBe List(GeometryUDT, StringType)

      And("collected on driver wkt should be the same as input one")
      geometryDf.selectExpr("ST_ASText(_c0)").as[String].collect() should contain theSameElementsAs List(
        "POINT (11 5)", "POINT (12 1)", "POINT (-1 -1)", "POINT (-3 5)", "POINT (9 8)", "POINT (4 3)",
        "POINT (-4 -5)", "POINT (4 -2)", "POINT (-3 1)", "POINT (-7 3)"
      )
    }

  }
}
