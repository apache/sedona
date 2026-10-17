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
package org.apache.sedona.sql.functions

import org.apache.sedona.common.Functions
import org.apache.sedona.common.utils.H3Utils
import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.functions.{col, expr, lit, monotonically_increasing_id}
import org.locationtech.jts.geom.Coordinate
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.mutable

class STH3Functions extends TestBaseScala with Matchers with GeometrySample with GivenWhenThen {
  import sparkSession.implicits._

  describe("should pass ST_H3CellIDs") {

    it("should return null while using ST_H3CellIDs when geometry is empty") {
      val geometryTable = sparkSession.sparkContext
        .parallelize(1 to 10)
        .toDF()
        .withColumn("geom", lit(null))

      When("using ST_MakePolygon on null geometries")
      val geometryTableWithCellIDs = geometryTable
        .withColumn("cell_ids", expr("ST_H3CellIDs(geom, 6, true)"))
        .select("cell_ids")
        .collect()
        .filter(r => r.get(0) != null)

      Then("no exception should be raised")
      require(geometryTableWithCellIDs.isEmpty)
    }

    it("should correctly return array of cell ids use of ST_H3CellIDs") {
      Given("DataFrame with valid Geometries")
      val geometryTable = Seq(
        "POINT(1 2)",
        "LINESTRING(-5 8, -6 1, -8 6, -2 5, -6 1, -5 8)",
        "POLYGON ((75 29, 77 29, 76 28, 75 29))")
        .map(geom => Tuple1(wktReader.read(geom)))
        .toDF("geom")

      When("generate ST_H3CellIDs from those geometries")
      val geometryDfWithCellIDs = geometryTable
        .withColumn("cell_ids", expr("ST_H3CellIDs(geom, 6, true)"))

      Then("valid should have list of Long type cell ids returned")
      geometryDfWithCellIDs
        .select("cell_ids")
        .collect()
        .foreach(r => require(r.get(0).isInstanceOf[mutable.WrappedArray[Long]] && r.size > 0))
    }

    it("use ST_H3CellIDs for spatial join") {
      Given("DataFrame with valid line strings")
      val polygonDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(geojsonInputLocation)
        .select(expr("ST_GeomFromGeoJSON(_c0)").as("countyshape"))
        .select(monotonically_increasing_id.as("id"), col("countyshape").as("geom"))
        .limit(100)
      val rightPolygons = polygonDf.filter("id > 50")
      rightPolygons.createOrReplaceTempView("rights")
      // generate a sub list of polygons
      val leftPolygons = polygonDf.filter("id <= 50")
      leftPolygons.createOrReplaceTempView("lefts")
      When(
        "generate the cellIds for both set of polygons, and explode into separate rows, join them by cellIds")
      val joinedDf = sparkSession.sql("""
          |with lcs as (
          | select id, geom, explode(ST_H3CellIDs(geom, 8, true)) as cellId from lefts
          |)
          |, rcs as (
          | select id, geom, explode(ST_H3CellIDs(geom, 8, true)) as cellId from rights
          |)
          |, join_res as (
          | select lcs.id lid, rcs.id rid, first(lcs.geom) lgeom, first(rcs.geom) rgeom from lcs join rcs on lcs.cellId = rcs.cellId group by 1, 2
          |)
          |select sum(if(ST_Intersects(lgeom, rgeom), 1, 0)) count_true_positive, count(1) count_positive from join_res
          |""".stripMargin)
      Then(
        "the geoms joined by cell ids should all really intersect in this case." +
          "Note that, cellIds equal doesn't necessarily mean the geoms intersect." +
          "Sedona added augmentation to H3 native cover functions to ensure fully coverage, call ST_Intersects to filter out redundancy")
      val res = joinedDf.collect()(0)
      require(res.get(0).asInstanceOf[Long] == 5)
      require(res.get(0).asInstanceOf[Long] <= res.get(1).asInstanceOf[Long])
    }

    it("should correctly calculate the distance between two h3 cells") {
      Given("DataFrame with valid Geometries")
      val geometryTable = Seq(Seq("POINT(1 2)", "POINT(1.23 1.59)"))
        .map(geom => Tuple2(wktReader.read(geom(0)), wktReader.read(geom(1))))
        .toDF("lgeom", "rgeom")

      Then("generate ST_H3CellIDs from those geometries")
      val distance = geometryTable
        .withColumn("lcell", expr("ST_H3CellIDs(lgeom, 8, true)[0]"))
        .withColumn("rcell", expr("ST_H3CellIDs(rgeom, 8, true)[0]"))
        .select(expr("ST_H3CellDistance(lcell, rcell)"))
        .collectAsList()
        .get(0)
        .get(0)
      val expects = Functions.h3CellDistance(
        H3Utils.coordinateToCell(new Coordinate(1, 2), 8),
        H3Utils.coordinateToCell(new Coordinate(1.23, 1.59), 8))
      distance should equal(expects)
    }

    it("should correctly retrieve the KRing neighbors of given cellId") {
      Given("DataFrame with valid Geometries")
      val geometryTable = Seq("POINT(1 2)").map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

      Then("generate ST_H3KRing from those geometries")
      val exacts = geometryTable
        .select(expr("ST_H3KRing(ST_H3CellIDs(geom, 8, true)[0], 3, true)"))
        .collectAsList()
        .get(0)
        .get(0)
        .asInstanceOf[mutable.WrappedArray[Long]]
        .toArray
      val expects = Functions.h3KRing(H3Utils.coordinateToCell(new Coordinate(1, 2), 8), 3, true)
      Then("the result of spark sql should match with direct common library call")
      exacts should equal(expects)
    }

    it("should return child cells at the requested resolution") {
      val parent = H3Utils.coordinateToCell(new Coordinate(1, 2), 5)
      val children = Seq((parent, 7))
        .toDF("cell", "resolution")
        .select(expr("ST_H3ToChildren(cell, resolution)"))
        .first()
        .getSeq[Long](0)

      children.size should equal(49)
      children.distinct.size should equal(49)
      children.foreach { child =>
        H3Utils.h3.getResolution(child) should equal(7)
        Functions.h3ToParent(child, 5) should equal(parent)
      }
      sparkSession
        .sql(s"SELECT ST_H3ToChildren($parent, 5)")
        .first()
        .getSeq[Long](0) should equal(Seq(parent))
    }

    it("should return six immediate children for a pentagon") {
      val parent = H3Utils.h3.getPentagons(5).iterator().next().longValue()
      val children = sparkSession
        .sql(s"SELECT ST_H3ToChildren($parent, 6)")
        .first()
        .getSeq[Long](0)
      children.size should equal(6)
      children.foreach(child => Functions.h3ToParent(child, 5) should equal(parent))
    }

    it("should propagate null inputs to ST_H3ToChildren") {
      val result = sparkSession
        .sql("""SELECT
            |ST_H3ToChildren(CAST(NULL AS BIGINT), 6),
            |ST_H3ToChildren(601041811137363967, CAST(NULL AS INT)),
            |ST_H3ToChildren(CAST(NULL AS BIGINT), CAST(NULL AS INT))
            |""".stripMargin)
        .first()
      (0 until 3).foreach(i => result.isNullAt(i) should be(true))
    }

    it("should uncompact cells at mixed resolutions") {
      val parent = H3Utils.coordinateToCell(new Coordinate(1, 2), 5)
      val other = H3Utils.coordinateToCell(new Coordinate(10, 20), 6)
      val cells = Seq((Seq(parent, other), 6))
        .toDF("cells", "resolution")
        .select(expr("ST_H3Uncompact(cells, resolution)"))
        .first()
        .getSeq[Long](0)
      cells.size should equal(8)
      cells.toSet should equal(Functions.h3ToChildren(parent, 6).toSet + other)

      val pentagon = H3Utils.h3.getPentagons(5).iterator().next().longValue()
      sparkSession
        .sql(s"SELECT ST_H3Uncompact(array($pentagon), 7)")
        .first()
        .getSeq[Long](0)
        .size should equal(41)
    }

    it("should preserve cells at the target resolution and return empty arrays") {
      val cell = H3Utils.coordinateToCell(new Coordinate(1, 2), 5)
      val result = sparkSession
        .sql(s"""SELECT ST_H3Uncompact(array($cell), 5),
             |ST_H3Uncompact(CAST(array() AS ARRAY<BIGINT>), 6)
             |""".stripMargin)
        .first()
      result.getSeq[Long](0) should equal(Seq(cell))
      result.getSeq[Long](1) should be(empty)
    }

    it("should propagate null inputs to ST_H3Uncompact") {
      val result = sparkSession
        .sql("""SELECT
            |ST_H3Uncompact(CAST(NULL AS ARRAY<BIGINT>), 6),
            |ST_H3Uncompact(array(601041811137363967L), CAST(NULL AS INT)),
            |ST_H3Uncompact(CAST(NULL AS ARRAY<BIGINT>), CAST(NULL AS INT))
            |""".stripMargin)
        .first()
      (0 until 3).foreach(i => result.isNullAt(i) should be(true))

      val inputs = Seq((None, Some(6)), (Some(Seq(601041811137363967L)), None), (None, None))
      inputs
        .toDF("cells", "resolution")
        .select(expr("ST_H3Uncompact(cells, resolution)"))
        .collect()
        .foreach(row => row.isNullAt(0) should be(true))
    }

    it("should reject a target resolution coarser than an input cell") {
      val cell = H3Utils.coordinateToCell(new Coordinate(1, 2), 6)
      val error = intercept[Exception] {
        sparkSession.sql(s"SELECT ST_H3Uncompact(array($cell), 5)").collect()
      }
      Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .exists(_.isInstanceOf[com.uber.h3core.exceptions.H3Exception]) should be(true)
    }

    it("should return the parent cell at the requested resolution") {
      Given("DataFrame with a valid Geometry")
      val geometryTable = Seq("POINT(1 2)").map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

      When("finding the resolution 5 parent of a resolution 8 cell")
      val result = geometryTable
        .withColumn("cell", expr("ST_H3CellIDs(geom, 8, true)[0]"))
        .select(col("cell"), expr("ST_H3ToParent(cell, 5)"))
        .first()
      val child = result.getLong(0)
      val parent = result.getLong(1)

      Then("the result should be the requested ancestor")
      H3Utils.h3.getResolution(parent) should equal(5)
      H3Utils.h3.cellToChildren(parent, 8).contains(child) should be(true)

      Then("null input should return null")
      sparkSession
        .sql("SELECT ST_H3ToParent(CAST(NULL AS BIGINT), 5)")
        .first()
        .isNullAt(0) should be(true)
    }
  }
}
