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

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.functions.{col, expr, lit, monotonically_increasing_id}
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.mutable

class STS2CellIDs extends TestBaseScala with Matchers with GeometrySample with GivenWhenThen {
  import sparkSession.implicits._

  describe("should pass ST_S2CellIDs"){

    it("should return null while using ST_S2CellIDs when geometry is empty") {
      val geometryTable = sparkSession.sparkContext.parallelize(1 to 10).toDF()
        .withColumn("geom", lit(null))

      When("using ST_MakePolygon on null geometries")
      val geometryTableWithCellIDs = geometryTable
        .withColumn("cell_ids", expr("ST_S2CellIDs(geom, 4)"))
        .select("cell_ids").collect().filter(
        r => r.get(0)!= null
      )

      Then("no exception should be raised")
      require(geometryTableWithCellIDs.isEmpty)
    }

    it("should correctly return array of cell ids use of ST_S2CellIDs"){
      Given("DataFrame with valid Geometries")
      val geometryTable = Seq(
        "POINT(1 2)",
        "LINESTRING(-5 8, -6 1, -8 6, -2 5, -6 1, -5 8)",
        "POLYGON ((75 29, 77 29, 77 29, 75 29))"

      ).map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

      When("generate ST_S2CellIDs from those geometries")
      val geometryDfWithCellIDs = geometryTable
        .withColumn("cell_ids", expr("ST_S2CellIDs(geom, 5)"))

      Then("valid should have list of Long type cell ids returned")
      geometryDfWithCellIDs.select("cell_ids").collect().foreach(
        r => require(r.get(0).isInstanceOf[mutable.WrappedArray[Long]] && r.size > 0)
      )
    }

    it("use ST_S2CellIDs for spatial join") {
      Given("DataFrame with valid line strings")
      val polygonDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(geojsonInputLocation)
        .select(expr("ST_GeomFromGeoJSON(_c0)").as("countyshape"))
        .select(
        monotonically_increasing_id.as("id"),
        col("countyshape").as("geom")
      ).limit(100)
      val rightPolygons = polygonDf.filter("id > 50")
      rightPolygons.createOrReplaceTempView("rights")
      // generate a sub list of polygons
      val leftPolygons = polygonDf.filter("id <= 50")
      leftPolygons.createOrReplaceTempView("lefts")
      When("generate the cellIds for both set of polygons, and explode into separate rows, join them by cellIds")
      val joinedDf = sparkSession.sql(
        """
          |with lcs as (
          | select id, geom, explode(ST_S2CellIDs(geom, 15)) as cellId from lefts
          |)
          |, rcs as (
          | select id, geom, explode(ST_S2CellIDs(geom, 15)) as cellId from rights
          |)
          |select sum(if(ST_Intersects(lcs.geom, rcs.geom), 1, 0)) count_true_positive, count(1) count_positive from lcs join rcs on lcs.cellId = rcs.cellId
          |""".stripMargin
      )
      Then("the geoms joined by cell ids should all really intersect in this case." +
        "Note that, cellIds equal doesn't necessarily mean the geoms intersect." +
        "If a coordinate fall on the border of 2 cells, S2 cover it with both cells. Use s2_intersects to filter out false positives")
      val res = joinedDf.collect()(0)
      require(
        res.get(1).asInstanceOf[Long] == 48
      )
      require(
        res.get(0) == res.get(1)
      )
    }
  }
}
