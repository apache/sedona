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
import org.apache.spark.sql.functions.{col, expr, lit, when}
import org.scalatest.{GivenWhenThen, Matchers}

class StMakePolygonSpec extends TestBaseScala with Matchers with GeometrySample with GivenWhenThen {
  import sparkSession.implicits._

  describe("should pass ST_MAkePolygon"){

    it("should return null while using ST_MakePolygon when geometry is empty"){
      Given("DataFrame with null line strings")
      val geometryTable = sparkSession.sparkContext.parallelize(1 to 10).toDF()
        .withColumn("geom", lit(null))

      When("using ST_MakePolygon on null geometries")
      val geometryTableWithPolygon = geometryTable
        .withColumn("polygon", expr("ST_MakePolygon(geom)"))

      Then("no exception should be raised")

      And("null geometry should be assigned")
      val result = geometryTableWithPolygon
        .select(when(col("polygon").isNull, lit(null)).otherwise(expr("ST_AsText(polygon)")))
        .as[String].distinct().collect()
        .toList

      result.size shouldBe 1
      result.head shouldBe null
    }

    it("should correctly create polygon based on given linestring with use of ST_MakePolygon"){
      Given("DataFrame with valid line strings")
      val geometryTable = Seq(
        "LINESTRING(75 29,77 29,77 29, 75 29)",
        "LINESTRING(-5 8, -6 1, -8 6, -2 5, -6 1, -5 8)",
        "LINESTRING(-3 -7, -8 -7, -3 -2, -8 -2, -3 -7)"

      ).map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

      When("using ST_MakePolygon on those geometries")
      val geometryDfWithPolygon = geometryTable
        .withColumn("poly", expr("ST_MakePolygon(geom)"))

      Then("valid Polygon should be created")
      geometryDfWithPolygon.selectExpr("ST_AsText(poly)").as[String]
        .collect() should contain theSameElementsAs Seq(
        "POLYGON ((75 29, 77 29, 77 29, 75 29))",
        "POLYGON ((-5 8, -6 1, -8 6, -2 5, -6 1, -5 8))",
        "POLYGON ((-3 -7, -8 -7, -3 -2, -8 -2, -3 -7))"
      )

    }

    it("should correctly create polygon based on given linestring and holes"){
      Given("DataFrame with valid line strings and list of line string holes")
      val geometryDataFrame = Seq(
        ("LINESTRING(0 5, 1 7, 2 9, 2 5, 5 7, 4 6, 3 2, 1 3, 0 5)", Seq("LINESTRING(2 3, 1 4, 2 4, 2 3)", "LINESTRING(2 4, 3 5, 3 4, 2 4)")),
          ("LINESTRING(7 -1, 7 6, 9 6, 9 1, 7 -1)", Seq("LINESTRING(6 2, 8 2, 8 1, 6 1, 6 2)")),
        ("LINESTRING(2 -2, 6 -2, 6 -7, 2 -7, 2 -2)", Seq("LINESTRING(6 2, 8 2, 8 1, 6 1, 6 2)"))
      ).map{
        case (geom, geometries) => Tuple2(
          wktReader.read(geom),
          geometries.map(wktReader.read)
        )}.toDF("geom", "holes")

      When("using ST_MakePolygon on those geometries")
      val transformedGeometriesWithHoles = geometryDataFrame
        .withColumn("poly", expr("ST_MakePolygon(geom, holes)"))
        .selectExpr("ST_AsText(poly)")
        .as[String]
        .collect()
        .toList

      Then("valid polygon with holes should be created")
      transformedGeometriesWithHoles should contain theSameElementsAs Seq(
        "POLYGON ((0 5, 1 7, 2 9, 2 5, 5 7, 4 6, 3 2, 1 3, 0 5), (2 3, 1 4, 2 4, 2 3), (2 4, 3 5, 3 4, 2 4))",
        "POLYGON ((7 -1, 7 6, 9 6, 9 1, 7 -1), (6 2, 8 2, 8 1, 6 1, 6 2))",
        "POLYGON ((2 -2, 6 -2, 6 -7, 2 -7, 2 -2), (6 2, 8 2, 8 1, 6 1, 6 2))"
      )
    }

    it("should return null values when calling ST_MakePolygon on given geometries which are not valid"){
      Given("line strings which can not be valid polygon")
      val geometryDataFrame = Seq(
        "LINESTRING(2 1,3 4,4 6)",
        "POINT(21 52)"
      ).map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

      When("calling ST_MakePolygon")
      val geometryDfWithPolygon = geometryDataFrame
        .withColumn("poly", expr("ST_MakePolygon(geom)"))

      Then("result should be null")
      val result = geometryDfWithPolygon.select("poly")
        .select(when(col("poly").isNull, lit(null)).otherwise(expr("ST_AsText(poly)")))
        .as[String].distinct().collect()
        .toList

      result.size shouldBe 1

      result.head shouldBe null
    }

    it("should allow to use function with use of sql"){
      Given("sql query with ST_MakePolygon")
      val mkPolygon = sparkSession.sql(
        """
            SELECT
              ST_MakePolygon(
              ST_GeomFromText('LINESTRING(7 -1, 7 6, 9 6, 9 1, 7 -1)'),
              ARRAY(ST_GeomFromText('LINESTRING(6 2, 8 2, 8 1, 6 1, 6 2)'))
              ) AS polygon
            """
      )

      Then("after evaluating count should be appropriate")
      mkPolygon.show(1, false)
      mkPolygon.count() shouldBe 1
    }
  }
}
