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
package org.apache.sedona.sql.geography

import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.common.geography.Constructors
import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.types.{StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.scalatest.prop.TableDrivenPropertyChecks

/**
 * Geography counterpart of [[org.apache.sedona.sql.PreserveSRIDSuite]]. Verifies that the
 * Geography expression chain preserves the input SRID through the InferredExpression boundary.
 * Geography→Geography functions (ST_Centroid, ST_Envelope, ST_Buffer, ST_GeomToGeography) are
 * tested directly. Scalar/predicate functions on Geography inputs are wrapped in an identity
 * passthrough — `IF(<scalar/pred>, geog1, geog1)` — so the surrounding expression returns a
 * Geography whose SRID can be asserted; failure of such a row signals either an evaluation
 * failure on the wrapped function or SRID being dropped somewhere in the chain.
 */
class PreserveSRIDGeographySuite extends TestBaseScala with TableDrivenPropertyChecks {
  private var testDf: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDf = prepareTestDataFrame()
  }

  describe("Preserve SRID (Geography)") {
    val testCases = Table(
      "test case",
      // Direct Geography→Geography
      ("ST_Centroid(geog1)", 1000),
      ("ST_Envelope(geog1)", 1000),
      ("ST_Envelope(geog1, true)", 1000),
      ("ST_Envelope(geog1, false)", 1000),
      ("ST_Buffer(geog1, 0)", 1000),
      ("ST_Buffer(geog1, 100)", 1000),
      ("ST_Buffer(geog1, 100, 'quad_segs=8')", 1000),
      // Cross-type boundaries
      ("ST_GeomToGeography(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 1000))", 1000),
      ("ST_GeogToGeometry(geog1)", 1000),
      // Predicates wrapped in identity passthrough
      ("IF(ST_Intersects(geog1, geog2), geog1, geog1)", 1000),
      ("IF(ST_Within(geog1, geog1), geog1, geog1)", 1000),
      ("IF(ST_DWithin(geog1, geog2, 1000.0), geog1, geog1)", 1000),
      ("IF(ST_Contains(geog1, geog1), geog1, geog1)", 1000),
      ("IF(ST_Equals(geog1, geog1), geog1, geog1)", 1000),
      // Scalar/string functions wrapped in identity passthrough
      ("IF(ST_Length(geog3) >= 0, geog1, geog1)", 1000),
      ("IF(ST_Area(geog1) >= 0, geog1, geog1)", 1000),
      ("IF(ST_Distance(geog1, geog2) >= 0, geog1, geog1)", 1000),
      ("IF(ST_NPoints(geog1) > 0, geog1, geog1)", 1000),
      ("IF(ST_NumGeometries(geog1) > 0, geog1, geog1)", 1000),
      ("IF(ST_GeometryType(geog1) IS NOT NULL, geog1, geog1)", 1000),
      ("IF(ST_AsText(geog1) IS NOT NULL, geog1, geog1)", 1000),
      ("IF(ST_AsEWKT(geog1) IS NOT NULL, geog1, geog1)", 1000))

    forAll(testCases) { case (expression: String, srid: Int) =>
      it(s"$expression") {
        testDf.selectExpr(expression).collect().foreach { row =>
          val value = row.getAs[AnyRef](0)
          value match {
            case geog: Geography => assert(geog.getSRID == srid)
            case geom: Geometry => assert(geom.getSRID == srid)
            case _ => fail(s"Unexpected result: $value")
          }
        }
      }
    }
  }

  private def prepareTestDataFrame(): DataFrame = {
    import scala.collection.JavaConverters._

    val schema = StructType(
      Seq(
        StructField("geog1", GeographyUDT()),
        StructField("geog2", GeographyUDT()),
        StructField("geog3", GeographyUDT())))
    val geog1 =
      Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 1000)
    val geog2 =
      Constructors.geogFromWKT("MULTILINESTRING ((0 0, 0 1), (0 1, 1 1))", 1000)
    val geog3 =
      Constructors.geogFromWKT("LINESTRING (0 0, 0 1, 1 1, 1 0)", 1000)
    val rows = Seq(Row(geog1, geog2, geog3))
    sparkSession.createDataFrame(rows.asJava, schema)
  }
}
