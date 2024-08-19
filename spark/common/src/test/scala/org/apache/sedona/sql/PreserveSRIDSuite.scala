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

import org.apache.sedona.common.Constructors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.mutable

class PreserveSRIDSuite extends TestBaseScala with TableDrivenPropertyChecks {
  private var testDf: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDf = prepareTestDataFrame()
  }

  describe("Preserve SRID") {
    val testCases = Table(
      "test case",
      ("ST_ConcaveHull(geom1, 1, false)", 1000),
      ("ST_ConcaveHull(geom1, 1, true)", 1000),
      ("ST_ConvexHull(geom1)", 1000),
      ("ST_Buffer(geom1, 1)", 1000),
      ("ST_ShiftLongitude(geom1)", 1000),
      ("ST_Envelope(geom1)", 1000),
      ("ST_Expand(geom1, 0)", 1000),
      ("ST_Centroid(geom1)", 1000),
      ("ST_Transform(geom1, 'EPSG:4326', 'EPSG:3857')", 3857),
      ("ST_Intersection(geom1, ST_Point(0, 1))", 1000),
      ("ST_MakeValid(geom1)", 1000),
      ("ST_ReducePrecision(geom1, 6)", 1000),
      ("ST_SimplifyVW(geom1, 0.1)", 1000),
      ("ST_SimplifyPolygonHull(geom1, 0.5)", 1000),
      ("ST_SetSRID(geom1, 2000)", 2000),
      ("ST_LineMerge(geom2)", 1000),
      ("ST_StartPoint(geom3)", 1000),
      ("ST_Snap(geom3, geom3, 0.1)", 1000),
      ("ST_Boundary(geom1)", 1000),
      ("ST_LineSubstring(geom3, 0.1, 0.9)", 1000),
      ("ST_LineInterpolatePoint(geom3, 0.1)", 1000),
      ("ST_EndPoint(geom3)", 1000),
      ("ST_ExteriorRing(geom1)", 1000),
      ("ST_GeometryN(geom2, 2)", 1000),
      ("ST_InteriorRingN(geom4, 0)", 1000),
      ("ST_Dump(geom2)", 1000),
      ("ST_DumpPoints(geom2)", 1000),
      ("ST_AddMeasure(geom3, 0, 1)", 1000),
      ("ST_AddPoint(geom3, ST_Point(0.5, 0.5), 1)", 1000),
      ("ST_RemovePoint(geom3, 1)", 1000),
      ("ST_SetPoint(geom3, 1, ST_Point(0.5, 0.5))", 1000),
      ("ST_ClosestPoint(geom1, geom2)", 1000),
      ("ST_FlipCoordinates(geom1)", 1000),
      ("ST_SubDivide(geom4, 4)", 1000),
      ("ST_MakeLine(geom3, geom3)", 1000),
      ("ST_Points(geom1)", 1000),
      ("ST_Polygon(ST_InteriorRingN(geom4, 0), 2000)", 2000),
      ("ST_Polygonize(geom5)", 1000),
      ("ST_MakePolygon(ST_ExteriorRing(geom4), ARRAY(ST_InteriorRingN(geom4, 0)))", 1000),
      ("ST_Difference(geom1, geom2)", 1000),
      ("ST_SymDifference(geom1, geom2)", 1000),
      ("ST_UnaryUnion(geom5)", 1000),
      ("ST_Union(geom1, geom2)", 1000),
      ("ST_Union(ARRAY(geom1, geom2))", 1000),
      ("ST_Multi(geom1)", 1000),
      ("ST_PointOnSurface(geom1)", 1000),
      ("ST_Reverse(geom1)", 1000),
      ("ST_PointN(geom3, 1)", 1000),
      ("ST_Force_2D(ST_AddMeasure(geom3, 0, 1))", 1000),
      ("ST_BuildArea(geom5)", 1000),
      ("ST_Normalize(geom5)", 1000),
      ("ST_LineFromMultiPoint(ST_Points(geom1))", 1000),
      ("ST_Split(geom1, ST_MakeLine(ST_Point(0.5, -10), ST_Point(0.5, 10)))", 1000),
      ("ST_CollectionExtract(geom5, 2)", 1000),
      ("ST_GeometricMedian(ST_Points(geom5))", 1000),
      ("ST_LocateAlong(ST_AddMeasure(geom3, 0, 1), 0.25)", 1000),
      ("ST_LongestLine(geom1, geom2)", 1000),
      ("ST_Force3D(geom1, 10)", 1000),
      ("ST_Force3DZ(geom1, 10)", 1000),
      ("ST_Force3DM(geom1, 10)", 1000),
      ("ST_Force4D(geom1, 5, 10)", 1000),
      ("ST_ForceCollection(geom1)", 1000),
      ("ST_ForcePolygonCW(ST_ForcePolygonCCW(geom1))", 1000),
      ("ST_ForcePolygonCCW(ST_ForcePolygonCW(geom1))", 1000),
      ("ST_Translate(geom1, 1, 2, 3)", 1000),
      ("ST_TriangulatePolygon(geom4)", 1000),
      ("ST_VoronoiPolygons(geom4)", 1000),
      ("ST_Affine(geom1, 1, 2, 1, 2, 1, 2)", 1000),
      ("ST_BoundingDiagonal(geom1)", 1000),
      ("ST_DelaunayTriangles(geom4)", 1000),
      ("ST_Rotate(geom1, 10)", 1000),
      ("ST_RotateX(geom1, 10)", 1000),
      ("ST_Collect(geom1, geom2, geom3)", 1000),
      ("ST_GeneratePoints(geom1, 3)", 1000))

    forAll(testCases) { case (expression: String, srid: Int) =>
      it(s"$expression") {
        testDf.selectExpr(expression).collect().foreach { row =>
          val value = row.getAs[AnyRef](0)
          value match {
            case geom: Geometry => assert(geom.getSRID == srid)
            case geoms: mutable.WrappedArray[Geometry] =>
              geoms.foreach(geom => assert(geom.getSRID == srid))
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
        StructField("geom1", GeometryUDT),
        StructField("geom2", GeometryUDT),
        StructField("geom3", GeometryUDT),
        StructField("geom4", GeometryUDT),
        StructField("geom5", GeometryUDT)))
    val geom1 = Constructors.geomFromWKT("POLYGON ((0 0, 1 0, 0.5 0.5, 1 1, 0 1, 0 0))", 1000)
    val geom2 =
      Constructors.geomFromWKT("MULTILINESTRING ((0 0, 0 1), (0 1, 1 1), (1 1, 1 0))", 1000)
    val geom3 = Constructors.geomFromWKT("LINESTRING (0 0, 0 1, 1 1, 1 0)", 1000)
    val geom4 = Constructors.geomFromWKT(
      "POLYGON (( 30 10, 40 40, 20 40, 10 20, 30 10 ), ( 20 30, 35 35, 30 20, 20 30 ))",
      1000)
    val geom5 = Constructors.geomFromWKT(
      """GEOMETRYCOLLECTION (LINESTRING (2 0, 2 1, 2 2),
        |LINESTRING (2 2, 2 3, 2 4), LINESTRING (0 2, 1 2, 2 2),
        |LINESTRING (2 2, 3 2, 4 2), LINESTRING (0 2, 1 3, 2 4),
        |LINESTRING (2 4, 3 3, 4 2))""".stripMargin,
      1000)
    val rows = Seq(Row(geom1, geom2, geom3, geom4, geom5))
    sparkSession.createDataFrame(rows.asJava, schema)
  }
}
