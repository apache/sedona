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

package org.apache.sedona.sql.ogc

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.expr
import org.scalatest.BeforeAndAfterAll

/**
 * Test for section GeometryTypes And Functions in OGC simple feature implementation specification for SQL.
 */
class GeometryTypesAndFunctionsTestScala extends TestBaseScala with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession.createDataFrame(Seq((119, "Route 75", 4, "MULTILINESTRING((10 48, 10 21, 10 0), (16 0,16 23,16 48))")))
      .toDF("fid", "name", "num_lanes", "centerlines")
      .withColumn("centerlines", expr("ST_GeomFromWKT(centerlines, 101)"))
      .createOrReplaceTempView("divided_routes")

    // A possible bug in the test specification, INSERT specifies 'BLUE LAKE' but test use 'Blue Lake'.
    sparkSession.createDataFrame(Seq((101, "Blue Lake", "POLYGON((52 18,66 23,73 9,48 6,52 18), (59 18,67 18,67 13,59 13,59 18))")))
      .toDF("fid", "name", "shore")
      .withColumn("shore", expr("ST_GeomFromWKT(shore, 101)"))
      .createOrReplaceTempView("lakes")

    sparkSession.createDataFrame(Seq((109, "Green Forest", "MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)), ((59 18,67 18,67 13,59 13,59 18)))")))
      .toDF("fid", "name", "boundary")
      .withColumn("boundary", expr("ST_GeomFromWKT(boundary, 101)"))
      .createOrReplaceTempView("forests")

    sparkSession.createDataFrame(Seq(
      (111, "Cam Stream", "LINESTRING( 38 48, 44 41, 41 36, 44 31, 52 18 )"),
      (112, null, "LINESTRING( 76 0, 78 4, 73 9 )")))
      .toDF("fid", "name", "centerline")
      .withColumn("centerline", expr("ST_GeomFromWKT(centerline, 101)"))
      .createOrReplaceTempView("streams")

    sparkSession.createDataFrame(Seq((120, null, "Stock Pond", "MULTIPOLYGON( ( ( 24 44, 22 42, 24 40, 24 44) ), ( ( 26 44, 26 40, 28 42, 26 44) ) )")))
      .toDF("fid", "name", "type", "shores")
      .withColumn("shores", expr("ST_GeomFromWKT(shores, 101)"))
      .createOrReplaceTempView("ponds")

    sparkSession.createDataFrame(Seq(
      (117, "Ashton", "POLYGON((62 48, 84 48, 84 30, 56 30, 56 34, 62 48))"),
      (118, "Goose Island", "POLYGON((67 13, 67 18, 59 18, 59 13, 67 13))")))
      .toDF("fid", "name", "boundary")
      .withColumn("boundary", expr("ST_GeomFromWKT(boundary, 101)"))
      .createOrReplaceTempView("named_places")

    sparkSession.createDataFrame(Seq(
      (102, "Route 5", null, 2, "LINESTRING( 0 18, 10 21, 16 23, 28 26, 44 31 )"),
      (103, "Route 5", "Main Street", 4, "LINESTRING( 44 31, 56 34, 70 38 )"),
      (104, "Route 5", null, 2, "LINESTRING( 70 38, 72 48 )"),
      (105, "Main Street", null, 4, "LINESTRING( 70 38, 84 42 )"),
      (106, "Dirt Road by Green Forest", null, 1, "LINESTRING( 28 26, 28 0 )")))
      .toDF("fid", "name", "aliases", "num_lanes", "centerline")
      .withColumn("centerline", expr("ST_GeomFromWKT(centerline, 101)"))
      .createOrReplaceTempView("road_segments")

    sparkSession.createDataFrame(Seq(
      (113, "123 Main Street", "POINT(52 30)", "POLYGON((50 31, 54 31, 54 29, 50 29, 50 31) )"),
      (114, "215 Main Street", "POINT(64 33)", "POLYGON((66 34, 62 34, 62 32, 66 32, 66 34) )")))
      .toDF("fid", "address", "position", "footprint")
      .withColumn("position", expr("ST_GeomFromWKT(position, 101)"))
      .withColumn("footprint", expr("ST_GeomFromWKT(footprint, 101)"))
      .createOrReplaceTempView("buildings")

    sparkSession.createDataFrame(Seq((110, "Cam Bridge", "POINT( 44 31 )")))
      .toDF("fid", "name", "position")
      .withColumn("position", expr("ST_GeomFromWKT(position, 101)"))
      .createOrReplaceTempView("bridges")
  }

  describe("Sedona-SQL OGC Geometry types and functions") {
    ignore("T1-T5") {
      // Tests T1-T5 should be implemented for completeness.
      // The tests do insert and select on srid and geometry_columns tables and should pass on any SQL implementation.
    }
    ignore("T6") {
      // ST_Dimension is not implemented in Sedona
      val actual = sparkSession.sql(
        """
          |SELECT ST_Dimension(shore)
          |FROM lakes
          |WHERE name = 'Blue Lake';
          |""".stripMargin).first().getInt(0)
      assert(actual == 2)
    }
    ignore("T7") {
      // ST_GeometryType is SQL/MM compliant but not OGC compliant.
      // Postgis has solved this by implementing both ST_GeometryType (SQL/MM) and GeometryType (OGC).

      // A possible bug in the test specification (C.3.3.3 Geometry types and functions schema test queries).
      // Query specifies lakes but probably should be divided_routes.
      val actual = sparkSession.sql(
        """
          |SELECT GeometryType(centerlines)
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getString(0)
      assert(actual == "MULTILINESTRING")
    }
    it("T8") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(boundary)
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((67 13, 67 18, 59 18, 59 13, 67 13))")
    }
    it("T9") {
      // Specification uses PolyFromWKB with SRID instead of ST_GeomFromWKB.
      // The test verifies ST_AsBinary compliance.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(boundary)))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((67 13, 67 18, 59 18, 59 13, 67 13))")
    }
    it("T10") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_SRID(boundary)
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getInt(0)
      assert(actual == 101)
    }
    it("T11") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_IsEmpty(centerline)
          |FROM road_segments
          |WHERE name = 'Route 5'
          |AND aliases = 'Main Street'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == false)
    }
    it("T12") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_IsSimple(shore)
          |FROM lakes
          |WHERE name = 'Blue Lake';
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T13") {
      // The test specification contains slightly different and invalid SQL.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Boundary(boundary))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getString(0)
      assert(actual == "LINESTRING (67 13, 67 18, 59 18, 59 13, 67 13)")
    }
    it("T14") {
      // The test specification contains slightly different and invalid SQL.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Envelope(boundary))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((59 13, 59 18, 67 18, 67 13, 59 13))")
    }
    it("T15") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_X(position)
          |FROM bridges
          |WHERE name = 'Cam Bridge';
          |""".stripMargin).first().getDouble(0)
      assert(actual == 44.00)
    }
    it("T16") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Y(position)
          |FROM bridges
          |WHERE name = 'Cam Bridge';
          |""".stripMargin).first().getDouble(0)
      assert(actual == 31.00)
    }
    it("T17") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_StartPoint(centerline))
          |FROM road_segments
          |WHERE fid = 102
          |""".stripMargin).first().getString(0)
      assert(actual == "POINT (0 18)")
    }
    it("T18") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_EndPoint(centerline))
          |FROM road_segments
          |WHERE fid = 102
          |""".stripMargin).first().getString(0)
      assert(actual == "POINT (44 31)")
    }
    it("T19") {
      // The test specification contains slightly different and redundant SQL.
      val actual = sparkSession.sql(
        """
          |SELECT ST_IsClosed(ST_Boundary(boundary))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T20") {
      // The test specification contains slightly different and redundant SQL.
      val actual = sparkSession.sql(
        """
          |SELECT ST_IsRing(ST_Boundary(boundary))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T21") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Length(centerline)
          |FROM road_segments
          |WHERE fid = 106
          |""".stripMargin).first().getDouble(0)
      assert(actual == 26.00)
    }
    ignore("T22") {
      // ST_NumPoints is not implemented in Sedona.
      val actual = sparkSession.sql(
        """
          |SELECT ST_NumPoints(centerline)
          |FROM road_segments
          |WHERE fid = 102
          |""".stripMargin).first().getInt(0)
      assert(actual == 5)
    }
    it("T23") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_PointN(centerline, 1))
          |FROM road_segments
          |WHERE fid = 102
          |""".stripMargin).first().getString(0)
      assert(actual == "POINT (0 18)")
    }
    it("T24") {
      // Probable error in the test specification.
      // Both Sedona and PostGIS agrees that the point should be 63 15.5 and not 53 15.5.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Centroid(boundary))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getString(0)
      assert(actual == "POINT (63 15.5)")
    }
    it("T25") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Contains(boundary,ST_PointOnSurface(boundary))
          |FROM named_places
          |WHERE name = 'Goose Island'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T26") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Area(boundary)
          |FROM named_places
          |WHERE name = 'Goose Island'
          |""".stripMargin).first().getDouble(0)
      assert(actual == 40.00)
    }
    it("T27") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_ExteriorRing(shore))
          |FROM lakes
          |WHERE name = 'Blue Lake';
          |""".stripMargin).first().getString(0)
      assert(actual == "LINESTRING (52 18, 66 23, 73 9, 48 6, 52 18)")
    }
    it("T28") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_NumInteriorRings(shore)
          |FROM lakes
          |WHERE name = 'Blue Lake';
          |""".stripMargin).first().getInt(0)
      assert(actual == 1)
    }
    ignore("T29") {
      // Interor rings are 1 indexed in OGC and PostGIS.
      // In Sedona they are 0 indexed.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_InteriorRingN(shore, 1))
          |FROM lakes
          |WHERE name = 'Blue Lake';
          |""".stripMargin).first().getString(0)
      assert(actual == "LINESTRING (59 18, 67 18, 67 13, 59 13, 59 18)")
    }
    it("T30") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_NumGeometries(centerlines)
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getInt(0)
      assert(actual == 2)
    }
    ignore("T31") {
      // Interor rings are 1 indexed in OGC and PostGIS.
      // In Sedona they are 0 indexed.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_GeometryN(centerlines, 2))
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getString(0)
      assert(actual == "LINESTRING (16 0, 16 23, 16 48)")
    }
    it("T32") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_IsClosed(centerlines)
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getBoolean(0)
      assert(actual == false)
    }
    it("T33") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Length(centerlines)
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getDouble(0)
      assert(actual == 96.00)
    }
    it("T34") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Centroid(shores))
          |FROM ponds
          |WHERE fid = 120;
          |""".stripMargin).first().getString(0)
      assert(actual == "POINT (25 42)")
    }
    it("T35") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Contains(shores, ST_PointOnSurface(shores))
          |FROM ponds
          |WHERE fid = 120;
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T36") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Area(shores)
          |FROM ponds
          |WHERE fid = 120;
          |""".stripMargin).first().getDouble(0)
      assert(actual == 8.00)
    }
    it("T37") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Equals(boundary,ST_GeomFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )',1))
          |FROM named_places
          |WHERE name = 'Goose Island';
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T38") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Disjoint(centerlines, boundary)
          |FROM divided_routes, named_places
          |WHERE divided_routes.name = 'Route 75'
          |AND named_places.name = 'Ashton'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T39") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Touches(centerline, shore)
          |FROM streams, lakes
          |WHERE streams.name = 'Cam Stream'
          |AND lakes.name = 'Blue Lake'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T40") {
      // Test specification has the order of the arguments reversed which is probably wrong.
      // The OGC tests in PostGIS has made the same interpretation.
      val actual = sparkSession.sql(
        """
          |SELECT ST_Within(footprint, boundary)
          |FROM named_places, buildings
          |WHERE named_places.name = 'Ashton'
          |AND buildings.address = '215 Main Street'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T41") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Overlaps(forests.boundary, named_places.boundary)
          |FROM forests, named_places
          |WHERE forests.name = 'Green Forest'
          |AND named_places.name = 'Ashton'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T42") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Crosses(road_segments.centerline, divided_routes.centerlines)
          |FROM road_segments, divided_routes
          |WHERE road_segments.fid = 102
          |AND divided_routes.name = 'Route 75'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T43") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Intersects(road_segments.centerline, divided_routes.centerlines)
          |FROM road_segments, divided_routes
          |WHERE road_segments.fid = 102
          |AND divided_routes.name = 'Route 75'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T44") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Contains(forests.boundary, named_places.boundary)
          |FROM forests, named_places
          |WHERE forests.name = 'Green Forest'
          |AND named_places.name = 'Ashton'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == false)
    }
    ignore("T45") {
      // This function is not implemented in Sedona.
      val actual = sparkSession.sql(
        """
          |SELECT ST_Relate(forests.boundary, named_places.boundary,'TTTTTTTTT')
          |FROM forests, named_places
          |WHERE forests.name = 'Green Forest'
          |AND named_places.name = 'Ashton'
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T46") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_Distance(position, boundary)
          |FROM bridges, named_places
          |WHERE bridges.name = 'Cam Bridge'
          |AND named_places.name = 'Ashton'
          |""".stripMargin).first().getDouble(0)
      assert(actual == 12)
    }
    it("T47") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Intersection(centerline, shore))
          |FROM streams, lakes
          |WHERE streams.name = 'Cam Stream'
          |AND lakes.name = 'Blue Lake'
          |""".stripMargin).first().getString(0)
      assert(actual == "POINT (52 18)")
    }
    it("T48") {
      // The test answer should be "POLYGON ((56 34, 62 48, 84 48, 84 42, 56 34))".
      // Sedona returns the same polygon but with a different starting point.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Difference(named_places.boundary, forests.boundary))
          |FROM named_places, forests
          |WHERE named_places.name = 'Ashton'
          |AND forests.name = 'Green Forest'
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((62 48, 84 48, 84 42, 56 34, 62 48))")
    }
    it("T49") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_Union(shore, boundary))
          |FROM lakes, named_places
          |WHERE lakes.name = 'Blue Lake'
          |AND named_places.name = 'Goose Island'
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((52 18, 66 23, 73 9, 48 6, 52 18))")
    }
    it("T50") {
      // Test specification uses 'Goose Island' but test SQL uses 'Ashton' as named_places.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_SymDifference(shore, boundary))
          |FROM lakes, named_places
          |WHERE lakes.name = 'Blue Lake'
          |AND named_places.name = 'Goose Island'
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((52 18, 66 23, 73 9, 48 6, 52 18))")
    }
    it("T51") {
      val actual = sparkSession.sql(
        """
          |SELECT count(*)
          |FROM buildings, bridges
          |WHERE ST_Contains(ST_Buffer(bridges.position, 15.0), buildings.footprint)
          |""".stripMargin).first().getLong(0)
      assert(actual == 1)
    }
    it("T52") {
      // Sedona returns the same polygon but with a different starting point.
      val actual = sparkSession.sql(
        """
          |SELECT ST_AsText(ST_ConvexHull(shore))
          |FROM lakes
          |WHERE lakes.name = 'Blue Lake'
          |""".stripMargin).first().getString(0)
      assert(actual == "POLYGON ((48 6, 52 18, 66 23, 73 9, 48 6))")
    }
  }
}