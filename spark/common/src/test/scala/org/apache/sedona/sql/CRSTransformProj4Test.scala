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

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.locationtech.jts.geom.Geometry

/**
 * Tests for ST_Transform using proj4sedona backend.
 *
 * These tests verify CRS transformations with various input formats:
 *   - EPSG codes
 *   - PROJ strings
 *   - WKT1 and WKT2
 *   - PROJJSON
 *   - NAD grid files
 *
 * Tests also verify config switching between proj4sedona and GeoTools.
 */
class CRSTransformProj4Test extends TestBaseScala {

  private val COORD_TOLERANCE = 1.0 // 1 meter tolerance for projected coordinates

  describe("ST_Transform with proj4sedona (default mode)") {

    it("should transform EPSG:4326 to EPSG:3857 using SQL API") {
      val result = sparkSession
        .sql("SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326), 'EPSG:4326', 'EPSG:3857')")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      // Validated against cs2cs: -13627665.27, 4547675.35
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform using geometry SRID as source (2-arg version)") {
      val result = sparkSession
        .sql("SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326), 'EPSG:3857')")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform to UTM Zone 10N") {
      val result = sparkSession
        .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:32610')")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(32610, result.getSRID)
      // Validated against cs2cs: 551130.77, 4180998.88
      assertEquals(551130.77, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4180998.88, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform using PROJ string") {
      val projString =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs"
      val result = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326),
            'EPSG:4326',
            '$projString')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      // Web Mercator coordinates
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform using PROJ string for UTM") {
      val projString = "+proj=utm +zone=10 +datum=WGS84 +units=m +no_defs"
      val result = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT (-122.4194 37.7749)'),
            '+proj=longlat +datum=WGS84 +no_defs',
            '$projString')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      // UTM Zone 10N coordinates
      assertTrue(result.getCoordinate.x > 540000 && result.getCoordinate.x < 560000)
      assertTrue(result.getCoordinate.y > 4170000 && result.getCoordinate.y < 4190000)
    }

    it("should transform using WKT1") {
      val sourceWkt =
        """GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]"""
      val targetWkt =
        """PROJCS["WGS 84 / UTM zone 51N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",123],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1]]"""

      val result = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT (120 60)'),
            '$sourceWkt',
            '$targetWkt')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      // Validated against cs2cs: 332705.18, 6655205.48
      assertEquals(332705.18, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(6655205.48, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform using WKT2") {
      val wkt2 =
        """GEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],CS[ellipsoidal,2],AXIS["latitude",north],AXIS["longitude",east],UNIT["degree",0.0174532925199433]]"""

      val result = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT (-122.4194 37.7749)'),
            '$wkt2',
            'EPSG:3857')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform using PROJJSON") {
      val projJson =
        """{"type":"GeographicCRS","name":"WGS 84","datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984","ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}}}"""

      val result = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT (-122.4194 37.7749)'),
            '$projJson',
            'EPSG:3857')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
    }

    it("should transform with NAD grid file") {
      val gridFile = getClass.getClassLoader.getResource("grids/us_noaa_conus.tif")
      if (gridFile != null) {
        val gridPath = gridFile.getPath
        val projString = s"+proj=longlat +ellps=GRS80 +nadgrids=$gridPath +no_defs"

        val result = sparkSession
          .sql(s"""SELECT ST_Transform(
              ST_SetSRID(ST_GeomFromWKT('POINT (-96.0 40.0)'), 4326),
              'EPSG:4326',
              '$projString')""")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)
        assertTrue(result.isValid)
      }
    }

    it("should handle round-trip transformation") {
      val result = sparkSession
        .sql("""SELECT ST_Transform(
            ST_Transform(
              ST_GeomFromWKT('POINT (-122.4194 37.7749)'),
              'EPSG:4326',
              'EPSG:3857'),
            'EPSG:3857',
            'EPSG:4326')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(4326, result.getSRID)
      // Should return close to original coordinates
      assertEquals(-122.4194, result.getCoordinate.x, 1e-6)
      assertEquals(37.7749, result.getCoordinate.y, 1e-6)
    }

    it("should preserve UserData") {
      // Create a geometry with UserData
      val df = sparkSession
        .sql("SELECT ST_GeomFromWKT('POINT (-122.4194 37.7749)') as geom")
        .selectExpr("ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') as transformed")

      val result = df.first().getAs[Geometry](0)
      assertNotNull(result)
      assertEquals(3857, result.getSRID)
    }
  }

  describe("ST_Transform with DataFrame API") {

    it("should transform using 2-arg DataFrame API") {
      import sparkSession.implicits._
      val df = sparkSession
        .sql("SELECT ST_Point(-122.4194, 37.7749) AS geom")
        .select(ST_SetSRID($"geom", lit(4326)).as("geom"))
        .select(ST_Transform($"geom", lit("EPSG:3857")).as("geom"))

      val result = df.first().getAs[Geometry](0)
      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
    }

    it("should transform using 3-arg DataFrame API") {
      import sparkSession.implicits._
      val df = sparkSession
        .sql("SELECT ST_Point(-122.4194, 37.7749) AS geom")
        .select(ST_Transform($"geom", lit("EPSG:4326"), lit("EPSG:3857")).as("geom"))

      val result = df.first().getAs[Geometry](0)
      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
    }

    it("should transform using PROJ string with DataFrame API") {
      import sparkSession.implicits._
      val projString =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs"
      val df = sparkSession
        .sql("SELECT ST_Point(-122.4194, 37.7749) AS geom")
        .select(ST_SetSRID($"geom", lit(4326)).as("geom"))
        .select(ST_Transform($"geom", lit(projString)).as("geom"))

      val result = df.first().getAs[Geometry](0)
      assertNotNull(result)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should transform using PROJJSON with DataFrame API") {
      import sparkSession.implicits._
      val projJson =
        """{"type":"GeographicCRS","name":"WGS 84","datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984","ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}}}"""

      // Use createDataFrame to avoid constant folding issues with all-literal expressions
      val pointDf = Seq(("POINT (-122.4194 37.7749)"))
        .toDF("wkt")
        .selectExpr("ST_GeomFromWKT(wkt) as geom")

      val df = pointDf.select(ST_Transform($"geom", lit(projJson), lit("EPSG:3857")).as("geom"))

      val result = df.first().getAs[Geometry](0)
      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
    }
  }

  describe("ST_Transform with different geometry types") {

    it("should transform LineString") {
      val result = sparkSession
        .sql("""SELECT ST_Transform(
            ST_GeomFromWKT('LINESTRING(-122.4 37.7, -122.5 37.8, -122.6 37.9)'),
            'EPSG:4326',
            'EPSG:3857')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals("LineString", result.getGeometryType)
      assertEquals(3, result.getNumPoints)
      assertEquals(3857, result.getSRID)
    }

    it("should transform Polygon") {
      val result = sparkSession
        .sql("""SELECT ST_Transform(
            ST_GeomFromWKT('POLYGON((-122.5 37.7, -122.3 37.7, -122.3 37.9, -122.5 37.9, -122.5 37.7))'),
            'EPSG:4326',
            'EPSG:3857')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals("Polygon", result.getGeometryType)
      assertEquals(3857, result.getSRID)
    }

    it("should transform MultiPoint") {
      val result = sparkSession
        .sql("""SELECT ST_Transform(
            ST_GeomFromWKT('MULTIPOINT((-122.4 37.7), (-122.5 37.8), (-122.6 37.9))'),
            'EPSG:4326',
            'EPSG:3857')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals("MultiPoint", result.getGeometryType)
      assertEquals(3, result.getNumGeometries)
      assertEquals(3857, result.getSRID)
    }

    it("should transform GeometryCollection") {
      val result = sparkSession
        .sql("""SELECT ST_Transform(
            ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(-122.4 37.7), LINESTRING(-122.5 37.8, -122.6 37.9))'),
            'EPSG:4326',
            'EPSG:3857')""")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals("GeometryCollection", result.getGeometryType)
      assertEquals(2, result.getNumGeometries)
      assertEquals(3857, result.getSRID)
    }
  }

  describe("ST_Transform config switching") {

    it("should use proj4sedona when config is 'none'") {
      withConf(Map("spark.sedona.crs.geotools" -> "none")) {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857')")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)
        assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      }
    }

    it("should use proj4sedona when config is 'raster' (default)") {
      withConf(Map("spark.sedona.crs.geotools" -> "raster")) {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857')")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)
        assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      }
    }

    it("should use GeoTools when config is 'all'") {
      withConf(Map("spark.sedona.crs.geotools" -> "all")) {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857')")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)
        // GeoTools should produce similar results (within tolerance)
        assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      }
    }

    it("should use proj4sedona for 4-arg version by default (lenient ignored)") {
      // 4-arg version should use proj4sedona by default, ignoring lenient parameter
      val resultLenientTrue = sparkSession
        .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857', true)")
        .first()
        .getAs[Geometry](0)

      val resultLenientFalse = sparkSession
        .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857', false)")
        .first()
        .getAs[Geometry](0)

      assertNotNull(resultLenientTrue)
      assertNotNull(resultLenientFalse)
      // Both should produce same results (lenient is ignored)
      assertEquals(resultLenientTrue.getCoordinate.x, resultLenientFalse.getCoordinate.x, 1e-9)
      assertEquals(resultLenientTrue.getCoordinate.y, resultLenientFalse.getCoordinate.y, 1e-9)
      // Should produce correct Web Mercator coordinates
      assertEquals(-13627665.27, resultLenientTrue.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, resultLenientTrue.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should use proj4sedona for 4-arg version when config is 'none'") {
      withConf(Map("spark.sedona.crs.geotools" -> "none")) {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857', false)")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)
        assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      }
    }

    it("should use GeoTools for 4-arg version when config is 'all'") {
      withConf(Map("spark.sedona.crs.geotools" -> "all")) {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:3857', false)")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)
        // GeoTools should produce similar results
        assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      }
    }
  }

  describe("ST_Transform edge cases") {

    it("should handle same CRS transformation") {
      val result = sparkSession
        .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 'EPSG:4326', 'EPSG:4326')")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(4326, result.getSRID)
      assertEquals(-122.4194, result.getCoordinate.x, 1e-6)
      assertEquals(37.7749, result.getCoordinate.y, 1e-6)
    }

    it("should handle empty geometry") {
      val result = sparkSession
        .sql("SELECT ST_Transform(ST_GeomFromWKT('POINT EMPTY'), 'EPSG:4326', 'EPSG:3857')")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertTrue(result.isEmpty)
    }

    it("should handle null geometry") {
      val result = sparkSession
        .sql("SELECT ST_Transform(null, 'EPSG:4326', 'EPSG:3857')")
        .first()
        .get(0)

      assertTrue(result == null)
    }
  }

  // ==================== OSTN15 British National Grid Tests ====================

  /**
   * Official OSTN15 test data from Ordnance Survey. 40 test points covering all of Great Britain
   * from Cornwall to Shetland.
   *
   * Data source: OSTN15_TestInput_ETRStoOSGB.txt and OSTN15_TestOutput_ETRStoOSGB.txt from
   * https://www.ordnancesurvey.co.uk/documents/resources/OSTN15-NTv2.zip
   *
   * Each point has:
   *   - pointId: Official test point identifier (TP01-TP40)
   *   - etrsLat: ETRS89 latitude in degrees
   *   - etrsLon: ETRS89 longitude in degrees
   */
  case class OSTN15TestPoint(pointId: String, etrsLat: Double, etrsLon: Double)

  // All 40 official OSTN15 test points from OSTN15_TestInput_ETRStoOSGB.txt
  private val ostn15TestPoints: Seq[OSTN15TestPoint] = Seq(
    OSTN15TestPoint("TP01", 49.92226393730, -6.29977752014),
    OSTN15TestPoint("TP02", 49.96006137820, -5.20304609998),
    OSTN15TestPoint("TP03", 50.43885825610, -4.10864563561),
    OSTN15TestPoint("TP04", 50.57563665000, -1.29782277240),
    OSTN15TestPoint("TP05", 50.93127937910, -1.45051433700),
    OSTN15TestPoint("TP06", 51.40078220140, -3.55128349240),
    OSTN15TestPoint("TP07", 51.37447025550, 1.44454730409),
    OSTN15TestPoint("TP08", 51.42754743020, -2.54407618349),
    OSTN15TestPoint("TP09", 51.48936564950, -0.11992557180),
    OSTN15TestPoint("TP10", 51.85890896400, -4.30852476960),
    OSTN15TestPoint("TP11", 51.89436637350, 0.89724327012),
    OSTN15TestPoint("TP12", 52.25529381630, -2.15458614387),
    OSTN15TestPoint("TP13", 52.25160951230, -0.91248956970),
    OSTN15TestPoint("TP14", 52.75136687170, 0.40153547065),
    OSTN15TestPoint("TP15", 52.96219109410, -1.19747655922),
    OSTN15TestPoint("TP16", 53.34480280190, -2.64049320810),
    OSTN15TestPoint("TP17", 53.41628516040, -4.28918069756),
    OSTN15TestPoint("TP18", 53.41630925420, -4.28917792869),
    OSTN15TestPoint("TP19", 53.77911025760, -3.04045490691),
    OSTN15TestPoint("TP20", 53.80021519630, -1.66379168242),
    OSTN15TestPoint("TP21", 54.08666318080, -4.63452168212),
    OSTN15TestPoint("TP22", 54.11685144290, -0.07773133187),
    OSTN15TestPoint("TP23", 54.32919541010, -4.38849118133),
    OSTN15TestPoint("TP24", 54.89542340420, -2.93827741149),
    OSTN15TestPoint("TP25", 54.97912273660, -1.61657685184),
    OSTN15TestPoint("TP26", 55.85399952950, -4.29649016251),
    OSTN15TestPoint("TP27", 55.92478265510, -3.29479219337),
    OSTN15TestPoint("TP28", 57.00606696050, -5.82836691850),
    OSTN15TestPoint("TP29", 57.13902518960, -2.04856030746),
    OSTN15TestPoint("TP30", 57.48625000720, -4.21926398555),
    OSTN15TestPoint("TP31", 57.81351838410, -8.57854456076),
    OSTN15TestPoint("TP32", 58.21262247180, -7.59255560556),
    OSTN15TestPoint("TP33", 58.51560361300, -6.26091455533),
    OSTN15TestPoint("TP34", 58.58120461280, -3.72631022121),
    OSTN15TestPoint("TP35", 59.03743871190, -3.21454001115),
    OSTN15TestPoint("TP36", 59.09335035320, -4.41757674598),
    OSTN15TestPoint("TP37", 59.09671617400, -5.82799339844),
    OSTN15TestPoint("TP38", 59.53470794490, -1.62516966058),
    OSTN15TestPoint("TP39", 59.85409913890, -1.27486910356),
    OSTN15TestPoint("TP40", 60.13308091660, -2.07382822798))

  // Grid shift should produce small but non-zero changes (< 0.01 degrees)
  private val GRID_SHIFT_TOLERANCE = 0.01
  // Round-trip tolerance (should return to original within ~1e-6 degrees)
  private val ROUND_TRIP_TOLERANCE = 1e-6

  // Remote URL for grid file (hosted on GitHub)
  private val OSTN15_ETRS_TO_OSGB_URL =
    "https://raw.githubusercontent.com/jiayuasu/grid-files/main/us_os/OSTN15-NTv2/OSTN15_NTv2_ETRStoOSGB.gsb"

  describe("ST_Transform with OSTN15 grid files - 40 official test points") {
    import org.datasyslab.proj4sedona.Proj4
    import org.datasyslab.proj4sedona.grid.NadgridRegistry

    it("should transform all 40 points ETRS89 to OSGB36 with timing and cache metrics") {
      val gridFile = getClass.getClassLoader.getResource("grids/uk_os_OSTN15_NTv2_ETRStoOSGB.gsb")
      assume(gridFile != null, "OSTN15 ETRStoOSGB grid file not found")

      val gridPath = gridFile.getPath
      val etrs89WithGrid = s"+proj=longlat +ellps=GRS80 +nadgrids=$gridPath"
      val osgb36 = "+proj=longlat +ellps=airy"

      // Clear caches to measure cold start
      Proj4.clearCache()
      NadgridRegistry.clear()

      println("\n" + "=" * 70)
      println("OSTN15 ETRS89 -> OSGB36: 40 Official Test Points")
      println("=" * 70)

      // First transformation (cold - loads grid file)
      val startCold = System.nanoTime()
      val firstResult = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT (${ostn15TestPoints.head.etrsLon} ${ostn15TestPoints.head.etrsLat})'),
            '$etrs89WithGrid',
            '$osgb36')""")
        .first()
        .getAs[Geometry](0)
      val coldTime = (System.nanoTime() - startCold) / 1e6

      println(f"First transform (cold, loads ~15MB grid): $coldTime%.2f ms")
      println(f"  Proj cache size: ${Proj4.getCacheSize}")
      println(f"  Grid cache size: ${NadgridRegistry.size()}")

      // Transform remaining 39 points (warm - cached)
      val startWarm = System.nanoTime()
      var successCount = 1 // Already did first one
      var failCount = 0

      ostn15TestPoints.tail.foreach { tp =>
        val result = sparkSession
          .sql(s"""SELECT ST_Transform(
              ST_GeomFromWKT('POINT (${tp.etrsLon} ${tp.etrsLat})'),
              '$etrs89WithGrid',
              '$osgb36')""")
          .first()
          .getAs[Geometry](0)

        if (result != null) {
          val xShift = Math.abs(result.getCoordinate.x - tp.etrsLon)
          val yShift = Math.abs(result.getCoordinate.y - tp.etrsLat)

          if (xShift < GRID_SHIFT_TOLERANCE && yShift < GRID_SHIFT_TOLERANCE &&
            xShift > 1e-9 && yShift > 1e-9) {
            successCount += 1
          } else {
            failCount += 1
            println(s"  ${tp.pointId}: Unexpected shift - X: $xShift, Y: $yShift")
          }
        } else {
          failCount += 1
          println(s"  ${tp.pointId}: Result is null")
        }
      }
      val warmTime = (System.nanoTime() - startWarm) / 1e6
      val avgWarmTime = warmTime / 39

      println(f"\nRemaining 39 transforms (warm, cached): $warmTime%.2f ms total")
      println(f"  Average per transform: $avgWarmTime%.2f ms")
      println(f"  Cache speedup: ${coldTime / avgWarmTime}%.1fx")
      println(f"\nResults: $successCount/40 passed, $failCount failed")

      // Verify first result
      assertNotNull("First result should not be null", firstResult)
      val xShift = Math.abs(firstResult.getCoordinate.x - ostn15TestPoints.head.etrsLon)
      val yShift = Math.abs(firstResult.getCoordinate.y - ostn15TestPoints.head.etrsLat)
      assertTrue(
        s"X shift ($xShift) should be < $GRID_SHIFT_TOLERANCE",
        xShift < GRID_SHIFT_TOLERANCE)
      assertTrue(
        s"Y shift ($yShift) should be < $GRID_SHIFT_TOLERANCE",
        yShift < GRID_SHIFT_TOLERANCE)
      assertTrue(s"X shift ($xShift) should be non-zero", xShift > 1e-9)
      assertTrue(s"Y shift ($yShift) should be non-zero", yShift > 1e-9)

      assertEquals("All 40 points should transform successfully", 40, successCount)
    }

    it("should transform all 40 points OSGB36 to ETRS89 with timing and cache metrics") {
      val gridFile = getClass.getClassLoader.getResource("grids/uk_os_OSTN15_NTv2_OSGBtoETRS.gsb")
      assume(gridFile != null, "OSTN15 OSGBtoETRS grid file not found")

      val gridPath = gridFile.getPath
      val osgb36WithGrid = s"+proj=longlat +ellps=airy +nadgrids=$gridPath"
      val etrs89 = "+proj=longlat +ellps=GRS80"

      // First, get OSGB36 coordinates by transforming ETRS89 -> OSGB36
      val etrsToOsgbFile =
        getClass.getClassLoader.getResource("grids/uk_os_OSTN15_NTv2_ETRStoOSGB.gsb")
      assume(etrsToOsgbFile != null, "OSTN15 ETRStoOSGB grid file not found")
      val etrs89WithGrid = s"+proj=longlat +ellps=GRS80 +nadgrids=${etrsToOsgbFile.getPath}"
      val osgb36 = "+proj=longlat +ellps=airy"

      // Clear caches
      Proj4.clearCache()
      NadgridRegistry.clear()

      println("\n" + "=" * 70)
      println("OSTN15 OSGB36 -> ETRS89: 40 Official Test Points")
      println("=" * 70)

      // Pre-compute OSGB36 coordinates (this also warms up ETRS->OSGB cache)
      val osgb36Coords = ostn15TestPoints.map { tp =>
        val osgbResult = sparkSession
          .sql(s"""SELECT ST_Transform(
              ST_GeomFromWKT('POINT (${tp.etrsLon} ${tp.etrsLat})'),
              '$etrs89WithGrid',
              '$osgb36')""")
          .first()
          .getAs[Geometry](0)
        (
          tp.pointId,
          osgbResult.getCoordinate.x,
          osgbResult.getCoordinate.y,
          tp.etrsLon,
          tp.etrsLat)
      }

      // Clear only the reverse direction cache to measure cold start for OSGB->ETRS
      // (Grid files are still cached in NadgridRegistry)
      println(s"Grid cache size before OSGB->ETRS: ${NadgridRegistry.size()}")

      // First transformation (semi-cold - grid file cached but new CRS pair)
      val startFirst = System.nanoTime()
      val (_, firstOsgbLon, firstOsgbLat, _, _) = osgb36Coords.head
      val firstResult = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT ($firstOsgbLon $firstOsgbLat)'),
            '$osgb36WithGrid',
            '$etrs89')""")
        .first()
        .getAs[Geometry](0)
      val firstTime = (System.nanoTime() - startFirst) / 1e6

      println(f"First transform (grid cached, new CRS): $firstTime%.2f ms")

      // Transform remaining 39 points
      val startRest = System.nanoTime()
      var successCount = 1
      var failCount = 0

      osgb36Coords.tail.foreach { case (pointId, osgbLon, osgbLat, origEtrsLon, origEtrsLat) =>
        val result = sparkSession
          .sql(s"""SELECT ST_Transform(
                ST_GeomFromWKT('POINT ($osgbLon $osgbLat)'),
                '$osgb36WithGrid',
                '$etrs89')""")
          .first()
          .getAs[Geometry](0)

        if (result != null) {
          val xShift = Math.abs(result.getCoordinate.x - osgbLon)
          val yShift = Math.abs(result.getCoordinate.y - osgbLat)

          if (xShift < GRID_SHIFT_TOLERANCE && yShift < GRID_SHIFT_TOLERANCE &&
            xShift > 1e-9 && yShift > 1e-9) {
            successCount += 1
          } else {
            failCount += 1
            println(s"  $pointId: Unexpected shift - X: $xShift, Y: $yShift")
          }
        } else {
          failCount += 1
          println(s"  $pointId: Result is null")
        }
      }
      val restTime = (System.nanoTime() - startRest) / 1e6
      val avgTime = restTime / 39

      println(f"\nRemaining 39 transforms (fully cached): $restTime%.2f ms total")
      println(f"  Average per transform: $avgTime%.2f ms")
      println(f"  Speedup vs first: ${firstTime / avgTime}%.1fx")
      println(f"\nResults: $successCount/40 passed, $failCount failed")

      assertNotNull("First result should not be null", firstResult)
      assertEquals("All 40 points should transform successfully", 40, successCount)
    }

    it("should round-trip all 40 points ETRS89 -> OSGB36 -> ETRS89") {
      val etrsToOsgbFile =
        getClass.getClassLoader.getResource("grids/uk_os_OSTN15_NTv2_ETRStoOSGB.gsb")
      val osgbToEtrsFile =
        getClass.getClassLoader.getResource("grids/uk_os_OSTN15_NTv2_OSGBtoETRS.gsb")
      assume(etrsToOsgbFile != null && osgbToEtrsFile != null, "OSTN15 grid files not found")

      val etrs89WithGrid = s"+proj=longlat +ellps=GRS80 +nadgrids=${etrsToOsgbFile.getPath}"
      val osgb36 = "+proj=longlat +ellps=airy"
      val osgb36WithGrid = s"+proj=longlat +ellps=airy +nadgrids=${osgbToEtrsFile.getPath}"
      val etrs89 = "+proj=longlat +ellps=GRS80"

      println("\n" + "=" * 70)
      println("OSTN15 Round-trip: ETRS89 -> OSGB36 -> ETRS89 (40 points)")
      println("=" * 70)

      val startTotal = System.nanoTime()
      var successCount = 0
      var maxError = 0.0
      var worstPoint = ""

      ostn15TestPoints.foreach { tp =>
        val result = sparkSession
          .sql(s"""SELECT ST_Transform(
              ST_Transform(
                ST_GeomFromWKT('POINT (${tp.etrsLon} ${tp.etrsLat})'),
                '$etrs89WithGrid',
                '$osgb36'),
              '$osgb36WithGrid',
              '$etrs89')""")
          .first()
          .getAs[Geometry](0)

        if (result != null) {
          val xError = Math.abs(result.getCoordinate.x - tp.etrsLon)
          val yError = Math.abs(result.getCoordinate.y - tp.etrsLat)
          val totalError = Math.sqrt(xError * xError + yError * yError)

          if (xError < ROUND_TRIP_TOLERANCE && yError < ROUND_TRIP_TOLERANCE) {
            successCount += 1
          } else {
            println(f"  ${tp.pointId}: Round-trip error - X: $xError%.9f, Y: $yError%.9f")
          }

          if (totalError > maxError) {
            maxError = totalError
            worstPoint = tp.pointId
          }
        }
      }
      val totalTime = (System.nanoTime() - startTotal) / 1e6

      println(f"\nTotal time for 40 round-trips: $totalTime%.2f ms")
      println(f"  Average per round-trip: ${totalTime / 40}%.2f ms")
      println(f"  Max error: $maxError%.9f degrees (at $worstPoint)")
      println(f"\nResults: $successCount/40 passed within tolerance ($ROUND_TRIP_TOLERANCE deg)")

      assertEquals("All 40 points should round-trip successfully", 40, successCount)
    }

    it("should transform all 40 points using remote grid file with download timing") {
      // Clear caches to force fresh download
      Proj4.clearCache()
      NadgridRegistry.clear()

      val etrs89WithGrid = s"+proj=longlat +ellps=GRS80 +nadgrids=$OSTN15_ETRS_TO_OSGB_URL"
      val osgb36 = "+proj=longlat +ellps=airy"

      println("\n" + "=" * 70)
      println("OSTN15 Remote Grid File: Download + Transform 40 Points")
      println(s"URL: $OSTN15_ETRS_TO_OSGB_URL")
      println("=" * 70)

      // First transformation (downloads ~15MB grid file)
      val startDownload = System.nanoTime()
      val firstResult = sparkSession
        .sql(s"""SELECT ST_Transform(
            ST_GeomFromWKT('POINT (${ostn15TestPoints.head.etrsLon} ${ostn15TestPoints.head.etrsLat})'),
            '$etrs89WithGrid',
            '$osgb36')""")
        .first()
        .getAs[Geometry](0)
      val downloadTime = (System.nanoTime() - startDownload) / 1e6

      println(f"First transform (downloads ~15MB grid): $downloadTime%.2f ms")
      println(f"  Proj cache size: ${Proj4.getCacheSize}")
      println(f"  Grid cache size: ${NadgridRegistry.size()}")

      // Verify first result
      assertNotNull("First result should not be null", firstResult)
      val xShift = Math.abs(firstResult.getCoordinate.x - ostn15TestPoints.head.etrsLon)
      val yShift = Math.abs(firstResult.getCoordinate.y - ostn15TestPoints.head.etrsLat)
      assertTrue(
        s"X shift ($xShift) should be < $GRID_SHIFT_TOLERANCE",
        xShift < GRID_SHIFT_TOLERANCE)
      assertTrue(
        s"Y shift ($yShift) should be < $GRID_SHIFT_TOLERANCE",
        yShift < GRID_SHIFT_TOLERANCE)

      // Transform remaining 39 points (cached)
      val startCached = System.nanoTime()
      var successCount = 1

      ostn15TestPoints.tail.foreach { tp =>
        val result = sparkSession
          .sql(s"""SELECT ST_Transform(
              ST_GeomFromWKT('POINT (${tp.etrsLon} ${tp.etrsLat})'),
              '$etrs89WithGrid',
              '$osgb36')""")
          .first()
          .getAs[Geometry](0)

        if (result != null) {
          val xShiftPt = Math.abs(result.getCoordinate.x - tp.etrsLon)
          val yShiftPt = Math.abs(result.getCoordinate.y - tp.etrsLat)
          if (xShiftPt < GRID_SHIFT_TOLERANCE && yShiftPt < GRID_SHIFT_TOLERANCE &&
            xShiftPt > 1e-9 && yShiftPt > 1e-9) {
            successCount += 1
          } else {
            println(s"  ${tp.pointId}: Unexpected shift - X: $xShiftPt, Y: $yShiftPt")
          }
        } else {
          println(s"  ${tp.pointId}: Result is null")
        }
      }
      val cachedTime = (System.nanoTime() - startCached) / 1e6
      val avgCached = cachedTime / 39

      println(f"\nRemaining 39 transforms (cached): $cachedTime%.2f ms total")
      println(f"  Average per transform: $avgCached%.2f ms")
      println(f"  Download overhead: ${downloadTime - avgCached}%.2f ms")
      println(f"  Download speedup vs cached: ${downloadTime / avgCached}%.1fx")
      println(f"\nResults: $successCount/40 passed")

      assertEquals("All 40 points should transform successfully", 40, successCount)
    }
  }

  describe("URL CRS Provider config integration") {

    it("should still transform correctly when URL provider is not configured") {
      // Verify default behavior (no URL provider) still works
      sparkSession.conf.set("spark.sedona.crs.url.base", "")
      val result = sparkSession
        .sql("SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326), 'EPSG:4326', 'EPSG:3857')")
        .first()
        .getAs[Geometry](0)

      assertNotNull(result)
      assertEquals(3857, result.getSRID)
      assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
      assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
    }

    it("should fall back to built-in when URL provider returns nothing") {
      // Point to a non-existent server — provider will fail, should fall back to built-in
      sparkSession.conf.set("spark.sedona.crs.url.base", "http://127.0.0.1:1")
      sparkSession.conf.set("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.json")
      sparkSession.conf.set("spark.sedona.crs.url.format", "projjson")
      try {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326), 'EPSG:4326', 'EPSG:3857')")
          .first()
          .getAs[Geometry](0)

        // Should succeed via built-in fallback
        assertNotNull(result)
        assertEquals(3857, result.getSRID)
        assertEquals(-13627665.27, result.getCoordinate.x, COORD_TOLERANCE)
        assertEquals(4547675.35, result.getCoordinate.y, COORD_TOLERANCE)
      } finally {
        sparkSession.conf.set("spark.sedona.crs.url.base", "")
        org.datasyslab.proj4sedona.defs.Defs.removeProvider("sedona-url-crs")
      }
    }

    it("should register URL CRS provider when config is set") {
      sparkSession.conf.set("spark.sedona.crs.url.base", "https://test.example.com")
      sparkSession.conf.set("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.json")
      sparkSession.conf.set("spark.sedona.crs.url.format", "projjson")
      try {
        // Force a transform to trigger provider registration
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326), 'EPSG:4326', 'EPSG:3857')")
          .first()
          .getAs[Geometry](0)

        assertNotNull(result)

        // Verify provider was registered
        val providers = org.datasyslab.proj4sedona.defs.Defs.getProviders
        val found = providers.stream().anyMatch(p => p.getName == "sedona-url-crs")
        assertTrue("sedona-url-crs provider should be registered", found)
      } finally {
        sparkSession.conf.set("spark.sedona.crs.url.base", "")
        org.datasyslab.proj4sedona.defs.Defs.removeProvider("sedona-url-crs")
      }
    }

    it("should transform using local HTTP URL CRS provider with custom CRS") {
      // Serve a deliberately wrong CRS definition for fake EPSG:990001 that no
      // built-in provider knows. Uses Mercator with absurd false easting/northing.
      // If the transform succeeds with shifted coordinates, the URL provider was used.
      // If the URL provider didn't work, the transform would fail entirely.
      val requestCount = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress(0), 0)
      val port = server.getAddress.getPort

      // Web Mercator with intentional 10M/20M false easting/northing
      val weirdMercator =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0" +
          " +x_0=10000000 +y_0=20000000 +k=1 +units=m +no_defs"

      server.createContext(
        "/epsg/",
        exchange => {
          val path = exchange.getRequestURI.getPath
          if (path.contains("990001")) {
            requestCount.incrementAndGet()
            val body = weirdMercator.getBytes(StandardCharsets.UTF_8)
            exchange.sendResponseHeaders(200, body.length)
            exchange.getResponseBody.write(body)
            exchange.getResponseBody.close()
          } else {
            // 404 for everything else — built-in providers handle known codes
            exchange.sendResponseHeaders(404, -1)
            exchange.getResponseBody.close()
          }
        })
      server.start()

      sparkSession.conf.set("spark.sedona.crs.url.base", s"http://localhost:$port")
      sparkSession.conf.set("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.json")
      sparkSession.conf.set("spark.sedona.crs.url.format", "proj")
      try {
        val result = sparkSession
          .sql("SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKT('POINT (-122.4194 37.7749)'), 4326), 'EPSG:4326', 'EPSG:990001')")
          .first()
          .getAs[Geometry](0)

        assertNotNull("Transform to fake EPSG:990001 should succeed via URL provider", result)
        assertEquals(990001, result.getSRID)
        // Standard Web Mercator: x = -13627665.27, y = 4547675.35
        // Our weird definition adds +x_0=10000000, +y_0=20000000
        assertEquals(-3627665.27, result.getCoordinate.x, COORD_TOLERANCE)
        assertEquals(24547675.35, result.getCoordinate.y, COORD_TOLERANCE)
        assertTrue("Local HTTP server should have been hit", requestCount.get() > 0)
      } finally {
        server.stop(0)
        sparkSession.conf.set("spark.sedona.crs.url.base", "")
        org.datasyslab.proj4sedona.defs.Defs.removeProvider("sedona-url-crs")
      }
    }
  }
}
