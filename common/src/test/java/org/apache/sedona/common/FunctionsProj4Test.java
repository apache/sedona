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
package org.apache.sedona.common;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

/**
 * Comprehensive tests for FunctionsProj4 CRS transformation.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>All CRS input formats (EPSG, WKT1, WKT2, PROJ string, PROJJSON)
 *   <li>NAD Grid files (local paths, optional/mandatory)
 *   <li>All geometry types
 *   <li>Edge cases (same CRS, missing SRID, invalid CRS, null/empty geometry)
 *   <li>SRID and UserData preservation
 * </ul>
 */
public class FunctionsProj4Test extends TestBase {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final WKTReader WKT_READER = new WKTReader();
  private static final double FP_TOLERANCE = 1e-6;

  // ==================== EPSG Code Tests ====================

  @Test
  public void testTransformEpsgToEpsg() {
    // WGS84 to Web Mercator
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    point.setSRID(4326);

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    // San Francisco in Web Mercator coordinates
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
    assertEquals(4547675.35, result.getCoordinate().y, 1.0);
  }

  @Test
  public void testTransformEpsgUsingGeometrySRID() {
    // Test using geometry's SRID as source CRS
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    point.setSRID(4326);

    Geometry result = FunctionsProj4.transform(point, "EPSG:3857");

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
  }

  @Test
  public void testTransformLosAngelesCoordinates() {
    // Test with coordinates near Los Angeles (-117.99, 32.01)
    // This is the coordinate used in Flink tests
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-117.99, 32.01));

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    // Expected values from Web Mercator formula:
    // x = lon * 20037508.34 / 180 = -13134586.72
    // y = ln(tan((90 + lat) * pi / 360)) * 20037508.34 / pi = 3764623.35
    assertEquals(-13134586.72, result.getCoordinate().x, 1.0);
    assertEquals(3764623.35, result.getCoordinate().y, 1.0);
  }

  @Test
  public void testTransformUtmZone() {
    // WGS84 to UTM Zone 10N (San Francisco)
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:32610");

    assertNotNull(result);
    assertEquals(32610, result.getSRID());
    // UTM coordinates for San Francisco
    assertTrue(result.getCoordinate().x > 540000 && result.getCoordinate().x < 560000);
    assertTrue(result.getCoordinate().y > 4170000 && result.getCoordinate().y < 4190000);
  }

  // ==================== PROJ String Tests ====================

  @Test
  public void testTransformProjString() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    String sourceCRS = "+proj=longlat +datum=WGS84 +no_defs";
    String targetCRS =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs";

    Geometry result = FunctionsProj4.transform(point, sourceCRS, targetCRS);

    assertNotNull(result);
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
    assertEquals(4547675.35, result.getCoordinate().y, 1.0);
  }

  @Test
  public void testTransformProjStringToEpsg() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    String sourceCRS = "+proj=longlat +datum=WGS84 +no_defs";

    Geometry result = FunctionsProj4.transform(point, sourceCRS, "EPSG:3857");

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
  }

  @Test
  public void testTransformProjStringUtm() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    String sourceCRS = "+proj=longlat +datum=WGS84 +no_defs";
    String targetCRS = "+proj=utm +zone=10 +datum=WGS84 +units=m +no_defs";

    Geometry result = FunctionsProj4.transform(point, sourceCRS, targetCRS);

    assertNotNull(result);
    assertTrue(result.getCoordinate().x > 540000 && result.getCoordinate().x < 560000);
  }

  // ==================== WKT1 Tests ====================

  @Test
  public void testTransformWkt1() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 60));

    String sourceWkt =
        "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]]";
    String targetWkt =
        "PROJCS[\"WGS 84 / UTM zone 51N\",GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0],PARAMETER[\"central_meridian\",123],PARAMETER[\"scale_factor\",0.9996],PARAMETER[\"false_easting\",500000],PARAMETER[\"false_northing\",0],UNIT[\"metre\",1]]";

    Geometry result = FunctionsProj4.transform(point, sourceWkt, targetWkt);

    assertNotNull(result);
    // Should be projected coordinates
    assertTrue(Math.abs(result.getCoordinate().x) > 100000);
    assertTrue(Math.abs(result.getCoordinate().y) > 1000000);
  }

  // ==================== WKT2 Tests ====================

  @Test
  public void testTransformWkt2() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    String wkt2 =
        "GEOGCRS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",ELLIPSOID[\"WGS 84\",6378137,298.257223563]],CS[ellipsoidal,2],AXIS[\"latitude\",north],AXIS[\"longitude\",east],UNIT[\"degree\",0.0174532925199433]]";

    Geometry result = FunctionsProj4.transform(point, wkt2, "EPSG:3857");

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
  }

  // ==================== PROJJSON Tests ====================

  @Test
  public void testTransformProjJson() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    String projJson =
        "{"
            + "\"type\": \"GeographicCRS\","
            + "\"name\": \"WGS 84\","
            + "\"datum\": {"
            + "\"type\": \"GeodeticReferenceFrame\","
            + "\"name\": \"World Geodetic System 1984\","
            + "\"ellipsoid\": {"
            + "\"name\": \"WGS 84\","
            + "\"semi_major_axis\": 6378137,"
            + "\"inverse_flattening\": 298.257223563"
            + "}"
            + "}"
            + "}";

    Geometry result = FunctionsProj4.transform(point, projJson, "EPSG:3857");

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
  }

  @Test
  public void testTransformProjJsonProjected() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    String projJson =
        "{"
            + "\"type\": \"ProjectedCRS\","
            + "\"name\": \"WGS 84 / UTM zone 10N\","
            + "\"base_crs\": {"
            + "\"type\": \"GeographicCRS\","
            + "\"datum\": {"
            + "\"ellipsoid\": {"
            + "\"semi_major_axis\": 6378137,"
            + "\"inverse_flattening\": 298.257223563"
            + "}"
            + "}"
            + "},"
            + "\"conversion\": {"
            + "\"method\": {\"name\": \"Transverse Mercator\"},"
            + "\"parameters\": ["
            + "{\"name\": \"Latitude of natural origin\", \"value\": 0, \"unit\": {\"conversion_factor\": 0.0174532925199433}},"
            + "{\"name\": \"Longitude of natural origin\", \"value\": -123, \"unit\": {\"conversion_factor\": 0.0174532925199433}},"
            + "{\"name\": \"Scale factor at natural origin\", \"value\": 0.9996},"
            + "{\"name\": \"False easting\", \"value\": 500000},"
            + "{\"name\": \"False northing\", \"value\": 0}"
            + "]"
            + "}"
            + "}";

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", projJson);

    assertNotNull(result);
    // UTM coordinates
    assertTrue(result.getCoordinate().x > 500000 && result.getCoordinate().x < 600000);
    assertTrue(result.getCoordinate().y > 4000000 && result.getCoordinate().y < 5000000);
  }

  // ==================== NAD Grid Tests (Local Files) ====================

  @Test
  public void testNadgridLocalFileAbsolute() {
    Path gridPath = Path.of("src/test/resources/grids/ca_nrc_ntv2_0.tif").toAbsolutePath();
    assumeTrue("Grid file not found", Files.exists(gridPath));

    // Toronto coordinates in NAD27
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.3832, 43.6532));

    String sourceCRS = "+proj=longlat +datum=NAD27 +nadgrids=" + gridPath + " +no_defs";
    String targetCRS = "EPSG:4326"; // WGS84

    Geometry result = FunctionsProj4.transform(point, sourceCRS, targetCRS);

    assertNotNull(result);
    // Grid shift should be small but non-zero
    assertNotEquals(point.getCoordinate().x, result.getCoordinate().x, 1e-6);
    assertNotEquals(point.getCoordinate().y, result.getCoordinate().y, 1e-6);
    // But should be close (< 0.001 degrees)
    assertEquals(point.getCoordinate().x, result.getCoordinate().x, 0.001);
    assertEquals(point.getCoordinate().y, result.getCoordinate().y, 0.001);
  }

  @Test
  public void testNadgridLocalFileRelative() {
    String relativePath = "./src/test/resources/grids/us_noaa_conus.tif";
    assumeTrue("Grid file not found", Files.exists(Path.of(relativePath)));

    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-77.0, 38.9));

    String sourceCRS = "+proj=longlat +datum=NAD83 +nadgrids=" + relativePath + " +no_defs";

    // Should not throw - grid file is found
    Geometry result = FunctionsProj4.transform(point, sourceCRS, "EPSG:4326");
    assertNotNull(result);
  }

  @Test
  public void testNadgridLocalFileOptionalNotFound() {
    // Optional grid (with @) should NOT throw when file doesn't exist
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.0, 43.0));

    String sourceCRS = "+proj=longlat +datum=NAD27 +nadgrids=@/nonexistent/path/grid.gsb +no_defs";
    String targetCRS = "EPSG:4326";

    // Should not throw - optional grid just gets skipped
    Geometry result = FunctionsProj4.transform(point, sourceCRS, targetCRS);
    assertNotNull(result);
  }

  @Test
  public void testNadgridLocalFileMandatoryNotFound() {
    // Mandatory grid (without @) SHOULD throw when file doesn't exist
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.0, 43.0));

    String sourceCRS = "+proj=longlat +datum=NAD27 +nadgrids=/nonexistent/path/grid.gsb +no_defs";
    String targetCRS = "EPSG:4326";

    try {
      FunctionsProj4.transform(point, sourceCRS, targetCRS);
      fail("Expected exception for mandatory grid not found");
    } catch (RuntimeException e) {
      // Expected
      assertTrue(e.getMessage().contains("grid") || e.getCause() != null);
    }
  }

  // ==================== Geometry Type Tests ====================

  @Test
  public void testTransformLineString() throws Exception {
    LineString line =
        (LineString) WKT_READER.read("LINESTRING(-122.4 37.7, -122.5 37.8, -122.6 37.9)");

    Geometry result = FunctionsProj4.transform(line, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof LineString);
    assertEquals(3, result.getNumPoints());
    assertEquals(3857, result.getSRID());
  }

  @Test
  public void testTransformPolygon() throws Exception {
    Polygon polygon =
        (Polygon)
            WKT_READER.read(
                "POLYGON((-122.5 37.7, -122.3 37.7, -122.3 37.9, -122.5 37.9, -122.5 37.7))");

    Geometry result = FunctionsProj4.transform(polygon, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof Polygon);
    assertEquals(3857, result.getSRID());
  }

  @Test
  public void testTransformPolygonWithHole() throws Exception {
    Polygon polygon =
        (Polygon)
            WKT_READER.read(
                "POLYGON((-122.5 37.7, -122.3 37.7, -122.3 37.9, -122.5 37.9, -122.5 37.7), "
                    + "(-122.45 37.75, -122.35 37.75, -122.35 37.85, -122.45 37.85, -122.45 37.75))");

    Geometry result = FunctionsProj4.transform(polygon, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof Polygon);
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
  }

  @Test
  public void testTransformMultiPoint() throws Exception {
    MultiPoint multiPoint =
        (MultiPoint) WKT_READER.read("MULTIPOINT((-122.4 37.7), (-122.5 37.8), (-122.6 37.9))");

    Geometry result = FunctionsProj4.transform(multiPoint, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof MultiPoint);
    assertEquals(3, result.getNumGeometries());
    assertEquals(3857, result.getSRID());
  }

  @Test
  public void testTransformMultiLineString() throws Exception {
    MultiLineString multiLine =
        (MultiLineString)
            WKT_READER.read(
                "MULTILINESTRING((-122.4 37.7, -122.5 37.8), (-122.6 37.9, -122.7 38.0))");

    Geometry result = FunctionsProj4.transform(multiLine, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof MultiLineString);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  public void testTransformMultiPolygon() throws Exception {
    MultiPolygon multiPolygon =
        (MultiPolygon)
            WKT_READER.read(
                "MULTIPOLYGON(((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7)), "
                    + "((-122.3 37.6, -122.2 37.6, -122.2 37.7, -122.3 37.7, -122.3 37.6)))");

    Geometry result = FunctionsProj4.transform(multiPolygon, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof MultiPolygon);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  public void testTransformGeometryCollection() throws Exception {
    GeometryCollection collection =
        (GeometryCollection)
            WKT_READER.read(
                "GEOMETRYCOLLECTION(POINT(-122.4 37.7), LINESTRING(-122.5 37.8, -122.6 37.9))");

    Geometry result = FunctionsProj4.transform(collection, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result instanceof GeometryCollection);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  public void testTransformPoint3D() {
    // Test 3D coordinate preservation
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749, 100.0));

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertFalse(Double.isNaN(result.getCoordinate().getZ()));
  }

  // ==================== Edge Cases ====================

  @Test
  public void testTransformNullGeometry() {
    Geometry result = FunctionsProj4.transform(null, "EPSG:4326", "EPSG:3857");
    assertNull(result);
  }

  @Test
  public void testTransformEmptyGeometry() {
    Point emptyPoint = GEOMETRY_FACTORY.createPoint();

    Geometry result = FunctionsProj4.transform(emptyPoint, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testTransformSameCRS() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    point.setSRID(4326);

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:4326");

    assertNotNull(result);
    assertEquals(4326, result.getSRID());
    // Coordinates should be unchanged
    assertEquals(point.getCoordinate().x, result.getCoordinate().x, FP_TOLERANCE);
    assertEquals(point.getCoordinate().y, result.getCoordinate().y, FP_TOLERANCE);
  }

  @Test
  public void testTransformSameCRSUpdatesSRID() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    point.setSRID(0); // No SRID set

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:4326");

    assertNotNull(result);
    assertEquals(4326, result.getSRID());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTransformNoSourceCRS() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    // No SRID set and no source CRS provided
    FunctionsProj4.transform(point, "EPSG:3857");
  }

  @Test(expected = Exception.class)
  public void testTransformInvalidCRS() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    FunctionsProj4.transform(point, "EPSG:4326", "INVALID:CRS");
  }

  // ==================== UserData Preservation ====================

  @Test
  public void testTransformPreservesUserData() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    String userData = "test-user-data";
    point.setUserData(userData);

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");

    assertNotNull(result);
    assertEquals(userData, result.getUserData());
  }

  // ==================== 4-arg (lenient parameter) Tests ====================

  @Test
  public void testTransform4ArgLenientTrue() {
    // Test 4-arg version with lenient=true
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857", true);

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    // Same result as 3-arg version
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
    assertEquals(4547675.35, result.getCoordinate().y, 1.0);
  }

  @Test
  public void testTransform4ArgLenientFalse() {
    // Test 4-arg version with lenient=false
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857", false);

    assertNotNull(result);
    assertEquals(3857, result.getSRID());
    // Same result as 3-arg version
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
    assertEquals(4547675.35, result.getCoordinate().y, 1.0);
  }

  @Test
  public void testTransform4ArgLenientIgnored() {
    // Verify lenient parameter is ignored - both true and false produce identical results
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    Geometry resultLenientTrue = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857", true);
    Geometry resultLenientFalse = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857", false);
    Geometry result3Arg = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");

    // All three should produce identical coordinates
    assertEquals(resultLenientTrue.getCoordinate().x, resultLenientFalse.getCoordinate().x, 1e-9);
    assertEquals(resultLenientTrue.getCoordinate().y, resultLenientFalse.getCoordinate().y, 1e-9);
    assertEquals(resultLenientTrue.getCoordinate().x, result3Arg.getCoordinate().x, 1e-9);
    assertEquals(resultLenientTrue.getCoordinate().y, result3Arg.getCoordinate().y, 1e-9);
  }

  @Test
  public void testTransform4ArgWithProjString() {
    // Test 4-arg version with PROJ string
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
    String projString =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs";

    Geometry result = FunctionsProj4.transform(point, "EPSG:4326", projString, true);

    assertNotNull(result);
    assertEquals(-13627665.27, result.getCoordinate().x, 1.0);
    assertEquals(4547675.35, result.getCoordinate().y, 1.0);
  }

  // ==================== Round-trip Tests ====================

  @Test
  public void testRoundTrip() {
    Point original = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));

    // Transform to Web Mercator and back
    Geometry projected = FunctionsProj4.transform(original, "EPSG:4326", "EPSG:3857");
    Geometry backToWgs84 = FunctionsProj4.transform(projected, "EPSG:3857", "EPSG:4326");

    assertNotNull(backToWgs84);
    assertEquals(original.getCoordinate().x, backToWgs84.getCoordinate().x, 1e-9);
    assertEquals(original.getCoordinate().y, backToWgs84.getCoordinate().y, 1e-9);
  }
}
