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

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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

  // ==================== URL CRS Provider Registration Tests ====================

  @Test
  public void testRegisterUrlCrsProviderNoOpOnNullOrEmpty() {
    // null and empty baseUrl should be no-ops, not throw
    FunctionsProj4.registerUrlCrsProvider(null, "/epsg/{code}.json", "projjson");
    FunctionsProj4.registerUrlCrsProvider("", "/epsg/{code}.json", "projjson");
    // No provider should have been registered
    assertNull("No provider should be registered for null/empty baseUrl", findUrlCrsProvider());
  }

  @Test
  public void testRegisterUrlCrsProviderRegistersAndIsIdempotent() {
    String testUrl = "https://test-crs-server.example.com";
    try {
      FunctionsProj4.registerUrlCrsProvider(testUrl, "/epsg/{code}.json", "projjson");
      assertNotNull("sedona-url-crs provider should be registered", findUrlCrsProvider());
      int countBefore = countProvidersByName("sedona-url-crs");

      // Second call with same config — should not add a duplicate
      FunctionsProj4.registerUrlCrsProvider(testUrl, "/epsg/{code}.json", "projjson");
      assertEquals(
          "Provider should not be duplicated", countBefore, countProvidersByName("sedona-url-crs"));
    } finally {
      FunctionsProj4.resetUrlCrsProviderForTest();
    }
  }

  @Test
  public void testRegisterUrlCrsProviderReRegistersOnConfigChange() {
    try {
      FunctionsProj4.registerUrlCrsProvider(
          "https://server-a.example.com", "/epsg/{code}.json", "projjson");
      assertEquals(
          org.datasyslab.proj4sedona.defs.CRSResult.Format.PROJJSON,
          findUrlCrsProvider().getFormat());

      // Change config — should re-register with new settings
      FunctionsProj4.registerUrlCrsProvider(
          "https://server-b.example.com", "/epsg/{code}.json", "wkt2");
      assertEquals(
          org.datasyslab.proj4sedona.defs.CRSResult.Format.WKT2, findUrlCrsProvider().getFormat());
    } finally {
      FunctionsProj4.resetUrlCrsProviderForTest();
    }
  }

  @Test
  public void testParseCrsFormatAllMappings() {
    // Verify all valid format strings map to the correct enum
    Object[][] cases = {
      {"projjson", org.datasyslab.proj4sedona.defs.CRSResult.Format.PROJJSON},
      {"proj", org.datasyslab.proj4sedona.defs.CRSResult.Format.PROJ4},
      {"wkt1", org.datasyslab.proj4sedona.defs.CRSResult.Format.WKT1},
      {"wkt2", org.datasyslab.proj4sedona.defs.CRSResult.Format.WKT2},
    };
    for (Object[] c : cases) {
      try {
        FunctionsProj4.registerUrlCrsProvider(
            "https://test.example.com", "/epsg/{code}", (String) c[0]);
        assertEquals("Format '" + c[0] + "'", c[1], findUrlCrsProvider().getFormat());
      } finally {
        FunctionsProj4.resetUrlCrsProviderForTest();
      }
    }
  }

  @Test
  public void testParseCrsFormatDefaultsAndCaseInsensitive() {
    // null, empty, unknown, and uppercase should all default to / map to PROJJSON
    String[] inputs = {null, "", "unknown-format", "PROJJSON", "ProjJson"};
    for (String input : inputs) {
      try {
        FunctionsProj4.registerUrlCrsProvider("https://test.example.com", "/epsg/{code}", input);
        assertEquals(
            "Format input '" + input + "' should resolve to PROJJSON",
            org.datasyslab.proj4sedona.defs.CRSResult.Format.PROJJSON,
            findUrlCrsProvider().getFormat());
      } finally {
        // Use the test reset so registeredUrlCrsConfig is also cleared
        FunctionsProj4.resetUrlCrsProviderForTest();
      }
    }
  }

  @Test
  public void testTransformWithLocalUrlCrsProvider() throws Exception {
    // Serve a deliberately wrong CRS definition for a fake EPSG code (990001)
    // that no built-in provider knows. The definition is a Mercator projection
    // with absurd false easting/northing (+x_0=10000000 +y_0=20000000).
    // If the transform succeeds with these shifted coordinates, the URL provider
    // resolved the CRS. If it didn't work, the transform would fail entirely
    // because no built-in provider knows EPSG:990001.
    AtomicInteger requestCount = new AtomicInteger(0);
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    int port = server.getAddress().getPort();

    // Web Mercator with intentional 10M/20M false easting/northing
    String weirdMercator =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0"
            + " +x_0=10000000 +y_0=20000000 +k=1 +units=m +no_defs";

    server.createContext(
        "/epsg/",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          if (path.contains("990001")) {
            requestCount.incrementAndGet();
            byte[] body = weirdMercator.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
          } else {
            // 404 for everything else — built-in providers handle known codes
            exchange.sendResponseHeaders(404, -1);
            exchange.getResponseBody().close();
          }
        });
    server.start();

    try {
      FunctionsProj4.registerUrlCrsProvider(
          "http://localhost:" + port, "/epsg/{code}.json", "proj");

      Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749));
      Geometry result = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:990001");

      assertNotNull("Transform to fake EPSG:990001 should succeed via URL provider", result);
      assertEquals(990001, result.getSRID());
      // Standard Web Mercator: x = -13627665.27, y = 4547675.35
      // Our weird definition adds +x_0=10000000, +y_0=20000000
      assertEquals(-3627665.27, result.getCoordinate().x, 1.0);
      assertEquals(24547675.35, result.getCoordinate().y, 1.0);
      assertTrue("Local HTTP server should have been hit", requestCount.get() > 0);
    } finally {
      server.stop(0);
      FunctionsProj4.resetUrlCrsProviderForTest();
    }
  }

  @Test
  public void testRegisterUrlCrsProviderConcurrentThreadSafety() throws Exception {
    // Verify that concurrent calls to registerUrlCrsProvider do not produce
    // duplicate providers or corrupt the registry. This exercises the
    // synchronized double-checked locking path.
    final int threadCount = 16;
    final String testUrl = "https://concurrent-test.example.com";
    final String pathTemplate = "/epsg/{code}.json";
    final String format = "projjson";

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CyclicBarrier barrier = new CyclicBarrier(threadCount);

    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threadCount; i++) {
        futures.add(
            pool.submit(
                () -> {
                  try {
                    // All threads wait at the barrier then race into registration
                    barrier.await();
                    FunctionsProj4.registerUrlCrsProvider(testUrl, pathTemplate, format);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }));
      }

      // Wait for all threads to complete and propagate any exceptions
      for (Future<?> f : futures) {
        f.get();
      }

      // After all concurrent registrations, there should be exactly 1 provider
      assertEquals(
          "Concurrent registration must produce exactly 1 provider",
          1,
          countProvidersByName("sedona-url-crs"));
    } finally {
      pool.shutdown();
      FunctionsProj4.resetUrlCrsProviderForTest();
    }
  }

  // Helper: count providers with a given name
  private int countProvidersByName(String name) {
    int count = 0;
    for (org.datasyslab.proj4sedona.defs.CRSProvider p :
        org.datasyslab.proj4sedona.defs.Defs.getProviders()) {
      if (name.equals(p.getName())) {
        count++;
      }
    }
    return count;
  }

  // Helper: find the registered UrlCRSProvider
  private org.datasyslab.proj4sedona.defs.UrlCRSProvider findUrlCrsProvider() {
    for (org.datasyslab.proj4sedona.defs.CRSProvider p :
        org.datasyslab.proj4sedona.defs.Defs.getProviders()) {
      if ("sedona-url-crs".equals(p.getName())
          && p instanceof org.datasyslab.proj4sedona.defs.UrlCRSProvider) {
        return (org.datasyslab.proj4sedona.defs.UrlCRSProvider) p;
      }
    }
    return null;
  }
}
