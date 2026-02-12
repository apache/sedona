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
import org.datasyslab.proj4sedona.Proj4;
import org.datasyslab.proj4sedona.defs.Defs;
import org.datasyslab.proj4sedona.grid.NadgridRegistry;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * Performance tests for Proj4sedona CRS transformation.
 *
 * <p>These tests measure:
 *
 * <ol>
 *   <li>Proj4sedona vs GeoTools performance comparison
 *   <li>Cache effects in Proj4sedona:
 *       <ul>
 *         <li>2.1 Built-in EPSG codes
 *         <li>2.2 EPSG codes with remote fetching from spatialreference.org
 *         <li>2.3 PROJ and WKT strings
 *         <li>2.4 Grid files (local and remote)
 *       </ul>
 * </ol>
 *
 * <p>Each test uses the pattern: 1 cold call (cache miss) + N warm calls (cache hits)
 */
public class FunctionsProj4PerformanceTest extends TestBase {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final int WARM_ITERATIONS = 10;

  // Test coordinates
  private static final double SF_LON = -122.4194;
  private static final double SF_LAT = 37.7749;

  // Remote grid file URL (OSTN15 from GitHub)
  private static final String REMOTE_GRID_URL =
      "https://raw.githubusercontent.com/jiayuasu/grid-files/main/us_os/OSTN15-NTv2/OSTN15_NTv2_ETRStoOSGB.gsb";

  // ==================== Helper Methods ====================

  private Point createTestPoint(double lon, double lat) {
    return GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat));
  }

  private void printHeader(String title) {
    System.out.println();
    System.out.println("=".repeat(70));
    System.out.println(title);
    System.out.println("=".repeat(70));
  }

  private void printResult(String label, double coldMs, double warmAvgUs, int cacheEntries) {
    double speedup = (coldMs * 1000) / warmAvgUs;
    System.out.printf("Cold (1 call):        %10.2f ms%n", coldMs);
    System.out.printf("Warm (%d calls):   %10.2f μs avg%n", WARM_ITERATIONS, warmAvgUs);
    System.out.printf("Cache speedup:        %10.0fx%n", speedup);
    if (cacheEntries >= 0) {
      System.out.printf("Proj cache entries:   %10d%n", cacheEntries);
    }
  }

  // ==================== 1. Proj4sedona vs GeoTools ====================

  @Test
  public void testProj4VsGeoToolsEpsgPerformance() throws Exception {
    printHeader("1. Proj4sedona vs GeoTools (EPSG:4326 -> EPSG:3857)");

    Point point = createTestPoint(SF_LON, SF_LAT);

    // ===== Proj4sedona =====
    System.out.println("\nProj4sedona:");
    Proj4.clearCache();

    // Cold call
    long coldStart = System.nanoTime();
    Geometry proj4ColdResult = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");
    double proj4ColdMs = (System.nanoTime() - coldStart) / 1e6;

    // Warm calls
    long warmStart = System.nanoTime();
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");
    }
    double proj4WarmTotalMs = (System.nanoTime() - warmStart) / 1e6;
    double proj4WarmAvgUs = (proj4WarmTotalMs * 1000) / WARM_ITERATIONS;

    printResult("Proj4sedona", proj4ColdMs, proj4WarmAvgUs, Proj4.getCacheSize());
    assertNotNull(proj4ColdResult);
    assertEquals(3857, proj4ColdResult.getSRID());

    // ===== GeoTools =====
    System.out.println("\nGeoTools:");

    // Cold call
    coldStart = System.nanoTime();
    Geometry gtColdResult = FunctionsGeoTools.transform(point, "EPSG:4326", "EPSG:3857");
    double gtColdMs = (System.nanoTime() - coldStart) / 1e6;

    // Warm calls
    warmStart = System.nanoTime();
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      FunctionsGeoTools.transform(point, "EPSG:4326", "EPSG:3857");
    }
    double gtWarmTotalMs = (System.nanoTime() - warmStart) / 1e6;
    double gtWarmAvgUs = (gtWarmTotalMs * 1000) / WARM_ITERATIONS;

    printResult("GeoTools", gtColdMs, gtWarmAvgUs, -1);
    assertNotNull(gtColdResult);

    // ===== Comparison =====
    double warmSpeedup = gtWarmAvgUs / proj4WarmAvgUs;
    System.out.printf(
        "%nComparison: Proj4sedona is %.1fx faster than GeoTools (warm)%n", warmSpeedup);

    // Verify both produce similar results
    assertEquals(
        proj4ColdResult.getCoordinate().x,
        gtColdResult.getCoordinate().x,
        1.0); // 1 meter tolerance
    assertEquals(proj4ColdResult.getCoordinate().y, gtColdResult.getCoordinate().y, 1.0);
  }

  // ==================== 2.1 Cache Effect: Built-in EPSG ====================

  @Test
  public void testCacheEffectBuiltInEpsgCode() {
    printHeader("2.1 Cache Effect: Built-in EPSG (EPSG:4326 -> EPSG:3857)");

    Point point = createTestPoint(SF_LON, SF_LAT);
    Proj4.clearCache();

    // Cold call
    long coldStart = System.nanoTime();
    Geometry coldResult = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");
    double coldMs = (System.nanoTime() - coldStart) / 1e6;

    // Warm calls
    long warmStart = System.nanoTime();
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      FunctionsProj4.transform(point, "EPSG:4326", "EPSG:3857");
    }
    double warmTotalMs = (System.nanoTime() - warmStart) / 1e6;
    double warmAvgUs = (warmTotalMs * 1000) / WARM_ITERATIONS;

    printResult("Built-in EPSG", coldMs, warmAvgUs, Proj4.getCacheSize());
    assertNotNull(coldResult);
    assertEquals(3857, coldResult.getSRID());
  }

  // ==================== 2.2 Cache Effect: Remote Fetch EPSG ====================

  @Test
  public void testCacheEffectRemoteFetchEpsgCode() {
    printHeader("2.2 Cache Effect: Remote Fetch EPSG (EPSG:2154 - French Lambert)");

    // EPSG:2154 (RGF93 / Lambert-93) is NOT in the built-in list
    // It requires fetching from spatialreference.org

    Point point = createTestPoint(2.3522, 48.8566); // Paris coordinates
    Proj4.clearCache();
    Defs.reset(); // Clear fetched definitions

    try {
      // Cold call (network fetch)
      long coldStart = System.nanoTime();
      Geometry coldResult = FunctionsProj4.transform(point, "EPSG:4326", "EPSG:2154");
      double coldMs = (System.nanoTime() - coldStart) / 1e6;

      // Warm calls
      long warmStart = System.nanoTime();
      for (int i = 0; i < WARM_ITERATIONS; i++) {
        FunctionsProj4.transform(point, "EPSG:4326", "EPSG:2154");
      }
      double warmTotalMs = (System.nanoTime() - warmStart) / 1e6;
      double warmAvgUs = (warmTotalMs * 1000) / WARM_ITERATIONS;

      printResult("Remote Fetch EPSG", coldMs, warmAvgUs, Proj4.getCacheSize());
      System.out.printf("Note: Cold time includes network fetch from spatialreference.org%n");
      assertNotNull(coldResult);
      assertEquals(2154, coldResult.getSRID());
    } catch (Exception e) {
      System.out.println("Skipped: Network fetch failed - " + e.getMessage());
      // Don't fail the test if network is unavailable
    }
  }

  // ==================== 2.3 Cache Effect: PROJ String ====================

  @Test
  public void testCacheEffectProjString() {
    printHeader("2.3 Cache Effect: PROJ String");

    Point point = createTestPoint(SF_LON, SF_LAT);
    String sourceCRS = "+proj=longlat +datum=WGS84 +no_defs";
    String targetCRS =
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs";

    Proj4.clearCache();

    // Cold call
    long coldStart = System.nanoTime();
    Geometry coldResult = FunctionsProj4.transform(point, sourceCRS, targetCRS);
    double coldMs = (System.nanoTime() - coldStart) / 1e6;

    // Warm calls
    long warmStart = System.nanoTime();
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      FunctionsProj4.transform(point, sourceCRS, targetCRS);
    }
    double warmTotalMs = (System.nanoTime() - warmStart) / 1e6;
    double warmAvgUs = (warmTotalMs * 1000) / WARM_ITERATIONS;

    printResult("PROJ String", coldMs, warmAvgUs, Proj4.getCacheSize());
    assertNotNull(coldResult);
    // Web Mercator coordinates
    assertEquals(-13627665.27, coldResult.getCoordinate().x, 1.0);
    assertEquals(4547675.35, coldResult.getCoordinate().y, 1.0);
  }

  @Test
  public void testCacheEffectWktString() {
    printHeader("2.3 Cache Effect: WKT String");

    Point point = createTestPoint(120, 60);
    String sourceWkt =
        "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]]";
    String targetWkt =
        "PROJCS[\"WGS 84 / UTM zone 51N\",GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0],PARAMETER[\"central_meridian\",123],PARAMETER[\"scale_factor\",0.9996],PARAMETER[\"false_easting\",500000],PARAMETER[\"false_northing\",0],UNIT[\"metre\",1]]";

    Proj4.clearCache();

    // Cold call
    long coldStart = System.nanoTime();
    Geometry coldResult = FunctionsProj4.transform(point, sourceWkt, targetWkt);
    double coldMs = (System.nanoTime() - coldStart) / 1e6;

    // Warm calls
    long warmStart = System.nanoTime();
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      FunctionsProj4.transform(point, sourceWkt, targetWkt);
    }
    double warmTotalMs = (System.nanoTime() - warmStart) / 1e6;
    double warmAvgUs = (warmTotalMs * 1000) / WARM_ITERATIONS;

    printResult("WKT String", coldMs, warmAvgUs, Proj4.getCacheSize());
    assertNotNull(coldResult);
  }

  // ==================== 2.4 Cache Effect: Grid Files ====================

  @Test
  public void testCacheEffectGridFileLocal() {
    printHeader("2.4 Cache Effect: Grid File (local)");

    Path gridPath = Path.of("src/test/resources/grids/ca_nrc_ntv2_0.tif").toAbsolutePath();
    assumeTrue("Grid file not found: " + gridPath, Files.exists(gridPath));

    // Toronto coordinates for Canadian grid
    Point point = createTestPoint(-79.3832, 43.6532);
    String sourceCRS = "+proj=longlat +datum=NAD27 +nadgrids=" + gridPath + " +no_defs";
    String targetCRS = "EPSG:4326"; // WGS84

    Proj4.clearCache();
    NadgridRegistry.clear();

    // Cold call (loads grid file)
    long coldStart = System.nanoTime();
    Geometry coldResult = FunctionsProj4.transform(point, sourceCRS, targetCRS);
    double coldMs = (System.nanoTime() - coldStart) / 1e6;

    // Warm calls
    long warmStart = System.nanoTime();
    for (int i = 0; i < WARM_ITERATIONS; i++) {
      FunctionsProj4.transform(point, sourceCRS, targetCRS);
    }
    double warmTotalMs = (System.nanoTime() - warmStart) / 1e6;
    double warmAvgUs = (warmTotalMs * 1000) / WARM_ITERATIONS;

    printResult("Grid File (local)", coldMs, warmAvgUs, Proj4.getCacheSize());
    System.out.printf("Grid cache entries:   %10d%n", NadgridRegistry.size());
    System.out.printf("Note: Cold time includes loading grid file from disk%n");
    assertNotNull(coldResult);
    assertTrue(NadgridRegistry.size() > 0);
  }

  @Test
  public void testCacheEffectGridFileRemote() {
    printHeader("2.4 Cache Effect: Grid File (remote)");

    // Use OSTN15 grid file from GitHub
    Point point = createTestPoint(-0.1276, 51.5074); // London coordinates
    String sourceCRS = "+proj=longlat +ellps=GRS80 +nadgrids=" + REMOTE_GRID_URL + " +no_defs";
    String targetCRS = "+proj=longlat +ellps=airy +no_defs";

    Proj4.clearCache();
    NadgridRegistry.clear();

    try {
      // Cold call (downloads grid file)
      long coldStart = System.nanoTime();
      Geometry coldResult = FunctionsProj4.transform(point, sourceCRS, targetCRS);
      double coldMs = (System.nanoTime() - coldStart) / 1e6;

      // Warm calls
      long warmStart = System.nanoTime();
      for (int i = 0; i < WARM_ITERATIONS; i++) {
        FunctionsProj4.transform(point, sourceCRS, targetCRS);
      }
      double warmTotalMs = (System.nanoTime() - warmStart) / 1e6;
      double warmAvgUs = (warmTotalMs * 1000) / WARM_ITERATIONS;

      printResult("Grid File (remote)", coldMs, warmAvgUs, Proj4.getCacheSize());
      System.out.printf("Grid cache entries:   %10d%n", NadgridRegistry.size());
      System.out.printf("Note: Cold time includes downloading grid file (~15MB)%n");
      assertNotNull(coldResult);
      assertTrue(NadgridRegistry.size() > 0);
    } catch (Exception e) {
      System.out.println("Skipped: Remote grid download failed - " + e.getMessage());
      // Don't fail the test if network is unavailable
    }
  }
}
