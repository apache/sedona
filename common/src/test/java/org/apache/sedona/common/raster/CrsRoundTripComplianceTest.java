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
package org.apache.sedona.common.raster;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;

/**
 * Round-trip compliance tests for RS_SetCRS and RS_CRS across representative EPSG codes.
 *
 * <p>For each EPSG code and each format (PROJ, PROJJSON, WKT1), this test:
 *
 * <ol>
 *   <li>Creates a raster with that CRS via RS_SetCRS("EPSG:xxxx")
 *   <li>Exports the CRS via RS_CRS(raster, format)
 *   <li>Re-imports the exported string via RS_SetCRS(exportedString)
 *   <li>Re-exports via RS_CRS(raster2, format) and verifies the exported string is identical
 * </ol>
 */
public class CrsRoundTripComplianceTest extends RasterTestBase {

  private static final Pattern WKT1_PROJECTION_PATTERN =
      Pattern.compile("PROJECTION\\[\"([^\"]+)\"");
  private static final Pattern WKT1_AUTHORITY_PATTERN =
      Pattern.compile("AUTHORITY\\[\"EPSG\",\\s*\"(\\d+)\"\\]");

  // ---------------------------------------------------------------------------
  // PROJ format round-trip tests
  // ---------------------------------------------------------------------------

  @Test
  public void testProjRoundTrip_Geographic_4326() throws FactoryException {
    assertProjRoundTrip(4326);
  }

  @Test
  public void testProjRoundTrip_Geographic_NAD83_4269() throws FactoryException {
    assertProjRoundTrip(4269);
  }

  @Test
  public void testProjRoundTrip_TransverseMercator_32617() throws FactoryException {
    assertProjRoundTrip(32617);
  }

  @Test
  public void testProjRoundTrip_PseudoMercator_3857() throws FactoryException {
    assertProjRoundTrip(3857);
  }

  @Test
  public void testProjRoundTrip_Mercator1SP_3395() throws FactoryException {
    assertProjRoundTrip(3395);
  }

  @Test
  public void testProjRoundTrip_LambertConformalConic2SP_2154() throws FactoryException {
    assertProjRoundTrip(2154);
  }

  @Test
  public void testProjRoundTrip_LambertAzimuthalEqualArea_Spherical_2163() throws FactoryException {
    assertProjRoundTrip(2163);
  }

  @Test
  public void testProjRoundTrip_AlbersEqualArea_5070() throws FactoryException {
    assertProjRoundTrip(5070);
  }

  @Test
  public void testProjRoundTrip_ObliqueStereographic_28992() throws FactoryException {
    assertProjRoundTrip(28992);
  }

  @Test
  public void testProjRoundTrip_PolarStereographicB_3031() throws FactoryException {
    assertProjRoundTrip(3031);
  }

  @Test
  public void testProjRoundTrip_LambertAzimuthalEqualArea_3035() throws FactoryException {
    assertProjRoundTrip(3035);
  }

  @Test
  public void testProjRoundTrip_Mercator1SP_Spherical_3785() throws FactoryException {
    assertProjRoundTrip(3785);
  }

  @Test
  public void testProjRoundTrip_EquidistantCylindrical_4087() throws FactoryException {
    assertProjRoundTrip(4087);
  }

  @Test
  public void testProjRoundTrip_PolarStereographicA_32661() throws FactoryException {
    assertProjRoundTrip(32661);
  }

  @Test
  public void testProjRoundTrip_TransverseMercator_OSGB_27700() throws FactoryException {
    assertProjRoundTrip(27700);
  }

  @Test
  public void testProjRoundTrip_AlbersEqualArea_Australian_3577() throws FactoryException {
    assertProjRoundTrip(3577);
  }

  @Test
  public void testProjRoundTrip_LambertConformalConic2SP_Vicgrid_3111() throws FactoryException {
    assertProjRoundTrip(3111);
  }

  @Test
  public void testProjRoundTrip_PolarStereographicB_NSIDC_3413() throws FactoryException {
    assertProjRoundTrip(3413);
  }

  @Test
  public void testProjRoundTrip_LambertAzimuthalEqualArea_EASE_6931() throws FactoryException {
    assertProjRoundTrip(6931);
  }

  // ---------------------------------------------------------------------------
  // PROJJSON format round-trip tests
  // ---------------------------------------------------------------------------

  @Test
  public void testProjJsonRoundTrip_Geographic_4326() throws FactoryException {
    assertProjJsonRoundTrip(4326);
  }

  @Test
  public void testProjJsonRoundTrip_Geographic_NAD83_4269() throws FactoryException {
    assertProjJsonRoundTrip(4269);
  }

  @Test
  public void testProjJsonRoundTrip_TransverseMercator_32617() throws FactoryException {
    assertProjJsonRoundTrip(32617);
  }

  @Test
  public void testProjJsonRoundTrip_PseudoMercator_3857() throws FactoryException {
    assertProjJsonRoundTrip(3857);
  }

  @Test
  public void testProjJsonRoundTrip_Mercator1SP_3395() throws FactoryException {
    assertProjJsonRoundTrip(3395);
  }

  @Test
  public void testProjJsonRoundTrip_LambertConformalConic2SP_2154() throws FactoryException {
    assertProjJsonRoundTrip(2154);
  }

  @Test
  public void testProjJsonRoundTrip_AlbersEqualArea_5070() throws FactoryException {
    assertProjJsonRoundTrip(5070);
  }

  @Test
  public void testProjJsonRoundTrip_ObliqueStereographic_28992() throws FactoryException {
    assertProjJsonRoundTrip(28992);
  }

  @Test
  public void testProjJsonRoundTrip_PolarStereographicB_3031() throws FactoryException {
    assertProjJsonRoundTrip(3031);
  }

  @Test
  public void testProjJsonRoundTrip_LambertAzimuthalEqualArea_3035() throws FactoryException {
    assertProjJsonRoundTrip(3035);
  }

  @Test
  public void testProjJsonRoundTrip_EquidistantCylindrical_4087() throws FactoryException {
    assertProjJsonRoundTrip(4087);
  }

  @Test
  public void testProjJsonRoundTrip_PolarStereographicA_32661() throws FactoryException {
    assertProjJsonRoundTrip(32661);
  }

  @Test
  public void testProjJsonRoundTrip_TransverseMercator_OSGB_27700() throws FactoryException {
    assertProjJsonRoundTrip(27700);
  }

  @Test
  public void testProjJsonRoundTrip_AlbersEqualArea_Australian_3577() throws FactoryException {
    assertProjJsonRoundTrip(3577);
  }

  @Test
  public void testProjJsonRoundTrip_LambertConformalConic2SP_Vicgrid_3111()
      throws FactoryException {
    assertProjJsonRoundTrip(3111);
  }

  @Test
  public void testProjJsonRoundTrip_PolarStereographicB_NSIDC_3413() throws FactoryException {
    assertProjJsonRoundTrip(3413);
  }

  @Test
  public void testProjJsonRoundTrip_LambertAzimuthalEqualArea_EASE_6931() throws FactoryException {
    assertProjJsonRoundTrip(6931);
  }

  // ---------------------------------------------------------------------------
  // WKT1 format round-trip tests
  // WKT1 includes AUTHORITY["EPSG","xxxx"] so SRID is always preserved.
  // ---------------------------------------------------------------------------

  @Test
  public void testWkt1RoundTrip_Geographic_4326() throws FactoryException {
    assertWkt1RoundTrip(4326);
  }

  @Test
  public void testWkt1RoundTrip_Geographic_NAD83_4269() throws FactoryException {
    assertWkt1RoundTrip(4269);
  }

  @Test
  public void testWkt1RoundTrip_TransverseMercator_32617() throws FactoryException {
    assertWkt1RoundTrip(32617);
  }

  @Test
  public void testWkt1RoundTrip_PseudoMercator_3857() throws FactoryException {
    assertWkt1RoundTrip(3857);
  }

  @Test
  public void testWkt1RoundTrip_Mercator1SP_3395() throws FactoryException {
    assertWkt1RoundTrip(3395);
  }

  @Test
  public void testWkt1RoundTrip_LambertConformalConic2SP_2154() throws FactoryException {
    assertWkt1RoundTrip(2154);
  }

  @Test
  public void testWkt1RoundTrip_LambertAzimuthalEqualArea_Spherical_2163() throws FactoryException {
    assertWkt1RoundTrip(2163);
  }

  @Test
  public void testWkt1RoundTrip_AlbersEqualArea_5070() throws FactoryException {
    assertWkt1RoundTrip(5070);
  }

  @Test
  public void testWkt1RoundTrip_ObliqueStereographic_28992() throws FactoryException {
    assertWkt1RoundTrip(28992);
  }

  @Test
  public void testWkt1RoundTrip_PolarStereographicB_3031() throws FactoryException {
    assertWkt1RoundTrip(3031);
  }

  @Test
  public void testWkt1RoundTrip_LambertAzimuthalEqualArea_3035() throws FactoryException {
    assertWkt1RoundTrip(3035);
  }

  @Test
  public void testWkt1RoundTrip_Mercator1SP_Spherical_3785() throws FactoryException {
    assertWkt1RoundTrip(3785);
  }

  @Test
  public void testWkt1RoundTrip_EquidistantCylindrical_4087() throws FactoryException {
    assertWkt1RoundTrip(4087);
  }

  @Test
  public void testWkt1RoundTrip_PolarStereographicA_32661() throws FactoryException {
    assertWkt1RoundTrip(32661);
  }

  @Test
  public void testWkt1RoundTrip_TransverseMercator_OSGB_27700() throws FactoryException {
    assertWkt1RoundTrip(27700);
  }

  @Test
  public void testWkt1RoundTrip_AlbersEqualArea_Australian_3577() throws FactoryException {
    assertWkt1RoundTrip(3577);
  }

  @Test
  public void testWkt1RoundTrip_LambertConformalConic2SP_Vicgrid_3111() throws FactoryException {
    assertWkt1RoundTrip(3111);
  }

  @Test
  public void testWkt1RoundTrip_PolarStereographicB_NSIDC_3413() throws FactoryException {
    assertWkt1RoundTrip(3413);
  }

  @Test
  public void testWkt1RoundTrip_LambertAzimuthalEqualArea_EASE_6931() throws FactoryException {
    assertWkt1RoundTrip(6931);
  }

  @Test
  public void testWkt1RoundTrip_Krovak_2065() throws FactoryException {
    // Krovak fails for PROJ/PROJJSON export but WKT1 is GeoTools-native, so it works
    assertWkt1RoundTrip(2065);
  }

  @Test
  public void testWkt1RoundTrip_HotineObliqueMercator_2056() throws FactoryException {
    // Hotine Oblique Mercator fails for PROJ/PROJJSON export but works for WKT1
    assertWkt1RoundTrip(2056);
  }

  // ---------------------------------------------------------------------------
  // WKT2 format round-trip tests
  // WKT2 goes through proj4sedona for both export and import.
  // Note: EPSG:28992 (Oblique Stereographic) is excluded due to floating-point
  // drift in latitude parameters across round-trips.
  // ---------------------------------------------------------------------------

  @Test
  public void testWkt2RoundTrip_Geographic_4326() throws FactoryException {
    assertWkt2RoundTrip(4326);
  }

  @Test
  public void testWkt2RoundTrip_Geographic_NAD83_4269() throws FactoryException {
    assertWkt2RoundTrip(4269);
  }

  @Test
  public void testWkt2RoundTrip_TransverseMercator_32617() throws FactoryException {
    assertWkt2RoundTrip(32617);
  }

  @Test
  public void testWkt2RoundTrip_PseudoMercator_3857() throws FactoryException {
    assertWkt2RoundTrip(3857);
  }

  @Test
  public void testWkt2RoundTrip_Mercator1SP_3395() throws FactoryException {
    assertWkt2RoundTrip(3395);
  }

  @Test
  public void testWkt2RoundTrip_LambertConformalConic2SP_2154() throws FactoryException {
    assertWkt2RoundTrip(2154);
  }

  @Test
  public void testWkt2RoundTrip_LambertAzimuthalEqualArea_Spherical_2163()
      throws FactoryException {
    assertWkt2RoundTrip(2163);
  }

  @Test
  public void testWkt2RoundTrip_AlbersEqualArea_5070() throws FactoryException {
    assertWkt2RoundTrip(5070);
  }

  @Test
  public void testWkt2RoundTrip_PolarStereographicB_3031() throws FactoryException {
    assertWkt2RoundTrip(3031);
  }

  @Test
  public void testWkt2RoundTrip_LambertAzimuthalEqualArea_3035() throws FactoryException {
    assertWkt2RoundTrip(3035);
  }

  @Test
  public void testWkt2RoundTrip_Mercator1SP_Spherical_3785() throws FactoryException {
    assertWkt2RoundTrip(3785);
  }

  @Test
  public void testWkt2RoundTrip_EquidistantCylindrical_4087() throws FactoryException {
    assertWkt2RoundTrip(4087);
  }

  @Test
  public void testWkt2RoundTrip_TransverseMercator_OSGB_27700() throws FactoryException {
    assertWkt2RoundTrip(27700);
  }

  @Test
  public void testWkt2RoundTrip_AlbersEqualArea_Australian_3577() throws FactoryException {
    assertWkt2RoundTrip(3577);
  }

  @Test
  public void testWkt2RoundTrip_LambertConformalConic2SP_Vicgrid_3111() throws FactoryException {
    assertWkt2RoundTrip(3111);
  }

  @Test
  public void testWkt2RoundTrip_PolarStereographicB_NSIDC_3413() throws FactoryException {
    assertWkt2RoundTrip(3413);
  }

  // ---------------------------------------------------------------------------
  // WKT2 import failures — WKT2 re-import fails for certain CRS
  // ---------------------------------------------------------------------------

  @Test
  public void testWkt2RoundTrip_PolarStereographicA_32661_importFails() throws FactoryException {
    assertWkt2ImportFails(32661);
  }

  @Test
  public void testWkt2RoundTrip_LambertAzimuthalEqualArea_EASE_6931_importFails()
      throws FactoryException {
    assertWkt2ImportFails(6931);
  }

  // ---------------------------------------------------------------------------
  // PROJJSON import failures — spherical datums not parseable after round-trip
  // ---------------------------------------------------------------------------

  @Test
  public void testProjJsonRoundTrip_LambertAzimuthalEqualArea_Spherical_2163_importFails()
      throws FactoryException {
    assertProjJsonImportFails(2163);
  }

  @Test
  public void testProjJsonRoundTrip_Mercator1SP_Spherical_3785_importFails()
      throws FactoryException {
    assertProjJsonImportFails(3785);
  }

  // ---------------------------------------------------------------------------
  // Export failures — projection types not supported by proj4sedona
  // ---------------------------------------------------------------------------

  @Test
  public void testSetCrsFails_LambertCylindricalEqualArea_6933() throws FactoryException {
    // GeoTools cannot decode EPSG:6933 at all (no transform for Lambert Cylindrical Equal Area)
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    assertThrows(
        "EPSG:6933 should fail at setCrs",
        IllegalArgumentException.class,
        () -> RasterEditors.setCrs(baseRaster, "EPSG:6933"));
  }

  @Test
  public void testExportFails_Krovak_2065() throws FactoryException {
    assertExportFails(2065);
  }

  @Test
  public void testExportFails_HotineObliqueMercator_2056() throws FactoryException {
    assertExportFails(2056);
  }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /**
   * Assert a full PROJ format round trip: EPSG → RS_CRS("proj") → RS_SetCRS → RS_CRS("proj") →
   * RS_SetCRS → RS_CRS("proj"). The first export from EPSG may carry extra metadata, so we verify
   * idempotency: the second and third exports (from PROJ string input) must be identical.
   */
  private void assertProjRoundTrip(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    // First export from EPSG
    String export1 = RasterAccessors.crs(raster1, "proj");
    assertNotNull("EPSG:" + epsg + " export to PROJ should not be null", export1);

    // Re-import from PROJ string and re-export
    GridCoverage2D raster2 = RasterEditors.setCrs(baseRaster, export1);
    String export2 = RasterAccessors.crs(raster2, "proj");
    assertNotNull("EPSG:" + epsg + " second export to PROJ should not be null", export2);

    // Third round-trip to verify idempotency
    GridCoverage2D raster3 = RasterEditors.setCrs(baseRaster, export2);
    String export3 = RasterAccessors.crs(raster3, "proj");
    assertNotNull("EPSG:" + epsg + " third export to PROJ should not be null", export3);

    assertEquals(
        "EPSG:" + epsg + " PROJ string should be stable after round trip (export2 == export3)",
        export2,
        export3);
  }

  /**
   * Assert a full PROJJSON format round trip: EPSG → RS_CRS("projjson") → RS_SetCRS →
   * RS_CRS("projjson") → RS_SetCRS → RS_CRS("projjson"). The first export from EPSG may carry
   * extra metadata (e.g., datum names), so we verify idempotency: the second and third exports
   * (from PROJJSON string input) must be identical.
   */
  private void assertProjJsonRoundTrip(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    // First export from EPSG
    String export1 = RasterAccessors.crs(raster1, "projjson");
    assertNotNull("EPSG:" + epsg + " export to PROJJSON should not be null", export1);

    // Re-import from PROJJSON string and re-export
    GridCoverage2D raster2 = RasterEditors.setCrs(baseRaster, export1);
    String export2 = RasterAccessors.crs(raster2, "projjson");
    assertNotNull("EPSG:" + epsg + " second export to PROJJSON should not be null", export2);

    // Third round-trip to verify idempotency
    GridCoverage2D raster3 = RasterEditors.setCrs(baseRaster, export2);
    String export3 = RasterAccessors.crs(raster3, "projjson");
    assertNotNull("EPSG:" + epsg + " third export to PROJJSON should not be null", export3);

    assertEquals(
        "EPSG:"
            + epsg
            + " PROJJSON string should be stable after round trip (export2 == export3)",
        export2,
        export3);
  }

  /**
   * Assert a full WKT2 format round trip: EPSG → RS_CRS("wkt2") → RS_SetCRS → RS_CRS("wkt2") →
   * RS_SetCRS → RS_CRS("wkt2"). The first export from EPSG may carry extra metadata, so we verify
   * idempotency: the second and third exports (from WKT2 string input) must be identical.
   */
  private void assertWkt2RoundTrip(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    // First export from EPSG
    String export1 = RasterAccessors.crs(raster1, "wkt2");
    assertNotNull("EPSG:" + epsg + " export to WKT2 should not be null", export1);

    // Re-import from WKT2 string and re-export
    GridCoverage2D raster2 = RasterEditors.setCrs(baseRaster, export1);
    String export2 = RasterAccessors.crs(raster2, "wkt2");
    assertNotNull("EPSG:" + epsg + " second export to WKT2 should not be null", export2);

    // Third round-trip to verify idempotency
    GridCoverage2D raster3 = RasterEditors.setCrs(baseRaster, export2);
    String export3 = RasterAccessors.crs(raster3, "wkt2");
    assertNotNull("EPSG:" + epsg + " third export to WKT2 should not be null", export3);

    assertEquals(
        "EPSG:" + epsg + " WKT2 string should be stable after round trip (export2 == export3)",
        export2,
        export3);
  }

  /**
   * Assert that WKT2 export succeeds but re-import fails for certain CRS types that proj4sedona
   * can serialize to WKT2 but cannot re-parse.
   */
  private void assertWkt2ImportFails(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    // Export should succeed
    String exported = RasterAccessors.crs(raster1, "wkt2");
    assertNotNull("EPSG:" + epsg + " export to WKT2 should succeed", exported);

    // Re-import should fail
    Exception thrown =
        assertThrows(
            "EPSG:" + epsg + " WKT2 re-import should fail",
            IllegalArgumentException.class,
            () -> RasterEditors.setCrs(baseRaster, exported));
    assertTrue(
        "Error message should mention CRS parsing",
        thrown.getMessage().contains("Cannot parse CRS string"));
  }

  /**
   * Assert that PROJJSON export succeeds but re-import fails (spherical datum CRS that proj4sedona
   * can export but GeoTools cannot re-parse).
   */
  private void assertProjJsonImportFails(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    // Export should succeed
    String exported = RasterAccessors.crs(raster1, "projjson");
    assertNotNull("EPSG:" + epsg + " export to PROJJSON should succeed", exported);

    // Re-import should fail
    Exception thrown =
        assertThrows(
            "EPSG:" + epsg + " PROJJSON re-import should fail for spherical datum",
            IllegalArgumentException.class,
            () -> RasterEditors.setCrs(baseRaster, exported));
    assertTrue(
        "Error message should mention CRS parsing",
        thrown.getMessage().contains("Cannot parse CRS string"));
  }

  /**
   * Assert that RS_CRS export fails for projection types not supported by proj4sedona. Tests both
   * "proj" and "projjson" formats.
   */
  private void assertExportFails(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    assertThrows(
        "EPSG:" + epsg + " export to PROJ should fail",
        Exception.class,
        () -> RasterAccessors.crs(raster1, "proj"));

    assertThrows(
        "EPSG:" + epsg + " export to PROJJSON should fail",
        Exception.class,
        () -> RasterAccessors.crs(raster1, "projjson"));
  }

  /**
   * Assert a full WKT1 format round trip: EPSG → RS_CRS("wkt1") → RS_SetCRS → RS_CRS("wkt1").
   * WKT1 includes AUTHORITY["EPSG","xxxx"] so SRID is always preserved.
   */
  private void assertWkt1RoundTrip(int epsg) throws FactoryException {
    GridCoverage2D baseRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 0, 0, 1);
    GridCoverage2D raster1 = RasterEditors.setCrs(baseRaster, "EPSG:" + epsg);

    // Export to WKT1
    String exported = RasterAccessors.crs(raster1, "wkt1");
    assertNotNull("EPSG:" + epsg + " export to WKT1 should not be null", exported);

    // Verify AUTHORITY clause is present with correct EPSG code
    String topAuthority = extractTopLevelAuthority(exported);
    assertEquals(
        "EPSG:" + epsg + " WKT1 should contain top-level AUTHORITY",
        String.valueOf(epsg),
        topAuthority);

    String projNameBefore = extractWkt1ProjectionName(exported);

    // Re-import and re-export
    GridCoverage2D raster2 = RasterEditors.setCrs(baseRaster, exported);
    String reExported = RasterAccessors.crs(raster2, "wkt1");
    assertNotNull("EPSG:" + epsg + " re-export to WKT1 should not be null", reExported);

    String projNameAfter = extractWkt1ProjectionName(reExported);
    assertEquals(
        "EPSG:" + epsg + " projection name should be stable after WKT1 round trip",
        projNameBefore,
        projNameAfter);

    // WKT1 always preserves SRID via AUTHORITY clause
    int sridAfter = RasterAccessors.srid(raster2);
    assertEquals("EPSG:" + epsg + " SRID should be preserved after WKT1 round trip", epsg, sridAfter);
  }

  // ---------------------------------------------------------------------------
  // Extraction helpers
  // ---------------------------------------------------------------------------

  /**
   * Extract PROJECTION name from WKT1, or "Geographic" for GEOGCS without PROJECTION.
   * Handles both PROJECTION["name"] and PROJECTION["name", AUTHORITY[...]].
   */
  private String extractWkt1ProjectionName(String wkt1) {
    Matcher m = WKT1_PROJECTION_PATTERN.matcher(wkt1);
    if (m.find()) {
      return m.group(1);
    }
    // Geographic CRS has no PROJECTION element
    if (wkt1.startsWith("GEOGCS[")) {
      return "Geographic";
    }
    fail("WKT1 should contain PROJECTION or be GEOGCS: " + wkt1.substring(0, Math.min(80, wkt1.length())));
    return null;
  }

  /**
   * Extract the top-level AUTHORITY EPSG code from WKT1. The top-level AUTHORITY is the last one
   * in the string (at the outermost nesting level).
   */
  private String extractTopLevelAuthority(String wkt1) {
    // Find the last AUTHORITY["EPSG","xxxx"] — that's the top-level one
    Matcher m = WKT1_AUTHORITY_PATTERN.matcher(wkt1);
    String lastCode = null;
    while (m.find()) {
      lastCode = m.group(1);
    }
    assertNotNull("WKT1 should contain AUTHORITY[\"EPSG\",\"xxxx\"]", lastCode);
    return lastCode;
  }

}
