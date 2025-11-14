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

import java.io.IOException;
import java.util.Arrays;
import org.apache.sedona.common.Constructors;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public class RasterBandAccessorsTest extends RasterTestBase {

  @Test
  public void testBandNoDataValueCustomBand() throws FactoryException {
    int width = 5, height = 10;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, width, height, 53, 51, 1, 1, 0, 0, 4326);
    double[] values = new double[width * height];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 1;
    }
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 2, 1d);
    assertNotNull(RasterBandAccessors.getBandNoDataValue(emptyRaster, 2));
    assertEquals(1, RasterBandAccessors.getBandNoDataValue(emptyRaster, 2), 1e-9);
    assertNull(RasterBandAccessors.getBandNoDataValue(emptyRaster));
  }

  @Test
  public void testBandNoDataValueDefaultBand() throws FactoryException {
    int width = 5, height = 10;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, width, height, 53, 51, 1, 1, 0, 0, 4326);
    double[] values = new double[width * height];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 1;
    }
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 1d);
    assertNotNull(RasterBandAccessors.getBandNoDataValue(emptyRaster));
    assertEquals(1, RasterBandAccessors.getBandNoDataValue(emptyRaster), 1e-9);
  }

  @Test
  public void testBandNoDataValueDefaultNoData() throws FactoryException {
    int width = 5, height = 10;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, "I", width, height, 53, 51, 1, 1, 0, 0, 0);
    double[] values = new double[width * height];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 1;
    }
    assertNull(RasterBandAccessors.getBandNoDataValue(emptyRaster, 1));
  }

  @Test
  public void testBandNoDataValueIllegalBand() throws FactoryException, IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RasterBandAccessors.getBandNoDataValue(raster, 2));
    assertEquals("Provided band index 2 is not present in the raster", exception.getMessage());
  }

  @Test
  public void testZonalStatsIntersectingNoPixelData() throws FactoryException, ParseException {
    double[][] pixelsValues =
        new double[][] {
          new double[] {
            3, 7, 5, 40, 61, 70, 60, 80, 27, 55, 35, 44, 21, 36, 53, 54, 86, 28, 45, 24, 99, 22, 18,
            98, 10
          }
        };
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "", 5, 5, 1, -1, 1, -1, 0, 0, 0, pixelsValues);
    Geometry extent =
        Constructors.geomFromWKT(
            "POLYGON ((5.822754 -6.620957, 6.965332 -6.620957, 6.965332 -5.834616, 5.822754 -5.834616, 5.822754 -6.620957))",
            0);

    Double actualZonalStats = RasterBandAccessors.getZonalStats(raster, extent, "mode");
    assertNull(actualZonalStats);

    String actualZonalStatsAll =
        Arrays.toString(RasterBandAccessors.getZonalStatsAll(raster, extent));
    String expectedZonalStatsAll = "[0.0, null, null, null, null, null, null, null, null]";
    assertEquals(expectedZonalStatsAll, actualZonalStatsAll);
  }

  @Test
  public void testZonalStatsForVerticalLineBug() throws Exception {
    String wkt =
        "POLYGON ((989675.3 221207.04013636176, 989675.3 221109.675, 989435.2175433404 221109.675, 989433.6550697018 221110.53743814293, 989419.3 221118.46099022447, 989419.3 221137.08165200218, 989449.9606422258 221192.9473339209, 989465.7613533186 221221.73707975633, 989508.9887178271 221300.49928501665, 989630.3976442496 221232.27312269452, 989639.3630089872 221227.23492283348, 989675.3 221207.04013636176))";

    Geometry geom = Constructors.geomFromWKT(wkt, 2263);

    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, 256, 256, 989419.3, 221365.675, 1, -1, 0, 0, 2263);

    Double actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "mean", true);
    Double expected = 0.0;
    assertEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testZonalStats() throws FactoryException, ParseException, IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 221170 4197590, 236722 4204770))";
    Geometry geom = Constructors.geomFromWKT(polygon, RasterAccessors.srid(raster));

    double actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", false, false);
    double expected = 1.0795427E7;
    assertEquals(expected, actual, 0d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 2, "mean", false, false);
    expected = 220.7711988936036;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "count");
    expected = 185104.0;
    assertEquals(expected, actual, 0.1d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 3, "variance", false, false);
    expected = 13553.040611690152;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, "max");
    expected = 255.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "min", false, false);
    expected = 0.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sd", false, false);
    expected = 92.3801832204387;
    assertEquals(expected, actual, FP_TOLERANCE);

    geom =
        Constructors.geomFromWKT(
            "POLYGON ((-77.96672569800863073 37.91971182746296876, -77.9688630154902711 37.89620133516485367, -77.93936803424354309 37.90517806858776595, -77.96672569800863073 37.91971182746296876))",
            0);
    Double statValue =
        RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", false, false, true);
    assertNotNull(statValue);

    Geometry nonIntersectingGeom =
        Constructors.geomFromWKT(
            "POLYGON ((-78.22106647832458748 37.76411511479908967, -78.20183062098976734 37.72863564460374874, -78.18088490966962922 37.76753482276972562, -78.22106647832458748 37.76411511479908967))",
            0);
    statValue =
        RasterBandAccessors.getZonalStats(
            raster, nonIntersectingGeom, 1, "sum", false, false, true);
    assertNull(statValue);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RasterBandAccessors.getZonalStats(
                raster, nonIntersectingGeom, 1, "sum", false, false, false));
  }

  @Test
  public void testZonalStatsWithMultiPolygonAndAllNoDataValues()
      throws IOException, ParseException, FactoryException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "allNoData.tiff");
    String polygon =
        "SRID=2263;MULTIPOLYGON (((994691.8668410989 222732.55147949007, 994838.9964562772 222657.33114599282, 994835.8129632992 222651.32007158513, 994866.5497303953 222635.3891649635, 994894.9626119346 222620.6621292782, 994897.9281735104 222627.20212832885, 994912.1027229384 222619.95519748307, 994956.740641462 222597.13404523468, 994906.6541878665 222501.29768674026, 994887.6889911637 222465.0093761085, 994837.7340744453 222492.35408672754, 994853.625535307 222529.50849369404, 994840.917607888 222537.65418126856, 994854.5478470477 222563.6338512456, 994741.1935887854 222632.66443061878, 994738.9934715481 222626.82248078822, 994702.4691429745 222651.24039529508, 994697.0023239915 222639.56843077522, 994678.9672543779 222648.3079601465, 994685.5799214249 222666.9009330018, 994665.0506513546 222677.4596114837, 994659.5075353233 222666.0714385335, 994632.9559455585 222677.04472308207, 994638.4121639903 222687.65317858435, 994591.4049194932 222709.53064703743, 994586.6935214218 222697.38903859386, 994616.2676593014 222684.09473134452, 994605.3813627969 222665.16367271703, 994622.8224811119 222657.34304865566, 994607.9605241728 222623.97575110715, 994564.0366535546 222648.86690007558, 994557.0215804322 222635.22727109503, 994598.6762462581 222611.8864745539, 994575.6274241732 222565.97242495706, 994491.2032010661 222611.58729253453, 994505.0971421576 222638.26311996125, 994514.7620293793 222656.81908144747, 994546.3321385295 222717.43209184796, 994583.15469286 222788.13068275736, 994691.8668410989 222732.55147949007)), ((994694.3583764347 222594.7954121792, 994682.9462650565 222576.25757348718, 994714.7318199909 222557.2271746981, 994731.0831743629 222587.97634928412, 994751.7483152337 222576.00672128247, 994733.293978232 222547.9483216761, 994757.1527337709 222535.40880340294, 994776.7344568828 222568.78807561978, 994798.9932181359 222556.2664852004, 994773.956981773 222512.28235275837, 994787.2044639152 222504.6640554264, 994803.0287520508 222535.95262718352, 994833.2445882489 222519.60684072823, 994816.8991777989 222489.39134959964, 994882.542139174 222455.1610012155, 994860.12053293 222412.2592701621, 994588.1726443635 222559.1938735171, 994614.5758814359 222614.37291736386, 994649.5364776199 222593.17272477841, 994663.1308743045 222615.95305783482, 994694.3583764347 222594.7954121792)))";
    Geometry geom = Constructors.geomFromEWKT(polygon);

    Double actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "count", false, false);
    double expected = 20470.0;
    assertEquals(expected, actual, 0d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", false, false);
    expected = 0;
    assertEquals(expected, actual, 0d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "mean", false, true);
    assertNull(actual);
  }

  @Test
  public void testZonalStatsWithNoData() throws IOException, FactoryException, ParseException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    String polygon =
        "POLYGON((-167.750000 87.750000, -155.250000 87.750000, -155.250000 40.250000, -180.250000 40.250000, -167.750000 87.750000))";
    // Testing implicit CRS transformation
    Geometry geom = Constructors.geomFromWKT(polygon, 0);

    double actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", false, true);
    double expected = 3229013.0;
    assertEquals(expected, actual, 0d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "mean", false, true);
    expected = 226.61330619692416;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "count");
    expected = 14249.0;
    assertEquals(expected, actual, 0.1d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, "variance");
    expected = 5596.966403503485;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, "max");
    expected = 255.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "min", false, true);
    expected = 1.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sd", false, true);
    expected = 74.81287592054916;
    assertEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testRasterization1() throws FactoryException, ParseException, IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test7.tiff");
    String wkt =
        "POLYGON ((-0.897979307443705 52.22640443968169, -0.897982946766236 52.22638696176784, -0.89799206869722 52.22637025568977, -0.898006322680796 52.22635496345145, -0.89802516094114 52.22634167272326, -0.898047859533683 52.2263308942584, -0.898073546166002 52.22632304226535, -0.898101233719275 52.22631841849023, -0.898129858182076 52.22631720062118, -0.89815831953887 52.22631943546002, -0.898185524042003 52.2263250371237, -0.898210426242843 52.22633379034471, -0.898232069167008 52.22634535874342, -0.898249621089847 52.22635929775475, -0.89826240749905 52.22637507171224, -0.898269937016031 52.22639207443311, -0.898271920279954 52.226409652513254, -0.898268281068583 52.226427130437145, -0.898259159228442 52.226443836537406, -0.89824490530156 52.226459128806695, -0.898226067055136 52.22647241956989, -0.898203368431614 52.226483198068486, -0.898177681728134 52.22649105008885, -0.898149994074354 52.226495673880706, -0.898121369497 52.22649689175339, -0.898092908029046 52.22649465690451, -0.898065703435071 52.22648905521864, -0.898040801177534 52.226480301966674, -0.898019158239452 52.226468733532954, -0.898001606347591 52.2264547944879, -0.897988820009547 52.22643902050307, -0.897981290593075 52.22642201776546, -0.897979307443705 52.22640443968169))";
    Geometry geom = Constructors.geomFromWKT(wkt, 4326);

    double[] actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false, false))
            .mapToDouble(Double::doubleValue)
            .toArray();

    double[] expected = new double[] {14.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testRasterization2() throws FactoryException, ParseException, IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");

    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((223494.02395197155 4217428.006125106, 223479.8395471539 4217398.373656692, 223505.34101226972 4217373.172295491, 223535.14466627932 4217360.826244754, 223565.4369792635 4217365.465341343, 223601.56141698774 4217373.684776339, 223638.23068926093 4217400.554304458, 223640.98980568675 4217424.9074383825, 223639.6956443374 4217443.73089679, 223617.52307020774 4217455.706515524, 223608.8979089979 4217478.55688384, 223580.76530214178 4217483.17859071, 223544.9615263639 4217484.393749003, 223510.7077988318 4217474.333149947, 223494.02395197155 4217428.006125106))",
            26918);

    double[] actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false, true))
            .mapToDouble(Double::doubleValue)
            .toArray();

    double[] expected = new double[] {7.0, 1785.0, 255.0, 255.0, 255.0, 0.0, 0.0, 255.0, 255.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testRasterization3() throws FactoryException, ParseException, IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test8.tiff");

    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON((0.719049156542861 52.250273602713776,0.71904724344863 52.2502560225192,0.71903977906225 52.250239009194175,0.719027050241651 52.250223216550275,0.719009546152358 52.25020925148843,0.718987939468514 52.25019765067622,0.718963060522109 52.25018885992432,0.718935865393908 52.25018321705467,0.718907399172285 52.25018093891829,0.718878755791851 52.250182113062095,0.71885103599512 52.250186694364565,0.718825305032585 52.250194506769795,0.718802551726619 52.25020525005296,0.718783650472204 52.25021851135763,0.718769327634697 52.25023378106131,0.718760133635908 52.25025047235962,0.718756421801201 52.25026794381658,0.718758334780629 52.250285524014515,0.718765799065996 52.25030253735607,0.718778527814752 52.250318330027135,0.718796031872307 52.25033229512267,0.718817638569352 52.25034389596996,0.718842517571885 52.250352686752976,0.718869712790598 52.25035832964506,0.718898179123338 52.25036060779177,0.718926822618574 52.25035943364462,0.718954542516319 52.25035485232562,0.718980273550699 52.25034703989321,0.719003026888402 52.25033629657635,0.719021928129617 52.2503230352366,0.719036250910994 52.2503077655018,0.719045444819272 52.25029107418108,0.719049156542861 52.250273602713776))",
            4326);
    double[] actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, true, true))
            .mapToDouble(Double::doubleValue)
            .toArray();

    double[] expected = new double[] {2.0, 10.0, 5.0, 5.0, 5.0, 0.0, 0.0, 5.0, 5.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testZonalStatsAll() throws IOException, FactoryException, ParseException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((-8673439.6642 4572993.5327, -8673155.5737 4563873.2099, -8701890.3259 4562931.7093, -8682522.8735 4572703.8908, -8673439.6642 4572993.5327))";
    Geometry geom = Constructors.geomFromWKT(polygon, 3857);

    double[] actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false, false, false))
            .mapToDouble(Double::doubleValue)
            .toArray();
    double[] expected =
        new double[] {
          185104.0,
          1.0795427E7,
          58.32087367104147,
          0.0,
          0.0,
          92.3801832204387,
          8534.098251841822,
          0.0,
          255.0
        };
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    geom =
        Constructors.geomFromWKT(
            "POLYGON ((-77.96672569800863073 37.91971182746296876, -77.9688630154902711 37.89620133516485367, -77.93936803424354309 37.90517806858776595, -77.96672569800863073 37.91971182746296876))",
            0);
    actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false, false, false))
            .mapToDouble(Double::doubleValue)
            .toArray();
    assertNotNull(actual);

    Geometry nonIntersectingGeom =
        Constructors.geomFromWKT(
            "POLYGON ((-78.22106647832458748 37.76411511479908967, -78.20183062098976734 37.72863564460374874, -78.18088490966962922 37.76753482276972562, -78.22106647832458748 37.76411511479908967))",
            0);
    Double[] actualNull =
        RasterBandAccessors.getZonalStatsAll(raster, nonIntersectingGeom, 1, false, false, true);
    assertNull(actualNull);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RasterBandAccessors.getZonalStatsAll(
                raster, nonIntersectingGeom, 1, false, false, false));
  }

  @Test
  public void testZonalStatsAllWithNoData() throws IOException, FactoryException, ParseException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    String polygon =
        "POLYGON((-167.750000 87.750000, -155.250000 87.750000, -155.250000 40.250000, -180.250000 40.250000, -167.750000 87.750000))";
    Geometry geom = Constructors.geomFromWKT(polygon, RasterAccessors.srid(raster));

    double[] actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false, true))
            .mapToDouble(Double::doubleValue)
            .toArray();
    double[] expected =
        new double[] {
          14249.0,
          3229013.0,
          226.61330619692416,
          255.0,
          255.0,
          74.81287592054916,
          5596.966403503485,
          1.0,
          255.0
        };

    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testZonalStatsAllWithEmptyRaster() throws FactoryException, ParseException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 6, 6, 1, -1, 1, -1, 0, 0, 4326);
    double[] bandValue =
        new double[] {
          0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 9, 0, 0, 5, 6, 0, 8, 0, 0, 4, 11, 11, 12, 0, 0, 13, 0, 15,
          16, 0, 0, 0, 0, 0, 0, 0
        };
    raster = MapAlgebra.addBandFromArray(raster, bandValue, 1);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 0d);
    // Testing implicit CRS transformation
    Geometry geom = Constructors.geomFromWKT("POLYGON((2 -2, 2 -6, 6 -6, 6 -2, 2 -2))", 0);

    double[] actual =
        Arrays.stream(RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false, true))
            .mapToDouble(Double::doubleValue)
            .toArray();
    double[] expected = new double[] {13.0, 114.0, 8.7692, 9.0, 11.0, 4.7285, 22.3589, 1.0, 16.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStatsAllWithAllNoData() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values =
        new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0d);
    double[] actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster);
    double[] expected = {0.0, 0.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStats() throws FactoryException, IOException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {
          1, 2, 0, 0, 0, 0, 7, 8, 0, 10, 11, 0, 0, 0, 0, 16, 17, 0, 19, 20, 21, 0, 23, 24, 25
        };
    double[] values2 =
        new double[] {
          0, 0, 28, 29, 0, 0, 0, 33, 34, 35, 36, 37, 38, 0, 0, 0, 0, 43, 44, 45, 46, 47, 48, 49, 50
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);

    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");

    double actual = RasterBandAccessors.getSummaryStats(emptyRaster, "count", 2, true);
    double expected = 16.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "sum", 2, true);
    expected = 642.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "mean", 2, true);
    expected = 40.125;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "stddev", 2, true);
    expected = 6.9988838395847095;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "min", 2, true);
    expected = 28.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "max", 2, true);
    expected = 50.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "count", 1, false);
    expected = 1036800.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "sum", 1, false);
    expected = 2.06233487E8;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "mean", 1, false);
    expected = 198.91347125792052;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "stddev", 1, false);
    expected = 95.09054096111336;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "min", 1, false);
    expected = 0.0;
    assertEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStatsAllWithEmptyRaster() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {
          1, 2, 0, 0, 0, 0, 7, 8, 0, 10, 11, 0, 0, 0, 0, 16, 17, 0, 19, 20, 21, 0, 23, 24, 25
        };
    double[] values2 =
        new double[] {
          0, 0, 28, 29, 0, 0, 0, 33, 34, 35, 36, 37, 38, 0, 0, 0, 0, 43, 44, 45, 46, 47, 48, 49, 50
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    double[] actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster, 1, false);
    double[] expected = {25.0, 204.0, 8.1600, 9.2765, 0.0, 25.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster, 2);
    expected = new double[] {16.0, 642.0, 40.125, 6.9988838395847095, 28.0, 50.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster);
    expected = new double[] {14.0, 204.0, 14.5714, 7.7617, 1.0, 25.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStatsAllWithRaster() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    double[] actual = RasterBandAccessors.getSummaryStatsAll(raster, 1, false);
    double[] expected = {1036800.0, 2.06233487E8, 198.9134, 95.0905, 0.0, 255.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(raster, 1);
    expected = new double[] {928192.0, 2.06233487E8, 222.1883, 70.2055, 1.0, 255.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(raster);
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testCountWithEmptyRaster() throws FactoryException {
    // With each parameter and excludeNoDataValue as true
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {0, 0, 0, 5, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0};
    double[] values2 =
        new double[] {0, 0, 0, 6, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    long actual = RasterBandAccessors.getCount(emptyRaster, 1, false);
    long expected = 25;
    assertEquals(expected, actual);

    // with just band parameter
    actual = RasterBandAccessors.getCount(emptyRaster, 2);
    expected = 4;
    assertEquals(expected, actual);

    // with no parameters except raster
    actual = RasterBandAccessors.getCount(emptyRaster);
    expected = 6;
    assertEquals(expected, actual);
  }

  @Test
  public void testCountWithEmptySkewedRaster() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, 5, 5, 23, -25, 1, -1, 2, 2, 0);
    double[] values1 =
        new double[] {0, 0, 0, 3, 4, 6, 0, 3, 2, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0, 2, 2, 0, 0};
    double[] values2 =
        new double[] {0, 0, 0, 0, 3, 2, 5, 6, 0, 0, 3, 2, 0, 0, 2, 3, 0, 0, 0, 0, 0, 3, 4, 4, 3};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    long actual = RasterBandAccessors.getCount(emptyRaster, 2, false);
    long expected = 25;
    assertEquals(expected, actual);

    // without excludeNoDataValue flag
    actual = RasterBandAccessors.getCount(emptyRaster, 1);
    expected = 10;
    assertEquals(expected, actual);

    // just with raster
    actual = RasterBandAccessors.getCount(emptyRaster);
    expected = 10;
    assertEquals(expected, actual);
  }

  @Test
  public void testCountWithRaster() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    long actual = RasterBandAccessors.getCount(raster, 1, false);
    long expected = 1036800;
    assertEquals(expected, actual);

    actual = RasterBandAccessors.getCount(raster, 1);
    expected = 928192;
    assertEquals(expected, actual);

    actual = RasterBandAccessors.getCount(raster);
    expected = 928192;
    assertEquals(expected, actual);
  }

  @Test
  public void testGetBand() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(4, 5, 5, 3, -215, 2, -2, 2, 2, 0);
    double[] values1 =
        new double[] {
          16, 0, 24, 33, 43, 49, 64, 0, 76, 77, 79, 89, 0, 116, 118, 125, 135, 0, 157, 190, 215,
          229, 241, 248, 249
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 3, 0d);
    GridCoverage2D resultRaster = RasterBandAccessors.getBand(emptyRaster, new int[] {3, 3, 3});
    int actual = RasterAccessors.numBands(resultRaster);
    int expected = 3;
    assertEquals(expected, actual);

    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(resultRaster), 0, 9).toArray();
    double[] expectedMetadata =
        Arrays.stream(RasterAccessors.metadata(emptyRaster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    double[] actualBandValues = MapAlgebra.bandAsArray(resultRaster, 3);
    double[] expectedBandValues = MapAlgebra.bandAsArray(emptyRaster, 3);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);
  }

  @Test
  public void testGetBandWithDataTypes() throws FactoryException, IOException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(4, "d", 5, 5, 3, -215, 2, -2, 2, 2, 0);
    double[] values1 =
        new double[] {
          16, 0, 24, 33, 43, 49, 64, 0, 76, 77, 79, 89, 0, 116, 118, 125, 135, 0, 157, 190, 215,
          229, 241, 248, 249
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    String actual = RasterBandAccessors.getBandType(emptyRaster, 1);
    String expected = "REAL_64BITS";
    assertEquals(expected, actual);

    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    raster = RasterBandAccessors.getBand(raster, new int[] {2, 1, 3});
    for (int i = 1; i <= RasterAccessors.numBands(raster); i++) {
      actual = RasterBandAccessors.getBandType(raster, i);
      expected = "UNSIGNED_8BITS";
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetBandWithRaster() throws IOException, FactoryException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    GridCoverage2D resultRaster = RasterBandAccessors.getBand(raster, new int[] {1, 2, 2, 2, 1});
    int actual = RasterAccessors.numBands(resultRaster);
    int expected = 5;
    assertEquals(actual, expected);

    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(resultRaster), 0, 9).toArray();
    double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    double[] actualBandValues = MapAlgebra.bandAsArray(raster, 2);
    double[] expectedBandValues = MapAlgebra.bandAsArray(resultRaster, 2);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);
  }

  @Test
  public void testBandPixelType() throws FactoryException {
    double[] values = new double[] {1.2, 1.1, 32.2, 43.2};

    // create double raster
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, "D", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("REAL_64BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("REAL_64BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    double[] bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    double[] expectedBandValuesD = new double[] {1.2, 1.1, 32.2, 43.2};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesD[i], bandValues[i], 1e-9);
    }
    // create float raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "F", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("REAL_32BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("REAL_32BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    float[] expectedBandValuesF = new float[] {1.2f, 1.1f, 32.2f, 43.2f};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesF[i], bandValues[i], 1e-9);
    }

    // create integer raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "I", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("SIGNED_32BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("SIGNED_32BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    int[] expectedBandValuesI = new int[] {1, 1, 32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesI[i], bandValues[i], 1e-9);
    }

    // create byte raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "B", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    assertEquals("UNSIGNED_8BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("UNSIGNED_8BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    byte[] expectedBandValuesB = new byte[] {1, 1, 32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesB[i], bandValues[i], 1e-9);
    }

    // create short raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "S", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("SIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("SIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    short[] expectedBandValuesS = new short[] {1, 1, 32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesS[i], bandValues[i], 1e-9);
    }

    // create unsigned short raster
    values = new double[] {-1.2, 1.1, -32.2, 43.2};
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "US", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("UNSIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("UNSIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);

    short[] expectedBandValuesUS = new short[] {-1, 1, -32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(
          Short.toUnsignedInt(expectedBandValuesUS[i]),
          Short.toUnsignedInt((short) bandValues[i]),
          1e-9);
    }
  }

  @Test
  public void testBandPixelTypeIllegalBand() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, "US", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> RasterBandAccessors.getBandType(emptyRaster, 5));
    assertEquals("Provided band index 5 is not present in the raster", exception.getMessage());
  }

  @Test
  public void testBandIsNoData() throws FactoryException {
    String[] dataTypes = new String[] {"B", "S", "US", "I", "F", "D"};
    int width = 3;
    int height = 3;
    double noDataValue = 5.0;
    double[] band1 = new double[width * height];
    double[] band2 = new double[width * height];
    Arrays.fill(band1, noDataValue);
    for (int k = 0; k < band2.length; k++) {
      band2[k] = k;
    }
    for (String dataType : dataTypes) {
      GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, dataType, 3, 3, 0, 0, 1);
      raster = MapAlgebra.addBandFromArray(raster, band1, 1, null);
      raster = MapAlgebra.addBandFromArray(raster, band2, 2, null);

      // Currently raster does not have a nodata value, isBandNoData always returns false
      assertFalse(RasterBandAccessors.bandIsNoData(raster, 1));
      assertFalse(RasterBandAccessors.bandIsNoData(raster, 2));

      // Set nodata value for both bands, now band 1 is filled with nodata values
      raster = RasterBandEditors.setBandNoDataValue(raster, 1, noDataValue);
      raster = RasterBandEditors.setBandNoDataValue(raster, 2, noDataValue);
      assertTrue(RasterBandAccessors.bandIsNoData(raster, 1));
      assertFalse(RasterBandAccessors.bandIsNoData(raster, 2));
    }
  }
}
