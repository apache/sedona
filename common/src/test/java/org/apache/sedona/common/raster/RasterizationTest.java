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

import static org.apache.sedona.common.raster.RasterAccessors.metadata;

import java.awt.image.Raster;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.FunctionsGeoTools;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class RasterizationTest extends RasterTestBase {
  private final WKTReader wktReader = new WKTReader();

  @Test
  public void testRasterizeGeomExtent() throws ParseException, FactoryException {
    GridCoverage2D testRaster = RasterConstructors.makeEmptyRaster(1, "F", 4, 4, 4, 4, 1);
    double[] metadata = metadata(testRaster);

    // Grid aligned polygon completely contained within raster extent
    String wktPolygon = "POLYGON ((5 2, 6 1, 5 3, 7 3, 5 2))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {5.0, 7.0, 1.0, 3.0}, testRaster, metadata, false);

    // Grid aligned polygon in line with raster extent
    wktPolygon = "POLYGON ((4 2, 6 0, 8 2, 6 4, 4 2))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {4.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // Polygon partially outside raster extent
    wktPolygon = "POLYGON ((3 1, 5 -1, 8 2, 6 4, 3 1))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {4.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // Polygon completely outside raster extent
    wktPolygon = "POLYGON ((-1 -1, 0 -2, 1 -1, 0 0, -1 -1))";
    validateRasterizeGeomExtent(wktPolygon, null, testRaster, metadata, false);

    //  Partial pixel alignment
    String wktPolygon5 = "POLYGON ((5.5 2.5, 4.5 0.5, 6.5 1.5, 5.5 2.5))";
    validateRasterizeGeomExtent(
        wktPolygon5, new double[] {4.0, 7.0, 0.0, 3.0}, testRaster, metadata, false);

    GridCoverage2D testRaster_frac =
        RasterConstructors.makeEmptyRaster(
            1, "F", 4, 4, 4.3333, 4.6666, 0.3333, -0.3333, 0, 0, 4326);
    double[] metadata_frac = metadata(testRaster_frac);

    // Grid aligned polygon completely contained within raster extent
    wktPolygon = "Polygon((4.6666 4.0, 4.9999 3.7777, 5.3332 4.0,  4.9999 4.3333,  4.6666 4.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.6666, 5.3332, 3.6667, 4.3333},
        testRaster_frac,
        metadata_frac,
        false);

    //  Partial pixel alignment
    wktPolygon = "Polygon((4.7 3.9, 4.9999 3.9, 5.1 4.0,  4.9999 4.2, 4.7 3.9))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.6666, 5.3332, 3.6667, 4.3333},
        testRaster_frac,
        metadata_frac,
        false);

    // polygon larger than raster extent
    wktPolygon = "Polygon((4.0 3.0, 4.0 2.0, 8.0 2.0, 8.0 5.0, 4.0 5.0, 4.0 3.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.3333, 5.6665, 3.3334, 4.6666},
        testRaster_frac,
        metadata_frac,
        false);

    GridCoverage2D testRaster_neg =
        RasterConstructors.makeEmptyRaster(
            1, "F", 4, 4, -4.3333, -4.6666, 0.3333, -0.3333, 0, 0, 4326);
    double[] metadata_neg = metadata(testRaster_neg);

    // polygon larger than raster extent
    wktPolygon = "Polygon((-8.0 -2.0, -2.0 -2.0, -2.0 -6.0, -8.0 -6.0, -8.0 -2.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {-4.3333, -3.0001, -5.9998, -4.6666},
        testRaster_neg,
        metadata_neg,
        false);

    // horizontal line
    String wktLine = "LINESTRING (5.0 2.0, 6.0 2.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {5.0, 6.0, 1.0, 3.0}, testRaster, metadata, false);

    // horizontal line at the top-left edge
    wktLine = "LINESTRING (4.0 4.0, 5.5 4.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 6.0, 3.0, 4.0}, testRaster, metadata, false);

    // horizontal line at the top-right edge
    wktLine = "LINESTRING (6.5 4.0, 8.0 4.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {6.0, 8.0, 3.0, 4.0}, testRaster, metadata, false);

    // horizontal line at the bottom edge
    wktLine = "LINESTRING (4.0 0.0, 8.0 0.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 8.0, 0.0, 1.0}, testRaster, metadata, false);

    // vertical line
    wktLine = "LINESTRING (5.0 2.0, 5.0 3.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // vertical line at the right edge
    wktLine = "LINESTRING (8.0 0.0, 8.0 3.5)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {7.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // vertical line at the left edge
    wktLine = "LINESTRING (4.0 0.0, 4.0 3.5)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 5.0, 0.0, 4.0}, testRaster, metadata, false);

    // diagonal line
    wktLine = "LINESTRING (5.0 2.0, 6.0 3.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // point tests
    String wktPoint = "POINT (5.0 2.0)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {4.0, 6.0, 1.0, 3.0}, testRaster, metadata, false);

    // intersecting 2 pixels
    wktPoint = "POINT (5.0 2.5)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {4.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // within pixel
    wktPoint = "POINT (5.25 2.25)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);
  }

  @Test
  public void testRasterizeGeomExtentWithBottomUpRaster() throws ParseException, FactoryException {
    GridCoverage2D testRaster = RasterConstructors.makeEmptyRaster(1, 4, 4, 4, 0, 1, 1, 0, 0, 4326);
    double[] metadata = metadata(testRaster);

    // Grid aligned polygon completely contained within raster extent
    String wktPolygon = "POLYGON ((5 2, 6 1, 5 3, 7 3, 5 2))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {5.0, 7.0, 1.0, 3.0}, testRaster, metadata, false);

    // Grid aligned polygon in line with raster extent
    wktPolygon = "POLYGON ((4 2, 6 0, 8 2, 6 4, 4 2))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {4.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // Polygon partially outside raster extent
    wktPolygon = "POLYGON ((3 1, 5 -1, 8 2, 6 4, 3 1))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {4.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // Polygon completely outside raster extent
    wktPolygon = "POLYGON ((-1 -1, 0 -2, 1 -1, 0 0, -1 -1))";
    validateRasterizeGeomExtent(wktPolygon, null, testRaster, metadata, false);

    //  Partial pixel alignment
    String wktPolygon5 = "POLYGON ((5.5 2.5, 4.5 0.5, 6.5 1.5, 5.5 2.5))";
    validateRasterizeGeomExtent(
        wktPolygon5, new double[] {4.0, 7.0, 0.0, 3.0}, testRaster, metadata, false);

    GridCoverage2D testRaster_frac =
        RasterConstructors.makeEmptyRaster(
            1, "F", 4, 4, 4.3333, 3.3334, 0.3333, 0.3333, 0, 0, 4326);
    double[] metadata_frac = metadata(testRaster_frac);

    // Grid aligned polygon completely contained within raster extent
    wktPolygon = "Polygon((4.6666 4.0, 4.9999 3.7777, 5.3332 4.0,  4.9999 4.3333,  4.6666 4.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.6666, 5.3332, 3.6667, 4.3333},
        testRaster_frac,
        metadata_frac,
        false);

    //  Partial pixel alignment
    wktPolygon = "Polygon((4.7 3.9, 4.9999 3.9, 5.1 4.0,  4.9999 4.2, 4.7 3.9))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.6666, 5.3332, 3.6667, 4.3333},
        testRaster_frac,
        metadata_frac,
        false);

    // polygon larger than raster extent
    wktPolygon = "Polygon((4.0 3.0, 4.0 2.0, 8.0 2.0, 8.0 5.0, 4.0 5.0, 4.0 3.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.3333, 5.6665, 3.3334, 4.6666},
        testRaster_frac,
        metadata_frac,
        false);

    GridCoverage2D testRaster_neg =
        RasterConstructors.makeEmptyRaster(
            1, "F", 4, 4, -4.3333, -5.9998, 0.3333, 0.3333, 0, 0, 4326);
    double[] metadata_neg = metadata(testRaster_neg);

    // polygon larger than raster extent
    wktPolygon = "Polygon((-8.0 -2.0, -2.0 -2.0, -2.0 -6.0, -8.0 -6.0, -8.0 -2.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {-4.3333, -3.0001, -5.9998, -4.6666},
        testRaster_neg,
        metadata_neg,
        false);

    // horizontal line
    String wktLine = "LINESTRING (5.0 2.0, 6.0 2.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {5.0, 6.0, 1.0, 3.0}, testRaster, metadata, false);

    // horizontal line at the upper left edge
    wktLine = "LINESTRING (4.0 4.0, 5.5 4.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 6.0, 3.0, 4.0}, testRaster, metadata, false);

    // horizontal line at the top-right edge
    wktLine = "LINESTRING (6.5 4.0, 8.0 4.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {6.0, 8.0, 3.0, 4.0}, testRaster, metadata, false);

    // horizontal line at the bottom edge
    wktLine = "LINESTRING (4.0 0.0, 8.0 0.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 8.0, 0.0, 1.0}, testRaster, metadata, false);

    // vertical line
    wktLine = "LINESTRING (5.0 2.0, 5.0 3.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // vertical line at the right edge
    wktLine = "LINESTRING (8.0 0.0, 8.0 3.5)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {7.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // vertical line at the left edge
    wktLine = "LINESTRING (4.0 0.0, 4.0 3.5)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 5.0, 0.0, 4.0}, testRaster, metadata, false);

    // diagonal line
    wktLine = "LINESTRING (5.0 2.0, 6.0 3.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // point tests
    String wktPoint = "POINT (5.0 2.0)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {4.0, 6.0, 1.0, 3.0}, testRaster, metadata, false);

    // intersecting 2 pixels
    wktPoint = "POINT (5.0 2.5)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {4.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // within pixel
    wktPoint = "POINT (5.25 2.25)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);
  }

  @Test
  public void testRasterizePolygonPartiallyTouchesRaster()
      throws ParseException, FactoryException, TransformException {
    // The metadata of an Alpha Earth image
    GridCoverage2D testRaster =
        RasterConstructors.makeEmptyRaster(1, 1024, 1024, 500000, 4976640, 10, 10, 0, 0, 32610);

    // Grid aligned polygon completely contained within raster extent
    String wktPolygon =
        "POLYGON ((-123.000206 44.960125, -123.000208 44.960031, -122.999961 44.960028, -122.99996 44.960105, -123 44.960105, -123 44.960123, -123.000206 44.960125))";
    Geometry geom = wktReader.read(wktPolygon);
    geom = FunctionsGeoTools.transform(geom, "EPSG:4326", "EPSG:32610");

    // The intersection of the polygon and the raster extent will be a GeometryCollection containing
    // Polygons and LineStrings. We should handle this case gracefully rather than throwing an
    // error.
    List<Object> result = Rasterization.rasterize(geom, testRaster, "B", 10, true, true);
    Assert.assertEquals(2, result.size());
  }

  private static final double DEM_PIXEL_SIZE = 1.0 / 3600.0;

  /**
   * A Copernicus GLO-30 style tile: 256x256, EPSG:4326, 1/3600 degree pixels. 1/3600 has no exact
   * double representation, so a loop that walks the extent by repeatedly adding the pixel size can
   * land a fraction of an ULP short of the extent maximum and run one iteration too many.
   */
  private static GridCoverage2D demLikeTile(double upperLeftX, double upperLeftY)
      throws FactoryException {
    return RasterConstructors.makeEmptyRaster(
        1, "D", 256, 256, upperLeftX, upperLeftY, DEM_PIXEL_SIZE, -DEM_PIXEL_SIZE, 0, 0, 4326);
  }

  /** Asserts the exact set of burned pixels of a rasterized coverage, e.g. "[(1,1)]" or "[]". */
  private void assertBurnedPixels(GridCoverage2D rasterized, String expected) {
    assertBurnedPixels(rasterized.getRenderedImage().getData(), expected);
  }

  /** Asserts the exact set of burned pixels, e.g. "[(255,255)]" or "[]". */
  private void assertBurnedPixels(List<Object> result, String expected) {
    assertBurnedPixels((Raster) result.get(0), expected);
  }

  private void assertBurnedPixels(Raster burned, String expected) {
    List<String> found = new ArrayList<>();
    for (int y = 0; y < burned.getHeight(); y++) {
      for (int x = 0; x < burned.getWidth(); x++) {
        if (burned.getSampleDouble(x, y, 0) != 0) {
          found.add("(" + x + "," + y + ")");
        }
      }
    }
    Assert.assertEquals("burned pixels", expected, found.toString());
  }

  /** Asserts that exactly one pixel, at (expectedX, expectedY), was burned. */
  private void assertSingleBurnedPixel(List<Object> result, int expectedX, int expectedY) {
    assertBurnedPixels(result, "[(" + expectedX + "," + expectedY + ")]");
  }

  @Test
  public void testRasterizePointOnTileCornerDoesNotOverrunGrid() throws FactoryException {
    GridCoverage2D testRaster = demLikeTile(-180.0, 60.0);
    // A point a fraction of an ULP off the tile's bottom-right corner - the coordinate you get by
    // stepping the grid one pixel at a time rather than multiplying out, as tiled data does. The
    // column loop in rasterizePoint used to overrun to x == 256 and setSample wrote past the end of
    // the 256x256 data buffer: ArrayIndexOutOfBoundsException: Index 65536 out of bounds for
    // length 65536.
    Geometry point =
        new GeometryFactory()
            .createPoint(
                new Coordinate(
                    -180.0 + 255 * DEM_PIXEL_SIZE + DEM_PIXEL_SIZE,
                    60.0 - 255 * DEM_PIXEL_SIZE - DEM_PIXEL_SIZE));

    assertSingleBurnedPixel(
        Rasterization.rasterize(point, testRaster, "D", 150, false, true), 255, 255);
  }

  @Test
  public void testRasterizePolygonGrazingTileCornerDoesNotOverrunGrid() throws FactoryException {
    // Tile origin chosen so the corner longitude/latitude land on the unfavourable side of the
    // rounding; most origins are unaffected, which is why the failure looked sporadic in the field.
    double upperLeftX = -180.0 + 822 * DEM_PIXEL_SIZE;
    double upperLeftY = 60.0 - 546 * DEM_PIXEL_SIZE;
    GridCoverage2D testRaster = demLikeTile(upperLeftX, upperLeftY);
    double rightEdge = upperLeftX + 256 * DEM_PIXEL_SIZE;
    double bottomEdge = upperLeftY - 256 * DEM_PIXEL_SIZE;

    // A zone far larger than the tile that touches it only at the bottom-right corner. The clip
    // against the raster extent degenerates to a point, so rasterizePolygon delegates to
    // rasterizePoint - the path RS_ZonalStatsAll hits when an admin zone merely grazes a tile.
    Geometry zone =
        new GeometryFactory()
            .createPolygon(
                new Coordinate[] {
                  new Coordinate(rightEdge, bottomEdge),
                  new Coordinate(rightEdge + 1.0, bottomEdge),
                  new Coordinate(rightEdge + 1.0, bottomEdge - 1.0),
                  new Coordinate(rightEdge, bottomEdge - 1.0),
                  new Coordinate(rightEdge, bottomEdge)
                });

    assertSingleBurnedPixel(
        Rasterization.rasterize(zone, testRaster, "D", 150, false, true), 255, 255);
  }

  @Test
  public void testRasterizePolygonGrazingTileCornerBurnsNothingInCentroidMode()
      throws FactoryException {
    // Same corner contact as above, under centroid semantics. Clipping degenerates the zone to a
    // point, but a point covers no pixel centre, so nothing should be burned - otherwise
    // RS_ZonalStatsAll reports a pixel count for a zone that contains no pixel centroid.
    double upperLeftX = -180.0 + 822 * DEM_PIXEL_SIZE;
    double upperLeftY = 60.0 - 546 * DEM_PIXEL_SIZE;
    GridCoverage2D testRaster = demLikeTile(upperLeftX, upperLeftY);
    double rightEdge = upperLeftX + 256 * DEM_PIXEL_SIZE;
    double bottomEdge = upperLeftY - 256 * DEM_PIXEL_SIZE;

    Geometry zone =
        new GeometryFactory()
            .createPolygon(
                new Coordinate[] {
                  new Coordinate(rightEdge, bottomEdge),
                  new Coordinate(rightEdge + 1.0, bottomEdge),
                  new Coordinate(rightEdge + 1.0, bottomEdge - 1.0),
                  new Coordinate(rightEdge, bottomEdge - 1.0),
                  new Coordinate(rightEdge, bottomEdge)
                });

    assertBurnedPixels(Rasterization.rasterize(zone, testRaster, "D", 150, false, false), "[]");
    Assert.assertEquals(
        Double.valueOf(0), RasterBandAccessors.getZonalStatsAll(testRaster, zone)[0]);
  }

  @Test
  public void testCroppedOutputResolvesBoundaryContactLikeFullExtent() throws FactoryException {
    // Cell envelopes must be built on the reference raster's grid, not the cropped output's origin.
    // The second point sits exactly on a row boundary and so touches two rows; deriving envelopes
    // from the crop origin put those edges an ULP off and dropped one of them.
    double p = DEM_PIXEL_SIZE;
    double ulx = -180.0;
    double uly = 60.0;
    GridCoverage2D testRaster = demLikeTile(ulx, uly);
    Geometry points =
        new GeometryFactory()
            .createMultiPointFromCoords(
                new Coordinate[] {
                  new Coordinate(ulx + 10.5 * p, uly - 10.5 * p),
                  new Coordinate(ulx + 20.5 * p, uly - 20 * p)
                });

    assertBurnedPixels(
        RasterConstructors.asRaster(points, testRaster, "D", true, 150, null),
        "[(0,0), (10,9), (10,10)]");
    // the same contacts, at full-raster offsets
    assertBurnedPixels(
        RasterConstructors.asRasterWithRasterExtent(points, testRaster, "D", true, 150, null),
        "[(10,10), (20,19), (20,20)]");
  }

  @Test
  public void testUnpairedInterceptPastGridIsDiscardedNotFolded() throws Exception {
    // The bottom scanline of this triangle touches only at x == 2, on a 2px wide raster. The
    // vertical edge leaves an unpaired intercept one past the grid; folding it onto the edge pixel
    // would burn a pixel the polygon never reaches, so it has to be discarded.
    GridCoverage2D testRaster =
        RasterConstructors.makeEmptyRaster(1, "D", 2, 2, 0, 2, 1, -1, 0, 0, 0);
    Geometry zone = wktReader.read("POLYGON ((1 1, 2 1, 2 0.5, 1 1))");

    assertBurnedPixels(
        RasterConstructors.asRasterWithRasterExtent(zone, testRaster, "D", false, 150, null), "[]");
    Assert.assertEquals(
        Double.valueOf(0), RasterBandAccessors.getZonalStatsAll(testRaster, zone)[0]);
  }

  @Test
  public void testLineTraversalDirectionIndependence() throws ParseException, FactoryException {
    // Rasterizing a segment must not depend on which endpoint comes first. This segment has slope
    // -5 and passes exactly through the lattice points (2, 15) and (3, 10); at those corners the
    // traversal must burn only the two cells the segment crosses, identically in both directions.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 20, 1, -1, 0, 0, 0);
    Geometry forward = Constructors.geomFromWKT("LINESTRING (1.25 18.75, 3.75 6.25)", 0);
    Geometry reverse = Constructors.geomFromWKT("LINESTRING (3.75 6.25, 1.25 18.75)", 0);
    double[] a =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(forward, raster, "d", false, 1d, 0d, false), 1);
    double[] b =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(reverse, raster, "d", false, 1d, 0d, false), 1);
    Assert.assertArrayEquals(a, b, 0d);
  }

  @Test
  public void testLineTraversalCornerCrossingSymmetry() throws ParseException, FactoryException {
    // North-up raster with unit pixels, so integer world coordinates land on grid corners.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 20, 1, -1, 0, 0, 0);

    // Shallow slope (1/2) through the corners (2,3), (4,4), (6,5), (8,6), (10,7).
    assertLineRasterizationSymmetric(raster, "LINESTRING (1.5 2.75, 10.5 7.25)");
    // Steep slope (3) through the corners (1,2), (2,5), (3,8), (4,11).
    assertLineRasterizationSymmetric(raster, "LINESTRING (0.75 1.25, 4.25 11.75)");
    // Steep negative slope (-2) through the corners (2,16), (3,14), (4,12), (5,10).
    assertLineRasterizationSymmetric(raster, "LINESTRING (1.25 17.5, 5.25 9.5)");
    // Slope 1 diagonal through a run of corners (3,3), (4,4), ..., (9,9).
    assertLineRasterizationSymmetric(raster, "LINESTRING (2.5 2.5, 9.5 9.5)");
  }

  @Test
  public void testLineTraversalBottomUpSymmetry() throws ParseException, FactoryException {
    // Bottom-up raster (positive scaleY exercises the row-flip branch in burnCell).
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 0, 1, 1, 0, 0, 0);
    // Slope 2 through the corners (2,4), (3,6), (4,8).
    assertLineRasterizationSymmetric(raster, "LINESTRING (1.25 2.5, 4.75 9.5)");
  }

  @Test
  public void testLineTraversalNonCornerSymmetry() throws ParseException, FactoryException {
    // A segment that never passes through a lattice point is unaffected by the corner-tie fix and
    // was already direction-independent; this guards against a regression in ordinary traversal.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 20, 1, -1, 0, 0, 0);
    assertLineRasterizationSymmetric(raster, "LINESTRING (1.3 2.7, 8.6 11.4)");
  }

  @Test
  public void testLineTraversalNearCornerSymmetry() throws ParseException, FactoryException {
    // A segment whose grid-line crossings fall so close to a lattice corner that the parametric
    // tMax comparison rounds asymmetrically between the two directions, flipping which off-diagonal
    // cell is burned. The robust orientation predicate resolves the crossing order identically in
    // both directions.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 50, 50, 0, 0, 1, -1, 0, 0, 0);
    assertLineRasterizationSymmetric(
        raster,
        "LINESTRING (13.157894736842104 -20.385964912280702, "
            + "23.157894736842106 -11.052631578947368)");
  }

  @Test
  public void testLineEndpointOnGridLineBurnsOnlyCrossedCells()
      throws ParseException, FactoryException {
    // GH-3120: the segment's end vertex (3.8 3.0) sits exactly on the horizontal grid line y = 3.
    // The row below is touched only at that single point, so it must not be burned; GDAL
    // (rasterio all_touched) agrees. The reversed segment must burn the identical set.
    GridCoverage2D raster = unitGrid6x6();
    double[] expected = {
      0, 0, 0, 0, 0, 0,
      0, 1, 1, 0, 0, 0,
      0, 0, 1, 1, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (1.5 4.5, 3.8 3.0)", expected);
    assertRasterizedGrid(raster, "LINESTRING (3.8 3.0, 1.5 4.5)", expected);

    // Leaving the grid line instead of arriving at it: the start vertex's row has positive-length
    // overlap and is burned, and only that side of the line.
    double[] expectedLeaving = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 1, 1, 0,
      0, 0, 0, 0, 1, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (3.8 3.0, 4.5 1.6)", expectedLeaving);
    assertRasterizedGrid(raster, "LINESTRING (4.5 1.6, 3.8 3.0)", expectedLeaving);
  }

  @Test
  public void testLineEndingAtLatticeCornerDoesNotStreak() throws ParseException, FactoryException {
    // GH-3120: this slope -1 segment ends exactly on the lattice corner (3, 3). The floor()-derived
    // end cell used to sit diagonally off the traversal's path, so the termination check never
    // fired and the walk burned an anti-diagonal streak across the raster. Only the two cells the
    // segment passes through may be burned, in both directions.
    GridCoverage2D raster = unitGrid6x6();
    double[] expected = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 1, 0, 0, 0,
      0, 1, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (1.5 1.5, 3.0 3.0)", expected);
    assertRasterizedGrid(raster, "LINESTRING (3.0 3.0, 1.5 1.5)", expected);

    // A segment contained in one cell that ends on the cell's corner burns only that cell.
    double[] expectedSingle = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 1, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (2.2 2.2, 3.0 3.0)", expectedSingle);
  }

  @Test
  public void testLineVertexApexOnLatticeCorner() throws ParseException, FactoryException {
    // A V whose apex vertex lies exactly on the lattice corner (3, 3): the cells above the apex are
    // touched only at that point and stay unburned, matching GDAL.
    GridCoverage2D raster = unitGrid6x6();
    double[] expected = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 1, 1, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (2.5 2.5, 3.0 3.0, 3.5 2.5)", expected);
  }

  @Test
  public void testLineAlongGridLineBurnsHalfOpenCells() throws ParseException, FactoryException {
    // Segments lying exactly along a grid line have no extent across it; the half-open floor()
    // convention keeps the row below / column right of the line, matching GDAL.
    GridCoverage2D raster = unitGrid6x6();
    double[] expectedAlongHorizontal = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 1, 1, 1, 1, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (1.5 3.0, 4.5 3.0)", expectedAlongHorizontal);
    double[] expectedAlongVertical = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 1, 0, 0,
      0, 0, 0, 1, 0, 0,
      0, 0, 0, 1, 0, 0,
      0, 0, 0, 1, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (3.0 1.5, 3.0 4.5)", expectedAlongVertical);
  }

  @Test
  public void testLineEndpointRuleDoesNotSnapNearbyCoordinates()
      throws ParseException, FactoryException {
    GridCoverage2D raster = unitGrid6x6();
    double[] expectedCrossing = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 1, 1, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };

    // Only an exactly integral pixel coordinate gets endpoint bias. The adjacent doubles cross
    // the grid line by a positive distance, however small, so both neighboring cells are burned.
    String nextUp = Double.toString(Math.nextUp(3.0));
    assertRasterizedGrid(raster, "LINESTRING (2.5 2.5, " + nextUp + " 2.5)", expectedCrossing);
    assertRasterizedGrid(raster, "LINESTRING (" + nextUp + " 2.5, 2.5 2.5)", expectedCrossing);

    String nextDown = Double.toString(Math.nextDown(3.0));
    assertRasterizedGrid(raster, "LINESTRING (3.5 2.5, " + nextDown + " 2.5)", expectedCrossing);
    assertRasterizedGrid(raster, "LINESTRING (" + nextDown + " 2.5, 3.5 2.5)", expectedCrossing);
  }

  @Test
  public void testAllTouchedPolygonWithVertexOnGridLine() throws ParseException, FactoryException {
    // The polygon from GH-3120: vertex (3.8 3.0) lies exactly on a horizontal grid line. The
    // allTouched result must match GDAL (rasterio all_touched) exactly.
    GridCoverage2D raster = unitGrid6x6();
    Geometry geom =
        Constructors.geomFromWKT("POLYGON ((1.5 1.5, 3.8 3.0, 4.5 4.4, 3.4 3.5, 1.5 1.5))", 0);
    double[] band =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(geom, raster, "d", true, 1d, 0d, false), 1);
    double[] expected = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 1, 0,
      0, 0, 1, 1, 1, 0,
      0, 1, 1, 1, 0, 0,
      0, 1, 1, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    Assert.assertArrayEquals(expected, band, 0d);
  }

  @Test
  public void testLineClippedAtFarRasterEdgeKeepsEdgePixels() throws FactoryException {
    // 1/3600 degree pixels have no exact double representation. On this tile origin the right edge
    // converts to pixel x = 256.00000000004, one rounding error past the grid, so a segment that
    // ends on or crosses it makes the traversal visit a column outside the raster, which burnCell
    // skips; the bottom edge lands just inside, at y = 255.99999999998886. Either way the edge cell
    // is visited before the walk leaves the grid, so no pixel inside the raster may be lost. The
    // reference is the same geometry rasterized on a larger raster with the same origin (so every
    // unclipped pixel coordinate is bit-for-bit identical), cropped back to the tile.
    double p = 1.0 / 3600.0;
    double ulx = -180.0 + 822 * p;
    double uly = 60.0 - 546 * p;
    GridCoverage2D tile =
        RasterConstructors.makeEmptyRaster(1, "d", 256, 256, ulx, uly, p, -p, 0, 0, 4326);
    GridCoverage2D larger =
        RasterConstructors.makeEmptyRaster(1, "d", 272, 272, ulx, uly, p, -p, 0, 0, 4326);
    double right = ulx + 256 * p;
    double bottom = uly - 256 * p;
    GeometryFactory factory = new GeometryFactory();

    // Leaves through the right edge along row 100: columns 200..255.
    LineString acrossRight =
        line(factory, ulx + 200.5 * p, uly - 100.5 * p, right + 10 * p, uly - 100.5 * p);
    assertBurnedCount(tile, acrossRight, 56, 255, 100);
    // Leaves through the bottom edge along column 100: rows 200..255.
    LineString acrossBottom =
        line(factory, ulx + 100.5 * p, uly - 200.5 * p, ulx + 100.5 * p, bottom - 10 * p);
    assertBurnedCount(tile, acrossBottom, 56, 100, 255);
    // Ends exactly on the extent maximum: columns / rows 250..255.
    LineString toRight = line(factory, ulx + 250.5 * p, uly - 100.5 * p, right, uly - 100.5 * p);
    assertBurnedCount(tile, toRight, 6, 255, 100);
    LineString toBottom = line(factory, ulx + 100.5 * p, uly - 250.5 * p, ulx + 100.5 * p, bottom);
    assertBurnedCount(tile, toBottom, 6, 100, 255);

    Geometry[] geometries = {
      acrossRight,
      acrossBottom,
      toRight,
      toBottom,
      // oblique segments leaving through the right edge, the bottom edge and the corner
      line(factory, ulx + 240.3 * p, uly - 180.6 * p, right + 7.1 * p, uly - 190.2 * p),
      line(factory, ulx + 180.6 * p, uly - 240.3 * p, ulx + 190.2 * p, bottom - 7.1 * p),
      line(factory, ulx + 240.3 * p, uly - 247.7 * p, right + 7.1 * p, bottom - 3.3 * p),
      line(factory, ulx + 250.5 * p, uly - 252.5 * p, right, bottom),
      // a polygon crossing both far edges; allTouched burns its ring through the same traversal
      factory.createPolygon(
          new Coordinate[] {
            new Coordinate(ulx + 230.3 * p, uly - 240.6 * p),
            new Coordinate(right + 9.2 * p, uly - 235.1 * p),
            new Coordinate(ulx + 245.7 * p, bottom - 8.4 * p),
            new Coordinate(ulx + 230.3 * p, uly - 240.6 * p)
          })
    };
    for (Geometry geom : geometries) {
      for (Geometry g : new Geometry[] {geom, geom.reverse()}) {
        double[] expected = crop(burn(g, larger, true), 272, 256, 256);
        Assert.assertArrayEquals(g.toText(), expected, burn(g, tile, true), 0d);
      }
    }
  }

  @Test
  public void testAllTouchedPointOnRowBoundaryBurnsBothRows() throws FactoryException {
    // The point sits on the row 0 / row 1 grid line, so under allTouched it touches both rows. The
    // extent snap round-trips through BigDecimal and lands one ULP off that grid line
    // (59.99916666666666 vs 59.99916666666667), so an exact == test did not widen the extent and
    // row 1 was never considered.
    double p = 1.0 / 3600.0;
    double ulx = -180.0;
    double uly = 60.0 - 2 * p;
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "D", 256, 256, ulx, uly, p, -p, 0, 0, 4326);
    Geometry point = new GeometryFactory().createPoint(new Coordinate(ulx + 10.5 * p, uly - p));

    assertBurnedPixels(
        Rasterization.rasterize(point, raster, "D", 150, false, true), "[(10,0), (10,1)]");
    ReferencedEnvelope extent =
        Rasterization.rasterizeGeomExtent(point, raster, metadata(raster), true);
    Assert.assertEquals(2, Math.round((extent.getMaxY() - extent.getMinY()) / p));
    Assert.assertEquals(uly, extent.getMaxY(), p * 1e-6);
  }

  @Test
  public void testAllTouchedPointJustInsideRowBurnsOnlyThatRow() throws FactoryException {
    // Two ULPs above the row 0 / row 1 grid line: within rounding of the line, so the extent is
    // widened to include row 1, but the point lies strictly inside row 0 and must burn only row 0.
    // The widened extent is only the search window; each cell is still tested against the point.
    double p = 1.0 / 3600.0;
    double ulx = -180.0;
    double uly = 60.0 - 2 * p;
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "D", 256, 256, ulx, uly, p, -p, 0, 0, 4326);
    Geometry point =
        new GeometryFactory().createPoint(new Coordinate(ulx + 10.5 * p, Math.nextUp(uly - p)));

    assertBurnedPixels(Rasterization.rasterize(point, raster, "D", 150, false, true), "[(10,0)]");
  }

  @Test
  public void testAllTouchedPointOnGridLinesAwayFromOriginBurnsEveryTouchedCell()
      throws FactoryException {
    // On this tile the row 35 / row 36 and the column 18 / column 19 grid lines both snap a few
    // ULPs off through the BigDecimal round trip, so a point on either line, or on their crossing,
    // lost the row or column on the far side of the line, in full-extent and cropped output alike.
    double p = 1.0 / 3600.0;
    double ulx = -180.0 + 152574 * p;
    double uly = 90.0 - 262004 * p;
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "D", 64, 64, ulx, uly, p, -p, 0, 0, 4326);
    GeometryFactory factory = new GeometryFactory();
    Geometry onRowLine = factory.createPoint(new Coordinate(ulx + 10.5 * p, uly - 36 * p));
    Geometry onColumnLine = factory.createPoint(new Coordinate(ulx + 19 * p, uly - 10.5 * p));
    Geometry onCorner = factory.createPoint(new Coordinate(ulx + 19 * p, uly - 36 * p));

    assertBurnedPixels(
        Rasterization.rasterize(onRowLine, raster, "D", 150, false, true), "[(10,35), (10,36)]");
    assertBurnedPixels(
        Rasterization.rasterize(onColumnLine, raster, "D", 150, false, true), "[(18,10), (19,10)]");
    assertBurnedPixels(
        Rasterization.rasterize(onCorner, raster, "D", 150, false, true),
        "[(18,35), (19,35), (18,36), (19,36)]");
    // cropped to the geometry extent: the same contacts at crop-local offsets
    assertBurnedPixels(
        Rasterization.rasterize(onCorner, raster, "D", 150, true, true),
        "[(0,0), (1,0), (0,1), (1,1)]");
  }

  private static LineString line(
      GeometryFactory factory, double x0, double y0, double x1, double y1) {
    return factory.createLineString(
        new Coordinate[] {new Coordinate(x0, y0), new Coordinate(x1, y1)});
  }

  private static double[] burn(Geometry geom, GridCoverage2D raster, boolean allTouched)
      throws FactoryException {
    return MapAlgebra.bandAsArray(
        RasterConstructors.asRaster(geom, raster, "d", allTouched, 1d, 0d, false), 1);
  }

  private static double[] crop(double[] band, int bandWidth, int width, int height) {
    double[] cropped = new double[width * height];
    for (int y = 0; y < height; y++) {
      System.arraycopy(band, y * bandWidth, cropped, y * width, width);
    }
    return cropped;
  }

  /** Asserts the burned pixel count of both directions of a line, and that an edge cell is set. */
  private static void assertBurnedCount(
      GridCoverage2D raster, LineString line, int expectedCount, int edgeX, int edgeY)
      throws FactoryException {
    int width = RasterAccessors.getWidth(raster);
    for (Geometry g : new Geometry[] {line, line.reverse()}) {
      double[] band = burn(g, raster, false);
      Assert.assertEquals(
          g.toText(), expectedCount, Arrays.stream(band).filter(v -> v != 0).count());
      Assert.assertEquals(g.toText(), 1d, band[edgeY * width + edgeX], 0d);
    }
  }

  private GridCoverage2D unitGrid6x6() throws FactoryException {
    return RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 6, 1, -1, 0, 0, 0);
  }

  private void assertRasterizedGrid(GridCoverage2D raster, String wkt, double[] expected)
      throws ParseException, FactoryException {
    Geometry geom = Constructors.geomFromWKT(wkt, 0);
    double[] band =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(geom, raster, "d", false, 1d, 0d, false), 1);
    Assert.assertArrayEquals(wkt, expected, band, 0d);
  }

  private void assertLineRasterizationSymmetric(GridCoverage2D raster, String wkt)
      throws ParseException, FactoryException {
    Geometry forward = Constructors.geomFromWKT(wkt, 0);
    Geometry reverse = forward.reverse();
    double[] forwardBand =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(forward, raster, "d", false, 1d, 0d, false), 1);
    double[] reverseBand =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(reverse, raster, "d", false, 1d, 0d, false), 1);
    Assert.assertArrayEquals(forwardBand, reverseBand, 0d);
  }

  private void validateRasterizeGeomExtent(
      String wkt,
      double[] expectedEnvelope,
      GridCoverage2D raster,
      double[] metadata,
      boolean allTouched)
      throws ParseException {
    Geometry geom = wktReader.read(wkt);
    ReferencedEnvelope envelope =
        Rasterization.rasterizeGeomExtent(geom, raster, metadata, allTouched);
    if (expectedEnvelope == null) {
      Assert.assertNull(envelope);
    } else {
      Assert.assertEquals(expectedEnvelope[0], envelope.getMinX(), FP_TOLERANCE);
      Assert.assertEquals(expectedEnvelope[1], envelope.getMaxX(), FP_TOLERANCE);
      Assert.assertEquals(expectedEnvelope[2], envelope.getMinY(), FP_TOLERANCE);
      Assert.assertEquals(expectedEnvelope[3], envelope.getMaxY(), FP_TOLERANCE);
    }
  }
}
