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
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.raster.serde.Serde;
import org.geotools.api.parameter.ParameterValueGroup;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.processing.operation.Crop;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
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

    // point tests: a point burns exactly the one cell that contains it under GDAL's half-open
    // rule, so its extent is that cell. On a grid corner that is the cell after both lines
    // in pixel order: right of it and, on this north-up raster, below it.
    String wktPoint = "POINT (5.0 2.0)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 1.0, 2.0}, testRaster, metadata, false);

    // on a vertical grid line: the cell to its right
    wktPoint = "POINT (5.0 2.5)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

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

    // point tests: a point burns exactly the one cell that contains it under GDAL's half-open
    // rule, so its extent is that cell. On a grid corner that is the cell after both lines
    // in pixel order: right of it and, on this bottom-up raster, above it.
    String wktPoint = "POINT (5.0 2.0)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // on a vertical grid line: the cell to its right
    wktPoint = "POINT (5.0 2.5)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

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

    // A zone far larger than the tile that touches it only at the bottom-right corner - the case
    // RS_ZonalStatsAll hits when an admin zone merely grazes a tile. It used to overrun the grid.
    // The zone covers no cell of the tile, so, as in GDAL (rasterio all_touched=True), it burns
    // nothing even with allTouched.
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

    assertBurnedPixels(Rasterization.rasterize(zone, testRaster, "D", 150, false, true), "[]");
    Assert.assertEquals(
        Double.valueOf(0), RasterBandAccessors.getZonalStatsAll(testRaster, zone, 1, true)[0]);
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
    // A point's cell must be resolved on the reference raster's grid, not the cropped output's
    // origin, so a cropped output burns the same cell as a full-extent one. The second point sits
    // on
    // a row boundary; under GDAL's half-open rule it burns exactly one cell, (20,19) here, which is
    // also what GDAL (rasterio all_touched=True) burns for these coordinates.
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
        RasterConstructors.asRaster(points, testRaster, "D", true, 150, null), "[(0,0), (10,9)]");
    // the same cells, at full-raster offsets
    assertBurnedPixels(
        RasterConstructors.asRasterWithRasterExtent(points, testRaster, "D", true, 150, null),
        "[(10,10), (20,19)]");
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
  public void testLineCrossingRasterWithBothEndpointsOutside()
      throws ParseException, FactoryException {
    // GH-3255: a segment whose endpoints are both outside the raster used to be dropped, so a line
    // spanning the raster burned nothing.
    GridCoverage2D raster = unitGrid6x6();
    double[] row3 = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      1, 1, 1, 1, 1, 1,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (-1 2.5, 7 2.5)", row3);
    assertRasterizedGrid(raster, "LINESTRING (7 2.5, -1 2.5)", row3);
    double[] diagonal = {
      0, 0, 0, 0, 0, 1,
      0, 0, 0, 0, 1, 0,
      0, 0, 0, 1, 0, 0,
      0, 0, 1, 0, 0, 0,
      0, 1, 0, 0, 0, 0,
      1, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (-1 -1, 7 7)", diagonal);
    assertRasterizedGrid(raster, "LINESTRING (7 7, -1 -1)", diagonal);
    // A middle segment with both vertices outside, between two that end inside.
    double[] zigzag = {
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 1,
      1, 1, 1, 1, 1, 1,
      1, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    };
    assertRasterizedGrid(raster, "LINESTRING (0.5 2.5, -1 3.5, 7 3.5, 5.5 4.5)", zigzag);

    // The other operations that rasterize a geometry see the same cells.
    Geometry spanning = wktReader.read("LINESTRING (-1 2.5, 7 2.5)");
    Assert.assertEquals(
        Double.valueOf(6), RasterBandAccessors.getZonalStats(raster, spanning, 1, "count"));
    GridCoverage2D cropped =
        RasterConstructors.asRaster(spanning, raster, "d", false, 1d, 0d, true);
    Assert.assertEquals(6, RasterAccessors.getWidth(cropped));
    Assert.assertEquals(1, RasterAccessors.getHeight(cropped));
    Assert.assertArrayEquals(
        new double[] {1, 1, 1, 1, 1, 1}, MapAlgebra.bandAsArray(cropped, 1), 0d);

    // Meeting the raster only at a corner, or only along a far edge, still burns nothing.
    double[] nothing = new double[36];
    assertRasterizedGrid(raster, "LINESTRING (-1 5, 1 7)", nothing);
    assertRasterizedGrid(raster, "LINESTRING (5 -1, 7 1)", nothing);
    assertRasterizedGrid(raster, "LINESTRING (6 -1, 6 7)", nothing);
    assertRasterizedGrid(raster, "LINESTRING (-1 0, 7 0)", nothing);
  }

  @Test
  public void testClippedLineKeepsItsSideOfAGridLine() throws ParseException, FactoryException {
    // GH-3255: inside the raster this line runs strictly above y = 3 until its last point. Where it
    // crosses the raster's left edge, y rounds to exactly 3 in double precision; taking that
    // rounded point as the start used to move the line to the row below.
    GridCoverage2D raster = unitGrid6x6();
    double[] rowAbove = new double[36];
    rowAbove[2 * 6] = 1;
    double[] rowBelow = new double[36];
    rowBelow[3 * 6] = 1;
    String above = Double.toString(Math.nextUp(3.0));
    String below = Double.toString(Math.nextDown(3.0));
    assertRasterizedGrid(raster, "LINESTRING (-1 " + above + ", 1 3.0)", rowAbove);
    assertRasterizedGrid(raster, "LINESTRING (1 3.0, -1 " + above + ")", rowAbove);
    assertRasterizedGrid(raster, "LINESTRING (-1 " + below + ", 1 3.0)", rowBelow);
    assertRasterizedGrid(raster, "LINESTRING (1 3.0, -1 " + below + ")", rowBelow);
    // Exactly on the grid line, the half-open row after it.
    assertRasterizedGrid(raster, "LINESTRING (-1 3.0, 1 3.0)", rowBelow);
  }

  @Test
  public void testLineWithExtremeFiniteEndpoints() throws FactoryException {
    // GH-3255: the difference of these endpoints overflows in double precision. They are valid
    // coordinates, and the line still crosses the raster along one row or one column.
    GridCoverage2D raster = unitGrid6x6();
    GeometryFactory factory = new GeometryFactory();
    for (boolean allTouched : new boolean[] {false, true}) {
      double[] band = burn(line(factory, -1e308, 2.5, 1e308, 2.5), raster, allTouched);
      double[] reversed = burn(line(factory, 1e308, 2.5, -1e308, 2.5), raster, allTouched);
      double[] column = burn(line(factory, 2.5, -1e308, 2.5, 1e308), raster, allTouched);
      for (int i = 0; i < 6; i++) {
        Assert.assertEquals(1d, band[3 * 6 + i], 0d);
        Assert.assertEquals(1d, column[i * 6 + 2], 0d);
      }
      Assert.assertEquals(6, Arrays.stream(band).filter(v -> v != 0).count());
      Assert.assertEquals(6, Arrays.stream(column).filter(v -> v != 0).count());
      Assert.assertArrayEquals(band, reversed, 0d);
    }
  }

  @Test
  public void testLineOnDecimalGridLines() throws ParseException, FactoryException {
    // GH-3256: a coordinate is on a grid line when it is in decimal, as for points and extents.
    // (0.3 - 0.6) / -0.1 is 2.9999999999999996 in binary floating point, but 0.3 is the grid line
    // 3 of this raster: the line ends on the corner (0.3, 0.3) and does not reach the row above.
    GridCoverage2D tenths =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 0.6, 0.1, -0.1, 0, 0, 0);
    double[] oneCell = new double[36];
    oneCell[3 * 6 + 2] = 1;
    assertRasterizedGrid(tenths, "LINESTRING (0.22 0.22, 0.3 0.3)", oneCell);
    assertRasterizedGrid(tenths, "LINESTRING (0.3 0.3, 0.22 0.22)", oneCell);
    Assert.assertArrayEquals(
        oneCell, burn(wktReader.read("LINESTRING (0.22 0.22, 0.3 0.3)"), tenths, true), 0d);

    // 0.30000000000000004 is the next double above 0.3, and what 0.1 + 2 * 0.1 evaluates to. It is
    // a different stored value, past the line: a line ending there reaches the next column.
    double[] twoCells = oneCell.clone();
    twoCells[3 * 6 + 3] = 1;
    assertRasterizedGrid(tenths, "LINESTRING (0.22 0.25, 0.3 0.25)", oneCell);
    assertRasterizedGrid(tenths, "LINESTRING (0.22 0.25, 0.30000000000000004 0.25)", twoCells);

    // 0.030000000000000002 is the next double above 0.03, a real crossing into column 3. The
    // double-precision quotient 0.030000000000000002 / 0.01 rounds to exactly 3, which used to stop
    // the line at the grid line.
    GridCoverage2D hundredths =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 0.06, 0.01, -0.01, 0, 0, 0);
    double[] crossing = new double[36];
    crossing[1 * 6 + 2] = 1;
    crossing[1 * 6 + 3] = 1;
    assertRasterizedGrid(
        hundredths, "LINESTRING (0.025 0.045, 0.030000000000000002 0.045)", crossing);

    // The same rule on a bottom-up raster with a large origin: 1000000.03 is its grid line 3,
    // although (1000000.03 - 1000000.0) / 0.01 is 3.0000000027939677.
    GridCoverage2D offset =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 1000000.0, 50.0, 0.01, 0.01, 0, 0, 0);
    double[] beforeLine = new double[36];
    beforeLine[1 * 6 + 2] = 1;
    double[] afterLine = new double[36];
    afterLine[1 * 6 + 3] = 1;
    assertRasterizedGrid(offset, "LINESTRING (1000000.025 50.015, 1000000.03 50.015)", beforeLine);
    assertRasterizedGrid(offset, "LINESTRING (1000000.03 50.015, 1000000.035 50.015)", afterLine);
  }

  @Test
  public void testAllTouchedLineMatchesGdal() throws ParseException, FactoryException {
    // GH-3322: with allTouched, a line burns the cells GDAL burns with ALL_TOUCHED=TRUE. The first
    // three segments cross grid corners exactly, and at each corner GDAL also burns one of the two
    // cells that meet the segment only at that corner. The fourth crosses no corner, so it burns
    // just the cells it passes through. Expected {col, row} cells are from rasterio 1.4.4
    // (GDAL 3.10.3) rasterize(all_touched=True) on the same grid.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 20, 1, -1, 0, 0, 0);
    assertAllTouchedLine(
        raster,
        "LINESTRING (1.5 2.75, 10.5 7.25)",
        new int[][] {
          {1, 17}, {2, 16}, {2, 17}, {3, 16}, {4, 15}, {4, 16}, {5, 15}, {6, 14}, {6, 15}, {7, 14},
          {8, 13}, {8, 14}, {9, 13}, {10, 12}, {10, 13}
        });
    assertAllTouchedLine(
        raster,
        "LINESTRING (0.75 1.25, 4.25 11.75)",
        new int[][] {
          {0, 18}, {1, 15}, {1, 16}, {1, 17}, {1, 18}, {2, 12}, {2, 13}, {2, 14}, {2, 15}, {3, 9},
          {3, 10}, {3, 11}, {3, 12}, {4, 8}, {4, 9}
        });
    assertAllTouchedLine(
        raster,
        "LINESTRING (2.5 2.5, 9.5 9.5)",
        new int[][] {
          {2, 17}, {3, 16}, {3, 17}, {4, 15}, {4, 16}, {5, 14}, {5, 15}, {6, 13}, {6, 14}, {7, 12},
          {7, 13}, {8, 11}, {8, 12}, {9, 10}, {9, 11}
        });
    assertAllTouchedLine(
        raster,
        "LINESTRING (1.3 2.7, 8.6 11.4)",
        new int[][] {
          {1, 16}, {1, 17}, {2, 15}, {2, 16}, {3, 14}, {3, 15}, {4, 12}, {4, 13}, {4, 14}, {5, 11},
          {5, 12}, {6, 10}, {6, 11}, {7, 9}, {7, 10}, {8, 8}, {8, 9}
        });
    // The extra cell at a corner is the one after it in pixel order, so the mirrored diagonal,
    // which runs towards larger rows, burns none: each corner's cell is already on its way.
    assertAllTouchedLine(
        raster,
        "LINESTRING (2.5 9.5, 9.5 2.5)",
        new int[][] {{2, 10}, {3, 11}, {4, 12}, {5, 13}, {6, 14}, {7, 15}, {8, 16}, {9, 17}});
    // Pixel rows run the other way on a bottom-up raster, and so does the rule.
    GridCoverage2D bottomUp =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 0, 1, 1, 0, 0, 0);
    Assert.assertEquals(
        15, countBurned(burn(wktReader.read("LINESTRING (2.5 9.5, 9.5 2.5)"), bottomUp, true)));
    Assert.assertEquals(
        8, countBurned(burn(wktReader.read("LINESTRING (2.5 2.5, 9.5 9.5)"), bottomUp, true)));
  }

  @Test
  public void testAllTouchedLineEndsOnGridLines() throws ParseException, FactoryException {
    // GH-3322: with allTouched, the first point of a segment in pixel order burns the cell that
    // contains it, and the last point burns nothing. Expected {col, row} cells are those of GDAL
    // 3.10.3 and 3.12.1.
    GridCoverage2D raster = unitGrid6x6();
    // From the corner (3, 3) towards smaller rows: the corner's own cell {3, 3} as well.
    assertAllTouchedLine(
        raster, "LINESTRING (3 3, 4.5 4.5)", new int[][] {{4, 1}, {3, 2}, {4, 2}, {3, 3}});
    // Ending on the corner (3, 3): its cell is not burned, the cell after the corner (2, 4) is.
    assertAllTouchedLine(raster, "LINESTRING (1.5 1.5, 3 3)", new int[][] {{2, 3}, {1, 4}, {2, 4}});
    // Through the whole raster: the corners on the raster's own edge add nothing.
    assertAllTouchedLine(
        raster,
        "LINESTRING (-1 -1, 7 7)",
        new int[][] {
          {5, 0}, {4, 1}, {5, 1}, {3, 2}, {4, 2}, {2, 3}, {3, 3}, {1, 4}, {2, 4}, {0, 5}, {1, 5}
        });
    // Coming in through the left edge exactly on a row line.
    assertAllTouchedLine(
        raster, "LINESTRING (-1 2.5, 1.5 3.75)", new int[][] {{0, 2}, {1, 2}, {0, 3}});
    // A V with its apex on a corner: the cells above the apex stay unburned, as without
    // allTouched.
    assertRasterizedGrid(
        raster,
        "LINESTRING (2.5 2.5, 3.0 3.0, 3.5 2.5)",
        burn(wktReader.read("LINESTRING (2.5 2.5, 3.0 3.0, 3.5 2.5)"), raster, true));
  }

  @Test
  public void testAllTouchedLineOrdersSegmentEndsByPixelX()
      throws ParseException, FactoryException {
    // The first point of a segment is its end with the smaller pixel x, also when the segment
    // stays within one column of cells. This one starts on the row line y = 3 and runs towards
    // smaller rows, so the cell after the line burns too. GDAL burns these two cells from 3.11 on;
    // up to 3.10 it ordered the ends of such a segment by row and burned only {3, 2}.
    GridCoverage2D raster = unitGrid6x6();
    assertAllTouchedLine(raster, "LINESTRING (3.5 3, 3.9 3.9)", new int[][] {{3, 2}, {3, 3}});
    // Leaning the other way, or upright, the end on the row line is the last point.
    assertAllTouchedLine(raster, "LINESTRING (3.5 3, 3.1 3.9)", new int[][] {{3, 2}});
    assertAllTouchedLine(raster, "LINESTRING (3.5 3, 3.5 3.9)", new int[][] {{3, 2}});
    // Across two columns, where every GDAL version agrees.
    assertAllTouchedLine(
        raster, "LINESTRING (3.5 3, 4.5 3.9)", new int[][] {{3, 2}, {4, 2}, {3, 3}});

    // A segment that meets the raster at one point of its edge has no first point apart from its
    // last, and burns nothing, as in GDAL: here it starts on the raster's top edge and leaves.
    for (String wkt :
        new String[] {
          "LINESTRING (2.5 6, 4 7)", "LINESTRING (2.5 6, 2.75 7)", "LINESTRING (2.5 6, 2.5 7)"
        }) {
      Geometry line = wktReader.read(wkt);
      Assert.assertEquals(wkt, 0, countBurned(burn(line, raster, true)));
      Assert.assertNull(wkt, RasterConstructors.asRaster(line, raster, "d", true, 1d, 0d, true));
    }
  }

  @Test
  public void testCroppedLineOnBottomUpRasterWithInexactTopEdge() throws Exception {
    // A bottom-up raster stores its lower-left corner. Its top edge is 0.1 + 6 * 0.1, which is
    // 0.7000000000000001 in floating point, and from there y = 0.3 is not on a row line. On the
    // raster's own grid it is exactly row line 2, and the line burns the row after it. The window
    // the output is cropped to has to be found on that same grid, or it holds the row before the
    // line and the output comes back null.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 0.1, 1, 0.1, 0, 0, 0);
    Geometry line = wktReader.read("LINESTRING (1.2 0.3, 4.5 0.3)");
    double[] expected = new double[36];
    Arrays.fill(expected, 2 * 6 + 1, 2 * 6 + 5, 1);
    for (boolean allTouched : new boolean[] {false, true}) {
      Assert.assertArrayEquals(expected, burn(line, raster, allTouched), 0d);
      GridCoverage2D cropped =
          RasterConstructors.asRaster(line, raster, "d", allTouched, 1d, 0d, true);
      Assert.assertNotNull(cropped);
      Assert.assertEquals(4, RasterAccessors.getWidth(cropped));
      Assert.assertEquals(4, countBurned(MapAlgebra.bandAsArray(cropped, 1)));
      assertCroppedMatchesFullExtent(raster, line, allTouched);
      Assert.assertEquals(
          4d, RasterBandAccessors.getZonalStatsAll(raster, line, 1, allTouched, false)[0], 0d);
    }
  }

  @Test
  public void testOutputKeepsTheGridOfTheReferenceRaster() throws ParseException, FactoryException {
    // The output is a raster in its own right, and what is rasterized or counted on it is decided
    // on its grid. That has to be the grid of the reference raster: the same pixel size, and an
    // origin on one of its grid lines. An output placed by its envelope gets a pixel size of (max
    // - min) / cells, which is 0.10000000000000002 for six rows of 0.1 down from 0.7. From there y
    // = 0.3 is no longer on a row line, and the line that burned one row selects the row beside it.
    Geometry line = wktReader.read("LINESTRING (1.2 0.3, 4.5 0.3)");
    double[][] grids = {{0.1, 0.1}, {0.7, -0.1}, {0.6, -0.1}, {0, 0.1}};
    for (double[] grid : grids) {
      GridCoverage2D raster =
          RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, grid[0], 1, grid[1], 0, 0, 0);
      double[] reference = RasterAccessors.metadata(raster);
      for (boolean cropped : new boolean[] {false, true}) {
        String label = "origin " + grid[0] + ", pixel " + grid[1] + ", cropped=" + cropped;
        GridCoverage2D output =
            RasterConstructors.asRaster(line, raster, "d", false, 1d, 0d, cropped);
        double[] metadata = RasterAccessors.metadata(output);
        Assert.assertEquals(label, reference[4], metadata[4], 0d);
        Assert.assertEquals(label, reference[5], metadata[5], 0d);
        if (!cropped) {
          Assert.assertEquals(label, reference[0], metadata[0], 0d);
          Assert.assertEquals(label, reference[1], metadata[1], 0d);
        }
        double[] band = MapAlgebra.bandAsArray(output, 1);
        Assert.assertEquals(label, 4, countBurned(band));
        // The same line burns the same cells of the output, and counts them.
        Assert.assertArrayEquals(label, band, burn(line, output, false), 0d);
        Assert.assertEquals(
            label,
            Double.valueOf(4),
            RasterBandAccessors.getZonalStats(output, line, 1, "count", false, true));

        // The grid of the output does not depend on the kind of geometry.
        for (String wkt :
            new String[] {
              "POINT (2.5 0.35)",
              "MULTIPOINT ((1.5 0.25), (4.5 0.45))",
              "POLYGON ((1.2 0.22, 4.5 0.22, 4.5 0.48, 1.2 0.48, 1.2 0.22))"
            }) {
          double[] other =
              RasterAccessors.metadata(
                  RasterConstructors.asRaster(
                      wktReader.read(wkt), raster, "d", false, 1d, 0d, cropped));
          Assert.assertEquals(label + " " + wkt, reference[4], other[4], 0d);
          Assert.assertEquals(label + " " + wkt, reference[5], other[5], 0d);
          assertOnGridLine(label + " " + wkt, other[0], reference[0], reference[4]);
          assertOnGridLine(label + " " + wkt, other[1], reference[1], reference[5]);
          if (!cropped) {
            Assert.assertEquals(label + " " + wkt, reference[0], other[0], 0d);
            Assert.assertEquals(label + " " + wkt, reference[1], other[1], 0d);
          }
        }
      }
    }

    // The origin of a cropped output is the reference grid line it starts at, as a decimal: row
    // line 2 of this raster is 0.3, where 0.1 + 2 * 0.1 is 0.30000000000000004 in floating point
    // and the raster's top edge less four rows is 0.3000000000000001.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 0.1, 1, 0.1, 0, 0, 0);
    GridCoverage2D cropped =
        RasterConstructors.asRaster(
            wktReader.read("LINESTRING (1.5 0.35, 4.5 0.35)"), raster, "d", false, 1d, 0d, true);
    double[] metadata = RasterAccessors.metadata(cropped);
    Assert.assertArrayEquals(new double[] {1, 0.3, 4, 1, 1, 0.1}, Arrays.copyOf(metadata, 6), 0d);
    // A line along that row line is on the first row line of the cropped raster.
    Assert.assertEquals(
        4, countBurned(burn(wktReader.read("LINESTRING (1.2 0.3, 4.5 0.3)"), cropped, false)));
  }

  @Test
  public void testOutputKeepsItsCellsWhenRastersAreSerialized() throws Exception {
    // Spark serializes a raster that passes from one projection to the next, and the serializer
    // keeps the cell-centre transform. The corner a raster reports is derived from that in floating
    // point, as centre - scale / 2. An output has to report the same grid before and after, or the
    // cells burned on it are not the cells the geometry selects there.
    Geometry line = wktReader.read("LINESTRING (1.2 0.3, 4.5 0.3)");
    double[][] grids = {{0.1, 0.1}, {0.7, -0.1}, {0.6, -0.1}, {0, 0.1}};
    for (double[] grid : grids) {
      GridCoverage2D made =
          RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, grid[0], 1, grid[1], 0, 0, 0);
      GridCoverage2D stored = serialized(made);
      // Not every raster keeps its corner: the one made with origin 0.1 and pixel size 0.1 reports
      // 0.10000000000000002 once stored. Its row line 2 is then 0.30000000000000002, and y = 0.3 is
      // inside row 1. Row line 1 is 0.20000000000000002, which is not a double: a cropped output
      // starting at the nearest double, 0.2, would have y = 0.3 on its own row line 1 and lose the
      // row, so it starts at the first double after the line.
      for (boolean allTouched : new boolean[] {false, true}) {
        Assert.assertEquals(4, countBurned(burn(line, stored, allTouched)));
        Assert.assertEquals(1, assertOutputsKeepTheirCellsWhenSerialized(stored, line, allTouched));
        if (keepsItsGridWhenStored(made)) {
          // Burned on the raster as it was made, and serialized afterwards.
          Assert.assertEquals(1, assertOutputsKeepTheirCellsWhenSerialized(made, line, allTouched));
        }
      }
    }

    // A cropped output starts on a grid line of the reference raster, and no stored raster has the
    // corner 0.1 with a pixel size of 0.1: the output would report 0.10000000000000002 once stored,
    // and the line on row line 2 of this raster would be before row line 1 of the output. The
    // window of this line starts on row line 1, at 0.1, so the output starts a row earlier.
    GridCoverage2D fromZero =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 0, 1, 0.1, 0, 0, 0);
    Geometry onRowLine2 = wktReader.read("LINESTRING (1.2 0.2, 4.5 0.2)");
    for (boolean allTouched : new boolean[] {false, true}) {
      GridCoverage2D cropped =
          RasterConstructors.asRaster(onRowLine2, fromZero, "d", allTouched, 1d, 0d, true);
      Assert.assertArrayEquals(
          new double[] {1, 0, 4, 3, 1, 0.1},
          Arrays.copyOf(RasterAccessors.metadata(cropped), 6),
          0d);
      Assert.assertTrue(keepsItsGridWhenStored(cropped));
      // The line burns the last of the three rows.
      double[] expected = new double[12];
      Arrays.fill(expected, 8, 12, 1);
      Assert.assertArrayEquals(expected, MapAlgebra.bandAsArray(cropped, 1), 0d);
      Assert.assertEquals(
          1, assertOutputsKeepTheirCellsWhenSerialized(fromZero, onRowLine2, allTouched));
    }
    // A column line is treated the same: 0.1 is the first column line after the origin here.
    GridCoverage2D columns =
        RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 6, 0.1, -1, 0, 0, 0);
    Geometry onColumnLine2 = wktReader.read("LINESTRING (0.2 1.5, 0.2 4.5)");
    GridCoverage2D cropped =
        RasterConstructors.asRaster(onColumnLine2, columns, "d", false, 1d, 0d, true);
    Assert.assertArrayEquals(
        new double[] {0, 5, 3, 4, 0.1, -1},
        Arrays.copyOf(RasterAccessors.metadata(cropped), 6),
        0d);
    Assert.assertTrue(keepsItsGridWhenStored(cropped));
    Assert.assertEquals(
        1, assertOutputsKeepTheirCellsWhenSerialized(columns, onColumnLine2, false));
  }

  @Test
  public void testOutputOnAGridWhoseLinesAreNotDoubles() throws Exception {
    // One arc second is 0.0002777777777777778 as a double, and the grid lines of a raster with
    // that pixel size are not doubles: no coordinate lies on them, and a node of the grid computed
    // in floating point lies within a unit in the last place of one, on either side.
    //
    // A full-extent output has the reference raster's grid, so such a node is on the same side of
    // every grid line there, and a line along the nodes selects on the output the cells it burned.
    // A cropped output starts at the double next to a grid line. Its grid lines are less than a
    // unit in the last place away from the reference raster's, which is enough to put a node on the
    // other side of one, and no other origin avoids that. So for a cropped output the cells are
    // compared for lines that keep away from the grid lines.
    double arcSecond = 1.0 / 3600;
    GeometryFactory factory = new GeometryFactory();
    Random random = new Random(3600);
    int compared = 0;
    for (double[] corner : new double[][] {{-122, 47}, {8, -33.9}, {116.5, 0.25}}) {
      for (boolean bottomUp : new boolean[] {false, true}) {
        double originX = corner[0] + random.nextInt(3600) * arcSecond;
        double originY = corner[1] + random.nextInt(3600) * arcSecond;
        double scaleY = bottomUp ? arcSecond : -arcSecond;
        GridCoverage2D raster =
            RasterConstructors.makeEmptyRaster(
                1, "d", 8, 8, originX, originY, arcSecond, scaleY, 0, 0, 4326);
        for (int i = 0; i < 60; i++) {
          int col0 = random.nextInt(8);
          int col1 = (col0 + 1 + random.nextInt(7)) % 8;
          int row0 = random.nextInt(8);
          int row1 = (row0 + 1 + random.nextInt(7)) % 8;
          for (double within : new double[] {0, 0.5}) {
            // An L along grid lines, or along the middles of a column and a row.
            LineString line =
                factory.createLineString(
                    new Coordinate[] {
                      new Coordinate(
                          originX + (col0 + within) * arcSecond,
                          originY + (row0 + within) * scaleY),
                      new Coordinate(
                          originX + (col0 + within) * arcSecond,
                          originY + (row1 + within) * scaleY),
                      new Coordinate(
                          originX + (col1 + within) * arcSecond, originY + (row1 + within) * scaleY)
                    });
            line.setSRID(4326);
            for (boolean allTouched : new boolean[] {false, true}) {
              for (boolean cropped : new boolean[] {false, true}) {
                if (cropped && within == 0) {
                  continue;
                }
                GridCoverage2D output;
                try {
                  output =
                      RasterConstructors.asRaster(line, raster, "d", allTouched, 1d, 0d, cropped);
                } catch (IllegalArgumentException e) {
                  // A line along the raster's edge may not intersect it at all.
                  continue;
                }
                if (output == null) {
                  continue;
                }
                double[] band = MapAlgebra.bandAsArray(output, 1);
                if (countBurned(band) == 0) {
                  continue;
                }
                String label = line.toText() + " allTouched=" + allTouched + " cropped=" + cropped;
                Assert.assertArrayEquals(label, band, burn(line, output, allTouched), 0d);
                Assert.assertEquals(
                    label,
                    Double.valueOf(countBurned(band)),
                    RasterBandAccessors.getZonalStats(output, line, 1, "count", allTouched, true));
                compared++;
              }
            }
          }
        }
      }
    }
    Assert.assertTrue("outputs compared: " + compared, compared > 1500);
  }

  @Test
  public void testReferenceRasterWhoseGridRangeDoesNotStartAtZero() throws Exception {
    // A GeoTools crop keeps the grid-to-world transform of its source and starts its grid range at
    // the first cell it keeps: here the cells 2..7 of a 10x10 unit raster, the square from (2, 2)
    // to (8, 8). The origin the metadata report is still the corner of grid cell (0, 0), which is
    // (0, 10). Cells have to be located from the corner of the raster's first cell, (2, 8), and the
    // output has to start there: located from (0, 10), the output covered the square from (0, 4)
    // to (6, 10).
    GridCoverage2D whole =
        RasterConstructors.makeEmptyRaster(1, "d", 10, 10, 0, 10, 1, -1, 0, 0, 0);
    Crop crop = new Crop();
    ParameterValueGroup parameters = crop.getParameters();
    parameters.parameter("Source").setValue(whole);
    parameters
        .parameter(Crop.PARAMNAME_ROI)
        .setValue(
            JTS.toGeometry(
                new ReferencedEnvelope(2, 8, 2, 8, whole.getCoordinateReferenceSystem2D())));
    GridCoverage2D raster = (GridCoverage2D) crop.doOperation(parameters, null);
    Assert.assertEquals(2, raster.getGridGeometry().getGridRange2D().x);
    Assert.assertEquals(2, raster.getGridGeometry().getGridRange2D().y);

    String[][] cases = {
      // The cell holding the point, the five cells of the line's row, the nine of the polygon.
      {"POINT (3.5 6.5)", "(1,1)"},
      {"LINESTRING (2.5 7.5, 6.5 7.5)", "(0,0) (1,0) (2,0) (3,0) (4,0)"},
      {
        "POLYGON ((3 3, 6 3, 6 6, 3 6, 3 3))",
        "(1,2) (2,2) (3,2) (1,3) (2,3) (3,3) (1,4) (2,4) (3,4)"
      }
    };
    for (String[] expected : cases) {
      Geometry geom = wktReader.read(expected[0]);
      GridCoverage2D fullExtent =
          RasterConstructors.asRaster(geom, raster, "d", false, 1d, 0d, false);
      // The output is the raster's square, from its first cell.
      Assert.assertArrayEquals(
          expected[0],
          new double[] {2, 8, 6, 6, 1, -1},
          Arrays.copyOf(RasterAccessors.metadata(fullExtent), 6),
          0d);
      Assert.assertEquals(raster.getEnvelope2D(), fullExtent.getEnvelope2D());
      assertBurnedCells(raster, geom, false, expected[1]);
      // The cropped output holds the same cells, placed from the same corner.
      GridCoverage2D cropped = RasterConstructors.asRaster(geom, raster, "d", false, 1d, 0d, true);
      double[] full = MapAlgebra.bandAsArray(fullExtent, 1);
      double[] window = RasterAccessors.metadata(cropped);
      double[] band = MapAlgebra.bandAsArray(cropped, 1);
      double[] placed = new double[full.length];
      int colOffset = (int) Math.round(window[0] - 2);
      int rowOffset = (int) Math.round(8 - window[1]);
      for (int row = 0; row < (int) window[3]; row++) {
        for (int col = 0; col < (int) window[2]; col++) {
          placed[(row + rowOffset) * 6 + col + colOffset] = band[row * (int) window[2] + col];
        }
      }
      Assert.assertArrayEquals(expected[0], full, placed, 0d);
    }
  }

  /** Whether a raster reports the same grid after it has been serialized. */
  private static boolean keepsItsGridWhenStored(GridCoverage2D raster)
      throws FactoryException, IOException, ClassNotFoundException {
    return Arrays.equals(
        Arrays.copyOf(RasterAccessors.metadata(raster), 8),
        Arrays.copyOf(RasterAccessors.metadata(serialized(raster)), 8));
  }

  private static GridCoverage2D serialized(GridCoverage2D raster)
      throws IOException, ClassNotFoundException {
    return Serde.deserialize(Serde.serialize(raster));
  }

  /**
   * Asserts that the geometry burns the same cells again on its full-extent and cropped outputs,
   * and counts them, when the outputs have been through the serializer. Returns 1 when something is
   * burned and 0 otherwise.
   */
  private static int assertOutputsKeepTheirCellsWhenSerialized(
      GridCoverage2D raster, Geometry geom, boolean allTouched)
      throws FactoryException, IOException, ClassNotFoundException {
    String label = geom.toText() + " allTouched=" + allTouched + " on " + raster.getEnvelope2D();
    int burnedAny = 0;
    for (boolean cropped : new boolean[] {false, true}) {
      GridCoverage2D output;
      try {
        output = RasterConstructors.asRaster(geom, raster, "d", allTouched, 1d, 0d, cropped);
      } catch (IllegalArgumentException e) {
        // The geometry does not intersect the raster.
        return 0;
      }
      if (output == null) {
        continue;
      }
      output = serialized(output);
      double[] band = MapAlgebra.bandAsArray(output, 1);
      if (countBurned(band) == 0) {
        continue;
      }
      burnedAny = 1;
      Assert.assertArrayEquals(
          label + " cropped=" + cropped, band, burn(geom, output, allTouched), 0d);
      Assert.assertEquals(
          label + " cropped=" + cropped,
          Double.valueOf(countBurned(band)),
          RasterBandAccessors.getZonalStats(output, geom, 1, "count", allTouched, true));
    }
    return burnedAny;
  }

  @Test
  public void testMemberBeyondTheRasterIsDiscardedOnItsBoundingBox() throws Exception {
    // Whether the window of a line also holds the cells after its last grid line takes a pass over
    // its coordinates. A member that cannot reach the raster even with those cells is discarded on
    // its bounding box alone: most members of a network rasterized onto one tile are such members.
    GridCoverage2D raster = unitGrid6x6();
    double[] metadata = RasterAccessors.metadata(raster);
    GeometryFactory factory = new GeometryFactory();
    CountedLine[] apart = {
      // West of the raster, its bounding box ending on column and row lines.
      new CountedLine(factory, -2, 1, -5, 3, -9, 2),
      // A segment along the column line before the raster's left edge burns the column before it.
      new CountedLine(factory, -4, 2, -1, 2, -1, 4),
      // North of the raster, along the row line before its top edge, and south of it.
      new CountedLine(factory, 1, 7, 3, 7, 3, 10),
      new CountedLine(factory, 1, -1, 3, -1, 3, -4),
    };
    // From the raster's right edge outwards: no cell follows that edge.
    CountedLine fromRightEdge = new CountedLine(factory, 6, 2, 6, 4, 9, 4);
    List<Geometry> members = new ArrayList<>(Arrays.asList(apart));
    members.add(fromRightEdge);
    members.add(line(factory, 1.5, 4.5, 3.5, 4.5));
    // This one reaches the raster only along its left edge, the first column line, and burns the
    // column after it.
    members.add(new CountedLine(factory, -3, 2, 0, 2, 0, 4));
    Geometry collection = factory.createGeometryCollection(members.toArray(new Geometry[0]));
    collection.getEnvelopeInternal();
    for (boolean allTouched : new boolean[] {false, true}) {
      assertBurnedCells(raster, collection, allTouched, "(1,1) (2,1) (3,1) (0,2) (0,3)");
      Assert.assertEquals(1, assertCroppedMatchesFullExtent(raster, collection, allTouched));

      // The assertions above print the members, so count from here. The members apart from the
      // raster have no window on it, and finding the window of any of the five fetches no
      // coordinates.
      for (int i = 0; i <= apart.length; i++) {
        CountedLine member = i < apart.length ? apart[i] : fromRightEdge;
        member.reads = 0;
        ReferencedEnvelope window =
            Rasterization.rasterizeGeomExtent(member, raster, metadata, allTouched);
        if (member != fromRightEdge) {
          Assert.assertNull(window);
        }
        Assert.assertEquals("member " + i + ", allTouched=" + allTouched, 0, member.reads);
      }
      // Nor does rasterizing the collection, for the members apart from the raster. One that
      // touches its edge is read by the test of whether the geometry intersects the raster.
      for (boolean cropped : new boolean[] {false, true}) {
        Rasterization.rasterize(collection, raster, "d", 1, cropped, allTouched);
        for (int i = 0; i < apart.length; i++) {
          Assert.assertEquals("member " + i + ", cropped=" + cropped, 0, apart[i].reads);
        }
      }
    }
  }

  /** A LineString that counts how often its coordinates are fetched. */
  private static final class CountedLine extends LineString {
    int reads;

    CountedLine(GeometryFactory factory, double... xy) {
      super(factory.getCoordinateSequenceFactory().create(coordinates(xy)), factory);
    }

    private static Coordinate[] coordinates(double[] xy) {
      Coordinate[] coordinates = new Coordinate[xy.length / 2];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(xy[2 * i], xy[2 * i + 1]);
      }
      return coordinates;
    }

    @Override
    public Coordinate[] getCoordinates() {
      reads++;
      return super.getCoordinates();
    }

    @Override
    public CoordinateSequence getCoordinateSequence() {
      reads++;
      return super.getCoordinateSequence();
    }
  }

  @Test
  public void testCroppedLineBurnsTheCellsOfTheFullExtent() throws Exception {
    // Which cells a line burns is decided on the reference raster's grid, and the window of the
    // cropped output has to hold all of them. Pixel sizes and origins here include ones whose
    // products are inexact in floating point, north-up and bottom-up, and the vertices lie on grid
    // lines, on cell centres and in between, inside and outside the raster. Each line is also
    // burned on a serialized copy of the raster, whose outputs are serialized in turn.
    double[][] grids = {
      {0, 6, 1, -1},
      {0, 0, 1, 1},
      {0, 0.1, 1, 0.1},
      {0.3, 0.7, 0.1, -0.1},
      {-0.9, 0.3, 0.3, 0.1},
      {100, 0.05, 30, 0.05},
      {-179.9, -89.9, 0.1, 0.7},
    };
    GeometryFactory factory = new GeometryFactory();
    Random random = new Random(3579);
    int nonEmpty = 0;
    int nonEmptyStored = 0;
    for (double[] grid : grids) {
      GridCoverage2D raster =
          RasterConstructors.makeEmptyRaster(
              1, "d", 6, 6, grid[0], grid[1], grid[2], grid[3], 0, 0, 0);
      GridCoverage2D stored = serialized(raster);
      boolean keepsItsGrid = keepsItsGridWhenStored(raster);
      for (int i = 0; i < 300; i++) {
        LineString line = randomLine(factory, random, grid);
        nonEmpty += assertCroppedMatchesFullExtent(raster, line, false);
        assertCroppedMatchesFullExtent(raster, line, true);
        Geometry multi = factory.createMultiLineString(new LineString[] {line});
        assertCroppedMatchesFullExtent(raster, multi, false);
        assertCroppedMatchesFullExtent(raster, multi, true);
        nonEmptyStored += assertOutputsKeepTheirCellsWhenSerialized(stored, line, false);
        assertOutputsKeepTheirCellsWhenSerialized(stored, line, true);
        if (keepsItsGrid) {
          assertOutputsKeepTheirCellsWhenSerialized(raster, line, false);
          assertOutputsKeepTheirCellsWhenSerialized(raster, line, true);
        }
        assertOutputsKeepTheirCellsWhenSerialized(stored, multi, false);
        assertOutputsKeepTheirCellsWhenSerialized(stored, multi, true);
      }
    }
    Assert.assertTrue("lines that burn something: " + nonEmpty, nonEmpty > 1000);
    Assert.assertTrue(
        "lines that burn something on a serialized raster: " + nonEmptyStored,
        nonEmptyStored > 1000);
  }

  @Test
  public void testSetValuesAndClipSelectTheCellsAsRasterBurns() throws Exception {
    // RS_SetValues and RS_Clip rasterize the geometry onto a mask cropped to it, and place the
    // mask on the raster. The mask starts on a grid line of the raster. On these grids the cell
    // index of a coordinate on a grid line is inexact in floating point, and the placement has to
    // find the cell after the line, not the one before it. The rasters have an SRID, so that
    // RS_SetValues does not assign them one first: that rebuilds the grid from its cell-centre
    // transform, which can move the origin by a rounding error.
    double[][] grids = {
      {0, 0.7, 1, -0.1},
      {0, 0.1, 1, 0.1},
      {0.3, 0.7, 0.1, -0.1},
      {-0.9, 0.3, 0.3, 0.1},
      {100, 0.05, 30, 0.05},
      {-179.9, -89.9, 0.1, 0.7},
    };
    GeometryFactory factory = new GeometryFactory();
    Random random = new Random(3579);
    double[] sevens = new double[36];
    Arrays.fill(sevens, 7);
    int compared = 0;
    for (double[] grid : grids) {
      GridCoverage2D raster =
          RasterConstructors.makeEmptyRaster(
              1, "d", 6, 6, grid[0], grid[1], grid[2], grid[3], 0, 0, 4326);
      GridCoverage2D filled = MapAlgebra.addBandFromArray(raster, sevens, 1);
      for (int i = 0; i < 150; i++) {
        LineString line = randomLine(factory, random, grid);
        line.setSRID(4326);
        for (boolean allTouched : new boolean[] {false, true}) {
          double[] burned;
          try {
            burned = burn(line, raster, allTouched);
          } catch (IllegalArgumentException e) {
            // The line does not intersect the raster.
            continue;
          }
          if (countBurned(burned) == 0) {
            continue;
          }
          String label =
              line.toText() + " allTouched=" + allTouched + " on " + Arrays.toString(grid);
          assertSameCells(
              label + ", RS_SetValues",
              burned,
              MapAlgebra.bandAsArray(
                  PixelFunctionEditors.setValues(raster, 1, line, 5, allTouched, false), 1));
          assertSameCells(
              label + ", RS_Clip",
              burned,
              MapAlgebra.bandAsArray(
                  RasterBandEditors.clip(filled, 1, line, allTouched, 0, false, true), 1));
          compared++;
        }
      }
    }
    Assert.assertTrue("lines compared: " + compared, compared > 500);
  }

  /** Asserts that two bands are non-zero in the same cells. */
  private static void assertSameCells(String label, double[] expected, double[] actual) {
    Assert.assertEquals(label, expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(label + ", cell " + i, expected[i] != 0, actual[i] != 0);
    }
  }

  /**
   * A line of two to four vertices on grid lines, on cell centres and in between, from two cells
   * before a 6x6 grid {originX, originY, scaleX, scaleY} to two cells after it.
   */
  private static LineString randomLine(GeometryFactory factory, Random random, double[] grid) {
    Coordinate[] coords = new Coordinate[2 + random.nextInt(3)];
    for (int v = 0; v < coords.length; v++) {
      double x = worldCoordinate(grid[0], grid[2], randomPixelCoordinate(random));
      double y = worldCoordinate(grid[1], grid[3], randomPixelCoordinate(random));
      // One vertex in three continues along a column or a row, so that segments lying on grid
      // lines, which burn the cells after them, are common.
      if (v > 0 && random.nextInt(3) == 0) {
        if (random.nextBoolean()) {
          x = coords[v - 1].x;
        } else {
          y = coords[v - 1].y;
        }
      }
      coords[v] = new Coordinate(x, y);
    }
    return factory.createLineString(coords);
  }

  @Test
  public void testCroppedLineKeepsTheCellsAfterItsLastGridLine() throws Exception {
    GridCoverage2D raster = unitGrid6x6();
    // The second segment lies on the row line y = 3, the last grid line of the line's bounding box
    // in pixel order, and burns the row after it. Those four cells are outside the bounding box,
    // and the cropped output has to hold them, with allTouched or without.
    Geometry bent = wktReader.read("LINESTRING (1.5 4.5, 1.5 3, 4.5 3)");
    // A line that touches the raster only along its top edge, which belongs to the first row.
    Geometry alongTop = wktReader.read("LINESTRING (2 6, 4 6, 4 8)");
    // Along the last column line of its bounding box, with a vertex repeated on the corner.
    Geometry upright = wktReader.read("LINESTRING (1.5 4.5, 3 4.5, 3 2.5, 3 2.5)");
    for (boolean allTouched : new boolean[] {false, true}) {
      assertBurnedCells(raster, bent, allTouched, "(1,1) (1,2) (1,3) (2,3) (3,3) (4,3)");
      assertBurnedCells(raster, alongTop, allTouched, "(2,0) (3,0)");
      assertBurnedCells(raster, upright, allTouched, "(1,1) (2,1) (3,1) (3,2) (3,3)");
      for (Geometry line : new Geometry[] {bent, alongTop, upright}) {
        Assert.assertEquals(1, assertCroppedMatchesFullExtent(raster, line, allTouched));
        Assert.assertEquals(1, assertCroppedMatchesFullExtent(raster, line.reverse(), allTouched));
      }
    }
    // A line that only ends on the last grid lines of its bounding box burns nothing beyond them,
    // and its cropped output is not widened.
    GridCoverage2D cropped =
        RasterConstructors.asRaster(
            wktReader.read("LINESTRING (1 5, 3 3)"), raster, "d", false, 1d, 0d, true);
    Assert.assertEquals(2, RasterAccessors.getWidth(cropped));
    Assert.assertEquals(2, RasterAccessors.getHeight(cropped));
  }

  /** Asserts the {col, row} cells a geometry burns at full extent, e.g. "(1,1) (2,1)". */
  private static void assertBurnedCells(
      GridCoverage2D raster, Geometry geom, boolean allTouched, String expected)
      throws FactoryException {
    int width = RasterAccessors.getWidth(raster);
    double[] band = burn(geom, raster, allTouched);
    StringBuilder cells = new StringBuilder();
    for (int i = 0; i < band.length; i++) {
      if (band[i] != 0) {
        cells.append(cells.length() == 0 ? "" : " ");
        cells.append('(').append(i % width).append(',').append(i / width).append(')');
      }
    }
    Assert.assertEquals(geom.toText() + " allTouched=" + allTouched, expected, cells.toString());
  }

  /** A pixel coordinate from two cells before a 6-cell axis to two after it. */
  private static double randomPixelCoordinate(Random random) {
    switch (random.nextInt(4)) {
      case 0:
        return random.nextInt(11) - 2;
      case 1:
        return random.nextInt(10) - 2 + 0.5;
      default:
        return Math.round((random.nextDouble() * 10 - 2) * 100) / 100.0;
    }
  }

  /** The double nearest to origin + pixel * scale, computed in decimal. */
  private static double worldCoordinate(double origin, double scale, double pixel) {
    return BigDecimal.valueOf(origin)
        .add(BigDecimal.valueOf(pixel).multiply(BigDecimal.valueOf(scale)))
        .doubleValue();
  }

  /** Asserts that a coordinate is origin + k * scale for an integer k, as decimals. */
  private static void assertOnGridLine(String label, double value, double origin, double scale) {
    BigDecimal offset = BigDecimal.valueOf(value).subtract(BigDecimal.valueOf(origin));
    Assert.assertEquals(
        label + ": " + value + " is not a grid line from " + origin + " by " + scale,
        0,
        offset.remainder(BigDecimal.valueOf(scale)).signum());
  }

  /**
   * Asserts that the output cropped to the geometry's extent burns the cells the full-extent output
   * burns, at the same place on the reference raster, and no others; or that it is null when the
   * full-extent output burns nothing. Both outputs have to be on the reference raster's grid, so
   * that the geometry burns the same cells of them again. Returns 1 when something is burned and 0
   * otherwise.
   */
  private static int assertCroppedMatchesFullExtent(
      GridCoverage2D raster, Geometry geom, boolean allTouched)
      throws FactoryException, IOException, ClassNotFoundException {
    String label = geom.toText() + " allTouched=" + allTouched + " on " + raster.getEnvelope2D();
    GridCoverage2D fullExtent;
    try {
      fullExtent = RasterConstructors.asRaster(geom, raster, "d", allTouched, 1d, 0d, false);
    } catch (IllegalArgumentException e) {
      // The geometry does not intersect the raster.
      return 0;
    }
    double[] full = MapAlgebra.bandAsArray(fullExtent, 1);
    double[] reference = RasterAccessors.metadata(raster);
    // The full-extent output has the reference raster's grid, bit for bit.
    Assert.assertArrayEquals(
        label,
        Arrays.copyOf(reference, 8),
        Arrays.copyOf(RasterAccessors.metadata(fullExtent), 8),
        0d);
    GridCoverage2D cropped =
        RasterConstructors.asRaster(geom, raster, "d", allTouched, 1d, 0d, true);
    if (cropped == null) {
      Assert.assertEquals(label, 0, countBurned(full));
      return 0;
    }
    double[] window = RasterAccessors.metadata(cropped);
    // The cropped output has the reference raster's pixel size, and its origin is on one of the
    // reference raster's grid lines.
    Assert.assertEquals(label, reference[4], window[4], 0d);
    Assert.assertEquals(label, reference[5], window[5], 0d);
    assertOnGridLine(label, window[0], reference[0], reference[4]);
    assertOnGridLine(label, window[1], reference[1], reference[5]);
    // And it reports that grid when it has been stored, as long as the reference raster does.
    if (keepsItsGridWhenStored(raster)) {
      Assert.assertTrue(label, keepsItsGridWhenStored(cropped));
    }
    if (countBurned(full) > 0) {
      // So the geometry burns the same cells of either output, and counts them.
      Double burned = (double) countBurned(full);
      for (GridCoverage2D output : new GridCoverage2D[] {fullExtent, cropped}) {
        Assert.assertArrayEquals(
            label, MapAlgebra.bandAsArray(output, 1), burn(geom, output, allTouched), 0d);
        Assert.assertEquals(
            label,
            burned,
            RasterBandAccessors.getZonalStats(output, geom, 1, "count", allTouched, true));
      }
    }
    int width = (int) reference[2];
    int windowWidth = (int) window[2];
    int windowHeight = (int) window[3];
    // Both rasters store the origin of their own first row and column.
    int colOffset = (int) Math.round((window[0] - reference[0]) / reference[4]);
    int rowOffset = (int) Math.round((window[1] - reference[1]) / reference[5]);
    double[] band = MapAlgebra.bandAsArray(cropped, 1);
    double[] placed = new double[full.length];
    for (int row = 0; row < windowHeight; row++) {
      for (int col = 0; col < windowWidth; col++) {
        placed[(row + rowOffset) * width + col + colOffset] = band[row * windowWidth + col];
      }
    }
    Assert.assertArrayEquals(label, full, placed, 0d);
    return countBurned(full) > 0 ? 1 : 0;
  }

  @Test
  public void testAllTouchedPolygonEdgeThroughGridCornerFromOutsideTheRaster()
      throws ParseException, FactoryException {
    // The edge from (-1, 0) to (5, 4) passes exactly through the grid corner (2, 2), and the
    // polygon touches cell {2, 4} only there. The edge starts outside the raster. A copy of the
    // polygon clipped to the raster starts that edge at (0, 0.6666666666666666) instead, a rounded
    // point, from where it passes beside the corner and through that cell. The outline is walked
    // on the polygon's own edges, so the cell stays unburned, as in GDAL.
    GridCoverage2D raster = unitGrid6x6();
    int[][] cells = {
      {0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 1}, {0, 2}, {1, 2}, {2, 2}, {3, 2}, {4, 2}, {0, 3},
      {1, 3}, {2, 3}, {3, 3}, {0, 4}, {1, 4}, {0, 5}
    };
    Geometry polygon = wktReader.read("POLYGON ((-1 0, 5 4, 1 5, -1 0))");
    assertAllTouchedCells(raster, polygon, cells);
    // The cropped output holds the same 17 cells.
    GridCoverage2D cropped = RasterConstructors.asRaster(polygon, raster, "d", true, 1d, 0d, true);
    Assert.assertEquals(17, countBurned(MapAlgebra.bandAsArray(cropped, 1)));

    // The same on a bottom-up raster with oblong pixels, and for an edge that leaves through the
    // raster's bottom edge. Expected cells are GDAL's (3.10.3 and 3.12.1).
    GridCoverage2D bottomUp =
        RasterConstructors.makeEmptyRaster(1, "d", 4, 4, -8, 16, 1, 2, 0, 0, 0);
    assertAllTouchedCells(
        bottomUp,
        wktReader.read("POLYGON ((-10 9, 0 27, -7.5 32, -10 9))"),
        new int[][] {
          {0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {3, 1}, {0, 2}, {1, 2}, {2, 2}, {3, 2},
          {0, 3}, {1, 3}, {2, 3}, {3, 3}
        });
    GridCoverage2D northUp = RasterConstructors.makeEmptyRaster(1, "d", 8, 9, 0, 0, 1, -1, 0, 0, 0);
    assertAllTouchedCells(
        northUp,
        wktReader.read("POLYGON ((8.5 -5, 1 4, 11 -0.5, 8.5 -5))"),
        new int[][] {
          {4, 0}, {5, 0}, {6, 0}, {7, 0}, {5, 1}, {6, 1}, {7, 1}, {6, 2}, {7, 2}, {6, 3}, {7, 3},
          {7, 4}
        });
  }

  /**
   * Asserts that a geometry, with its coordinates in either order, burns exactly the given {col,
   * row} cells with allTouched.
   */
  private static void assertAllTouchedCells(GridCoverage2D raster, Geometry geom, int[][] cells)
      throws FactoryException {
    int width = RasterAccessors.getWidth(raster);
    double[] expected = new double[width * RasterAccessors.getHeight(raster)];
    for (int[] cell : cells) {
      expected[cell[1] * width + cell[0]] = 1;
    }
    for (Geometry g : new Geometry[] {geom, geom.reverse()}) {
      Assert.assertArrayEquals(g.toText(), expected, burn(g, raster, true), 0d);
    }
  }

  @Test
  public void testZonalStatsCountForLineWithAllTouched() throws ParseException, FactoryException {
    // GH-3322: RS_ZonalStats ignored allTouched for lines. With it, these segments count the 15
    // cells GDAL/rasterio report; without it, their counts are unchanged.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 20, 1, -1, 0, 0, 0);
    String[] lines = {
      "LINESTRING (1.5 2.75, 10.5 7.25)",
      "LINESTRING (0.75 1.25, 4.25 11.75)",
      "LINESTRING (2.5 2.5, 9.5 9.5)"
    };
    double[] defaultCounts = {10, 11, 8};
    for (int i = 0; i < lines.length; i++) {
      Geometry line = Constructors.geomFromWKT(lines[i], 0);
      Assert.assertEquals(
          lines[i], 15d, RasterBandAccessors.getZonalStatsAll(raster, line, 1, true, false)[0], 0d);
      Assert.assertEquals(
          lines[i],
          defaultCounts[i],
          RasterBandAccessors.getZonalStatsAll(raster, line, 1, false, false)[0],
          0d);
    }
  }

  @Test
  public void testAllTouchedLineCroppedOutputBurnsSameCells()
      throws ParseException, FactoryException {
    // The allTouched line walk runs on the reference raster's grid; cropping the output to the
    // geometry's extent must not lose or shift any of its cells.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 20, 20, 0, 20, 1, -1, 0, 0, 0);
    Geometry line = Constructors.geomFromWKT("LINESTRING (1.5 2.75, 10.5 7.25)", 0);
    double[] cropped =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(line, raster, "d", true, 1d, 0d, true), 1);
    // The output covers columns 1 to 10 and rows 12 to 17 of the reference grid.
    Assert.assertEquals(10 * 6, cropped.length);
    Assert.assertEquals(15, countBurned(cropped));
    // Cell {2, 17} of the reference grid, which GDAL burns at the corner (2, 3).
    Assert.assertEquals(1d, cropped[(17 - 12) * 10 + (2 - 1)], 0d);
  }

  private static long countBurned(double[] band) {
    return Arrays.stream(band).filter(v -> v != 0).count();
  }

  /**
   * Asserts that a line, in both directions, burns exactly the given {col, row} cells with
   * allTouched.
   */
  private static void assertAllTouchedLine(GridCoverage2D raster, String wkt, int[][] cells)
      throws ParseException, FactoryException {
    int width = RasterAccessors.getWidth(raster);
    double[] expected = new double[width * RasterAccessors.getHeight(raster)];
    for (int[] cell : cells) {
      expected[cell[1] * width + cell[0]] = 1;
    }
    Geometry line = Constructors.geomFromWKT(wkt, 0);
    for (Geometry g : new Geometry[] {line, line.reverse()}) {
      Assert.assertArrayEquals(g.toText(), expected, burn(g, raster, true), 0d);
    }
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

  // GDAL's half-open boundary rule (GH-3425). A cell contains its first edge in pixel order but not
  // its last, so a geometry never burns a cell it touches only on its boundary. The expected cells
  // are what GDAL 3.12 (rasterio.features.rasterize) burns on the same grids, identically with
  // all_touched on and off.

  /** An 8x8 grid of unit pixels over [0, 8] x [0, 8], north-up or bottom-up. */
  private static GridCoverage2D unitGrid8x8(boolean bottomUp) throws FactoryException {
    return bottomUp
        ? RasterConstructors.makeEmptyRaster(1, "D", 8, 8, 0, 0, 1, 1, 0, 0, 0)
        : RasterConstructors.makeEmptyRaster(1, "D", 8, 8, 0, 8, 1, -1, 0, 0, 0);
  }

  /** Asserts the burned pixels at full extent, with allTouched both off and on. */
  private void assertBurnedLikeGdal(GridCoverage2D raster, String wkt, String expected)
      throws ParseException, FactoryException {
    Geometry geom = wktReader.read(wkt);
    for (boolean allTouched : new boolean[] {false, true}) {
      assertBurnedPixels(
          RasterConstructors.asRasterWithRasterExtent(geom, raster, "D", allTouched, 150, null),
          expected);
    }
  }

  @Test
  public void testPointBurnsTheHalfOpenCellContainingIt() throws ParseException, FactoryException {
    GridCoverage2D northUp = unitGrid8x8(false);
    // On a row line, a column line or a grid corner, only the cell after the line(s) in pixel
    // order.
    assertBurnedLikeGdal(northUp, "POINT (3.5 5)", "[(3,3)]");
    assertBurnedLikeGdal(northUp, "POINT (3 4.5)", "[(3,3)]");
    assertBurnedLikeGdal(northUp, "POINT (3 5)", "[(3,3)]");
    // The raster's left and top edges belong to its cells; its right and bottom edges do not.
    assertBurnedLikeGdal(northUp, "POINT (0 4.5)", "[(0,3)]");
    assertBurnedLikeGdal(northUp, "POINT (3.5 8)", "[(3,0)]");
    assertBurnedLikeGdal(northUp, "POINT (8 4.5)", "[]");
    assertBurnedLikeGdal(northUp, "POINT (3.5 0)", "[]");
    assertBurnedLikeGdal(northUp, "POINT (8 0)", "[]");

    // Bottom-up, pixel order runs up the raster: a point on a horizontal grid line falls in the
    // cell above it, and the top edge is the far edge.
    GridCoverage2D bottomUp = unitGrid8x8(true);
    assertBurnedLikeGdal(bottomUp, "POINT (3.5 3)", "[(3,3)]");
    assertBurnedLikeGdal(bottomUp, "POINT (3 3)", "[(3,3)]");
    assertBurnedLikeGdal(bottomUp, "POINT (3.5 0)", "[(3,0)]");
    assertBurnedLikeGdal(bottomUp, "POINT (3.5 8)", "[]");
    assertBurnedLikeGdal(bottomUp, "POINT (8 8)", "[]");

    // Cropped to the geometry, the output is that single cell, or nothing on the far edge.
    GridCoverage2D cropped =
        RasterConstructors.asRaster(
            wktReader.read("POINT (3 5)"), northUp, "D", true, 150, null, true);
    Assert.assertEquals(1, RasterAccessors.getWidth(cropped));
    Assert.assertEquals(1, RasterAccessors.getHeight(cropped));
    Assert.assertEquals(3.0, RasterAccessors.getUpperLeftX(cropped), FP_TOLERANCE);
    Assert.assertEquals(5.0, RasterAccessors.getUpperLeftY(cropped), FP_TOLERANCE);
    Assert.assertNull(
        RasterConstructors.asRaster(
            wktReader.read("POINT (8 4.5)"), northUp, "D", true, 150, null, true));
  }

  @Test
  public void testGridAlignedPolygonBurnsOnlyItsInterior() throws ParseException, FactoryException {
    // Edges on grid lines touch the cells beyond them but overlap only the interior, so allTouched
    // burns exactly the interior, along an inner corner and around a hole as well.
    for (boolean bottomUp : new boolean[] {false, true}) {
      GridCoverage2D raster = unitGrid8x8(bottomUp);
      String box =
          bottomUp ? "POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))" : "POLYGON ((2 6, 4 6, 4 4, 2 4, 2 6))";
      String lShape =
          bottomUp
              ? "POLYGON ((2 2, 6 2, 6 4, 4 4, 4 6, 2 6, 2 2))"
              : "POLYGON ((2 6, 6 6, 6 4, 4 4, 4 2, 2 2, 2 6))";
      String boxWithHole =
          bottomUp
              ? "POLYGON ((1 1, 7 1, 7 7, 1 7, 1 1), (3 3, 5 3, 5 5, 3 5, 3 3))"
              : "POLYGON ((1 7, 7 7, 7 1, 1 1, 1 7), (3 5, 5 5, 5 3, 3 3, 3 5))";
      assertBurnedLikeGdal(raster, box, "[(2,2), (3,2), (2,3), (3,3)]");
      assertBurnedLikeGdal(
          raster,
          lShape,
          "[(2,2), (3,2), (4,2), (5,2), (2,3), (3,3), (4,3), (5,3), (2,4), (3,4), (2,5), (3,5)]");
      assertBurnedLikeGdal(
          raster,
          boxWithHole,
          "[(1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (1,2), (2,2), (3,2), (4,2), (5,2), (6,2),"
              + " (1,3), (2,3), (5,3), (6,3), (1,4), (2,4), (5,4), (6,4), (1,5), (2,5), (3,5),"
              + " (4,5), (5,5), (6,5), (1,6), (2,6), (3,6), (4,6), (5,6), (6,6)]");
    }
  }

  @Test
  public void testPolygonTouchingRasterOnlyOnItsBoundaryCoversNothing()
      throws ParseException, FactoryException, TransformException {
    GridCoverage2D raster = unitGrid8x8(false);
    String[] touching = {
      "POLYGON ((-2 6, 0 6, 0 4, -2 4, -2 6))", // left edge
      "POLYGON ((8 6, 10 6, 10 4, 8 4, 8 6))", // right edge
      "POLYGON ((2 10, 4 10, 4 8, 2 8, 2 10))", // top edge
      "POLYGON ((2 0, 4 0, 4 -2, 2 -2, 2 0))", // bottom edge
      "POLYGON ((8 0, 10 0, 10 -2, 8 -2, 8 0))" // bottom-right corner
    };
    for (String wkt : touching) {
      assertBurnedLikeGdal(raster, wkt, "[]");
      Geometry zone = wktReader.read(wkt);
      for (boolean allTouched : new boolean[] {false, true}) {
        // No pixel is covered, so there is no extent to crop the output to.
        Assert.assertNull(
            RasterConstructors.asRaster(zone, raster, "D", allTouched, 150, null, true));
        Assert.assertEquals(
            Double.valueOf(0),
            RasterBandAccessors.getZonalStatsAll(raster, zone, 1, allTouched)[0]);
      }
    }

    Geometry rightEdge = wktReader.read(touching[1]);
    // RS_SetValues leaves the raster unchanged.
    GridCoverage2D updated = PixelFunctionEditors.setValues(raster, 1, rightEdge, 7, true);
    Assert.assertArrayEquals(new double[64], MapAlgebra.bandAsArray(updated, 1), 0);
    // RS_Clip keeps no pixel; cropping has nothing to crop to.
    GridCoverage2D clipped = RasterBandEditors.clip(raster, 1, rightEdge, true, -1, false, false);
    double[] allNoData = new double[64];
    Arrays.fill(allNoData, -1);
    Assert.assertArrayEquals(allNoData, MapAlgebra.bandAsArray(clipped, 1), 0);
    Assert.assertNull(RasterBandEditors.clip(raster, 1, rightEdge, true, -1, true, true));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> RasterBandEditors.clip(raster, 1, rightEdge, true, -1, true, false));
  }

  @Test
  public void testZoneOnTileBoundaryIsCountedOnceAcrossTiles()
      throws ParseException, FactoryException {
    // Two adjacent 8x8 unit tiles, A over x in [0, 8] and B over x in [8, 16], and one raster
    // covering both. Every grid line belongs to exactly one cell, so per-tile counts of a zone that
    // ends or starts on the A|B boundary add up to its count on the single raster.
    GridCoverage2D tileA = RasterConstructors.makeEmptyRaster(1, "D", 8, 8, 0, 8, 1, -1, 0, 0, 0);
    GridCoverage2D tileB = RasterConstructors.makeEmptyRaster(1, "D", 8, 8, 8, 8, 1, -1, 0, 0, 0);
    GridCoverage2D mosaic = RasterConstructors.makeEmptyRaster(1, "D", 16, 8, 0, 8, 1, -1, 0, 0, 0);
    for (String wkt :
        new String[] {
          "POLYGON ((5 2, 8 2, 8 5, 5 5, 5 2))", "POLYGON ((8 2, 11 2, 11 5, 8 5, 8 2))"
        }) {
      Geometry zone = wktReader.read(wkt);
      for (boolean allTouched : new boolean[] {false, true}) {
        double inA = RasterBandAccessors.getZonalStatsAll(tileA, zone, 1, allTouched)[0];
        double inB = RasterBandAccessors.getZonalStatsAll(tileB, zone, 1, allTouched)[0];
        double inMosaic = RasterBandAccessors.getZonalStatsAll(mosaic, zone, 1, allTouched)[0];
        Assert.assertEquals(9, inMosaic, 0);
        Assert.assertEquals(inMosaic, inA + inB, 0);
      }
    }
  }

  @Test
  public void testCroppedMixedCollectionKeepsEveryMembersCells()
      throws ParseException, FactoryException {
    // The point lies on the collection envelope's last column line, so its half-open cell is past
    // that envelope. The cropped window is the union of the members' windows, so the cropped
    // output still burns the same cells as the full-extent one, which are GDAL's.
    GridCoverage2D raster = unitGrid8x8(false);
    String wkt = "GEOMETRYCOLLECTION (POLYGON ((1 7, 3 7, 3 5, 1 5, 1 7)), POINT (6 4.5))";
    assertBurnedLikeGdal(raster, wkt, "[(1,1), (2,1), (1,2), (2,2), (6,3)]");
    for (boolean allTouched : new boolean[] {false, true}) {
      GridCoverage2D cropped =
          RasterConstructors.asRaster(
              wktReader.read(wkt), raster, "D", allTouched, 150, null, true);
      Assert.assertEquals(1.0, RasterAccessors.getUpperLeftX(cropped), FP_TOLERANCE);
      Assert.assertEquals(7.0, RasterAccessors.getUpperLeftY(cropped), FP_TOLERANCE);
      assertBurnedPixels(cropped, "[(0,0), (1,0), (0,1), (1,1), (5,2)]");
    }
  }

  @Test
  public void testCroppedOutputIsNullOnlyWhenNothingIsBurnedOrOverlapped()
      throws ParseException, FactoryException, TransformException {
    GridCoverage2D raster = unitGrid8x8(false);
    // Each of these only touches the raster: a polygon meeting it diagonally at its top-right
    // corner (its window still holds that corner cell), and lines along its right and bottom
    // edges. GDAL burns nothing for them, and with nothing burned and no pixel area overlapped
    // there is nothing to crop to.
    String[] touchingOnly = {
      "POLYGON ((9 7, 7 9, 10 10, 9 7))", "LINESTRING (8 1, 8 6)", "LINESTRING (1 0, 6 0)"
    };
    for (String wkt : touchingOnly) {
      assertBurnedLikeGdal(raster, wkt, "[]");
      for (boolean allTouched : new boolean[] {false, true}) {
        Assert.assertNull(
            RasterConstructors.asRaster(
                wktReader.read(wkt), raster, "D", allTouched, 150, null, true));
      }
    }
    Geometry corner = wktReader.read(touchingOnly[0]);
    Assert.assertNull(RasterBandEditors.clip(raster, 1, corner, true, -1, true, true));
    double[] allNoData = new double[64];
    Arrays.fill(allNoData, -1);
    Assert.assertArrayEquals(
        allNoData,
        MapAlgebra.bandAsArray(
            RasterBandEditors.clip(raster, 1, corner, true, -1, false, false), 1),
        0);
    Assert.assertArrayEquals(
        new double[64],
        MapAlgebra.bandAsArray(PixelFunctionEditors.setValues(raster, 1, corner, 7, true), 1),
        0);

    // A polygon that overlaps a pixel without covering its centre burns nothing without allTouched,
    // as in GDAL, but it does overlap the pixel, so its cropped output is that pixel, unburned.
    Geometry small = wktReader.read("POLYGON ((3.1 4.1, 3.4 4.1, 3.4 4.4, 3.1 4.4, 3.1 4.1))");
    assertBurnedPixels(
        RasterConstructors.asRasterWithRasterExtent(small, raster, "D", false, 150, null), "[]");
    GridCoverage2D cropped =
        RasterConstructors.asRaster(small, raster, "D", false, 150, null, true);
    Assert.assertEquals(1, RasterAccessors.getWidth(cropped));
    Assert.assertEquals(1, RasterAccessors.getHeight(cropped));
    Assert.assertEquals(3.0, RasterAccessors.getUpperLeftX(cropped), FP_TOLERANCE);
    Assert.assertEquals(5.0, RasterAccessors.getUpperLeftY(cropped), FP_TOLERANCE);
    assertBurnedPixels(cropped, "[]");
    assertBurnedPixels(
        RasterConstructors.asRaster(small, raster, "D", true, 150, null, true), "[(0,0)]");
  }
}
