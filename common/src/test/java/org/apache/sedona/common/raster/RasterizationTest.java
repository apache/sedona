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

import org.apache.sedona.common.Constructors;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
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
  public void testLineCrossingRasterWithBothEndpointsOutside()
      throws ParseException, FactoryException {
    // A segment that crosses the raster must be rasterized even though neither endpoint is inside
    // it. Clipping used to treat "both endpoints outside" as "no intersection" and drop it.
    GridCoverage2D raster = unitGrid6x6();

    double[] row3 = new double[36];
    java.util.Arrays.fill(row3, 18, 24, 1d);
    assertLineBurns(raster, "LINESTRING (-1 2.5, 7 2.5)", row3);
    assertLineBurns(raster, "LINESTRING (7 2.5, -1 2.5)", row3);

    double[] column2 = new double[36];
    for (int row = 0; row < 6; row++) {
      column2[row * 6 + 2] = 1d;
    }
    assertLineBurns(raster, "LINESTRING (2.5 -1, 2.5 7)", column2);
    assertLineBurns(raster, "LINESTRING (2.5 7, 2.5 -1)", column2);

    double[] antiDiagonal = new double[36];
    for (int row = 0; row < 6; row++) {
      antiDiagonal[row * 6 + (5 - row)] = 1d;
    }
    assertLineBurns(raster, "LINESTRING (-1 -1, 7 7)", antiDiagonal);
    assertLineBurns(raster, "LINESTRING (7 7, -1 -1)", antiDiagonal);
  }

  @Test
  public void testClippedEndpointKeepsGridLineSide() throws ParseException, FactoryException {
    // Inside the raster this line is strictly above y = 3, so it belongs to row 2. Interpolating
    // the clipped endpoint at x = 0 rounds it onto y = 3 exactly, which used to move the burn to
    // row 3.
    GridCoverage2D raster = unitGrid6x6();
    double[] expected = new double[36];
    expected[2 * 6] = 1d;
    assertLineBurns(raster, "LINESTRING (-1 3.0000000000000004, 1 3.0)", expected);
    assertLineBurns(raster, "LINESTRING (1 3.0, -1 3.0000000000000004)", expected);

    // The mirrored case below the grid line stays in row 3.
    double[] below = new double[36];
    below[3 * 6] = 1d;
    assertLineBurns(raster, "LINESTRING (-1 2.9999999999999996, 1 3.0)", below);
    assertLineBurns(raster, "LINESTRING (1 3.0, -1 2.9999999999999996)", below);

    // The same rounding on the other axis: strictly left of x = 3 inside the raster, so column 2.
    double[] leftOfColumn3 = new double[36];
    leftOfColumn3[5 * 6 + 2] = 1d;
    assertLineBurns(raster, "LINESTRING (2.9999999999999996 -1, 3.0 1)", leftOfColumn3);
    assertLineBurns(raster, "LINESTRING (3.0 1, 2.9999999999999996 -1)", leftOfColumn3);
  }

  @Test
  public void testLineTouchingRasterAtASinglePointBurnsNothing()
      throws ParseException, FactoryException {
    // The segment meets the window only at the corner (6, 6) in world space, which is a single
    // point and therefore has no length inside the raster.
    GridCoverage2D raster = unitGrid6x6();
    assertLineBurns(raster, "LINESTRING (5 7, 7 5)", new double[36]);
    assertLineBurns(raster, "LINESTRING (7 5, 5 7)", new double[36]);
  }

  @Test
  public void testLineWithExtremeCoordinatesCrossesTheRaster()
      throws ParseException, FactoryException {
    // Endpoints far enough apart that their difference overflows to infinity. The clip must still
    // produce the crossing rather than a NaN-driven omission or a partial streak.
    GridCoverage2D raster = unitGrid6x6();

    double[] row3 = new double[36];
    java.util.Arrays.fill(row3, 18, 24, 1d);
    assertLineBurns(raster, "LINESTRING (-1E308 2.5, 1E308 2.5)", row3);
    assertLineBurns(raster, "LINESTRING (1E308 2.5, -1E308 2.5)", row3);

    double[] column2 = new double[36];
    for (int row = 0; row < 6; row++) {
      column2[row * 6 + 2] = 1d;
    }
    assertLineBurns(raster, "LINESTRING (2.5 -1E308, 2.5 1E308)", column2);
    assertLineBurns(raster, "LINESTRING (2.5 1E308, 2.5 -1E308)", column2);
  }

  @Test
  public void testDegenerateLineStringBurnsItsOwnCell() throws ParseException, FactoryException {
    // A LineString that was degenerate to begin with still marks the cell holding it, unlike a
    // nondegenerate segment that clipping collapses to a single point.
    GridCoverage2D raster = unitGrid6x6();
    double[] expected = new double[36];
    expected[3 * 6 + 2] = 1d;
    assertLineBurns(raster, "LINESTRING (2.5 2.5, 2.5 2.5)", expected);
  }

  @Test
  public void testLineClipAgreesWithExactRationalOracle() throws ParseException, FactoryException {
    // Cross-check the clipper against an independent exact-rational clip of the same segment
    // against the same window. The oracle runs entirely in BigInteger rationals, so a segment that
    // grazes a grid line is resolved rather than dropped.
    GridCoverage2D raster = unitGrid6x6();
    String[] segments = {
      "LINESTRING (-1 2.5, 7 2.5)",
      "LINESTRING (-1 -1, 7 7)",
      "LINESTRING (-4 3, 9 3)",
      "LINESTRING (3 -4, 3 9)",
      "LINESTRING (-1 3.0000000000000004, 1 3.0)",
      "LINESTRING (-2 0.5, 8 5.5)",
      "LINESTRING (0.5 -2, 5.5 8)",
      "LINESTRING (-3 7, 7 -3)",
      "LINESTRING (1.25 5.75, 3.75 0.25)",
      "LINESTRING (1.5 1.5, 4.5 4.5)",
      "LINESTRING (0 0, 6 6)",
      "LINESTRING (0 6, 6 0)",
      "LINESTRING (-1 6, 1 4)",
      "LINESTRING (2.5 2.5, 3.5 2.5)",
      "LINESTRING (-0.5 0.25, 6.5 0.25)",
    };

    for (String wkt : segments) {
      Geometry forward = Constructors.geomFromWKT(wkt, 0);
      double[] band =
          MapAlgebra.bandAsArray(
              RasterConstructors.asRaster(forward, raster, "d", false, 1d, 0d, false), 1);
      double[] reverseBand =
          MapAlgebra.bandAsArray(
              RasterConstructors.asRaster(forward.reverse(), raster, "d", false, 1d, 0d, false), 1);
      Assert.assertArrayEquals(wkt + " must not depend on vertex order", band, reverseBand, 0d);

      Coordinate[] coords = forward.getCoordinates();
      // unitGrid6x6 puts the origin at (0, 6) with unit pixels and scaleY = -1.
      SegmentClipOracle.Clip clip =
          SegmentClipOracle.clip(coords[0].x, 6 - coords[0].y, coords[1].x, 6 - coords[1].y, 6, 6);

      if (clip == null) {
        Assert.assertArrayEquals(
            wkt + " has no length inside the raster", new double[36], band, 0d);
        continue;
      }
      assertBurned(wkt + " start", band, clip.x0, clip.x1, clip.y0, clip.y1);
      assertBurned(wkt + " end", band, clip.x1, clip.x0, clip.y1, clip.y0);
    }
  }

  private static void assertBurned(
      String message,
      double[] band,
      SegmentClipOracle.Rational x,
      SegmentClipOracle.Rational otherX,
      SegmentClipOracle.Rational y,
      SegmentClipOracle.Rational otherY) {
    int column = SegmentClipOracle.cellOf(x, otherX);
    int row = SegmentClipOracle.cellOf(y, otherY);
    if (column < 0 || column >= 6 || row < 0 || row >= 6) {
      return;
    }
    Assert.assertEquals(
        message + " expected cell (row " + row + ", column " + column + ") to be burned",
        1d,
        band[row * 6 + column],
        0d);
  }

  private void assertLineBurns(GridCoverage2D raster, String wkt, double[] expected)
      throws ParseException, FactoryException {
    Geometry geom = Constructors.geomFromWKT(wkt, 0);
    double[] band =
        MapAlgebra.bandAsArray(
            RasterConstructors.asRaster(geom, raster, "d", false, 1d, 0d, false), 1);
    Assert.assertArrayEquals(wkt, expected, band, 0d);
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
