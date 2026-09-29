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

import static org.apache.sedona.common.utils.RasterUtils.getRasterScaleY;
import static org.apache.sedona.common.utils.RasterUtils.getRasterUpperLeftY;

import java.awt.image.WritableRaster;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.algorithm.CGAlgorithmsDD;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;

public class Rasterization {
  protected static List<Object> rasterize(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      boolean useGeometryExtent,
      boolean allTouched)
      throws FactoryException {
    return rasterize(geom, raster, pixelType, value, useGeometryExtent, allTouched, null);
  }

  protected static List<Object> rasterize(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      boolean useGeometryExtent,
      boolean allTouched,
      Double backgroundValue)
      throws FactoryException {

    // Validate the input geometry and raster metadata
    double[] metadata = RasterAccessors.metadata(raster);
    validateRasterMetadata(metadata);
    geom = RasterUtils.transformToRasterCRS(raster, geom);
    if (!RasterPredicates.intersectsInRasterCoordinateSpace(raster, geom)) {
      throw new IllegalArgumentException("Geometry does not intersect Raster.");
    }

    ReferencedEnvelope rasterExtent = raster.getEnvelope2D();

    ReferencedEnvelope geomExtent = rasterizeGeomExtent(geom, raster, metadata, allTouched);
    if (geomExtent == null && useGeometryExtent) {
      // The geometry covers no cell under GDAL's half-open rule, for example a point on the
      // raster's right or bottom edge or a polygon that only touches the raster, so there is no
      // extent to crop the output to.
      return null;
    }

    RasterizationParams params =
        calculateRasterizationParams(raster, useGeometryExtent, metadata, geomExtent, pixelType);

    fillBackground(params.writableRaster, backgroundValue);

    rasterizeGeometry(raster, metadata, geom, params, geomExtent, value, allTouched);

    // Create a GridCoverage2D for the rasterized result
    GridCoverageFactory coverageFactory = new GridCoverageFactory();
    GridCoverage2D rasterized;

    if (useGeometryExtent) {
      rasterized = coverageFactory.create("rasterized", params.writableRaster, geomExtent);
    } else {
      rasterized = coverageFactory.create("rasterized", params.writableRaster, rasterExtent);
    }

    // If original raster is bottom-up, flip the rasterized result vertically
    if (params.bottomUp) {
      rasterized = RasterUtils.flipVerticallyGridSpace(rasterized);
    }

    // Return results compatible with the original function
    List<Object> objects = new ArrayList<>();
    objects.add(params.writableRaster);
    objects.add(rasterized);

    return objects;
  }

  /**
   * Fills every pixel of the freshly-allocated raster with the background value, so pixels never
   * covered by the geometry read back as that value instead of the allocation default of 0.
   *
   * <p>A null background, or a background of positive zero, keeps the zero-initialized allocation
   * as-is. Negative zero, however, must still be filled: it is a legitimate noDataValue that is
   * distinct from the allocation's {@code +0.0}. RS_Count (and other nodata-aware readers) compare
   * pixels against the band's nodata metadata with {@link Double#compare}, which orders {@code
   * -0.0} before {@code +0.0}, so a {@code -0.0} nodata left unfilled would leave the untouched
   * pixels as {@code +0.0} and be miscounted as data. Filling makes the pixel value and the nodata
   * metadata agree on the sign of zero.
   */
  private static void fillBackground(WritableRaster writableRaster, Double backgroundValue) {
    // Double.compare(x, 0.0) == 0 is true only for +0.0; -0.0 compares as -1 and is filled.
    if (backgroundValue == null || Double.compare(backgroundValue, 0.0) == 0) {
      return;
    }
    int width = writableRaster.getWidth();
    double[] row = new double[width];
    Arrays.fill(row, backgroundValue);
    for (int y = 0; y < writableRaster.getHeight(); y++) {
      writableRaster.setSamples(0, y, width, 1, 0, row);
    }
  }

  private static void rasterizeGeometry(
      GridCoverage2D raster,
      double[] metadata,
      Geometry geom,
      RasterizationParams params,
      ReferencedEnvelope geomExtent,
      double value,
      boolean allTouched) {

    // For instances where sub geometry is completely outside raster
    if (geomExtent == null) {
      return;
    }

    switch (geom.getGeometryType()) {
      case "GeometryCollection":
      case "MultiPolygon":
      case "MultiPoint":
        rasterizeGeometryCollection(raster, metadata, geom, params, value, allTouched);
        break;
      case "Point":
        rasterizePoint(geom, metadata, params, value);
        break;
      case "LineString":
      case "MultiLineString":
      case "LinearRing":
        rasterizeLineString(geom, params, value, geomExtent);
        break;
      default:
        rasterizePolygon(raster, metadata, geom, params, geomExtent, value, allTouched);
        break;
    }
  }

  private static void rasterizeGeometryCollection(
      GridCoverage2D raster,
      double[] metadata,
      Geometry geom,
      RasterizationParams params,
      double value,
      boolean allTouched) {

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry subGeom = geom.getGeometryN(i);
      ReferencedEnvelope subGeomExtent = rasterizeGeomExtent(subGeom, raster, metadata, allTouched);
      rasterizeGeometry(raster, metadata, subGeom, params, subGeomExtent, value, allTouched);
    }
  }

  /** Folds a pixel index that overshot the grid by a rounding artifact back onto the edge pixel. */
  private static int clampToGrid(int index, int size) {
    return Math.max(0, Math.min(index, size - 1));
  }

  /**
   * Burns the single cell that contains the point under GDAL's half-open rule, whatever allTouched
   * is: a point on a grid line or a grid corner burns exactly one cell, and a point on the raster's
   * far edge burns none. See {@link #halfOpenCell}.
   */
  private static void rasterizePoint(
      Geometry geom, double[] metadata, RasterizationParams params, double value) {
    if (geom.isEmpty()) {
      return;
    }
    int[] cell = halfOpenCell(geom.getCoordinate(), metadata);
    if (cell == null) {
      return;
    }

    // Where the output grid sits within the reference raster's grid: zero unless the output was
    // cropped to the geometry extent. The cell is resolved on the reference grid and only the
    // write index is crop-local, so a cropped output burns the same cell as a full-extent one.
    int colOffset = (int) Math.round((params.upperLeftX - params.rasterUpperLeftX) / params.scaleX);
    int rowOffset = (int) Math.round((params.upperLeftY - params.rasterUpperLeftY) / params.scaleY);
    int col = cell[0] - colOffset;
    int row = cell[1] - rowOffset;

    int width = params.writableRaster.getWidth();
    int height = params.writableRaster.getHeight();
    if (col < 0 || col >= width || row < 0 || row >= height) {
      return;
    }
    // Rows count down from the geographic top here; reverse them for bottom-up rasters.
    int yIndex = params.bottomUp ? height - 1 - row : row;
    params.writableRaster.setSample(col, yIndex, 0, value);
  }

  /**
   * Returns the cell {col, row} containing the coordinate under GDAL's half-open rule, with the row
   * counted down from the raster's geographic top, or null when the coordinate is off the raster.
   *
   * <p>A cell contains its first edge in pixel order but not its last, so a coordinate on a grid
   * line belongs to the cell after the line, and one on the raster's right or bottom edge (in pixel
   * order) belongs to no cell. The index is computed in the raster's own pixel order, so on a
   * bottom-up raster a point on a horizontal grid line falls in the cell above it, as in GDAL.
   */
  private static int[] halfOpenCell(Coordinate coord, double[] metadata) {
    int width = (int) metadata[2];
    int height = (int) metadata[3];
    int col = floorPixelIndex(coord.x, metadata[0], metadata[4]);
    int row = floorPixelIndex(coord.y, metadata[1], metadata[5]);
    if (col < 0 || col >= width || row < 0 || row >= height) {
      return null;
    }
    return new int[] {col, metadata[5] > 0 ? height - 1 - row : row};
  }

  /**
   * Computes floor((coord - upperLeft) / scale) exactly, on the same decimal values that {@link
   * #toPixelIndex} snaps extents with, so a coordinate on a grid line is not pushed across it by
   * floating-point error. Saturates at the int range for coordinates far off the raster.
   */
  private static int floorPixelIndex(double coord, double upperLeft, double scale) {
    BigDecimal index =
        BigDecimal.valueOf(coord)
            .subtract(BigDecimal.valueOf(upperLeft))
            .divide(BigDecimal.valueOf(scale), 0, RoundingMode.FLOOR);
    if (index.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
      return Integer.MAX_VALUE;
    }
    if (index.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
      return Integer.MIN_VALUE;
    }
    return index.intValue();
  }

  private static void rasterizeLineString(
      Geometry geom, RasterizationParams params, double value, ReferencedEnvelope geomExtent) {
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      LineString line = (LineString) geom.getGeometryN(i);
      Coordinate[] coords = line.getCoordinates();

      for (int j = 0; j < coords.length - 1; j++) {
        // Extract start and end points for the segment
        LineSegment clippedSegment =
            clipSegmentToRasterBounds(coords[j], coords[j + 1], geomExtent);

        Coordinate start;
        Coordinate end;
        if (clippedSegment != null) {
          start = new Coordinate(clippedSegment.p0.x, clippedSegment.p0.y);
          end = new Coordinate(clippedSegment.p1.x, clippedSegment.p1.y);
        } else {
          continue; // Skip case where segment is completely outside geomExtent
        }

        double x0 = (start.x - params.upperLeftX) / params.scaleX;
        double y0 = (start.y - params.upperLeftY) / params.scaleY;
        double x1 = (end.x - params.upperLeftX) / params.scaleX;
        double y1 = (end.y - params.upperLeftY) / params.scaleY;

        traverseSegment(params, x0, y0, x1, y1, value);
      }
    }
  }

  /**
   * Burns every grid cell the segment passes through, in pixel space where cell (i, j) covers [i, i
   * + 1) x [j, j + 1). Uses exact cell traversal (Amanatides-Woo): it steps from one cell-boundary
   * crossing to the next, so a cell is burned whenever the segment enters it, however briefly. A
   * fixed-step sampling walk instead misses cells crossed over a distance shorter than the step.
   *
   * <p>When an endpoint's pixel coordinate is exactly integral, its cell is selected from the side
   * containing the segment interior. This avoids burning the neighboring cell that is contacted
   * only at the endpoint, and it keeps an exact-corner end cell reachable by the traversal.
   */
  private static void traverseSegment(
      RasterizationParams params, double x0, double y0, double x1, double y1, double value) {
    traverseSegment(params, x0, y0, x1, y1, value, false, false);
  }

  /**
   * As {@link #traverseSegment(RasterizationParams, double, double, double, double, double)}, for a
   * segment that may lie exactly on a grid line. A segment with no extent along an axis whose pixel
   * coordinate is integral runs between two rows (or columns) of cells; by default it burns the
   * half-open one after the line, and {@code flatTowardsNegativeX} / {@code flatTowardsNegativeY}
   * select the one before it instead.
   */
  private static void traverseSegment(
      RasterizationParams params,
      double x0,
      double y0,
      double x1,
      double y1,
      double value,
      boolean flatTowardsNegativeX,
      boolean flatTowardsNegativeY) {

    int width = params.writableRaster.getWidth();
    int height = params.writableRaster.getHeight();

    double dx = x1 - x0;
    double dy = y1 - y0;

    int stepX = (int) Math.signum(dx);
    int stepY = (int) Math.signum(dy);

    boolean flatX = dx == 0 && flatTowardsNegativeX;
    boolean flatY = dy == 0 && flatTowardsNegativeY;
    int x = interiorCell(x0, dx < 0 || flatX);
    int y = interiorCell(y0, dy < 0 || flatY);
    int endX = interiorCell(x1, dx > 0 || flatX);
    int endY = interiorCell(y1, dy > 0 || flatY);

    // The endpoints are clipped to the geometry extent, so the walk covers at most the grid's
    // width + height cells; the bound also guards against a floating-point overshoot never reaching
    // the end cell.
    int maxSteps = width + height + 2;
    for (int i = 0; i <= maxSteps; i++) {
      burnCell(params, x, y, value, width, height);
      if (x == endX && y == endY) {
        break;
      }
      // Decide which axis to advance from the side of the segment on which the next cell corner in
      // the direction of travel lies. The robust double-double orientation predicate resolves that
      // side exactly, so the crossing order is computed identically regardless of endpoint order. A
      // corner off the segment steps the single axis that keeps the walk on the segment's side; a
      // corner the segment passes exactly through steps both axes at once, burning only the two
      // cells the segment truly crosses rather than the off-diagonal pair. This supersedes the
      // parametric tMax comparison, whose floating-point tie was fragile: recomputed from opposite
      // endpoints in the two directions it could round asymmetrically and flip the step near a
      // corner, burning a direction-dependent off-diagonal cell.
      int crossingOrder;
      if (stepX == 0) {
        crossingOrder = 1;
      } else if (stepY == 0) {
        crossingOrder = -1;
      } else {
        double gridX = stepX > 0 ? x + 1.0 : x;
        double gridY = stepY > 0 ? y + 1.0 : y;
        int orientation = CGAlgorithmsDD.orientationIndex(x0, y0, x1, y1, gridX, gridY);
        crossingOrder = -orientation * stepX * stepY;
      }
      if (crossingOrder < 0) {
        x += stepX;
      } else if (crossingOrder > 0) {
        y += stepY;
      } else {
        x += stepX;
        y += stepY;
      }
    }
  }

  /**
   * Grid index of the cell containing the segment interior next to an endpoint. Only an exactly
   * integral pixel coordinate is biased to the negative-side cell; nearby nonintegral coordinates
   * and axes with no extent retain the ordinary half-open {@code floor()} result.
   */
  private static int interiorCell(double coord, boolean extendsNegative) {
    int cell = (int) Math.floor(coord);
    if (extendsNegative && coord == cell) {
      cell--;
    }
    return cell;
  }

  private static void burnCell(
      RasterizationParams params, int x, int y, double value, int width, int height) {
    // Reverse the y index for bottom-up rasters
    int rasterY = params.bottomUp ? height - 1 - y : y;
    if (x >= 0 && x < width && rasterY >= 0 && rasterY < height) {
      params.writableRaster.setSample(x, rasterY, 0, value);
    }
  }

  private static LineSegment clipSegmentToRasterBounds(
      Coordinate p1, Coordinate p2, ReferencedEnvelope geomExtent) {
    double minX = geomExtent.getMinX();
    double maxX = geomExtent.getMaxX();
    double minY = geomExtent.getMinY();
    double maxY = geomExtent.getMaxY();

    double x1 = p1.x, y1 = p1.y;
    double x2 = p2.x, y2 = p2.y;

    boolean p1Inside = isInsideBounds(x1, y1, minX, maxX, minY, maxY);
    boolean p2Inside = isInsideBounds(x2, y2, minX, maxX, minY, maxY);

    if (p1Inside && p2Inside) {
      // Both points inside: no clipping needed
      return new LineSegment(p1, p2);
    }

    if (!p1Inside && !p2Inside) {
      // Both points outside: no clipping needed
      return null;
    }

    double dx = x2 - x1;
    double dy = y2 - y1;

    // Clip using parametric line equation
    double[] tValues = {0, 1}; // Stores valid segment proportions

    // Clip against minX and maxX
    if (dx != 0) {
      double tMin = (minX - x1) / dx;
      double tMax = (maxX - x1) / dx;
      if (dx < 0) {
        double temp = tMin;
        tMin = tMax;
        tMax = temp;
      }
      tValues[0] = Math.max(tValues[0], tMin);
      tValues[1] = Math.min(tValues[1], tMax);
    }

    // Clip against minY and maxY
    if (dy != 0) {
      double tMin = (minY - y1) / dy;
      double tMax = (maxY - y1) / dy;
      if (dy < 0) {
        double temp = tMin;
        tMin = tMax;
        tMax = temp;
      }
      tValues[0] = Math.max(tValues[0], tMin);
      tValues[1] = Math.min(tValues[1], tMax);
    }

    // If tValues are invalid (segment is outside), return null
    if (tValues[0] > tValues[1]) {
      return null; // No valid clipped segment
    }

    // Compute new clipped endpoints
    Coordinate newP1 = new Coordinate(x1 + tValues[0] * dx, y1 + tValues[0] * dy);
    Coordinate newP2 = new Coordinate(x1 + tValues[1] * dx, y1 + tValues[1] * dy);

    return new LineSegment(newP1, newP2);
  }

  // Helper function to check if a point is inside the bounding box
  private static boolean isInsideBounds(
      double x, double y, double minX, double maxX, double minY, double maxY) {
    return x >= minX && x <= maxX && y >= minY && y <= maxY;
  }

  static ReferencedEnvelope rasterizeGeomExtent(
      Geometry geom, GridCoverage2D raster, double[] metadata, boolean allTouched) {

    validateRasterMetadata(metadata);

    // A point burns exactly the half-open cell it lies in, so its extent is that cell.
    if (geom instanceof Puntal) {
      return pointCellsExtent(geom, raster, metadata);
    }
    // A mixed collection's window is the union of its members' windows, so each member keeps the
    // cells it burns.
    if (geom.getClass() == GeometryCollection.class) {
      ReferencedEnvelope union = null;
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        ReferencedEnvelope member =
            rasterizeGeomExtent(geom.getGeometryN(i), raster, metadata, allTouched);
        if (member == null) {
          continue;
        }
        if (union == null) {
          union = new ReferencedEnvelope(member);
        } else {
          union.expandToInclude(member);
        }
      }
      return union;
    }

    ReferencedEnvelope geomExtent =
        new ReferencedEnvelope(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());

    // Using BigDecimal to avoid floating point errors
    double upperLeftX = metadata[0];
    double upperLeftY = getRasterUpperLeftY(metadata);
    double scaleX = metadata[4];
    double scaleY = getRasterScaleY(metadata);

    // Compute the aligned min/max values
    double alignedMinX =
        toWorldCoordinate(
            toPixelIndex(geomExtent.getMinX(), scaleX, upperLeftX, true), scaleX, upperLeftX);
    double alignedMinY =
        toWorldCoordinate(
            toPixelIndex(geomExtent.getMinY(), scaleY, upperLeftY, true), scaleY, upperLeftY);
    double alignedMaxX =
        toWorldCoordinate(
            toPixelIndex(geomExtent.getMaxX(), scaleX, upperLeftX, false), scaleX, upperLeftX);
    double alignedMaxY =
        toWorldCoordinate(
            toPixelIndex(geomExtent.getMaxY(), scaleY, upperLeftY, false), scaleY, upperLeftY);

    // Ensure the snapped AOI window is at least one pixel wide/tall.
    // After snapping the continuous bbox to pixel edges, a thin line can
    // collapse to min == max along an axis (i.e., a degenerate 0-width/0-height window).
    // That would produce an empty search region and skip rasterization entirely.
    //
    // We grow the aligned window by exactly one pixel on each side, using the
    // pixel size (scaleX > 0, scaleY < 0 for north-up). This yields at least a
    // 1-pixel-wide/1-pixel-tall search window and guarantees we visit the pixel(s)
    // the geometry lies in.
    if (alignedMaxX == alignedMinX) {
      alignedMinX -= scaleX;
      alignedMaxX += scaleX;
    }
    if (alignedMaxY == alignedMinY) {
      alignedMinY += scaleY;
      alignedMaxY -= scaleY;
    }

    // Get the extent of the original raster
    double originalMinX = raster.getEnvelope().getMinimum(0);
    double originalMinY = raster.getEnvelope().getMinimum(1);
    double originalMaxX = raster.getEnvelope().getMaximum(0);
    double originalMaxY = raster.getEnvelope().getMaximum(1);

    // Quick bbox intersection test for when rasterizeGeomExtent gets called independently
    // return null if they do not intersect
    if (alignedMinX >= originalMaxX
        || alignedMaxX <= originalMinX
        || alignedMinY >= originalMaxY
        || alignedMaxY <= originalMinY) {
      return null;
    }

    // Widen the window of a line by one pixel on any side that lies exactly on a grid line (always
    // for a MultiLineString, and with allTouched for a LineString).
    //
    // This only sizes the search window and the cropped output; which cells a line burns is decided
    // by traverseSegment under GDAL's half-open rule. A line lying on a grid line burns the cells
    // after it, which sit outside the snapped window when the line is the window's right or bottom
    // edge. A polygon or point never burns a cell it touches only on its boundary, as in GDAL, so
    // its window is not widened.
    if (geom instanceof Lineal && (allTouched || geom instanceof MultiLineString)) {
      alignedMinX -= (geomExtent.getMinX() == alignedMinX) ? scaleX : 0;
      alignedMinY += (geomExtent.getMinY() == alignedMinY) ? scaleY : 0;
      alignedMaxX += (geomExtent.getMaxX() == alignedMaxX) ? scaleX : 0;
      alignedMaxY -= (geomExtent.getMaxY() == alignedMaxY) ? scaleY : 0;
    }

    // Clamp the aligned extent to the original raster extent
    alignedMinX = Math.max(alignedMinX, originalMinX);
    alignedMinY = Math.max(alignedMinY, originalMinY);
    alignedMaxX = Math.min(alignedMaxX, originalMaxX);
    alignedMaxY = Math.min(alignedMaxY, originalMaxY);

    // Create the aligned raster extent
    ReferencedEnvelope alignedRasterExtent =
        new ReferencedEnvelope(
            alignedMinX,
            alignedMaxX,
            alignedMinY,
            alignedMaxY,
            geomExtent.getCoordinateReferenceSystem());

    return alignedRasterExtent;
  }

  /**
   * The extent of the half-open cells the points fall in, or null when none of them lies on the
   * raster. See {@link #halfOpenCell}.
   */
  private static ReferencedEnvelope pointCellsExtent(
      Geometry geom, GridCoverage2D raster, double[] metadata) {
    double upperLeftX = metadata[0];
    double upperLeftY = getRasterUpperLeftY(metadata);
    double scaleX = metadata[4];
    double scaleY = getRasterScaleY(metadata);

    Envelope cells = new Envelope();
    for (Coordinate coord : geom.getCoordinates()) {
      int[] cell = halfOpenCell(coord, metadata);
      if (cell == null) {
        continue;
      }
      cells.expandToInclude(
          toWorldCoordinate(cell[0], scaleX, upperLeftX),
          toWorldCoordinate(cell[1], scaleY, upperLeftY));
      cells.expandToInclude(
          toWorldCoordinate(cell[0] + 1, scaleX, upperLeftX),
          toWorldCoordinate(cell[1] + 1, scaleY, upperLeftY));
    }
    return cells.isNull()
        ? null
        : new ReferencedEnvelope(cells, raster.getCoordinateReferenceSystem2D());
  }

  private static double toPixelIndex(double coord, double scale, double upperLeft, boolean isMin) {
    BigDecimal rel = BigDecimal.valueOf(coord).subtract(BigDecimal.valueOf(upperLeft));

    BigDecimal px = rel.divide(BigDecimal.valueOf(scale), 16, RoundingMode.FLOOR);
    if (scale > 0) {
      return isMin
          ? px.setScale(0, RoundingMode.FLOOR).doubleValue()
          : px.setScale(0, RoundingMode.CEILING).doubleValue();
    } else {
      return isMin
          ? px.setScale(0, RoundingMode.CEILING).doubleValue()
          : px.setScale(0, RoundingMode.FLOOR).doubleValue();
    }
  }

  private static double toWorldCoordinate(double pixelIndex, double scale, double upperLeft) {
    return BigDecimal.valueOf(pixelIndex)
        .multiply(BigDecimal.valueOf(scale))
        .add(BigDecimal.valueOf(upperLeft))
        .doubleValue();
  }

  private static RasterizationParams calculateRasterizationParams(
      GridCoverage2D raster,
      boolean useGeometryExtent,
      double[] metadata,
      ReferencedEnvelope geomExtent,
      String pixelType) {

    double upperLeftX = 0;
    double upperLeftY = 0;
    if (useGeometryExtent) {
      upperLeftX = geomExtent.getMinX();
      upperLeftY = geomExtent.getMaxY();
    } else {
      upperLeftX = metadata[0];
      upperLeftY = getRasterUpperLeftY(metadata);
    }

    WritableRaster writableRaster;
    if (useGeometryExtent) {
      int geomExtentWidth = (int) (Math.round(geomExtent.getWidth() / metadata[4]));
      int geomExtentHeight =
          (int) (Math.round(geomExtent.getHeight() / -getRasterScaleY(metadata)));

      writableRaster =
          RasterFactory.createBandedRaster(
              RasterUtils.getDataTypeCode(pixelType), geomExtentWidth, geomExtentHeight, 1, null);
    } else {
      int rasterWidth = (int) raster.getGridGeometry().getGridRange2D().getWidth();
      int rasterHeight = (int) raster.getGridGeometry().getGridRange2D().getHeight();
      writableRaster =
          RasterFactory.createBandedRaster(
              RasterUtils.getDataTypeCode(pixelType), rasterWidth, rasterHeight, 1, null);
    }

    boolean bottomUp = metadata[5] > 0;

    return new RasterizationParams(
        writableRaster,
        pixelType,
        metadata[4],
        getRasterScaleY(metadata),
        upperLeftX,
        upperLeftY,
        metadata[0],
        getRasterUpperLeftY(metadata),
        bottomUp);
  }

  private static void validateRasterMetadata(double[] rasterMetadata) {
    if (rasterMetadata[4] < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid raster metadata: scaleX is negative (%.2f). Right-to-left rasters are not supported.",
              rasterMetadata[4]));
    }
    if (rasterMetadata[6] != 0 || rasterMetadata[7] != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid raster metadata: skewX is %.2f and skewY is %.2f. Both values must be zero.",
              rasterMetadata[6], rasterMetadata[7]));
    }
  }

  private static class RasterizationParams {
    WritableRaster writableRaster;
    String pixelType;
    double scaleX;
    double scaleY;
    double upperLeftX;
    double upperLeftY;
    // Origin of the reference raster's grid. Equal to upperLeftX/Y unless the output is cropped to
    // the geometry extent. Cell envelopes are built from this origin because it is the grid the
    // geometry extent was snapped against, so cell edges and snapped extents agree bit for bit.
    double rasterUpperLeftX;
    double rasterUpperLeftY;
    boolean bottomUp;

    RasterizationParams(
        WritableRaster writableRaster,
        String pixelType,
        double scaleX,
        double scaleY,
        double upperLeftX,
        double upperLeftY,
        double rasterUpperLeftX,
        double rasterUpperLeftY,
        boolean bottomUp) {
      this.writableRaster = writableRaster;
      this.pixelType = pixelType;
      this.scaleX = scaleX;
      this.scaleY = scaleY;
      this.upperLeftX = upperLeftX;
      this.upperLeftY = upperLeftY;
      this.rasterUpperLeftX = rasterUpperLeftX;
      this.rasterUpperLeftY = rasterUpperLeftY;
      this.bottomUp = bottomUp;
    }
  }

  private static void rasterizePolygon(
      GridCoverage2D raster,
      double[] metadata,
      Geometry geom,
      RasterizationParams params,
      ReferencedEnvelope geomExtent,
      double value,
      boolean allTouched) {
    if (!(geom instanceof Polygon)) {
      throw new IllegalArgumentException("Only Polygon geometry is supported");
    }

    // Clip polygon to rasterExtent
    // Using buffer(0) to avoid TopologyException : found non-noded intersection.
    Geometry clippedGeom =
        Functions.intersection(JTS.toGeometry((BoundingBox) geomExtent), Functions.buffer(geom, 0));

    if (clippedGeom instanceof MultiPolygon) {
      for (int i = 0; i < clippedGeom.getNumGeometries(); i++) {
        Geometry subGeom = clippedGeom.getGeometryN(i);
        rasterizePolygon(raster, metadata, subGeom, params, geomExtent, value, allTouched);
      }
    } else if (clippedGeom instanceof Polygon) {
      Polygon polygon = (Polygon) clippedGeom;

      // Compute scanline X-intercepts
      Map<Double, TreeSet<Double>> scanlineIntersections =
          computeScanlineIntersections(polygon, params, value, geomExtent, allTouched);

      // Process intersections to get startXs and endXs for each scanline
      Map<Integer, List<int[]>> scanlineFillRanges = computeFillRanges(scanlineIntersections);

      // Burn values between startX and endX pairs
      fillPolygon(scanlineFillRanges, params, value);
    } else {
      // Clipped geometry could be anything, such as a GeometryCollection, although such cases are
      // rare. Delegate to rasterizeGeometry to handle all these cases.
      //
      // Points and lines left behind by the clip - where the polygon only touches the raster or the
      // window - are boundary contact: they overlap no cell. As in GDAL, a polygon never burns a
      // cell it touches only on its boundary, allTouched or not, so keep only the areal parts.
      Geometry toRasterize = arealComponents(clippedGeom);
      if (!toRasterize.isEmpty()) {
        rasterizeGeometry(raster, metadata, toRasterize, params, geomExtent, value, allTouched);
      }
    }
  }

  /** Keeps only the polygonal parts of a geometry, dropping any point or line components. */
  private static Geometry arealComponents(Geometry geom) {
    List<Geometry> polygons = new ArrayList<>();
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry part = geom.getGeometryN(i);
      if (part instanceof Polygon) {
        polygons.add(part);
      }
    }
    return geom.getFactory().buildGeometry(polygons);
  }

  /** Computes scanline intersections by iterating over polygon edges. */
  private static Map<Double, TreeSet<Double>> computeScanlineIntersections(
      Polygon polygon,
      RasterizationParams params,
      double value,
      ReferencedEnvelope geomExtent,
      boolean allTouched) {

    Map<Double, TreeSet<Double>> scanlineIntersections = new HashMap<>();
    List<LineString> allRings = new ArrayList<>();
    allRings.add(polygon.getExteriorRing());

    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      allRings.add(polygon.getInteriorRingN(i));
    }

    for (int r = 0; r < allRings.size(); r++) {
      LineString ring = allRings.get(r);
      Coordinate[] coords = ring.getCoordinates();
      int numPoints = coords.length;

      if (allTouched) {
        burnRingOutline(coords, r == 0, params, value, geomExtent);
      }

      for (int i = 0; i < numPoints - 1; i++) {
        Coordinate worldP1 = coords[i];
        Coordinate worldP2 = coords[i + 1];

        // Ensure worldP1.y ≤ worldP2.y
        if (worldP1.y > worldP2.y) {
          Coordinate temp = worldP1;
          worldP1 = worldP2;
          worldP2 = temp;
        }

        if (worldP1.y == worldP2.y) {
          continue; // Skip horizontal edges
        }

        // Calculate scan line limits to iterate between for each segment
        // Using BigDecimal to avoid floating point errors
        double yStart =
            Math.round(
                (BigDecimal.valueOf(worldP1.y)
                        .subtract(BigDecimal.valueOf(params.upperLeftY))
                        .divide(BigDecimal.valueOf(params.scaleY), RoundingMode.CEILING))
                    .doubleValue());

        double yEnd =
            Math.round(
                (BigDecimal.valueOf(worldP2.y)
                        .subtract(BigDecimal.valueOf(params.upperLeftY))
                        .divide(BigDecimal.valueOf(params.scaleY), RoundingMode.FLOOR))
                    .doubleValue());

        // Contain y range within geomExtent; Use centroid y line as scan line
        yEnd = Math.max(0.5, Math.abs(yEnd) + 0.5);
        yStart = Math.min((params.writableRaster.getHeight() - 0.5), Math.abs(yStart) - 0.5);

        double p1X = (worldP1.x - params.upperLeftX) / params.scaleX;
        double p1Y = (worldP1.y - params.upperLeftY) / params.scaleY;

        if (worldP1.x == worldP2.x) {
          // Vertical line case: directly set xIntercept. Avoid divide by zero error when
          // calculating slope
          for (double y = yStart; y >= yEnd; y--) {
            double xIntercept = p1X; // Vertical line, xIntercept is constant
            scanlineIntersections.computeIfAbsent(y, k -> new TreeSet<>()).add(xIntercept);
          }
        } else {
          double slope = (worldP2.y - worldP1.y) / (worldP2.x - worldP1.x);
          double xMin = (geomExtent.getMinX() - params.upperLeftX) / params.scaleX;
          double xMax = (geomExtent.getMaxX() - params.upperLeftX) / params.scaleX;

          for (double y = yStart; y >= yEnd; y--) {
            // p1X, p1Y and y are in pixel units while slope is world-units dy
            // over dx, so converting the pixel dy to world units (scaleY) and
            // the resulting world dx back to pixels (scaleX) keeps the
            // intercept in pixel space for any pixel aspect ratio.
            double xIntercept = p1X + ((y - p1Y) * params.scaleY / slope / params.scaleX);
            if ((xIntercept < xMin) || (xIntercept >= xMax)) {
              continue; // Skip xIntercepts outside geomExtent
            }
            scanlineIntersections.computeIfAbsent(y, k -> new TreeSet<>()).add(xIntercept);
          }
        }
      }
    }
    return scanlineIntersections;
  }

  /**
   * Burns every cell a polygon ring's outline passes through, for allTouched.
   *
   * <p>An outline edge that lies exactly on a grid line touches the cells on both sides of it but
   * overlaps only the one on the polygon's side. Burning the half-open default instead would burn a
   * cell outside a grid-aligned polygon along its right and bottom edges. As in GDAL, a polygon
   * never burns a cell it touches only on its boundary, so such an edge burns the cell on the
   * polygon's side, found from the ring's orientation.
   */
  private static void burnRingOutline(
      Coordinate[] coords,
      boolean isShell,
      RasterizationParams params,
      double value,
      ReferencedEnvelope geomExtent) {
    if (coords.length < 4) {
      return;
    }
    // The polygon's interior is on the left of a counter-clockwise shell, and on the right of a
    // counter-clockwise hole.
    boolean interiorOnLeft = Orientation.isCCW(coords) == isShell;
    double side = interiorOnLeft ? 1 : -1;

    for (int i = 0; i < coords.length - 1; i++) {
      LineSegment clipped = clipSegmentToRasterBounds(coords[i], coords[i + 1], geomExtent);
      if (clipped == null) {
        continue;
      }
      // The left normal of the direction of travel is (-dy, dx) in world space. Pixel x grows with
      // world x when scaleX is positive, and pixel y grows as world y falls (scaleY < 0 here).
      double worldDx = coords[i + 1].x - coords[i].x;
      double worldDy = coords[i + 1].y - coords[i].y;
      boolean interiorTowardsNegativeWorldX = -worldDy * side < 0;
      boolean interiorTowardsPositiveWorldY = worldDx * side > 0;
      boolean flatTowardsNegativeX = interiorTowardsNegativeWorldX == (params.scaleX > 0);
      boolean flatTowardsNegativeY = interiorTowardsPositiveWorldY;

      double x0 = (clipped.p0.x - params.upperLeftX) / params.scaleX;
      double y0 = (clipped.p0.y - params.upperLeftY) / params.scaleY;
      double x1 = (clipped.p1.x - params.upperLeftX) / params.scaleX;
      double y1 = (clipped.p1.y - params.upperLeftY) / params.scaleY;
      traverseSegment(params, x0, y0, x1, y1, value, flatTowardsNegativeX, flatTowardsNegativeY);
    }
  }

  /** Computes startX and endX pairs for each scanline by leveraging sorted X-intercepts. */
  private static Map<Integer, List<int[]>> computeFillRanges(
      Map<Double, TreeSet<Double>> scanlineIntersections) {

    Map<Integer, List<int[]>> scanlineFillRanges = new HashMap<>();

    for (Map.Entry<Double, TreeSet<Double>> entry : scanlineIntersections.entrySet()) {
      double y = entry.getKey();
      TreeSet<Double> xIntercepts = entry.getValue();
      List<int[]> ranges = new ArrayList<>();

      Iterator<Double> it = xIntercepts.iterator();
      while (it.hasNext()) {
        double x1 = it.next();
        if (!it.hasNext()) {
          ranges.add(new int[] {(int) Math.floor(x1)});
          continue;
        }
        double x2 = it.next();

        // Compute centroids
        double firstCentroid = Math.ceil(x1 - 0.5) + 0.5;
        double lastCentroid = Math.floor(x2 + 0.5) - 0.5;

        if (lastCentroid < firstCentroid) {
          continue; // No valid pixel to include
        }
        if (lastCentroid == firstCentroid) {
          ranges.add(new int[] {(int) Math.floor(firstCentroid)});
        } else {
          ranges.add(new int[] {(int) Math.floor(firstCentroid), (int) Math.floor(lastCentroid)});
        }
      }

      scanlineFillRanges.put((int) Math.floor(y), ranges);
    }

    return scanlineFillRanges;
  }

  /** Burns pixels between startX and endX for each scanline. */
  private static void fillPolygon(
      Map<Integer, List<int[]>> scanlineFillRanges, RasterizationParams params, double value) {

    for (Map.Entry<Integer, List<int[]>> entry : scanlineFillRanges.entrySet()) {
      int y = entry.getKey();

      // Adjust for bottom-up rasters
      // Reverse the y index
      if (params.bottomUp) {
        y = params.writableRaster.getHeight() - 1 - y;
      }

      int width = params.writableRaster.getWidth();

      for (int[] range : entry.getValue()) {
        if (range.length == 1) {
          // A scanline with an odd number of intercepts leaves an unpaired one, burned as a single
          // pixel. Vertical edges keep their x-intercept unfiltered, so an edge touching only at
          // the extent maximum lands one past the grid. Discard it rather than folding it onto the
          // edge pixel: unlike the point path there is no per-cell intersection test here, so
          // folding would burn a pixel the polygon does not reach.
          if (range[0] >= 0 && range[0] < width) {
            params.writableRaster.setSample(range[0], y, 0, value);
          }
          continue;
        }
        int xStart = clampToGrid(range[0], width);
        int xEnd = clampToGrid(range[1], width);

        for (int x = xStart; x <= xEnd; x++) {
          params.writableRaster.setSample(x, y, 0, value);
        }
      }
    }
  }
}
