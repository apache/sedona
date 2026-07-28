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
    if (!RasterPredicates.rsIntersects(raster, geom)) {
      throw new IllegalArgumentException("Geometry does not intersect Raster.");
    }

    ReferencedEnvelope rasterExtent = raster.getEnvelope2D();

    ReferencedEnvelope geomExtent = rasterizeGeomExtent(geom, raster, metadata, allTouched);

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
      boolean allTouched)
      throws FactoryException {

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
        rasterizePoint(geom, params, geomExtent, value);
        break;
      case "LineString":
      case "MultiLineString":
      case "LinearRing":
        rasterizeLineString(geom, params, value, geomExtent);
        break;
      default:
        rasterizePolygon(geom, params, geomExtent, value, allTouched);
        break;
    }
  }

  private static void rasterizeGeometryCollection(
      GridCoverage2D raster,
      double[] metadata,
      Geometry geom,
      RasterizationParams params,
      double value,
      boolean allTouched)
      throws FactoryException {

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry subGeom = geom.getGeometryN(i);
      ReferencedEnvelope subGeomExtent = rasterizeGeomExtent(subGeom, raster, metadata, allTouched);
      rasterizeGeometry(raster, metadata, subGeom, params, subGeomExtent, value, allTouched);
    }
  }

  private static void rasterizePoint(
      Geometry geom, RasterizationParams params, ReferencedEnvelope geomExtent, double value) {

    int startX = (int) Math.round(((geomExtent.getMinX() - params.upperLeftX) / params.scaleX));
    int startY = (int) Math.round(((geomExtent.getMinY() - params.upperLeftY) / params.scaleY));
    int x = startX;
    int y = startY;

    for (double worldY = geomExtent.getMaxY();
        worldY > geomExtent.getMinY();
        worldY += params.scaleY, y--) {
      x = startX;
      for (double worldX = geomExtent.getMinX();
          worldX < geomExtent.getMaxX();
          worldX += params.scaleX, x++) {

        // Make zero indexed
        int yIndex = y - 1;

        // Adjust for bottom-up rasters
        // Reverse the y index
        if (params.bottomUp) {
          yIndex = params.writableRaster.getHeight() - 1 - yIndex;
        }

        // Create envelope for this pixel
        double cellMaxX = worldX + params.scaleX;
        double cellMaxY = worldY + params.scaleY;

        boolean intersects =
            geom.intersects(JTS.toGeometry(new Envelope(worldX, cellMaxX, worldY, cellMaxY)));

        if (intersects) {
          params.writableRaster.setSample(x, yIndex, 0, value);
        }
      }
    }
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
   */
  private static void traverseSegment(
      RasterizationParams params, double x0, double y0, double x1, double y1, double value) {

    int width = params.writableRaster.getWidth();
    int height = params.writableRaster.getHeight();

    double dx = x1 - x0;
    double dy = y1 - y0;

    int stepX = (int) Math.signum(dx);
    int stepY = (int) Math.signum(dy);

    int x = (int) Math.floor(x0);
    int y = (int) Math.floor(y0);
    int endX = (int) Math.floor(x1);
    int endY = (int) Math.floor(y1);

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

    // Always enable allTouched for MultiLineString and MultiPoint.
    //
    // Rationale:
    // - Points and lines cannot be rasterized using "pixel-center inside geometry" logic
    //   because they have no area. For these geometry types, we must use "any touch"
    //   semantics to mark pixels they intersect/touch.
    // - Single Point/LineString cases are already safeguarded by the zero-envelope
    //   expansion logic at lines 367:373 (ensuring a non-degenerate search window).
    // - For Polygons, whether we expand the snapped extent depends on the caller’s
    //   allTouched setting (center-based vs any-intersection semantics).
    //
    // Enabling allTouched here guarantees correct handling for multi-part point/line
    // geometries whose parts may sit exactly on pixel boundaries.
    if (Objects.equals(geom.getGeometryType(), "MultiLineString")) {
      allTouched = true;
    }
    if (Objects.equals(geom.getGeometryType(), "MultiPoint")) {
      allTouched = true;
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
    // After snapping the continuous bbox to pixel edges, a point or thin line can
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

    // Handle "allTouched" behavior when geometry edges line up exactly with pixel boundaries.
    //
    // Normally, each pixel is considered only if its *center* falls inside the geometry.
    // But if an edge of the geometry sits exactly on a pixel boundary, the geometry
    // might "touch" neighboring pixels without covering their centers — and those pixels
    // would be skipped.
    //
    // When allTouched = true, we expand the aligned bounding box outward by one pixel
    // on any side where the geometry’s edge exactly matches a pixel boundary.
    // This guarantees we include those neighboring pixels that the geometry merely touches.
    //
    // We only expand sides that line up perfectly with grid lines (equal coordinates).
    // The scaleX / scaleY values (which already encode pixel size and direction) ensure
    // this expansion moves exactly one pixel outward in each direction.
    if (allTouched) {
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
    boolean bottomUp;

    RasterizationParams(
        WritableRaster writableRaster,
        String pixelType,
        double scaleX,
        double scaleY,
        double upperLeftX,
        double upperLeftY,
        boolean bottomUp) {
      this.writableRaster = writableRaster;
      this.pixelType = pixelType;
      this.scaleX = scaleX;
      this.scaleY = scaleY;
      this.upperLeftX = upperLeftX;
      this.upperLeftY = upperLeftY;
      this.bottomUp = bottomUp;
    }
  }

  public static void rasterizePolygon(
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

    if (Objects.equals(clippedGeom.getGeometryType(), "MultiPolygon")) {
      for (int i = 0; i < clippedGeom.getNumGeometries(); i++) {
        Geometry subGeom = clippedGeom.getGeometryN(i);
        rasterizePolygon(subGeom, params, geomExtent, value, allTouched);
      }
      return;
    }

    Polygon polygon = (Polygon) clippedGeom;

    // Compute scanline X-intercepts
    Map<Double, TreeSet<Double>> scanlineIntersections =
        computeScanlineIntersections(polygon, params, value, geomExtent, allTouched);

    // Process intersections to get startXs and endXs for each scanline
    Map<Integer, List<int[]>> scanlineFillRanges = computeFillRanges(scanlineIntersections);

    // Burn values between startX and endX pairs
    fillPolygon(scanlineFillRanges, params, value);
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

    for (LineString ring : allRings) {
      Coordinate[] coords = ring.getCoordinates();
      int numPoints = coords.length;

      if (allTouched) {
        rasterizeLineString(ring, params, value, geomExtent);
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

      for (int[] range : entry.getValue()) {
        if (range.length == 1) {
          params.writableRaster.setSample(range[0], y, 0, value);
          continue;
        }
        int xStart = range[0];
        int xEnd = range[1];

        for (int x = xStart; x <= xEnd; x++) {
          params.writableRaster.setSample(x, y, 0, value);
        }
      }
    }
  }
}
