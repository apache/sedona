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
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.PolygonExtracter;

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
      // The geometry's window holds no cell of the raster, for example a point on the raster's
      // right or bottom edge or a polygon that only touches one of its edges, so there is no
      // extent to crop the output to.
      return null;
    }

    RasterizationParams params =
        calculateRasterizationParams(raster, useGeometryExtent, metadata, geomExtent, pixelType);

    fillBackground(params.writableRaster, backgroundValue);

    rasterizeGeometry(raster, metadata, geom, params, geomExtent, value, allTouched);

    if (useGeometryExtent && !params.anyBurned && !overlapsRasterArea(geom, rasterExtent)) {
      // The window can hold cells that the geometry only touches, for example the corner cell of a
      // polygon meeting the raster at that corner, or the edge column of a line along the
      // raster's right edge. Under GDAL's half-open rule it burns none of them and it overlaps no
      // pixel's area, so there is nothing to crop the output to either. A polygon that overlaps a
      // pixel without covering its centre keeps its cropped, unburned output.
      return null;
    }

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
        rasterizeLineString(geom, params, value, allTouched);
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
    params.burn(col, yIndex, value);
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
   * GridLines#enclosing} snaps extents with, so a coordinate on a grid line is not pushed across it
   * by floating-point error. Saturates at the int range for coordinates far off the raster.
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

  /**
   * Burns every cell a line passes through over a positive length. A segment lying exactly on a
   * grid line burns the half-open cells after the line, in the raster's own pixel order.
   *
   * <p>Without allTouched, a cell the line meets only at a point is not burned. With allTouched,
   * such a cell is burned when the point belongs to it under the half-open rule, as GDAL's
   * ALL_TOUCHED does: the cell after a grid corner that a segment crosses exactly, and the cell
   * containing a segment's first point in pixel order. See {@link GridLineWalker} for the exact
   * rule, for how a coordinate is matched to a grid line, and for how segments that leave the
   * raster are clipped.
   */
  private static void rasterizeLineString(
      Geometry geom, RasterizationParams params, double value, boolean allTouched) {
    GridLineWalker walker = params.lineWalker();
    GridLineWalker.CellVisitor burn = (col, row) -> params.burnRasterCell(col, row, value);
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Coordinate[] coords = geom.getGeometryN(i).getCoordinates();
      for (int j = 0; j < coords.length - 1; j++) {
        if (allTouched) {
          walker.walkTouched(coords[j].x, coords[j].y, coords[j + 1].x, coords[j + 1].y, burn);
        } else {
          walker.walk(
              coords[j].x, coords[j].y, coords[j + 1].x, coords[j + 1].y, false, false, burn);
        }
      }
    }
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

    if (geom.isEmpty()) {
      return null;
    }

    // The window is found on the raster's own grid, from its stored origin and signed pixel size:
    // the grid, and the decimal values, that also decide which cells a point or a line burns. A
    // bottom-up raster does not store its top edge. That is origin + height * scale in floating
    // point, and a row line located from there can be one rounding error away from the row line
    // GridLineWalker sees, which would leave the cells it burns outside the window.
    Envelope bounds = geom.getEnvelopeInternal();
    int width = (int) metadata[2];
    int height = (int) metadata[3];
    boolean bottomUp = metadata[5] > 0;
    GridLines columns =
        GridLines.enclosing(bounds.getMinX(), bounds.getMaxX(), metadata[0], metadata[4]);
    // Pixel rows grow with world y only on a bottom-up raster.
    GridLines rows =
        bottomUp
            ? GridLines.enclosing(bounds.getMinY(), bounds.getMaxY(), metadata[1], metadata[5])
            : GridLines.enclosing(bounds.getMaxY(), bounds.getMinY(), metadata[1], metadata[5]);

    // Ensure the snapped AOI window is at least one pixel wide/tall.
    // After snapping the continuous bbox to pixel edges, a thin line can
    // collapse to first == last along an axis (i.e., a degenerate 0-width/0-height window).
    // That would produce an empty search region and skip rasterization entirely.
    //
    // We grow the window by exactly one pixel on each side. This yields at least a
    // 1-pixel-wide/1-pixel-tall search window and guarantees we visit the pixel(s)
    // the geometry lies in.
    columns.growIfCollapsed();
    rows.growIfCollapsed();

    // Widen the window of a line where it has to hold cells beyond the snapped bounding box.
    //
    // This only sizes the search window and the cropped output; which cells a line burns is decided
    // by GridLineWalker under GDAL's half-open rule. A segment lying on a grid line burns the cells
    // after it, which sit outside the snapped window when that line is the window's last edge in
    // pixel order; with allTouched, so does a segment that starts on that line. The window of a
    // MultiLineString, and of a LineString with allTouched, is widened by one pixel on every side
    // that lies exactly on a grid line. The window of any other line is widened on a last edge
    // that one of its segments lies on. A polygon or point never burns a cell it touches only on
    // its boundary, as in GDAL, so its window is not widened.
    if (geom instanceof Lineal) {
      if (allTouched || geom instanceof MultiLineString) {
        columns.widenWhereOnLine();
        rows.widenWhereOnLine();
      } else {
        double lastY = bottomUp ? bounds.getMaxY() : bounds.getMinY();
        if (columns.lastOnLine && hasSegmentAlong(geom, true, bounds.getMaxX())) {
          columns.last++;
        }
        if (rows.lastOnLine && hasSegmentAlong(geom, false, lastY)) {
          rows.last++;
        }
      }
    }

    // Quick bbox intersection test for when rasterizeGeomExtent gets called independently
    // return null if they do not intersect
    if (columns.misses(width) || rows.misses(height)) {
      return null;
    }

    // The window in world coordinates. Its rows are counted down from the raster's geographic top,
    // as the output grid counts them.
    double upperLeftX = metadata[0];
    double upperLeftY = getRasterUpperLeftY(metadata);
    double scaleX = metadata[4];
    double scaleY = getRasterScaleY(metadata);
    long top = bottomUp ? height - rows.last : rows.first;
    long bottom = bottomUp ? height - rows.first : rows.last;
    double alignedMinX = toWorldCoordinate(columns.first, scaleX, upperLeftX);
    double alignedMaxX = toWorldCoordinate(columns.last, scaleX, upperLeftX);
    double alignedMaxY = toWorldCoordinate(top, scaleY, upperLeftY);
    double alignedMinY = toWorldCoordinate(bottom, scaleY, upperLeftY);

    // Clamp the aligned extent to the original raster extent
    alignedMinX = Math.max(alignedMinX, raster.getEnvelope().getMinimum(0));
    alignedMinY = Math.max(alignedMinY, raster.getEnvelope().getMinimum(1));
    alignedMaxX = Math.min(alignedMaxX, raster.getEnvelope().getMaximum(0));
    alignedMaxY = Math.min(alignedMaxY, raster.getEnvelope().getMaximum(1));

    // Create the aligned raster extent
    return new ReferencedEnvelope(
        alignedMinX,
        alignedMaxX,
        alignedMinY,
        alignedMaxY,
        raster.getCoordinateReferenceSystem2D());
  }

  /**
   * Whether a line has a segment, possibly one with no length, that lies on the vertical line x =
   * value, or on the horizontal line y = value.
   */
  private static boolean hasSegmentAlong(Geometry lineal, boolean vertical, double value) {
    for (int i = 0; i < lineal.getNumGeometries(); i++) {
      Coordinate[] coords = lineal.getGeometryN(i).getCoordinates();
      for (int j = 0; j < coords.length - 1; j++) {
        double a = vertical ? coords[j].x : coords[j].y;
        double b = vertical ? coords[j + 1].x : coords[j + 1].y;
        if (a == value && b == value) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * A run of grid lines along one axis of a raster, in its own pixel order: line 0 is at the
   * raster's stored origin and line k at origin + k * scale.
   */
  private static final class GridLines {
    // Lines far beyond any raster are folded onto these, which compare against a raster the same.
    private static final BigDecimal FAR = BigDecimal.valueOf(1L << 60);

    long first;
    long last;
    // Whether the coordinate each of the two lines was snapped from lies exactly on it.
    boolean firstOnLine;
    boolean lastOnLine;

    /**
     * The grid lines that enclose two world coordinates: the last line at or before {@code low} and
     * the first line at or after {@code high}, where {@code low} is the coordinate with the smaller
     * pixel coordinate. Decided on decimal values, as in {@link Rasterization#floorPixelIndex}.
     */
    static GridLines enclosing(double low, double high, double origin, double scale) {
      BigDecimal decimalOrigin = BigDecimal.valueOf(origin);
      BigDecimal decimalScale = BigDecimal.valueOf(scale);
      BigDecimal lowOffset = BigDecimal.valueOf(low).subtract(decimalOrigin);
      BigDecimal highOffset = BigDecimal.valueOf(high).subtract(decimalOrigin);
      BigDecimal before = lowOffset.divide(decimalScale, 0, RoundingMode.FLOOR);
      BigDecimal after = highOffset.divide(decimalScale, 0, RoundingMode.CEILING);
      GridLines lines = new GridLines();
      lines.first = before.max(FAR.negate()).min(FAR).longValue();
      lines.last = after.max(FAR.negate()).min(FAR).longValue();
      lines.firstOnLine = before.multiply(decimalScale).compareTo(lowOffset) == 0;
      lines.lastOnLine = after.multiply(decimalScale).compareTo(highOffset) == 0;
      return lines;
    }

    void growIfCollapsed() {
      if (first == last) {
        first--;
        last++;
        // Neither edge of the window is the line the coordinates lie on any more.
        firstOnLine = false;
        lastOnLine = false;
      }
    }

    /** Whether no cell of an axis with {@code size} cells lies between the two lines. */
    boolean misses(int size) {
      return first >= size || last <= 0;
    }

    void widenWhereOnLine() {
      if (firstOnLine) {
        first--;
      }
      if (lastOnLine) {
        last++;
      }
    }
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
        bottomUp,
        metadata);
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
    // The reference raster's grid in its own pixel order: its size, the world coordinates of its
    // first column and row lines, and its signed pixel size.
    int rasterWidth;
    int rasterHeight;
    double rasterOriginX;
    double rasterOriginY;
    double rasterPixelScaleX;
    double rasterPixelScaleY;
    // Where the output grid sits within the reference raster's grid, in cells from its left and
    // from its geographic top: zero unless the output is cropped to the geometry extent.
    int colOffset;
    int rowOffset;
    // Whether any cell has been burned; a cropped output with nothing burned may have nothing in
    // it.
    boolean anyBurned;
    private GridLineWalker lineWalker;

    /** The walker for line segments over the reference raster's grid. */
    GridLineWalker lineWalker() {
      if (lineWalker == null) {
        lineWalker =
            new GridLineWalker(
                rasterWidth,
                rasterHeight,
                rasterOriginX,
                rasterOriginY,
                rasterPixelScaleX,
                rasterPixelScaleY);
      }
      return lineWalker;
    }

    /**
     * Burns a cell given by its column and row on the reference raster, in the raster's own pixel
     * order, if the output grid holds it. The cell is resolved on the reference grid and only the
     * write index is local to the output, so a cropped output burns the same cells as a full-extent
     * one.
     */
    void burnRasterCell(int col, int row, double value) {
      // Count the row down from the geographic top, then move the cell onto the output grid.
      int rowFromTop = bottomUp ? rasterHeight - 1 - row : row;
      int x = col - colOffset;
      int y = rowFromTop - rowOffset;
      int height = writableRaster.getHeight();
      if (x < 0 || x >= writableRaster.getWidth() || y < 0 || y >= height) {
        return;
      }
      // Rows are written in the raster's own order; reverse them for bottom-up rasters.
      burn(x, bottomUp ? height - 1 - y : y, value);
    }

    /** Burns one cell of the output raster. */
    void burn(int x, int y, double value) {
      writableRaster.setSample(x, y, 0, value);
      anyBurned = true;
    }

    /** Burns an inclusive horizontal span, recording the burn once for the entire span. */
    void burnSpan(int xStart, int xEnd, int y, double value) {
      if (xStart > xEnd) {
        return;
      }
      for (int x = xStart; x <= xEnd; x++) {
        writableRaster.setSample(x, y, 0, value);
      }
      anyBurned = true;
    }

    RasterizationParams(
        WritableRaster writableRaster,
        String pixelType,
        double scaleX,
        double scaleY,
        double upperLeftX,
        double upperLeftY,
        double rasterUpperLeftX,
        double rasterUpperLeftY,
        boolean bottomUp,
        double[] metadata) {
      this.rasterWidth = (int) metadata[2];
      this.rasterHeight = (int) metadata[3];
      this.rasterOriginX = metadata[0];
      this.rasterOriginY = metadata[1];
      this.rasterPixelScaleX = metadata[4];
      this.rasterPixelScaleY = metadata[5];
      this.colOffset = (int) Math.round((upperLeftX - rasterUpperLeftX) / scaleX);
      this.rowOffset = (int) Math.round((upperLeftY - rasterUpperLeftY) / scaleY);
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

    // Using buffer(0) to avoid TopologyException : found non-noded intersection.
    Geometry valid = Functions.buffer(geom, 0);

    if (allTouched) {
      // The outline is walked along the polygon's own edges, which GridLineWalker clips to the
      // raster cell by cell. The copy clipped below gets new vertices where an edge crosses the
      // window, and those are rounded: an edge passing exactly through a grid corner would then
      // pass beside it, and burn a cell the polygon only touches there.
      burnOutlines(valid, params, value);
    }

    // Clip polygon to rasterExtent, for the fill. The outline is burned by now, so the clipped
    // parts are filled without allTouched.
    Geometry clippedGeom = Functions.intersection(JTS.toGeometry((BoundingBox) geomExtent), valid);

    if (allTouched) {
      // Where the polygon continues beyond the window, the clip closes it with edges along the
      // window's sides. The polygon overlaps the cells beside those edges, and the scanline fill
      // below can leave some of them out, for example when a vertex lies on a scanline at the
      // window's side.
      burnWindowEdges(clippedGeom, geomExtent, params, value);
    }

    if (clippedGeom instanceof MultiPolygon) {
      for (int i = 0; i < clippedGeom.getNumGeometries(); i++) {
        Geometry subGeom = clippedGeom.getGeometryN(i);
        rasterizePolygon(raster, metadata, subGeom, params, geomExtent, value, false);
      }
    } else if (clippedGeom instanceof Polygon) {
      Polygon polygon = (Polygon) clippedGeom;

      // Compute scanline X-intercepts
      Map<Double, TreeSet<Double>> scanlineIntersections =
          computeScanlineIntersections(polygon, params, geomExtent);

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
        rasterizeGeometry(raster, metadata, toRasterize, params, geomExtent, value, false);
      }
    }
  }

  /**
   * Whether the geometry's polygonal parts overlap the raster's area, rather than only touching its
   * boundary or lying outside it. Points and lines have no area, so they never do.
   */
  private static boolean overlapsRasterArea(Geometry geom, ReferencedEnvelope rasterExtent) {
    List<?> polygons = PolygonExtracter.getPolygons(geom);
    if (polygons.isEmpty()) {
      return false;
    }
    Geometry rasterArea = null;
    for (Object polygon : polygons) {
      Polygon polygonal = (Polygon) polygon;
      try {
        // A valid polygon with positive area wholly inside the raster must overlap its area. This
        // avoids constructing the raster polygon and relating it for unburned subpixel polygons.
        if (rasterExtent.contains(polygonal.getEnvelopeInternal())
            && polygonal.getArea() > 0
            && polygonal.isValid()) {
          return true;
        }
        if (rasterArea == null) {
          rasterArea = JTS.toGeometry((BoundingBox) rasterExtent);
        }
        if (polygonal.relate(rasterArea, "T********")) {
          return true;
        }
      } catch (TopologyException e) {
        // An invalid polygon cannot be related exactly. Assume it overlaps, which keeps the
        // cropped output as it was before rather than dropping it.
        return true;
      }
    }
    return false;
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
      Polygon polygon, RasterizationParams params, ReferencedEnvelope geomExtent) {

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

  /** Burns the outline of every polygon of a polygonal geometry, for allTouched. */
  private static void burnOutlines(Geometry polygonal, RasterizationParams params, double value) {
    for (int i = 0; i < polygonal.getNumGeometries(); i++) {
      Geometry part = polygonal.getGeometryN(i);
      if (!(part instanceof Polygon) || part.isEmpty()) {
        continue;
      }
      Polygon polygon = (Polygon) part;
      burnRingOutline(polygon.getExteriorRing().getCoordinates(), true, params, value);
      for (int r = 0; r < polygon.getNumInteriorRing(); r++) {
        burnRingOutline(polygon.getInteriorRingN(r).getCoordinates(), false, params, value);
      }
    }
  }

  /**
   * Burns the cells beside the edges of a clipped polygon that run along a side of the window it
   * was clipped to, for allTouched. Such an edge lies on a grid line, and the polygon is on the
   * window's side of it.
   */
  private static void burnWindowEdges(
      Geometry clipped, ReferencedEnvelope window, RasterizationParams params, double value) {
    GridLineWalker walker = params.lineWalker();
    GridLineWalker.CellVisitor burn = (col, row) -> params.burnRasterCell(col, row, value);
    for (Object polygon : PolygonExtracter.getPolygons(clipped)) {
      Polygon part = (Polygon) polygon;
      for (int r = -1; r < part.getNumInteriorRing(); r++) {
        Coordinate[] coords =
            (r < 0 ? part.getExteriorRing() : part.getInteriorRingN(r)).getCoordinates();
        for (int i = 0; i < coords.length - 1; i++) {
          Coordinate a = coords[i];
          Coordinate b = coords[i + 1];
          if (a.x == b.x && (a.x == window.getMinX() || a.x == window.getMaxX())) {
            walker.walk(a.x, a.y, b.x, b.y, a.x == window.getMaxX(), false, burn);
          } else if (a.y == b.y && (a.y == window.getMinY() || a.y == window.getMaxY())) {
            // Pixel y grows with world y only on a bottom-up raster.
            boolean windowTowardsPositiveWorldY = a.y == window.getMinY();
            walker.walk(
                a.x, a.y, b.x, b.y, false, windowTowardsPositiveWorldY != params.bottomUp, burn);
          }
        }
      }
    }
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
      Coordinate[] coords, boolean isShell, RasterizationParams params, double value) {
    if (coords.length < 4) {
      return;
    }
    // The polygon's interior is on the left of a counter-clockwise shell, and on the right of a
    // counter-clockwise hole.
    boolean interiorOnLeft = Orientation.isCCW(coords) == isShell;
    double side = interiorOnLeft ? 1 : -1;

    GridLineWalker walker = params.lineWalker();
    GridLineWalker.CellVisitor burn = (col, row) -> params.burnRasterCell(col, row, value);
    for (int i = 0; i < coords.length - 1; i++) {
      // The left normal of the direction of travel is (-dy, dx) in world space. Pixel x grows with
      // world x, and pixel y grows with world y only on a bottom-up raster.
      double worldDx = coords[i + 1].x - coords[i].x;
      double worldDy = coords[i + 1].y - coords[i].y;
      boolean flatTowardsNegativeX = -worldDy * side < 0;
      boolean interiorTowardsPositiveWorldY = worldDx * side > 0;
      boolean flatTowardsNegativeY = interiorTowardsPositiveWorldY != params.bottomUp;
      walker.walk(
          coords[i].x,
          coords[i].y,
          coords[i + 1].x,
          coords[i + 1].y,
          flatTowardsNegativeX,
          flatTowardsNegativeY,
          burn);
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
            params.burn(range[0], y, value);
          }
          continue;
        }
        int xStart = clampToGrid(range[0], width);
        int xEnd = clampToGrid(range[1], width);

        params.burnSpan(xStart, xEnd, y, value);
      }
    }
  }
}
