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

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.AffineTransform2D;
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
    RasterizationParams params =
        burn(
            geom,
            raster,
            pixelType,
            value,
            useGeometryExtent ? Extent.GEOMETRY : Extent.RASTER,
            allTouched,
            backgroundValue);
    if (params == null) {
      return null;
    }

    // Create a GridCoverage2D for the rasterized result, on the reference raster's grid. The rows
    // of a bottom-up raster were written in its own order, so the grid needs no flipping.
    GridCoverage2D rasterized =
        RasterUtils.create(
            params.writableRaster,
            outputGridGeometry(raster, params.grid, params, useGeometryExtent),
            null,
            null);

    // Return results compatible with the original function
    List<Object> objects = new ArrayList<>();
    objects.add(params.writableRaster);
    objects.add(rasterized);

    return objects;
  }

  /**
   * The pixels of a raster that a geometry selects, as a mask over the window of the raster that
   * holds them.
   *
   * <p>This is for a function that reads the raster's pixels under a geometry. The mask is as large
   * as the geometry, not as the raster, and it comes with its place in the raster in whole pixels.
   * The place of a raster cropped to the geometry would have to be found again from its
   * georeference, which is a rounded number for a grid line that is not a double.
   *
   * <p>The pixels are those the geometry burns on an output at the raster's full extent, see {@link
   * Extent#WINDOW}.
   */
  static final class Selection {
    /**
     * One sample per pixel of the window, non-zero where the geometry selects the pixel. Rows are
     * in the raster's own pixel order, and the first sample is at (0, 0).
     */
    final Raster mask;

    /** The column of the window's first pixel, counted from the raster's first pixel. */
    final int x;

    /** The row of the window's first pixel, counted from the raster's first pixel in its order. */
    final int y;

    private Selection(Raster mask, int x, int y) {
      this.mask = mask;
      this.x = x;
      this.y = y;
    }
  }

  /**
   * Selects the pixels of a raster under a geometry: the pixels that {@link #rasterize} burns.
   *
   * @param geom the geometry, in any CRS
   * @param raster the raster
   * @param allTouched whether to select every pixel the geometry touches
   * @return the selection, or null when the geometry selects no pixel and overlaps none
   * @throws IllegalArgumentException if the geometry does not intersect the raster
   */
  static Selection select(Geometry geom, GridCoverage2D raster, boolean allTouched)
      throws FactoryException {
    RasterizationParams params = burn(geom, raster, "B", 1, Extent.WINDOW, allTouched, null);
    if (params == null) {
      return null;
    }
    int height = params.writableRaster.getHeight();
    // Row offsets count from the raster's geographic top, and the rows of the mask are in the
    // raster's own order, as in outputGridGeometry.
    int firstRow =
        params.bottomUp ? params.rasterHeight - params.rowOffset - height : params.rowOffset;
    return new Selection(params.writableRaster, params.colOffset, firstRow);
  }

  /** What the band a geometry is burned into covers. */
  private enum Extent {
    /** The reference raster. */
    RASTER,

    /**
     * The geometry's window, for an output raster of its own: the band starts on grid lines that a
     * stored raster can start on, see {@link #startOnStorableLines}.
     */
    GEOMETRY,

    /**
     * The geometry's window as it is, for a mask that is placed in the reference raster by whole
     * pixels. The polygon fill is computed as for {@link #RASTER}, so the mask holds the pixels
     * that an output at full extent holds, see {@link RasterizationParams#fillInRasterFrame}.
     */
    WINDOW
  }

  /**
   * Burns a geometry into a new band on the grid of a raster.
   *
   * @param extent what the band covers
   * @return the band and its place in the raster, or null when the band would cover the geometry's
   *     window and the geometry neither burns nor overlaps any pixel
   */
  private static RasterizationParams burn(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      Extent extent,
      boolean allTouched,
      Double backgroundValue)
      throws FactoryException {
    boolean useGeometryExtent = extent != Extent.RASTER;

    // Validate the input geometry and raster metadata
    double[] rasterMetadata = RasterAccessors.metadata(raster);
    validateRasterMetadata(rasterMetadata);
    ReferenceGrid grid = ReferenceGrid.of(raster, rasterMetadata);
    geom = RasterUtils.transformToRasterCRS(raster, geom);
    Polygon outline = grid.outline(geom.getFactory());
    if (!outline.intersects(geom)) {
      throw new IllegalArgumentException("Geometry does not intersect Raster.");
    }

    ReferencedEnvelope rasterExtent =
        new ReferencedEnvelope(
            outline.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());

    ReferencedEnvelope geomExtent = rasterizeGeomExtent(geom, raster, grid, allTouched);
    if (geomExtent == null && useGeometryExtent) {
      // The geometry's window holds no cell of the raster, for example a point on the raster's
      // right or bottom edge or a polygon that only touches one of its edges, so there is no
      // extent to crop the output to.
      return null;
    }

    // The geometry is burned within its window. The output can start a cell before it.
    ReferencedEnvelope outputExtent = geomExtent;
    if (extent == Extent.GEOMETRY) {
      outputExtent = startOnStorableLines(geomExtent, grid);
    } else if (extent == Extent.WINDOW) {
      outputExtent = withColumnAfter(geomExtent, grid);
    }
    RasterizationParams params =
        calculateRasterizationParams(raster, useGeometryExtent, grid, outputExtent, pixelType);
    if (extent == Extent.WINDOW) {
      params.fillInRasterFrame();
    }

    fillBackground(params.writableRaster, backgroundValue);

    rasterizeGeometry(raster, grid, geom, params, geomExtent, value, allTouched);

    if (useGeometryExtent && !params.anyBurned && !overlapsRasterArea(geom, rasterExtent)) {
      // The window can hold cells that the geometry only touches, for example the corner cell of a
      // polygon meeting the raster at that corner, or the edge column of a line along the
      // raster's right edge. Under GDAL's half-open rule it burns none of them and it overlaps no
      // pixel's area, so there is nothing to crop the output to either. A polygon that overlaps a
      // pixel without covering its centre keeps its cropped, unburned output.
      return null;
    }
    return params;
  }

  /**
   * Whether a geometry reaches the raster, on the footprint {@link #rasterize} rejects a geometry
   * by: the outline of the grid that cells are located on, see {@link ReferenceGrid}. A function
   * that returns early for a geometry outside the raster and rasterizes the others has to decide
   * that with this test. The raster's convex hull is not the same rectangle before the raster is
   * stored: for a raster made with the corner 0.1 and a pixel size of 0.1 the hull starts at 0.1
   * and the outline at 0.10000000000000002. A point at 0.1 passed a test on the hull and was
   * rejected by the rasterizer, and the same point on the same raster read back from a shuffle
   * failed both.
   *
   * <p>The two are compared as {@link RasterPredicates#rsIntersects(GridCoverage2D, Geometry)}
   * compares them: as they are when the geometry is in the raster's CRS, which is then the test of
   * the rasterizer itself, and in WGS84 otherwise.
   *
   * @param raster the raster
   * @param geom the geometry, in any CRS
   * @return true if the geometry intersects the outline of the raster's grid
   */
  static boolean intersects(GridCoverage2D raster, Geometry geom) throws FactoryException {
    double[] rasterMetadata = RasterAccessors.metadata(raster);
    if (!canLocateCells(rasterMetadata)) {
      // No grid is read from such a raster, and rasterize rejects it for every geometry.
      return RasterPredicates.rsIntersects(raster, geom);
    }
    Polygon outline = ReferenceGrid.of(raster, rasterMetadata).outline(geom.getFactory());
    return RasterPredicates.intersects(outline, raster.getCoordinateReferenceSystem(), geom);
  }

  /**
   * The grid of the reference raster as cells are located on it: the origin and the pixel size
   * every function reads from the raster once it has been stored, with the origin moved to the
   * raster's first cell.
   *
   * <p><b>The stored origin.</b> Spark serializes a raster when it passes from one projection to
   * the next, but not between two functions nested in one expression, and which of the two happens
   * is the optimizer's choice. The serializer keeps the cell-centre transform, and the corner a
   * raster reports afterwards is derived from it in floating point, see {@link #storedCorner}. That
   * is not always the corner the raster was made with: a raster made with the corner 0.1 and a
   * pixel size of 0.1 reports 0.10000000000000002. On the decimal values that decide which cell a
   * coordinate is in, those are two grids. The coordinate 0.3 is on a row line of the first and
   * inside a row of the second. Cells are located on the second whether the raster has been stored
   * yet or not, so a geometry selects the same cells at every step of a query.
   *
   * <p><b>The first cell.</b> The grid range of a raster does not have to start at zero. A GeoTools
   * crop keeps the grid-to-world transform of its source and starts its range at the first cell it
   * keeps, so the origin the metadata report is the corner of grid cell (0, 0), while the first
   * cell of the raster's data is cell (range.x, range.y). Cells are counted from that first cell.
   * Its corner, origin + range * scale, is kept in decimal because it need not be a double. Rounded
   * to one, it would move every grid line of the raster by the rounding error, and a coordinate
   * that close to a grid line would select another cell than on the raster the crop was made from.
   */
  static final class ReferenceGrid {
    /** The raster's metadata, with the origin replaced by the doubles nearest to the two below. */
    final double[] metadata;

    /** World x of the first column line of the raster's first cell. */
    final BigDecimal originX;

    /** World y of the first row line, in pixel order, of the raster's first cell. */
    final BigDecimal originY;

    private ReferenceGrid(double[] metadata, BigDecimal originX, BigDecimal originY) {
      this.metadata = metadata;
      this.originX = originX;
      this.originY = originY;
    }

    /**
     * @param raster the reference raster
     * @param rasterMetadata its metadata, as {@link RasterAccessors#metadata} returns them
     */
    static ReferenceGrid of(GridCoverage2D raster, double[] rasterMetadata) {
      GridEnvelope2D range = raster.getGridGeometry().getGridRange2D();
      BigDecimal originX = firstLine(range.x, rasterMetadata[4], rasterMetadata[0]);
      BigDecimal originY = firstLine(range.y, rasterMetadata[5], rasterMetadata[1]);
      double[] metadata = rasterMetadata.clone();
      metadata[0] = originX.doubleValue();
      metadata[1] = originY.doubleValue();
      return new ReferenceGrid(metadata, originX, originY);
    }

    private static BigDecimal firstLine(int firstCell, double scale, double corner) {
      BigDecimal origin = BigDecimal.valueOf(storedCorner(corner, scale));
      return firstCell == 0 ? origin : gridLine(firstCell, scale, origin).stripTrailingZeros();
    }

    /**
     * The raster's rectangle as this grid places it, to tell whether a geometry reaches the raster
     * at all. The corners are computed as the raster's grid-to-world transform computes them, so
     * for a raster that reports the corner it was made with this is its convex hull.
     */
    Polygon outline(GeometryFactory factory) {
      double x0 = metadata[0];
      double y0 = metadata[1];
      double x1 = metadata[2] * metadata[4] + x0;
      double y1 = metadata[3] * metadata[5] + y0;
      return factory.createPolygon(
          new Coordinate[] {
            new Coordinate(x0, y0),
            new Coordinate(x1, y0),
            new Coordinate(x1, y1),
            new Coordinate(x0, y1),
            new Coordinate(x0, y0)
          });
    }
  }

  /**
   * The corner that a raster with the given corner and pixel size reports once it has been stored.
   * The serializer keeps the cell centre, corner + scale / 2, and the corner is read back from it
   * as centre - scale / 2, both rounded. A raster made with a corner that was read back this way
   * reports that corner again: in binary floating point with rounding to nearest, adding a number,
   * subtracting it and adding it again gives what the first addition gave, so the centre is the
   * same one.
   */
  static double storedCorner(double corner, double scale) {
    double half = scale / 2;
    return (corner + half) - half;
  }

  /**
   * The grid of the output raster, which is the reference raster's grid: its pixel size, and an
   * origin on one of its grid lines.
   *
   * <p>An output at full extent takes the reference raster's grid geometry as it is, when its grid
   * range starts at zero as the output's does. Stored, the two report the same origin, the one
   * cells were located from, see {@link ReferenceGrid}. A cropped output starts a whole number of
   * cells into the reference grid, and its origin is that grid line, computed in decimal as {@link
   * GridLineWalker} and {@link #floorPixelIndex} read the grid. See {@link #startOnStorableLines}
   * for the lines a cropped output starts on. The output for a reference raster whose grid range
   * does not start at zero is placed like a cropped one, at the raster's first cell.
   *
   * <p>When the reference raster's origin and pixel size are short decimals, so are its grid lines,
   * and the output's grid lines are the same numbers. Otherwise a grid line is not a double. The
   * output then starts at the double next to it, see {@link #firstCoordinateOfCell}, and all its
   * grid lines are less than a unit in the last place of that origin away from the reference
   * raster's. A coordinate that close to a grid line, such as a grid node computed from the origin
   * and the pixel size in floating point, can be in the neighbouring cell on the output. No origin
   * avoids that: a raster with a double origin cannot repeat the grid lines of another raster from
   * a line of it that is not a double. Only the output that takes the reference raster's grid
   * geometry is exact on such a grid.
   *
   * <p>A grid derived from the output's envelope, as (max - min) / cells, has a pixel size one
   * rounding error away from the reference raster's: 0.10000000000000002 for six rows of 0.1 down
   * from 0.7. A coordinate on a grid line of the reference raster is then beside that line on the
   * output, and a geometry rasterized onto the output, or counted on it, selects other cells than
   * the ones it burned.
   */
  private static GridGeometry2D outputGridGeometry(
      GridCoverage2D raster, ReferenceGrid grid, RasterizationParams params, boolean cropped) {
    double[] metadata = grid.metadata;
    GridGeometry2D referenceGrid = raster.getGridGeometry();
    GridEnvelope2D referenceRange = referenceGrid.getGridRange2D();
    if (!cropped && referenceRange.x == 0 && referenceRange.y == 0) {
      return referenceGrid;
    }
    int width = params.writableRaster.getWidth();
    int height = params.writableRaster.getHeight();
    // The offsets count cells from the reference raster's left and from its geographic top. Rows
    // are stored in the raster's own order, so the first row of a bottom-up output is the bottom
    // row of the window.
    int firstRow =
        params.bottomUp ? params.rasterHeight - params.rowOffset - height : params.rowOffset;
    AffineTransform2D gridToWorld =
        new AffineTransform2D(
            metadata[4],
            0,
            0,
            metadata[5],
            firstCoordinateOfCell(params.colOffset, metadata[4], grid.originX),
            firstCoordinateOfCell(firstRow, metadata[5], grid.originY));
    return new GridGeometry2D(
        new GridEnvelope2D(0, 0, width, height),
        PixelInCell.CELL_CORNER,
        gridToWorld,
        raster.getCoordinateReferenceSystem2D(),
        null);
  }

  /** The grid line origin + index * scale, in decimal. */
  private static BigDecimal gridLine(long index, double scale, BigDecimal origin) {
    return BigDecimal.valueOf(index).multiply(BigDecimal.valueOf(scale)).add(origin);
  }

  /**
   * The first coordinate, in pixel order, that belongs to a cell of the raster's grid: the grid
   * line origin + index * scale when a double has that decimal value, and otherwise the double next
   * to it on the cell's side.
   *
   * <p>With that double as its origin, the first grid line of a cropped output separates all
   * doubles exactly as the reference raster's grid line does. The double on the other side of the
   * line would itself be on the output's first grid line while it is before the line on the
   * reference raster, and it is often a coordinate that matters. A raster that reports the corner
   * 0.10000000000000002 for a pixel size of 0.1, as the one made with the corner 0.1 does once it
   * is serialized, has its grid line 1 at 0.20000000000000002. The nearest double is 0.2, which
   * lies before that line: as the origin it would put the coordinate 0.3 on the output's row line
   * 1, while on this raster 0.3 is inside row 1.
   */
  private static double firstCoordinateOfCell(long index, double scale, BigDecimal origin) {
    BigDecimal line = gridLine(index, scale, origin);
    double nearest = line.doubleValue();
    int side = BigDecimal.valueOf(nearest).compareTo(line);
    if (scale > 0 && side < 0) {
      return Math.nextUp(nearest);
    }
    if (scale < 0 && side > 0) {
      return Math.nextDown(nearest);
    }
    return nearest;
  }

  /**
   * Moves the first column line and the first row line of a window, in the raster's own pixel
   * order, back to lines that a stored raster can start on.
   *
   * <p>A cropped output starts on those two lines, and Spark serializes it when it passes from one
   * projection to the next. The serializer keeps the cell-centre transform, and the corner a raster
   * reports is derived from that in floating point, as centre - scale / 2. Not every corner comes
   * out of that: for a pixel size of 0.1 the centres 0.15 and 0.15000000000000002 give the corners
   * 0.09999999999999999 and 0.10000000000000002, and no centre gives 0.1. An output starting on
   * such a line would report another origin once it is stored, and a segment lying on one of the
   * reference raster's grid lines would then be beside the grid lines of the output. Whichever
   * neighbouring corner the output took instead, either such a segment or one that ends on a grid
   * line would select other cells on the output than it burned.
   *
   * <p>So the window starts a cell earlier instead. The output stays on the reference grid, with a
   * first row or column that the geometry does not reach.
   */
  private static ReferencedEnvelope startOnStorableLines(
      ReferencedEnvelope window, ReferenceGrid grid) {
    double[] metadata = grid.metadata;
    boolean bottomUp = metadata[5] > 0;
    double firstY = bottomUp ? window.getMinY() : window.getMaxY();
    long column = Math.round((window.getMinX() - metadata[0]) / metadata[4]);
    long row = Math.round((firstY - metadata[1]) / metadata[5]);
    long storableColumn = storableLine(column, metadata[4], grid.originX);
    long storableRow = storableLine(row, metadata[5], grid.originY);
    if (storableColumn == column && storableRow == row) {
      return window;
    }
    double minX =
        storableColumn == column
            ? window.getMinX()
            : toWorldCoordinate(storableColumn, metadata[4], metadata[0]);
    double y =
        storableRow == row ? firstY : toWorldCoordinate(storableRow, metadata[5], metadata[1]);
    return new ReferencedEnvelope(
        minX,
        window.getMaxX(),
        bottomUp ? y : window.getMinY(),
        bottomUp ? window.getMaxY() : y,
        window.getCoordinateReferenceSystem());
  }

  /**
   * Widens a window by the column after it, where the raster has one.
   *
   * <p>The polygon fill burns an unpaired x-intercept of a scanline as a single pixel, see {@link
   * #fillPolygon}. For a vertical edge on the window's last column line that pixel is in the column
   * after the window, outside the polygon. An output at full extent holds it, and a mask that has
   * to select the pixels of the full extent needs room for it.
   */
  private static ReferencedEnvelope withColumnAfter(ReferencedEnvelope window, ReferenceGrid grid) {
    double[] metadata = grid.metadata;
    long lastLine = Math.round((window.getMaxX() - metadata[0]) / metadata[4]);
    if (lastLine >= (long) metadata[2]) {
      return window;
    }
    return new ReferencedEnvelope(
        window.getMinX(),
        toWorldCoordinate(lastLine + 1, metadata[4], metadata[0]),
        window.getMinY(),
        window.getMaxY(),
        window.getCoordinateReferenceSystem());
  }

  /**
   * The last grid line at or before the given one, in pixel order, that a stored raster can start
   * on. A line that is not a double is left as it is: the raster's corner is not a short decimal
   * then, and no line of its grid is one. Line 0 is the raster's own origin.
   */
  private static long storableLine(long index, double scale, BigDecimal origin) {
    while (index > 0) {
      BigDecimal line = gridLine(index, scale, origin);
      double corner = line.doubleValue();
      boolean lineIsDouble = BigDecimal.valueOf(corner).compareTo(line) == 0;
      if (!lineIsDouble || storedCorner(corner, scale) == corner) {
        break;
      }
      index--;
    }
    return index;
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
      ReferenceGrid grid,
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
        rasterizeGeometryCollection(raster, grid, geom, params, value, allTouched);
        break;
      case "Point":
        rasterizePoint(geom, grid, params, value);
        break;
      case "LineString":
      case "MultiLineString":
      case "LinearRing":
        rasterizeLineString(geom, params, value, allTouched);
        break;
      default:
        rasterizePolygon(raster, grid, geom, params, geomExtent, value, allTouched);
        break;
    }
  }

  private static void rasterizeGeometryCollection(
      GridCoverage2D raster,
      ReferenceGrid grid,
      Geometry geom,
      RasterizationParams params,
      double value,
      boolean allTouched) {

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry subGeom = geom.getGeometryN(i);
      ReferencedEnvelope subGeomExtent = rasterizeGeomExtent(subGeom, raster, grid, allTouched);
      rasterizeGeometry(raster, grid, subGeom, params, subGeomExtent, value, allTouched);
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
      Geometry geom, ReferenceGrid grid, RasterizationParams params, double value) {
    if (geom.isEmpty()) {
      return;
    }
    int[] cell = halfOpenCell(geom.getCoordinate(), grid);
    if (cell == null) {
      return;
    }

    // Where the output grid sits within the reference raster's grid: zero unless the output was
    // cropped to the geometry extent. The cell is resolved on the reference grid and only the
    // write index is crop-local, so a cropped output burns the same cell as a full-extent one.
    int col = cell[0] - params.colOffset;
    int row = cell[1] - params.rowOffset;

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
  private static int[] halfOpenCell(Coordinate coord, ReferenceGrid grid) {
    double[] metadata = grid.metadata;
    int width = (int) metadata[2];
    int height = (int) metadata[3];
    int col = floorPixelIndex(coord.x, grid.originX, metadata[4]);
    int row = floorPixelIndex(coord.y, grid.originY, metadata[5]);
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
  private static int floorPixelIndex(double coord, BigDecimal upperLeft, double scale) {
    BigDecimal index =
        BigDecimal.valueOf(coord)
            .subtract(upperLeft)
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
    return rasterizeGeomExtent(geom, raster, ReferenceGrid.of(raster, metadata), allTouched);
  }

  private static ReferencedEnvelope rasterizeGeomExtent(
      Geometry geom, GridCoverage2D raster, ReferenceGrid grid, boolean allTouched) {
    double[] metadata = grid.metadata;

    // A point burns exactly the half-open cell it lies in, so its extent is that cell.
    if (geom instanceof Puntal) {
      return pointCellsExtent(geom, raster, grid);
    }
    // A mixed collection's window is the union of its members' windows, so each member keeps the
    // cells it burns.
    if (geom.getClass() == GeometryCollection.class) {
      ReferencedEnvelope union = null;
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        ReferencedEnvelope member =
            rasterizeGeomExtent(geom.getGeometryN(i), raster, grid, allTouched);
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

    // The window is found on the raster's own grid, from its origin and signed pixel size: the
    // grid, and the decimal values, that also decide which cells a point or a line burns. A
    // bottom-up raster does not store its top edge. That is origin + height * scale in floating
    // point, and a row line located from there can be one rounding error away from the row line
    // GridLineWalker sees, which would leave the cells it burns outside the window.
    Envelope bounds = geom.getEnvelopeInternal();
    int width = (int) metadata[2];
    int height = (int) metadata[3];
    boolean bottomUp = metadata[5] > 0;
    GridLines columns =
        GridLines.enclosing(bounds.getMinX(), bounds.getMaxX(), grid.originX, metadata[4]);
    // Pixel rows grow with world y only on a bottom-up raster.
    GridLines rows =
        bottomUp
            ? GridLines.enclosing(bounds.getMinY(), bounds.getMaxY(), grid.originY, metadata[5])
            : GridLines.enclosing(bounds.getMaxY(), bounds.getMinY(), grid.originY, metadata[5]);

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
        // Finding such a segment takes a pass over the line's coordinates. A line whose window
        // misses the raster even when widened is discarded before that, on its bounding box alone:
        // of a collection rasterized onto one tile, most members are such lines. The cells after a
        // last edge at or beyond the raster's far edge are off the raster as well, so that edge
        // needs no pass either.
        if (columns.missesEvenWidened(width) || rows.missesEvenWidened(height)) {
          return null;
        }
        double lastY = bottomUp ? bounds.getMaxY() : bounds.getMinY();
        if (columns.lastOnLine
            && columns.last < width
            && hasSegmentAlong(geom, true, bounds.getMaxX())) {
          columns.last++;
        }
        if (rows.lastOnLine && rows.last < height && hasSegmentAlong(geom, false, lastY)) {
          rows.last++;
        }
      }
    }

    // Quick bbox intersection test for when rasterizeGeomExtent gets called independently
    // return null if they do not intersect
    if (columns.misses(width) || rows.misses(height)) {
      return null;
    }

    // The window in world coordinates, kept within the raster. Its rows are counted down from the
    // raster's geographic top, as the output grid counts them. The edges of the raster are lines
    // of the same grid as the rest of the window. The raster's envelope is not used for them: it
    // starts at the corner the raster has in memory, which is not always the corner the grid
    // starts at, see ReferenceGrid.
    double upperLeftX = metadata[0];
    double upperLeftY = getRasterUpperLeftY(metadata);
    double scaleX = metadata[4];
    double scaleY = getRasterScaleY(metadata);
    long left = Math.max(columns.first, 0);
    long right = Math.min(columns.last, width);
    long top = Math.max(bottomUp ? height - rows.last : rows.first, 0);
    long bottom = Math.min(bottomUp ? height - rows.first : rows.last, height);
    double alignedMinX = toWorldCoordinate(left, scaleX, upperLeftX);
    double alignedMaxX = toWorldCoordinate(right, scaleX, upperLeftX);
    double alignedMaxY = toWorldCoordinate(top, scaleY, upperLeftY);
    double alignedMinY = toWorldCoordinate(bottom, scaleY, upperLeftY);

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
   * A run of grid lines along one axis of a raster, in its own pixel order: line 0 is at the origin
   * of the raster's {@link ReferenceGrid} and line k at origin + k * scale.
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
    static GridLines enclosing(double low, double high, BigDecimal decimalOrigin, double scale) {
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

    /** The same after the last line has moved one cell on, which is as far as a window widens. */
    boolean missesEvenWidened(int size) {
      return first >= size || last < 0;
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
      Geometry geom, GridCoverage2D raster, ReferenceGrid grid) {
    double[] metadata = grid.metadata;
    double upperLeftX = metadata[0];
    double upperLeftY = getRasterUpperLeftY(metadata);
    double scaleX = metadata[4];
    double scaleY = getRasterScaleY(metadata);

    Envelope cells = new Envelope();
    for (Coordinate coord : geom.getCoordinates()) {
      int[] cell = halfOpenCell(coord, grid);
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
      ReferenceGrid grid,
      ReferencedEnvelope geomExtent,
      String pixelType) {

    double[] metadata = grid.metadata;
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
        grid);
  }

  /** Whether {@link #validateRasterMetadata} accepts the raster. */
  private static boolean canLocateCells(double[] rasterMetadata) {
    return !(rasterMetadata[4] < 0 || rasterMetadata[6] != 0 || rasterMetadata[7] != 0);
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
    BigDecimal rasterOriginX;
    BigDecimal rasterOriginY;
    double rasterPixelScaleX;
    double rasterPixelScaleY;
    // Where the output grid sits within the reference raster's grid, in cells from its left and
    // from its geographic top: zero unless the output is cropped to the geometry extent.
    int colOffset;
    int rowOffset;
    // Whether any cell has been burned; a cropped output with nothing burned may have nothing in
    // it.
    boolean anyBurned;
    // The grid of the reference raster that cells are located on.
    final ReferenceGrid grid;
    // The frame the polygon fill computes its scanlines in: its origin is (upperLeftX,
    // upperLeftY), this is its size in pixels, and the band starts at this column and row of it,
    // the row counted from the frame's geographic top. The frame is the band itself unless
    // fillInRasterFrame has been called.
    int frameWidth;
    int frameHeight;
    int frameCol;
    int frameRow;
    private GridLineWalker lineWalker;

    /**
     * Makes the polygon fill compute in the frame of the whole reference raster, as it does for an
     * output at full extent, while the band stays the geometry's window.
     *
     * <p>The fill selects a pixel when its centre is between two x-intercepts of the polygon's
     * edges, and it computes an intercept in floating point from the origin of its frame. When a
     * pixel centre lies exactly on an edge, the intercept comes out a rounding error to one side or
     * the other, and which side depends on that origin. So the fill of one polygon can differ by
     * such a pixel between a band cropped to the geometry and a band at full extent. A mask that
     * has to select the pixels of the full extent is filled in its frame.
     */
    void fillInRasterFrame() {
      upperLeftX = rasterUpperLeftX;
      upperLeftY = rasterUpperLeftY;
      frameWidth = rasterWidth;
      frameHeight = rasterHeight;
      frameCol = colOffset;
      frameRow = rowOffset;
    }

    /**
     * Burns an inclusive span of a row of the fill's frame, as far as the band holds it. The row is
     * counted from the frame's geographic top.
     */
    void burnFrameSpan(int xStart, int xEnd, int rowFromTop, double value) {
      int height = writableRaster.getHeight();
      int row = rowFromTop - frameRow;
      if (row < 0 || row >= height) {
        return;
      }
      int first = Math.max(xStart - frameCol, 0);
      int last = Math.min(xEnd - frameCol, writableRaster.getWidth() - 1);
      // Rows are written in the raster's own order; reverse them for bottom-up rasters.
      burnSpan(first, last, bottomUp ? height - 1 - row : row, value);
    }

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
        ReferenceGrid grid) {
      double[] metadata = grid.metadata;
      this.rasterWidth = (int) metadata[2];
      this.rasterHeight = (int) metadata[3];
      this.rasterOriginX = grid.originX;
      this.rasterOriginY = grid.originY;
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
      this.grid = grid;
      this.frameWidth = writableRaster.getWidth();
      this.frameHeight = writableRaster.getHeight();
    }
  }

  private static void rasterizePolygon(
      GridCoverage2D raster,
      ReferenceGrid grid,
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
        rasterizePolygon(raster, grid, subGeom, params, geomExtent, value, false);
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
        rasterizeGeometry(raster, grid, toRasterize, params, geomExtent, value, false);
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
        yStart = Math.min((params.frameHeight - 0.5), Math.abs(yStart) - 0.5);

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
      // The scanline's row in the fill's frame, counted from its geographic top.
      int y = entry.getKey();
      int width = params.frameWidth;

      for (int[] range : entry.getValue()) {
        if (range.length == 1) {
          // A scanline with an odd number of intercepts leaves an unpaired one, burned as a single
          // pixel. Vertical edges keep their x-intercept unfiltered, so an edge touching only at
          // the extent maximum lands one past the grid. Discard it rather than folding it onto the
          // edge pixel: unlike the point path there is no per-cell intersection test here, so
          // folding would burn a pixel the polygon does not reach.
          if (range[0] >= 0 && range[0] < width) {
            params.burnFrameSpan(range[0], range[0], y, value);
          }
          continue;
        }
        int xStart = clampToGrid(range[0], width);
        int xEnd = clampToGrid(range[1], width);

        params.burnFrameSpan(xStart, xEnd, y, value);
      }
    }
  }
}
