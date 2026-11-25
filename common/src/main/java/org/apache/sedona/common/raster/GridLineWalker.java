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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Visits the cells of a raster grid that a line segment passes through.
 *
 * <p>The walk runs in the raster's own pixel order: cell (col, row) covers the half-open square
 * [col, col + 1) x [row, row + 1) of pixel space, where a world coordinate maps to the pixel
 * coordinate {@code (coordinate - origin) / scale}. A cell is visited when the segment overlaps it
 * over a positive length. A segment lying exactly on a grid line overlaps neither side, and takes
 * the half-open cell after the line.
 *
 * <p><b>Grid-boundary rule.</b> Whether a stored coordinate lies on a grid line is decided on
 * decimal values, the same ones the point and extent paths of {@link Rasterization} use: every
 * double is read as the shortest decimal that round-trips to it ({@link BigDecimal#valueOf(double)}
 * ), and the pixel coordinate is the exact quotient of those decimals. So {@code 0.3} on a grid
 * with origin {@code 0.6} and scale {@code -0.1} is exactly the grid line 3, although {@code (0.3 -
 * 0.6) / -0.1} is {@code 2.9999999999999996} in binary floating point. A coordinate off that
 * relation keeps its exact side, however close: {@code 0.30000000000000004}, the next double above
 * {@code 0.3} and also what {@code 0.1 + 2 * 0.1} evaluates to, lies past the line.
 *
 * <p><b>Clipping.</b> A segment that leaves the grid is never replaced by clipped coordinates,
 * which would have to be rounded. The walk finds the cell where the segment enters the grid and the
 * cell where it leaves, and steps between them by testing on which side of the original segment
 * each cell corner lies. Both the cell of a coordinate and the side of a corner are first computed
 * in double precision together with a bound on the error; only when the bound cannot settle the
 * answer, or the coordinates overflow, is it recomputed exactly in decimal arithmetic. Ordinary
 * segments therefore allocate nothing, and neither does a segment whose ends are both beyond the
 * same side of the grid, whatever its coordinates.
 *
 * <p>An instance holds the state of one walk at a time and is not thread-safe.
 */
final class GridLineWalker {

  /** Receives the cells a walk visits, as a column and a row in the raster's own pixel order. */
  @FunctionalInterface
  interface CellVisitor {
    void visit(int col, int row);
  }

  // Floors of exact pixel coordinates are clamped to this range. It is far beyond any grid, so a
  // clamped value still compares against the grid like the true one.
  private static final long FAR = 1L << 60;
  private static final BigDecimal FAR_DECIMAL = BigDecimal.valueOf(FAR);

  // The double-precision pixel coordinate (world - origin) / scale differs from the exact decimal
  // one for three reasons. The coordinate and the origin each stand for a decimal within half a
  // unit in the last place of themselves, and their difference is rounded once more: together at
  // most 1.5 units in the last place of each, and a unit in the last place of v is at most |v| *
  // 2^-52. Per unit of |world| + |origin| this allows 2 * 2^-52, to be divided by the scale. The
  // division and the scale's own decimal then add a relative error of less than 2^-52 of the
  // result, allowed for four times over.
  private static final double WORLD_ROUNDING = 0x1p-51;
  private static final double PIXEL_ROUNDING = 0x1p-50;
  // Slack for the rounding of the error bound itself.
  private static final double BOUND_SLACK = 1.0000001;
  // Bound, relative to the magnitude of its two products, on the rounding error of the corner
  // determinant: a few units in the last place, 2^-49 is sixteen.
  private static final double DETERMINANT_ROUNDING = 0x1p-49;

  /** One endpoint of the segment being walked. */
  private static final class End {
    // Whether the world and double-precision pixel coordinates are set, and whether the exact
    // floors are.
    boolean placed, resolved;
    // World coordinates.
    double wx, wy;
    // Pixel coordinates in double precision, and bounds on their distance from the exact ones.
    double px, py, ex, ey;
    // Floors of the exact pixel coordinates, and whether those are whole numbers.
    long fx, fy;
    boolean ix, iy;
    // Exact decimal world coordinates relative to the origin, computed on demand.
    BigDecimal nx, ny;
  }

  private final int width;
  private final int height;
  private final double originX;
  private final double originY;
  private final double scaleX;
  private final double scaleY;
  // Sign of scaleY. scaleX is always positive.
  private final int signY;
  // Parts of the bound on the distance between a double-precision pixel coordinate and the exact
  // decimal one; see WORLD_ROUNDING.
  private final double errorPerWorldX;
  private final double errorPerWorldY;
  private final double errorOfOriginX;
  private final double errorOfOriginY;

  // Exact decimal origin and scale, computed on demand.
  private BigDecimal decOriginX, decOriginY, decScaleX, decScaleY;

  private End tail = new End();
  private End head = new End();

  // Direction of the current segment in pixel space, from tail to head.
  private int stepX, stepY;
  private double dx, dy;
  // Per-segment parts of the error bound of the corner determinant; see orient.
  private double boundBase, boundPerRow, boundPerCol;
  // The bound for any corner between the two endpoints, which the corners of the walk are.
  private double walkBound;
  // Per-segment parts of the exact corner determinant, computed on demand; see exactOrient.
  private boolean exactTermsReady;
  private BigDecimal exactPerRow, exactPerCol, exactConstant;
  // The same three as whole numbers of one decimal unit, in 128 bits each, when they fit.
  private boolean exactTermsInt128;
  private long perRowHigh, perRowLow, perColHigh, perColLow, constantHigh, constantLow;

  // Results of the last floor computation and the last entry search.
  private boolean integral;
  private long entryCol, entryRow;
  private boolean entryClipped, entryThroughColumnLine, entryOnGridLine;

  /**
   * @param width number of columns of the grid
   * @param height number of rows of the grid
   * @param originX world x of the grid's first column line
   * @param originY world y of the grid's first row line
   * @param scaleX pixel width; positive
   * @param scaleY pixel height; negative for a north-up raster, positive for a bottom-up one
   */
  GridLineWalker(
      int width, int height, double originX, double originY, double scaleX, double scaleY) {
    this.width = width;
    this.height = height;
    this.originX = originX;
    this.originY = originY;
    this.scaleX = scaleX;
    this.scaleY = scaleY;
    this.signY = scaleY < 0 ? -1 : 1;
    this.errorPerWorldX = WORLD_ROUNDING / Math.abs(scaleX);
    this.errorPerWorldY = WORLD_ROUNDING / Math.abs(scaleY);
    // The MIN_VALUE terms cover coordinates and quotients too small for the relative bounds.
    this.errorOfOriginX =
        Math.abs(originX) * errorPerWorldX
            + 4 * Double.MIN_VALUE / Math.abs(scaleX)
            + Double.MIN_VALUE;
    this.errorOfOriginY =
        Math.abs(originY) * errorPerWorldY
            + 4 * Double.MIN_VALUE / Math.abs(scaleY)
            + Double.MIN_VALUE;
  }

  /**
   * Visits every cell of the grid that the segment from (x0, y0) to (x1, y1), in world coordinates,
   * overlaps over a positive length. A segment with no length visits the half-open cell containing
   * it. Reversing the segment visits the same cells.
   *
   * <p>A segment that runs along a grid line visits the half-open cells after the line, or the ones
   * before it when {@code flatTowardsNegativeX} (for a segment along a column line) or {@code
   * flatTowardsNegativeY} (along a row line) is set.
   *
   * <p>Segments with a NaN or infinite coordinate are skipped.
   */
  void walk(
      double x0,
      double y0,
      double x1,
      double y1,
      boolean flatTowardsNegativeX,
      boolean flatTowardsNegativeY,
      CellVisitor visitor) {
    walk(x0, y0, x1, y1, flatTowardsNegativeX, flatTowardsNegativeY, false, visitor);
  }

  /**
   * Visits every cell of the grid that the segment touches, as GDAL's ALL_TOUCHED does for lines.
   *
   * <p>These are the cells {@link #walk(double, double, double, double, boolean, boolean,
   * CellVisitor)} visits, and also the cells the segment meets at a single point when that point
   * belongs to them under the half-open rule. The ends of the segment are ordered by their pixel
   * coordinates: the first is the end with the smaller pixel x, or, for a segment along a column,
   * the end with the smaller pixel y. The part of the segment inside the grid is taken without its
   * last point; every other point of it selects the cell [col, col + 1) x [row, row + 1) containing
   * it. Beyond the cells the segment passes through, this adds:
   *
   * <ul>
   *   <li>where the segment crosses a grid corner exactly, the cell after that corner, which is one
   *       of the two cells the segment meets only there when it runs towards smaller rows;
   *   <li>the cell containing the first point, which differs from the cell the segment starts in
   *       when that point lies on a row line and the segment runs from it towards smaller rows.
   * </ul>
   *
   * <p>A part that is a single point is its own last point: a segment that meets the grid only at
   * one point of its edge visits nothing, even when that point is the segment's first.
   *
   * <p>This is the order of GDAL 3.11 and later. Earlier versions ordered the ends of a sloped
   * segment that stays within one column of cells by row instead, and so did not visit the cell
   * containing its first point. GDAL also computes the set in double precision with tolerances: it
   * treats a segment within 0.01 pixel of vertical or horizontal as exactly so, and trims 0.0001
   * pixel off the far end of such a segment. This walk has no tolerance, so it differs from GDAL
   * for those segments.
   */
  void walkTouched(double x0, double y0, double x1, double y1, CellVisitor visitor) {
    walk(x0, y0, x1, y1, false, false, true, visitor);
  }

  private void walk(
      double x0,
      double y0,
      double x1,
      double y1,
      boolean flatTowardsNegativeX,
      boolean flatTowardsNegativeY,
      boolean touched,
      CellVisitor visitor) {
    if (!(Double.isFinite(x0)
        && Double.isFinite(y0)
        && Double.isFinite(x1)
        && Double.isFinite(y1))) {
      return;
    }
    // Consecutive segments of a line share a vertex; keep what is already known about it.
    if (head.placed && head.wx == x0 && head.wy == y0) {
      End shared = head;
      head = tail;
      tail = shared;
    } else {
      place(tail, x0, y0);
    }
    place(head, x1, y1);
    if (beyondOneSideOfTheGrid()) {
      // Most segments of a geometry much larger than the raster end here, before anything exact
      // is computed for them.
      return;
    }
    resolve(tail);
    resolve(head);

    // Shortest decimals keep the order of the doubles they stand for, so comparing the doubles
    // gives the exact direction.
    stepX = x1 > x0 ? 1 : (x1 < x0 ? -1 : 0);
    stepY = (y1 > y0 ? 1 : (y1 < y0 ? -1 : 0)) * signY;
    dx = head.px - tail.px;
    dy = head.py - tail.py;
    double errX = tail.ex + head.ex;
    double errY = tail.ey + head.ey;
    boundBase =
        BOUND_SLACK
            * (Math.abs(dx) * tail.ey + Math.abs(dy) * tail.ex + errX * tail.ey + errY * tail.ex);
    boundPerRow = BOUND_SLACK * errX;
    boundPerCol = BOUND_SLACK * errY;
    // A corner of the walk is at most three pixels further from the tail than the head is, along
    // either axis: the walk runs between the cells next to the two endpoints.
    double spanX = Math.abs(dx) + 3;
    double spanY = Math.abs(dy) + 3;
    walkBound =
        BOUND_SLACK
            * (boundBase
                + spanY * boundPerRow
                + spanX * boundPerCol
                + DETERMINANT_ROUNDING * (Math.abs(dx) * spanY + Math.abs(dy) * spanX));
    exactTermsReady = false;

    boolean flatX = stepX == 0 && flatTowardsNegativeX;
    boolean flatY = stepY == 0 && flatTowardsNegativeY;

    // The cells holding the segment's interior next to each endpoint. Columns and rows change
    // monotonically along a segment, so it reaches the grid only if both ranges overlap it.
    long tailCol = interiorCell(tail.fx, tail.ix, stepX < 0 || flatX);
    long headCol = interiorCell(head.fx, head.ix, stepX > 0 || flatX);
    if (Math.max(tailCol, headCol) < 0 || Math.min(tailCol, headCol) >= width) {
      return;
    }
    long tailRow = interiorCell(tail.fy, tail.iy, stepY < 0 || flatY);
    long headRow = interiorCell(head.fy, head.iy, stepY > 0 || flatY);
    if (Math.max(tailRow, headRow) < 0 || Math.min(tailRow, headRow) >= height) {
      return;
    }

    // The first point of the segment in pixel order is its end with the smaller pixel x, or, for a
    // segment along a column, its end with the smaller pixel y.
    boolean firstIsHead = stepX != 0 ? stepX < 0 : stepY < 0;

    if (!findEntry(false, tailCol, tailRow)) {
      return;
    }
    int col = (int) entryCol;
    int row = (int) entryRow;
    boolean firstPointOnRowLine = !firstIsHead && entersOnRowLine(tail);
    if (!findEntry(true, headCol, headRow)) {
      return;
    }
    int endCol = (int) entryCol;
    int endRow = (int) entryRow;
    if (firstIsHead) {
      firstPointOnRowLine = entersOnRowLine(head);
    }

    if (touched && firstPointOnRowLine && (firstIsHead ? stepY > 0 : stepY < 0)) {
      // The first point of the part inside the grid lies on a row line, and the segment leaves it
      // towards smaller rows: the point belongs to the cell after the line, one row past the cell
      // the segment starts in.
      visitOnGrid(firstIsHead ? endCol : col, (firstIsHead ? endRow : row) + 1L, visitor);
    }

    // Both cells are on the grid and the walk between them is monotone, so it stays on the grid
    // and takes at most width + height steps. The bound only guards against an endless loop.
    long maxSteps = (long) width + height + 2;
    for (long i = 0; i <= maxSteps; i++) {
      visitor.visit(col, row);
      if (col == endCol && row == endRow) {
        break;
      }
      // The side of the segment on which the next cell corner in the direction of travel lies
      // tells which grid line the segment crosses first. A corner on the segment is crossed by
      // both lines at once: the walk steps diagonally, visiting only the two cells the segment
      // truly passes through.
      int order;
      if (stepX == 0) {
        order = 1;
      } else if (stepY == 0) {
        order = -1;
      } else {
        long cornerCol = stepX > 0 ? col + 1L : col;
        long cornerRow = stepY > 0 ? row + 1L : row;
        // Most corners are far enough from the segment for the determinant to exceed the bound
        // that holds along the whole walk. The others, and a determinant or bound that is not a
        // number, go through the bound of the corner itself and the exact form.
        double det = dx * (cornerRow - tail.py) - dy * (cornerCol - tail.px);
        int side = det > walkBound ? 1 : (det < -walkBound ? -1 : orient(cornerCol, cornerRow));
        order = -side * stepX * stepY;
        if (touched && order == 0 && stepX != stepY) {
          // The corner belongs to the cell after it. Running towards larger columns and rows that
          // is the next cell of the walk, and towards smaller ones the current cell; otherwise it
          // is a cell the segment meets only at this corner.
          visitOnGrid(cornerCol, cornerRow, visitor);
        }
      }
      if (order <= 0) {
        col += stepX;
      }
      if (order >= 0) {
        row += stepY;
      }
    }
  }

  private void visitOnGrid(long col, long row, CellVisitor visitor) {
    if (col >= 0 && col < width && row >= 0 && row < height) {
      visitor.visit((int) col, (int) row);
    }
  }

  /**
   * Whether the part of the segment inside the grid, travelled from the given endpoint as the last
   * entry search did, starts on a row line: at the endpoint itself when that is on the grid, or
   * where the segment comes in through a column line.
   */
  private boolean entersOnRowLine(End from) {
    return entryClipped ? entryThroughColumnLine && entryOnGridLine : from.iy;
  }

  /**
   * Finds the first cell of the grid that the segment's interior enters, travelling from the tail,
   * or from the head when {@code reversed}. {@code startCol} and {@code startRow} are the cell
   * holding the interior next to the endpoint the travel starts from.
   *
   * @return false when the segment's interior never enters the grid
   */
  private boolean findEntry(boolean reversed, long startCol, long startRow) {
    entryClipped = false;
    entryThroughColumnLine = false;
    entryOnGridLine = false;
    boolean needCol = startCol < 0 || startCol >= width;
    boolean needRow = startRow < 0 || startRow >= height;
    if (!needCol && !needRow) {
      entryCol = startCol;
      entryRow = startRow;
      return true;
    }
    entryClipped = true;
    int travelX = reversed ? -stepX : stepX;
    int travelY = reversed ? -stepY : stepY;
    // The grid lines through which the travel can enter the grid's columns and its rows. An
    // endpoint outside the columns moves towards them, since the column ranges overlap.
    long columnLine = travelX > 0 ? 0 : width;
    long rowLine = travelY > 0 ? 0 : height;

    boolean throughColumnLine;
    if (needCol && needRow) {
      // The travel is inside the grid only after crossing both lines, so it enters through the
      // one it crosses last.
      int order = crossingOrder(columnLine, rowLine);
      if (reversed) {
        order = -order;
      }
      if (order == 0) {
        // Through the grid's corner, into the corner cell.
        entryCol = travelX > 0 ? 0 : width - 1;
        entryRow = travelY > 0 ? 0 : height - 1;
        return true;
      }
      throughColumnLine = order > 0;
    } else {
      throughColumnLine = needCol;
    }

    entryThroughColumnLine = throughColumnLine;
    if (throughColumnLine) {
      long row = cellAfterCrossing(true, columnLine, travelY, startRow);
      if (row < 0 || row >= height) {
        // The segment passes the column line outside the grid's rows, or only at their corner.
        return false;
      }
      entryCol = travelX > 0 ? 0 : width - 1;
      entryRow = row;
    } else {
      long col = cellAfterCrossing(false, rowLine, travelX, startCol);
      if (col < 0 || col >= width) {
        return false;
      }
      entryCol = col;
      entryRow = travelY > 0 ? 0 : height - 1;
    }
    return true;
  }

  /**
   * The row (for a column line) or column (for a row line) of the cell that holds the segment's
   * interior right after the travel crosses the given grid line. Values below 0 and from the grid's
   * size up are only meaningful as "off the grid".
   *
   * @param columnLine whether {@code line} is a column line x = line, or a row line y = line
   * @param travel the travel's direction along the line: -1, 0 or 1
   * @param flatCell the result when the segment does not move along the line at all
   */
  private long cellAfterCrossing(boolean columnLine, long line, int travel, long flatCell) {
    if (travel == 0) {
      return flatCell;
    }
    int size = columnLine ? height : width;
    // sideOfCrossing(g) is the sign of g - c, where c is where the segment crosses the line.
    int first = sideOfCrossing(columnLine, line, 0);
    if (first > 0) {
      return -1;
    }
    if (first == 0) {
      entryOnGridLine = true;
      return travel > 0 ? 0 : -1;
    }
    int last = sideOfCrossing(columnLine, line, size);
    if (last < 0) {
      return size;
    }
    if (last == 0) {
      entryOnGridLine = true;
      return travel > 0 ? size : size - 1;
    }

    // 0 < c < size. Find lo with lo <= c < lo + 1, starting from the double-precision crossing and
    // bisecting if that estimate is off.
    long lo = 0;
    long hi = size;
    int sideOfLo = first;
    double estimate =
        columnLine
            ? tail.py + (line - tail.px) * (dy / dx)
            : tail.px + (line - tail.py) * (dx / dy);
    long guess = estimate > 0 && estimate < size ? (long) estimate : -1;
    if (guess == 0) {
      guess = 1;
    }
    int guided = 2;
    while (hi - lo > 1) {
      long probe = (guided > 0 && guess > lo && guess < hi) ? guess : (lo + hi) >>> 1;
      guided--;
      int side = sideOfCrossing(columnLine, line, probe);
      if (side <= 0) {
        lo = probe;
        sideOfLo = side;
        guess = probe + 1;
      } else {
        hi = probe;
        guess = probe - 1;
      }
    }
    if (sideOfLo == 0) {
      // The segment crosses the line exactly at a grid corner; the interior continues on the side
      // it travels to.
      entryOnGridLine = true;
      return travel > 0 ? lo : lo - 1;
    }
    return lo;
  }

  private int sideOfCrossing(boolean columnLine, long line, long g) {
    return columnLine ? orient(line, g) * stepX : -orient(g, line) * stepY;
  }

  /**
   * Which of the grid lines x = gx and y = gy the segment crosses first, travelling from the tail:
   * negative for the column line, positive for the row line, zero when it passes exactly through
   * their corner. Only meaningful for a segment that moves along both axes.
   */
  private int crossingOrder(long gx, long gy) {
    return -orient(gx, gy) * stepX * stepY;
  }

  /**
   * The cell holding the interior of the segment next to an endpoint, along one axis. Only a
   * coordinate exactly on a grid line is biased, to the cell before the line when the segment
   * extends that way; any other coordinate keeps its half-open floor.
   */
  private static long interiorCell(long floor, boolean onGridLine, boolean extendsNegative) {
    return onGridLine && extendsNegative ? floor - 1 : floor;
  }

  /** Sets an endpoint's world coordinates and its pixel coordinates in double precision. */
  private void place(End end, double wx, double wy) {
    end.placed = true;
    end.resolved = false;
    end.wx = wx;
    end.wy = wy;
    end.nx = null;
    end.ny = null;
    end.px = (wx - originX) / scaleX;
    end.ex = Math.abs(wx) * errorPerWorldX + errorOfOriginX + Math.abs(end.px) * PIXEL_ROUNDING;
    end.py = (wy - originY) / scaleY;
    end.ey = Math.abs(wy) * errorPerWorldY + errorOfOriginY + Math.abs(end.py) * PIXEL_ROUNDING;
  }

  /**
   * Finds the cell of an endpoint. This is exact, and for a coordinate on a grid line, or within
   * rounding error of one, it takes decimal arithmetic.
   */
  private void resolve(End end) {
    if (end.resolved) {
      return;
    }
    end.resolved = true;
    end.fx = floor(end.px, end.ex, end, true);
    end.ix = integral;
    end.fy = floor(end.py, end.ey, end, false);
    end.iy = integral;
  }

  /**
   * Whether both endpoints lie strictly before the grid's first column line, or strictly after its
   * last, or the same for its rows, so that the segment cannot reach a cell. The test is on the
   * double-precision pixel coordinates and their error bounds, and it is conservative: an endpoint
   * on one of those lines, or too close to one to tell, is left to the exact test that follows.
   */
  private boolean beyondOneSideOfTheGrid() {
    // Written so that a NaN or infinite coordinate or bound is left to the exact test.
    return (tail.px + tail.ex < 0 && head.px + head.ex < 0)
        || (tail.px - tail.ex > width && head.px - head.ex > width)
        || (tail.py + tail.ey < 0 && head.py + head.ey < 0)
        || (tail.py - tail.ey > height && head.py - head.ey > height);
  }

  /** Floor of the exact pixel coordinate; sets {@link #integral} to whether it is whole. */
  private long floor(double pixel, double error, End end, boolean xAxis) {
    // Written so that a NaN or infinite pixel or error falls through to the exact path.
    double nearest = Math.rint(pixel);
    if (Math.abs(pixel - nearest) > error) {
      integral = false;
      return (long) Math.floor(pixel);
    }
    BigDecimal numerator = xAxis ? numeratorX(end) : numeratorY(end);
    BigDecimal scale = xAxis ? decScaleX() : decScaleY();
    if (error < 0.25 && Math.abs(nearest) < 0x1p52) {
      // The exact coordinate is less than half a pixel from this whole number, so its side of that
      // number settles the floor without a division.
      long whole = (long) nearest;
      int side = numerator.compareTo(scale.multiply(BigDecimal.valueOf(whole))) * scale.signum();
      integral = side == 0;
      return side < 0 ? whole - 1 : whole;
    }
    BigDecimal floor = numerator.divide(scale, 0, RoundingMode.FLOOR);
    integral = floor.multiply(scale).compareTo(numerator) == 0;
    if (floor.compareTo(FAR_DECIMAL) >= 0) {
      return FAR;
    }
    if (floor.compareTo(FAR_DECIMAL.negate()) <= 0) {
      return -FAR;
    }
    return floor.longValueExact();
  }

  /**
   * The side of the segment, directed from tail to head, on which the grid corner (gx, gy) lies in
   * pixel space: the sign of {@code (head - tail) x (corner - tail)}, zero when the corner is on
   * the segment's line.
   */
  private int orient(long gx, long gy) {
    double a = gy - tail.py;
    double b = gx - tail.px;
    double p = dx * a;
    double q = dy * b;
    double det = p - q;
    // The effect of the endpoints' errors on det: with dx off by errX, a by tail.ey, dy by errY
    // and b by tail.ex, it is |dx| * tail.ey + |a| * errX + |dy| * tail.ex + |b| * errY plus the
    // second-order terms. The parts that do not depend on the corner are in boundBase. The last
    // term covers the rounding of det itself.
    double bound =
        boundBase
            + Math.abs(a) * boundPerRow
            + Math.abs(b) * boundPerCol
            + DETERMINANT_ROUNDING * (Math.abs(p) + Math.abs(q));
    // Written so that a NaN or infinite determinant or bound falls through to the exact path.
    if (Math.abs(det) > bound) {
      return det > 0 ? 1 : -1;
    }
    return exactOrient(gx, gy);
  }

  private int exactOrient(long gx, long gy) {
    // With X and Y the decimal world coordinates relative to the origin, the determinant in pixel
    // space is
    //   ((X1 - X0) * (gy * scaleY - Y0) - (Y1 - Y0) * (gx * scaleX - X0)) / (scaleX * scaleY)
    // and scaleX is positive. The products that do not depend on the corner are kept per segment.
    if (!exactTermsReady) {
      BigDecimal x0 = numeratorX(tail);
      BigDecimal y0 = numeratorY(tail);
      BigDecimal deltaX = numeratorX(head).subtract(x0);
      BigDecimal deltaY = numeratorY(head).subtract(y0);
      exactPerRow = deltaX.multiply(decScaleY());
      exactPerCol = deltaY.multiply(decScaleX());
      exactConstant = deltaY.multiply(x0).subtract(deltaX.multiply(y0));
      exactTermsReady = true;
      prepareInt128Terms();
    }
    if (exactTermsInt128 && gx >= 0 && gx <= width && gy >= 0 && gy <= height) {
      return signOfInt128Combination(
              perRowHigh, perRowLow, gy, perColHigh, perColLow, gx, constantHigh, constantLow)
          * signY;
    }
    return exactPerRow
            .multiply(BigDecimal.valueOf(gy))
            .subtract(exactPerCol.multiply(BigDecimal.valueOf(gx)))
            .add(exactConstant)
            .signum()
        * signY;
  }

  /**
   * Brings the three terms of the exact corner determinant to one decimal scale and keeps them as
   * 128-bit whole numbers, when the determinant of every corner of the grid fits in 128 bits. A
   * segment that passes within rounding error of many corners then decides each of them without
   * allocating. Coordinates and pixel sizes of ordinary precision fit; the decimal form remains for
   * the ones that do not.
   */
  private void prepareInt128Terms() {
    int scale = Math.max(exactPerRow.scale(), Math.max(exactPerCol.scale(), exactConstant.scale()));
    BigInteger perRow = exactPerRow.setScale(scale).unscaledValue();
    BigInteger perCol = exactPerCol.setScale(scale).unscaledValue();
    BigInteger constant = exactConstant.setScale(scale).unscaledValue();
    BigInteger largest =
        perRow
            .abs()
            .multiply(BigInteger.valueOf(height))
            .add(perCol.abs().multiply(BigInteger.valueOf(width)))
            .add(constant.abs());
    exactTermsInt128 = largest.bitLength() <= 126;
    if (exactTermsInt128) {
      perRowHigh = perRow.shiftRight(64).longValue();
      perRowLow = perRow.longValue();
      perColHigh = perCol.shiftRight(64).longValue();
      perColLow = perCol.longValue();
      constantHigh = constant.shiftRight(64).longValue();
      constantLow = constant.longValue();
    }
  }

  /**
   * The sign of {@code u * m - v * n + w} for three 128-bit two's complement numbers, each given as
   * its high and low 64 bits, and two multipliers from 0 to 2^31. The result must fit in 128 bits,
   * which the caller ensures.
   */
  static int signOfInt128Combination(
      long uHigh, long uLow, long m, long vHigh, long vLow, long n, long wHigh, long wLow) {
    long low = uLow * m;
    long high = uHigh * m + unsignedMultiplyHigh(uLow, m);
    long subtractedLow = vLow * n;
    long subtractedHigh = vHigh * n + unsignedMultiplyHigh(vLow, n);
    long differenceLow = low - subtractedLow;
    long differenceHigh =
        high - subtractedHigh - (Long.compareUnsigned(low, subtractedLow) < 0 ? 1 : 0);
    long sumLow = differenceLow + wLow;
    long sumHigh =
        differenceHigh + wHigh + (Long.compareUnsigned(sumLow, differenceLow) < 0 ? 1 : 0);
    if (sumHigh != 0) {
      return sumHigh < 0 ? -1 : 1;
    }
    return sumLow != 0 ? 1 : 0;
  }

  /** The high 64 bits of an unsigned 64-bit value times a multiplier that is not negative. */
  private static long unsignedMultiplyHigh(long unsigned, long multiplier) {
    return Math.multiplyHigh(unsigned, multiplier) + (unsigned < 0 ? multiplier : 0);
  }

  private BigDecimal numeratorX(End end) {
    if (end.nx == null) {
      if (decOriginX == null) {
        decOriginX = BigDecimal.valueOf(originX);
      }
      end.nx = BigDecimal.valueOf(end.wx).subtract(decOriginX);
    }
    return end.nx;
  }

  private BigDecimal numeratorY(End end) {
    if (end.ny == null) {
      if (decOriginY == null) {
        decOriginY = BigDecimal.valueOf(originY);
      }
      end.ny = BigDecimal.valueOf(end.wy).subtract(decOriginY);
    }
    return end.ny;
  }

  private BigDecimal decScaleX() {
    if (decScaleX == null) {
      decScaleX = BigDecimal.valueOf(scaleX);
    }
    return decScaleX;
  }

  private BigDecimal decScaleY() {
    if (decScaleY == null) {
      decScaleY = BigDecimal.valueOf(scaleY);
    }
    return decScaleY;
  }
}
