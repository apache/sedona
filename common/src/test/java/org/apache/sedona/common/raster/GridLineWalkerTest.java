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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.junit.Assume;
import org.junit.Test;

public class GridLineWalkerTest {

  /** A raster grid: its size, the world coordinates of its first lines, and its pixel size. */
  private static final class Grid {
    final int width;
    final int height;
    final double originX;
    final double originY;
    final double scaleX;
    final double scaleY;

    Grid(int width, int height, double originX, double originY, double scaleX, double scaleY) {
      this.width = width;
      this.height = height;
      this.originX = originX;
      this.originY = originY;
      this.scaleX = scaleX;
      this.scaleY = scaleY;
    }

    GridLineWalker walker() {
      return new GridLineWalker(width, height, originX, originY, scaleX, scaleY);
    }

    /** The double nearest to the world x of the column line (or fractional column) {@code col}. */
    double worldX(double col) {
      return world(originX, scaleX, col);
    }

    double worldY(double row) {
      return world(originY, scaleY, row);
    }

    private static double world(double origin, double scale, double pixel) {
      return BigDecimal.valueOf(origin)
          .add(BigDecimal.valueOf(pixel).multiply(BigDecimal.valueOf(scale)))
          .doubleValue();
    }

    @Override
    public String toString() {
      return String.format(
          "grid %dx%d origin (%s, %s) scale (%s, %s)",
          width, height, originX, originY, scaleX, scaleY);
    }
  }

  /** An exact fraction of two decimals, for the oracle. */
  private static final class Fraction implements Comparable<Fraction> {
    static final Fraction ZERO = new Fraction(BigDecimal.ZERO, BigDecimal.ONE);
    static final Fraction ONE = new Fraction(BigDecimal.ONE, BigDecimal.ONE);

    final BigDecimal numerator;
    final BigDecimal denominator;

    Fraction(BigDecimal numerator, BigDecimal denominator) {
      if (denominator.signum() < 0) {
        numerator = numerator.negate();
        denominator = denominator.negate();
      }
      this.numerator = numerator;
      this.denominator = denominator;
    }

    @Override
    public int compareTo(Fraction other) {
      return numerator.multiply(other.denominator).compareTo(other.numerator.multiply(denominator));
    }

    static Fraction max(Fraction a, Fraction b) {
      return a.compareTo(b) >= 0 ? a : b;
    }

    static Fraction min(Fraction a, Fraction b) {
      return a.compareTo(b) <= 0 ? a : b;
    }
  }

  private static BigDecimal dec(double value) {
    return BigDecimal.valueOf(value);
  }

  /**
   * The range of the segment's parameter over which one pixel coordinate lies in [cell, cell + 1],
   * or null when it never does. The pixel coordinate at parameter t is (n0 + t * (n1 - n0)) /
   * scale. A coordinate that does not change is in the cell under the half-open rule, [cell, cell +
   * 1), or (cell, cell + 1] when {@code flatTowardsNegative}.
   */
  private static Fraction[] parameterRange(
      BigDecimal n0, BigDecimal n1, BigDecimal scale, int cell, boolean flatTowardsNegative) {
    BigDecimal low = BigDecimal.valueOf(cell).multiply(scale);
    BigDecimal high = BigDecimal.valueOf(cell + 1L).multiply(scale);
    BigDecimal delta = n1.subtract(n0);
    if (delta.signum() == 0) {
      // Compare the pixel coordinate n0 / scale with the cell's two lines.
      int vsLow = new Fraction(n0, scale).compareTo(new Fraction(low, scale));
      int vsHigh = new Fraction(n0, scale).compareTo(new Fraction(high, scale));
      boolean inside =
          flatTowardsNegative ? (vsLow > 0 && vsHigh <= 0) : (vsLow >= 0 && vsHigh < 0);
      return inside ? new Fraction[] {Fraction.ZERO, Fraction.ONE} : null;
    }
    Fraction a = new Fraction(low.subtract(n0), delta);
    Fraction b = new Fraction(high.subtract(n0), delta);
    return new Fraction[] {Fraction.min(a, b), Fraction.max(a, b)};
  }

  /**
   * The cells the segment overlaps over a positive length, or, for a segment with no length, the
   * half-open cell containing it. Decided cell by cell in exact decimal arithmetic, clipping the
   * segment to each cell independently of the walker.
   */
  private static TreeSet<Integer> oracle(
      Grid grid,
      double x0,
      double y0,
      double x1,
      double y1,
      boolean flatTowardsNegativeX,
      boolean flatTowardsNegativeY) {
    BigDecimal scaleX = dec(grid.scaleX);
    BigDecimal scaleY = dec(grid.scaleY);
    BigDecimal nx0 = dec(x0).subtract(dec(grid.originX));
    BigDecimal nx1 = dec(x1).subtract(dec(grid.originX));
    BigDecimal ny0 = dec(y0).subtract(dec(grid.originY));
    BigDecimal ny1 = dec(y1).subtract(dec(grid.originY));
    TreeSet<Integer> cells = new TreeSet<>();
    for (int row = 0; row < grid.height; row++) {
      Fraction[] rangeY = parameterRange(ny0, ny1, scaleY, row, flatTowardsNegativeY);
      if (rangeY == null) {
        continue;
      }
      for (int col = 0; col < grid.width; col++) {
        Fraction[] rangeX = parameterRange(nx0, nx1, scaleX, col, flatTowardsNegativeX);
        if (rangeX == null) {
          continue;
        }
        Fraction from = Fraction.max(Fraction.ZERO, Fraction.max(rangeX[0], rangeY[0]));
        Fraction to = Fraction.min(Fraction.ONE, Fraction.min(rangeX[1], rangeY[1]));
        if (from.compareTo(to) < 0) {
          cells.add(row * grid.width + col);
        }
      }
    }
    return cells;
  }

  private static TreeSet<Integer> walk(
      GridLineWalker walker,
      Grid grid,
      double x0,
      double y0,
      double x1,
      double y1,
      boolean flatTowardsNegativeX,
      boolean flatTowardsNegativeY) {
    TreeSet<Integer> cells = new TreeSet<>();
    walker.walk(
        x0,
        y0,
        x1,
        y1,
        flatTowardsNegativeX,
        flatTowardsNegativeY,
        (col, row) -> {
          assertTrue("column on the grid", col >= 0 && col < grid.width);
          assertTrue("row on the grid", row >= 0 && row < grid.height);
          assertTrue("cell visited once", cells.add(row * grid.width + col));
        });
    return cells;
  }

  private static void assertMatchesOracle(
      Grid grid, double x0, double y0, double x1, double y1, boolean flatX, boolean flatY) {
    String segment = String.format("%s, segment (%s %s, %s %s)", grid, x0, y0, x1, y1);
    TreeSet<Integer> expected = oracle(grid, x0, y0, x1, y1, flatX, flatY);
    GridLineWalker walker = grid.walker();
    assertEquals(segment, expected, walk(walker, grid, x0, y0, x1, y1, flatX, flatY));
    assertEquals(segment + " reversed", expected, walk(walker, grid, x1, y1, x0, y0, flatX, flatY));
  }

  private static String cells(Grid grid, double x0, double y0, double x1, double y1) {
    return format(grid, walk(grid.walker(), grid, x0, y0, x1, y1, false, false));
  }

  private static String format(Grid grid, TreeSet<Integer> visited) {
    List<String> cells = new ArrayList<>();
    for (int cell : visited) {
      cells.add("(" + cell % grid.width + "," + cell / grid.width + ")");
    }
    return cells.toString();
  }

  /** A range of the segment's parameter, each end of which is open or closed. */
  private static final class Span {
    static final Span ALL = new Span(Fraction.ZERO, false, Fraction.ONE, false);

    final Fraction from;
    final boolean fromOpen;
    final Fraction to;
    final boolean toOpen;

    Span(Fraction from, boolean fromOpen, Fraction to, boolean toOpen) {
      this.from = from;
      this.fromOpen = fromOpen;
      this.to = to;
      this.toOpen = toOpen;
    }

    boolean isEmpty() {
      int order = from.compareTo(to);
      return order > 0 || (order == 0 && (fromOpen || toOpen));
    }

    Span intersect(Span other) {
      int startOrder = from.compareTo(other.from);
      Fraction newFrom = startOrder >= 0 ? from : other.from;
      boolean newFromOpen =
          startOrder > 0
              ? fromOpen
              : (startOrder < 0 ? other.fromOpen : fromOpen || other.fromOpen);
      int endOrder = to.compareTo(other.to);
      Fraction newTo = endOrder <= 0 ? to : other.to;
      boolean newToOpen =
          endOrder < 0 ? toOpen : (endOrder > 0 ? other.toOpen : toOpen || other.toOpen);
      return new Span(newFrom, newFromOpen, newTo, newToOpen);
    }
  }

  /**
   * The range of the segment's parameter over which one pixel coordinate, (n0 + t * (n1 - n0)) /
   * scale, lies in [low, high), or in [low, high] when {@code closed}; null when it never does.
   */
  private static Span parameterSpan(
      BigDecimal n0, BigDecimal n1, BigDecimal scale, long low, long high, boolean closed) {
    BigDecimal lowN = BigDecimal.valueOf(low).multiply(scale);
    BigDecimal highN = BigDecimal.valueOf(high).multiply(scale);
    BigDecimal delta = n1.subtract(n0);
    if (delta.signum() == 0) {
      int vsLow = new Fraction(n0, scale).compareTo(new Fraction(lowN, scale));
      int vsHigh = new Fraction(n0, scale).compareTo(new Fraction(highN, scale));
      return vsLow >= 0 && (closed ? vsHigh <= 0 : vsHigh < 0) ? Span.ALL : null;
    }
    Fraction atLow = new Fraction(lowN.subtract(n0), delta);
    Fraction atHigh = new Fraction(highN.subtract(n0), delta);
    boolean increasing = delta.signum() == scale.signum();
    return increasing
        ? new Span(atLow, false, atHigh, !closed)
        : new Span(atHigh, !closed, atLow, false);
  }

  /**
   * The cells the segment touches as GDAL's ALL_TOUCHED defines it, decided cell by cell in exact
   * decimal arithmetic: the part of the segment inside the closed grid, without its last point in
   * pixel order, selects every half-open cell that contains one of its points.
   */
  private static TreeSet<Integer> touchedOracle(
      Grid grid, double x0, double y0, double x1, double y1) {
    BigDecimal scaleX = dec(grid.scaleX);
    BigDecimal scaleY = dec(grid.scaleY);
    BigDecimal nx0 = dec(x0).subtract(dec(grid.originX));
    BigDecimal nx1 = dec(x1).subtract(dec(grid.originX));
    BigDecimal ny0 = dec(y0).subtract(dec(grid.originY));
    BigDecimal ny1 = dec(y1).subtract(dec(grid.originY));
    int stepX = nx1.subtract(nx0).signum() * scaleX.signum();
    int stepY = ny1.subtract(ny0).signum() * scaleY.signum();
    if (stepX == 0 && stepY == 0) {
      return oracle(grid, x0, y0, x1, y1, false, false);
    }
    TreeSet<Integer> cells = new TreeSet<>();
    Span insideX = parameterSpan(nx0, nx1, scaleX, 0, grid.width, true);
    Span insideY = parameterSpan(ny0, ny1, scaleY, 0, grid.height, true);
    if (insideX == null || insideY == null) {
      return cells;
    }
    Span inside = Span.ALL.intersect(insideX).intersect(insideY);
    if (inside.isEmpty() || inside.from.compareTo(inside.to) == 0) {
      // Outside the grid, or meeting it at a single point.
      return cells;
    }
    // The first end in pixel order: the one with the smaller pixel x, or, for a segment along a
    // column, the one with the smaller pixel y.
    boolean firstIsTail = stepX != 0 ? stepX > 0 : stepY > 0;
    Span part =
        firstIsTail
            ? new Span(inside.from, false, inside.to, true)
            : new Span(inside.from, true, inside.to, false);
    for (int row = 0; row < grid.height; row++) {
      Span spanY = parameterSpan(ny0, ny1, scaleY, row, row + 1L, false);
      if (spanY == null) {
        continue;
      }
      for (int col = 0; col < grid.width; col++) {
        Span spanX = parameterSpan(nx0, nx1, scaleX, col, col + 1L, false);
        if (spanX != null && !part.intersect(spanX).intersect(spanY).isEmpty()) {
          cells.add(row * grid.width + col);
        }
      }
    }
    return cells;
  }

  private static TreeSet<Integer> walkTouched(
      GridLineWalker walker, Grid grid, double x0, double y0, double x1, double y1) {
    TreeSet<Integer> cells = new TreeSet<>();
    walker.walkTouched(
        x0,
        y0,
        x1,
        y1,
        (col, row) -> {
          assertTrue("column on the grid", col >= 0 && col < grid.width);
          assertTrue("row on the grid", row >= 0 && row < grid.height);
          assertTrue("cell visited once", cells.add(row * grid.width + col));
        });
    return cells;
  }

  private static String touchedCells(Grid grid, double x0, double y0, double x1, double y1) {
    TreeSet<Integer> cells = walkTouched(grid.walker(), grid, x0, y0, x1, y1);
    assertEquals("reversed", cells, walkTouched(grid.walker(), grid, x1, y1, x0, y0));
    return format(grid, cells);
  }

  private static void assertTouchedMatchesOracle(
      Grid grid, double x0, double y0, double x1, double y1) {
    String segment = String.format("%s, touched segment (%s %s, %s %s)", grid, x0, y0, x1, y1);
    TreeSet<Integer> expected = touchedOracle(grid, x0, y0, x1, y1);
    GridLineWalker walker = grid.walker();
    assertEquals(segment, expected, walkTouched(walker, grid, x0, y0, x1, y1));
    assertEquals(segment + " reversed", expected, walkTouched(walker, grid, x1, y1, x0, y0));
    // Every cell the segment passes through is among the cells it touches.
    assertTrue(segment, expected.containsAll(oracle(grid, x0, y0, x1, y1, false, false)));
  }

  private static final Grid UNIT = new Grid(6, 6, 0, 6, 1, -1);

  @Test
  public void segmentCrossingTheGridWithBothEndpointsOutside() {
    // GH-3255: these used to burn nothing, because a segment whose endpoints were both outside the
    // raster was taken to miss it.
    assertEquals("[(0,3), (1,3), (2,3), (3,3), (4,3), (5,3)]", cells(UNIT, -1, 2.5, 7, 2.5));
    assertEquals("[(2,0), (2,1), (2,2), (2,3), (2,4), (2,5)]", cells(UNIT, 2.5, 9, 2.5, -3));
    assertEquals("[(5,0), (4,1), (3,2), (2,3), (1,4), (0,5)]", cells(UNIT, -1, -1, 7, 7));
    // Through one corner region only.
    assertEquals("[(0,0), (1,0), (0,1)]", cells(UNIT, -1, 3.5, 2.5, 7));
    // The same segments are found from either end.
    assertEquals(cells(UNIT, -1, 2.5, 7, 2.5), cells(UNIT, 7, 2.5, -1, 2.5));
    assertEquals(cells(UNIT, -1, 3.5, 2.5, 7), cells(UNIT, 2.5, 7, -1, 3.5));
  }

  @Test
  public void segmentMissingTheGridBurnsNothing() {
    // Beside the grid, parallel to an edge or not.
    assertEquals("[]", cells(UNIT, -1, -2, -1, 8));
    assertEquals("[]", cells(UNIT, -3, 7, 9, 7));
    assertEquals("[]", cells(UNIT, -2, 5, 1, 9));
    // Through a corner of the grid and nothing else of it.
    assertEquals("[]", cells(UNIT, -1, 5, 1, 7));
    assertEquals("[]", cells(UNIT, 5, -1, 7, 1));
    // Along the far edges, which belong to no cell.
    assertEquals("[]", cells(UNIT, 6, -1, 6, 7));
    assertEquals("[]", cells(UNIT, -1, 0, 7, 0));
    // Along the near edges, which belong to the first column and the first row.
    assertEquals("[(0,0), (0,1), (0,2), (0,3), (0,4), (0,5)]", cells(UNIT, 0, -1, 0, 7));
    assertEquals("[(0,0), (1,0), (2,0), (3,0), (4,0), (5,0)]", cells(UNIT, -1, 6, 7, 6));
  }

  @Test
  public void clippedSegmentKeepsItsSideOfAGridLine() {
    // GH-3255: inside the grid this segment is strictly above y = 3 except at its last point. Its
    // intersection with the grid's left edge rounds to y = 3 in double precision, which used to
    // move it to the row below.
    double above = Math.nextUp(3.0);
    double below = Math.nextDown(3.0);
    assertEquals("[(0,2)]", cells(UNIT, -1, above, 1, 3));
    assertEquals("[(0,2)]", cells(UNIT, 1, 3, -1, above));
    assertEquals("[(0,3)]", cells(UNIT, -1, below, 1, 3));
    assertEquals("[(0,3)]", cells(UNIT, -1, 3, 1, 3));
    // The same across a column line, entering through the top edge.
    assertEquals("[(3,0)]", cells(UNIT, above, 7, 3, 5));
    assertEquals("[(2,0)]", cells(UNIT, below, 7, 3, 5));
    assertEquals("[(3,0)]", cells(UNIT, 3, 7, 3, 5));
  }

  @Test
  public void extremeFiniteEndpoints() {
    // GH-3255: the differences of these coordinates overflow in double precision.
    double far = 1e308;
    assertEquals("[(0,3), (1,3), (2,3), (3,3), (4,3), (5,3)]", cells(UNIT, -far, 2.5, far, 2.5));
    assertEquals("[(2,0), (2,1), (2,2), (2,3), (2,4), (2,5)]", cells(UNIT, 2.5, far, 2.5, -far));
    assertEquals("[(5,0), (4,1), (3,2), (2,3), (1,4), (0,5)]", cells(UNIT, -far, -far, far, far));
    assertEquals(cells(UNIT, -far, 2.5, far, 2.5), cells(UNIT, far, 2.5, -far, 2.5));
    Grid fine = new Grid(4, 4, 0, 0.04, 0.01, -0.01);
    assertEquals("[(0,1), (1,1), (2,1), (3,1)]", cells(fine, -far, 0.025, far, 0.025));
    assertEquals("[]", cells(UNIT, far, far, far, -far));
    assertMatchesOracle(UNIT, -far, 1.5, far, 4.5, false, false);
    assertMatchesOracle(fine, -far, -far / 3, far / 7, far, false, false);
  }

  @Test
  public void nonFiniteCoordinatesAreSkipped() {
    assertEquals("[]", cells(UNIT, Double.NaN, 2.5, 3, 2.5));
    assertEquals("[]", cells(UNIT, 1, 2.5, Double.POSITIVE_INFINITY, 2.5));
  }

  @Test
  public void decimalCoordinatesOnGridLines() {
    // GH-3256: (0.3 - 0.6) / -0.1 is 2.9999999999999996 in binary floating point, but 0.3 is the
    // grid line 3 of this grid. The segment ends on it and must not reach the row above.
    Grid tenths = new Grid(6, 6, 0, 0.6, 0.1, -0.1);
    assertEquals("[(2,3)]", cells(tenths, 0.22, 0.22, 0.3, 0.3));
    assertEquals("[(2,3)]", cells(tenths, 0.22, 0.25, 0.3, 0.25));
    // 0.1 + 2 * 0.1 evaluates to 0.30000000000000004, the next double above 0.3. It is a different
    // stored value and lies past the line, so a segment ending there reaches the next column.
    double affine = 0.1 + 2 * 0.1;
    assertEquals(Math.nextUp(0.3), affine, 0);
    assertEquals("[(2,3), (3,3)]", cells(tenths, 0.22, 0.25, affine, 0.25));
    // Ending past both lines, the diagonal passes exactly through their corner into the cell
    // diagonally across.
    assertEquals("[(3,2), (2,3)]", cells(tenths, 0.22, 0.22, affine, affine));

    // 0.030000000000000002 is the next double above 0.03: a real crossing into column 3, although
    // the double-precision quotient 0.030000000000000002 / 0.01 rounds to exactly 3.
    Grid hundredths = new Grid(6, 6, 0, 0.06, 0.01, -0.01);
    assertEquals(3.0, Math.nextUp(0.03) / 0.01, 0);
    assertEquals("[(2,1), (3,1)]", cells(hundredths, 0.025, 0.045, Math.nextUp(0.03), 0.045));
    assertEquals("[(2,1)]", cells(hundredths, 0.025, 0.045, 0.03, 0.045));

    // A large origin: (1000000.03 - 1000000.0) / 0.01 is 3.0000000027939677.
    Grid offset = new Grid(6, 6, 1000000.0, 50.06, 0.01, -0.01);
    assertEquals("[(2,1)]", cells(offset, 1000000.025, 50.045, 1000000.03, 50.045));
    assertEquals("[(3,1)]", cells(offset, 1000000.03, 50.045, 1000000.035, 50.045));

    // A bottom-up grid of thirds: 0.3333333333333333 * 3 is not 1 in decimal, so 1.0 is just past
    // the third line; the walk still agrees with exact arithmetic on these stored values.
    Grid thirds = new Grid(6, 6, 0, 0, 0.3333333333333333, 0.3333333333333333);
    assertEquals("[(2,1), (3,1)]", cells(thirds, 0.9, 0.5, 1.0, 0.5));
    assertMatchesOracle(thirds, 0.9, 0.5, 1.0, 0.5, false, false);
  }

  @Test
  public void neighbouringDoublesKeepTheirSide() {
    // Only a coordinate exactly on a grid line is treated as on it; the doubles next to it cross
    // it or stop short of it, on every kind of grid.
    Grid[] grids = {
      UNIT,
      new Grid(6, 6, 0, 0, 1, 1),
      new Grid(6, 6, 0, 0.6, 0.1, -0.1),
      new Grid(6, 6, 1000000.0, 2000000.0, 0.25, -0.25),
      new Grid(6, 6, -179.5, 89.5, 30, 30)
    };
    for (Grid grid : grids) {
      double line = grid.worldX(3);
      double y = grid.worldY(2.5);
      double start = grid.worldX(2.5);
      assertEquals(grid.toString(), "[(2,2)]", cells(grid, start, y, line, y));
      assertEquals(grid.toString(), "[(2,2)]", cells(grid, start, y, Math.nextDown(line), y));
      assertEquals(grid.toString(), "[(2,2), (3,2)]", cells(grid, start, y, Math.nextUp(line), y));

      double rowLine = grid.worldY(3);
      double x = grid.worldX(2.5);
      double rowStart = grid.worldY(2.5);
      double beyond = grid.scaleY > 0 ? Math.nextUp(rowLine) : Math.nextDown(rowLine);
      double short_ = grid.scaleY > 0 ? Math.nextDown(rowLine) : Math.nextUp(rowLine);
      assertEquals(grid.toString(), "[(2,2)]", cells(grid, x, rowStart, x, rowLine));
      assertEquals(grid.toString(), "[(2,2)]", cells(grid, x, rowStart, x, short_));
      assertEquals(grid.toString(), "[(2,2), (2,3)]", cells(grid, x, rowStart, x, beyond));
    }
  }

  @Test
  public void segmentAlongAGridLine() {
    // The half-open cells after the line in pixel order, or the ones before it on request.
    GridLineWalker walker = UNIT.walker();
    assertEquals(
        new TreeSet<>(Arrays.asList(3 * 6 + 1, 3 * 6 + 2, 3 * 6 + 3)),
        walk(walker, UNIT, 1.5, 3, 3.5, 3, false, false));
    assertEquals(
        new TreeSet<>(Arrays.asList(2 * 6 + 1, 2 * 6 + 2, 2 * 6 + 3)),
        walk(walker, UNIT, 1.5, 3, 3.5, 3, false, true));
    assertEquals(
        new TreeSet<>(Arrays.asList(1 * 6 + 3, 2 * 6 + 3)),
        walk(walker, UNIT, 3, 4.5, 3, 3.5, false, false));
    assertEquals(
        new TreeSet<>(Arrays.asList(1 * 6 + 2, 2 * 6 + 2)),
        walk(walker, UNIT, 3, 4.5, 3, 3.5, true, false));
    // On a bottom-up grid pixel rows run up, so the cells after a row line are the ones above it.
    Grid bottomUp = new Grid(6, 6, 0, 0, 1, 1);
    assertEquals("[(1,3), (2,3), (3,3)]", cells(bottomUp, 1.5, 3, 3.5, 3));
  }

  @Test
  public void touchedSegmentCrossingGridCorners() {
    // GH-3322. Each expectation is what GDAL 3.10 and 3.12 burn with ALL_TOUCHED on the same grid.
    // These segments run towards smaller rows through the corners (3,17), (4,16), ..., (9,11); at
    // each the cell after the corner, which the segment meets only there, is touched as well.
    Grid north = new Grid(20, 20, 0, 20, 1, -1);
    assertEquals(8, walk(north.walker(), north, 2.5, 2.5, 9.5, 9.5, false, false).size());
    assertEquals(
        "[(9,10), (8,11), (9,11), (7,12), (8,12), (6,13), (7,13), (5,14), (6,14), (4,15), (5,15),"
            + " (3,16), (4,16), (2,17), (3,17)]",
        touchedCells(north, 2.5, 2.5, 9.5, 9.5));
    assertEquals(15, walkTouched(north.walker(), north, 1.5, 2.75, 10.5, 7.25).size());
    assertEquals(15, walkTouched(north.walker(), north, 0.75, 1.25, 4.25, 11.75).size());
    // Running towards larger rows, the cell after each corner is the next cell of the walk, so
    // nothing is added.
    assertEquals(
        "[(2,10), (3,11), (4,12), (5,13), (6,14), (7,15), (8,16), (9,17)]",
        touchedCells(north, 2.5, 9.5, 9.5, 2.5));
    // Pixel rows run the other way on a bottom-up grid, and so does the rule.
    Grid bottomUp = new Grid(20, 20, 0, 0, 1, 1);
    assertEquals(8, walkTouched(bottomUp.walker(), bottomUp, 2.5, 2.5, 9.5, 9.5).size());
    assertEquals(15, walkTouched(bottomUp.walker(), bottomUp, 2.5, 9.5, 9.5, 2.5).size());
    // A segment that crosses no corner touches exactly the cells it passes through.
    assertEquals(
        walk(north.walker(), north, 1.3, 2.7, 8.6, 11.4, false, false),
        walkTouched(north.walker(), north, 1.3, 2.7, 8.6, 11.4));

    // Through the whole grid along its anti-diagonal: the corners on the grid's edge add nothing.
    assertEquals(
        "[(5,0), (4,1), (5,1), (3,2), (4,2), (2,3), (3,3), (1,4), (2,4), (0,5), (1,5)]",
        touchedCells(UNIT, -1, -1, 7, 7));
    assertEquals("[(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)]", touchedCells(UNIT, -1, 7, 7, -1));
  }

  @Test
  public void touchedSegmentEndingOnGridLines() {
    // GH-3322. Each expectation is what GDAL 3.10 and 3.12 burn with ALL_TOUCHED on the same grid.
    // The first point of a segment in pixel order selects the cell containing it; the last selects
    // nothing.
    // From the corner (3,3) towards smaller rows: the corner's own cell (3,3) is touched.
    assertEquals("[(4,1), (3,2), (4,2), (3,3)]", touchedCells(UNIT, 3, 3, 4.5, 4.5));
    assertEquals("[(4,1), (3,2)]", cells(UNIT, 3, 3, 4.5, 4.5));
    // From the corner towards larger rows: it is the cell the segment starts in anyway.
    assertEquals("[(3,3), (4,4)]", touchedCells(UNIT, 3, 3, 4.5, 1.5));
    // Ending on the corner (3,3): the last point selects nothing.
    assertEquals("[(2,3), (1,4), (2,4)]", touchedCells(UNIT, 1.5, 1.5, 3, 3));
    assertEquals("[(1,1), (2,2)]", touchedCells(UNIT, 1.5, 4.5, 3, 3));
    assertEquals("[(1,1), (2,1), (2,2), (3,2)]", touchedCells(UNIT, 1.5, 4.5, 3.8, 3));
    // Starting on a row line and running towards smaller rows: the cell after the line too.
    assertEquals("[(3,2), (4,2), (3,3)]", touchedCells(UNIT, 3.5, 3, 4.5, 3.9));
    // Horizontal and vertical segments keep their first point's cell and drop their last.
    assertEquals("[(1,3), (2,3), (3,3)]", touchedCells(UNIT, 1.5, 2.5, 4, 2.5));
    assertEquals("[(2,1), (2,2), (2,3)]", touchedCells(UNIT, 2.5, 4.5, 2.5, 2));
    assertEquals("[(2,2), (2,3), (2,4)]", touchedCells(UNIT, 2.5, 1.5, 2.5, 4));
    // Coming into the grid through its left edge exactly at a row line.
    assertEquals("[(0,2), (1,2), (0,3)]", touchedCells(UNIT, -1, 2.5, 1.5, 3.75));
  }

  @Test
  public void touchedSegmentOrdersItsEndsByPixelX() {
    // The first point is the end with the smaller pixel x, however short the segment is along x.
    // This one stays within column 3 and leaves the row line y = 3 towards smaller rows: its left
    // end is on the line, so the cell after the line is touched. GDAL burns the same two cells
    // from 3.11 on; up to 3.10 it ordered the ends of a segment within one column of cells by row
    // and burned only (3,2).
    assertEquals("[(3,2), (3,3)]", touchedCells(UNIT, 3.5, 3, 3.9, 3.9));
    assertEquals("[(3,2)]", cells(UNIT, 3.5, 3, 3.9, 3.9));
    // Leaning the other way, the end on the row line is the right end: the last point.
    assertEquals("[(3,2)]", touchedCells(UNIT, 3.5, 3, 3.1, 3.9));
    // Only a segment along a column is ordered by pixel y, and its upper end comes first.
    assertEquals("[(3,2)]", touchedCells(UNIT, 3.5, 3, 3.5, 3.9));
    assertEquals("[(3,2), (3,3)]", touchedCells(UNIT, 3.5, 3.9, 3.5, 2.5));
    // The same within one column of a bottom-up grid, where rows grow with y.
    Grid bottomUp = new Grid(6, 6, 0, 0, 1, 1);
    assertEquals("[(3,2), (3,3)]", touchedCells(bottomUp, 3.5, 3, 3.9, 2.1));
    assertEquals("[(3,2)]", touchedCells(bottomUp, 3.5, 3, 3.1, 2.1));
  }

  @Test
  public void touchedSegmentMeetingTheGridAtOnePoint() {
    // The part of the segment inside the grid is one point, which is its last point as well as
    // its first, so nothing is touched. This holds for a first point on the grid's first row
    // line too, although a point there belongs to a cell of the first row: GDAL (3.10 and 3.12)
    // burns nothing for these either.
    assertEquals("[]", touchedCells(UNIT, 2.5, 6, 4, 7.5));
    assertEquals("[]", touchedCells(UNIT, 2.5, 6, 2.75, 7));
    assertEquals("[]", touchedCells(UNIT, 2.5, 6, 2.5, 7));
    assertEquals("[]", touchedCells(UNIT, 2.5, 6, 1, 7));
    assertEquals("[]", touchedCells(UNIT, 0, 2.5, -1, 3));
    assertEquals("[]", touchedCells(UNIT, -1, 5, 1, 7));
    // Running along the first row line instead of leaving it, the segment has a first point.
    assertEquals("[(2,0), (3,0)]", touchedCells(UNIT, 2.5, 6, 4, 6));
  }

  @Test
  public void segmentsBeyondOneSideOfTheGrid() {
    // Both ends before the first column or row line, or after the last one: no cell, whether the
    // ends are on grid nodes, between them or far away.
    assertEquals("[]", cells(UNIT, -3, 1, -1, 5));
    assertEquals("[]", cells(UNIT, -2.5, -4, -0.25, 30));
    assertEquals("[]", cells(UNIT, 7, 1, 9, 5));
    assertEquals("[]", cells(UNIT, 1, 7, 5, 9));
    assertEquals("[]", cells(UNIT, 1, -1, 5, -3));
    assertEquals("[]", cells(UNIT, 1e9, 2, 2e9, 4));
    assertEquals("[]", touchedCells(UNIT, 7, 1, 9, 5));
    // Ends on the last column and row lines are not beyond them, and still reach no cell. Ends on
    // the first ones are in the first column and row.
    assertEquals("[]", cells(UNIT, 6, 1, 8, 5));
    assertEquals("[]", cells(UNIT, 1, 0, 5, -2));
    assertEquals("[(0,1), (0,2), (0,3), (0,4)]", cells(UNIT, 0, 1.5, 0, 4.5));
    assertEquals("[(0,3)]", cells(UNIT, -3, 1, 0.5, 2.5));
    // A segment with its ends beyond two different sides can still cross the grid.
    assertEquals("[(0,4), (0,5), (1,5)]", cells(UNIT, -1, 2.5, 2.5, -1));
    // The walker keeps a vertex shared with the next segment, also when it skipped the first one.
    GridLineWalker walker = UNIT.walker();
    assertEquals(new TreeSet<Integer>(), walk(walker, UNIT, -3, 1, -1, 2.5, false, false));
    assertEquals(
        oracle(UNIT, -1, 2.5, 2.5, 2.5, false, false),
        walk(walker, UNIT, -1, 2.5, 2.5, 2.5, false, false));
  }

  @Test
  public void segmentsBeyondTheGridNeedNoDecimalArithmetic() {
    // A line much larger than the raster has most of its vertices outside it. Finding the cell of
    // a vertex on a grid node takes decimal arithmetic, which allocates; a segment with both ends
    // beyond the same side of the grid has to be skipped before that. The 200,000 segments here
    // allocated 97 MB when every vertex was resolved first.
    java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    Assume.assumeTrue(bean instanceof com.sun.management.ThreadMXBean);
    com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) bean;
    Assume.assumeTrue(threads.isThreadAllocatedMemorySupported());
    Assume.assumeTrue(threads.isThreadAllocatedMemoryEnabled());

    Grid tile = new Grid(256, 256, 500000, 4100000, 30, -30);
    GridLineWalker walker = tile.walker();
    int[] visited = new int[1];
    GridLineWalker.CellVisitor count = (col, row) -> visited[0]++;
    long thread = Thread.currentThread().getId();
    long before = threads.getThreadAllocatedBytes(thread);
    // A staircase along grid nodes east of the tile, within its rows.
    for (int i = 0; i < 100000; i++) {
      double x = tile.originX + 30.0 * (300 + i);
      double y = tile.originY - 30.0 * (i % 200);
      walker.walk(x, y, x + 30, y, false, false, count);
      walker.walk(x + 30, y, x + 30, y - 30, false, false, count);
    }
    long allocated = threads.getThreadAllocatedBytes(thread) - before;
    assertEquals(0, visited[0]);
    assertTrue("allocated " + allocated + " bytes", allocated < (1 << 20));
  }

  @Test
  public void gridLargerThanTheIntRange() {
    // The walk is bounded by width + height steps, a sum that does not fit an int here.
    Grid huge = new Grid(Integer.MAX_VALUE, Integer.MAX_VALUE, 0, 0, 1, 1);
    List<String> visited = new ArrayList<>();
    huge.walker()
        .walk(0.5, 0.5, 3.5, 2.5, false, false, (col, row) -> visited.add(col + "," + row));
    assertEquals("[0,0, 1,0, 1,1, 2,1, 2,2, 3,2]", visited.toString());
    visited.clear();
    huge.walker()
        .walkTouched(
            2147483640.5,
            2147483646.5,
            2147483649.5,
            2147483646.5,
            (col, row) -> visited.add(col + "," + row));
    assertEquals(7, visited.size());
    assertEquals("2147483646,2147483646", visited.get(6));
  }

  @Test
  public void segmentWithNoLength() {
    assertEquals("[(2,3)]", cells(UNIT, 2.5, 2.5, 2.5, 2.5));
    assertEquals("[(3,3)]", cells(UNIT, 3, 3, 3, 3));
    assertEquals("[]", cells(UNIT, 6, 3, 6, 3));
    assertEquals("[]", cells(UNIT, 7, 8, 7, 8));
  }

  @Test
  public void consecutiveSegmentsShareAVertex() {
    // The walker keeps what it knows about a shared vertex; the result must not depend on it.
    GridLineWalker walker = UNIT.walker();
    double[][] path = {{-1, 2.5}, {3, 3}, {3, 5}, {7.5, 0.5}, {3, 3}, {0.25, 0.25}};
    for (int i = 0; i < path.length - 1; i++) {
      double[] a = path[i];
      double[] b = path[i + 1];
      assertEquals(
          oracle(UNIT, a[0], a[1], b[0], b[1], false, false),
          walk(walker, UNIT, a[0], a[1], b[0], b[1], false, false));
    }
  }

  @Test
  public void randomSegmentsMatchTheExactOracle() {
    // Grids with unit, decimal, repeating-fraction and large pixel sizes, north-up and bottom-up,
    // at small and large origins. Coordinates are drawn on grid lines, next to them, on cell
    // centres, anywhere, and far outside, so that corner crossings, grid-aligned vertices and
    // clipped segments are all common.
    double[] scales = {1, 0.1, 0.25, 0.01, 0.3333333333333333, 1.0 / 3600, 30, 2.5};
    double[] origins = {0, 0.6, -179.5, 1000000.0, 123456.789, 0.001};
    Random random = new Random(3255);
    int segments = 0;
    for (int g = 0; g < 400; g++) {
      double scaleX = scales[random.nextInt(scales.length)];
      double scaleY = scales[random.nextInt(scales.length)] * (random.nextBoolean() ? 1 : -1);
      Grid grid =
          new Grid(
              1 + random.nextInt(9),
              1 + random.nextInt(9),
              origins[random.nextInt(origins.length)],
              origins[random.nextInt(origins.length)],
              scaleX,
              scaleY);
      for (int s = 0; s < 40; s++) {
        double x0 = randomCoordinate(random, grid, true);
        double y0 = randomCoordinate(random, grid, false);
        double x1 = random.nextInt(8) == 0 ? x0 : randomCoordinate(random, grid, true);
        double y1 = random.nextInt(8) == 0 ? y0 : randomCoordinate(random, grid, false);
        assertMatchesOracle(grid, x0, y0, x1, y1, random.nextBoolean(), random.nextBoolean());
        assertTouchedMatchesOracle(grid, x0, y0, x1, y1);
        segments++;
      }
    }
    assertEquals(16000, segments);
  }

  private static double randomCoordinate(Random random, Grid grid, boolean xAxis) {
    int size = xAxis ? grid.width : grid.height;
    double pixel;
    switch (random.nextInt(10)) {
      case 0:
      case 1:
      case 2:
        // On a grid line, from two cells before the grid to two after it.
        pixel = random.nextInt(size + 5) - 2;
        break;
      case 3:
        // On a cell centre.
        pixel = random.nextInt(size + 4) - 2 + 0.5;
        break;
      case 4:
        // Far outside.
        pixel = (random.nextBoolean() ? 1 : -1) * (1e6 + random.nextInt(1000) / 8.0);
        break;
      default:
        // Anywhere near the grid, to three decimals.
        pixel = Math.round((random.nextDouble() * (size + 4) - 2) * 1000) / 1000.0;
    }
    double world = xAxis ? grid.worldX(pixel) : grid.worldY(pixel);
    switch (random.nextInt(12)) {
      case 0:
        return Math.nextUp(world);
      case 1:
        return Math.nextDown(world);
      default:
        return world;
    }
  }
}
