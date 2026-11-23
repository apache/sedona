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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
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
    List<String> cells = new ArrayList<>();
    for (int cell : walk(grid.walker(), grid, x0, y0, x1, y1, false, false)) {
      cells.add("(" + cell % grid.width + "," + cell / grid.width + ")");
    }
    return cells.toString();
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
