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

/**
 * Exact-rational reference for clipping a pixel-space segment to the window [0, width] x [0,
 * height], used by the rasterization tests to check the production clipper without sharing any of
 * its arithmetic.
 *
 * <p>Every double is an exact rational, so the whole clip runs without rounding and a segment that
 * grazes a grid line is resolved rather than discarded. Deliberately unoptimized: correctness here
 * matters, speed does not.
 */
final class SegmentClipOracle {

  private SegmentClipOracle() {}

  /** A rational number kept with a positive denominator. */
  static final class Rational implements Comparable<Rational> {
    final BigInteger num;
    final BigInteger den;

    private Rational(BigInteger num, BigInteger den) {
      if (den.signum() < 0) {
        num = num.negate();
        den = den.negate();
      }
      BigInteger g = num.gcd(den);
      if (g.signum() != 0 && !g.equals(BigInteger.ONE)) {
        num = num.divide(g);
        den = den.divide(g);
      }
      this.num = num;
      this.den = den;
    }

    static Rational of(long value) {
      return new Rational(BigInteger.valueOf(value), BigInteger.ONE);
    }

    /** Exact, because {@code new BigDecimal(double)} is the double's exact value. */
    static Rational of(double value) {
      if (!Double.isFinite(value)) {
        throw new IllegalArgumentException("Not a finite double: " + value);
      }
      BigDecimal exact = new BigDecimal(value);
      BigInteger unscaled = exact.unscaledValue();
      int scale = exact.scale();
      if (scale >= 0) {
        return new Rational(unscaled, BigInteger.TEN.pow(scale));
      }
      return new Rational(unscaled.multiply(BigInteger.TEN.pow(-scale)), BigInteger.ONE);
    }

    Rational add(Rational o) {
      return new Rational(num.multiply(o.den).add(o.num.multiply(den)), den.multiply(o.den));
    }

    Rational subtract(Rational o) {
      return new Rational(num.multiply(o.den).subtract(o.num.multiply(den)), den.multiply(o.den));
    }

    Rational multiply(Rational o) {
      return new Rational(num.multiply(o.num), den.multiply(o.den));
    }

    Rational divide(Rational o) {
      if (o.num.signum() == 0) {
        throw new ArithmeticException("Division by zero");
      }
      return new Rational(num.multiply(o.den), den.multiply(o.num));
    }

    int signum() {
      return num.signum();
    }

    boolean isInteger() {
      return den.equals(BigInteger.ONE);
    }

    /** Largest integer not greater than this value. */
    BigInteger floor() {
      BigInteger[] qr = num.divideAndRemainder(den);
      if (qr[1].signum() < 0) {
        return qr[0].subtract(BigInteger.ONE);
      }
      return qr[0];
    }

    @Override
    public int compareTo(Rational o) {
      return num.multiply(o.den).compareTo(o.num.multiply(den));
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Rational && compareTo((Rational) o) == 0;
    }

    @Override
    public int hashCode() {
      return num.hashCode() * 31 + den.hashCode();
    }

    @Override
    public String toString() {
      return isInteger() ? num.toString() : num + "/" + den;
    }
  }

  /** The exact clipped segment, or {@code null} when the intersection has no length. */
  static final class Clip {
    final Rational x0;
    final Rational y0;
    final Rational x1;
    final Rational y1;

    Clip(Rational x0, Rational y0, Rational x1, Rational y1) {
      this.x0 = x0;
      this.y0 = y0;
      this.x1 = x1;
      this.y1 = y1;
    }
  }

  /**
   * Clips the segment to [0, width] x [0, height] exactly.
   *
   * @return the clipped segment, or null when the segment and the window share no positive-length
   *     piece. A single-point touch, such as a corner, counts as no length.
   */
  static Clip clip(double px0, double py0, double px1, double py1, int width, int height) {
    Rational x0 = Rational.of(px0);
    Rational y0 = Rational.of(py0);
    Rational x1 = Rational.of(px1);
    Rational y1 = Rational.of(py1);

    Rational tEnter = Rational.of(0);
    Rational tExit = Rational.of(1);

    Rational[][] axes = {{x0, x1, Rational.of(width)}, {y0, y1, Rational.of(height)}};
    for (Rational[] axis : axes) {
      Rational from = axis[0];
      Rational to = axis[1];
      Rational far = axis[2];
      Rational delta = to.subtract(from);
      if (delta.signum() == 0) {
        if (from.signum() < 0 || from.compareTo(far) > 0) {
          return null;
        }
        continue;
      }
      Rational tAtZero = Rational.of(0).subtract(from).divide(delta);
      Rational tAtFar = far.subtract(from).divide(delta);
      Rational tNear = delta.signum() > 0 ? tAtZero : tAtFar;
      Rational tFarther = delta.signum() > 0 ? tAtFar : tAtZero;
      if (tNear.compareTo(tEnter) > 0) {
        tEnter = tNear;
      }
      if (tFarther.compareTo(tExit) < 0) {
        tExit = tFarther;
      }
    }

    if (tEnter.compareTo(tExit) >= 0) {
      return null;
    }
    return new Clip(
        lerp(x0, x1, tEnter), lerp(y0, y1, tEnter), lerp(x0, x1, tExit), lerp(y0, y1, tExit));
  }

  private static Rational lerp(Rational from, Rational to, Rational t) {
    return from.add(to.subtract(from).multiply(t));
  }

  /**
   * Zero-based cell holding an endpoint of the clipped segment, where cell i covers [i, i + 1). An
   * endpoint exactly on a grid line belongs to the cell holding the segment's interior, matching
   * how the traversal selects its first and last cell.
   */
  static int cellOf(Rational coordinate, Rational otherEnd) {
    BigInteger floor = coordinate.floor();
    boolean interiorIsBelow = otherEnd.compareTo(coordinate) < 0;
    if (interiorIsBelow && coordinate.isInteger()) {
      floor = floor.subtract(BigInteger.ONE);
    }
    return floor.intValueExact();
  }
}
