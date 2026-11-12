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
package org.apache.spark.sql.sedona_sql.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.sedona.common.S2Geography.WKBGeography;
import org.apache.spark.sql.catalyst.util.Geography;
import org.apache.spark.sql.catalyst.util.Geometry;
import org.junit.Test;

public class PublicWkbHeaderAdapterTest {
  private static final int Z = 0x80000000;
  private static final int M = 0x40000000;
  private static final int SRID = 0x20000000;

  @Test
  public void isoInputsAreBorrowedUnchanged() {
    for (boolean le : new boolean[] {false, true}) {
      for (int d = 0; d < 4; d++) {
        double[] values =
            d == 0
                ? new double[] {1, 2}
                : d == 3 ? new double[] {1, 2, 3, 4} : new double[] {1, 2, 3};
        byte[] source = point(le, 1 + d * 1000, 0, values);
        assertSame("Already ISO input need not be copied", source, adapt(source, source));
      }
    }
  }

  @Test
  public void ewkbLayoutFlagsNormalizeInBothByteOrders() {
    for (boolean le : new boolean[] {false, true}) {
      for (int d = 1; d < 4; d++) {
        int flags = (d == 1 || d == 3 ? Z : 0) | (d >= 2 ? M : 0);
        double[] values = d == 3 ? new double[] {1, 2, 3, 4} : new double[] {1, 2, 3};
        byte[] iso = adapt(point(le, 1 | flags, 0, values), point(le, 1 + d * 1000, 0, values));
        geometryAccepts(iso);
        geographyAccepts(iso);
      }
    }
  }

  @Test
  public void embeddedSridIsRemovedAndExplicitNativeSridWins() {
    for (boolean le : new boolean[] {false, true}) {
      byte[] iso =
          adapt(point(le, 1 | Z | M | SRID, -1, 1, 2, 3, 4), point(le, 3001, 0, 1, 2, 3, 4));
      geometryAccepts(iso);
      geographyAccepts(iso);
    }
  }

  @Test
  public void nestedByteOrdersAndIndependentEmbeddedSridsArePreserved() {
    byte[] iso = adapt(nested(), nestedIso());
    geometryAccepts(iso);
    geographyAccepts(iso);
  }

  @Test
  public void allSevenBaseTypesPreservePayloads() {
    for (boolean le : new boolean[] {false, true}) {
      byte[] p = point(le, 1 | SRID, 3857, 1, 2);
      byte[] pi = point(le, 1, 0, 1, 2);
      byte[] l = cat(header(le, 2 | SRID, 3857), intBytes(le, 2), coords(le, 1, 2, 2, 3));
      byte[] li = cat(header(le, 2, 0), intBytes(le, 2), coords(le, 1, 2, 2, 3));
      byte[] pg =
          cat(
              header(le, 3 | SRID, 3857),
              intBytes(le, 1),
              intBytes(le, 5),
              coords(le, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0));
      byte[] pgi =
          cat(
              header(le, 3, 0),
              intBytes(le, 1),
              intBytes(le, 5),
              coords(le, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0));
      for (byte[] iso :
          new byte[][] {
            adapt(p, pi),
            adapt(l, li),
            adapt(pg, pgi),
            adapt(collection(le, 4 | SRID, 3857, p), collection(le, 4, 0, pi)),
            adapt(collection(le, 5 | SRID, 3857, l), collection(le, 5, 0, li)),
            adapt(collection(le, 6 | SRID, 3857, pg), collection(le, 6, 0, pgi)),
            adapt(collection(le, 7 | SRID, 3857, p, l, pg), collection(le, 7, 0, pi, li, pgi))
          }) {
        geometryAccepts(iso);
        geographyAccepts(iso);
      }
    }
  }

  @Test
  public void dimensionalEmptiesRetainDeclaredLayout() {
    byte[] p =
        adapt(
            point(true, 1 | Z | M | SRID, 4326, Double.NaN, Double.NaN, Double.NaN, Double.NaN),
            point(true, 3001, 0, Double.NaN, Double.NaN, Double.NaN, Double.NaN));
    geometryAccepts(p);
    geographyAccepts(p);
    for (int base = 2; base <= 7; base++) {
      byte[] iso =
          adapt(
              cat(header(false, base | Z | M | SRID, 4326), intBytes(false, 0)),
              cat(header(false, base + 3000, 0), intBytes(false, 0)));
      geometryAccepts(iso);
      geographyAccepts(iso);
    }
  }

  @Test
  public void rawNanPayloadsAndNegativeZeroRemainExact() {
    double nan = Double.longBitsToDouble(0x7ff8000000000042L);
    adapt(
        point(false, 1 | Z | M | SRID, 4326, -0.0, 2, nan, -0.0),
        point(false, 3001, 0, -0.0, 2, nan, -0.0));
  }

  @Test
  public void wkbGeographyStaysLazyAcrossNativeEncoding() throws ReflectiveOperationException {
    boolean previous = WKBGeography.isEagerShapeIndex();
    WKBGeography.setEagerShapeIndex(false);
    try {
      WKBGeography geog = WKBGeography.fromWKB(nested(), 4326);
      byte[] original = geog.getWKBBytes().clone();
      assertLazyCaches(geog);
      geographyAccepts(PublicWkbHeaderAdapter.toIsoWkb(geog.getWKBBytes()));
      assertLazyCaches(geog);
      assertArrayEquals("Encoding must not mutate retained S2 bytes", original, geog.getWKBBytes());
    } finally {
      WKBGeography.setEagerShapeIndex(previous);
    }
  }

  @Test
  public void everyTruncationOfNestedEwkbIsRejected() {
    byte[] nested = nested();
    for (int n = 0; n < nested.length; n++) {
      adapterRejects(Arrays.copyOf(nested, n));
    }
  }

  @Test
  public void invalidFramingAndCountsRejectWithoutOverflow() {
    adapterRejects(new byte[] {2, 1, 0, 0, 0});
    adapterRejects(header(true, 8, 0));
    adapterRejects(header(true, 4001, 0));
    adapterRejects(cat(header(true, 2, 0), intBytes(true, -1)));
    adapterRejects(cat(header(true, 2 | Z | M, 0), intBytes(true, Integer.MAX_VALUE)));
    adapterRejects(cat(header(true, 2 | Z | M, 0), intBytes(true, 1 << 30)));
    adapterRejects(cat(header(true, 3, 0), intBytes(true, Integer.MAX_VALUE)));
    adapterRejects(cat(header(true, 7, 0), intBytes(true, Integer.MAX_VALUE)));
  }

  @Test
  public void trailingBytesRemainUnderSparkValidation() {
    byte[] trailing = new byte[] {0, 1, 2, 3};
    byte[] iso =
        adapt(
            cat(point(true, 1 | Z | SRID, 3857, 1, 2, 3), trailing),
            cat(point(true, 1001, 0, 1, 2, 3), trailing));
    geometryAccepts(iso);
    geographyAccepts(iso);
  }

  @Test
  public void sparkOwnsSemanticValidation() {
    sparkRejects(point(true, 1 | SRID, 4326, 181, 2), true);
    sparkRejects(cat(header(true, 2 | Z, 0), intBytes(true, 1), coords(true, 1, 2, 3)), false);
    sparkRejects(
        cat(
            header(true, 3 | SRID, 4326),
            intBytes(true, 1),
            intBytes(true, 4),
            coords(true, 0, 0, 1, 0, 1, 1, 0, 1)),
        false);
    sparkRejects(
        collection(true, 7, 0, point(true, 1, 0, 1, 2), point(true, 1 | Z, 0, 1, 2, 3)), false);
  }

  @Test
  public void deepCollectionHeaderWalkIsIterative() {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    for (int n = 0; n < 20000; n++) {
      byte[] prefix = cat(header(true, 7, 0), intBytes(true, 1));
      b.write(prefix, 0, prefix.length);
    }
    byte[] leaf = point(true, 1, 0, 1, 2);
    b.write(leaf, 0, leaf.length);
    byte[] source = b.toByteArray();
    assertSame("Already ISO input need not be copied", source, adapt(source, source));
    // Only the adapter is exercised: Spark's own collection depth is a separate contract.
  }

  private static byte[] cat(byte[]... parts) {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      b.write(p, 0, p.length);
    }
    return b.toByteArray();
  }

  private static byte[] intBytes(boolean le, int value) {
    return ByteBuffer.allocate(4)
        .order(le ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN)
        .putInt(value)
        .array();
  }

  private static byte[] header(boolean le, int type, int srid) {
    return (type & SRID) == 0
        ? cat(new byte[] {(byte) (le ? 1 : 0)}, intBytes(le, type))
        : cat(new byte[] {(byte) (le ? 1 : 0)}, intBytes(le, type), intBytes(le, srid));
  }

  private static byte[] coords(boolean le, double... values) {
    ByteBuffer b =
        ByteBuffer.allocate(values.length * 8)
            .order(le ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    for (double value : values) {
      b.putLong(Double.doubleToRawLongBits(value));
    }
    return b.array();
  }

  private static byte[] point(boolean le, int type, int srid, double... values) {
    return cat(header(le, type, srid), coords(le, values));
  }

  private static byte[] collection(boolean le, int type, int srid, byte[]... children) {
    return cat(header(le, type, srid), intBytes(le, children.length), cat(children));
  }

  private static byte[] nested() {
    return collection(
        true,
        7 | Z | M | SRID,
        3857,
        point(false, 1 | Z | M | SRID, 9999, 1, 2, 3, 4),
        collection(false, 7 | Z | M | SRID, -1, point(true, 1 | Z | M, 0, 5, 6, 7, 8)));
  }

  private static byte[] nestedIso() {
    return collection(
        true,
        3007,
        0,
        point(false, 3001, 0, 1, 2, 3, 4),
        collection(false, 3007, 0, point(true, 3001, 0, 5, 6, 7, 8)));
  }

  private static byte[] adapt(byte[] source, byte[] expected) {
    byte[] before = source.clone();
    byte[] actual = PublicWkbHeaderAdapter.toIsoWkb(source);
    assertArrayEquals("Expected ISO headers and unchanged payload", expected, actual);
    assertArrayEquals("Adapter must not mutate retained source bytes", before, source);
    return actual;
  }

  private static void geometryAccepts(byte[] value) {
    Geometry geometry = Geometry.fromWkb(value, 4326);
    assertEquals("Explicit geometry SRID must be retained", 4326, geometry.srid());
    geometry.toWkb(ByteOrder.LITTLE_ENDIAN);
  }

  private static void geographyAccepts(byte[] value) {
    Geography geography = Geography.fromWkb(value, 4326);
    assertEquals("Explicit geography SRID must be retained", 4326, geography.srid());
    geography.toWkb(ByteOrder.LITTLE_ENDIAN);
  }

  private static void adapterRejects(byte[] source) {
    try {
      PublicWkbHeaderAdapter.toIsoWkb(source);
      fail("Adapter accepted structurally invalid input of length " + source.length);
    } catch (IllegalArgumentException expected) {
      // Structural validation belongs to the header walker.
    }
  }

  private static void sparkRejects(byte[] source, boolean geography) {
    byte[] iso = PublicWkbHeaderAdapter.toIsoWkb(source);
    try {
      if (geography) {
        Geography.fromWkb(iso, 4326);
      } else {
        Geometry.fromWkb(iso, 4326);
      }
      fail("Default Spark accepted semantically invalid input");
    } catch (RuntimeException expected) {
      // The adapter accepts the framing; Spark owns spatial semantic validation.
    }
  }

  private static void assertLazyCaches(WKBGeography geog) throws ReflectiveOperationException {
    for (String name : new String[] {"jtsGeometry", "s2Geography", "shapeIndexGeography"}) {
      Field field = WKBGeography.class.getDeclaredField(name);
      field.setAccessible(true);
      assertNull("Encoding must not initialize cache " + name, field.get(geog));
    }
  }
}
