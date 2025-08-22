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
package org.apache.sedona.bench;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark to test the performance of decoding Point and MultiPoint geography types with
 * different dimensions (XY, XYZ, XYZM) and numbers of points from WKT format.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class DecodeBenchPointWKT {

  @Param({"1", "16", "256", "4096"})
  public int points;

  @Param({"XY", "XYZ", "XYM", "XYZM"})
  public String dimension;

  private String wktPoint;
  private String wktMultiPoint;

  @Setup(Level.Trial)
  public void setup() {
    // POINT <Z|M|ZM> (...)
    wktPoint = String.format(Locale.ROOT, "POINT%s (%s)", dimToken(dimension), coord(0, dimension));

    // MULTIPOINT <Z|M|ZM> ((...),(...),...)
    StringBuilder sb = new StringBuilder("MULTIPOINT").append(dimToken(dimension)).append(" (");
    for (int i = 0; i < points; i++) {
      if (i > 0) sb.append(", ");
      sb.append('(').append(coord(i, dimension)).append(')');
    }
    sb.append(')');
    wktMultiPoint = sb.toString();
  }

  private static String dimToken(String dim) {
    switch (dim) {
      case "XY":
        return "";
      case "XYZ":
        return " Z";
      case "XYM":
        return " M";
      case "XYZM":
        return " ZM";
      default:
        throw new IllegalArgumentException("Unknown dimension: " + dim);
    }
  }

  // Emit coordinates with small non-zero Z/M to ensure real parsing work
  private static String coord(int i, String dim) {
    double x = i, y = i;
    double z = (i % 11) + 0.25;
    double m = i * 0.1 + 0.5;
    switch (dim) {
      case "XY":
        return String.format(Locale.ROOT, "%.6f %.6f", x, y);
      case "XYZ":
        return String.format(Locale.ROOT, "%.6f %.6f %.6f", x, y, z);
      case "XYM":
        return String.format(Locale.ROOT, "%.6f %.6f %.6f", x, y, m);
      case "XYZM":
        return String.format(Locale.ROOT, "%.6f %.6f %.6f %.6f", x, y, z, m);
      default:
        throw new IllegalArgumentException(dim);
    }
  }

  @Benchmark
  public void decode_point(Blackhole bh) throws Exception {
    Geography g = Constructors.geogFromWKT(wktPoint, 4326);
    bh.consume(g);
  }

  @Benchmark
  public void decode_multipoint(Blackhole bh) throws Exception {
    Geography g = Constructors.geogFromWKT(wktMultiPoint, 4326);
    bh.consume(g);
  }
}
