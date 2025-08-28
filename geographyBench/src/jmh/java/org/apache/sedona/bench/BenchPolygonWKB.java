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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import one.profiler.AsyncProfiler;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.apache.sedona.common.S2Geography.WKBWriter;
import org.apache.sedona.common.S2Geography.WKTReader;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.*;
import org.openjdk.jmh.runner.IterationType;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class BenchPolygonWKB {
  /** XY or XYZ */
  @Param({"XY", "XYZ"})
  public String dim;

  /** number of polygons in MULTIPOLYGON */
  @Param({"1", "16", "256", "1024"})
  public int nPolygons;

  /** vertices per polygon outer ring (>=4; last coord auto‑closed) */
  @Param({"4", "16", "256", "1024"})
  public int nVerticesPerRing;

  /** WKB endianness */
  @Param({"LE", "BE"})
  public String endianness;

  // ---- Fixtures (prepared once per trial) ----
  private String wktPolygon;
  private String wktMultiPolygon;
  private Geography geoPolygon;
  private Geography geoMultiPolygon;
  // Payloads for READ benchmarks are now hand-built
  private byte[] wkbReadPolygonLE, wkbReadPolygonBE;
  private byte[] wkbReadMultiLE, wkbReadMultiBE;

  @Setup(Level.Trial)
  public void setup() throws ParseException, IOException {
    int v = Math.max(4, nVerticesPerRing);
    int p = Math.max(1, nPolygons);

    // WKT and Geography objects are still needed for the WRITE benchmarks
    wktPolygon = buildPolygonWKT(v, dim, 0);
    wktMultiPolygon = buildMultiPolygonWKT(p, v, dim);
    WKTReader wktReader = new WKTReader();
    geoPolygon = wktReader.read(wktPolygon);
    geoMultiPolygon = wktReader.read(wktMultiPolygon);

    // THE FIX: Build custom WKB for the READ benchmarks
    boolean isXYZ = "XYZ".equals(dim);
    wkbReadPolygonLE = buildPolygonWKB(true, isXYZ, v, 0);
    wkbReadPolygonBE = buildPolygonWKB(false, isXYZ, v, 0);
    wkbReadMultiLE = buildMultiPolygonWKB(true, isXYZ, p, v);
    wkbReadMultiBE = buildMultiPolygonWKB(false, isXYZ, p, v);
  }

  // ---- WRITE (Geography -> WKB) ----
  @Benchmark
  public double wkb_write_polygon(Blackhole bh, BenchPolygonWKB.ProfilerHook ph)
      throws IOException {
    return write(geoPolygon, bh);
  }

  @Benchmark
  public double wkb_write_multipolygon(Blackhole bh, BenchPolygonWKB.ProfilerHook ph)
      throws IOException {
    return write(geoMultiPolygon, bh);
  }

  private double write(Geography g, Blackhole bh) throws IOException {
    int outDims = ("XY".equals(dim) ? 2 : 3);
    int order =
        "LE".equals(endianness) ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;
    WKBWriter writer = new WKBWriter(outDims, order);
    byte[] out = writer.write(g);
    long sum = 0;
    for (byte b : out) sum += (b & 0xFF); // prevent DCE
    bh.consume(out);
    return (double) sum;
  }

  // ---- READ (WKB -> Geography) ----
  @Benchmark
  public void wkb_read_polygon(Blackhole bh, BenchPolygonWKB.ProfilerHook ph)
      throws IOException, ParseException {
    read(("LE".equals(endianness) ? wkbReadPolygonLE : wkbReadPolygonBE), bh);
  }

  @Benchmark
  public void wkb_read_multipolygon(Blackhole bh, BenchPolygonWKB.ProfilerHook ph)
      throws IOException, ParseException {
    read(("LE".equals(endianness) ? wkbReadMultiLE : wkbReadMultiBE), bh);
  }

  private void read(byte[] src, Blackhole bh) throws IOException, ParseException {
    WKBReader reader = new WKBReader();
    Geography g = reader.read(src);
    bh.consume(g);
    bh.consume(g.numShapes());
  }

  // ---- Hand-built WKB for READ benches ----

  private static byte[] buildPolygonWKB(boolean little, boolean xyz, int nVertices, int polyIndex) {
    int type = xyz ? 1003 : 3; // Polygon type
    int doubles = xyz ? 3 : 2;
    // endian, type, num_rings, num_coords, coords
    int len = 1 + 4 + 4 + 4 + (8 * doubles * nVertices);

    ByteBuffer bb =
        ByteBuffer.allocate(len).order(little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    bb.put(little ? (byte) 1 : (byte) 0);
    bb.putInt(type);
    bb.putInt(1); // num_rings = 1 (simple polygon with no holes)
    bb.putInt(nVertices);

    // Build coordinates for the ring
    double cx = -170.0 + polyIndex * 0.5;
    double cy = -60.0 + polyIndex * 0.5;
    double rDeg = 0.1 + (polyIndex % 5) * 0.02;

    for (int i = 0; i < nVertices - 1; i++) {
      double ang = (2.0 * Math.PI * i) / (nVertices - 1);
      double x = cx + rDeg * Math.cos(ang);
      double y = cy + rDeg * Math.sin(ang);
      putCoord(bb, x, y, xyz, i);
    }
    // Close the ring
    putCoord(bb, cx + rDeg, cy, xyz, 0);

    return bb.array();
  }

  private static byte[] buildMultiPolygonWKB(
      boolean little, boolean xyz, int nPolys, int nVertices) {
    int multiType = xyz ? 1006 : 6; // MultiPolygon type
    int header = 1 + 4 + 4; // endian, type, num_polygons
    // Get the size of a single polygon WKB to calculate total length
    byte[] singlePoly = buildPolygonWKB(little, xyz, nVertices, 0);
    int len = header + (nPolys * singlePoly.length);

    ByteBuffer bb =
        ByteBuffer.allocate(len).order(little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    bb.put(little ? (byte) 1 : (byte) 0);
    bb.putInt(multiType);
    bb.putInt(nPolys);
    for (int i = 0; i < nPolys; i++) {
      // The reader expects each polygon to be a full, self-contained WKB geometry
      bb.put(buildPolygonWKB(little, xyz, nVertices, i));
    }
    return bb.array();
  }

  private static void putCoord(ByteBuffer bb, double x, double y, boolean xyz, int i) {
    bb.putDouble(x);
    bb.putDouble(y);
    if (xyz) {
      double z = (i % 19) + 0.5;
      bb.putDouble(z);
    }
  }

  // ---- Helpers: WKT generators ----

  private static String buildPolygonWKT(int verticesPerRing, String dim, int polyIndex) {
    String dimToken = "XYZ".equals(dim) ? " Z" : "";
    String ring = buildRing(verticesPerRing, dim, polyIndex);
    return "POLYGON" + dimToken + " ((" + ring + "))";
  }

  private static String buildMultiPolygonWKT(int p, int verticesPerRing, String dim) {
    String dimToken = "XYZ".equals(dim) ? " Z" : "";
    String polys =
        IntStream.range(0, p)
            .mapToObj(i -> "((" + buildRing(verticesPerRing, dim, i) + "))")
            .collect(Collectors.joining(", "));
    return "MULTIPOLYGON" + dimToken + " (" + polys + ")";
  }

  private static String buildRing(int v, String dim, int polyIndex) {
    if (v < 4) throw new IllegalArgumentException("Polygon ring must have >= 4 vertices");
    double cx = -170.0 + polyIndex * 0.5;
    double cy = -60.0 + polyIndex * 0.5;
    double rDeg = 0.1 + (polyIndex % 5) * 0.02;

    String vertices =
        IntStream.range(0, v - 1)
            .mapToObj(
                i -> {
                  double ang = (2.0 * Math.PI * i) / (v - 1);
                  double x = cx + rDeg * Math.cos(ang);
                  double y = cy + rDeg * Math.sin(ang);
                  return formatCoord(x, y, dim, i);
                })
            .collect(Collectors.joining(", "));

    String first = formatCoord(cx + rDeg, cy, dim, 0);
    return vertices + ", " + first;
  }

  private static String formatCoord(double x, double y, String dim, int i) {
    if ("XY".equals(dim)) {
      return String.format(Locale.ROOT, "%.6f %.6f", x, y);
    } else {
      double z = (i % 19) + 0.5;
      return String.format(Locale.ROOT, "%.6f %.6f %.6f", x, y, z);
    }
  }

  // =====================================================================
  // == Async-profiler hook (runs inside the fork) ==
  // =====================================================================

  // -------- Async-profiler hook (runs inside fork) --------
  /** Per-iteration profiler: start on measurement iterations, stop after each iteration. */
  @State(Scope.Benchmark)
  public static class ProfilerHook {
    @Param({"cpu"})
    public String event;

    @Param({"jfr"})
    public String format;

    @Param({"1ms"})
    public String interval;

    private AsyncProfiler profiler;
    private Path outDir;

    @Setup(Level.Trial)
    public void trial() throws Exception {
      profiler = AsyncProfiler.getInstance();
      outDir = Paths.get("profiles");
      Files.createDirectories(outDir);
    }

    @Setup(Level.Iteration)
    public void start(BenchmarkParams b, IterationParams it) throws Exception {
      if (it.getType() != IterationType.MEASUREMENT) return;
      String base = String.format("%s-iter%02d-%s", b.getBenchmark(), it.getCount(), event);
      File out =
          outDir
              .resolve(base + (format.equalsIgnoreCase("jfr") ? ".jfr" : ".html"))
              .toAbsolutePath()
              .toFile();

      // Using 'all-user' helps the profiler find the correct forked JMH process.
      // The filter is removed to avoid accidentally hiding the benchmark thread.
      String common = String.format("event=%s,interval=%s,cstack=fp,threads", event, interval);

      if ("jfr".equalsIgnoreCase(format)) {
        profiler.execute("start," + common + ",jfr,file=" + out.getAbsolutePath());
      } else {
        profiler.execute("start," + common);
        System.setProperty("ap.out", out.getAbsolutePath());
        System.setProperty("ap.format", format);
      }
    }

    @TearDown(Level.Iteration)
    public void stop(IterationParams it) throws Exception {
      if (it.getType() != IterationType.MEASUREMENT) return;
      if ("jfr".equalsIgnoreCase(format)) {
        profiler.execute("stop");
      } else {
        String file = System.getProperty("ap.out");
        String fmt = System.getProperty("ap.format", "flamegraph");
        profiler.execute(String.format("stop,file=%s,output=%s", file, fmt));
      }
    }
  }
}
