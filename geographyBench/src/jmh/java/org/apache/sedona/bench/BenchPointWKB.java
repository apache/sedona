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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class BenchPointWKB {

  @Param({"XY", "XYZ"})
  public String dim;

  @Param({"LE", "BE"})
  public String endianness;

  @Param({"1", "16", "256", "1024", "4096", "65536"})
  public int nPoints;

  // Fixtures
  private String wktPoint, wktMulti;
  private Geography geoPoint, geoMulti;

  // Hand-built payloads for READ benches (explicit WKB layout)
  private byte[] wkbReadPointLE, wkbReadPointBE;
  private byte[] wkbReadMultiLE, wkbReadMultiBE;

  @Setup(Level.Trial)
  public void setup() throws ParseException, IOException {
    wktPoint = buildPointWKT(dim);
    wktMulti = buildMultiPointWKT(nPoints, dim); // <-- double-paren per-point

    WKTReader wktReader = new WKTReader();
    geoPoint = wktReader.read(wktPoint);
    geoMulti = wktReader.read(wktMulti);

    // Precompute READ payloads with explicit layout the reader expects
    boolean isXYZ = "XYZ".equals(dim);
    wkbReadPointLE = buildPointWKB(true, isXYZ, 0);
    wkbReadPointBE = buildPointWKB(false, isXYZ, 0);
    wkbReadMultiLE = buildMultiPointWKB(true, isXYZ, nPoints);
    wkbReadMultiBE = buildMultiPointWKB(false, isXYZ, nPoints);
  }

  // ---------------- WRITE (Geography -> WKB) ----------------

  @Benchmark
  public double wkb_write_point(Blackhole bh, BenchPointWKB.ProfilerHook ph) throws IOException {
    return write(geoPoint, bh);
  }

  @Benchmark
  public double wkb_write_multipoint(Blackhole bh, BenchPointWKB.ProfilerHook ph)
      throws IOException {
    return write(geoMulti, bh);
  }

  private double write(Geography g, Blackhole bh) throws IOException {
    int outDims = "XY".equals(dim) ? 2 : 3;
    int order =
        "LE".equals(endianness) ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;
    WKBWriter writer = new WKBWriter(outDims, order);
    byte[] out = writer.write(g);
    long sum = 0;
    for (byte b : out) sum += (b & 0xFF);
    bh.consume(out);
    return (double) sum;
  }

  // ---------------- READ (WKB -> Geography) ----------------

  @Benchmark
  public void wkb_read_point(Blackhole bh, BenchPointWKB.ProfilerHook ph)
      throws IOException, ParseException {
    byte[] src = "LE".equals(endianness) ? wkbReadPointLE : wkbReadPointBE;
    readInto(src, bh);
  }

  @Benchmark
  public void wkb_read_multipoint(Blackhole bh, BenchPointWKB.ProfilerHook ph)
      throws IOException, ParseException {
    byte[] src = "LE".equals(endianness) ? wkbReadMultiLE : wkbReadMultiBE;
    readInto(src, bh);
  }

  private void readInto(byte[] src, Blackhole bh) throws IOException, ParseException {
    WKBReader reader = new WKBReader();
    Geography g = reader.read(src);
    bh.consume(g);
    bh.consume(g.numShapes());
  }

  // ---------------- Hand-built WKB for READ benches ----------------

  private static byte[] buildPointWKB(boolean little, boolean xyz, int index) {
    // (endianness)1 + (type)4 + coords
    int type = xyz ? 1001 : 1;
    int doubles = xyz ? 3 : 2;
    int len = 1 + 4 + 8 * doubles; // No coordinate count for a single Point

    ByteBuffer bb =
        ByteBuffer.allocate(len).order(little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    bb.put(little ? (byte) 1 : (byte) 0);
    bb.putInt(type);
    double[] c = pointCoord(index, xyz);
    bb.putDouble(c[0]);
    bb.putDouble(c[1]);
    if (xyz) bb.putDouble(c[2]);
    return bb.array();
  }

  private static byte[] buildMultiPointWKB(boolean little, boolean xyz, int n) {
    int pointType = xyz ? 1001 : 1;
    int multiType = xyz ? 1004 : 4;
    int doubles = xyz ? 3 : 2;

    // header: endian(1) + type(4) + count(4)
    int header = 1 + 4 + 4;
    // each point: endian(1) + type(4) + doubles*8
    int perPoint = 1 + 4 + 8 * doubles;

    ByteBuffer bb =
        ByteBuffer.allocate(header + n * perPoint)
            .order(little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

    // MultiPoint header
    bb.put(little ? (byte) 1 : (byte) 0);
    bb.putInt(multiType);
    bb.putInt(n);

    // Each Point as a full WKB geometry
    for (int i = 0; i < n; i++) {
      bb.put(little ? (byte) 1 : (byte) 0); // inner endian
      bb.putInt(pointType); // inner type (POINT)
      double[] c = pointCoord(i, xyz);
      bb.putDouble(c[0]);
      bb.putDouble(c[1]);
      if (xyz) bb.putDouble(c[2]);
    }
    return bb.array();
  }

  private static double[] pointCoord(int i, boolean xyz) {
    double x = 10.0 + i * 0.001;
    double y = 20.0 + i * 0.002;
    if (!xyz) return new double[] {x, y};
    double z = (i % 13) + 0.125;
    return new double[] {x, y, z};
  }

  // ---------------- WKT helpers (for writer fixtures) ----------------

  private static String buildPointWKT(String dim) {
    return "POINT" + ("XYZ".equals(dim) ? " Z" : "") + " (" + coord(0, dim) + ")";
  }

  // IMPORTANT: MULTIPOINT requires double-paren ((x y), (x y), ...)
  private static String buildMultiPointWKT(int n, String dim) {
    if (n <= 1) return buildPointWKT(dim);
    String dimToken = "XYZ".equals(dim) ? " Z" : "";
    String pts =
        IntStream.range(0, n)
            .mapToObj(i -> "(" + coord(i, dim) + ")") // wrap each point!
            .collect(Collectors.joining(", "));
    return "MULTIPOINT" + dimToken + " (" + pts + ")";
  }

  private static String coord(int i, String dim) {
    double x = 10.0 + i * 0.001;
    double y = 20.0 + i * 0.002;
    if ("XY".equals(dim)) return String.format(Locale.ROOT, "%.6f %.6f", x, y);
    double z = (i % 13) + 0.125;
    return String.format(Locale.ROOT, "%.6f %.6f %.6f", x, y, z);
  }

  // -------- Sanity: confirm shape count/bytes scale with nPoints --------
  @TearDown(Level.Trial)
  public void sanity() throws IOException {
    int outDims = "XY".equals(dim) ? 2 : 3;
    int order =
        "LE".equals(endianness) ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;
    WKBWriter w = new WKBWriter(outDims, order);
    byte[] out = w.write(geoMulti);

    int coordsPerPt = "XY".equals(dim) ? 2 : 3;
    int expectedApprox = 1 + 4 + 4 + nPoints * (1 + 4 + 8 * coordsPerPt); // header + N*(point)
    System.out.printf(
        "sanity: dim=%s order=%s n=%d shapes=%d bytes=%d (~%d)%n",
        dim, endianness, nPoints, geoMulti.numShapes(), out.length, expectedApprox);
  }

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
