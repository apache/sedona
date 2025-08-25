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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import one.profiler.AsyncProfiler;
import one.profiler.AsyncProfilerLoader;
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
public class BenchPolygonWKB {
  /** Limit dimension to XY / XYZ only */
  @Param({"XY", "XYZ"})
  public String dim;

  /** number of polygons in MULTIPOLYGON */
  @Param({"1", "1", "1", "1", "16", "256", "1028"})
  public int nPolygons;

  /** number of vertices per polygon ring (must be >= 4) */
  @Param({"4", "16", "256", "1028", "1028", "1028"})
  public int nVerticesPerRing;

  /** WKB endianness */
  @Param({"LE", "BE"})
  public String endianness;

  // Fixtures prepared once per trial
  private String wkt;
  private Geography geo;
  private byte[] wkbLE;
  private byte[] wkbBE;

  @Setup(Level.Trial)
  public void setup() throws ParseException, IOException, org.locationtech.jts.io.ParseException {
    wkt = buildMultiPolygonWKT(nPolygons, nVerticesPerRing, dim);

    // Precompute Geography for writer bench
    WKTReader wktReader = new WKTReader();
    geo = wktReader.read(wkt);

    // Precompute WKB for reader bench
    int outDims = ("XY".equals(dim) ? 2 : 3);
    WKBWriter le = new WKBWriter(outDims, ByteOrderValues.LITTLE_ENDIAN);
    WKBWriter be = new WKBWriter(outDims, ByteOrderValues.BIG_ENDIAN);
    wkbLE = le.write(geo);
    wkbBE = be.write(geo);
  }

  /** WKT → Geography (parse only) */
  @Benchmark
  public void wkt_read(Blackhole bh) throws ParseException, org.locationtech.jts.io.ParseException {
    WKTReader reader = new WKTReader();
    Geography g = reader.read(wkt);
    bh.consume(g);
    bh.consume(g.numShapes());
  }

  /** Geography → WKB (serialize only) */
  @Benchmark
  public double wkb_write(Blackhole bh, ProfilerHook ph) throws IOException {
    // choose output dimensions (2 = XY, 3 = XYZ)
    int outDims = ("XY".equals(dim) ? 2 : 3);
    int order =
        "LE".equals(endianness) ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;

    // do the actual write
    WKBWriter writer = new WKBWriter(outDims, order);
    byte[] out = writer.write(geo);
    long sum = 0;
    for (byte b : out) {
      sum += (b & 0xFF);
    }
    bh.consume(out);
    return (double) sum;
  }

  /** WKB → Geography (deserialize only) */
  @Benchmark
  public void wkb_read(Blackhole bh, ProfilerHook ph)
      throws IOException, org.locationtech.jts.io.ParseException {
    WKBReader reader = new WKBReader();
    byte[] src = "LE".equals(endianness) ? wkbLE : wkbBE;
    Geography g = reader.read(src);
    bh.consume(g);
    bh.consume(g.numShapes());
  }

  // ---------- helpers ----------

  private static String buildMultiPolygonWKT(int numPolygons, int numVerticesPerRing, String dim) {
    if (numVerticesPerRing < 4) {
      throw new IllegalArgumentException(
          "A polygon ring must have at least 4 vertices to be closed.");
    }
    String dimToken = "XYZ".equals(dim) ? " Z" : "";
    String polygons =
        IntStream.range(0, numPolygons)
            .mapToObj(
                i -> {
                  // Generate vertices for the outer ring
                  int baseCoordIndex = i * numVerticesPerRing;
                  String firstCoord = coord(baseCoordIndex, dim);
                  String vertices =
                      IntStream.range(0, numVerticesPerRing - 1)
                          .mapToObj(j -> coord(baseCoordIndex + j, dim))
                          .collect(Collectors.joining(", "));
                  // A ring must be closed, so the last coordinate is the same as the first
                  String ring = vertices + ", " + firstCoord;
                  // For simplicity, we only generate polygons with one outer ring and no holes.
                  return "((" + ring + "))";
                })
            .collect(Collectors.joining(", "));
    return "MULTIPOLYGON" + dimToken + " (" + polygons + ")";
  }

  // Emit small non-zero Z in XYZ to ensure real parsing work
  private static String coord(int i, String dim) {
    // Generate non-overlapping convex polygons for simplicity
    double angle = 2 * Math.PI * (i % 100) / 100;
    double radius = 10 + (i / 100);
    double x = radius * Math.cos(angle);
    double y = radius * Math.sin(angle);

    if ("XY".equals(dim)) {
      return String.format(Locale.ROOT, "%.6f %.6f", x, y);
    } else { // XYZ
      double z = (i % 11) + 0.25;
      return String.format(Locale.ROOT, "%.6f %.6f %.6f", x, y, z);
    }
  }
  // =====================================================================
  // == Async-profiler hook (per-iteration, not inside the benchmark) ==
  // =====================================================================

  /** Per-iteration profiler: start on measurement iterations, stop after each iteration. */
  @State(Scope.Benchmark)
  public static class ProfilerHook {
    @Param({"cpu"})
    public String event; // cpu | alloc | wall | lock

    @Param({"jfr"})
    public String format; // jfr | flamegraph | collapsed

    @Param({"1ms"})
    public String interval; // e.g., 1ms (for CPU), ignored by alloc

    private AsyncProfiler profiler;
    private Path outDir;

    @Setup(Level.Trial)
    public void trial() throws Exception {
      profiler = AsyncProfilerLoader.load();
      outDir = Paths.get("profiles");
      Files.createDirectories(outDir);
    }

    @Setup(Level.Iteration)
    public void start(BenchmarkParams b, IterationParams it) throws Exception {
      if (it.getType() != IterationType.MEASUREMENT) return; // skip warmups
      // Make a readable, unique file per iteration
      String base = String.format("%s-iter%02d-%s", b.getBenchmark(), it.getCount(), event);
      File out =
          outDir
              .resolve(base + (format.equals("jfr") ? ".jfr" : ".html"))
              .toAbsolutePath()
              .toFile();
      if ("jfr".equals(format)) {
        profiler.execute(
            String.format("start,jfr,event=%s,interval=%s,file=%s", event, interval, out));
      } else {
        // For non-JFR, start now; we'll set file/output on stop
        profiler.execute(String.format("start,event=%s,interval=%s", event, interval));
        System.setProperty("ap.out", out.getAbsolutePath());
        System.setProperty("ap.format", format);
      }
    }

    @TearDown(Level.Iteration)
    public void stop(IterationParams it) throws Exception {
      if (it.getType() != IterationType.MEASUREMENT) return;
      if ("jfr".equals(format)) {
        profiler.execute("stop");
      } else {
        String file = System.getProperty("ap.out");
        String fmt = System.getProperty("ap.format", "flamegraph");
        profiler.execute(String.format("stop,file=%s,output=%s", file, fmt));
      }
    }
  }
}
