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
public class BenchPolylineWKB {

  /** XY or XYZ */
  @Param({"XY", "XYZ"})
  public String dim;

  /** number of lines in MULTILINESTRING */
  @Param({"1", "16", "256", "1024"})
  public int nLines;

  /** number of vertices per line (min 2) */
  @Param({"2", "16", "256", "1024"})
  public int nVerticesPerLine;

  /** WKB endianness */
  @Param({"LE", "BE"})
  public String endianness;

  // ---- Fixtures (prepared once per trial) ----
  private String wktLine;
  private String wktMulti;
  private Geography geoLine;
  private Geography geoMulti;
  private byte[] wkbReadSingleLE, wkbReadSingleBE;
  private byte[] wkbReadMultiLE, wkbReadMultiBE;

  @Setup(Level.Trial)
  public void setup() throws ParseException, IOException {
    wktLine = buildLineWKT(Math.max(2, nVerticesPerLine), dim);
    wktMulti = buildMultiLineWKT(Math.max(1, nLines), Math.max(2, nVerticesPerLine), dim);

    WKTReader wktReader = new WKTReader();
    geoLine = wktReader.read(wktLine);
    geoMulti = wktReader.read(wktMulti);

    boolean isXYZ = "XYZ".equals(dim);
    wkbReadSingleLE = buildLineWKB(true, isXYZ, nVerticesPerLine);
    wkbReadSingleBE = buildLineWKB(false, isXYZ, nVerticesPerLine);
    wkbReadMultiLE = buildMultiLineWKB(true, isXYZ, nLines, nVerticesPerLine);
    wkbReadMultiBE = buildMultiLineWKB(false, isXYZ, nLines, nVerticesPerLine);
  }

  // ---- WRITE (Geography -> WKB) ----
  @Benchmark
  public double wkb_write_line(Blackhole bh, BenchPolylineWKB.ProfilerHook ph) throws IOException {
    return write(geoLine, bh);
  }

  @Benchmark
  public double wkb_write_multiline(Blackhole bh, BenchPolylineWKB.ProfilerHook ph)
      throws IOException {
    return write(geoMulti, bh);
  }

  private double write(Geography g, Blackhole bh) throws IOException {
    int outDims = ("XY".equals(dim) ? 2 : 3);
    int order =
        "LE".equals(endianness) ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;
    WKBWriter writer = new WKBWriter(outDims, order);
    byte[] out = writer.write(g);
    long sum = 0;
    for (byte b : out) sum += (b & 0xFF);
    bh.consume(out);
    return (double) sum;
  }

  // ---- READ (WKB -> Geography) ----
  @Benchmark
  public void wkb_read_line(Blackhole bh, BenchPolylineWKB.ProfilerHook ph)
      throws IOException, ParseException {
    read(("LE".equals(endianness) ? wkbReadSingleLE : wkbReadSingleBE), bh);
  }

  @Benchmark
  public void wkb_read_multiline(Blackhole bh, BenchPolylineWKB.ProfilerHook ph)
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

  private static byte[] buildLineWKB(boolean little, boolean xyz, int nVertices) {
    int type = xyz ? 1002 : 2; // LineString type
    int doubles = xyz ? 3 : 2;
    int len = 1 + 4 + 4 + (8 * doubles * nVertices); // endian, type, count, coords

    ByteBuffer bb =
        ByteBuffer.allocate(len).order(little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    bb.put(little ? (byte) 1 : (byte) 0);
    bb.putInt(type);
    bb.putInt(nVertices);
    for (int i = 0; i < nVertices; i++) {
      double[] c = pointCoord(i, xyz);
      bb.putDouble(c[0]);
      bb.putDouble(c[1]);
      if (xyz) bb.putDouble(c[2]);
    }
    return bb.array();
  }

  private static byte[] buildMultiLineWKB(boolean little, boolean xyz, int nLines, int nVertices) {
    int multiType = xyz ? 1005 : 5; // MultiLineString type
    int header = 1 + 4 + 4; // endian, type, num_lines
    byte[] singleLine = buildLineWKB(little, xyz, nVertices);
    int len = header + (nLines * singleLine.length);

    ByteBuffer bb =
        ByteBuffer.allocate(len).order(little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    bb.put(little ? (byte) 1 : (byte) 0);
    bb.putInt(multiType);
    bb.putInt(nLines);
    for (int i = 0; i < nLines; i++) {
      bb.put(singleLine);
    }
    return bb.array();
  }

  private static double[] pointCoord(int i, boolean xyz) {
    double x = 100.0 + i * 0.001;
    double y = 50.0 + i * 0.002;
    if (!xyz) return new double[] {x, y};
    double z = (i % 17) + 0.375;
    return new double[] {x, y, z};
  }

  // ---- Helpers ----
  private static String buildLineWKT(int nVertices, String dim) {
    String dimToken = "XYZ".equals(dim) ? " Z" : "";
    String coords =
        IntStream.range(0, nVertices)
            .mapToObj(i -> coord(i, dim))
            .collect(Collectors.joining(", "));
    return "LINESTRING" + dimToken + " (" + coords + ")";
  }

  private static String buildMultiLineWKT(int numLines, int numVerticesPerLine, String dim) {
    String dimToken = "XYZ".equals(dim) ? " Z" : "";
    String lines =
        IntStream.range(0, numLines)
            .mapToObj(
                i -> {
                  String coords =
                      IntStream.range(0, numVerticesPerLine)
                          .mapToObj(j -> coord(i * numVerticesPerLine + j, dim))
                          .collect(Collectors.joining(", "));
                  return "(" + coords + ")";
                })
            .collect(Collectors.joining(", "));
    return "MULTILINESTRING" + dimToken + " (" + lines + ")";
  }

  // emit deterministic coords; add non-zero Z in XYZ
  private static String coord(int i, String dim) {
    double x = 100.0 + i * 0.001;
    double y = 50.0 + i * 0.002;
    if ("XY".equals(dim)) {
      return String.format(Locale.ROOT, "%.6f %.6f", x, y);
    } else {
      double z = (i % 17) + 0.375;
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
