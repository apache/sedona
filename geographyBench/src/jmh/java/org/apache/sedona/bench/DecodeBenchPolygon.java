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

import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.google.common.geometry.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import one.profiler.AsyncProfiler;
import org.apache.sedona.common.S2Geography.*;
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
public class DecodeBenchPolygon {

  // -------- Params --------
  @Param({"1", "16", "256", "1024"})
  public int numPolygons;

  @Param({"4", "16", "256", "1024"})
  public int verticesPerPolygon;

  @Param({"XY", "XYZ"})
  public String dimension;

  @Param({"COMPACT"})
  public String pointEncoding;

  // -------- Reused geometries --------
  private List<S2Polygon> polygons;

  // -------- Tagged payloads (POLYGON/MULTIPOLYGON) --------
  private byte[] taggedPolygonBytes;
  private byte[] taggedMultiPolygonBytes;
  private UnsafeInput taggedPolygonIn;
  private UnsafeInput taggedMultiPolygonIn;

  // -------- Raw coder payloads (using the SAME polygons) --------
  private byte[] rawMultiPolygonBytes;
  private UnsafeInput rawMultiPolygonIn;

  // ---------------- Setup ----------------
  @Setup(Level.Trial)
  public void setup() throws Exception {
    polygons = buildPolygons(numPolygons, verticesPerPolygon);

    // --- Tagged POLYGON ---
    {
      EncodeOptions opts = new EncodeOptions();
      applyPointEncodingPreference(opts, pointEncoding);
      Geography g = new PolygonGeography(polygons.get(0));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      g.encodeTagged(baos, opts);
      taggedPolygonBytes = baos.toByteArray();
    }

    // --- Tagged MULTIPOLYGON ---
    {
      EncodeOptions opts = new EncodeOptions();
      applyPointEncodingPreference(opts, pointEncoding);
      Geography g = new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, polygons);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      g.encodeTagged(baos, opts);
      taggedMultiPolygonBytes = baos.toByteArray();
    }

    // --- Raw S2 coder payload from the SAME polygons ---
    rawMultiPolygonBytes = encodeMultiPolygonPayload(polygons);

    // Inputs for tagged payloads
    taggedPolygonIn = new UnsafeInput(taggedPolygonBytes);
    taggedMultiPolygonIn = new UnsafeInput(taggedMultiPolygonBytes);

    // Input for raw payload
    rawMultiPolygonIn = new UnsafeInput(rawMultiPolygonBytes);

    System.out.printf(
        "numPolygons=%d, verticesPerPolygon=%d, enc=%s, tagged[POLYGON]=%dB, tagged[MULTIPOLYGON]=%dB, rawS2=%dB%n",
        numPolygons,
        verticesPerPolygon,
        pointEncoding,
        taggedPolygonBytes.length,
        taggedMultiPolygonBytes.length,
        rawMultiPolygonBytes.length);
  }

  @Setup(Level.Invocation)
  public void rewind() {
    // Rewind Kryo inputs
    taggedPolygonIn.rewind();
    taggedMultiPolygonIn.rewind();
    rawMultiPolygonIn.rewind();
  }

  // =====================================================================
  // == Benchmarks (TAGGED) ==
  // =====================================================================

  @Benchmark
  public void tagged_polygon_full(DecodeBenchPolygon.ProfilerHook ph, Blackhole bh)
      throws IOException {
    Geography g = Geography.decodeTagged(taggedPolygonIn);
    bh.consume(g);
  }

  @Benchmark
  public void tagged_multipolygon_decode(DecodeBenchPolygon.ProfilerHook ph, Blackhole bh)
      throws IOException {
    Geography g = Geography.decodeTagged(taggedMultiPolygonIn);
    bh.consume(g);
  }

  @Benchmark
  public double tagged_polygon_encode(DecodeBenchPolygon.ProfilerHook ph, Blackhole bh)
      throws IOException {
    EncodeOptions opts = new EncodeOptions();
    applyPointEncodingPreference(opts, pointEncoding);
    Geography g = new PolygonGeography(polygons.get(0));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    g.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();
    long s = 0;
    for (byte b : data) s += (b & 0xFF);
    bh.consume(data);
    return (double) s;
  }

  @Benchmark
  public double tagged_multipolygon_encode(DecodeBenchPolygon.ProfilerHook ph, Blackhole bh)
      throws IOException {
    EncodeOptions opts = new EncodeOptions();
    applyPointEncodingPreference(opts, pointEncoding);
    Geography g = new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, polygons);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    g.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();
    long s = 0;
    for (byte b : data) s += (b & 0xFF);
    bh.consume(data);
    return (double) s;
  }

  @Benchmark
  public int tagged_multipolygon_tagOnly() throws IOException {
    // Header + covering + count only (no geometry decode)
    EncodeTag tag = EncodeTag.decode(taggedMultiPolygonIn);
    tag.skipCovering(taggedMultiPolygonIn);
    int n = taggedMultiPolygonIn.readInt(false); // varint length of payload we wrote
    return n;
  }

  // =====================================================================
  // == Benchmarks (RAW S2 coder) ==
  // =====================================================================

  @Benchmark
  public double raw_S2multipolygon_decode(DecodeBenchPolygon.ProfilerHook ph) throws IOException {
    int b0 = rawMultiPolygonIn.read();
    int b1 = rawMultiPolygonIn.read();
    int b2 = rawMultiPolygonIn.read();
    int b3 = rawMultiPolygonIn.read();
    int count = (b0 & 0xFF) | ((b1 & 0xFF) << 8) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 24);

    List<S2Polygon> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      S2Polygon p = S2Polygon.decode(rawMultiPolygonIn);
      out.add(p);
    }
    double acc = 0;
    // Consume the data to prevent Dead Code Elimination (DCE)
    for (S2Polygon polygon : out) {
      acc += polygon.numLoops();
    }
    return acc;
  }

  @Benchmark
  public double raw_S2polygon_compact_encode(DecodeBenchPolygon.ProfilerHook ph, Blackhole bh)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);
    // LITTLE-ENDIAN count:
    int n = polygons.size();
    out.writeByte(n & 0xFF);
    out.writeByte((n >>> 8) & 0xFF);
    out.writeByte((n >>> 16) & 0xFF);
    out.writeByte((n >>> 24) & 0xFF);

    for (S2Polygon polygon : polygons) {
      // Use the high-level API: writes the coder id + payload that S2Polyline.decode expects
      S2Polygon.COMPACT_CODER.encode(polygon, out);
    }
    out.flush();
    // Materialize once to make the work observable & defeat DCE
    byte[] arr = out.toBytes();
    long s = 0;
    for (byte b : arr) s += (b & 0xFF);
    bh.consume(arr);
    return (double) s;
  }

  // =====================================================================
  // == Helpers ==
  // =====================================================================

  /**
   * Encode the list of S2Polygons into a raw S2 payload. The format is [count (varint)]
   * [polygon_1_bytes] [polygon_2_bytes] ...
   */
  static byte[] encodeMultiPolygonPayload(List<S2Polygon> polygons) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);
    // LITTLE-ENDIAN count:
    int n = polygons.size();
    out.writeByte(n & 0xFF);
    out.writeByte((n >>> 8) & 0xFF);
    out.writeByte((n >>> 16) & 0xFF);
    out.writeByte((n >>> 24) & 0xFF);

    for (S2Polygon polygon : polygons) {
      polygon.encode(out);
    }
    out.flush();
    return baos.toByteArray();
  }

  /** Builds a list of simple, non-overlapping S2Polygons. */
  public static List<S2Polygon> buildPolygons(int numPolygons, int verticesPerPolygon) {
    List<S2Polygon> result = new ArrayList<>(numPolygons);
    double radiusDegrees = 0.1; // Small radius for each polygon's vertices from its center
    for (int j = 0; j < numPolygons; j++) {
      // Shift each polygon center to avoid major overlaps
      double centerLat = -60.0 + j * 0.5;
      double centerLng = -170.0 + j * 0.5;
      S2Point center = S2LatLng.fromDegrees(centerLat, centerLng).toPoint();

      List<S2Point> vertices = new ArrayList<>();
      for (int i = 0; i < verticesPerPolygon; i++) {
        double angle = 2 * Math.PI * i / verticesPerPolygon;
        // Create points in a circle around the center point. Note: This is an approximation on a
        // sphere.
        double lat = centerLat + radiusDegrees * Math.cos(angle);
        double lng = centerLng + radiusDegrees * Math.sin(angle);
        vertices.add(S2LatLng.fromDegrees(lat, lng).toPoint());
      }
      S2Loop loop = new S2Loop(vertices);
      // Ensure the loop has the correct orientation (counter-clockwise for shells)
      loop.normalize();
      result.add(new S2Polygon(loop));
    }
    return result;
  }

  private static void applyPointEncodingPreference(EncodeOptions opts, String enc) {
    if ("COMPACT".equals(enc)) {
      opts.setEnableLazyDecode(false);
      opts.setCodingHint(EncodeOptions.CodingHint.COMPACT);
    } else {
      opts.setEnableLazyDecode(true);
    }
  }

  // =====================================================================
  // == Async-profiler hook (per-iteration, not inside the benchmark) ==
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
