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
import com.google.common.geometry.PrimitiveArrays;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
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
import org.apache.sedona.common.S2Geography.EncodeOptions;
import org.apache.sedona.common.S2Geography.EncodeTag;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.PointGeography;
import org.apache.sedona.common.S2Geography.SinglePointGeography;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Thread)
public class DecodeBenchPoint {

  // -------- Params --------
  @Param({"1", "16", "256", "1024", "4096", "65536"})
  public int points;

  @Param({"XY", "XYZ"})
  public String dimension;

  @Param({"COMPACT", "FAST"})
  public String pointEncoding;

  // -------- Reused points --------
  private List<S2Point> pts;

  // -------- Tagged payloads (POINT/MULTIPOINT) --------
  private byte[] taggedPointBytes;
  private byte[] taggedMultiPointBytes;
  private UnsafeInput taggedPointIn;
  private UnsafeInput taggedMultiPointIn;

  // -------- Raw coder payloads (using the SAME pts) --------
  private PrimitiveArrays.Bytes rawCompactBytesAdapter;
  private PrimitiveArrays.Cursor compactCur;
  private PrimitiveArrays.Bytes rawFastBytesAdapter;
  private PrimitiveArrays.Cursor fastCur;

  // ---------------- Setup ----------------
  @Setup(Level.Trial)
  public void setup() throws Exception {
    pts = buildPoints(points);

    // --- Tagged POINT ---
    {
      EncodeOptions opts = new EncodeOptions();
      applyPointEncodingPreference(opts, pointEncoding);
      Geography g = new SinglePointGeography(pts.get(0));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      g.encodeTagged(baos, opts);
      taggedPointBytes = baos.toByteArray();
    }

    // --- Tagged MULTIPOINT ---
    {
      EncodeOptions opts = new EncodeOptions();
      applyPointEncodingPreference(opts, pointEncoding);
      Geography g = new PointGeography(pts);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      g.encodeTagged(baos, opts);
      taggedMultiPointBytes = baos.toByteArray();
    }

    // --- Raw coder payloads from the SAME points (COMPACT & FAST) ---
    byte[] rawCompactBytes = encodePointsPayload(pts, /*compact=*/ true);
    byte[] rawFastBytes = encodePointsPayload(pts, /*compact=*/ false);

    // Inputs for tagged payloads
    taggedPointIn = new UnsafeInput(taggedPointBytes);
    taggedMultiPointIn = new UnsafeInput(taggedMultiPointBytes);

    // Bytes adapters & cursors
    rawCompactBytesAdapter = bytesOverArray(rawCompactBytes);
    compactCur = rawCompactBytesAdapter.cursor();
    rawFastBytesAdapter = bytesOverArray(rawFastBytes);
    fastCur = rawFastBytesAdapter.cursor();

    System.out.printf(
        "points=%d enc=%s  tagged[POINT]=%dB  tagged[MULTIPOINT]=%dB  rawCompact=%dB  rawFast=%dB%n",
        points,
        pointEncoding,
        taggedPointBytes.length,
        taggedMultiPointBytes.length,
        rawCompactBytes.length,
        rawFastBytes.length);
  }

  @Setup(Level.Invocation)
  public void rewind() {
    // Rewind Kryo inputs
    taggedPointIn.rewind();
    taggedMultiPointIn.rewind();
    // Reset S2 cursor positions
    compactCur.position = 0;
    fastCur.position = 0;
  }

  // =====================================================================
  // == Benchmarks (TAGGED) ==
  // =====================================================================

  @Benchmark
  public void tagged_point_decode(DecodeBenchPoint.ProfilerHook ph, Blackhole bh)
      throws IOException {
    Geography g = Geography.decodeTagged(taggedPointIn);
    bh.consume(g);
  }

  @Benchmark
  public void tagged_multipoint_decode(DecodeBenchPoint.ProfilerHook ph, Blackhole bh)
      throws IOException {
    // Note: profiling is handled per-iteration by ProfilerHook; keep the body clean.
    Geography g = Geography.decodeTagged(taggedMultiPointIn);
    bh.consume(g);
  }

  @Benchmark
  public double tagged_multipoint_encode(DecodeBenchPoint.ProfilerHook ph, Blackhole bh)
      throws IOException {
    EncodeOptions opts = new EncodeOptions();
    applyPointEncodingPreference(opts, pointEncoding);
    Geography g = new PointGeography(pts);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    g.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();
    long s = 0;
    for (byte b : data) s += (b & 0xFF);
    bh.consume(data);
    return (double) s;
  }

  @Benchmark
  public double tagged_point_encode(DecodeBenchPoint.ProfilerHook ph, Blackhole bh)
      throws IOException {
    EncodeOptions opts = new EncodeOptions();
    applyPointEncodingPreference(opts, pointEncoding);
    Geography g = new SinglePointGeography(pts.get(0));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    g.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();
    long s = 0;
    for (byte b : data) s += (b & 0xFF);
    bh.consume(data);
    return (double) s;
  }

  @Benchmark
  public int tagged_multipoint_tagOnly() throws IOException {
    // Header + covering + count only (no point decode)
    EncodeTag tag = EncodeTag.decode(taggedMultiPointIn);
    tag.skipCovering(taggedMultiPointIn);
    int n = taggedMultiPointIn.readInt(false); // varint length of payload we wrote
    return n;
  }

  // =====================================================================
  // == Benchmarks (RAW S2 coder) ==
  // =====================================================================

  @Benchmark
  public double raw_S2points_compact_decode(DecodeBenchPoint.ProfilerHook ph) throws IOException {
    List<S2Point> out = S2Point.Shape.COMPACT_CODER.decode(rawCompactBytesAdapter, compactCur);
    double acc = 0;
    for (int i = 0; i < out.size(); i++) {
      S2Point p = out.get(i);
      acc += p.getX() + p.getY() + p.getZ();
    }
    return acc; // returning prevents DCE
  }

  @Benchmark
  public double raw_S2points_fast_decode(DecodeBenchPoint.ProfilerHook ph) throws IOException {
    List<S2Point> out = S2Point.Shape.FAST_CODER.decode(rawFastBytesAdapter, fastCur);
    double acc = 0;
    for (int i = 0; i < out.size(); i++) {
      S2Point p = out.get(i);
      acc += p.getX() + p.getY() + p.getZ();
    }
    return acc; // returning prevents DCE
  }

  @Benchmark
  public double raw_S2points_compact_encode(DecodeBenchPoint.ProfilerHook ph, Blackhole bh)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);
    S2Point.Shape.COMPACT_CODER.encode(S2Point.Shape.fromList(pts), out);
    // Materialize once to make the work observable & defeat DCE
    byte[] arr = out.toBytes();
    long s = 0;
    for (byte b : arr) s += (b & 0xFF);
    bh.consume(arr);
    return (double) s;
  }

  @Benchmark
  public double raw_S2points_fast_encode(DecodeBenchPoint.ProfilerHook ph, Blackhole bh)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);
    S2Point.Shape.FAST_CODER.encode(S2Point.Shape.fromList(pts), out);
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

  /** Encode the list of S2Points into the raw S2 payload for COMPACT/FAST coder. */
  static byte[] encodePointsPayload(List<S2Point> points, boolean compact) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);
    if (compact) {
      S2Point.Shape.COMPACT_CODER.encode(S2Point.Shape.fromList(points), out);
    } else {
      S2Point.Shape.FAST_CODER.encode(S2Point.Shape.fromList(points), out);
    }
    out.flush();
    return baos.toByteArray();
  }

  private static List<S2Point> buildPoints(int n) {
    // Deterministic pseudo-grid; S2Point is inherently 3D, 'dimension' is retained as a param only
    List<S2Point> out = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      double lat = -60.0 + (i % 120);
      double lng = -170.0 + (i % 340);
      out.add(S2LatLng.fromDegrees(lat, lng).toPoint());
    }
    return out;
  }

  private static void applyPointEncodingPreference(EncodeOptions opts, String enc) {
    if ("COMPACT".equals(enc)) {
      opts.setCodingHint(EncodeOptions.CodingHint.COMPACT);
      opts.setEnableLazyDecode(false);
    } else if ("FAST".equals(enc)) {
      opts.setCodingHint(EncodeOptions.CodingHint.FAST);
      opts.setEnableLazyDecode(true);
    }
  }

  private static PrimitiveArrays.Bytes bytesOverArray(byte[] arr) {
    return bytesOverSlice(arr, 0, arr.length);
  }

  private static PrimitiveArrays.Bytes bytesOverSlice(byte[] buf, int off, int len) {
    return new PrimitiveArrays.Bytes() {
      @Override
      public long length() {
        return len;
      }

      @Override
      public byte get(long i) {
        return buf[off + (int) i];
      }
    };
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
