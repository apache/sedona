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
public class DecodeBenchPolyline {

  // -------- Params --------
  @Param({"1", "16", "256", "1024"})
  public int numPolylines;

  @Param({"2", "16", "256", "1024"})
  public int verticesPerPolyline;

  @Param({"XY", "XYZ"})
  public String dimension;

  @Param({"COMPACT"})
  public String pointEncoding;

  // -------- Reused geometries --------
  private List<S2Polyline> polylines;

  // -------- Tagged payloads (POLYLINE/MULTIPOLYLINE) --------
  private byte[] taggedPolylineBytes;
  private byte[] taggedMultiPolylineBytes;
  private UnsafeInput taggedPolylineIn;
  private UnsafeInput taggedMultiPolylineIn;

  // -------- Raw coder payloads (using the SAME polylines) --------
  private byte[] rawMultiPolylineBytes;
  private UnsafeInput rawMultiPolylineIn;

  // ---------------- Setup ----------------
  @Setup(Level.Trial)
  public void setup() throws Exception {
    polylines = buildPolylines(numPolylines, verticesPerPolyline);

    // --- Tagged POLYLINE ---
    {
      EncodeOptions opts = new EncodeOptions();
      applyPointEncodingPreference(opts, pointEncoding);
      Geography g = new SinglePolylineGeography(polylines.get(0));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      g.encodeTagged(baos, opts);
      taggedPolylineBytes = baos.toByteArray();
    }

    // --- Tagged MULTIPOLYLINE ---
    {
      EncodeOptions opts = new EncodeOptions();
      applyPointEncodingPreference(opts, pointEncoding);
      Geography g = new PolylineGeography(polylines);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      g.encodeTagged(baos, opts);
      taggedMultiPolylineBytes = baos.toByteArray();
    }

    // --- Raw S2 coder payload from the SAME polylines ---
    rawMultiPolylineBytes = encodeMultiPolylinePayload(polylines);

    // Inputs for tagged payloads
    taggedPolylineIn = new UnsafeInput(taggedPolylineBytes);
    taggedMultiPolylineIn = new UnsafeInput(taggedMultiPolylineBytes);

    // Input for raw payload
    rawMultiPolylineIn = new UnsafeInput(rawMultiPolylineBytes);

    System.out.printf(
        "numPolylines=%d, verticesPerPolyline=%d, enc=%s, tagged[POLYLINE]=%dB, tagged[MULTIPOLYLINE]=%dB, rawS2=%dB%n",
        numPolylines,
        verticesPerPolyline,
        pointEncoding,
        taggedPolylineBytes.length,
        taggedMultiPolylineBytes.length,
        rawMultiPolylineBytes.length);
  }

  @Setup(Level.Invocation)
  public void rewind() {
    // Rewind Kryo inputs
    taggedPolylineIn.rewind();
    taggedMultiPolylineIn.rewind();
    rawMultiPolylineIn.rewind();
  }

  // =====================================================================
  // == Benchmarks (TAGGED) ==
  // =====================================================================

  @Benchmark
  public void tagged_polyline_decode(DecodeBenchPolyline.ProfilerHook ph, Blackhole bh)
      throws IOException {
    Geography g = Geography.decodeTagged(taggedPolylineIn);
    bh.consume(g);
  }

  @Benchmark
  public void tagged_multipolyline_decode(DecodeBenchPolyline.ProfilerHook ph, Blackhole bh)
      throws IOException {
    // Note: profiling is handled per-iteration by ProfilerHook; keep the body clean.
    Geography g = Geography.decodeTagged(taggedMultiPolylineIn);
    bh.consume(g);
  }

  @Benchmark
  public double tagged_polyline_encode(DecodeBenchPolyline.ProfilerHook ph, Blackhole bh)
      throws IOException {
    EncodeOptions opts = new EncodeOptions();
    applyPointEncodingPreference(opts, pointEncoding);
    Geography g = new PolylineGeography(polylines.get(0));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    g.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();
    long s = 0;
    for (byte b : data) s += (b & 0xFF);
    bh.consume(data);
    return (double) s;
  }

  @Benchmark
  public double tagged_multipolyline_encode(DecodeBenchPolyline.ProfilerHook ph, Blackhole bh)
      throws IOException {
    EncodeOptions opts = new EncodeOptions();
    applyPointEncodingPreference(opts, pointEncoding);
    Geography g = new PolylineGeography(polylines);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    g.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();
    long s = 0;
    for (byte b : data) s += (b & 0xFF);
    bh.consume(data);
    return (double) s;
  }

  @Benchmark
  public int tagged_multipolyline_tagOnly() throws IOException {
    // Header + covering + count only (no geometry decode)
    EncodeTag tag = EncodeTag.decode(taggedMultiPolylineIn);
    tag.skipCovering(taggedMultiPolylineIn);
    int n = taggedMultiPolylineIn.readInt(false); // varint length of payload we wrote
    return n;
  }

  // =====================================================================
  // == Benchmarks (RAW S2 coder) ==
  // =====================================================================

  @Benchmark
  public double raw_S2multipolyline_decode(DecodeBenchPolyline.ProfilerHook ph) throws IOException {
    int b0 = rawMultiPolylineIn.read();
    int b1 = rawMultiPolylineIn.read();
    int b2 = rawMultiPolylineIn.read();
    int b3 = rawMultiPolylineIn.read();
    int count = (b0 & 0xFF) | ((b1 & 0xFF) << 8) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 24);

    List<S2Polyline> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      S2Polyline p = S2Polyline.decode(rawMultiPolylineIn);
      out.add(p);
    }
    double acc = 0;
    // Consume the data to prevent Dead Code Elimination (DCE)
    for (S2Polyline polyline : out) {
      if (polyline.numVertices() > 0) {
        S2Point p = polyline.vertex(0);
        acc += p.getX() + p.getY() + p.getZ();
      }
    }
    return acc;
  }

  @Benchmark
  public double raw_S2polyline_compact_encode(DecodeBenchPolyline.ProfilerHook ph, Blackhole bh)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);
    // LITTLE-ENDIAN count:
    int n = polylines.size();
    out.writeByte(n & 0xFF);
    out.writeByte((n >>> 8) & 0xFF);
    out.writeByte((n >>> 16) & 0xFF);
    out.writeByte((n >>> 24) & 0xFF);

    for (S2Polyline polyline : polylines) {
      // Use the high-level API: writes the coder id + payload that S2Polyline.decode expects
      S2Polyline.COMPACT_CODER.encode(polyline, out);
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
   * Encode the list of S2Polylines into a raw S2 payload. The format is [count (varint)]
   * [polyline_1_bytes] [polyline_2_bytes] ...
   */
  static byte[] encodeMultiPolylinePayload(List<S2Polyline> polylines) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);

    // Write count as a fixed 4-byte int (match reader)
    // LITTLE-ENDIAN count:
    int n = polylines.size();
    out.writeByte(n & 0xFF);
    out.writeByte((n >>> 8) & 0xFF);
    out.writeByte((n >>> 16) & 0xFF);
    out.writeByte((n >>> 24) & 0xFF);

    for (S2Polyline polyline : polylines) {
      // Use the high-level API: writes the coder id + payload that S2Polyline.decode expects
      S2Polyline.COMPACT_CODER.encode(polyline, out);
    }
    out.flush();
    return baos.toByteArray();
  }

  public static List<S2Polyline> buildPolylines(int numPolylines, int verticesPerPolyline) {
    List<S2Polyline> result = new ArrayList<>(numPolylines);
    for (int j = 0; j < numPolylines; j++) {
      List<S2Point> vertices = new ArrayList<>(verticesPerPolyline);
      for (int i = 0; i < verticesPerPolyline; i++) {
        // Shift each polyline so they don’t overlap completely
        double latDeg = -60.0 + (i % 120) + j * 0.5;
        double lngDeg = -170.0 + (i % 340) + j * 0.5;
        S2LatLng ll = S2LatLng.fromDegrees(latDeg, lngDeg).normalized();
        vertices.add(ll.toPoint());
      }
      result.add(new S2Polyline(vertices));
    }
    return result;
  }

  private static void applyPointEncodingPreference(EncodeOptions opts, String enc) {
    // Adjust to your real EncodeOptions API; placeholder keeps parity with your earlier code.
    // If you have an explicit "use compact vs fast" switch, set it here.
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
