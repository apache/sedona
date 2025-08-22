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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.sedona.common.S2Geography.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class DecodeBenchPoint {
  // -------- Params --------
  @Param({"1", "16", "256", "4096", "65536"})
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
  private PrimitiveArrays.Bytes rawFastBytesAdapter;
  private PrimitiveArrays.Cursor compactCur;
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

    // Create the Bytes adapters and then get the cursors from them.
    rawCompactBytesAdapter = bytesOverArray(rawCompactBytes);
    rawFastBytesAdapter = bytesOverArray(rawFastBytes);
    compactCur = rawCompactBytesAdapter.cursor();
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
    taggedPointIn.rewind();
    taggedMultiPointIn.rewind();

    // Reset the cursor's position field directly to 0
    compactCur.position = 0;
    fastCur.position = 0;
  }

  // ---------------- TAGGED Benchmarks ----------------
  @Benchmark
  public void tagged_point_tagOnly(Blackhole bh) throws IOException {
    EncodeTag tag = EncodeTag.decode(taggedPointIn);
    tag.skipCovering(taggedPointIn);
    bh.consume(tag.getKind());
    bh.consume(tag.getFlags());
  }

  @Benchmark
  public void tagged_multipoint_tagOnly(Blackhole bh) throws IOException {
    EncodeTag tag = EncodeTag.decode(taggedMultiPointIn);
    tag.skipCovering(taggedMultiPointIn);
    bh.consume(tag.getKind());
    bh.consume(tag.getFlags());
  }

  @Benchmark
  public void tagged_point_full(Blackhole bh) throws IOException {
    Geography g = Geography.decodeTagged(taggedPointIn);
    bh.consume(g);
  }

  @Benchmark
  public void tagged_multipoint_full(Blackhole bh) throws IOException {
    Geography g = Geography.decodeTagged(taggedMultiPointIn);
    bh.consume(g);
  }

  @Benchmark
  public void decodeWrap_asGeography_time(Blackhole bh) throws IOException {
    List<S2Point> out = S2Point.Shape.COMPACT_CODER.decode(rawCompactBytesAdapter, compactCur);
    PointGeography g = new PointGeography(out);
    bh.consume(g);
  }

  // ---------------- RAW S2Point.Shape coder decode (no tags/wrappers) ----------------
  @Benchmark
  public double raw_S2points_compact_decode(Blackhole bh) throws IOException {
    List<S2Point> out = S2Point.Shape.COMPACT_CODER.decode(rawCompactBytesAdapter, compactCur);
    double acc = 0;
    for (int i = 0; i < out.size(); i++) {
      S2Point p = out.get(i);
      acc += p.getX() + p.getY() + p.getZ();
    }
    return acc; // returning prevents DCE; no Blackhole needed
  }

  //  @Benchmark
  //  public void raw_S2points_fast_decode(Blackhole bh) throws IOException {
  //    List<S2Point> out = S2Point.Shape.FAST_CODER.decode(rawFastBytesAdapter, fastCur);
  //    bh.consume(out.size());
  //  }

  // =====================================================================
  // == Helpers: payload framing & coder bridges ==
  // =====================================================================

  // CORRECTED VERSION
  static byte[] encodePointsPayload(List<S2Point> points, boolean compact) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output out = new Output(baos);

    // Encode the list directly to the stream.
    if (compact) {
      S2Point.Shape.COMPACT_CODER.encode(S2Point.Shape.fromList(points), out);
    } else {
      S2Point.Shape.FAST_CODER.encode(S2Point.Shape.fromList(points), out);
    }
    out.flush();

    // Return the raw S2 payload directly.
    return baos.toByteArray();
  }

  // ---------------- Misc helpers ----------------

  private static List<S2Point> buildPoints(int n) {
    List<S2Point> out = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      double lat = -60.0 + (i % 120);
      double lng = -170.0 + (i % 340);
      out.add(S2LatLng.fromDegrees(lat, lng).toPoint());
    }
    return out;
  }

  private static void applyPointEncodingPreference(EncodeOptions opts, String pointEncoding) {
    if ("COMPACT".equals(pointEncoding)) {
      opts.setEnableLazyDecode(false);
    } else {
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
}
