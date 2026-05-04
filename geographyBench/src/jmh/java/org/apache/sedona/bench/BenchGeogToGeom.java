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

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import one.profiler.AsyncProfiler;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.Geography.GeographyKind;
import org.apache.sedona.common.S2Geography.MultiPolygonGeography;
import org.apache.sedona.common.S2Geography.PolygonGeography;
import org.apache.sedona.common.geography.Constructors;
import org.locationtech.jts.geom.Geometry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

/**
 * Benchmarks converting Sedona S2Geography -> JTS Geometry: - PolygonGeography -> JTS Polygon -
 * MultiPolygonGeography -> JTS MultiPolygon
 *
 * <p>Requirements: - JMH on classpath - async-profiler + ap-loader (if you want the profiler hook
 * enabled)
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class BenchGeogToGeom {

  // ============== Params ==============
  // -------- Params --------
  @Param({"1", "16", "256", "1024"})
  public int numPolygons;

  @Param({"4", "16", "256", "1024"})
  public int verticesPerPolygon;

  @Param({"XY", "XYZ"})
  public String dimension;

  /** Kept for parity with your other benches; not used by this conversion. */
  @Param({"COMPACT"})
  public String pointEncoding;

  // ============== Reused data ==============
  private List<S2Polygon> polygons; // synthetic S2 polygons
  private Geography polygonGeog; // Geography (single polygon)
  private Geography multiPolygonGeog; // Geography (multi polygon)

  // ============== Setup ==============
  @Setup(Level.Trial)
  public void setup() {
    this.polygons = buildPolygons(numPolygons, verticesPerPolygon);

    // Single polygon geography (use the first polygon)
    this.polygonGeog = new PolygonGeography(polygons.get(0));

    // MultiPolygon geography (all polygons)
    this.multiPolygonGeog = new MultiPolygonGeography(GeographyKind.MULTIPOLYGON, polygons);
  }

  // ============== Benchmarks ==============
  /** Convert a single PolygonGeography to JTS Geometry. */
  @Benchmark
  public void geogPolygon_toJts(ProfilerHook ph, Blackhole bh) {
    Geometry jts = Constructors.geogToGeometry(polygonGeog);
    bh.consume(jts);
  }

  /** Convert a MultiPolygonGeography to JTS Geometry. */
  @Benchmark
  public void geogMultiPolygon_toJts(ProfilerHook ph, Blackhole bh) {
    Geometry jts = Constructors.geogToGeometry(multiPolygonGeog);
    bh.consume(jts);
  }

  // ============== Helpers ==============
  /**
   * Builds non‑overlapping S2Polygons by placing each shell on a small circle around a translated
   * center. Each polygon consists of a single outer loop; no holes are added for simplicity.
   */
  public static List<S2Polygon> buildPolygons(int count, int verticesPerPolygon) {
    List<S2Polygon> result = new ArrayList<>(Math.max(1, count));
    double radiusDeg = 0.1; // small circle around the center

    for (int j = 0; j < Math.max(1, count); j++) {
      // spread centers to avoid overlaps and pathological degeneracies
      double centerLat = -60.0 + j * 0.5;
      double centerLng = -170.0 + j * 0.5;

      List<S2Point> verts = new ArrayList<>(Math.max(3, verticesPerPolygon));
      for (int i = 0; i < Math.max(3, verticesPerPolygon); i++) {
        double angle = (2.0 * Math.PI * i) / Math.max(3, verticesPerPolygon);
        double lat = centerLat + radiusDeg * Math.cos(angle);
        double lng = centerLng + radiusDeg * Math.sin(angle);
        verts.add(S2LatLng.fromDegrees(lat, lng).toPoint());
      }

      S2Loop shell = new S2Loop(verts);
      shell.normalize(); // CCW for shells
      result.add(new S2Polygon(shell));
    }
    return result;
  }

  // ============== Async‑profiler hook (optional) ==============
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
      String common = String.format("event=%s,interval=%s,all-user", event, interval);

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
