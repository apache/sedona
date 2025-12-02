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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import one.profiler.AsyncProfiler;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.locationtech.jts.geom.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

/**
 * Benchmarks converting JTS Geometry -> Sedona S2Geography: - Polygon -> PolygonGeography -
 * MultiPolygon -> MultiPolygonGeography
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class BenchGeomToGeog {

  // ============== Params ==============
  /**
   * Number of polygons in the MultiPolygon benchmark. Polygon benchmark uses the first polygon
   * only.
   */
  // -------- Params --------
  @Param({"1", "16", "256", "1024"})
  public int numPolygons;

  @Param({"4", "16", "256", "1024"})
  public int verticesPerPolygon;

  /** XY or XYZ coordinates in the generated JTS polygons. */
  @Param({"XY", "XYZ"})
  public String dimension;

  // ============== Reused data ==============
  private GeometryFactory gf;
  private List<Polygon> polygons; // generated JTS polygons
  private Polygon polygon; // first polygon
  private MultiPolygon multiPolygon; // union as MultiPolygon

  // ============== Setup ==============
  @Setup(Level.Trial)
  public void setup() {
    // SRID=4326 for geographic degrees; PrecisionModel floating
    gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326);

    polygons = buildPolygons(numPolygons, verticesPerPolygon, dimension, gf);
    polygon = polygons.get(0);

    Polygon[] arr = polygons.toArray(new Polygon[0]);
    multiPolygon = gf.createMultiPolygon(arr);
  }

  // ============== Benchmarks ==============
  @Benchmark
  public void geomPolygon_toGeog(ProfilerHook ph, Blackhole bh) {
    Geography g = Constructors.geomToGeography(polygon);
    bh.consume(g);
  }

  @Benchmark
  public void geomMultiPolygon_toGeog(ProfilerHook ph, Blackhole bh) {
    Geography g = Constructors.geomToGeography(multiPolygon);
    bh.consume(g);
  }

  // ============== Helpers ==============
  /**
   * Builds non-overlapping JTS Polygons in EPSG:4326. Each polygon is a simple closed ring sampled
   * on a small circle (no holes).
   */
  public static List<Polygon> buildPolygons(
      int count, int verticesPerPolygon, String dimension, GeometryFactory gf) {
    int n = Math.max(1, count);
    int v = Math.max(3, verticesPerPolygon);
    boolean useZ = "XYZ".equalsIgnoreCase(dimension);

    List<Polygon> result = new ArrayList<>(n);
    double radiusDeg = 0.1; // small radius around each center

    for (int j = 0; j < n; j++) {
      double centerLat = -60.0 + j * 0.5;
      double centerLng = -170.0 + j * 0.5;

      Coordinate[] coords = new Coordinate[v + 1];
      for (int i = 0; i < v; i++) {
        double angle = (2.0 * Math.PI * i) / v;
        double lat = centerLat + radiusDeg * Math.cos(angle);
        double lng = centerLng + radiusDeg * Math.sin(angle);
        double z = useZ ? 10.0 * Math.sin(angle) : Double.NaN; // smooth Z variation
        coords[i] = useZ ? new Coordinate(lng, lat, z) : new Coordinate(lng, lat);
      }
      // close the ring
      coords[v] = new Coordinate(coords[0]);

      LinearRing shell = gf.createLinearRing(coords);
      Polygon poly = gf.createPolygon(shell, null);
      result.add(poly);
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
