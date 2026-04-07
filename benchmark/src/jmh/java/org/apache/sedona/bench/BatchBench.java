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

import java.util.concurrent.TimeUnit;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Functions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Batch-processing benchmarks inspired by sedona-db's Criterion benchmarks. Instead of measuring a
 * single function call, these iterate over arrays of random geometries to measure amortized per-row
 * cost.
 *
 * <p>Patterns:
 * <ul>
 *   <li><b>Array-Array</b>: Both inputs are arrays (e.g., spatial join: distance(col_a[i], col_b[i]))</li>
 *   <li><b>Array-Scalar</b>: One array, one constant (e.g., spatial filter: contains(constant_polygon, col[i]))</li>
 * </ul>
 *
 * <p>Run: {@code java -jar benchmark/target/sedona-benchmark-*.jar BatchBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Thread)
public class BatchBench {

  @Param({"1024"})
  public int batchSize;

  @Param({"10", "100", "500"})
  public int vertices;

  // ─── Random data arrays ────────────────────────────────────────────────

  private Geography[] geogPoints;
  private Geography[] geogPolygons;
  private Geography[] geogLines;
  private Geography scalarPolygon; // constant polygon for filter patterns

  private Geometry[] jtsPoints;
  private Geometry[] jtsPolygons;

  @Setup(Level.Trial)
  public void setup() throws ParseException, org.locationtech.jts.io.ParseException {
    long seed = 42;
    geogPoints = RandomGeoGenerator.randomGeogPoints(batchSize, seed);
    geogPolygons = RandomGeoGenerator.randomGeogPolygons(batchSize, vertices, seed + 1);
    geogLines = RandomGeoGenerator.randomGeogLineStrings(batchSize, vertices, seed + 2);

    // Scalar polygon for filter pattern: a large box
    scalarPolygon =
        org.apache.sedona.common.geography.Constructors.geogFromWKT(
            "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))", 4326);

    // JTS baselines (same seed for comparable data)
    jtsPoints = RandomGeoGenerator.randomJtsPoints(batchSize, seed);
    jtsPolygons = RandomGeoGenerator.randomJtsPolygons(batchSize, vertices, seed + 1);
  }

  // ─── ST_Distance: Array-Array (point vs point) ────────────────────────

  @Benchmark
  public void distance_array_array_points(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.distance(geogPoints[i], geogPoints[(i + 1) % batchSize]));
    }
  }

  @Benchmark
  public void distance_array_array_points_geometry_baseline(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(
          org.apache.sedona.common.Functions.distance(
              jtsPoints[i], jtsPoints[(i + 1) % batchSize]));
    }
  }

  // ─── ST_Distance: Array-Scalar (point vs polygon) ─────────────────────

  @Benchmark
  public void distance_array_scalar_point_polygon(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.distance(geogPoints[i], scalarPolygon));
    }
  }

  // ─── ST_Area: Array (polygons) ─────────────────────────────────────────

  @Benchmark
  public void area_array_polygons(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.area(geogPolygons[i]));
    }
  }

  @Benchmark
  public void area_array_polygons_geometry_baseline(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(org.apache.sedona.common.Functions.area(jtsPolygons[i]));
    }
  }

  // ─── ST_Length: Array (linestrings) ────────────────────────────────────

  @Benchmark
  public void length_array_linestrings(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.length(geogLines[i]));
    }
  }

  // ─── ST_Contains: Scalar-Array (constant polygon vs point array) ──────

  @Benchmark
  public void contains_scalar_array_polygon_points(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.contains(scalarPolygon, geogPoints[i]));
    }
  }

  @Benchmark
  public void contains_scalar_array_polygon_points_geometry_baseline(Blackhole bh) {
    Geometry jtsScalarPolygon = jtsPolygons[0]; // use first polygon as scalar
    for (int i = 0; i < batchSize; i++) {
      bh.consume(org.apache.sedona.common.Predicates.contains(jtsScalarPolygon, jtsPoints[i]));
    }
  }

  // ─── ST_Intersects: Array-Scalar (points vs constant polygon) ─────────

  @Benchmark
  public void intersects_array_scalar_points_polygon(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.intersects(geogPoints[i], scalarPolygon));
    }
  }

  @Benchmark
  public void intersects_array_scalar_points_polygon_geometry_baseline(Blackhole bh) {
    Geometry jtsScalarPolygon = jtsPolygons[0];
    for (int i = 0; i < batchSize; i++) {
      bh.consume(org.apache.sedona.common.Predicates.intersects(jtsPoints[i], jtsScalarPolygon));
    }
  }

  // ─── ST_Equals: Array-Array (polygon vs polygon) ──────────────────────

  @Benchmark
  public void equals_array_array_polygons(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.equals(geogPolygons[i], geogPolygons[i]));
    }
  }

  @Benchmark
  public void equals_array_array_polygons_geometry_baseline(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(org.apache.sedona.common.Predicates.equals(jtsPolygons[i], jtsPolygons[i]));
    }
  }

  // ─── ST_MaxDistance: Array-Array (polygon vs polygon) ──────────────────

  @Benchmark
  public void maxDistance_array_array_polygons(Blackhole bh) {
    for (int i = 0; i < batchSize; i++) {
      bh.consume(
          Functions.maxDistance(geogPolygons[i], geogPolygons[(i + 1) % batchSize]));
    }
  }

  // ─── ST_ClosestPoint: Array-Scalar (linestring vs point) ──────────────

  @Benchmark
  public void closestPoint_array_scalar_lines_point(Blackhole bh) {
    Geography scalarPoint = geogPoints[0];
    for (int i = 0; i < batchSize; i++) {
      bh.consume(Functions.closestPoint(geogLines[i], scalarPoint));
    }
  }
}
