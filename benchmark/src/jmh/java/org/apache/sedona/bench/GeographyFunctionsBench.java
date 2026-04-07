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
import org.apache.sedona.common.S2Geography.WKBGeography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for all planned Geography ST functions.
 *
 * <p>Covers every function from the Geography roadmap:
 * <ul>
 *   <li>Constructors: ST_GeogFromWKB, ST_GeogFromEWKB, ST_GeogFromWKT, ST_GeogFromEWKT,
 *       ST_GeogFromText, ST_GeogFromGeohash</li>
 *   <li>Converters: ST_GeogToGeometry, ST_GeomToGeography</li>
 *   <li>Level 0 (WKB only): ST_Envelope</li>
 *   <li>Level 1 (JTS only): ST_AsEWKT, ST_AsText, ST_NPoints, ST_GeometryType,
 *       ST_NumGeometries, ST_Centroid</li>
 *   <li>Level 2 (JTS + Spheroid): ST_Distance, ST_Area, ST_Length</li>
 *   <li>Level 3 (S2 required): ST_MaxDistance, ST_ClosestPoint, ST_MinimumClearanceLine,
 *       ST_Equals, ST_Intersects, ST_Contains</li>
 * </ul>
 *
 * <p>Run: {@code mvn package -pl benchmark -am && java -jar benchmark/target/sedona-benchmark-*.jar
 * GeographyFunctionsBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class GeographyFunctionsBench {

  @Param({"point", "linestring_16", "polygon_16", "polygon_64"})
  public String geometryType;

  // --- Test data (set by geometryType param) ---
  private Geography geogA;
  private Geography geogB;
  private Geography container;
  private Geography pointInside;
  private Geography pointOutside;

  // Equivalent JTS geometries for Geometry baseline comparison
  private Geometry jtsA;
  private Geometry jtsB;
  private Geometry jtsContainer;
  private Geometry jtsPointInside;
  private Geometry jtsPointOutside;

  // Pre-built data for constructor benchmarks
  private String wktA;
  private String ewktA;
  private byte[] wkbA;

  @Setup(Level.Trial)
  public void setup() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();

    switch (geometryType) {
      case "point":
        wktA = "POINT (0 0)";
        geogA = Constructors.geogFromWKT(wktA, 4326);
        geogB = Constructors.geogFromWKT("POINT (1 1)", 4326);
        container = Constructors.geogFromWKT("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", 4326);
        jtsA = jtsReader.read("POINT (0 0)");
        jtsB = jtsReader.read("POINT (1 1)");
        jtsContainer = jtsReader.read("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))");
        break;
      case "linestring_16":
        wktA = buildLineWKT(16);
        geogA = Constructors.geogFromWKT(wktA, 4326);
        geogB = Constructors.geogFromWKT("POINT (0.5 1)", 4326);
        container = Constructors.geogFromWKT("POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))", 4326);
        jtsA = jtsReader.read(wktA);
        jtsB = jtsReader.read("POINT (0.5 1)");
        jtsContainer = jtsReader.read("POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))");
        break;
      case "polygon_16":
        wktA = buildCirclePolygonWKT(16, 0.5);
        geogA = Constructors.geogFromWKT(wktA, 4326);
        geogB = Constructors.geogFromWKT(buildCirclePolygonWKT(16, 0.3, 0.2, 0.2), 4326);
        container = Constructors.geogFromWKT("POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))", 4326);
        jtsA = jtsReader.read(wktA);
        jtsB = jtsReader.read(buildCirclePolygonWKT(16, 0.3, 0.2, 0.2));
        jtsContainer = jtsReader.read("POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))");
        break;
      case "polygon_64":
        wktA = buildCirclePolygonWKT(64, 0.5);
        geogA = Constructors.geogFromWKT(wktA, 4326);
        geogB = Constructors.geogFromWKT(buildCirclePolygonWKT(64, 0.3, 0.2, 0.2), 4326);
        container = Constructors.geogFromWKT("POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))", 4326);
        jtsA = jtsReader.read(wktA);
        jtsB = jtsReader.read(buildCirclePolygonWKT(64, 0.3, 0.2, 0.2));
        jtsContainer = jtsReader.read("POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))");
        break;
      default:
        throw new IllegalArgumentException("Unknown geometryType: " + geometryType);
    }
    pointInside = Constructors.geogFromWKT("POINT (0 0)", 4326);
    pointOutside = Constructors.geogFromWKT("POINT (5 5)", 4326);
    jtsPointInside = jtsReader.read("POINT (0 0)");
    jtsPointOutside = jtsReader.read("POINT (5 5)");

    // Pre-build constructor inputs
    ewktA = "SRID=4326;" + wktA;
    wkbA = ((WKBGeography) geogA).getWKBBytes();
  }

  // ─── Constructors ──────────────────────────────────────────────────────────

  @Benchmark
  public void ST_GeogFromWKB(Blackhole bh) throws ParseException {
    bh.consume(Constructors.geogFromWKB(wkbA, 4326));
  }

  @Benchmark
  public void ST_GeogFromWKT(Blackhole bh) throws ParseException {
    bh.consume(Constructors.geogFromWKT(wktA, 4326));
  }

  @Benchmark
  public void ST_GeogFromEWKT(Blackhole bh) throws ParseException {
    bh.consume(Constructors.geogFromEWKT(ewktA));
  }

  // ─── Converters ────────────────────────────────────────────────────────────

  @Benchmark
  public void ST_GeogToGeometry(Blackhole bh) {
    bh.consume(Constructors.geogToGeometry(geogA));
  }

  @Benchmark
  public void ST_GeomToGeography(Blackhole bh) {
    bh.consume(Constructors.geomToGeography(jtsA));
  }

  // ─── Level 0+1: WKB/JTS structural operations ─────────────────────────────

  @Benchmark
  public void ST_Envelope(Blackhole bh) {
    bh.consume(Functions.getEnvelope(geogA, false));
  }

  @Benchmark
  public void ST_AsEWKT(Blackhole bh) {
    bh.consume(Functions.asEWKT(geogA));
  }

  @Benchmark
  public void ST_AsText(Blackhole bh) {
    bh.consume(Functions.asText(geogA));
  }

  @Benchmark
  public void ST_NPoints(Blackhole bh) {
    bh.consume(Functions.nPoints(geogA));
  }

  @Benchmark
  public void ST_GeometryType(Blackhole bh) {
    bh.consume(Functions.geometryType(geogA));
  }

  @Benchmark
  public void ST_NumGeometries(Blackhole bh) {
    bh.consume(Functions.numGeometries(geogA));
  }

  @Benchmark
  public void ST_Centroid(Blackhole bh) {
    bh.consume(Functions.centroid(geogA));
  }

  // ─── Level 2: JTS + Spheroid (geodesic metrics) ───────────────────────────

  @Benchmark
  public void ST_Distance(Blackhole bh) {
    bh.consume(Functions.distance(geogA, geogB));
  }

  @Benchmark
  public void ST_Distance_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Functions.distance(jtsA, jtsB));
  }

  @Benchmark
  public void ST_Area(Blackhole bh) {
    bh.consume(Functions.area(geogA));
  }

  @Benchmark
  public void ST_Area_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Functions.area(jtsA));
  }

  @Benchmark
  public void ST_Length(Blackhole bh) {
    bh.consume(Functions.length(geogA));
  }

  @Benchmark
  public void ST_Length_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Functions.length(jtsA));
  }

  // ─── Level 3: S2 required ─────────────────────────────────────────────────

  @Benchmark
  public void ST_MaxDistance(Blackhole bh) {
    bh.consume(Functions.maxDistance(geogA, geogB));
  }

  @Benchmark
  public void ST_MaxDistance_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Functions.maxDistance(jtsA, jtsB));
  }

  @Benchmark
  public void ST_ClosestPoint(Blackhole bh) {
    bh.consume(Functions.closestPoint(geogA, geogB));
  }

  @Benchmark
  public void ST_ClosestPoint_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Functions.closestPoint(jtsA, jtsB));
  }

  @Benchmark
  public void ST_MinimumClearanceLine(Blackhole bh) {
    bh.consume(Functions.minimumClearanceLine(geogA, geogB));
  }

  @Benchmark
  public void ST_Intersects_true(Blackhole bh) {
    bh.consume(Functions.intersects(container, pointInside));
  }

  @Benchmark
  public void ST_Intersects_true_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Predicates.intersects(jtsContainer, jtsPointInside));
  }

  @Benchmark
  public void ST_Intersects_false(Blackhole bh) {
    bh.consume(Functions.intersects(container, pointOutside));
  }

  @Benchmark
  public void ST_Intersects_false_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Predicates.intersects(jtsContainer, jtsPointOutside));
  }

  @Benchmark
  public void ST_Contains_true(Blackhole bh) {
    bh.consume(Functions.contains(container, pointInside));
  }

  @Benchmark
  public void ST_Contains_true_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Predicates.contains(jtsContainer, jtsPointInside));
  }

  @Benchmark
  public void ST_Contains_false(Blackhole bh) {
    bh.consume(Functions.contains(container, pointOutside));
  }

  @Benchmark
  public void ST_Contains_false_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Predicates.contains(jtsContainer, jtsPointOutside));
  }

  @Benchmark
  public void ST_Equals(Blackhole bh) {
    bh.consume(Functions.equals(geogA, geogA));
  }

  @Benchmark
  public void ST_Equals_geometry_baseline(Blackhole bh) {
    bh.consume(org.apache.sedona.common.Predicates.equals(jtsA, jtsA));
  }

  // ─── Geometry builders ─────────────────────────────────────────────────────

  private static String buildLineWKT(int vertices) {
    StringBuilder sb = new StringBuilder("LINESTRING (");
    for (int i = 0; i < vertices; i++) {
      if (i > 0) sb.append(", ");
      double t = (double) i / (vertices - 1);
      sb.append(String.format("%.6f %.6f", t, Math.sin(t * Math.PI)));
    }
    sb.append(")");
    return sb.toString();
  }

  private static String buildCirclePolygonWKT(int vertices, double radius) {
    return buildCirclePolygonWKT(vertices, radius, 0.0, 0.0);
  }

  private static String buildCirclePolygonWKT(
      int vertices, double radius, double centerLon, double centerLat) {
    StringBuilder sb = new StringBuilder("POLYGON ((");
    for (int i = 0; i <= vertices; i++) {
      if (i > 0) sb.append(", ");
      double angle = 2.0 * Math.PI * (i % vertices) / vertices;
      sb.append(
          String.format(
              "%.6f %.6f",
              centerLon + radius * Math.cos(angle), centerLat + radius * Math.sin(angle)));
    }
    sb.append("))");
    return sb.toString();
  }
}
