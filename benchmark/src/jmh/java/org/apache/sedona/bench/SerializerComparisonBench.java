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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.GeographySerializer;
import org.apache.sedona.common.S2Geography.GeographyWKBSerializer;
import org.apache.sedona.common.S2Geography.ShapeIndexGeography;
import org.apache.sedona.common.S2Geography.WKBGeography;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.apache.sedona.common.S2Geography.WKBWriter;
import org.apache.sedona.common.S2Geography.WKTReader;
import org.apache.sedona.common.geography.Functions;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares end-to-end ST function performance under different serialization scenarios.
 *
 * Two source format scenarios:
 *
 * - wkb_source (GeoParquet scenario): Raw data is WKB bytes (as stored in GeoParquet).
 *   Both the WKB path and the "S2 from WKB" path must parse WKB. The WKB path wraps bytes
 *   lazily; the S2 path fully parses WKB to S2 Geography upfront.
 * - s2native_source (legacy scenario): Raw data is S2-native serialized bytes. The S2
 *   path deserializes directly to S2 Geography; the WKB path must go through WKB
 *   deserialization.
 *
 * Run: java -jar benchmark/target/sedona-benchmark-*.jar SerializerComparisonBench
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Thread)
public class SerializerComparisonBench {

  @Param({"point", "polygon_16", "polygon_64"})
  public String geometryType;

  // ─── Pre-serialized bytes ──────────────────────────────────────────────

  // WKB format (as stored in GeoParquet): [0xFF][SRID][WKB payload]
  private byte[] wkbSerializedA;
  private byte[] wkbSerializedB;
  private byte[] wkbSerializedContainer;
  private byte[] wkbSerializedPointInside;

  // S2-native format (legacy Sedona format)
  private byte[] s2SerializedA;
  private byte[] s2SerializedB;
  private byte[] s2SerializedContainer;
  private byte[] s2SerializedPointInside;

  // Raw WKB bytes (pure ISO WKB, no header — as stored in GeoParquet Binary column)
  private byte[] rawWkbA;
  private byte[] rawWkbB;
  private byte[] rawWkbContainer;
  private byte[] rawWkbPointInside;

  @Setup(Level.Trial)
  public void setup() throws ParseException, IOException {
    String wktA, wktB;
    switch (geometryType) {
      case "point":
        wktA = "POINT (0 0)";
        wktB = "POINT (1 1)";
        break;
      case "polygon_16":
        wktA = buildCirclePolygonWKT(16, 0.5, 0, 0);
        wktB = buildCirclePolygonWKT(16, 0.3, 0.2, 0.2);
        break;
      case "polygon_64":
        wktA = buildCirclePolygonWKT(64, 0.5, 0, 0);
        wktB = buildCirclePolygonWKT(64, 0.3, 0.2, 0.2);
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + geometryType);
    }

    String wktContainer = "POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))";
    String wktPointInside = "POINT (0 0)";

    // Build S2 Geography objects
    WKTReader s2Reader = new WKTReader();
    Geography s2A = s2Reader.read(wktA);
    s2A.setSRID(4326);
    Geography s2B = s2Reader.read(wktB);
    s2B.setSRID(4326);
    Geography s2Container = s2Reader.read(wktContainer);
    s2Container.setSRID(4326);
    Geography s2PointInside = s2Reader.read(wktPointInside);
    s2PointInside.setSRID(4326);

    // Serialize to WKB format (GeographyWKBSerializer: 0xFF header)
    Geography wkbA = WKBGeography.fromS2Geography(s2A);
    Geography wkbB = WKBGeography.fromS2Geography(s2B);
    Geography wkbContainer = WKBGeography.fromS2Geography(s2Container);
    Geography wkbPointInside = WKBGeography.fromS2Geography(s2PointInside);

    wkbSerializedA = GeographyWKBSerializer.serialize(wkbA);
    wkbSerializedB = GeographyWKBSerializer.serialize(wkbB);
    wkbSerializedContainer = GeographyWKBSerializer.serialize(wkbContainer);
    wkbSerializedPointInside = GeographyWKBSerializer.serialize(wkbPointInside);

    // Serialize to S2-native format (legacy)
    s2SerializedA = GeographySerializer.serialize(s2A);
    s2SerializedB = GeographySerializer.serialize(s2B);
    s2SerializedContainer = GeographySerializer.serialize(s2Container);
    s2SerializedPointInside = GeographySerializer.serialize(s2PointInside);

    // Raw WKB bytes (pure ISO WKB — what GeoParquet stores)
    WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN, false);
    rawWkbA = wkbWriter.write(s2A);
    rawWkbB = wkbWriter.write(s2B);
    rawWkbContainer = wkbWriter.write(s2Container);
    rawWkbPointInside = wkbWriter.write(s2PointInside);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    WKBGeography.setEagerShapeIndex(false);
  }

  // ═══════════════════════════════════════════════════════════════════════
  // GeoParquet scenario: source data is raw WKB bytes
  // This is the realistic scenario for data stored in GeoParquet.
  // Both paths start from the same WKB bytes.
  // ═══════════════════════════════════════════════════════════════════════

  // ─── ST_Distance from GeoParquet ───────────────────────────────────────

  /** WKB path: raw WKB → WKBGeography (zero-parse) → lazy S2 → S2ClosestEdgeQuery */
  @Benchmark
  public void geoparquet_distance_wkb(Blackhole bh) {
    Geography a = WKBGeography.fromWKB(rawWkbA, 4326);
    Geography b = WKBGeography.fromWKB(rawWkbB, 4326);
    bh.consume(Functions.distance(a, b));
  }

  /** S2 path: raw WKB → full S2 parse → ShapeIndex → S2ClosestEdgeQuery */
  @Benchmark
  public void geoparquet_distance_s2parse(Blackhole bh) throws ParseException {
    WKBReader reader = new WKBReader();
    Geography a = reader.read(rawWkbA);
    Geography b = reader.read(rawWkbB);
    ShapeIndexGeography idxA = new ShapeIndexGeography(a);
    ShapeIndexGeography idxB = new ShapeIndexGeography(b);
    bh.consume(
        new org.apache.sedona.common.S2Geography.Distance()
                .S2_distance(idxA, idxB)
            * 6371008.8);
  }

  // ─── ST_Area from GeoParquet ───────────────────────────────────────────

  @Benchmark
  public void geoparquet_area_wkb(Blackhole bh) {
    Geography a = WKBGeography.fromWKB(rawWkbA, 4326);
    bh.consume(Functions.area(a));
  }

  @Benchmark
  public void geoparquet_area_s2parse(Blackhole bh) throws ParseException {
    WKBReader reader = new WKBReader();
    Geography a = reader.read(rawWkbA);
    bh.consume(org.apache.sedona.common.sphere.Spheroid.area(toJTS(a)));
  }

  // ─── ST_Contains from GeoParquet ───────────────────────────────────────

  @Benchmark
  public void geoparquet_contains_wkb(Blackhole bh) {
    Geography container = WKBGeography.fromWKB(rawWkbContainer, 4326);
    Geography pt = WKBGeography.fromWKB(rawWkbPointInside, 4326);
    bh.consume(Functions.contains(container, pt));
  }

  @Benchmark
  public void geoparquet_contains_s2parse(Blackhole bh) throws ParseException {
    WKBReader reader = new WKBReader();
    Geography container = reader.read(rawWkbContainer);
    Geography pt = reader.read(rawWkbPointInside);
    ShapeIndexGeography idxC = new ShapeIndexGeography(container);
    ShapeIndexGeography idxP = new ShapeIndexGeography(pt);
    bh.consume(
        new org.apache.sedona.common.S2Geography.Predicates()
            .S2_contains(
                idxC,
                idxP,
                new com.google.common.geometry.S2BooleanOperation.Options()));
  }

  // ─── ST_Intersects from GeoParquet ─────────────────────────────────────

  @Benchmark
  public void geoparquet_intersects_wkb(Blackhole bh) {
    Geography container = WKBGeography.fromWKB(rawWkbContainer, 4326);
    Geography pt = WKBGeography.fromWKB(rawWkbPointInside, 4326);
    bh.consume(Functions.intersects(container, pt));
  }

  @Benchmark
  public void geoparquet_intersects_s2parse(Blackhole bh) throws ParseException {
    WKBReader reader = new WKBReader();
    Geography container = reader.read(rawWkbContainer);
    Geography pt = reader.read(rawWkbPointInside);
    ShapeIndexGeography idxC = new ShapeIndexGeography(container);
    ShapeIndexGeography idxP = new ShapeIndexGeography(pt);
    bh.consume(
        new org.apache.sedona.common.S2Geography.Predicates()
            .S2_intersects(
                idxC,
                idxP,
                new com.google.common.geometry.S2BooleanOperation.Options()));
  }

  // ═══════════════════════════════════════════════════════════════════════
  // Legacy scenario: source data is pre-serialized in each format
  // WKB path uses GeographyWKBSerializer; S2 path uses GeographySerializer
  // ═══════════════════════════════════════════════════════════════════════

  @Benchmark
  public void legacy_distance_wkb(Blackhole bh) throws IOException {
    Geography a = GeographyWKBSerializer.deserialize(wkbSerializedA);
    Geography b = GeographyWKBSerializer.deserialize(wkbSerializedB);
    bh.consume(Functions.distance(a, b));
  }

  @Benchmark
  public void legacy_distance_s2native(Blackhole bh) throws IOException {
    Geography a = GeographySerializer.deserialize(s2SerializedA);
    Geography b = GeographySerializer.deserialize(s2SerializedB);
    bh.consume(Functions.distance(a, b));
  }

  @Benchmark
  public void legacy_contains_wkb(Blackhole bh) throws IOException {
    Geography container = GeographyWKBSerializer.deserialize(wkbSerializedContainer);
    Geography pt = GeographyWKBSerializer.deserialize(wkbSerializedPointInside);
    bh.consume(Functions.contains(container, pt));
  }

  @Benchmark
  public void legacy_contains_s2native(Blackhole bh) throws IOException {
    Geography container = GeographySerializer.deserialize(s2SerializedContainer);
    Geography pt = GeographySerializer.deserialize(s2SerializedPointInside);
    bh.consume(Functions.contains(container, pt));
  }

  // ─── Helpers ───────────────────────────────────────────────────────────

  private static org.locationtech.jts.geom.Geometry toJTS(Geography g) {
    return org.apache.sedona.common.geography.Constructors.geogToGeometry(g);
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
