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
import org.apache.sedona.common.S2Geography.WKBGeography;
import org.apache.sedona.common.S2Geography.WKTReader;
import org.apache.sedona.common.geography.Functions;
import org.locationtech.jts.io.ParseException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares end-to-end ST function performance using the old S2-native serializer vs the new WKB
 * serializer.
 *
 * <p>Each benchmark simulates the real Spark UDT path:
 * <ol>
 *   <li>Deserialize bytes → Geography object</li>
 *   <li>Execute an ST function</li>
 * </ol>
 *
 * <p>This measures what matters in practice: the combined cost of deserialization + function
 * execution, not just the function in isolation.
 *
 * <p>Run: {@code java -jar benchmark/target/sedona-benchmark-*.jar SerializerComparisonBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class SerializerComparisonBench {

    @Param({"point", "polygon_16", "polygon_64"})
    public String geometryType;

    // Pre-serialized bytes in both formats
    private byte[] wkbBytesA;
    private byte[] wkbBytesB;
    private byte[] s2BytesA;
    private byte[] s2BytesB;

    // Container polygon for predicate tests (both formats)
    private byte[] wkbBytesContainer;
    private byte[] s2BytesContainer;
    private byte[] wkbBytesPointInside;
    private byte[] s2BytesPointInside;

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

        // Build S2 Geography objects (via WKTReader for S2 normalization)
        WKTReader s2Reader = new WKTReader();
        Geography s2A = s2Reader.read(wktA); s2A.setSRID(4326);
        Geography s2B = s2Reader.read(wktB); s2B.setSRID(4326);
        Geography s2Container = s2Reader.read(wktContainer); s2Container.setSRID(4326);
        Geography s2PointInside = s2Reader.read(wktPointInside); s2PointInside.setSRID(4326);

        // Build WKBGeography objects
        Geography wkbA = WKBGeography.fromS2Geography(s2A);
        Geography wkbB = WKBGeography.fromS2Geography(s2B);
        Geography wkbContainer = WKBGeography.fromS2Geography(s2Container);
        Geography wkbPointInside = WKBGeography.fromS2Geography(s2PointInside);

        // Serialize to bytes in both formats
        s2BytesA = GeographySerializer.serialize(s2A);
        s2BytesB = GeographySerializer.serialize(s2B);
        s2BytesContainer = GeographySerializer.serialize(s2Container);
        s2BytesPointInside = GeographySerializer.serialize(s2PointInside);

        wkbBytesA = GeographyWKBSerializer.serialize(wkbA);
        wkbBytesB = GeographyWKBSerializer.serialize(wkbB);
        wkbBytesContainer = GeographyWKBSerializer.serialize(wkbContainer);
        wkbBytesPointInside = GeographyWKBSerializer.serialize(wkbPointInside);
    }

    // ─── ST_Distance: deserialize + compute ──────────────────────────────────

    @Benchmark
    public void distance_wkb(Blackhole bh) throws IOException {
        Geography a = GeographyWKBSerializer.deserialize(wkbBytesA);
        Geography b = GeographyWKBSerializer.deserialize(wkbBytesB);
        bh.consume(Functions.distance(a, b));
    }

    @Benchmark
    public void distance_s2native(Blackhole bh) throws IOException {
        Geography a = GeographySerializer.deserialize(s2BytesA);
        Geography b = GeographySerializer.deserialize(s2BytesB);
        bh.consume(Functions.distance(a, b));
    }

    // ─── ST_Area: deserialize + compute ──────────────────────────────────────

    @Benchmark
    public void area_wkb(Blackhole bh) throws IOException {
        Geography a = GeographyWKBSerializer.deserialize(wkbBytesA);
        bh.consume(Functions.area(a));
    }

    @Benchmark
    public void area_s2native(Blackhole bh) throws IOException {
        Geography a = GeographySerializer.deserialize(s2BytesA);
        bh.consume(Functions.area(a));
    }

    // ─── ST_Length: deserialize + compute ─────────────────────────────────────

    @Benchmark
    public void length_wkb(Blackhole bh) throws IOException {
        Geography a = GeographyWKBSerializer.deserialize(wkbBytesA);
        bh.consume(Functions.length(a));
    }

    @Benchmark
    public void length_s2native(Blackhole bh) throws IOException {
        Geography a = GeographySerializer.deserialize(s2BytesA);
        bh.consume(Functions.length(a));
    }

    // ─── ST_Contains: deserialize + compute ──────────────────────────────────

    @Benchmark
    public void contains_wkb(Blackhole bh) throws IOException {
        Geography container = GeographyWKBSerializer.deserialize(wkbBytesContainer);
        Geography pt = GeographyWKBSerializer.deserialize(wkbBytesPointInside);
        bh.consume(Functions.contains(container, pt));
    }

    @Benchmark
    public void contains_s2native(Blackhole bh) throws IOException {
        Geography container = GeographySerializer.deserialize(s2BytesContainer);
        Geography pt = GeographySerializer.deserialize(s2BytesPointInside);
        bh.consume(Functions.contains(container, pt));
    }

    // ─── ST_Intersects: deserialize + compute ────────────────────────────────

    @Benchmark
    public void intersects_wkb(Blackhole bh) throws IOException {
        Geography container = GeographyWKBSerializer.deserialize(wkbBytesContainer);
        Geography pt = GeographyWKBSerializer.deserialize(wkbBytesPointInside);
        bh.consume(Functions.intersects(container, pt));
    }

    @Benchmark
    public void intersects_s2native(Blackhole bh) throws IOException {
        Geography container = GeographySerializer.deserialize(s2BytesContainer);
        Geography pt = GeographySerializer.deserialize(s2BytesPointInside);
        bh.consume(Functions.intersects(container, pt));
    }

    // ─── ST_Equals: deserialize + compute ────────────────────────────────────

    @Benchmark
    public void equals_wkb(Blackhole bh) throws IOException {
        Geography a = GeographyWKBSerializer.deserialize(wkbBytesA);
        Geography a2 = GeographyWKBSerializer.deserialize(wkbBytesA);
        bh.consume(Functions.equals(a, a2));
    }

    @Benchmark
    public void equals_s2native(Blackhole bh) throws IOException {
        Geography a = GeographySerializer.deserialize(s2BytesA);
        Geography a2 = GeographySerializer.deserialize(s2BytesA);
        bh.consume(Functions.equals(a, a2));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static String buildCirclePolygonWKT(int vertices, double radius,
                                                 double centerLon, double centerLat) {
        StringBuilder sb = new StringBuilder("POLYGON ((");
        for (int i = 0; i <= vertices; i++) {
            if (i > 0) sb.append(", ");
            double angle = 2.0 * Math.PI * (i % vertices) / vertices;
            sb.append(String.format("%.6f %.6f",
                    centerLon + radius * Math.cos(angle),
                    centerLat + radius * Math.sin(angle)));
        }
        sb.append("))");
        return sb.toString();
    }
}
