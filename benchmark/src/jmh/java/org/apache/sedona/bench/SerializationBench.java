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
import org.apache.sedona.common.geography.Constructors;
import org.locationtech.jts.io.ParseException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for Geography serialization: WKBGeography (new) vs S2-native (legacy).
 *
 * Measures:
 * - Serialize: Geography -> bytes (WKB vs S2-native)
 * - Deserialize: bytes -> Geography (WKB vs S2-native)
 * - Constructor: geogFromWKB (zero-parse) vs geogFromWKT
 * - Lazy cache: first vs subsequent access to JTS and S2
 *
 * Run: java -jar benchmark/target/sedona-benchmark-*.jar SerializationBench
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class SerializationBench {

    @Param({"point", "polygon_small", "polygon_large"})
    public String geometryType;

    // S2-native Geography (for legacy serialization baseline)
    private Geography s2Geography;

    // WKBGeography (for new serialization)
    private Geography wkbGeography;

    // Pre-serialized bytes
    private byte[] wkbBytes;
    private byte[] s2NativeBytes;

    // Raw WKB for constructor benchmark
    private byte[] rawWkb;

    // WKT string for constructor benchmark
    private String wkt;

    @Setup(Level.Trial)
    public void setup() throws ParseException, IOException {
        switch (geometryType) {
            case "point":
                wkt = "POINT (30 10)";
                break;
            case "polygon_small":
                wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
                break;
            case "polygon_large":
                wkt = buildLargePolygonWKT(64);
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + geometryType);
        }

        // Build S2 Geography via WKTReader (legacy path)
        s2Geography = new WKTReader().read(wkt);
        s2Geography.setSRID(4326);

        // Build WKBGeography via constructor (new path)
        wkbGeography = Constructors.geogFromWKT(wkt, 4326);

        // Pre-serialize for deserialize benchmarks
        wkbBytes = GeographyWKBSerializer.serialize(wkbGeography);
        s2NativeBytes = GeographySerializer.serialize(s2Geography);

        // Raw WKB for geogFromWKB benchmark
        rawWkb = ((WKBGeography) wkbGeography).getWKBBytes();
    }

    // ─── Serialize benchmarks ────────────────────────────────────────────────

    @Benchmark
    public void serialize_wkb(Blackhole bh) throws IOException {
        bh.consume(GeographyWKBSerializer.serialize(wkbGeography));
    }

    @Benchmark
    public void serialize_s2native(Blackhole bh) throws IOException {
        bh.consume(GeographySerializer.serialize(s2Geography));
    }

    // ─── Deserialize benchmarks ──────────────────────────────────────────────

    @Benchmark
    public void deserialize_wkb(Blackhole bh) throws IOException {
        bh.consume(GeographyWKBSerializer.deserialize(wkbBytes));
    }

    @Benchmark
    public void deserialize_s2native(Blackhole bh) throws IOException {
        bh.consume(GeographyWKBSerializer.deserialize(s2NativeBytes));
    }

    // ─── Constructor benchmarks ──────────────────────────────────────────────

    @Benchmark
    public void construct_fromWKB(Blackhole bh) throws ParseException {
        // Zero-parse: just wraps the byte array
        bh.consume(Constructors.geogFromWKB(rawWkb, 4326));
    }

    @Benchmark
    public void construct_fromWKT(Blackhole bh) throws ParseException {
        // Parses WKT -> S2 -> WKBGeography
        bh.consume(Constructors.geogFromWKT(wkt, 4326));
    }

    // ─── Lazy cache benchmarks ───────────────────────────────────────────────

    @Benchmark
    public void lazyCache_jts_firstAccess(Blackhole bh) throws ParseException {
        // Create fresh WKBGeography each time to measure first JTS parse
        WKBGeography fresh = WKBGeography.fromWKB(rawWkb, 4326);
        bh.consume(fresh.getJTSGeometry());
    }

    @Benchmark
    public void lazyCache_jts_cachedAccess(Blackhole bh) {
        // JTS is already cached from setup
        bh.consume(((WKBGeography) wkbGeography).getJTSGeometry());
    }

    @Benchmark
    public void lazyCache_s2_firstAccess(Blackhole bh) throws ParseException {
        // Create fresh WKBGeography each time to measure first S2 parse
        WKBGeography fresh = WKBGeography.fromWKB(rawWkb, 4326);
        bh.consume(fresh.getS2Geography());
    }

    @Benchmark
    public void lazyCache_s2_cachedAccess(Blackhole bh) {
        // S2 is already cached from setup (triggered by geogFromWKT path)
        bh.consume(((WKBGeography) wkbGeography).getS2Geography());
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static String buildLargePolygonWKT(int vertices) {
        StringBuilder sb = new StringBuilder("POLYGON ((");
        double step = 360.0 / vertices;
        for (int i = 0; i <= vertices; i++) {
            double angle = Math.toRadians(i * step);
            double lon = 0.5 * Math.cos(angle);
            double lat = 0.5 * Math.sin(angle);
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.6f %.6f", lon, lat));
        }
        sb.append("))");
        return sb.toString();
    }
}
