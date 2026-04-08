<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Sedona Geography Benchmarks

JMH microbenchmarks measuring the performance of all Geography ST functions.

## What is benchmarked

### GeographyFunctionsBench

Benchmarks every function from the Geography roadmap across multiple geometry types. Geography objects are pre-constructed — this measures **function execution cost** with cached lazy state (JTS, S2, ShapeIndex all warmed up).

**Geometry types** (parameterized via `geometryType`):
- `point` — single points
- `linestring_16` — linestring with 16 vertices
- `polygon_16` — circular polygon with 16 vertices
- `polygon_64` — circular polygon with 64 vertices

**Functions benchmarked:**

| Category | Functions |
|----------|----------|
| Constructors | ST_GeogFromWKB, ST_GeogFromWKT, ST_GeogFromEWKT |
| Converters | ST_GeogToGeometry, ST_GeomToGeography |
| Level 0+1 (Structural) | ST_Envelope, ST_AsEWKT, ST_AsText, ST_NPoints, ST_GeometryType, ST_NumGeometries, ST_Centroid |
| Level 2 (Geodesic metrics) | ST_Distance, ST_Area, ST_Length (with Geometry baselines) |
| Level 3 (S2 required) | ST_MaxDistance, ST_ClosestPoint, ST_MinimumClearanceLine, ST_Intersects, ST_Contains, ST_Equals (with Geometry baselines) |

### SerializationBench

Measures serialization/deserialization performance and lazy caching overhead.

**Geometry types** (parameterized): `point`, `polygon_small`, `polygon_large` (64 vertices)

**Operations benchmarked:**
- `serialize_wkb` vs `serialize_s2native` — WKBGeography (new) vs S2-native (legacy) serialization
- `deserialize_wkb` vs `deserialize_s2native` — deserialization round-trip
- `construct_fromWKB` vs `construct_fromWKT` — zero-parse WKB path vs WKT parsing
- `lazyCache_jts_firstAccess` vs `lazyCache_jts_cachedAccess` — JTS lazy parse overhead
- `lazyCache_s2_firstAccess` vs `lazyCache_s2_cachedAccess` — S2 lazy parse overhead

### BatchBench

Batch-processing benchmarks inspired by sedona-db's Criterion benchmarks. Iterates over arrays of **1024 randomly generated geometries** (via `RandomGeoGenerator`) to measure amortized per-row cost with realistic scattered data.

**Parameters:**
- `batchSize` — rows per batch (default: 1024)
- `vertices` — vertex count per geometry: `10`, `100`, `500`

**Patterns:**
- **Array-Array**: Both inputs are arrays (spatial join: `distance(col_a[i], col_b[i])`)
- **Array-Scalar**: One array, one constant (spatial filter: `contains(constant_polygon, col[i])`)
- **Scalar-Array**: Reversed (spatial filter: `contains(col[i], constant_point)`)

**Functions benchmarked:** ST_Distance, ST_Area, ST_Length, ST_Contains, ST_Intersects, ST_Equals, ST_MaxDistance, ST_ClosestPoint — each with Geometry baselines where applicable.

### SerializerComparisonBench

Compares end-to-end performance of ST functions using the new WKB serializer vs the legacy S2-native serializer. Each benchmark **freshly deserializes** bytes into a Geography object, then executes the ST function — no cache reuse. This simulates the real Spark UDT cold path.

**Geometry types** (parameterized): `point`, `polygon_16`, `polygon_64`

**Functions benchmarked** (each measured with both serializers):
- ST_Distance, ST_Area, ST_Length, ST_Contains, ST_Intersects, ST_Equals

## How to build

From the project root:

```bash
# Build sedona-common first (required dependency)
mvn install -pl common -am -DskipTests -q

# Build the benchmark JAR
cd benchmark && mvn package -q
```

## How to run

```bash
# Run all benchmarks
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar

# Run specific benchmark class
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar GeographyFunctionsBench
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar SerializationBench
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar SerializerComparisonBench

# Run specific method
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar "GeographyFunctionsBench.ST_Distance"

# Quick run (fewer iterations, for development)
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar -wi 2 -i 3 -f 1

# Run with specific geometry type only
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar -p geometryType=polygon_64

# Output results as JSON
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar -rf json -rff results.json
```

### JMH parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-wi` | 3 | Warmup iterations |
| `-i` | 5 | Measurement iterations |
| `-f` | 1 | Forks (JVM restarts) |
| `-r` | 1s | Time per iteration |
| `-p geometryType=X` | all | Filter geometry type |
| `-rf json` | text | Output format (json, csv, text) |
| `-rff file` | stdout | Output file |

## Sample results

> Environment: macOS Darwin 24.2.0, Apple M-series, JDK 11, JMH 1.37, 2-3 warmup + 3 measurement iterations, 1 fork.
> Results will vary by hardware. Run your own benchmarks to get numbers for your environment.

### Geography Function Performance (cached objects)

These benchmarks operate on pre-constructed Geography objects where the lazy JTS, S2, and ShapeIndex caches are already warmed up. This represents the steady-state performance after the first access.

#### Constructors

| Function | Point (ns/op) | Polygon_16 (ns/op) | Notes |
|----------|:-------------:|:-------------------:|-------|
| ST_GeogFromWKB | 3 | 3 | Zero-parse, just wraps byte array |
| ST_GeogFromWKT | 2,003 | 12,286 | S2 WKT parse + WKB conversion |
| ST_GeogFromEWKT | 2,146 | 12,463 | Same as WKT + SRID extraction |
| ST_GeomToGeography | 264 | 10,173 | S2 builder normalization + WKB wrap |
| ST_GeogToGeometry | 2 | 2 | Returns cached JTS, near-zero cost |

#### Level 0+1: Structural (JTS-only path)

| Function | Point (ns/op) | Polygon_16 (ns/op) | Notes |
|----------|:-------------:|:-------------------:|-------|
| ST_NPoints | 2 | 2 | JTS cached, trivial accessor |
| ST_GeometryType | 5 | 5 | JTS cached, trivial accessor |
| ST_NumGeometries | 2 | 2 | JTS cached, trivial accessor |
| ST_Centroid | 47 | 317 | JTS centroid computation + WKB wrap |
| ST_Envelope | 139 | 1,118 | S2 region bounds (Level 3 internally) |
| ST_AsText | 450 | 5,292 | JTS WKT writer |
| ST_AsEWKT | 961 | 6,354 | S2 WKT writer (includes SRID) |

#### Level 2: JTS + Spheroid (geodesic metrics)

| Function | Geography | Geometry baseline | Ratio | Notes |
|----------|:---------:|:-----------------:|:-----:|-------|
| **Point** |
| ST_Distance | 245 | 12 | 21x | S2ClosestEdgeQuery (geometry-to-geometry) |
| ST_Area | 2 | 2 | 1x | No-op for points |
| ST_Length | 2 | 1 | 1x | No-op for points |
| **Polygon (16 vertices)** |
| ST_Distance | 1,625 | 620 | 2.6x | S2ClosestEdgeQuery (geometry-to-geometry) |
| ST_Area | 23,466 | 11 | 2,133x | Geodesic area vs planar area |
| ST_Length | 2 | 1 | 1x | No-op for polygons |

#### Level 3: S2 required (with cached ShapeIndex)

| Function | Geography | Geometry baseline | Ratio | Notes |
|----------|:---------:|:-----------------:|:-----:|-------|
| **Point** |
| ST_MaxDistance | 233 | 22 | 11x | S2 furthest edge query |
| ST_ClosestPoint | 69 | 36 | 1.9x | Point fast-path |
| ST_MinimumClearanceLine | 122 | — | — | No Geometry equivalent (2-arg) |
| ST_Contains (true) | 287 | 8 | 36x | S2BooleanOperation + cached ShapeIndex |
| ST_Contains (false) | 236 | 7 | 34x | |
| ST_Intersects (true) | 958 | 9 | 106x | S2BooleanOperation |
| ST_Intersects (false) | 644 | 2 | 322x | |
| ST_Equals | 56 | 242 | **0.23x** | **Geography 4x faster!** |
| **Polygon (16 vertices)** |
| ST_MaxDistance | 21,023 | 3,224 | 6.5x | S2 furthest edge, scales with vertices |
| ST_ClosestPoint | 1,429 | 623 | 2.3x | S2 edge query |
| ST_MinimumClearanceLine | 1,660 | — | — | |
| ST_Contains (true) | 681 | 8 | 85x | S2BooleanOperation + cached ShapeIndex |
| ST_Contains (false) | 225 | 7 | 32x | |
| ST_Intersects (true) | 245 | 9 | 27x | |
| ST_Intersects (false) | 613 | 2 | 307x | |
| ST_Equals | 153 | 16,523 | **0.009x** | **Geography 108x faster!** |

All values in nanoseconds per operation (ns/op). Lower is better.

---

### WKB Serializer vs S2-Native Serializer (deserialize + execute, no cache)

These benchmarks **freshly deserialize** Geography from stored bytes on every iteration, then execute the ST function. No ShapeIndex cache reuse. This simulates the worst-case Spark UDT path where each row is deserialized independently.

#### Point

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|:-----------:|:------------------:|:-----------:|
| ST_Distance | 4,556 | 1,255 | 3.6x slower |
| ST_Area | 58 | 334 | **5.8x faster** |
| ST_Length | 58 | 336 | **5.8x faster** |
| ST_Contains | 7,995 | 2,154 | 3.7x slower |
| ST_Intersects | 7,315 | 1,358 | 5.4x slower |
| ST_Equals | 4,110 | 938 | 4.4x slower |

#### Polygon (16 vertices)

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|:-----------:|:------------------:|:-----------:|
| ST_Distance | 24,396 | 5,656 | 4.3x slower |
| ST_Area | 24,674 | 25,523 | **1.03x faster** |
| ST_Length | 337 | 1,711 | **5.1x faster** |
| ST_Contains | 8,088 | 2,119 | 3.8x slower |
| ST_Intersects | 7,287 | 1,364 | 5.3x slower |
| ST_Equals | 22,090 | 4,074 | 5.4x slower |

#### Polygon (64 vertices)

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|:-----------:|:------------------:|:-----------:|
| ST_Distance | 153,298 | 90,545 | 1.7x slower |
| ST_Area | 94,010 | 98,764 | **1.05x faster** |
| ST_Length | 1,191 | 5,031 | **4.2x faster** |
| ST_Contains | 8,153 | 2,114 | 3.9x slower |
| ST_Intersects | 7,376 | 1,360 | 5.4x slower |
| ST_Equals | 84,547 | 18,741 | 4.5x slower |

#### Analysis

**WKB wins for Level 1-2 functions** that only need JTS (ST_Area, ST_Length, structural accessors). Near-zero deserialization cost makes these 1-6x faster.

**S2-native wins for Level 3 functions** (predicates, distance) in the cold path. S2-native data arrives pre-parsed — no WKB→S2→ShapeIndex conversion needed. The WKB path must build a fresh ShapeIndex on every deserialization, which costs ~4-8 microseconds.

**ST_Distance changed**: Now uses S2ClosestEdgeQuery for true geometry-to-geometry minimum distance (consistent with sedona-db), instead of centroid-to-centroid. This is more correct but slower in the cold path because it requires ShapeIndex construction.

**In practice, the cold-path overhead is amortized**: When the same Geography object is used multiple times (e.g., spatial filter on a column, join condition), the ShapeIndex cache in WKBGeography eliminates redundant index construction. The "cached objects" benchmark above shows the steady-state performance.

**Important**: This comparison is biased toward S2-native because the S2 path starts from pre-parsed S2 bytes. In the GeoParquet scenario below, both paths start from WKB — which tells a very different story.

---

### GeoParquet Scenario: Both paths start from WKB

This is the realistic scenario for data stored in GeoParquet, where the source data is always WKB bytes. Both the WKB path and the S2-parse-from-WKB path start from the same raw WKB bytes — a fair apples-to-apples comparison.

- **WKB path**: raw WKB → `WKBGeography` (zero-parse wrap) → lazy S2/JTS on demand
- **S2 parse path**: raw WKB → `WKBReader.read()` (full S2 parse) → `ShapeIndexGeography` → execute

#### Point

| Function | WKB path (ns/op) | S2 parse path (ns/op) | Comparison |
|----------|:----------------:|:---------------------:|:----------:|
| ST_Distance | 4,435 | 4,552 | **~same** |
| ST_Area | 50 | 3,371 | **68x faster** |
| ST_Contains | 8,229 | 8,108 | **~same** |
| ST_Intersects | 7,375 | 7,437 | **~same** |

#### Polygon (16 vertices)

| Function | WKB path (ns/op) | S2 parse path (ns/op) | Comparison |
|----------|:----------------:|:---------------------:|:----------:|
| ST_Distance | 24,962 | 24,864 | **~same** |
| ST_Area | 23,767 | 36,464 | **1.5x faster** |
| ST_Contains | 8,285 | 8,136 | **~same** |
| ST_Intersects | 7,370 | 7,330 | **~same** |

#### Polygon (64 vertices)

| Function | WKB path (ns/op) | S2 parse path (ns/op) | Comparison |
|----------|:----------------:|:---------------------:|:----------:|
| ST_Distance | 154,501 | 155,380 | **~same** |
| ST_Area | 96,072 | 134,015 | **1.4x faster** |
| ST_Contains | 8,186 | 8,197 | **~same** |
| ST_Intersects | 7,380 | 7,570 | **~same** |

#### Analysis

**When source data is GeoParquet (WKB), the WKB serializer has zero performance penalty.** Both paths pay the same WKB → S2 parse cost for predicates (ST_Contains, ST_Intersects, ST_Distance). The WKB path is faster for ST_Area because it routes through JTS + Spheroid, skipping S2 entirely.

This is the strongest justification for WKB as the default format:
- **For GeoParquet data**: WKB is never slower and sometimes 1.4-68x faster
- **For legacy S2-native data**: backward compatibility is maintained via format byte detection
- **For interoperability**: WKB is the universal standard (GeoParquet, PostGIS, BigQuery, DuckDB)

---

### Batch Processing (1024 rows, random data)

Inspired by sedona-db's Criterion benchmarks. These iterate over arrays of randomly generated geometries to measure **amortized per-row cost** in realistic batch-processing scenarios. Uses `RandomGeoGenerator` for scattered geometries with configurable vertex count.

#### Array-Array pattern (e.g., spatial join)

| Function | Geom type | 10 vtx (ns/row) | 500 vtx (ns/row) | Scaling |
|----------|-----------|:----------------:|:-----------------:|:-------:|
| ST_Distance (Geography) | point-point | 411 | 388 | O(1) |
| ST_Distance (Geometry baseline) | point-point | 38 | 25 | O(1) |
| ST_Equals (Geography) | poly-poly | 366 | 3,245 | O(n) |
| ST_Equals (Geometry baseline) | poly-poly | 12,516 | 677,355 | O(n^2) |
| ST_MaxDistance (Geography) | poly-poly | 9,009 | 1,378,957 | O(n^2) |

#### Array-Scalar pattern (e.g., spatial filter)

| Function | Geom type | 10 vtx (ns/row) | 500 vtx (ns/row) | Scaling |
|----------|-----------|:----------------:|:-----------------:|:-------:|
| ST_Distance (point vs polygon) | arr-scalar | 495 | 662 | mild |
| ST_Contains (poly vs point) | scalar-arr | 283 | 250 | O(1) |
| ST_Contains (Geometry baseline) | scalar-arr | 13 | 7 | O(1) |
| ST_Intersects (point vs poly) | arr-scalar | 593 | 559 | O(1) |
| ST_Intersects (Geometry baseline) | arr-scalar | 10 | 6 | O(1) |
| ST_ClosestPoint (line vs point) | arr-scalar | 525 | 6,011 | O(n) |

#### Unary Array pattern

| Function | Geom type | 10 vtx (ns/row) | 500 vtx (ns/row) | Scaling |
|----------|-----------|:----------------:|:-----------------:|:-------:|
| ST_Area (Geography) | polygon | 15,462 | 632,662 | O(n) |
| ST_Area (Geometry baseline) | polygon | 57 | 2,150 | O(n) |
| ST_Length (Geography) | linestring | 11,795 | 627,960 | O(n) |

All values in nanoseconds per row (total batch time / 1024). Lower is better.

#### Analysis

- **Scalar-Array predicates are efficient**: When one geometry is constant (e.g., `WHERE ST_Contains(region, point_col)`), the ShapeIndex for the constant is built once and reused across all 1024 rows. ST_Contains costs ~250-283 ns/row regardless of polygon complexity.
- **ST_Equals Geography massively outscales Geometry**: At 500 vertices, Geography is **209x faster** (3.2 us vs 677 us/row). S2's `S2BooleanOperation::Equals` with spatial indexing is O(n), while JTS `equalsTopo` is O(n^2).
- **Geodesic area/length scale linearly** with vertex count — one GeographicLib call per edge.
- **ST_MaxDistance scales O(n^2)** — `S2FurthestEdgeQuery` examines all edge pairs. At 500 vertices this reaches ~1.4 ms/row.

## Interpreting results

- **Three benchmark tiers**: `GeographyFunctionsBench` (single-call, cached), `SerializerComparisonBench` (cold deserialization), `BatchBench` (batch processing with random data). Real workloads fall between these extremes.
- **Geography vs Geometry ratios** are expected to be high for functions that do fundamentally different work (geodesic vs Euclidean). These are not overhead ratios.
- **ST_Equals is faster on Geography** for complex geometries: S2's `S2BooleanOperation::Equals()` is O(n) with spatial indexing, while JTS `equalsTopo()` is O(n^2).
- **Scalar-Array pattern** is the most realistic for spatial filters — one constant geometry against a column of data.
- **Constructor cost**: `ST_GeogFromWKB` at 3 ns is ~650x faster than `ST_GeogFromWKT` — the zero-parse WKB path is a major advantage of the WKBGeography architecture.
- Results are in nanoseconds per operation (ns/op) or nanoseconds per row (ns/row). Lower is better.
