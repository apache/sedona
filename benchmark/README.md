# Sedona Geography Benchmarks

JMH microbenchmarks measuring the performance of all Geography ST functions.

## What is benchmarked

### GeographyFunctionsBench

Benchmarks every function from the Geography roadmap across multiple geometry types.

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

### SerializerComparisonBench

Compares end-to-end performance of ST functions using the new WKB serializer vs the legacy S2-native serializer. Each benchmark simulates the real Spark UDT path: deserialize bytes into a Geography object, then execute the ST function.

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

> Environment: macOS Darwin 24.2.0, Apple M-series, JDK 11, JMH 1.37, 2-3 warmup + 3-5 measurement iterations, 1 fork.
> Results will vary by hardware. Run your own benchmarks to get numbers for your environment.

### Geography Function Performance

#### Constructors

| Function | Point (ns/op) | Polygon_16 (ns/op) | Notes |
|----------|:-------------:|:-------------------:|-------|
| ST_GeogFromWKB | 3 | 3 | Zero-parse, just wraps byte array |
| ST_GeogFromWKT | 1,958 | 12,391 | S2 WKT parse + WKB conversion |
| ST_GeogFromEWKT | 1,986 | 12,617 | Same as WKT + SRID extraction |
| ST_GeomToGeography | 278 | 10,052 | S2 builder normalization + WKB wrap |
| ST_GeogToGeometry | 2 | 2 | Returns cached JTS, near-zero cost |

#### Level 0+1: Structural (JTS-only path)

| Function | Point (ns/op) | Polygon_16 (ns/op) | Notes |
|----------|:-------------:|:-------------------:|-------|
| ST_NPoints | 2 | 2 | JTS cached, trivial accessor |
| ST_GeometryType | 5 | 5 | JTS cached, trivial accessor |
| ST_NumGeometries | 2 | 2 | JTS cached, trivial accessor |
| ST_Centroid | 42 | 316 | JTS centroid computation + WKB wrap |
| ST_Envelope | 139 | 1,108 | S2 region bounds (Level 3 internally) |
| ST_AsText | 455 | 5,206 | JTS WKT writer |
| ST_AsEWKT | 982 | 6,042 | S2 WKT writer (includes SRID) |

#### Level 2: JTS + Spheroid (geodesic metrics)

| Function | Geography | Geometry baseline | Ratio | Notes |
|----------|:---------:|:-----------------:|:-----:|-------|
| **Point** |
| ST_Distance | 1,149 | 12 | 96x | Geodesic vs Euclidean — different computation |
| ST_Area | 2 | 2 | 1x | No-op for points |
| ST_Length | 2 | 2 | 1x | No-op for points |
| **Polygon (16 vertices)** |
| ST_Distance | 1,632 | 621 | 2.6x | Centroid extraction + geodesic |
| ST_Area | 23,062 | 11 | 2,096x | Geodesic area vs planar area |
| ST_Length | 2 | 1 | 1x | No-op for polygons (perimeter = ST_Perimeter) |

#### Level 3: S2 required

| Function | Geography | Geometry baseline | Ratio | Notes |
|----------|:---------:|:-----------------:|:-----:|-------|
| **Point** |
| ST_MaxDistance | 604 | 23 | 26x | S2 furthest edge query |
| ST_ClosestPoint | 106 | 29 | 3.6x | Point fast-path |
| ST_MinimumClearanceLine | 186 | — | — | No Geometry equivalent (2-arg) |
| ST_Contains (true) | 723 | 8 | 90x | S2 boolean op + ShapeIndex build |
| ST_Intersects (true) | 1,765 | 9 | 196x | S2 boolean op + ShapeIndex build |
| ST_Equals | 342 | 271 | 1.3x | S2 vs JTS topological equality |
| **Polygon (16 vertices)** |
| ST_MaxDistance | 24,588 | 3,208 | 7.7x | S2 furthest edge, scales with vertices |
| ST_ClosestPoint | 2,634 | 654 | 4x | S2 edge query |
| ST_MinimumClearanceLine | 2,815 | — | — | |
| ST_Contains (true) | 1,249 | 8 | 156x | S2 boolean op + ShapeIndex build |
| ST_Intersects (true) | 637 | 9 | 71x | S2 boolean op |
| ST_Equals | 2,607 | 16,268 | **0.16x** | **Geography 6x faster!** S2 O(n) vs JTS O(n^2) |

All values in nanoseconds per operation (ns/op). Lower is better.

---

### WKB Serializer vs S2-Native Serializer (deserialize + execute)

Measures the end-to-end cost of deserializing Geography from stored bytes and then executing an ST function. This simulates the real Spark UDT execution path.

#### Point

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|:-----------:|:------------------:|:-----------:|
| ST_Distance | 1,257 | 1,833 | **1.5x faster** |
| ST_Area | 54 | 334 | **6.2x faster** |
| ST_Length | 57 | 327 | **5.7x faster** |
| ST_Contains | 8,030 | 2,095 | 3.8x slower |
| ST_Intersects | 7,496 | 1,357 | 5.5x slower |
| ST_Equals | 4,108 | 922 | 4.5x slower |

#### Polygon (16 vertices)

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|:-----------:|:------------------:|:-----------:|
| ST_Distance | 2,296 | 5,035 | **2.2x faster** |
| ST_Area | 23,739 | 24,977 | **1.05x faster** |
| ST_Length | 334 | 1,705 | **5.1x faster** |
| ST_Contains | 8,047 | 2,080 | 3.9x slower |
| ST_Intersects | 7,328 | 1,335 | 5.5x slower |
| ST_Equals | 22,363 | 3,983 | 5.6x slower |

#### Polygon (64 vertices)

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|:-----------:|:------------------:|:-----------:|
| ST_Distance | 5,147 | 12,954 | **2.5x faster** |
| ST_Area | 88,551 | 92,152 | **1.04x faster** |
| ST_Length | 1,191 | 5,037 | **4.2x faster** |
| ST_Contains | 8,017 | 2,086 | 3.8x slower |
| ST_Intersects | 7,367 | 1,370 | 5.4x slower |
| ST_Equals | 83,273 | 18,438 | 4.5x slower |

#### Analysis

**WKB wins for Level 2 (JTS + Spheroid) metric functions:**
- ST_Distance, ST_Length, ST_Area are **1.04x to 6.2x faster** with WKB. The WKB path benefits from near-zero deserialization cost — it just stores the byte array, then lazily parses to JTS only when needed. The S2-native path pays for full S2 object reconstruction on every deserialization.

**S2-native wins for Level 3 (S2 predicate) functions:**
- ST_Contains, ST_Intersects, ST_Equals are **3.8x to 5.6x slower** with WKB. The WKB path must parse WKB to S2 Geography and then build a ShapeIndex on every call, while S2-native already has S2 objects ready after deserialization.

**Why WKB is the right default:** Most analytics workloads are dominated by metric operations (distance, area, length), which are the most common Geography functions. For predicate-heavy workloads, the lazy S2 cache in `WKBGeography` amortizes the parse cost across repeated operations on the same object — these benchmarks measure the worst case (fresh deserialize on every call, no cache reuse).

## Interpreting results

- **Geography vs Geometry ratios** are expected to be high for metric functions (ST_Distance, ST_Area) because Geography uses geodesic math on the WGS84 ellipsoid while Geometry uses trivial planar Euclidean math. These measure fundamentally different computations, not WKBGeography overhead.
- **Serializer comparison** measures the combined cost of deserialization + function execution, which is what matters in practice (the Spark UDT path).
- **Constructor benchmarks** show that `ST_GeogFromWKB` at 3 ns is ~600x faster than `ST_GeogFromWKT` — the zero-parse WKB path is a major advantage of the WKBGeography architecture.
- **Level 1 structural functions** (ST_NPoints, ST_GeometryType, ST_NumGeometries) run at 2-5 ns because they hit the cached JTS Geometry — effectively free after the first access.
- Results are in nanoseconds per operation (ns/op). Lower is better.
