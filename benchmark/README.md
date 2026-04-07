# Sedona Geography Benchmarks

JMH microbenchmarks measuring the performance of Geography ST functions compared to their Geometry equivalents.

## What is benchmarked

### GeographyFunctionsBench

Compares Geography (spherical, WGS84) vs Geometry (planar, JTS) for each ST function across multiple geometry types.

**Geometry types** (parameterized via `geometryType`):
- `point` — single points
- `linestring_16` — linestring with 16 vertices
- `polygon_16` — circular polygon with 16 vertices
- `polygon_64` — circular polygon with 64 vertices

**Functions benchmarked:**

| Level | Geography function | Geometry baseline | Implementation path |
|-------|-------------------|-------------------|---------------------|
| 2 | ST_Distance | `Functions.distance` | JTS + `Spheroid.distance()` (GeographicLib WGS84) |
| 2 | ST_Area | `Functions.area` | JTS + `Spheroid.area()` (GeographicLib WGS84) |
| 2 | ST_Length | `Functions.length` | JTS + `Spheroid.length()` (GeographicLib WGS84) |
| 3 | ST_MaxDistance | `Functions.maxDistance` | S2 `S2FurthestEdgeQuery` |
| 3 | ST_ClosestPoint | `Functions.closestPoint` | S2 `S2ClosestEdgeQuery` |
| 3 | ST_MinimumClearanceLine | — | S2 `S2ClosestEdgeQuery` (no Geometry equivalent with 2 args) |
| 3 | ST_Intersects | `Predicates.intersects` | S2 `S2BooleanOperation.intersects()` |
| 3 | ST_Contains | `Predicates.contains` | S2 `S2BooleanOperation.contains()` |
| 3 | ST_Equals | `Predicates.equals` | S2 `S2BooleanOperation.equals()` |

### SerializationBench

Measures serialization/deserialization performance and lazy caching overhead.

**Geometry types** (parameterized via `geometryType`): `point`, `polygon_small`, `polygon_large` (64 vertices)

**Operations benchmarked:**
- `serialize_wkb` vs `serialize_s2native` — WKBGeography (new) vs S2-native (legacy) serialization
- `deserialize_wkb` vs `deserialize_s2native` — deserialization round-trip
- `construct_fromWKB` vs `construct_fromWKT` — zero-parse WKB path vs WKT parsing
- `lazyCache_jts_firstAccess` vs `lazyCache_jts_cachedAccess` — JTS lazy parse overhead
- `lazyCache_s2_firstAccess` vs `lazyCache_s2_cachedAccess` — S2 lazy parse overhead

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
# Run all benchmarks (takes ~15 minutes with default settings)
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar

# Run specific benchmark class
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar GeographyFunctionsBench
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar SerializationBench

# Run specific method
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar "GeographyFunctionsBench.distance"

# Quick run (fewer iterations, for development)
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar -wi 2 -i 3 -f 1

# Run with specific geometry type only
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar -p geometryType=polygon_64

# Output results as JSON
java -jar benchmark/target/sedona-benchmark-1.9.0-SNAPSHOT.jar -rf json -rff results.json
```

## JMH parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-wi` | 3 | Warmup iterations |
| `-i` | 5 | Measurement iterations |
| `-f` | 1 | Forks (JVM restarts) |
| `-r` | 1s | Time per iteration |
| `-p geometryType=X` | all | Filter geometry type |
| `-rf json` | text | Output format (json, csv, text) |
| `-rff file` | stdout | Output file |

### SerializerComparisonBench

Compares end-to-end performance of ST functions using the new WKB serializer vs the legacy S2-native serializer. Each benchmark simulates the real Spark UDT path: deserialize bytes into a Geography object, then execute the ST function.

**Geometry types** (parameterized via `geometryType`): `point`, `polygon_16`, `polygon_64`

**Functions benchmarked** (each measured with both serializers):
- `distance_wkb` vs `distance_s2native`
- `area_wkb` vs `area_s2native`
- `length_wkb` vs `length_s2native`
- `contains_wkb` vs `contains_s2native`
- `intersects_wkb` vs `intersects_s2native`
- `equals_wkb` vs `equals_s2native`

## Sample results

> Environment: macOS Darwin 24.2.0, Apple M-series, JDK 11, JMH 1.37, 3 warmup + 5 measurement iterations, 1 fork.
> Results will vary by hardware. Run your own benchmarks to get numbers for your environment.

### WKB Serializer vs S2-Native Serializer (deserialize + execute)

#### Point

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|------------:|-------------------:|------------:|
| ST_Distance | 1,241 | 1,825 | **1.5x faster** |
| ST_Area | 56 | 339 | **6.1x faster** |
| ST_Length | 53 | 331 | **6.2x faster** |
| ST_Contains | 7,934 | 2,155 | 3.7x slower |
| ST_Intersects | 7,396 | 1,370 | 5.4x slower |
| ST_Equals | 4,021 | 914 | 4.4x slower |

#### Polygon (16 vertices)

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|------------:|-------------------:|------------:|
| ST_Distance | 2,347 | 5,114 | **2.2x faster** |
| ST_Area | 23,070 | 24,078 | **1.04x faster** |
| ST_Length | 329 | 1,708 | **5.2x faster** |
| ST_Contains | 8,070 | 2,090 | 3.9x slower |
| ST_Intersects | 7,344 | 1,337 | 5.5x slower |
| ST_Equals | 22,986 | 4,088 | 5.6x slower |

#### Polygon (64 vertices)

| Function | WKB (ns/op) | S2-native (ns/op) | WKB speedup |
|----------|------------:|-------------------:|------------:|
| ST_Distance | 5,290 | 12,870 | **2.4x faster** |
| ST_Area | 90,141 | 93,992 | **1.04x faster** |
| ST_Length | 1,166 | 5,031 | **4.3x faster** |
| ST_Contains | 8,153 | 2,096 | 3.9x slower |
| ST_Intersects | 7,324 | 1,353 | 5.4x slower |
| ST_Equals | 80,429 | 18,508 | 4.3x slower |

#### Analysis

**WKB wins for Level 2 (JTS + Spheroid) metric functions:**
- ST_Distance, ST_Length, ST_Area are **1.04x to 6.2x faster** with WKB. The WKB path benefits from near-zero deserialization cost — it just stores the byte array, then lazily parses to JTS only when needed. The S2-native path pays for full S2 object reconstruction on every deserialization.

**S2-native wins for Level 3 (S2 predicate) functions:**
- ST_Contains, ST_Intersects, ST_Equals are **3.7x to 5.6x slower** with WKB. The WKB path must parse WKB to S2 Geography and then build a ShapeIndex on every call, while S2-native already has S2 objects ready after deserialization.

**Why WKB is the right default:** Most analytics workloads are dominated by metric operations (distance, area, length), which are the most common Geography functions. For predicate-heavy workloads, the lazy S2 cache in `WKBGeography` amortizes the parse cost across repeated operations on the same object — these benchmarks measure the worst case (fresh deserialize on every call, no cache reuse).

## Interpreting results

- **Geography vs Geometry ratios** are expected to be high for metric functions (ST_Distance, ST_Area, ST_Length) because Geography uses geodesic math on the WGS84 ellipsoid while Geometry uses trivial planar Euclidean math. These measure fundamentally different computations, not WKBGeography overhead.
- **Serializer comparison** measures the combined cost of deserialization + function execution, which is what matters in practice (the Spark UDT path).
- **Lazy cache benchmarks** measure the one-time cost of parsing WKB to JTS or S2, and the near-zero cost of subsequent cached access.
- Results are in nanoseconds per operation (ns/op). Lower is better.
