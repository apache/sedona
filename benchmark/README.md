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

## Interpreting results

- **Geography vs Geometry ratios** are expected to be high for metric functions (ST_Distance, ST_Area, ST_Length) because Geography uses geodesic math on the WGS84 ellipsoid while Geometry uses trivial planar Euclidean math. These measure fundamentally different computations, not WKBGeography overhead.
- **Serialization benchmarks** directly measure the overhead of the WKB format vs S2-native format.
- **Lazy cache benchmarks** measure the one-time cost of parsing WKB to JTS or S2, and the near-zero cost of subsequent cached access.
- Results are in nanoseconds per operation (ns/op). Lower is better.
