# Sedona Geography Type Alignment Plan

## Goal
Support Geography type and operations in Sedona using a shared serialization format with SedonaDB, while maintaining performance comparable to Geometry operations.

---

## Architecture Context

### Runtime Differences
| Aspect | Sedona (Java) | SedonaDB (Rust) |
|--------|---------------|-----------------|
| Memory | JVM heap | Off-heap (Arrow) |
| Engine | Spark SQL | DataFusion |
| Integration | Native Spark | Comet plugin |

### Interoperability Model
Interop happens at the **data format level**, not in-memory object level:

```
Storage Layer (Parquet/Arrow with GeoArrow WKB)
                    │
                    ▼
        ┌───────────────────────┐
        │   Arrow IPC Format    │  ← Shared binary format
        └───────────────────────┘
           │                 │
           ▼                 ▼
    ┌────────────┐    ┌────────────┐
    │  SedonaDB  │    │   Sedona   │
    │  Wkb<'a>   │    │ Geography  │
    │ (zero-copy)│    │ (S2 parsed)│
    └────────────┘    └────────────┘
```

### Comet Integration
- Comet executes SedonaDB kernels natively
- Arrow batches passed between JVM ↔ Native via memory mapping
- Both systems must agree on WKB byte layout and GeoArrow metadata

---

## Shared Format Specification

### Target Format: GeoArrow WKB with Spherical Edges

| Property | Value |
|----------|-------|
| Extension Name | `geoarrow.wkb` |
| Storage Type | `Binary` or `BinaryView` |
| Metadata | `{"edges":"spherical","crs":<crs-spec>}` |
| Byte Layout | Standard ISO WKB |

### Key Difference from Geometry
- **Geometry**: `{"edges":"planar"}` or no edges attribute
- **Geography**: `{"edges":"spherical"}`

---

## Phase 1: WKB Serialization with WKBGeography — ✅ DONE

> Implemented Option B: WKB with Cached S2 (Runtime Only)

### What Was Built

**Core classes (new):**
- `WKBGeography.java` — Geography subclass storing WKB bytes as primary representation, with lazy JTS and S2 caches (double-checked locking). Factory methods: `fromWKB`, `fromJTS`, `fromS2Geography`.
- `GeographyWKBSerializer.java` — WKB serializer with `0xFF` format byte. Backward-compatible: reads legacy S2-native format (first byte 1-10) via `GeographySerializer`.

**Serialization switch:**
- `GeographyUDT.scala` — switched from `GeographySerializer` to `GeographyWKBSerializer`. All new data written as WKB; existing S2-native data still readable.

**Constructor updates (`Constructors.java`):**
- `geogFromWKB()` → zero-parse `WKBGeography.fromWKB()` with EWKB SRID extraction
- `geogFromWKT()` → S2 WKTReader (for proper spherical normalization) → `WKBGeography.fromS2Geography()`
- `geomToGeography()` → S2 builder (for dedup) → `WKBGeography.fromS2Geography()`
- `geogToGeometry()` → fast path via `getJTSGeometry()` for WKBGeography instances

**Geography functions (`geography/Functions.java`):**

| Function | Path | Returns |
|----------|------|---------|
| ST_Distance | JTS + `Spheroid.distance()` | meters |
| ST_Area | JTS + `Spheroid.area()` | m² |
| ST_Length | JTS + `Spheroid.length()` | meters |
| ST_MaxDistance | S2 `S2FurthestEdgeQuery` | meters |
| ST_ClosestPoint | S2 `S2ClosestEdgeQuery` | Geography point |
| ST_MinimumClearanceLine | S2 `S2ClosestEdgeQuery` | Geography linestring |
| ST_Equals | S2 `S2BooleanOperation.equals()` | boolean |
| ST_Intersects | S2 `S2BooleanOperation.intersects()` | boolean |
| ST_Contains | S2 `S2BooleanOperation.contains()` | boolean |
| ST_AsEWKT | `Geography.toEWKT()` | string |
| ST_Envelope | S2 `region().getRectBound()` | Geography |

**Spark expression dual-dispatch (`Functions.scala`, `Predicates.scala`):**
- `ST_Distance`, `ST_Area`, `ST_Length`, `ST_MaxDistance`, `ST_ClosestPoint` — `InferredExpression` with both Geometry and Geography overloads
- `ST_Contains`, `ST_Intersects`, `ST_Equals` — converted from `ST_Predicate` base to `InferredExpression` dual-dispatch (Option B approach)
- `JoinQueryDetector.scala` — updated to match the three converted predicates before the `ST_Predicate` catch-all

**Tests:**
- 26 unit tests in `WKBGeographyTest.java` (lazy parsing, round-trips, backward compat, SRID, constructors)
- 24 unit tests in `FunctionTest.java` (metrics, predicates, envelope)
- All 1030 common module tests pass

### Operation Tiers

```
┌─────────────────────────────────────────────────────────────────┐
│  Level 0: WKB Bytes Only (no parsing)                          │
│  ST_AsBinary, ST_AsEWKB, ST_GeomFromWKB                         │
├─────────────────────────────────────────────────────────────────┤
│  Level 1: JTS Parse (WKB → JTS Geometry, lazy cached)           │
│  ST_AsText, ST_NumPoints, ST_GeometryType, ST_Envelope,         │
│  ST_X, ST_Y, ST_StartPoint, ST_EndPoint, ST_Centroid (planar)   │
├─────────────────────────────────────────────────────────────────┤
│  Level 2: JTS + GeographicLib (geodesic formulas)               │
│  ST_Distance, ST_Length, ST_Area, ST_Perimeter,                 │
│  ST_Azimuth, ST_Project                                         │
├─────────────────────────────────────────────────────────────────┤
│  Level 3: S2 Required (spherical predicates/operations)         │
│  ST_Intersects, ST_Contains, ST_Within, ST_Covers,              │
│  ST_Equals, ST_MaxDistance, ST_ClosestPoint,                     │
│  ST_MinimumClearanceLine, ST_DWithin, ST_Buffer,                │
│  ST_Intersection, ST_Union, Spatial Join (Range, etc.)          │
└─────────────────────────────────────────────────────────────────┘
```

### Performance Targets

| Operation | Target (vs Geometry) |
|-----------|---------------------|
| Serialization (WKB round-trip) | < 1.2x slower |
| ST_Distance / ST_Area / ST_Length | < 1.5x slower |
| Point-in-polygon (S2 predicate) | < 2x slower |
| Spatial join (S2 predicate) | < 3x slower |

---

## Phase 2: Remaining Work

### 2.1 Join Path: Geography Column → JTS Geometry

**File**: `spark/common/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryBase.scala`

In `toSpatialRDD`, detect Geography column type and extract JTS for spatial join operations.

### 2.2 GeoArrow Metadata

Add `{"edges":"spherical"}` extension metadata to `GeographyUDT` for Comet/SedonaDB interop.

### 2.3 Range Join Support

S2 cell indexing for Geography range joins — backlog item.

---

## Phase 4: Full GeoArrow Integration (Long-term)

### 4.1 Native GeoArrow Array Support
- [ ] Support reading Geography from GeoArrow Parquet files
- [ ] Support writing Geography to GeoArrow Parquet files
- [ ] Integrate with `sedona-geoparquet` (SedonaDB module)

### 4.2 Deprecate Native Format
- [ ] Add migration utility: native → WKB
- [ ] Document migration path for existing users
- [ ] Remove native format in future major version

### 4.3 Spatial Index Separation
- [ ] Move S2CellUnion covering out of serialization
- [ ] Store covering in separate column or metadata
- [ ] Enable pruning without format coupling

---

## Phase 5: Cross-System Interoperability (Long-term)

### 5.1 Comet Integration
```
Spark SQL Query
      │
      ▼
┌─────────────────────────────────────────────┐
│  Comet: Decides JVM vs Native execution     │
├─────────────────────────────────────────────┤
│  Geography op supported in SedonaDB?        │
│  ├─ Yes → Execute native (SedonaDB kernel)  │
│  └─ No  → Fallback to JVM (Sedona)          │
└─────────────────────────────────────────────┘
```
- [ ] Ensure Sedona Geography UDT produces Arrow-compatible batches
- [ ] Implement fallback path for ops not in SedonaDB
- [ ] Minimize JVM ↔ Native transitions (batch operations)

### 5.2 Data Format Compatibility
- [ ] Verify WKB bytes identical between systems
- [ ] Verify GeoArrow metadata parsing consistent
- [ ] Add round-trip tests: Sedona → Parquet → SedonaDB → Parquet → Sedona

### 5.3 External System Compatibility
- [ ] Test with PostGIS Geography import/export
- [ ] Test with BigQuery Geography
- [ ] Document compatibility matrix

---

## Success Criteria

1. **Format Alignment**: Geography data portable between Sedona and SedonaDB without conversion
2. **Performance**: Geography operations within 2x of equivalent Geometry operations
3. **Compatibility**: GeoArrow/GeoParquet files readable by both systems
4. **Migration**: Clear upgrade path for existing Sedona Geography users
5. **Comet Ready**: Seamless execution path switching between JVM and native

---

## Design Notes

### Why Different In-Memory Representations Are OK

| System | In-Memory | Reason |
|--------|-----------|--------|
| SedonaDB | `Wkb<'a>` (zero-copy ref) | Rust lifetime system enables safe borrowing |
| Sedona | `Geography` (S2 objects) | JVM GC manages object lifecycle |

**The contract is on serialized bytes, not objects:**
- Both produce identical WKB bytes when writing
- Both accept identical WKB bytes when reading
- In-memory representation is an implementation detail

### Performance Tradeoff
- SedonaDB: Fast iteration (zero-copy), pays cost at operation time
- Sedona: Pays parse cost upfront, fast S2 operations after
- Comet: Use SedonaDB for bulk operations, Sedona for complex fallbacks

### WKB Efficiency Concerns for Geography

**WKB is NOT the most efficient format for spherical data:**

| Metric | WKB | S2 Native (Sedona current) |
|--------|-----|---------------------------|
| Point size | 16 bytes | 8 bytes (COMPACT) |
| Edge info | External metadata | Inherent |
| Spatial index | Not included | Optional covering |
| S2 op cost | Parse + convert | Direct access |

**Why WKB anyway?**
- SedonaDB/Comet requires it
- GeoParquet standard
- Universal tooling support

**Mitigation strategies:**
1. **Lazy parsing** - Don't convert to S2 until needed
2. **Caching** - Cache S2 structures after first use
3. **Comet offload** - Let SedonaDB handle S2 ops natively
4. **Hybrid storage** - WKB for interchange, S2 for internal ops

### Chosen Format: Option B — WKB with Cached S2 (Runtime Only)

```
┌─────────────────────────────────────────────────────────┐
│ Storage: WKB (GeoArrow compatible)                      │
│ Runtime: Lazy JTS + Lazy S2 cache per object            │
│   - First metric op triggers JTS parse (cached)         │
│   - First predicate op triggers S2 parse (cached)       │
│   - WKB bytes always retained as primary representation │
└─────────────────────────────────────────────────────────┘
```

**Why Option B**: Balances GeoArrow/SedonaDB compatibility (WKB storage) with operational performance (lazy caching avoids redundant parsing). Most analytics queries (distance, area, length) only need JTS + GeographicLib and never trigger S2 parsing.

**Alternatives considered but not chosen:**
- Option A (WKB + Sidecar Covering): Adds schema complexity; covering can be added later as an optimization
- Option C (Dual Format Write): Configuration burden; two code paths to maintain

### When Does Sedona Need S2 Parsing?

| Scenario | S2 Parsing Needed? | Reason |
|----------|-------------------|--------|
| Comet + SedonaDB has the op | No | Native execution, WKB passthrough |
| Comet + op not in SedonaDB | Maybe | Depends on operation type |
| Pure Spark (no Comet) | Maybe | Depends on operation type |
| Read/Write only | No | Just serialize/deserialize WKB |

### Operation Categories (Without Comet)

See Phase 1 "Operation Tiers" for the current categorization. Key principle: most queries use Level 0-2 operations (JTS path). S2 parsing only for Level 3 (predicates, spatial operations).

### Can We Avoid S2 Entirely in Sedona Java?

**Answer: Partially yes, for many common operations.**

| Category | S2 Needed? | Alternative |
|----------|-----------|-------------|
| Metric ops (distance, area, length) | No | JTS + GeographicLib (already in Sedona) |
| Structural ops (numPoints, type) | No | JTS |
| Serialization (WKB read/write) | No | JTS WKBReader/Writer |
| Predicates (intersects, contains) | **Yes** | S2 or equivalent spherical library |
| Spatial join | **Yes** | S2 cell indexing |

**Recommendation**: Implement tiered approach where S2 is only loaded for predicate operations. Most analytics queries (distance calculations, aggregations) can run entirely on JTS + GeographicLib.

---

---

## Appendix: Function Implementation Roadmap

### Current Status

| Function | Status | S2 Required? | Implementation Path |
|----------|--------|--------------|---------------------|
| **Constructors (Done)** |
| ST_GeogFromWKB | ✅ Done | No | WKB parse |
| ST_GeogFromEWKB | ✅ Done | No | WKB parse |
| ST_GeogFromWKT | ✅ Done | No | WKT parse |
| ST_GeogFromEWKT | ✅ Done | No | WKT parse |
| ST_GeogFromText | ✅ Done | No | WKT parse |
| ST_GeogFromGeohash | ✅ Done | No | Geohash decode |
| ST_TryToGeography | ✅ Done | No | Validation |
| ST_ToGeography | ✅ Done | No | Type cast |
| **Converters (Done)** |
| ST_GeogToGeometry | ✅ Done | No | Type cast |
| ST_GeomToGeography | ✅ Done | No | Type cast |
| ST_Envelope | ✅ Done | No | JTS |
| **Output (Done)** |
| ST_AsEWKT | ✅ Done | No | `Geography.toEWKT()` via S2 WKTWriter |
| **Metrics (Done)** |
| ST_Distance | ✅ Done | **No** | `WKBGeography.getJTSGeometry()` → `Spheroid.distance()` |
| ST_Area | ✅ Done | **No** | `WKBGeography.getJTSGeometry()` → `Spheroid.area()` |
| ST_Length | ✅ Done | **No** | `WKBGeography.getJTSGeometry()` → `Spheroid.length()` |
| ST_MaxDistance | ✅ Done | **Yes** | S2 `S2FurthestEdgeQuery` → radians → meters |
| ST_ClosestPoint | ✅ Done | **Yes** | S2 `S2ClosestEdgeQuery` → Geography point |
| ST_MinimumClearanceLine | ✅ Done | **Yes** | S2 `S2ClosestEdgeQuery` → Geography linestring |
| **Predicates (Done)** |
| ST_Equals | ✅ Done | **Yes** | S2 `S2BooleanOperation.equals()` |
| ST_Intersects | ✅ Done | **Yes** | S2 `S2BooleanOperation.intersects()` |
| ST_Contains | ✅ Done | **Yes** | S2 `S2BooleanOperation.contains()` |
| **Join (Backlog)** |
| Range join support | 📋 Backlog | **Yes** | S2 cell indexing |
| **Infrastructure** |
| benchmark | 🔄 In Progress | - | JMH module |

### Implementation Priority

```
Phase 1 (Foundation - WKBGeography): ✅ DONE
├── WKBGeography class (WKB bytes + lazy JTS + lazy S2)
├── GeographyWKBSerializer (0xFF format byte + backward compat)
├── GeographyUDT switch to WKB serialization
└── Constructor updates (return WKBGeography)

Phase 2 (Metric Functions - No S2): ✅ DONE
├── ST_AsEWKT
├── ST_Distance      ← WKBGeography → JTS → Spheroid.distance()
├── ST_Area          ← WKBGeography → JTS → Spheroid.area()
└── ST_Length        ← WKBGeography → JTS → Spheroid.length()

Phase 3 (S2 Functions - via lazy cache): ✅ DONE
├── ST_MaxDistance         ← S2FurthestEdgeQuery → radians → meters
├── ST_ClosestPoint        ← S2ClosestEdgeQuery → Geography point
├── ST_MinimumClearanceLine ← S2ClosestEdgeQuery → Geography linestring
├── ST_Equals              ← S2BooleanOperation.equals()
├── ST_Intersects          ← S2BooleanOperation.intersects()
└── ST_Contains            ← S2BooleanOperation.contains()

Phase 4 (Remaining):
├── Join path: TraitJoinQueryBase Geography → JTS for spatial joins
├── GeoArrow metadata for Comet/SedonaDB interop
└── Range join support (S2 cell indexing)
```

### Key Observation

**All planned functions are implemented, including Spark expression wiring!**

- 13 functions use JTS path only (no S2 parse): constructors, converters, metric ops
- 6 functions use S2 via lazy cache: ST_MaxDistance, ST_ClosestPoint, ST_MinimumClearanceLine, ST_Equals, ST_Intersects, ST_Contains
- Spark dual-dispatch wired for all functions via `InferredExpression` pattern
- Remaining: Join path updates, GeoArrow metadata, Range join support

---

## References

- [GeoArrow Specification](https://geoarrow.org/extension-types)
- [SedonaDB Geography Implementation](https://github.com/apache/sedona-db)
- [PR #2307 - Geography Benchmarks](https://github.com/apache/sedona/pull/2307)
- [S2Geometry Library](https://s2geometry.io/)
