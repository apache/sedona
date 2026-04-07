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

## Phase 1: WKB Serialization with WKBGeography (Mid-term)

> Full implementation cycle: new classes → UDT switch → constructor updates → unit tests → Spark integration tests

### 1.1 New Class: `WKBGeography`

**File (new)**: `common/src/main/java/org/apache/sedona/common/S2Geography/WKBGeography.java`

A `Geography` subclass that stores WKB bytes as primary representation with lazy JTS and S2 caches.

```java
public class WKBGeography extends Geography {
    private final byte[] wkbBytes;
    private volatile Geometry jtsGeometry;      // lazy, via JTS WKBReader
    private volatile Geography s2Geography;     // lazy, via S2Geography.WKBReader

    // Factory methods
    public static WKBGeography fromWKB(byte[] wkb, int srid);
    public static WKBGeography fromJTS(Geometry jts);
    public static WKBGeography fromS2Geography(Geography s2geog);

    // Lazy accessors (double-checked locking)
    public byte[] getWKBBytes();
    public Geometry getJTSGeometry();
    public Geography getS2Geography();

    // Geography abstract methods — delegate to lazy S2
    @Override public int dimension();
    @Override public int numShapes();
    @Override public S2Shape shape(int id);
    @Override public S2Region region();
}
```

**Implementation details:**
- Constructor: `super(GeographyKind.UNINITIALIZED)`, stores `wkbBytes` and `srid`
- `fromWKB(byte[], int)` — wraps raw bytes, zero parse cost
- `fromJTS(Geometry)` — uses `org.locationtech.jts.io.WKBWriter` to serialize JTS → bytes
- `fromS2Geography(Geography)` — uses existing `S2Geography.WKBWriter` (with `includeSRID=false`) to convert S2 → bytes
- `getJTSGeometry()` — lazy parse via `org.locationtech.jts.io.WKBReader` (standard JTS reader)
- `getS2Geography()` — lazy parse via `org.apache.sedona.common.S2Geography.WKBReader` (existing S2 reader)
- `dimension()`, `numShapes()`, `shape(id)`, `region()` — delegate to `getS2Geography()` (triggers S2 parse only on first call)
- `toString()` / `toText()` / `toEWKT()` — delegate to `getS2Geography()` methods (these rely on WKTWriter which operates on S2 Geography)

**Tasks:**
- [ ] Implement `WKBGeography` class
- [ ] Implement `fromWKB`, `fromJTS`, `fromS2Geography` factory methods
- [ ] Implement lazy `getJTSGeometry()` and `getS2Geography()` with double-checked locking
- [ ] Delegate abstract methods to lazy S2 cache

### 1.2 New Class: `GeographyWKBSerializer`

**File (new)**: `common/src/main/java/org/apache/sedona/common/S2Geography/GeographyWKBSerializer.java`

Replaces `GeographySerializer` as the default serializer with backward-compatible format discrimination.

**Format discrimination** (first byte):
```
0xFF         → WKB format (new): rest of buffer = EWKB with SRID
1-10         → S2-native format (legacy): reinterpret as GeographyKind byte
0 (UNINIT)   → never produced by either format in practice
```

**Serialize** (`Geography → byte[]`):
```java
public static byte[] serialize(Geography geog) {
    byte[] wkb;
    if (geog instanceof WKBGeography) {
        wkb = ((WKBGeography) geog).getWKBBytes();   // fast path
    } else {
        // slow path: S2 Geography → WKB via existing S2Geography.WKBWriter
        WKBWriter writer = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN, false);
        wkb = writer.write(geog);
    }
    // Prepend: [0xFF][4-byte SRID big-endian][WKB payload]
    byte[] result = new byte[1 + 4 + wkb.length];
    result[0] = (byte) 0xFF;
    int srid = geog.getSRID();
    result[1] = (byte) (srid >> 24);
    result[2] = (byte) (srid >> 16);
    result[3] = (byte) (srid >> 8);
    result[4] = (byte) srid;
    System.arraycopy(wkb, 0, result, 5, wkb.length);
    return result;
}
```

**Deserialize** (`byte[] → Geography`):
```java
public static Geography deserialize(byte[] buffer) {
    if ((buffer[0] & 0xFF) == 0xFF) {
        // New format: extract SRID and WKB
        int srid = ((buffer[1] & 0xFF) << 24) | ((buffer[2] & 0xFF) << 16)
                 | ((buffer[3] & 0xFF) << 8)  |  (buffer[4] & 0xFF);
        byte[] wkb = Arrays.copyOfRange(buffer, 5, buffer.length);
        return WKBGeography.fromWKB(wkb, srid);
    }
    // Legacy S2-native format
    return GeographySerializer.deserialize(buffer);
}
```

**Note**: SRID is stored separately (not in EWKB) because the WKB payload should be pure ISO WKB for GeoArrow compatibility. The `S2Geography.WKBWriter` is called with `includeSRID=false`.

**Tasks:**
- [ ] Implement `GeographyWKBSerializer` with `serialize` and `deserialize`
- [ ] Ensure backward compat: S2-native bytes (first byte 1-10) route to `GeographySerializer`
- [ ] Ensure forward compat: new WKB bytes (first byte 0xFF) produce `WKBGeography`

### 1.3 Update `GeographyUDT` (The Switch)

**File (modify)**: `spark/common/src/main/scala/org/apache/spark/sql/sedona_sql/UDT/GeographyUDT.scala`

```scala
// Change import: add GeographyWKBSerializer
// Change serialize/deserialize to use GeographyWKBSerializer
override def serialize(obj: Geography): Array[Byte] =
    GeographyWKBSerializer.serialize(obj)
override def deserialize(datum: Any): Geography =
    datum match { case value: Array[Byte] => GeographyWKBSerializer.deserialize(value) }
```

**Effect**: All new Geography data written as WKB. Existing S2-native data still readable via format byte check.

**Tasks:**
- [ ] Switch `GeographyUDT` to use `GeographyWKBSerializer`

### 1.4 Update Constructors to Return `WKBGeography`

**File (modify)**: `common/src/main/java/org/apache/sedona/common/geography/Constructors.java`

| Constructor | Current | New |
|-------------|---------|-----|
| `geogFromWKB(byte[])` | `WKBReader().read(wkb)` → S2 Geography | `WKBGeography.fromWKB(wkb, 0)` — **zero parse** |
| `geogFromWKB(byte[], int)` | `WKBReader().read(wkb)` + setSRID | `WKBGeography.fromWKB(wkb, srid)` — **zero parse** |
| `geogFromWKT(String, int)` | `WKTReader().read(wkt)` → S2 Geography | Parse WKT to JTS via `org.locationtech.jts.io.WKTReader` → `WKBGeography.fromJTS(jts)` |
| `geogFromEWKT(String)` | Delegates to geogFromWKT | Same delegation, returns WKBGeography |
| `geomToGeography(Geometry)` | Build S2 shapes manually | `WKBGeography.fromJTS(geom)` — **skip S2 build** |
| `geogToGeometry(Geography)` | S2→JTS via coordinate extraction | If WKBGeography → `getJTSGeometry()` (fast); else existing S2→JTS path |

**Tasks:**
- [ ] Update `geogFromWKB` to return `WKBGeography.fromWKB()`
- [ ] Update `geogFromWKT` to go through JTS intermediate → `WKBGeography.fromJTS()`
- [ ] Update `geomToGeography` to return `WKBGeography.fromJTS()`
- [ ] Add fast path in `geogToGeometry` for `WKBGeography`
- [ ] Keep existing S2-based `geomToGeography` as private helper (still needed by `fromS2Geography`)

### 1.5 Unit Tests

**File (new)**: `common/src/test/java/org/apache/sedona/common/S2Geography/WKBGeographyTest.java`

Tests following existing patterns in `WKBReaderTest.java` and `WKBWriterTest.java`:

```java
public class WKBGeographyTest {
    // 1) WKBGeography creation and lazy parsing
    @Test public void fromWKB_point_lazyParse();         // WKB bytes → WKBGeography, verify JTS is null until accessed
    @Test public void fromWKB_polygon_lazyParse();
    @Test public void fromJTS_point();                   // JTS Point → WKBGeography → getJTSGeometry() round-trip
    @Test public void fromJTS_linestring();
    @Test public void fromJTS_polygon();
    @Test public void fromJTS_multiPolygon();
    @Test public void fromJTS_collection();
    @Test public void fromS2Geography_point();           // S2 Geography → WKBGeography → getS2Geography() round-trip

    // 2) Lazy S2 delegation
    @Test public void dimension_triggersS2Parse();       // dimension() should work via lazy S2
    @Test public void numShapes_triggersS2Parse();
    @Test public void region_triggersS2Parse();
    @Test public void toString_works();                  // toString/toEWKT delegates to S2

    // 3) Serializer round-trip
    @Test public void serialize_deserialize_point();
    @Test public void serialize_deserialize_polygon_withSRID();
    @Test public void serialize_deserialize_collection();
    @Test public void serialize_deserialize_emptyCases();

    // 4) Backward compat: S2-native bytes → new deserializer
    @Test public void deserialize_legacyS2Native_point();
    @Test public void deserialize_legacyS2Native_polygon();

    // 5) SRID preservation
    @Test public void srid_preservedThroughRoundTrip();
    @Test public void srid_zero_default();

    // 6) Constructor integration
    @Test public void geogFromWKB_returnsWKBGeography();
    @Test public void geogFromWKT_returnsWKBGeography();
    @Test public void geomToGeography_returnsWKBGeography();
    @Test public void geogToGeometry_fastPath();         // WKBGeography → JTS without S2 parse
}
```

**Tasks:**
- [ ] Implement all unit tests above
- [ ] Verify 1e-12 precision tolerance for coordinate round-trips (matching `WKBWriterTest` patterns)
- [ ] Verify backward compat by creating S2-native bytes via `GeographySerializer.serialize()` and reading via `GeographyWKBSerializer.deserialize()`

### 1.6 Spark Integration Tests

**File (modify)**: `spark/common/src/test/scala/org/apache/sedona/sql/geography/ConstructorsTest.scala`

Add tests to verify the UDT switch works end-to-end through Spark SQL:

```scala
// New test cases to add:
test("WKB round-trip through Spark DataFrame") {
  // Create Geography via ST_GeogFromWKT → write to DataFrame → read back → verify
}
test("WKB serialization backward compat") {
  // Manually create S2-native serialized bytes → verify GeographyUDT.deserialize reads them
}
test("ST_GeogFromWKB returns WKBGeography type") {
  // Verify the returned object is instanceof WKBGeography
}
```

**File (modify)**: `spark/common/src/test/scala/org/apache/sedona/sql/geography/FunctionsTest.scala`

Add test to verify existing Geography functions (ST_Envelope, ST_AsEWKT) still work with WKBGeography:

```scala
test("ST_Envelope works with WKBGeography") { ... }
test("ST_AsEWKT works with WKBGeography") { ... }
```

**Tasks:**
- [ ] Add Spark SQL round-trip test through DataFrame write/read
- [ ] Add backward compatibility test with S2-native bytes
- [ ] Verify existing `ConstructorsTest` and `FunctionsTest` pass without changes (regression)

---

## Phase 3: WKB with Cached S2 Implementation (Option B)

**Chosen approach**: Store WKB as the primary representation. Lazily parse to JTS or S2 on demand and cache the result. This gives GeoArrow compatibility by default, zero-parse for serialization-only workloads, and fast metric operations via JTS + GeographicLib.

```
┌─────────────────────────────────────────────────────────────┐
│ Storage: WKB (GeoArrow compatible)                          │
│ Runtime: Lazy JTS + Lazy S2 cache per object                │
│   - First metric op (distance/area/length) triggers JTS     │
│   - First predicate op (intersects/contains) triggers S2    │
│   - Caches reused for subsequent ops on same object         │
└─────────────────────────────────────────────────────────────┘
```

### 3.1 New Class: `WKBGeography`

**File**: `common/src/main/java/org/apache/sedona/common/S2Geography/WKBGeography.java`

```java
public class WKBGeography extends Geography {
    // Primary storage — always present
    private final byte[] wkbBytes;

    // Lazy caches — populated on first access
    private volatile Geometry jtsGeometry;      // via JTS WKBReader
    private volatile Geography s2Geography;     // via S2Geography WKBReader

    // Fast accessors
    public byte[] getWKBBytes() { return wkbBytes; }           // zero cost
    public Geometry getJTSGeometry() { /* double-checked locking */ }
    public Geography getS2Geography() { /* double-checked locking */ }

    // Factory methods
    public static WKBGeography fromWKB(byte[] wkb, int srid);
    public static WKBGeography fromJTS(Geometry jts);           // JTS WKBWriter → bytes
    public static WKBGeography fromS2Geography(Geography s2);   // S2 WKBWriter → bytes

    // Geography abstract methods — delegate to lazy S2
    @Override public int dimension()        { return getS2Geography().dimension(); }
    @Override public int numShapes()        { return getS2Geography().numShapes(); }
    @Override public S2Shape shape(int id)  { return getS2Geography().shape(id); }
    @Override public S2Region region()      { return getS2Geography().region(); }
}
```

**Design decisions**:
- Extends `Geography` so it's a drop-in everywhere Geography is accepted
- Uses `GeographyKind.UNINITIALIZED` — actual kind determined by WKB content
- Thread-safe via `volatile` + double-checked locking (sufficient for Spark partition-level parallelism)
- S2 abstract methods (`dimension`, `numShapes`, `shape`, `region`) delegate to lazy S2 cache — only parsed when S2-specific operations are called

**Tasks:**
- [ ] Implement `WKBGeography` with lazy JTS and S2 caches
- [ ] Add `fromWKB`, `fromJTS`, `fromS2Geography` factory methods
- [ ] Delegate Geography abstract methods to lazy `getS2Geography()`
- [ ] Unit tests for lazy parsing behavior and thread safety

### 3.2 New Class: `GeographyWKBSerializer`

**File**: `common/src/main/java/org/apache/sedona/common/S2Geography/GeographyWKBSerializer.java`

**Format discrimination strategy** (backward compatible):
```
First byte = 0xFF  →  WKB format (new): payload = EWKB bytes with SRID
First byte = 1-10  →  S2-native format (legacy): reinterpret as GeographyKind
```
`0xFF` is safe because no `GeographyKind` has value 255.

**Serialize**:
```java
static byte[] serialize(Geography geog) {
    if (geog instanceof WKBGeography) {
        // Fast path: already have WKB bytes, just prepend version marker
        return concat(0xFF, ((WKBGeography) geog).getWKBBytes());
    }
    // Slow path: convert S2 Geography → WKB via existing WKBWriter
    byte[] wkb = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, true).write(geog);
    return concat(0xFF, wkb);
}
```

**Deserialize**:
```java
static Geography deserialize(byte[] buffer) {
    if ((buffer[0] & 0xFF) == 0xFF) {
        // New WKB format: strip version byte, parse EWKB
        byte[] ewkb = Arrays.copyOfRange(buffer, 1, buffer.length);
        return WKBGeography.fromWKB(ewkb, extractSRID(ewkb));
    }
    // Legacy S2-native format: delegate to existing deserializer
    return GeographySerializer.deserialize(buffer);
}
```

**Tasks:**
- [ ] Implement `GeographyWKBSerializer` with format discrimination
- [ ] Add backward compatibility for S2-native deserialization
- [ ] Round-trip tests: serialize → deserialize → verify equality
- [ ] Cross-format tests: read legacy S2-native data with new deserializer

### 3.3 Update `GeographyUDT` (The Switch)

**File**: `spark/common/src/main/scala/org/apache/spark/sql/sedona_sql/UDT/GeographyUDT.scala`

```scala
// Before:
override def serialize(obj: Geography): Array[Byte] =
    GeographySerializer.serialize(obj)
override def deserialize(datum: Any): Geography =
    datum match { case value: Array[Byte] => GeographySerializer.deserialize(value) }

// After:
override def serialize(obj: Geography): Array[Byte] =
    GeographyWKBSerializer.serialize(obj)
override def deserialize(datum: Any): Geography =
    datum match { case value: Array[Byte] => GeographyWKBSerializer.deserialize(value) }
```

This is the "flip the switch" moment:
- All **new** data is written as WKB (GeoArrow compatible)
- All **existing** S2-native data is still readable (backward compat via format byte)

**Tasks:**
- [ ] Switch UDT to use `GeographyWKBSerializer`
- [ ] Run existing Geography test suite to verify backward compatibility

### 3.4 Update Constructors to Return `WKBGeography`

**File**: `common/src/main/java/org/apache/sedona/common/geography/Constructors.java`

| Constructor | New behavior | Parse cost |
|-------------|-------------|------------|
| `geogFromWKB(byte[])` | Wrap bytes directly in `WKBGeography` | **Zero** |
| `geogFromWKT(String)` | WKT → JTS → JTS WKBWriter → `WKBGeography` | WKT parse only |
| `geomToGeography(Geometry)` | JTS WKBWriter → `WKBGeography` | WKB write only |
| `geogToGeometry(Geography)` | If WKBGeography → `getJTSGeometry()` (fast); else S2→JTS | Depends |

Key optimization: `geogFromWKB()` becomes near-zero-cost since we just store the byte array.

**Tasks:**
- [ ] Update `geogFromWKB` to return `WKBGeography` directly (no S2 parse)
- [ ] Update `geogFromWKT` to go through JTS intermediate
- [ ] Update `geomToGeography` to produce `WKBGeography`
- [ ] Add fast path in `geogToGeometry` for `WKBGeography` instances

### 3.5 Route Geography Functions Through Tiered Paths

**File**: `common/src/main/java/org/apache/sedona/common/geography/Functions.java`

**Helper** (converts Geography → JTS without S2 parse):
```java
private static Geometry toJTS(Geography g) {
    if (g instanceof WKBGeography) return ((WKBGeography) g).getJTSGeometry();
    return Constructors.geogToGeometry(g);  // fallback for raw S2 Geography
}
```

**New metric functions** (reuse existing `Spheroid.java` — no new geodesic code needed):
```java
public static double distance(Geography g1, Geography g2) {
    return Spheroid.distance(toJTS(g1), toJTS(g2));  // returns meters
}
public static double area(Geography g) {
    return Spheroid.area(toJTS(g));                   // returns m²
}
public static double length(Geography g) {
    return Spheroid.length(toJTS(g));                 // returns meters
}
```

**Existing Foundation** — these already work, no changes needed:
```java
Spheroid.distance(jtsGeom1, jtsGeom2);  // Geodesic.WGS84.Inverse() → meters
Spheroid.length(jtsGeom);                // PolygonArea for perimeter → meters
Spheroid.area(jtsGeom);                  // PolygonArea for area → m²
```

**Predicate functions** (`Predicates.java`) remain unchanged — they receive S2 objects via `getS2Geography()` which triggers lazy S2 parse only when needed.

**Operations by implementation path:**

| Path | Operations | Cost |
|------|------------|------|
| WKB only | ST_AsBinary, ST_AsEWKB, ST_SRID | Zero parse |
| JTS only | ST_AsText, ST_NumPoints, ST_GeometryType, ST_Envelope | WKB → JTS (lazy, cached) |
| JTS + Spheroid | ST_Distance, ST_Area, ST_Length, ST_Perimeter | WKB → JTS + geodesic formulas |
| S2 required | ST_Intersects, ST_Contains, ST_Buffer, Spatial Join predicates | WKB → S2 (lazy, cached) |

**Tasks:**
- [ ] Add `toJTS` helper in `Functions.java`
- [ ] Implement `distance`, `area`, `length` using `Spheroid` methods
- [ ] Register Spark SQL expressions (`ST_Distance`, `ST_Area`, `ST_Length` for Geography)
- [ ] Benchmark: Expect 5-10x speedup for metric operations vs S2 path

### 3.6 Join Path: Geography Column → JTS Geometry

**File**: `spark/common/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryBase.scala`

In `toSpatialRDD`, detect Geography column type and extract JTS:
```scala
val isGeographyColumn = shapeExpression.dataType.isInstanceOf[GeographyUDT]
val shape = if (isGeographyColumn) {
    val geog = GeographyWKBSerializer.deserialize(bytes).asInstanceOf[WKBGeography]
    geog.getJTSGeometry()
} else {
    GeometrySerializer.deserialize(bytes)
}
```

The existing `isGeography` flag in `KNNJoinExec` and `DistanceJoinExec` already selects Haversine/Spheroid distance metrics on JTS Geometry objects, so no further join changes are needed.

**Tasks:**
- [ ] Update `toSpatialRDD` to handle Geography columns
- [ ] Verify KNN and Distance joins work with WKBGeography-sourced JTS

### 3.7 Performance Targets

| Operation | Target (vs Geometry) |
|-----------|---------------------|
| Serialization (WKB round-trip) | < 1.2x slower |
| ST_Distance / ST_Area / ST_Length | < 1.5x slower |
| Point-in-polygon (S2 predicate) | < 2x slower |
| Spatial join (S2 predicate) | < 3x slower |

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

```
┌─────────────────────────────────────────────────────────────────┐
│  Level 0: WKB Bytes Only (no parsing)                          │
│  ST_AsBinary, ST_AsEWKB, ST_GeomFromWKB                         │
├─────────────────────────────────────────────────────────────────┤
│  Level 1: JTS Parse (WKB → JTS Geometry)                        │
│  ST_AsText, ST_NumPoints, ST_GeometryType, ST_Envelope,         │
│  ST_X, ST_Y, ST_StartPoint, ST_EndPoint, ST_Centroid (planar)   │
├─────────────────────────────────────────────────────────────────┤
│  Level 2: JTS + GeographicLib (geodesic formulas)               │
│  ST_Distance, ST_Length, ST_Area, ST_Perimeter,                 │
│  ST_Azimuth, ST_Project                                         │
├─────────────────────────────────────────────────────────────────┤
│  Level 3: S2 Required (spherical predicates/operations)         │
│  ST_Intersects, ST_Contains, ST_Within, ST_Covers,              │
│  ST_DWithin, ST_Buffer, ST_Intersection, ST_Union,              │
│  Spatial Join (KNN, Range, etc.)                                │
└─────────────────────────────────────────────────────────────────┘
```

**Goal**: Most queries use Level 0-2 operations. S2 parsing only for Level 3.

**Implication for Sedona:**
- Sedona should support **lazy S2 parsing** - only construct S2 objects when JVM execution is required
- For Comet users: Geography columns are just WKB byte arrays passed to native
- For non-Comet users: S2 parsing happens on-demand per operation

**Long-term direction:**
- As SedonaDB coverage grows, fewer ops need JVM fallback
- S2 parsing in Sedona becomes a compatibility/fallback layer
- Eventually may deprecate JVM Geography ops entirely (breaking change)

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
├── Spark predicate expression wiring (ST_Equals/Intersects/Contains for Geography)
├── Range join support (spatial index)
└── GeoArrow metadata for Comet/SedonaDB interop
```

### Key Observation

**All planned functions are now implemented!**

- 13 functions use JTS path only (no S2 parse): constructors, converters, metric ops
- 6 functions use S2 via lazy cache: ST_MaxDistance, ST_ClosestPoint, ST_MinimumClearanceLine, ST_Equals, ST_Intersects, ST_Contains
- Remaining: Range join support (S2 cell indexing), Spark predicate expression wiring

---

## References

- [GeoArrow Specification](https://geoarrow.org/extension-types)
- [SedonaDB Geography Implementation](https://github.com/apache/sedona-db)
- [PR #2307 - Geography Benchmarks](https://github.com/apache/sedona/pull/2307)
- [S2Geometry Library](https://s2geometry.io/)
