---
date:
  created: 2026-09-25
links:
  - "SEDONA-231: Redundant Serde Elimination": https://github.com/apache/sedona/pull/792
  - Map algebra in Sedona: https://sedona.apache.org/latest/api/sql/Raster-Map-Algebra-Operators/RS_MapAlgebra/
  - Sedona Python setup: https://sedona.apache.org/latest/setup/install-python/
authors:
  - jia
title: "The Serialization Tax, and How Sedona Stopped Paying It"
slug: the-serialization-tax-and-how-sedona-stopped-paying-it
---

# The Serialization Tax, and How Sedona Stopped Paying It

Transform a building footprint to a projected coordinate system, buffer it by ten meters, then calculate its area. Three spatial operations, written as one SQL expression. There is also work hidden between those operations: converting each intermediate geometry to bytes, only for the next function to turn it back into an object.

Apache Sedona's Spark SQL implementation avoids those intermediate conversions by passing objects directly between functions that support it. The mechanism is small, but its effect depends on the workload. The measurements below show where it matters, where other work dominates, and what can interrupt it.

![Two evaluation paths for geometry and raster functions: the naive path encodes and decodes intermediate results; Sedona passes the objects directly to the next function](serialization-tax-cover.png)

<!-- more -->

## The work between the functions

In Spark, Sedona stores geometry values in rows using a binary representation. Spatial functions such as `ST_Buffer` operate on geometry objects. Reading a stored geometry therefore requires deserialization: decoding the bytes into an object. Returning a geometry to a row requires serialization: encoding the object as bytes again. For an in-memory raster, that serialized representation includes metadata and pixel values.

Consider the building query:

```sql
SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), 10)))
FROM buildings
```

If each function uses the row representation to return its result, `ST_Transform` decodes the input and encodes the transformed geometry. `ST_Buffer` decodes that geometry and encodes the buffer. `ST_Area` decodes it once more to calculate a number.

That is three geometry decodes and two encodes per building. Only the first decode is needed to read the input. The two intermediate round trips do no spatial work; they are the serialization tax. In a raster chain, the same pattern can require repeatedly encoding and decoding large pixel arrays.

## Pass the object to the next function

Sedona gives expressions a way to return their result before it is serialized. The interface is a Scala trait called `SerdeAware`:

```scala
trait SerdeAware {
  def evalWithoutSerialization(input: InternalRow): Any
}
```

Many Sedona scalar functions inherit this interface through `InferredExpression`. When a function reads a geometry argument, it checks whether the child expression implements `SerdeAware`. If so, it requests the geometry object directly. Otherwise, it evaluates the child normally and decodes the result:

```scala
def toGeometry(input: InternalRow): Geometry = {
  if (inputExpression.isInstanceOf[SerdeAware]) {
    inputExpression.asInstanceOf[SerdeAware].evalWithoutSerialization(input).asInstanceOf[Geometry]
  } else {
    inputExpression.eval(input).asInstanceOf[Array[Byte]] match {
      case binary: Array[Byte] => GeometrySerializer.deserialize(binary)
      case _ => null
    }
  }
}
```

In the building query, `ST_Transform` decodes the stored geometry once. It passes its result to `ST_Buffer`, which passes its result to `ST_Area`. There are no intermediate geometry encodes or decodes, and the final result is a number. If the outermost function returns a geometry instead, Sedona serializes that final geometry for the row.

The same mechanism supports raster, geography, and box arguments. For example, `RS_MapAlgebra(RS_Resample(raster, ...), ...)` receives the resampled raster without an intermediate serialization of its pixels. This does not eliminate the pixel processing performed by either function.

The geometry optimization shipped in Sedona 1.4.0 through [SEDONA-231](https://github.com/apache/sedona/pull/792); raster support followed in 1.4.1 through [SEDONA-270](https://github.com/apache/sedona/pull/810). It applies automatically where the participating expressions support this path.

## Put the round trips back and measure

To compare the two evaluation paths, we ran the same spatial operations with and without expressions inserted between them. An `IF` expression does not implement `SerdeAware`, so it makes the inner function serialize its result and the outer function deserialize it.

For these building records, `IF(length(id) >= 0, x, NULL)` returns `x`: their IDs are non-null strings. The condition remained in the optimized plans used for the measurements, and the aggregate results matched. That matters: a condition Spark optimizes away would no longer force the round trip.

??? example "The nested query, the barriers, and a control"

    ```sql
    -- nested: one decode, no intermediate encode
    SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), 10)))
    FROM buildings;

    -- barriers between spatial functions: force intermediate serialization
    SELECT SUM(ST_Area(IF(length(id) >= 0,
                  ST_Buffer(IF(length(id) >= 0,
                    ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), NULL), 10), NULL)))
    FROM buildings;

    -- control: add a condition around the numeric result, preserving object passing
    SELECT SUM(IF(length(id) >= 0,
                  ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), 10)), NULL))
    FROM buildings;
    ```

The tests used Sedona 1.9.1 on Spark 3.5.4, with `local[8]`, 14 GB of driver memory, and inputs repartitioned to 64 partitions and cached before timing. The datasets were 3.82 million buildings in Washington State, 39 Washington county geometries repeated 2,000 times to make 78,000 rows, and 256 generated rasters of 1024 by 1024 pixels. The buildings and county boundaries came from Overture Maps release `2026-08-19.0`. The chart reports the fastest of three runs for each query.

![Query runtimes with direct nesting and with intermediate barriers: 17.48 and 17.96 seconds for three geometry functions; 27.87 and 33.04 for five; 4.47 and 4.81 for raster functions; 1.24 and 1.63 for coordinate operations on counties; 0.69 and 1.24 for the same operations on buildings](serialization-tax-chart.svg)

*Percentages show the extra runtime of the barrier query relative to the nested query: `(barrier / nested - 1) × 100`. Function chains in the chart omit arguments for readability.*

The largest relative difference came from inexpensive coordinate operations. Flipping coordinates, reversing their order, translating them, and counting points took **0.69 seconds with direct nesting and 1.24 seconds with barriers** on the buildings. The barrier version took about 80% longer; equivalently, the nested version used about 44% less time. Those percentages use different baselines, so 80% is not the share of query time saved.

The executable expression for that chain is:

```sql
ST_NPoints(ST_Translate(ST_Reverse(ST_FlipCoordinates(geometry)), 1.0, 1.0))
```

On the replicated county geometries, the same chain took 1.24 seconds nested and 1.63 seconds with barriers, a 31% increase. These inputs averaged about 1,722 vertices per geometry, compared with about eight for the buildings.

When the spatial work was more expensive, the relative difference was smaller. The transform-buffer-area query took 17.48 seconds nested and 17.96 seconds with barriers, a 3% gap smaller than the observed variation between runs. A five-function chain that also simplified the transformed geometry and took the buffered result's convex hull took 27.87 versus 33.04 seconds, a 19% increase. Resampling the rasters, applying map algebra, and calculating summary statistics took 4.47 versus 4.81 seconds, an 8% increase.

These are end-to-end comparisons of two query plans. The barriers also add condition evaluation and can affect execution beyond serialization. The control query above took 20.09 seconds despite preserving direct object passing, and the equivalent raster control took 4.89 seconds. With only three runs per query, these results do not isolate serialization cost or establish a reliable gain for the small differences. They illustrate why avoiding intermediate conversions can matter most when the spatial operations themselves are inexpensive.

## Where the chain breaks

Direct object passing depends on the expression tree Spark actually executes. A parent must use the object-aware argument path, and its child must implement `SerdeAware`. Serialization returns at boundaries such as:

- an intervening expression such as `IF` or `CASE` that remains after optimization and does not support object passing;
- a Python UDF, which crosses the JVM/Python boundary;
- a spatial intermediate result materialized in a table, a cache, or a shuffle.

Naming an intermediate column does not necessarily create such a boundary. In the tested DataFrame query, three `withColumn` calls followed by an aggregation collapsed into one projection: `ST_Area` directly over `ST_Buffer` directly over `ST_Transform`. Spark can combine those steps, so writing the query in several DataFrame calls can preserve the same optimization as nested SQL. Inspect the optimized plan when it matters; separate calls alone do not tell you whether an intermediate result is materialized.

## Keep intermediate results inside the expression

Compose compatible spatial functions in one expression and Sedona can keep intermediate geometries and rasters as objects. There is no setting to enable. The useful distinction is whether Spark can pass each result directly to the next function or must serialize it at a boundary. Avoid unnecessary boundaries, especially in chains of inexpensive operations where conversion can be a substantial part of the work.
