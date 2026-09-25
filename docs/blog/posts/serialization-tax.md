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

Sedona keeps a geometry or a raster in a row as bytes. A spatial function wants an object: a geometry it can walk, a raster with its pixels. So every function decodes its input, computes, and encodes its output, and a query with three functions inside each other pays three times. Since Sedona 1.4.0 a nested function hands its object straight to the function around it, and the row is decoded once. Below, the trick, and what it is worth: from 3% to 80% of the query time, measured on 3.82 million Washington State buildings, 78,000 county polygons and 256 rasters.

![The title over two rows: on the naive path a building footprint and a raster pass through three functions with a mosaic of their bytes between every step, on Sedona's path the same objects pass straight from function to function](serialization-tax-cover.png)

<!-- more -->

## Rows are bytes, functions want objects

A row in Sedona is a flat buffer of bytes. A geometry column holds Sedona's compact binary encoding of the shape. A raster column holds the raster's metadata and all of its pixels. A function such as `ST_Buffer` cannot work on bytes, so it decodes them into a geometry object, buffers it, and encodes the result back into bytes for the row. `RS_Resample` does the same with a raster, and a raster's bytes are its pixels.

One function, one round trip. Now nest three:

```sql
SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), 10)))
FROM buildings
```

Evaluated the naive way, `ST_Transform` decodes the column and encodes its result. `ST_Buffer` decodes that and encodes again. `ST_Area` decodes a third time. Three decodes and two encodes for one number, and the two encodes in the middle exist only to be undone a moment later. A raster chain of the same shape encodes and decodes every pixel twice for nothing.

## The trick: hand over the object

Sedona's fix is a five-line trait, the Scala word for an interface:

```scala
trait SerdeAware {
  def evalWithoutSerialization(input: InternalRow): Any
}
```

Every Sedona function implements it, because the base class all functions share does. When a function fetches an argument, it first checks whether the child expression is a Sedona expression. If it is, it asks for the object and skips the bytes:

```scala
def toGeometry(input: InternalRow): Geometry = {
  if (inputExpression.isInstanceOf[SerdeAware]) {
    inputExpression.asInstanceOf[SerdeAware].evalWithoutSerialization(input).asInstanceOf[Geometry]
  } else {
    GeometrySerializer.deserialize(inputExpression.eval(input).asInstanceOf[Array[Byte]])
  }
}
```

The same check exists for rasters, geographies, and 2D and 3D boxes. `RS_MapAlgebra(RS_Resample(raster, ...), ...)` passes the resampled raster along without writing a pixel. In the query above, only the outermost result is ever encoded, and the column is decoded once. The test in the repository pins it down on a three-function tree: zero decodes and at most one encode. The change landed in March 2023 as [SEDONA-231](https://github.com/apache/sedona/pull/792) and shipped in Sedona 1.4.0, so every release since has had it.

## Measure it

To see the tax, bring it back. Any expression that is not Sedona's, placed between two Sedona functions, makes the inner one encode and the outer one decode. `IF(length(id) >= 0, x, NULL)` is such a barrier: always true, never folded away by the optimizer, and it changes no value. Each query plan was checked to confirm the barriers survived.

??? example "The three queries"

    ```sql
    -- nested: one decode, no intermediate encode
    SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), 10))) FROM b;

    -- barriers between the functions: the naive path
    SELECT SUM(ST_Area(IF(length(id) >= 0,
                  ST_Buffer(IF(length(id) >= 0,
                    ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), NULL), 10), NULL)))
    FROM b;

    -- one barrier on top: the cost of the barrier itself
    SELECT SUM(IF(length(id) >= 0,
                  ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32610'), 10)), NULL))
    FROM b;
    ```

![Horizontal bars for five expressions, each with a blue nested bar and an orange barrier bar: 17.5 against 18.0 seconds for three heavy functions on buildings, 27.9 against 33.0 for five, 4.5 against 4.8 for three raster functions, 1.2 against 1.6 for four cheap functions on county polygons, and 0.7 against 1.2 for four cheap functions on buildings](serialization-tax-chart.svg)

The tax depends on how much work the functions do per byte. Three heavy functions on the buildings, a coordinate transform, a buffer and an area, take 17.5 s nested and 18.0 s with barriers, a 3% difference inside the noise between runs. A building has 8 vertices, and buffering it costs far more than encoding it. Five heavy functions take 27.9 s against 33.0 s, 19%, because the shapes in the middle of that chain are buffered and larger. Three raster functions on 256 rasters of 1024 by 1024 take 4.5 s against 4.8 s, 8%. Cheap functions show the other end. Four functions that only rewrite coordinates, `ST_NPoints(ST_Translate(ST_Reverse(ST_FlipCoordinates(geometry))))`, take 1.24 s against 1.63 s on 78,000 county polygons of 1,722 vertices each: 31%. On the buildings they take 0.69 s against 1.24 s: 80%. When the function is cheap, the round trip is most of the bill, and the hand-over removes it.

## Where the chain breaks

The hand-over works only between Sedona functions that sit directly inside each other in one expression. Anything else in between is a barrier and brings the bytes back:

- a function that is not Sedona's, or a `CASE`, between two spatial functions;
- a Python UDF, which moves the bytes to a Python worker and back;
- an intermediate column written to a table, cached, or sent through a shuffle.

The DataFrame API is safe: three `withColumn` calls for the transform, the buffer and the area collapse into one projection, and the plan shows the same nested tree as the SQL, `ST_Area` directly over `ST_Buffer` directly over `ST_Transform`.

## The point

Nest the geometry and raster functions, and Sedona passes the objects along. The row is decoded once, the intermediate shapes and rasters never become bytes, and the saving runs from a few percent on heavy functions to most of the query on cheap ones. It has worked this way since Sedona 1.4.0, in every function, with nothing to switch on.

*Buildings from Overture Maps. Measurements on Apache Sedona 1.9.1 on Spark 3.5.4, one machine with 8 cores.*
