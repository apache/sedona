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

# Geopandas on Sedona

This guide outlines a few important considerations when contributing changes to the GeoPandas component of Apache Sedona as a developer. Again, **this guide is targeted towards contributors**; the official documentation is more tailored towards users.

**General Approach**: This component is built on top of the PySpark Pandas API. The `GeoDataFrame` and `GeoSeries` classes both inherit from pyspark pandas' `ps.DataFrame` and `ps.Series` classes, respectively. When possible, it is generally preferred to leverage pyspark pandas' implementation of a function and extending it from there (i.e. find a way to leverage `super()` rather than copying over parts of logic). The code structure resembles the structure of the [Geopandas repo](https://github.com/geopandas/geopandas).

**Lazy Evaluation**: Spark uses lazy evaluation. Spark's distributed and lazy nature occasionally comes in the way of implementing functionality in the same way the original GeoPandas library does so. For example, GeoPandas has many checks for invalid crs in many places (e.g `GeoSeries.__init__()`, `set_crs()`). Sedona's implementation for getting the `crs` currently is expensive compared to GeoPandas because it requires us to run an eager `ST_SRID()` query. If we eagerly query for the crs in every initialization of `GeoSeries`, all of our function calls (e.g `.area()`, etc) would also become eager and would incur a noticeable slowdown, resulting in a poor user experience.

**Maintaining Order**: Because Spark uses distributed data, maintaining the order of data across operations takes extra time and effort. Maintaining order for some operations is not very meaningful. In those cases, it's reasonable to skip the post-sorting to avoid an unnecessary performance hit. Documentation should document this behavior. For example, `sjoin` currently does not maintain the traditional pandas dataframe order after performing the join. This follows the same convention as traditional PySpark Pandas. The user can always post-sort using a separate function such as `sort_index()`, but we should avoid sorting unnecessarily by default.

**Conventions**: The conventional shorthand for the Sedona Geopandas package is `sgpd`. Notice it's the same as the geopandas shorthand (`gpd`), except prefixed with an 's'. The conventional short hands for adjacent packages are shown below also.

```python
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
import sedona.spark.geopandas as sgpd
```

**Conversion Methods**: Sedona's Implementation of Geopandas provides useful methods to convert to and from other dataframes using the following methods. These apply to both `GeoDataFrame` and `GeoSeries`:

- `to_geopandas()`: Sedona Geo(DataFrame/Series) to Geopandas
- `to_geoframe()`: Sedona GeoSeries to Sedona GeoDataFrame
- `to_spark_pandas()`: Sedona Geo(DataFrame/Series) to Pandas on PySpark
- `to_spark()` (inherited): Sedona GeoDataFrame to Spark DataFrame
- `to_frame()` (inherited): Sedona GeoSeries to PySpark Pandas DataFrame

**GeoSeries functions**: Most geometry manipulation operations in Geopandas are considered GeoSeries functions. However, we can call them from a `GeoDataFrame` object as well to execute on its active geometry column. We implement the functions in the `GeoSeries` class. However in `base.py`, we add a `_delegate_to_geometry_column()` call to allow the `GeoDataFrame` to also execute the function on its active geometry column. We also specify the docstring for the function here instead of `GeoSeries`, so that both `GeoDataFrame` and `GeoSeries` will inherit the shared docstring (avoiding duplicated docstrings).

**Explain Query Plans**: Because these dataframe abstractions are built on Spark, we can retrieve the query plan for an operation for a Dataframe by using the `.spark.explain()` method.

Example:

```python
geoseries = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])])
# Currently PySpark pandas Series does not have the spark.explain() method, so a workaround is to convert it to a dataframe first
print(geoseries.area.to_frame().spark.explain(extended=True))
```

```
== Parsed Logical Plan ==
Project [__index_level_0__#19L, 0#27 AS None#31]
+- Project [ **org.apache.spark.sql.sedona_sql.expressions.ST_Area**   AS 0#27, __index_level_0__#19L, __natural_order__#23L]
   +- Project [__index_level_0__#19L, 0#20, monotonically_increasing_id() AS __natural_order__#23L]
      +- LogicalRDD [__index_level_0__#19L, 0#20], false

== Analyzed Logical Plan ==
...

== Optimized Logical Plan ==
...

== Physical Plan ==
Project [__index_level_0__#19L,  **org.apache.spark.sql.sedona_sql.expressions.ST_Area**   AS None#31]
+- *(1) Scan ExistingRDD[__index_level_0__#19L,0#20]
```

## Suggested Short Readings:

- [PySpark Pandas Best Practices](https://spark.apache.org/docs/latest/api/python/tutorial/pandas_on_spark/best_practices.html) - This mentions other useful notes to consider such as why `__iter__()` is not supported
- [Geopandas User Guide](https://geopandas.org/en/stable/docs/user_guide/data_structures.html) - Specifically it's useful to understand the GeoDataFrame's "active geometry column"

## Other References

- [Public API Pages for Pandas API on Spark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html)
- [Geopandas API Page](https://geopandas.org/en/stable/docs/reference.html)
