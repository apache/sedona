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

# 在 Sedona 上的 Geopandas

本指南列出了在 Apache Sedona 的 GeoPandas 组件上以开发者身份贡献变更时需要注意的若干重要事项。请再次注意：**本指南面向贡献者**；面向用户的官方文档另有侧重。

**总体思路**：本组件构建在 PySpark Pandas API 之上。`GeoDataFrame` 与 `GeoSeries` 类分别继承自 pyspark pandas 的 `ps.DataFrame` 与 `ps.Series`。在可能的情况下，应尽量复用 pyspark pandas 已有的实现并在其基础上扩展（即尽量通过 `super()` 调用，而不是把现有逻辑复制过来）。代码组织结构与 [Geopandas 仓库](https://github.com/geopandas/geopandas) 类似。

**惰性求值**：Spark 使用惰性求值。Spark 的分布式与惰性特性有时会让我们无法完全照搬原版 GeoPandas 的实现方式。例如 GeoPandas 在多处对非法 CRS 做了校验（如 `GeoSeries.__init__()`、`set_crs()` 等）。Sedona 当前获取 `crs` 的实现相比 GeoPandas 较为昂贵，因为它需要跑一次立即求值的 `ST_SRID()` 查询。如果在每次 `GeoSeries` 初始化时都立即查询 CRS，那么所有函数调用（如 `.area()` 等）也会变成立即求值，性能会明显下降，用户体验变差。

**保留顺序**：由于 Spark 使用分布式数据，在多次操作之间保留顺序需要付出额外的时间和精力。某些操作维持顺序意义不大，此时跳过额外的 sort 是合理的，否则会带来不必要的性能开销。文档中应说明这种行为。例如 `sjoin` 在执行连接后并不会保持传统 pandas DataFrame 的顺序，这与传统 PySpark Pandas 保持一致。用户随时可以通过 `sort_index()` 等额外函数后排序，但默认实现应避免不必要的排序。

**约定**：Sedona Geopandas 包的常用别名是 `sgpd`，与 geopandas 的别名 `gpd` 类似，仅多了一个 `s` 前缀。相邻包的常用别名见下：

```python
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
import sedona.spark.geopandas as sgpd
```

**转换方法**：Sedona 的 Geopandas 实现提供了在多种 DataFrame 之间互转的便捷方法。下列方法对 `GeoDataFrame` 与 `GeoSeries` 都可用：

- `to_geopandas()`：Sedona Geo(DataFrame/Series) 转为 Geopandas
- `to_geoframe()`：Sedona GeoSeries 转为 Sedona GeoDataFrame
- `to_spark_pandas()`：Sedona Geo(DataFrame/Series) 转为 Pandas on PySpark
- `to_spark()`（继承）：Sedona GeoDataFrame 转为 Spark DataFrame
- `to_frame()`（继承）：Sedona GeoSeries 转为 PySpark Pandas DataFrame

**GeoSeries 函数**：Geopandas 中的几何操作大多是 GeoSeries 函数，但也可以从 `GeoDataFrame` 对象上调用，作用在其活动几何列（active geometry column）上。我们在 `GeoSeries` 类中实现这些函数；同时在 `base.py` 中加上 `_delegate_to_geometry_column()` 调用，让 `GeoDataFrame` 也可以在自己的活动几何列上执行该函数。我们把函数的 docstring 写在 `base.py` 而不是 `GeoSeries`，这样 `GeoDataFrame` 与 `GeoSeries` 都能继承同一份 docstring（避免重复）。

**查看查询计划**：由于这些 DataFrame 抽象建立在 Spark 之上，可以通过 `.spark.explain()` 方法获取一次操作的查询计划。

示例：

```python
geoseries = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])])
# PySpark pandas Series 目前没有 spark.explain() 方法，可先转为 DataFrame 再调用
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

## 推荐阅读

- [PySpark Pandas Best Practices](https://spark.apache.org/docs/latest/api/python/tutorial/pandas_on_spark/best_practices.html) —— 其中提到了一些值得注意的细节，比如为什么不支持 `__iter__()`。
- [Geopandas 用户指南](https://geopandas.org/en/stable/docs/user_guide/data_structures.html) —— 特别有助于理解 GeoDataFrame 中 “活动几何列（active geometry column）” 的概念。

## 其他参考

- [Pandas API on Spark 公开 API 页面](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html)
- [Geopandas API 页面](https://geopandas.org/en/stable/docs/reference.html)
