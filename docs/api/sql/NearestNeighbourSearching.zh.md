
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

Sedona 通过提供地理空间 k 近邻（kNN）连接方法来支持对地理空间数据进行最近邻搜索。该方法基于地理临近性识别给定空间点或区域的 k 个最近邻，通常使用空间坐标以及合适的距离度量（如欧氏距离或大圆距离）。

## ST_KNN

简介：用于在空间数据集中查找一个点或区域的 k 个最近邻的连接操作。

格式：`ST_KNN(R: Table, S: Table, k: Integer, use_spheroid: Boolean)`

其中 R 是查询侧表，S 是对象侧表，K 是邻居数量。use_spheroid 是一个布尔值，决定是否使用椭球距离。

查询侧表包含用于在对象侧表中查找 k 近邻的几何对象。

当查询数据或对象数据中存在非点几何（其他几何类型）时，会取每个几何对象的质心。

当距离上出现并列时，只有在下面的 sedona 配置被设置为 true 时，结果才会包含所有并列的几何对象：

**关于内连接的说明：**

- `ST_KNN` 连接仅支持 left inner join。
- 它只返回那些至少存在一个在 k 近邻范围内的匹配邻居的对。
- 如果某个查询点没有有效邻居（例如 k 设得太大），它会被从结果中排除。

```
spark.sedona.join.knn.includeTieBreakers=true
```

### Geography 上的 ST_KNN

格式：`ST_KNN(R: Geography, S: Geography, k: Integer)`

起始版本：`v2.0.0`

`ST_KNN` 也接受 [Geography](geography/Geography-Functions.md) 列。与上文相同，R 是查询侧，S 是对象侧。每一条查询行会与距离它最近的 `k` 条对象行连接。

邻居按 [ST_Distance](geography/Geography-Functions/ST_Distance.md) 对两个 geography 返回的距离排序，即两者在球面上的最短距离，单位为米。由此带来三点不同：

- 没有 `use_spheroid` 参数。Geography 始终在球面上度量，因此只有三参数形式接受 geography。
- 线和面不会被替换为质心。到一个面的距离是到该面上最近点的距离；当查询对象位于面内时，距离为零。
- 跨越反子午线（±180° 经线）以及极点附近的邻居都能被正确找到。

两个参数都必须是 geography。如果要把 geography 与 geometry 连接，请先用 [ST_GeomToGeography](geography/Geography-Constructors/ST_GeomToGeography.md) 转换 geometry。

Geography 上的 `ST_KNN` 连接以广播连接的方式执行，因此其中一侧必须能放入内存。两侧都可以被广播。请给较小的一侧加上广播提示，或者把 `sedona.join.autoBroadcastJoinThreshold` 设置得足够大，让 Sedona 自动广播它。如果两侧都没有被广播，查询规划会报错。仅支持内连接。

本页的其他规则同样适用于 geography：NULL 和空的 geography 会被跳过；只有当 `spark.sedona.join.knn.includeTieBreakers` 为 `true` 时才返回并列的结果；`ON` 中的其他谓词作用于 `ST_KNN` 已选出的邻居。

SQL 示例：

```sql
SELECT /*+ BROADCAST(a) */
    s.name AS station,
    a.name AS airport,
    ROUND(ST_Distance(s.geog, a.geog) / 1000) AS km
FROM stations AS s
JOIN airports AS a
ON ST_KNN(s.geog, a.geog, 1)
ORDER BY station
```

使用下面两张表：

```
stations                              airports
name          geog                    name    geog
Suva          POINT (178.44 -18.14)   Apia    POINT (-171.76 -13.83)
Longyearbyen  POINT (15.63 78.22)     Noumea  POINT (166.46 -22.27)
                                      Alert   POINT (-62.35 82.50)
                                      Tromso  POINT (18.96 69.65)
```

输出：

```
+------------+-------+------+
|station     |airport|km    |
+------------+-------+------+
|Longyearbyen|Tromso |958.0 |
|Suva        |Apia   |1152.0|
+------------+-------+------+
```

Apia 与 Suva 分别位于反子午线的两侧。按经纬度的度数看，它比 Noumea 远得多；但在球面上，它才是两者中较近的一个。

### 谓词下推注意事项：

在 ST_KNN 之后对结果 DataFrame 施加的过滤条件，部分可能会被下推到 kNN 连接的对象侧。这意味着这些过滤会在 kNN 连接执行之前应用到对象侧的读取阶段。如果你希望过滤在 kNN 连接之后再生效，请先将 kNN 连接的结果物化，然后再应用过滤条件。

例如，可以使用以下方式：

Scala 示例：

```
val knnResult = knnJoinDF.cache()
val filteredResult = knnResult.filter(condition)
```

SQL 示例：

```
CREATE OR REPLACE TEMP VIEW knnResult AS
SELECT * FROM (
  -- Your KNN join SQL here
) AS knnView;
CACHE TABLE knnResult;
SELECT * FROM knnResult WHERE condition;
```

### 优化屏障

使用 `barrier` 函数可以阻止谓词下推，并在复杂的空间连接中控制谓词的求值顺序。该函数通过在运行时对布尔表达式求值来创建一个优化屏障。

`barrier` 函数接收一个作为字符串的布尔表达式，之后是若干对变量名及其取值，这些取值会在该表达式中被替换：

```sql
barrier(expression, var_name1, var_value1, var_name2, var_value2, ...)
```

过滤条件相对于 KNN 连接的放置位置会改变查询的语义：

- **在 KNN 之前过滤**：先对数据进行过滤，再在过滤后的子集上查找 K 个最近邻。回答的是“评分高的餐厅中，最近的 K 家是哪几家？”
- **在 KNN 之后过滤**：先在全部数据中查找 K 个最近邻，再对结果进行过滤。回答的是“最近的 K 家餐厅中，哪几家是高评分的？”

### 示例

查找距离每家豪华酒店最近的 3 家高评分餐厅，并确保 KNN 连接先完成，再进行过滤。

```sql
SELECT
    h.name AS hotel,
    r.name AS restaurant,
    r.rating
FROM hotels AS h
INNER JOIN restaurants AS r
ON ST_KNN(h.geometry, r.geometry, 3, false)
WHERE barrier('rating > 4.0 AND stars >= 4',
              'rating', r.rating,
              'stars', h.stars)
```

借助 barrier 函数，这条查询会先为每家酒店找到 3 家最近的餐厅（不考虑评分），随后再过滤，仅保留餐厅评分大于 4.0 且酒店星级不低于 4 的配对。如果不使用 barrier，优化器可能会将过滤下推，将查询改写为先过滤出高评分餐厅与豪华酒店，再在这些过滤后的子集中找最近的 3 个。

### 在 ST_KNN 连接中处理 SQL 定义的表：

在 Sedona 中，如果使用硬编码的 SQL select 语句创建 DataFrame，并随后在 `ST_KNN` 连接中使用它们，Sedona 可能会以绕过 kNN 连接逻辑的方式来优化查询。具体地说，如果你像下面这样用硬编码 SQL 创建 DataFrame：

```scala
val df1 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom1")
val df2 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom2")

val df = df1.join(df2, expr("ST_KNN(geom1, geom2, 1)"))
```

Sedona 可能会把这次连接优化为类似下面的形式：

```sql
SELECT ST_KNN(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0), 1)
```

结果是，ST_KNN 函数被当作用户自定义函数（UDF）处理，而非一个连接操作，从而阻止 Sedona 走 kNN 连接的执行路径。与典型的 UDF 不同，ST_KNN 函数会跨 DataFrame 作用在多行上，而不是仅作用在单行上。当出现这种情况时，查询会以 UnsupportedOperationException 失败，提示不支持该 KNN 谓词。

解决方法：

为防止 Spark 的优化绕过 kNN 连接逻辑，必须先将由硬编码 SQL select 创建的 DataFrame 物化，再执行连接。可以通过缓存 DataFrame 告诉 Spark 不要进行这种不希望的优化：

```scala
val df1 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom1").cache()
val df2 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom2").cache()

val df = df1.join(df2, expr("ST_KNN(geom1, geom2, 1)"))
```

通过 `.cache()` 物化 DataFrame 后，Spark 逻辑计划中会走正确的 kNN 连接路径，避免将 ST_KNN 当成普通 UDF 处理的优化。

### SQL 示例

假设我们有两张表 `QUERIES` 与 `OBJECTS`，数据如下：

QUERIES 表：

```
ID  GEOMETRY            NAME
1   POINT(1 1)	        station1
2   POINT(10 10)	    station2
3   POINT(-0.5 -0.5)	station3
```

OBJECTS 表：

```
ID  GEOMETRY            NAME
1	POINT(11 5)         bank1
2	POINT(12 1)         bank2
3	POINT(-1 -1)        bank3
4	POINT(-3 5)         bank4
5	POINT(9 8)          bank5
6	POINT(4 3)          bank6
7	POINT(-4 -5)        bank7
8	POINT(4 -2)         bank8
9	POINT(-3 1)         bank9
10	POINT(-7 3)         bank10
11	POINT(11 5)         bank11
12	POINT(12 1)         bank12
13	POINT(-1 -1)        bank13
14	POINT(-3 5)         bank14
15	POINT(9 8)          bank15
16	POINT(4 3)          bank16
17	POINT(-4 -5)        bank17
18	POINT(4 -2)         bank18
19	POINT(-3 1)         bank19
20	POINT(-7 3)         bank20
```

```sql
SELECT
    QUERIES.ID AS QUERY_ID,
    QUERIES.GEOMETRY AS QUERIES_GEOM,
    OBJECTS.GEOMETRY AS OBJECTS_GEOM
FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOMETRY, OBJECTS.GEOMETRY, 4, FALSE)
```

输出：

```
+--------+-----------------+-------------+
|QUERY_ID|QUERIES_GEOM     |OBJECTS_GEOM |
+--------+-----------------+-------------+
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (4 3)  |
|1       |POINT (1 1)      |POINT (4 3)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (11 5) |
|2       |POINT (10 10)    |POINT (11 5) |
+--------+-----------------+-------------+
```
