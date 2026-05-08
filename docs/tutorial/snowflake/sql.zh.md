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

完成安装后，您就可以开始使用 Sedona 函数了。请使用具备访问数据库权限的用户重新登录 Snowflake。

!!!note
	使用 Sedona 函数时请始终保留 schema 名 `SEDONA`（如 `SEDONA.ST_GeomFromWKT`），以避免与 Snowflake 内置函数冲突。

## 在 Snowflake 中访问 Sedona 函数

请先确保引用了正确的函数。默认情况下，Sedona 函数位于 `SEDONASNOW.SEDONA` 这个数据库.schema 下：

```sql
USE DATABASE SEDONASNOW;
```

也可以使用其他数据库，并通过完全限定名调用，例如 `SEDONASNOW.SEDONA.ST_GeomFromText`。

## 创建示例表

下面创建一个 `city_tbl`，其中包含城市的位置与名称。每个位置以 WKT 字符串表示：

```sql
CREATE OR REPLACE TABLE city_tbl (wkt STRING, city_name STRING);
INSERT INTO city_tbl(wkt, city_name) VALUES ('POINT (-122.33 47.61)', 'Seattle');
INSERT INTO city_tbl(wkt, city_name) VALUES ('POINT (-122.42 37.76)', 'San Francisco');
```

可以查看该表的内容：

```sql
SELECT *
FROM city_tbl;
```

输出：

```
WKT	CITY_NAME
POINT (-122.33 47.61)	Seattle
POINT (-122.42 37.76)	San Francisco
```

## 创建 Geometry/Geography 列

SedonaSQL 中所有几何运算都作用在 Geometry/Geography 类型对象上。因此在执行任何查询之前，需要在表上构造一列 Geometry/Geography 类型列：

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT SEDONA.ST_GeomFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

`city_tbl_geom` 表中的 `geom` 列现在是 `Binary` 类型，其中数据以 Sedona 能理解的格式存储。该查询的输出会以 WKB 二进制形式显示几何对象，类似如下：

```
GEOM CITY_NAME
010100000085eb51b81e955ec0ae47e17a14ce4740  Seattle
01010000007b14ae47e19a5ec0e17a14ae47e14240  San Francisco
```

如需以人类可读的方式查看该列内容，可以使用 `SEDONA.ST_AsText`，例如：

```sql
SELECT SEDONA.ST_AsText(geom), city_name
FROM city_tbl_geom
```

也可以创建 Snowflake 原生 Geometry / Geography 类型列。例如，以下代码创建一个 Snowflake 原生 Geometry 类型列（注意函数名称没有 `SEDONA` 前缀）：

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT ST_GeometryFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

下面的代码创建一个 Snowflake 原生 Geography 类型列（同样没有 `SEDONA` 前缀）：

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT ST_GeographyFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

!!!note
	SedonaSQL 提供了大量构造 Geometry 列的函数，详见 [SedonaSQL API](../../api/snowflake/vector-data/Geometry-Functions.md)。

## 检查经纬度顺序（lon/lat）

在 SedonaSnow `v1.4.1` 及之前的版本中，下列函数使用 lat/lon 顺序：

* SEDONA.ST_Transform
* SEDONA.ST_DistanceSphere
* SEDONA.ST_DistanceSpheroid

下列函数使用 `lon/lat` 顺序：

* SEDONA.ST_GeomFromGeoHash
* SEDONA.ST_GeoHash
* SEDONA.ST_S2CellIDs

自 Sedona `v1.5.0` 起，所有函数统一使用 lon/lat 顺序。

如果原始数据的坐标顺序与所需顺序不一致，可使用 `SEDONA.ST_FlipCoordinates(geom: Geometry)` 进行翻转。

上文示例数据采用 lon/lat 顺序，可按如下方式翻转坐标：

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT SEDONA.ST_FlipCoordinates(geom) AS geom, city_name
FROM city_tbl_geom
```

再次查看表内容，此时已变为 lat/lon 顺序：

```sql
SELECT SEDONA.ST_AsText(geom), city_name
FROM city_tbl_geom
```

输出：

```
GEOM	CITY_NAME
POINT (47.61 -122.33)	Seattle
POINT (37.76 -122.42)	San Francisco
```

## 保存为普通列

要将表持久化到永久存储中，只需把 Geometry 类型列里的每个几何对象转换回普通字符串再保存即可。

可使用以下代码把表中的 Geometry 列还原为 WKT 字符串列：

```sql
SELECT SEDONA.ST_AsText(geom)
FROM city_tbl_geom
```

!!!note
	SedonaSQL 提供了多种保存 Geometry 列的函数，详见 [SedonaSQL API](../../api/snowflake/vector-data/Overview.md)。

## 转换坐标参考系

Sedona 不会自动管理一列 Geometry 中所有几何对象的坐标单位（基于度还是基于米）。SedonaSQL 中所有相关距离的单位与 Geometry 列中几何对象的单位保持一致。

要转换 Geometry 列的坐标参考系，可使用 `SEDONA.ST_Transform(A:geometry, SourceCRS:string, TargetCRS:string)`。

`SEDONA.ST_Transform` 第一个 EPSG 代码 EPSG:4326 是源 CRS——也就是最常见的基于度的 CRS（WGS84）。

第二个 EPSG 代码 EPSG:3857 是目标 CRS——最常见的基于米的 CRS。

`SEDONA.ST_Transform` 会把这些几何对象的 CRS 从 EPSG:4326 转换到 EPSG:3857。详细 CRS 信息可以在 [EPSG.io](https://epsg.io/) 找到。

!!!note
	该函数在 1.5.0+ 中使用 lon/lat 顺序，在 1.4.1 及之前使用 lat/lon 顺序。可以使用 `SEDONA.ST_FlipCoordinates` 交换 X 与 Y。

按以下方式转换示例数据：

```sql
SELECT SEDONA.ST_AsText(SEDONA.ST_Transform(geom, 'epsg:4326', 'epsg:3857')), city_name
FROM city_tbl_geom
```

输出：

```
POINT (6042216.250411431 -13617713.308741156)  Seattle
POINT (4545577.120361927 -13627732.06291255)  San Francisco
```

`SEDONA.ST_Transform` 同样支持 OGC WKT 格式的 CRS 字符串。下面的查询输出与上面相同，但目标 CRS 以 OGC WKT 字符串给出：

```sql
SELECT SEDONA.ST_AsText(SEDONA.ST_Transform(geom, 'epsg:4326', 'PROJCS["WGS 84 / Pseudo-Mercator",
     GEOGCS["WGS 84",
         DATUM["WGS_1984",
             SPHEROID["WGS 84",6378137,298.257223563,
                 AUTHORITY["EPSG","7030"]],
             AUTHORITY["EPSG","6326"]],
         PRIMEM["Greenwich",0,
             AUTHORITY["EPSG","8901"]],
         UNIT["degree",0.0174532925199433,
             AUTHORITY["EPSG","9122"]],
         AUTHORITY["EPSG","4326"]],
     PROJECTION["Mercator_1SP"],
     PARAMETER["central_meridian",0],
     PARAMETER["scale_factor",1],
     PARAMETER["false_easting",0],
     PARAMETER["false_northing",0],
     UNIT["metre",1,
         AUTHORITY["EPSG","9001"]],
     AXIS["Easting",EAST],
     AXIS["Northing",NORTH],
     EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs"],
     AUTHORITY["EPSG","3857"]]')), city_name
FROM city_tbl_geom
```

## 范围查询

使用 ==SEDONA.ST_Contains==、==SEDONA.ST_Intersects==、==SEDONA.ST_Within== 在单列上执行范围查询。

下例查找位于给定多边形内的所有几何对象：

```sql
SELECT *
FROM city_tbl_geom
WHERE SEDONA.ST_Contains(SEDONA.ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), geom)
```

!!!note
	了解如何创建 Geometry 类型的查询窗口请参阅 [SedonaSQL API](../../api/snowflake/vector-data/Geometry-Functions.md)。

## KNN 查询

使用 ==SEDONA.ST_Distance==、==SEDONA.ST_DistanceSphere==、==ST_DistanceSpheroid== 计算距离并排序。

下面的代码返回距离给定点最近的 5 个对象：

```sql
SELECT geom, SEDONA.ST_Distance(SEDONA.ST_Point(1.0, 1.0), geom) AS distance
FROM city_tbl_geom
ORDER BY distance DESC
LIMIT 5
```

## 范围连接查询

!!!warning
	Sedona 在 Snowflake 中的范围连接不会触发 Sedona 的优化空间连接算法（Sedona Spark 会触发），它使用的是 Snowflake 默认的笛卡尔连接，速度非常慢。建议改用 Sedona 的 S2 等值连接，或者使用 Snowflake 原生 ST 函数 + 原生 `Geography` 类型来执行范围连接，这样可以触发 Snowflake 的 `GeoJoin` 算法。

简介：从 A、B 两侧的几何对象中找到所有满足某种谓词的几何对组合。

示例：

构造演示用的表：

```sql
CREATE OR REPLACE TABLE polygondf AS
SELECT SEDONA.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') polygonshape;
```

```sql
CREATE OR REPLACE TABLE pointdf AS
SELECT SEDONA.ST_GeomFromText('POINT(0.5 0.5)') pointshape;
```

使用不同的空间谓词执行查询：

```sql
SELECT *
FROM polygondf, pointdf
WHERE SEDONA.ST_Contains(polygondf.polygonshape,pointdf.pointshape)
```

```sql
SELECT *
FROM polygondf, pointdf
WHERE SEDONA.ST_Intersects(polygondf.polygonshape,pointdf.pointshape)
```

```sql
SELECT *
FROM pointdf, polygondf
WHERE SEDONA.ST_Within(pointdf.pointshape, polygondf.polygonshape)
```

对应的更快的空间连接（仅使用 Snowflake 原生函数）写法：

```sql
WITH polygondf AS (
    SELECT ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') polygonshape
),
pointdf AS (
    SELECT ST_GeomFromText('POINT(0.5 0.5)') pointshape
)
SELECT *
FROM polygondf, pointdf
WHERE ST_Contains(polygondf.polygonshape,pointdf.pointshape)
```

`ST_Intersects`、`ST_Within` 等其他谓词也可以原生使用。

## 距离连接

!!!warning
	Sedona 在 Snowflake 中的距离连接不会触发 Sedona 的优化空间连接算法（Sedona Spark 会触发），它使用的是 Snowflake 默认的笛卡尔连接，速度非常慢。建议改用 Sedona 的 S2 等值连接，或者使用 Snowflake 原生 ST 函数 + 原生 `Geography` 类型来执行范围连接，从而触发 Snowflake 的 `GeoJoin` 算法。

简介：从 A、B 两侧的几何对象中找到所有距离小于等于某阈值的对组合。支持的平面欧氏距离函数包括 `SEDONA.ST_Distance`、`SEDONA.ST_HausdorffDistance`、`SEDONA.ST_FrechetDistance`，基于米的测地距离函数包括 `SEDONA.ST_DistanceSpheroid` 与 `SEDONA.ST_DistanceSphere`。Snowflake 仅原生支持 `ST_Distance`。

先创建两个仅含一个几何对象的演示表：

```sql
CREATE OR REPLACE TABLE pointdf2 AS
SELECT SEDONA.ST_GeomFromText('POINT(0 0)') pointshape;
```

```sql
CREATE OR REPLACE TABLE polygondf2 AS
SELECT SEDONA.ST_GeomFromText('POLYGON((0.5 0.5, 0.5 1, 1 1, 1 0.5, 0.5 0.5))') polygonshape;
```

平面欧氏距离示例：

`POINT(0 0)` 与 `POINT(0.5 0.5)` 之间的常规 L2 欧氏距离是 0.5 的平方根，因此下面这条查询返回这唯一一对点：

```sql
SELECT *
FROM pointdf, pointdf2
WHERE SEDONA.ST_Distance(pointdf.pointshape,pointdf2.pointshape) <= sqrt(0.5)
```

两个不相离的多边形之间的 L2 距离为 0，因此下面的查询返回这唯一的多边形对：

```sql
SELECT *
FROM polygondf, polygondf2
WHERE SEDONA.ST_Distance(polygondf.polygonshape,polygondf2.polygonshape) <= 0
```

但 Hausdorff 与 Fréchet 距离未必如此。下面的两个查询都不会返回任何行：

```sql
SELECT *
FROM polygondf, polygondf2
WHERE SEDONA.ST_HausdorffDistance(polygondf.polygonshape, polygondf2.polygonshape, 0.3) <= 0
```

```sql
SELECT *
FROM polygondf, polygondf2
WHERE SEDONA.ST_FrechetDistance(polygondf.polygonshape, polygondf2.polygonshape)  <= 0
```

注意：只有 Hausdorff 距离接受第三个参数 `densityFraction`，取值范围 $(0,1]$，值越小结果越精确。

!!!warning
	如果使用 `SEDONA.ST_Distance`、`SEDONA.ST_HausdorffDistance` 或 `SEDONA.ST_FrechetDistance` 等平面欧氏距离函数作为谓词，Sedona 不会管理 `distance` 的单位（度还是米），其单位与几何对象保持一致。如果坐标是经纬度，那么 `distance` 的单位应当是度而不是米或英里。要修改几何对象的单位，要么通过 [SEDONA.ST_Transform](../../api/snowflake/vector-data/Spatial-Reference-System/ST_Transform.md) 转换到基于米的坐标系；如果不希望转换数据，请改用 `SEDONA.ST_DistanceSpheroid` 或 `SEDONA.ST_DistanceSphere`。

例如，下面的查询大约返回 78.45 千米——因为即使没有显式设置 CRS，几何对象也被默认视为经纬度：

```sql
SELECT SEDONA.ST_DistanceSpheroid(pointdf.pointshape,pointdf2.pointshape)
FROM pointdf, pointdf2
```

下面这条查询展示了：在 `GEOGRAPHY` 对象上使用 Snowflake 原生的 `ST_DISTANCE` 与 `SEDONA.ST_DistanceSphere`、`SEDONA.ST_DistanceSpheroid` 给出的结果相近：

```sql
SELECT
    SEDONA.ST_DistanceSphere(
        pointdf.pointshape,
        pointdf2.pointshape
    ) SEDONA_ST_DistanceSphere,
    --
    SEDONA.ST_DistanceSpheroid(
        pointdf.pointshape,
        pointdf2.pointshape
    ) SEDONA_ST_DistanceSpheroid,
    --
    ST_DISTANCE(
        TO_GEOGRAPHY(SEDONA.st_astext(pointdf.pointshape)),
        TO_GEOGRAPHY(SEDONA.st_astext(pointdf2.pointshape) )
    ) SFKL_ST_DISTANCE
FROM pointdf, pointdf2
```

输出：

| SEDONA_ST_DISTANCESPHERE | SEDONA_ST_DISTANCESPHEROID | SFKL_ST_DISTANCE |
| ------------------------ | -------------------------- | ---------------- |
| 78626.28640698           | 78451.248031239            | 78626.311089506  |

## 基于 Google S2 的近似等值连接

可以使用 Sedona 内置的 Google S2 函数来执行近似等值连接。该方法借助 Snowflake 内置的等值连接算法，可以选择跳过 refinement 步骤来牺牲精度换取性能。Sedona 的 S2 函数与 Snowflake 原生的 H3、Geohash 函数互为补充。

操作步骤如下：

### 1. 为两张表生成 S2 ID

使用 [SEDONA.ST_S2CellIds](../../api/snowflake/vector-data/Spatial-Indexing/ST_S2CellIDs.md) 为每个几何对象生成 cell ID，单个几何对象可能产生多个 ID。

```sql
SELECT * FROM lefts, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(lefts.geom, 15))) s1
```

```sql
SELECT * FROM rights, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(rights.geom, 15))) s2
```

### 2. 执行等值连接

按 S2 cellId 对两张表做等值连接：

```sql
SELECT lcs.id AS lcs_id, lcs.geom AS lcs_geom, lcs.name AS lcs_name, rcs.id AS rcs_id, rcs.geom AS rcs_geom, rcs.name AS rcs_name
FROM lcs JOIN rcs ON lcs.cellId = rcs.cellId
```

### 3. 可选：refine 结果

由于 S2 cellId 的特性，连接结果可能存在少量假阳性，具体程度取决于 S2 level：level 越小，cell 越大，展开行数越少，但假阳性越多。

为保证正确性，可使用任一 [空间谓词](../../api/snowflake/vector-data/Geometry-Functions.md#predicates) 过滤掉假阳性。将下面的查询替换步骤 2 中的查询：

```sql
SELECT lcs.id AS lcs_id, lcs.geom AS lcs_geom, lcs.name AS lcs_name, rcs.id AS rcs_id, rcs.geom AS rcs_geom, rcs.name AS rcs_name
FROM lcs, rcs
WHERE lcs.cellId = rcs.cellId AND ST_Contains(lcs.geom, rcs.geom)
```

可以看到，相比步骤 2 多了一个 `ST_Contains` 过滤来剔除假阳性，也可以使用 `ST_Intersects` 等其他谓词。

!!!tip
	如果不要求 100% 准确度且希望更快的查询速度，可以跳过此步骤。

### 4. 可选：去重

由于生成 S2 cellId 时使用了 `Flatten`，结果 DataFrame 中可能出现 `<lcs_geom, rcs_geom>` 重复匹配。可通过 GroupBy 去重：

```sql
SELECT lcs_id, rcs_id, ANY_VALUE(lcs_geom), ANY_VALUE(lcs_name), ANY_VALUE(rcs_geom), ANY_VALUE(rcs_name)
FROM joinresult
GROUP BY (lcs_id, rcs_id)
```

`ANY_VALUE` 用于在多个重复值中取一个。

如果没有为每个几何对象提供唯一 ID，也可以直接按几何分组：

```sql
SELECT lcs_geom, rcs_geom, ANY_VALUE(lcs_name), ANY_VALUE(rcs_name)
FROM joinresult
GROUP BY (lcs_geom, rcs_geom)
```

!!!note
	做点-多边形 join 时不会出现该问题，可以放心忽略；只有 polygon-polygon、polygon-linestring、linestring-linestring 等连接才会遇到此问题。

### 5. 完整示例

下面的查询创建了 2 个感兴趣区域（AOI）和 5 种空间查询（用经纬度边界框表示），分别为两张表生成 S2 覆盖，再以 S2 cell ID 进行合并。可以叠加空间过滤来保证连接结果的精确性。

下例中的几何对象使用 Snowflake 原生 `ST_GeogFromText`（被视作 geography），用 `ST_GeomFromText` 也是可行的。注意 `SEDONA.ST_S2CellIDs` 接受 Snowflake `GEOGRAPHY` 或 `GEOMETRY` 对象以及一个整数 S2 精度。

```sql
-- lng/lat 顺序：无需翻转坐标
-- lefts 是感兴趣区域（AOI）
with lefts AS (
  SELECT index AS poly_id_left, value AS wkt FROM TABLE(
      SPLIT_TO_TABLE (
          'POLYGON ((-74.64966372842101805 44.92318068906040196, -73.05513946490677313 44.92318068906040196, -73.05513946490677313 45.9127817308399031, -74.64966372842101805 45.9127817308399031, -74.64966372842101805 44.92318068906040196))|POLYGON ((-71.72125014775386376 46.58534825561803672, -70.72763860520016976 46.58534825561803672, -70.72763860520016976 47.26007441306773416, -71.72125014775386376 47.26007441306773416, -71.72125014775386376 46.58534825561803672))', '|'
          )
      )

),
-- rights 是查询多边形
-- 第 1 个多边形与两个 AOI 都相交
-- 第 2 个多边形包含两个 AOI
-- 第 3 个多边形被 AOI 1 包含
-- 第 4 个多边形与 AOI 1 接触
-- 第 5 个多边形与两个 AOI 都不相交
rights AS (
  SELECT index AS poly_id_right, value AS wkt FROM TABLE(
      SPLIT_TO_TABLE (
          'POLYGON ((-73.51160030163114811 45.54300066590783302, -71.31736631503980561 45.54300066590783302, -71.31736631503980561 46.82515071005796869, -73.51160030163114811 46.82515071005796869, -73.51160030163114811 45.54300066590783302))|POLYGON ((-75.26522832 44.23585380, -69.77500453509208 44.23585380, -69.77500453509208 47.59180373699041411, -75.26522832 47.59180373699041411, -75.26522832 44.23585380))|POLYGON ((-73.82001807503861812 45.0759163125215423, -73.34011383205873358 45.0759163125215423, -73.34011383205873358 45.35768613572972185, -73.82001807503861812 45.35768613572972185, -73.82001807503861812 45.0759163125215423))|POLYGON ((-74.64966372842101805 44.92318068906040196, -75.26522832 44.92318068906040196, -75.26522832 44.23585380, -74.64966372842101805 44.23585380, -74.64966372842101805 44.92318068906040196))|POLYGON ((-71.65791191749059408 44.19369241163229844, -70.2497216546401404 44.19369241163229844, -70.2497216546401404 44.96946189775351144, -71.65791191749059408 44.96946189775351144, -71.65791191749059408 44.19369241163229844))', '|'
          )
      )
),
-- 为两张表的多边形分别计算 S2 cell 覆盖
-- S2 会对多边形进行离散化，分辨率越高结果越精确，但计算开销也越大
-- 两张表必须使用相同 level 进行离散化，本例为 10
lefts_s2 AS (
    SELECT * FROM lefts, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(ST_GeogFromText(lefts.wkt), 10)))
),
rights_s2 AS (
    SELECT * FROM rights, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(ST_GeogFromText(rights.wkt), 10)))
)
-- 按 S2 索引（int）合并，并 group by 以恢复原多边形而非 S2 cell
-- 加上空间谓词可保证精确性；如果更看重速度可以省略
-- 预期除第 5 个查询外都会匹配
-- 期望结果共 6 行：1（接触）+ 1（被包含）+ 2（与两者相交）+ 2（包含两者）
-- AOI 1（poly_id_left 1）应出现 4 次，AOI 2（poly_id_left 2）应出现 2 次
SELECT rights_s2.wkt ,  lefts_s2.wkt, LISTAGG(DISTINCT poly_id_right::TEXT) poly_id_right, LISTAGG(DISTINCT poly_id_left::TEXT) poly_id_left
FROM lefts_s2,rights_s2
WHERE rights_s2.value = lefts_s2.value AND NOT ST_DISJOINT(ST_GeogFromText(rights_s2.wkt) , ST_GeogFromText( lefts_s2.wkt ) )
GROUP BY rights_s2.wkt ,  lefts_s2.wkt

```

本例中省略 `not ST_DISJOINT(ST_GeogFromText(rights_s2.wkt) , ST_GeogFromText( lefts_s2.wkt ) )` 也会得到相同的结果。

### 距离连接中的 S2 用法

S2 同样适用于距离连接。先使用 `SEDONA.ST_Buffer(geometry, distance)` 把其中一列原始几何对象包裹起来。如果原始几何是点，`SEDONA.ST_Buffer` 会把它们变成半径为 `distance` 的圆。注意：Snowflake 没有原生的 `ST_Buffer` 函数。

由于坐标位于经纬度系统，`distance` 的单位应为度而不是米或英里。可以通过 `METER_DISTANCE/111000.0` 做近似换算，再过滤掉假阳性。靠近极地或反子午线的数据可能会因此产生不准确的结果。

简而言之，先在左表上跑下面这条查询（替换 `METER_DISTANCE` 为米值），然后在步骤 1 中基于 `buffered_geom` 列生成 S2 ID，再在原始 `geom` 列上执行步骤 2、3、4：

```sql
SELECT id, geom, SEDONA.ST_Buffer(geom, METER_DISTANCE/111000.0) AS buffered_geom, name
FROM lefts
```

## Sedona 独有的函数

Sedona 实现了 200 多个地理空间矢量与栅格函数，远多于 Snowflake 原生函数。例如：

* [SEDONA.ST_3DDistance](../../api/snowflake/vector-data/Measurement-Functions/ST_3DDistance.md)
* [SEDONA.ST_Force2D](../../api/snowflake/vector-data/Geometry-Editors/ST_Force_2D.md)
* [SEDONA.ST_GeometryN](../../api/snowflake/vector-data/Geometry-Accessors/ST_GeometryN.md)
* [SEDONA.ST_MakeValid](../../api/snowflake/vector-data/Geometry-Validation/ST_MakeValid.md)
* [SEDONA.ST_Multi](../../api/snowflake/vector-data/Geometry-Editors/ST_Multi.md)
* [SEDONA.ST_NumGeometries](../../api/snowflake/vector-data/Geometry-Accessors/ST_NumGeometries.md)
* [SEDONA.ST_ReducePrecision](../../api/snowflake/vector-data/Geometry-Processing/ST_ReducePrecision.md)
* [SEDONA.ST_SubdivideExplode](../../api/snowflake/vector-data/Overlay-Functions/ST_SubDivideExplode.md)

可点击上面的链接深入了解这些函数。更多函数请参考 [SedonaSQL API](../../api/snowflake/vector-data/Overview.md)。

## 与 Snowflake 原生函数互通

Sedona 可以与 Snowflake 原生函数无缝互通，主要有两种方式：

* 使用 `Sedona 函数` 创建 Geometry 列，然后使用 Snowflake 原生函数与 Sedona 函数共同查询；
* 使用 `Snowflake 原生函数` 创建 Geometry/Geography 列，然后使用 Snowflake 原生函数与 Sedona 函数共同查询。

下面分别进行演示。

### 由 Sedona 几何构造器创建的几何

这种情况下 Sedona 使用 EWKB 作为几何对象的输入/输出类型。如果数据集中已是内置的 Snowflake GEOMETRY/GEOGRAPHY 类型，可以通过该函数轻松转换为 EWKB。

#### 从 Snowflake 原生函数到 Sedona 函数

下例中 `SEDONA.ST_X` 是 Sedona 函数，`ST_GeommetryFromWkt` 与 `ST_AsEWKB` 是 Snowflake 原生函数：

```sql
SELECT SEDONA.ST_X(ST_AsEWKB(ST_GeometryFromWkt('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))) FROM {{geometry_table}};
```

#### 从 Sedona 函数到 Snowflake 原生函数

下例中 `SEDONA.ST_GeomFromText` 是 Sedona 函数，`ST_AREA` 与 `to_geometry` 是 Snowflake 原生函数：

```sql
SELECT ST_AREA(to_geometry(SEDONA.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')));
```

#### 优点：

Sedona 的几何构造器比 Snowflake 原生函数更强大，主要优势如下：

* Sedona 提供了更多构造器，尤其是用于 3D（XYZ）几何对象的构造器，Snowflake 原生函数不具备。
* WKB 序列化更高效。如果需要使用多个 Sedona 函数，建议采用此方式，可能带来 2 倍左右的性能提升。
* 几何对象的 SRID 信息会被保留。下面这种方式会丢失 SRID 信息。

### 由 Snowflake 几何构造器创建的 Geometry / Geography

这种情况下 Sedona 使用 Snowflake 原生 GEOMETRY/GEOGRAPHY 类型作为输入/输出类型，序列化格式为 GeoJSON 字符串。

```sql
SELECT ST_AREA(SEDONA.ST_Buffer(ST_GeometryFromWkt('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), 1));
```

下例中 `SEDONA.ST_Buffer` 是 Sedona 函数，`ST_GeommetryFromWkt` 与 `ST_AREA` 是 Snowflake 原生函数。

可以看到，Sedona 函数与 Snowflake 原生函数可以混用而无需显式转换。

#### 优点：

* 无需显式转换几何类型，使用更方便。

注意：Snowflake 原生会把 Geometry 类型数据序列化为 GeoJSON 字符串再传给 UDF。GeoJSON 规范不包含 SRID。因此，如果直接混用 Snowflake 函数与 Sedona 函数而不使用 `WKB`，SRID 信息会丢失。

下例中 SRID=4326 信息会丢失：

```sql
SELECT ST_AsEWKT(SEDONA.ST_SetSRID(ST_GeometryFromWKT('POINT(1 2)'), 4326))
```

输出：

```
SRID=0;POINT(1 2)
```

## 已知问题

1. Sedona Snowflake 不支持 `M` 维度，原因是 WKB 序列化的限制。Sedona Spark 与 Sedona Flink 由于使用了内部专有序列化格式，因此支持 XYZM。Sedona Snowflake 中虽然存在与 `M` 相关的函数，但所有 `M` 值会被忽略。
2. Sedona H3 函数尚不可用，因为 Snowflake 不允许在 UDF 中嵌入 C 代码。
3. 由于 Snowflake 当前限制（`Data type GEOMETRY is not supported in non-SQL UDTF return type`），所有用户自定义表函数（UDTF）只能配合 Sedona 构造器创建的几何对象使用，包括：
   	* SEDONA.ST_MinimumBoundingRadius
   	* SEDONA.ST_Intersection_Aggr
   	* SEDONA.ST_SubDivideExplode
   	* SEDONA.ST_Envelope_Aggr
   	* SEDONA.ST_Union_Aggr
   	* SEDONA.ST_Collect
   	* SEDONA.ST_Dump
4. Snowflake 中只提供 Sedona 的 ST 函数，栅格函数（RS 函数）暂未支持。
