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

本页介绍如何使用 SedonaSQL 管理空间数据。==示例代码以 Java 编写，但同样适用于 Scala==。

SedonaSQL 支持 SQL/MM Part3 空间 SQL 标准，提供四类 SQL 算子（如下文所示），所有算子都可以直接通过以下方式调用：

```java
Table myTable = tableEnv.sqlQuery("YOUR_SQL")
```

SedonaSQL 详细的 API 说明请参阅 [SedonaSQL API](../../api/flink/Overview.md)。

## 配置依赖

1. 阅读 [Sedona Maven Central 坐标](../../setup/maven-coordinates.md)
2. 在 build.sbt 或 pom.xml 中添加 Sedona 依赖
3. 在 build.sbt 或 pom.xml 中添加 [Flink 依赖](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/)
4. 参考 [SQL 示例项目](../demo.md)

## 初始化 Stream Environment

在程序起始处使用以下代码初始化 `StreamExecutionEnvironment`：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
```

## 初始化 SedonaContext

在创建好 `StreamExecutionEnvironment` 与 `StreamTableEnvironment` 后，添加以下代码：

==Sedona >= 1.4.1==

```java
StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);
```

==Sedona <1.4.1==

下面这种方式自 Sedona 1.4.1 起已弃用，请改用上面的方式创建 SedonaContext。

```java
SedonaFlinkRegistrator.registerType(env);
SedonaFlinkRegistrator.registerFunc(tableEnv);
```

!!!warning
	Sedona 内置了一套高效的几何与索引序列化器，忘记启用它们会导致内存占用过高。

该函数会注册 Sedona 的用户自定义类型与用户自定义函数。

## 创建 Geometry 类型列

SedonaSQL 中所有几何运算都作用在 Geometry 类型对象上。因此在执行任何查询之前，需要先在 DataFrame 上构造一列 Geometry 类型列。

假设有一张如下的 Flink Table `tbl`：

```
+----+--------------------------------+--------------------------------+
| op |                   geom_polygon |                   name_polygon |
+----+--------------------------------+--------------------------------+
| +I | POLYGON ((-0.5 -0.5, -0.5 0... |                       polygon0 |
| +I | POLYGON ((0.5 0.5, 0.5 1.5,... |                       polygon1 |
| +I | POLYGON ((1.5 1.5, 1.5 2.5,... |                       polygon2 |
| +I | POLYGON ((2.5 2.5, 2.5 3.5,... |                       polygon3 |
| +I | POLYGON ((3.5 3.5, 3.5 4.5,... |                       polygon4 |
| +I | POLYGON ((4.5 4.5, 4.5 5.5,... |                       polygon5 |
| +I | POLYGON ((5.5 5.5, 5.5 6.5,... |                       polygon6 |
| +I | POLYGON ((6.5 6.5, 6.5 7.5,... |                       polygon7 |
| +I | POLYGON ((7.5 7.5, 7.5 8.5,... |                       polygon8 |
| +I | POLYGON ((8.5 8.5, 8.5 9.5,... |                       polygon9 |
+----+--------------------------------+--------------------------------+
10 rows in set
```

可以这样创建一张带有 Geometry 类型列的 Table：

```java
sedona.createTemporaryView("myTable", tbl)
Table geomTbl = sedona.sqlQuery("SELECT ST_GeomFromWKT(geom_polygon) as geom_polygon, name_polygon FROM myTable")
geomTbl.execute().print()
```

输出如下：

```
+----+--------------------------------+--------------------------------+
| op |                   geom_polygon |                   name_polygon |
+----+--------------------------------+--------------------------------+
| +I | POLYGON ((-0.5 -0.5, -0.5 0... |                       polygon0 |
| +I | POLYGON ((0.5 0.5, 0.5 1.5,... |                       polygon1 |
| +I | POLYGON ((1.5 1.5, 1.5 2.5,... |                       polygon2 |
| +I | POLYGON ((2.5 2.5, 2.5 3.5,... |                       polygon3 |
| +I | POLYGON ((3.5 3.5, 3.5 4.5,... |                       polygon4 |
| +I | POLYGON ((4.5 4.5, 4.5 5.5,... |                       polygon5 |
| +I | POLYGON ((5.5 5.5, 5.5 6.5,... |                       polygon6 |
| +I | POLYGON ((6.5 6.5, 6.5 7.5,... |                       polygon7 |
| +I | POLYGON ((7.5 7.5, 7.5 8.5,... |                       polygon8 |
| +I | POLYGON ((8.5 8.5, 8.5 9.5,... |                       polygon9 |
+----+--------------------------------+--------------------------------+
10 rows in set
```

虽然外观与输入一致，但 `geom_polygon` 列的类型已经变为 ==Geometry==。

可以打印 schema 进行验证：

```java
geomTbl.printSchema()
```

输出：

```
(
  `geom_polygon` RAW('org.locationtech.jts.geom.Geometry', '...'),
  `name_polygon` STRING
)
```

!!!note
	SedonaSQL 提供了大量构造 Geometry 列的函数，详见 [SedonaSQL 构造器 API](../../api/flink/Geometry-Functions.md)。

## 转换坐标参考系

Sedona 不会自动管理一列 Geometry 中所有几何对象的坐标单位（基于度还是基于米）。SedonaSQL 中所有相关距离的单位与 Geometry 列中几何对象的单位保持一致。

如需对前面创建的 Geometry 列进行 CRS 转换，可使用以下代码：

```java
Table geomTbl3857 = sedona.sqlQuery("SELECT ST_Transform(countyshape, "epsg:4326", "epsg:3857") AS geom_polygon, name_polygon FROM myTable")
geomTbl3857.execute().print()
```

`ST_Transform` 第一个 EPSG 代码 EPSG:4326 是源 CRS——也就是最常见的基于度的 CRS（WGS84）。

`ST_Transform` 第二个 EPSG 代码 EPSG:3857 是目标 CRS——最常见的基于米的 CRS。

`ST_Transform` 会把这些几何对象的 CRS 从 EPSG:4326 转换到 EPSG:3857。详细的 CRS 信息可以在 [EPSG.io](https://epsg.io/) 找到。

!!!note
	了解不同的空间查询谓词请阅读 [SedonaSQL ST_Transform API](../../api/flink/Spatial-Reference-System/ST_Transform.md)。

例如，存储美国坐标的 Table 转换前后会变为如下样子：

转换前：

```
+----+--------------------------------+--------------------------------+
| op |                     geom_point |                     name_point |
+----+--------------------------------+--------------------------------+
| +I |                POINT (32 -118) |                          point |
| +I |                POINT (33 -117) |                          point |
| +I |                POINT (34 -116) |                          point |
| +I |                POINT (35 -115) |                          point |
| +I |                POINT (36 -114) |                          point |
| +I |                POINT (37 -113) |                          point |
| +I |                POINT (38 -112) |                          point |
| +I |                POINT (39 -111) |                          point |
| +I |                POINT (40 -110) |                          point |
| +I |                POINT (41 -109) |                          point |
+----+--------------------------------+--------------------------------+
```

转换后：

```
+----+--------------------------------+--------------------------------+
| op |                            _c0 |                     name_point |
+----+--------------------------------+--------------------------------+
| +I | POINT (-13135699.91360628 3... |                          point |
| +I | POINT (-13024380.422813008 ... |                          point |
| +I | POINT (-12913060.932019735 ... |                          point |
| +I | POINT (-12801741.44122646 4... |                          point |
| +I | POINT (-12690421.950433187 ... |                          point |
| +I | POINT (-12579102.459639912 ... |                          point |
| +I | POINT (-12467782.96884664 4... |                          point |
| +I | POINT (-12356463.478053367 ... |                          point |
| +I | POINT (-12245143.987260092 ... |                          point |
| +I | POINT (-12133824.496466817 ... |                          point |
+----+--------------------------------+--------------------------------+
```

构造完 Geometry 类型列后，即可执行各类空间查询。

## 范围查询

使用 ==ST_Contains==、==ST_Intersects== 等谓词可以在单列上执行范围查询。

下例查找位于给定多边形内的所有 county：

```java
geomTable = sedona.sqlQuery(
  "
    SELECT *
    FROM spatialdf
    WHERE ST_Contains (ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape)
  ")
geomTable.execute().print()
```

!!!note
	了解不同的空间查询谓词请阅读 [SedonaSQL Predicate API](../../api/flink/Geometry-Functions.md#predicates)。

## KNN 查询

使用 ==ST_Distance== 计算距离并排序。

下面的代码返回距离给定多边形最近的 5 个对象：

```java
geomTable = sedona.sqlQuery(
  "
    SELECT countyname, ST_Distance(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape) AS distance
    FROM geomTable
    ORDER BY distance DESC
    LIMIT 5
  ")
geomTable.execute().print()
```

## 连接查询

下面的等值连接借助 Flink 内置的 equi-join 算法。可以选择跳过 Sedona 的 refinement 步骤来牺牲一部分查询精度。完整示例可参考 [SQL 示例项目](../demo.md)。

操作步骤如下：

### 1. 为两张表生成 S2 ID

使用 [ST_S2CellIds](../../api/flink/Spatial-Indexing/ST_S2CellIDs.md) 为每个几何对象生成 cell ID，单个几何对象可能产生多个 ID。

```sql
SELECT id, geom, name, ST_S2CellIDs(geom, 15) as idarray
FROM lefts
```

```sql
SELECT id, geom, name, ST_S2CellIDs(geom, 15) as idarray
FROM rights
```

### 2. 展开 ID 数组

S2 cell ID 是整数数组，需要把它们展开成多行，以便后续按 ID 进行连接。

```
SELECT id, geom, name, cellId
FROM lefts CROSS JOIN UNNEST(lefts.idarray) AS tmpTbl1(cellId)
```

```
SELECT id, geom, name, cellId
FROM rights CROSS JOIN UNNEST(rights.idarray) AS tmpTbl2(cellId)
```

### 3. 执行等值连接

按 S2 cellId 对两张表做等值连接：

```sql
SELECT lcs.id as lcs_id, lcs.geom as lcs_geom, lcs.name as lcs_name, rcs.id as rcs_id, rcs.geom as rcs_geom, rcs.name as rcs_name
FROM lcs JOIN rcs ON lcs.cellId = rcs.cellId
```

### 4. 可选：refine 结果

由于 S2 cellId 的特性，连接结果可能存在少量假阳性，具体程度取决于 S2 level 的选择：level 越小，cell 越大，展开行数越少，但假阳性越多。

为了保证正确性，可使用任一 [空间谓词](../../api/sql/Geometry-Functions.md#predicates) 过滤掉假阳性，将下面的查询替换步骤 3 中的查询：

```sql
SELECT lcs.id as lcs_id, lcs.geom as lcs_geom, lcs.name as lcs_name, rcs.id as rcs_id, rcs.geom as rcs_geom, rcs.name as rcs_name
FROM lcs, rcs
WHERE lcs.cellId = rcs.cellId AND ST_Contains(lcs.geom, rcs.geom)
```

可以看到，这里相比步骤 2 的查询多加了一个 `ST_Contains` 过滤来剔除假阳性，也可以使用 `ST_Intersects` 等其他谓词。

!!!tip
	如果不要求 100% 的准确度且希望更快的查询速度，可以跳过此步骤。

### 5. 可选：去重

由于生成 S2 cellId 时使用了展开操作，结果 DataFrame 中可能出现 `<lcs_geom, rcs_geom>` 重复匹配。可以通过 GroupBy 查询去重：

```sql
SELECT lcs_id, rcs_id, FIRST_VALUE(lcs_geom), FIRST_VALUE(lcs_name), first(rcs_geom), first(rcs_name)
FROM joinresult
GROUP BY (lcs_id, rcs_id)
```

`FIRST_VALUE` 函数用于在多个重复值中取第一个。

如果没有为每个几何对象提供唯一 ID，也可以直接按几何分组：

```sql
SELECT lcs_geom, rcs_geom, first(lcs_name), first(rcs_name)
FROM joinresult
GROUP BY (lcs_geom, rcs_geom)
```

!!!note
	做点-多边形 join 时不会出现该问题，可以放心忽略；只有 polygon-polygon、polygon-linestring、linestring-linestring 等连接才会遇到此问题。

### 距离连接中的 S2 用法

S2 同样适用于距离连接。先使用 `ST_Buffer(geometry, distance)` 把其中一列原始几何对象包裹起来。如果原始几何对象是点，`ST_Buffer` 会把它们变成半径为 `distance` 的圆。

例如，在步骤 1 之前，先在左表上运行：

```sql
SELECT id, ST_Buffer(geom, DISTANCE), name
FROM lefts
```

由于坐标位于经纬度系统中，`distance` 的单位应为度而不是米或英里。可以根据实际米值估算对应的度数，可借助 [此换算工具](https://lucidar.me/en/online-unit-converter-length-to-angle/convert-degrees-to-meters/#online-converter)。

## 把空间 Table 转换为空间 DataStream

### 获取 DataStream

使用 TableEnv 的 `toDataStream` 函数：

```java
DataStream<Row> geomStream = sedona.toDataStream(geomTable)
```

### 取出 Geometry

接着用 Map 从每个 Row 对象中取出 Geometry：

```java
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = geomStream.map(new MapFunction<Row, Geometry>() {
            @Override
            public Geometry map(Row value) throws Exception {
                return (Geometry) value.getField(0);
            }
        });
geometries.print();
```

输出如下：

```
14> POLYGON ((1.5 1.5, 1.5 2.5, 2.5 2.5, 2.5 1.5, 1.5 1.5))
2> POLYGON ((5.5 5.5, 5.5 6.5, 6.5 6.5, 6.5 5.5, 5.5 5.5))
5> POLYGON ((8.5 8.5, 8.5 9.5, 9.5 9.5, 9.5 8.5, 8.5 8.5))
16> POLYGON ((3.5 3.5, 3.5 4.5, 4.5 4.5, 4.5 3.5, 3.5 3.5))
12> POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))
13> POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))
15> POLYGON ((2.5 2.5, 2.5 3.5, 3.5 3.5, 3.5 2.5, 2.5 2.5))
3> POLYGON ((6.5 6.5, 6.5 7.5, 7.5 7.5, 7.5 6.5, 6.5 6.5))
1> POLYGON ((4.5 4.5, 4.5 5.5, 5.5 5.5, 5.5 4.5, 4.5 4.5))
4> POLYGON ((7.5 7.5, 7.5 8.5, 8.5 8.5, 8.5 7.5, 7.5 7.5))
```

### 在 Geometry 中保存非空间属性

可以把其他非空间属性拼接到 Geometry 的 `userData` 字段中保存，便于后续恢复。`userData` 字段可以是任意对象类型。

```java
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = geomStream.map(new MapFunction<Row, Geometry>() {
            @Override
            public Geometry map(Row value) throws Exception {
                Geometry geom = (Geometry) value.getField(0);
                geom.setUserData(value.getField(1));
                return geom;
            }
        });
geometries.print();
```

`print` 命令不会输出 `userData` 字段。可以这样获取：

```java
import org.locationtech.jts.geom.Geometry;

geometries.map(new MapFunction<Geometry, String>() {
            @Override
            public String map(Geometry value) throws Exception
            {
                return (String) value.getUserData();
            }
        }).print();
```

输出如下：

```
13> polygon9
6> polygon2
10> polygon6
11> polygon7
5> polygon1
12> polygon8
8> polygon4
4> polygon0
7> polygon3
9> polygon5
```

## 把空间 DataStream 转换为空间 Table

### 使用 Sedona 的 FormatUtils 创建 Geometry

* 从 WKT 字符串创建 Geometry

```java
import org.apache.sedona.common.utils.FormatUtils;
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = text.map(new MapFunction<String, Geometry>() {
            @Override
            public Geometry map(String value) throws Exception
            {
                FormatUtils formatUtils = new FormatUtils(FileDataSplitter.WKT, false);
                return formatUtils.readGeometry(value);
            }
        })
```

* 从字符串 `1.1, 2.2` 创建 Point，使用 `,` 作为分隔符。

```java
import org.apache.sedona.common.utils.FormatUtils;
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = text.map(new MapFunction<String, Geometry>() {
            @Override
            public Geometry map(String value) throws Exception
            {
                FormatUtils<Geometry> formatUtils = new FormatUtils(",", false, GeometryType.POINT);
                return formatUtils.readGeometry(value);
            }
        })
```

* 从字符串 `1.1, 1.1, 10.1, 10.1` 创建 Polygon，表示以 (1.1, 1.1) 与 (10.1, 10.1) 为最小/最大角点的矩形。

```java
import org.apache.sedona.common.utils.FormatUtils;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = text.map(new MapFunction<String, Geometry>() {
            @Override
            public Geometry map(String value) throws Exception
            {
            	  // 在此实现 minX、minY、maxX、maxY 四个 double 值的解析逻辑
            	  ...
	            Coordinate[] coordinates = new Coordinate[5];
	            coordinates[0] = new Coordinate(minX, minY);
	            coordinates[1] = new Coordinate(minX, maxY);
	            coordinates[2] = new Coordinate(maxX, maxY);
	            coordinates[3] = new Coordinate(maxX, minY);
	            coordinates[4] = coordinates[0];
	            GeometryFactory geometryFactory = new GeometryFactory();
	            return geometryFactory.createPolygon(coordinates);
            }
        })
```

### 创建 Row 对象

把 Geometry 包进 Flink Row 中放入 `geomStream`。也可以在 Row 中放入其他属性。本例为所有几何对象赋了同一个常量 `myName`。

```java
import org.apache.sedona.common.utils.FormatUtils;
import org.locationtech.jts.geom.Geometry;
import org.apache.flink.types.Row;

DataStream<Row> geomStream = text.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception
            {
                FormatUtils formatUtils = new FormatUtils(FileDataSplitter.WKT, false);
                return Row.of(formatUtils.readGeometry(value), "myName");
            }
        })
```

### 得到空间 Table

使用 TableEnv 的 `fromDataStream` 函数，并指定列名 `geom` 与 `geom_name`：

```java
Table geomTable = sedona.fromDataStream(geomStream, "geom", "geom_name")
```
