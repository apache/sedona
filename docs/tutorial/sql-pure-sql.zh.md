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

从 ==Sedona v1.0.1== 开始，您可以在纯 Spark SQL 环境中使用 Sedona，示例代码均以 SQL 编写。

SedonaSQL 支持 SQL/MM Part3 空间 SQL 标准。SedonaSQL 详细的 API 说明请参阅 [SedonaSQL API](../api/sql/Overview.md)。

## 启动会话

按以下方式启动 `spark-sql`（请将 `<VERSION>` 替换为实际版本，如 `{{ sedona.current_version }}`）：

!!! abstract "使用 Apache Sedona 启动 spark-sql"

	=== "Spark 3.3+ 与 Scala 2.12"

        ```sh
        spark-sql --packages org.apache.sedona:sedona-spark-shaded-3.3_2.12:<VERSION>,org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }} \
          --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
          --conf spark.kryo.registrator=org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator \
          --conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions
        ```

        请将 artifact 名称中的 `3.3` 替换为对应的 Spark major.minor 版本。

这会注册 SedonaSQL 与 SedonaViz 的全部类型、函数与优化规则。

## 加载数据

下面使用 `examples/sql` 目录中的数据。从 CSV 文件加载数据需要执行以下两条命令：

使用以下代码加载数据并创建原始 DataFrame：

```sql
CREATE TABLE IF NOT EXISTS pointraw (_c0 string, _c1 string)
USING csv
OPTIONS(header='false')
LOCATION '<some path>/sedona/examples/sql/src/test/resources/testpoint.csv';

CREATE TABLE IF NOT EXISTS polygonraw (_c0 string, _c1 string, _c2 string, _c3 string)
USING csv
OPTIONS(header='false')
LOCATION '<some path>/sedona/examples/sql/src/test/resources/testenvelope.csv';

```

## 转换数据

需要把点和多边形数据转换为对应的几何类型：

```sql
CREATE OR REPLACE TEMP VIEW pointdata AS
  SELECT ST_Point(cast(pointraw._c0 as Decimal(24,20)), cast(pointraw._c1 as Decimal(24,20))) AS pointshape
  FROM pointraw;

CREATE OR REPLACE TEMP VIEW polygondata AS
  select ST_PolygonFromEnvelope(cast(polygonraw._c0 as Decimal(24,20)),
        cast(polygonraw._c1 as Decimal(24,20)), cast(polygonraw._c2 as Decimal(24,20)),
        cast(polygonraw._c3 as Decimal(24,20))) AS polygonshape
  FROM polygonraw;
```

## 处理数据

例如，对多边形和点数据做一次连接：

```sql
SELECT * from polygondata, pointdata
WHERE ST_Contains(polygondata.polygonshape, pointdata.pointshape)
      AND ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondata.polygonshape)
LIMIT 5;
```

## `GEOMETRY` 数据类型支持

Sedona 提供了一个 Spark SQL 解析器扩展，使 DDL 语句中可以直接使用 `GEOMETRY` 数据类型。例如，可以在创建表时声明带几何列的 schema：

```sql
CREATE TABLE geom_table (id STRING, version INT, geometry GEOMETRY)
USING geoparquet
LOCATION '/path/to/geoparquet_geom_table';

SELECT * FROM geom_table LIMIT 10;
```

该 SQL 解析器扩展默认启用。如果它与其他扩展存在冲突需要禁用，请在启动 `spark-sql` 时通过 `--conf spark.sedona.enableParserExtension=false` 关闭。
