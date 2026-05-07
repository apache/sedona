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

# SedonaSnow

SedonaSnow 将 Apache Sedona 的 200 多个地理空间函数直接引入到 Snowflake 环境中，作为 Snowflake 原生空间函数的有力补充。

## 主要优势

* **200+ 空间函数**：例如 3D 距离、几何对象校验、精度归约等
* **高性能空间连接**：Sedona 针对空间连接做了专门优化
* **无缝集成**：与 Snowflake 原生函数协同工作
* **数据无需移动**：所有处理都在 Snowflake 内部完成

## 快速上手

下面是一个使用 SedonaSnow 在 Snowflake 表上执行查询的示例。

```sql
USE DATABASE SEDONASNOW;

SELECT SEDONA.ST_GeomFromWKT(wkt) AS geom
FROM your_table;

SELECT SEDONA.ST_3DDistance(geom1, geom2) FROM spatial_data;
```

下面是一个空间连接的示例：

```sql
SELECT * FROM lefts, rights
WHERE lefts.cellId = rights.cellId;
```

可以看出，SedonaSnow 能够无缝地接入现有的 Snowflake 环境。

## 后续步骤

如果您正在 Snowflake 中进行严肃的空间分析工作，SedonaSnow 是一个非常合适的选择。它运行速度快，并提供了丰富的空间函数。SedonaSnow 在不需要将数据迁移到其他平台的前提下，突破了 Snowflake 内置空间函数的能力限制。
