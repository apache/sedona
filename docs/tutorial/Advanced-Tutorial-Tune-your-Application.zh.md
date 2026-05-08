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

# 进阶教程：调优您的 Sedona RDD 应用

在进入这篇进阶教程之前，请确保您已经在本机尝试过若干 Sedona 函数。

## 选择合适的 Sedona 版本

Sedona 的版本号包含三级（例如 0.8.1）。

第一级表示该版本进行了较大的结构重设计，可能带来显著的 API 变化与性能差异。

第二级（如 0.8）表明该版本包含显著的性能提升、重要的新功能以及 API 变化。如果您是 Sedona 老用户并希望升级到这种版本，需要谨慎对待 API 变更。升级前请阅读 [Sedona 版本发布说明](../setup/release-notes.md)，确认能接受相应的 API 变化。

第三级（如 0.8.1）则只包含 bug 修复、少量小的新特性以及轻微的性能提升，不会包含任何 API 变化。升级到此类版本是安全的。我们强烈建议同一二级版本下的所有 Sedona 用户都升级到该级别的最新版本。

## 选择合适的 Spatial RDD 构造方式

Sedona 为每种 SpatialRDD（PointRDD、PolygonRDD、LineStringRDD）提供了多种构造方式。一般来说，您可以从两种入口开始：

1. 从 HDFS、S3 等数据源初始化 SpatialRDD。一个典型示例如下：

```java
public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
```

2. 从已有 RDD 初始化 SpatialRDD。一个典型示例如下：

```java
public PointRDD(JavaRDD<Point> rawSpatialRDD, StorageLevel newLevel)
```

可以注意到这些构造函数都接受一个 `StorageLevel` 参数。这是为了让 Spark 缓存 SpatialRDD 的一个属性 `rawSpatialRDD`。Sedona 这样做是因为它需要通过若干 Spark “Action” 计算数据集边界与近似总数；这些信息在执行 Spatial Join Query 与 Distance Join Query 时非常有用。

但有时您对自己的数据集十分了解，那么可以手动提供这些信息，调用如下形式的 Spatial RDD 构造函数：

```java
public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount) {
```

手动提供数据集边界与近似总数能让 Sedona 在初始化时跳过若干较慢的 “Action”。

## 缓存被反复使用的 Spatial RDD

每个 SpatialRDD（PointRDD、PolygonRDD、LineStringRDD）都包含 4 个 RDD 属性：

1. rawSpatialRDD：由 SpatialRDD 构造方法生成的 RDD。
2. spatialPartitionedRDD：基于 rawSpatialRDD 进行空间分区后的 RDD。注意：该 RDD 中存在被复制的空间对象。
3. indexedRawRDD：基于 rawSpatialRDD 建索引后的 RDD。
4. indexedRDD：基于 spatialPartitionedRDD 建索引后的 RDD。注意：该 RDD 中存在被复制的空间对象。

这 4 个 RDD 不会同时存在，所以无需担心内存问题。
它们在不同查询中分别被调用：

1. Spatial Range Query / KNN Query，未启用索引：使用 rawSpatialRDD。
2. Spatial Range Query / KNN Query，启用索引：使用 indexedRawRDD。
3. Spatial Join Query / Distance Join Query，未启用索引：使用 spatialPartitionedRDD。
4. Spatial Join Query / Distance Join Query，启用索引：使用 indexedRDD。

因此，如果您会多次执行上述某种查询，最好将对应的 RDD 缓存到内存中。常见的使用场景包括：

1. 在 Spatial Autocorrelation、Spatial Co-location Pattern Mining 等空间数据挖掘任务中，可能需要迭代地执行 Spatial Join / Spatial Self-join 来计算邻接矩阵。这种情况下请缓存被反复查询的 spatialPartitionedRDD/indexedRDD。
2. 在 [Livy](https://github.com/cloudera/livy)、[Spark Job Server](https://github.com/spark-jobserver/spark-jobserver) 等 Spark RDD 共享应用中，多名用户可能在同一份 Spatial RDD 上以不同谓词执行 Spatial Range Query / KNN Query，此时建议缓存 rawSpatialRDD/indexedRawRDD。

## 留意 Spatial RDD 的分区数

有时用户反映某些场景下执行时间较慢。第一步请始终考虑增加 SpatialRDD 的分区数（建议为原值的 2 - 8 倍），可以在初始化 SpatialRDD 时进行设置，这往往能显著提升性能。

之后您可以再考虑调整 Spark 的其他参数，例如使用 Kryo 序列化器，或调整缓存到内存的 RDD 比例。
