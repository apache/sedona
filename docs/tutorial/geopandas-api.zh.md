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

# Apache Sedona 上的 GeoPandas API

Apache Sedona 上的 GeoPandas API 提供了与 GeoPandas 一致的接口，可以让您的地理空间分析突破单机的局限。该 API 把熟悉的 GeoPandas DataFrame 语法与 Apache Sedona 在 Apache Spark 上的分布式处理能力结合起来，让您能够使用同样的代码模式处理行星级（planetary-scale）的数据集。

## 概览

### 什么是 Apache Sedona 的 GeoPandas API？

Apache Sedona 的 GeoPandas API 是一层兼容层，让您能够在分布式地理空间数据上使用 GeoPandas 风格的操作。您原有的 GeoPandas 代码不再受限于单机处理能力，可以充分利用 Apache Spark 集群进行大规模地理空间分析。

### 主要优势

- **熟悉的 API**：使用您已经熟悉的 GeoPandas 语法与方法
- **分布式处理**：突破单机限制，处理大规模数据集
- **惰性求值**：受益于 Apache Sedona 的查询优化与延迟执行
- **高性能**：利用分布式计算高效完成复杂的地理空间运算
- **平滑迁移**：以最少的代码改动迁移现有 GeoPandas 工作流

## 准备工作

Apache Sedona 的 GeoPandas API 通过 PySpark 的 pandas-on-Spark 集成自动管理 SparkSession。准备方式有两种：

### 方式 1：自动创建 SparkSession（推荐）

GeoPandas API 会自动使用 PySpark 的默认 SparkSession：

```python
from sedona.spark.geopandas import GeoDataFrame, read_parquet

# 无需显式配置 SparkSession，使用默认会话
# API 会自动初始化 Sedona context
```

### 方式 2：手动配置 SparkSession

如果需要自定义 SparkSession，或需要在某些环境下显式控制：

```python
from sedona.spark.geopandas import GeoDataFrame, read_parquet
from sedona.spark import SedonaContext

# 创建并配置 SparkSession
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

# GeoPandas API 会使用上面配置好的会话
```

### 方式 3：复用现有 SparkSession

如果已有 SparkSession（如 Databricks、EMR 等托管环境）：

```python
from sedona.spark.geopandas import GeoDataFrame, read_parquet
from sedona.spark import SedonaContext

# 复用现有 SparkSession（如 Databricks 中的 `spark`）
sedona = SedonaContext.create(spark)  # `spark` 即已有会话
```

### SparkSession 是如何被管理的

GeoPandas API 借助 PySpark 的 pandas-on-Spark 自动管理 SparkSession 生命周期：

1. **默认会话**：导入 `sedona.spark.geopandas` 时，会自动通过 `pyspark.pandas.utils.default_session()` 获取 PySpark 的默认会话。

2. **自动注册 Sedona 函数**：必要时 API 会自动把 Sedona 的空间函数与优化注册到 SparkSession。

3. **透明集成**：所有 GeoPandas 操作在底层都被翻译为 Spark SQL 操作，使用所配置的 SparkSession 执行。

4. **无需手动管理 context**：与传统的 Sedona 用法不同，您通常不需要显式调用 `SedonaContext.create()`，除非有自定义配置需求。

这种设计让 API 更加易用，把 SparkSession 管理的复杂度隐藏起来，同时仍能充分发挥分布式处理的能力。

### S3 配置

在使用 S3 数据时，GeoPandas API 使用 Spark 内置的 S3 支持，而不是 s3fs 等外部库。可通过 Spark 配置开启对公开 S3 桶的匿名访问：

```python
from sedona.spark import SedonaContext

# 公开 S3 桶的匿名访问
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.bucket.bucket-name.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)
```

需要鉴权访问 S3 时，请使用合适的 AWS 凭据提供者：

```python
# 使用 IAM 角色（在 EC2/EMR 上推荐）
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    )
    .getOrCreate()
)

# 使用 access key（不推荐用于生产环境）
config = (
    SedonaContext.builder()
    .config("spark.hadoop.fs.s3a.access.key", "your-access-key")
    .config("spark.hadoop.fs.s3a.secret.key", "your-secret-key")
    .getOrCreate()
)
```

## 基本用法

### 引入 API

不再直接导入 GeoPandas，而是从 Sedona 的 GeoPandas 模块导入：

```python
# 传统的 GeoPandas 导入
# import geopandas as gpd

# Sedona GeoPandas API 的导入
import sedona.spark.geopandas as gpd

# 或
from sedona.spark.geopandas import GeoDataFrame, read_parquet
```

### 读取数据

API 支持读取多种地理空间格式，包括来自云存储的 Parquet 文件。要以匿名凭据访问 S3，请配置 Spark 使用匿名 AWS 凭据：

```python
from sedona.spark import SedonaContext

# 配置 Spark 进行匿名 S3 访问
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.bucket.wherobots-examples.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)

# 直接从 S3 加载 GeoParquet 文件
s3_path = "s3://wherobots-examples/data/onboarding_1/nyc_buildings.parquet"
nyc_buildings = gpd.read_parquet(s3_path)

# 显示基本信息
print(f"Dataset shape: {nyc_buildings.shape}")
print(f"Columns: {nyc_buildings.columns.tolist()}")
nyc_buildings.head()
```

### 空间过滤

使用 `cx` 进行分布式坐标范围筛选；如需将相交的几何裁剪到掩膜边界，
可继续使用 `clip`：

```python
from shapely.geometry import box

# 中央公园的边界框
central_park_bbox = box(
    -73.973,
    40.764,  # 左下角（经度，纬度）
    -73.951,
    40.789,  # 右上角（经度，纬度）
)

# 筛选与坐标范围相交的要素
central_park_buildings = nyc_buildings.cx[
    -73.973:-73.951,
    40.764:40.789,
]

# 将筛选后的几何裁剪到精确的多边形边界
central_park_buildings = central_park_buildings.clip(central_park_bbox)

# 显示结果
print(
    central_park_buildings[["BUILD_ID", "PROP_ADDR", "height_val", "geometry"]].head()
)
```

**针对大规模数据的替代方案——使用空间连接：**

```python
# 用边界框创建一个 GeoDataFrame
bbox_gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[central_park_bbox], crs="EPSG:4326")

# 使用空间连接筛选边界框内的建筑
central_park_buildings = nyc_buildings.sjoin(bbox_gdf, predicate="intersects")
```

## 高级操作

### 空间连接

可使用与 GeoPandas 相同的语法执行空间连接：

```python
# 加载两份数据集
left_df = gpd.read_parquet("s3://bucket/left_data.parquet")
right_df = gpd.read_parquet("s3://bucket/right_data.parquet")

# 使用 distance 谓词的空间连接
result = left_df.sjoin(right_df, predicate="dwithin", distance=50)

# 其他空间谓词
intersects_result = left_df.sjoin(right_df, predicate="intersects")
contains_result = left_df.sjoin(right_df, predicate="contains")
```

### 坐标参考系操作

在不同坐标参考系（CRS）之间转换几何对象：

```python
# 设置初始 CRS
buildings = gpd.read_parquet("buildings.parquet")
buildings = buildings.set_crs("EPSG:4326")

# 转换为投影坐标系以便计算面积
buildings_projected = buildings.to_crs("EPSG:3857")

# 计算面积
buildings_projected["area"] = buildings_projected.geometry.area
```

### 几何运算

应用几何变换与分析：

```python
# 缓冲区操作
buffered = buildings.geometry.buffer(100)  # 100 米缓冲

# 几何属性
buildings["is_valid"] = buildings.geometry.is_valid
buildings["is_simple"] = buildings.geometry.is_simple
buildings["bounds"] = buildings.geometry.bounds

# 距离计算
from shapely.geometry import Point

reference_point = Point(-73.9857, 40.7484)  # 时代广场
buildings["distance_to_times_square"] = buildings.geometry.distance(reference_point)

# 面积与周长（需要使用投影 CRS）
buildings_projected = buildings.to_crs("EPSG:3857")  # Web Mercator
buildings_projected["area"] = buildings_projected.geometry.area
buildings_projected["perimeter"] = buildings_projected.geometry.length
```

## 性能考虑

### 何时仍使用传统 GeoPandas：

- 小数据集（< 1GB）
- 对本地数据的简单操作
- 需要完整的功能覆盖
- 单机处理已经足够

### 何时使用 Apache Sedona 的 GeoPandas API：

- 大规模数据集（> 1GB）
- 复杂的地理空间分析
- 需要分布式处理
- 数据保存在云存储（S3、HDFS 等）

## 已支持的操作

Apache Sedona 的 GeoPandas API 已实现最常用的 GeoSeries 与 GeoDataFrame 操作：

### 数据 I/O

- `read_parquet()` —— 读取 GeoParquet 文件
- `read_file()` —— 读取多种地理空间格式
- `to_parquet()` —— 写出为 Parquet 格式

### 空间操作

- `sjoin()` —— 多种谓词的空间连接
- `cx` —— 基于坐标范围的空间筛选
- `clip()` —— 使用标量、矩形或分布式掩膜裁剪几何
- `buffer()` —— 几何缓冲
- `distance()` —— 距离计算
- `intersects()`、`contains()`、`within()` —— 空间谓词
- `sindex` —— 空间索引（功能有限）

### CRS 操作

- `set_crs()` —— 设置坐标参考系
- `to_crs()` —— 在 CRS 之间转换
- `crs` —— 访问 CRS 信息

### 几何属性

- `area`、`length`、`bounds` —— 几何度量
- `is_valid`、`is_simple`、`is_empty` —— 几何校验
- `centroid`、`envelope`、`boundary` —— 几何属性
- `x`、`y`、`z`、`has_z` —— 坐标访问
- `total_bounds`、`estimate_utm_crs` —— 包围盒与 CRS 工具

### 空间运算

- `buffer()` —— 几何缓冲
- `distance()` —— 距离计算
- `intersects()`、`contains()`、`within()` —— 空间谓词
- `intersection()` —— 几何相交
- `make_valid()` —— 几何校验与修复
- `sample_points()` —— 使用原生分布式表达式按面积对多边形采样、按长度对线采样
- `cx` —— 基于坐标范围的空间筛选
- `clip()` —— 分布式几何裁剪
- `sindex` —— 空间索引（功能有限）

### 分布式几何聚合

- `GeoDataFrame.dissolve()` —— 对行进行分组，并使用 Sedona 原生分布式聚合
  对每组几何执行并集
- `sedona.spark.geopandas.tools.collect()` —— 将分布式 GeoSeries 聚合为一个
  几何，并在需要时使用同构多部件几何

```python
from sedona.spark.geopandas.tools import collect

by_region = buildings.dissolve(
    by="region",
    aggfunc={"population": "sum", "name": "first"},
)
all_building_parts = collect(buildings.geometry)
```

`dissolve` 支持 `unary` 并集方法，并会明确拒绝定点精度 `grid_size`、
`coverage` 与 `disjoint_subset` 并集。多个属性聚合会保留为 pandas-on-Spark
的两级 `MultiIndex`；GeoPandas 则把对应元组标签放在单级 object 索引中。
原生 `first`、`last` 与 `count` 聚合支持所有属性类型；`nunique` 支持数值、
布尔与字符串列；`min`、`max`、`sum`、`mean`、`median`、`std` 与 `var`
支持数值和布尔列。字符串 `sum` 与所有 `prod` 聚合都会被明确拒绝。可调用
聚合仅限 Python 内置的 `min`、`max`、`sum`，以及 NumPy 的 `amin`、
`amax`、`min`、`max`、`sum`、`mean`、`median`、`std` 与 `var`。
两个操作都在 Spark executor 上聚合输入行。`collect` 只会把聚合元数据和
该 API 所需的单个几何结果传输到 driver。

### 数据转换

- `to_geopandas()` —— 转换为传统 GeoPandas
- `GeoDataFrame.to_wkb()`、`GeoDataFrame.to_wkt()` —— 在分布式
  pandas-on-Spark DataFrame 中把所有几何列序列化为 WKB/WKT
- `points_from_xy()`、`GeoSeries.from_xy()` —— 通过坐标列创建分布式
  GeoSeries，且不收集分布式输入
- `geom_type` —— 获取几何类型

与 GeoPandas 不同，`GeoDataFrame.to_wkb()` 和 `GeoDataFrame.to_wkt()` 返回
惰性的 pandas-on-Spark DataFrame。仅在需要本地 pandas DataFrame 时，才对结果
调用 `.to_pandas()`。

`points_from_xy()` 会让 pandas-on-Spark 坐标 Series 保持分布式执行。本地
列表、NumPy 数组和 pandas 对象会先在 driver 上物化，因此大型坐标列应使用
分布式 Series。类似地，大型逐行 `sample_points()` 数量向量也应使用分布式
`size` Series。采样输出与逐行中间计算量会随请求的数量增长；当前线采样器会先
物化该行的采样点，再把它们收集为 MultiPoint。

## 完整工作流示例

```python
import sedona.spark.geopandas as gpd
from sedona.spark import SedonaContext

# 配置 Spark 进行匿名 S3 访问
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.bucket.wherobots-examples.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)

# 加载数据
DATA_DIR = "s3://wherobots-examples/data/geopandas_blog/"
overture_size = "1M"
postal_codes_path = DATA_DIR + "postal-code/"
overture_path = DATA_DIR + overture_size + "/" + "overture-buildings/"

postal_codes = gpd.read_parquet(postal_codes_path)
buildings = gpd.read_parquet(overture_path)

# 空间分析
buildings = buildings.set_crs("EPSG:4326")
buildings_projected = buildings.to_crs("EPSG:3857")

# 计算面积并过滤
buildings_projected["area"] = buildings_projected.geometry.area
large_buildings = buildings_projected[buildings_projected["area"] > 1000]

result = large_buildings.sjoin(postal_codes, predicate="intersects")

# 按邮政编码聚合
summary = (
    result.groupby("postal_code")
    .agg({"area": "sum", "BUILD_ID": "count"})
    .rename(columns={"BUILD_ID": "building_count"})
)

print(summary.head())
```

## 资源与贡献

完整且最新的 API 文档（包含方法签名、参数与示例）请参阅：

**📚 [GeoPandas API 文档](https://sedona.apache.org/latest/api/pydocs/sedona.spark.geopandas.html)**

Apache Sedona 的 GeoPandas API 是一个开源项目，欢迎参与贡献：可在 [GitHub issue tracker](https://github.com/apache/sedona/issues/2230) 报告 bug、提出新需求或贡献代码。更多贡献指南请参阅 [贡献者指南](../community/develop.md)。
