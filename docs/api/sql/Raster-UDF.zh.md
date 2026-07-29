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

## 栅格 UDF

Python UDF 可以接收栅格列作为输入，并返回普通的 Spark 值或一个新的栅格。在 UDF 内部，每个栅格单元都以
`SedonaRaster` 对象的形式出现，因此像素数据可以直接交给 NumPy、SciPy、scikit-learn、rasterio 或 Python
环境中任何其他库处理。

Python UDF 接收栅格**输入**从 `v1.6.0` 起支持；从 UDF **返回**栅格从 `v1.9.1` 起支持。

### 何时该用 UDF 而不是地图代数

[`RS_MapAlgebra`](Raster-map-algebra.md) 与 Python UDF 解决的问题有重叠，两者并不互相取代：

| | `RS_MapAlgebra` | Python UDF |
|---|---|---|
| 运行位置 | JVM；Jiffle 脚本会被编译为字节码 | Python worker；像素需要跨进程传输 |
| 表达方式 | 用 [Jiffle](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle) 语言编写逐像素脚本 | 在 NumPy 视图上编写整数组运算的 Python 代码 |
| 可用库 | 无 | 环境中安装的任意库 —— NumPy、SciPy、scikit-learn、rasterio |
| 输出 | 栅格 | 栅格，或任意 Spark 类型 |
| 输出的 NODATA | 由 `noDataValue` 参数显式指定 | 继承自输入 —— 参见[限制](#limits) |
| 可用范围 | SQL，因此所有语言绑定都能用 | 仅 Python |

如果运算是能用一小段表达式写完的逐像素算术，就用 `RS_MapAlgebra`：此时数据不离开 JVM，代价更低。如果需要用到
某个库、算法需要同时读取多个像素（卷积、分类、形态学运算），或者结果根本不是栅格，就用 UDF。

对于较重的负载，[`pandas_udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html)
可以把 Python 往返开销摊薄到一批行上。它接收的是序列化后的字节，而不是 `SedonaRaster` 对象，因此需要自行反序列化：

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from sedona.spark.raster import raster_serde


@pandas_udf(DoubleType())
def mean_batch(s: pd.Series) -> pd.Series:
    def one(buf):
        with raster_serde.deserialize(buf) as raster:
            return float(raster.as_numpy().mean())

    return s.apply(one)


df.select(mean_batch(col("rast")).alias("mean"))
```

### 读取像素数据

`SedonaRaster` 提供三种方式访问同一份像素数据：

```python
raster.as_numpy()  # CHW 顺序的 ndarray（波段、高、宽）
raster.as_numpy_masked()  # 同上，但 NODATA 会被替换为 NaN
raster.as_rasterio()  # rasterio.DatasetReader（只读）
```

元数据以属性形式提供 —— `raster.width`、`raster.height`、`raster.crs_wkt`、`raster.affine_trans`
以及 `raster.bands_meta`。

### 栅格 → 标量

任意 Spark 返回类型均可。在 `@udf` 装饰器上声明返回类型，然后像使用普通函数一样使用该 UDF：

```python
from pyspark.sql.functions import col, udf


@udf(returnType="double")
def mean_udf(raster):
    return float(raster.as_numpy().mean())


df.select(mean_udf(col("rast")).alias("mean"))
```

同一个 UDF 也可以注册成具名函数供 SQL 使用。直接传入已装饰的 UDF 即可 —— 返回类型已经附带在上面：

```python
sedona.udf.register("mean_udf", mean_udf)
sedona.sql("SELECT mean_udf(rast) AS mean FROM raster_table")
```

### 栅格 → 栅格

若要返回栅格，把返回类型声明为 `RasterType()`，并用 `SedonaRaster.with_bands()` 构造结果。该方法接收一个存放
新像素值的 NumPy 数组，并沿用源栅格的 CRS、仿射变换以及其他空间元数据：

```python
import numpy as np
from pyspark.sql.functions import col, udf

from sedona.spark.sql.types import RasterType


@udf(returnType=RasterType())
def mask_udf(raster):
    band1 = raster.as_numpy()[0]
    mask = (band1 < 1400).astype(np.float32)
    return raster.with_bands(mask)


df.select(mask_udf(col("rast")).alias("mask_rast"))
```

`with_bands()` 接受 CHW 顺序（波段 × 高 × 宽）的数组，单波段结果也可以简写为 HW 顺序（高 × 宽）。波段数与
dtype 都可以与输入不同 —— 上面的例子就把一个多波段场景变成了单个 `float32` 波段。返回的 `SedonaRaster`
会自动序列化回 JVM 端，因此输出列就是一个普通栅格列，所有 `RS_` 函数都能接收。

### NDVI：地图代数写法与 UDF 写法

同一个计算的两种写法。用 `RS_MapAlgebra`：

```sql
SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') AS ndvi
FROM raster_table
```

用 UDF：

```python
@udf(returnType=RasterType())
def ndvi(raster):
    a = raster.as_numpy().astype(np.float64)
    red, nir = a[0], a[3]
    return raster.with_bands((nir - red) / (nir + red + 1e-10))


df.select(ndvi(col("rast")).alias("ndvi"))
```

两者都会在输入的网格上产生一个单波段 `double` 栅格。注意波段下标的差异：Jiffle 的 `rast[0]` 和 NumPy 的
`a[0]` 都表示第一个波段，但消费结果的 SQL 函数（`RS_BandAsArray`、`RS_BandNoDataValue` 等）的波段编号从 1 开始。

### 双栅格

UDF 可以接收任意多个栅格列，这覆盖了 `RS_MapAlgebra` 五参数形式的用途。元数据由你调用 `with_bands()` 的那个
栅格提供，因此要选结果所属网格对应的那一个：

```python
@udf(returnType=RasterType())
def delta(after, before):
    diff = after.as_numpy()[0] - before.as_numpy()[0]
    return after.with_bands(diff)


df.select(delta(col("after"), col("before")).alias("delta"))
```

两个栅格必须已经位于同一网格上 —— 参见[限制](#limits)。如果不是，先用
[`RS_ReprojectMatch`](Raster-Operators/RS_ReprojectMatch.md) 对齐。

### 在 UDF 中使用 rasterio

`as_rasterio()` 返回一个 `rasterio.DatasetReader`，它复用同一份像素缓冲区，因此 rasterio 与 GDAL 的算法可以
直接作用于栅格列。该 dataset 是只读的；若要返回栅格，需要把得到的数组再交给 `with_bands()`：

```python
import rasterio.fill


@udf(returnType=RasterType())
def fill_udf(raster):
    with raster.as_rasterio() as src:
        filled = rasterio.fill.fillnodata(src.read(1), mask=src.read_masks(1))
    return raster.with_bands(filled)


df.select(fill_udf(col("rast")).alias("filled"))
```

这一模式适用于任何不改变网格的 rasterio 运算 —— `fillnodata`、`sieve`、在已有网格上栅格化等。改变 CRS、
分辨率或范围的运算无法这样返回，参见[限制](#limits)。

{% raw %}

### 限制 {#limits}

{% endraw %}

#### 输出必须位于输入的网格上

`with_bands()` 要求新数组的高、宽与源栅格一致，并且会沿用源栅格的 CRS 与仿射变换。传入形状不同的数组会抛出：

```
ValueError: Spatial dimensions (2, 2) don't match raster (3, 4)
```

没有办法返回 CRS、像元大小或范围不同的栅格，因此重投影、warp、重采样以及裁剪到新范围都无法在 Python UDF
内部完成。请改为在 UDF 前后使用 [`RS_Resample`](Raster-Operators/RS_Resample.md)、
[`RS_ReprojectMatch`](Raster-Operators/RS_ReprojectMatch.md) 或
[`RS_Clip`](Raster-Operators/RS_Clip.md)，或者用 [Scala](#scala-and-java) 编写 UDF —— 该限制对 Scala 不适用。

`RS_MapAlgebra` 同样是保持网格不变的，所以这一点并不是两者之间的差异。

#### NODATA 继承自输入

`RS_MapAlgebra` 可以通过 `noDataValue` 显式指定 NODATA，而 UDF 无法设置它所返回栅格的 NODATA 值。输出的每个
波段都会从相同位置的输入波段继承 NODATA；超出输入波段数的新增波段则继承输入**最后一个**波段的 NODATA。

这一点需要留意，因为掩膜的含义通常与它所来自的场景不同。如果输入的第一个波段的 NODATA 是 `0`，那么上面
`mask_udf` 返回的 0/1 掩膜的 NODATA 也是 `0` —— 于是掩膜中所有未置位的像素对那些遵循 NODATA 的函数而言都是
不可见的，下面两个计数会不一致：

```python
df.select(mask_udf(col("rast")).alias("mask_rast")).selectExpr(
    "RS_Count(mask_rast, 1, true) AS set_pixels",  # 零值被当作 NODATA 跳过
    "RS_Count(mask_rast, 1, false) AS all_pixels",
)
```

只要输出的 NODATA 与输入不同，就在 UDF 之后显式设置：

```python
df.select(mask_udf(col("rast")).alias("mask_rast")).selectExpr(
    "RS_SetBandNoDataValue(mask_rast, 1, -9999) AS mask_rast"
)
```

!!!note
    对于超出输入波段数的新增波段，Python 侧的 `raster.bands_meta[i].nodata` 报告 `NaN`，而 JVM 侧的
    [`RS_BandNoDataValue`](Raster-Band-Accessors/RS_BandNoDataValue.md) 报告继承来的值。把一个第 4 波段
    NODATA 为 `-1` 的 4 波段栅格扩展到 8 个波段，Python 得到
    `[nan, nan, nan, -1.0, nan, nan, nan, nan]`，而 SQL 对第 4 到第 8 波段都返回 `-1.0`。请以 SQL 函数为准。

#### 并非所有 NumPy dtype 都能保留

传给 `with_bands()` 的数组会被映射到 Java 的 data buffer 类型，其中有三种情况需要注意：

| NumPy dtype | 结果 |
|---|---|
| `uint8`、`int16`、`uint16`、`int32`、`float32`、`float64` | 直接映射 |
| `uint32` | 存储为有符号 32 位；超过 2<sup>31</sup>−1 的值会静默溢出 |
| `int8` | 存储为无符号字节；负值会被重新解释 —— `-2` 读回来是 `254` |
| `int64`、`uint64` | 抛出 `ValueError` 拒绝 |

不确定时，转成 `float64`。

{% raw %}

### Scala 与 Java {#scala-and-java}

{% endraw %}

在 JVM 端栅格表示为 GeoTools 的 `GridCoverage2D`，并且 `RasterUDT` 已经为该类注册，因此 Scala UDF 可以
直接接收和返回栅格列，无需额外配置：

```scala
import org.apache.spark.sql.functions.{col, udf}
import org.geotools.coverage.grid.GridCoverage2D

// 栅格 → 标量
val numBands = udf((raster: GridCoverage2D) => raster.getNumSampleDimensions)
df.select(numBands(col("rast")).alias("num_bands"))

// 栅格 → 栅格
val process = udf((raster: GridCoverage2D) => transform(raster))
df.select(process(col("rast")).alias("rast"))
```

上文提到的便利层 —— `as_numpy()`、`with_bands()` 以及各元数据访问器 —— 仅在 Python 中可用。在 Scala 中你直接
面向 GeoTools API 编程。

这也意味着上面两条限制都不适用。Scala UDF 自行构造它返回的 `GridCoverage2D`，因此可以完全掌控波段数、CRS、
像元大小、范围与 NODATA —— 它可以接收一个 EPSG:3857 的 4×3 栅格，返回一个 EPSG:4326 的 7×5 栅格。如果你需要
一个会改变网格的 UDF，目前应该用 Scala 来写。

### 延伸阅读

* [地图代数](Raster-map-algebra.md) —— `RS_MapAlgebra` 方案及其 Jiffle 脚本语法
* [在 Python 中处理栅格 DataFrame](../../tutorial/raster.md#working-with-raster-dataframes-in-python) —— 把栅格收集到 driver 端
* [栅格函数](Raster-Functions.md) —— 完整的 `RS_` 算子清单
