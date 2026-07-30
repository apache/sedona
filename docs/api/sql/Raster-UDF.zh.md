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

在 Sedona 中处理栅格，推荐使用 UDF。[`RS_MapAlgebra`](Raster-map-algebra.md) 自 `v1.9.1` 起弃用，并将在未来
版本中移除；[下面的 NDVI 示例](#ndvi-as-map-algebra-and-as-a-udf)给出了两种写法的对照，便于迁移。

{% raw %}

### 读取像素数据 {#reading-pixel-data}

{% endraw %}

`SedonaRaster` 提供三种方式访问同一份像素数据：

```python
raster.as_numpy()  # CHW 顺序的 ndarray（波段、高、宽）
raster.as_numpy_masked()  # 普通 ndarray，但 NODATA 像素会被替换为 NaN
raster.as_rasterio()  # 只读 rasterio.DatasetReader；不附带 NODATA
```

元数据以属性形式提供 —— `raster.width`、`raster.height`、`raster.crs_wkt`、`raster.affine_trans`
以及 `raster.bands_meta`。Python 波段索引 `i` 的权威 NODATA 声明是
`raster.bands_meta[i].nodata`。索引 `0` 对应两个 NumPy 数组中的通道 `0`，也对应 rasterio 和
Sedona SQL 函数中的波段 `1`。元数据值为 `NaN` 表示该波段没有声明 NODATA。

这三种访问方式返回的结果并不会都保留该声明：

| 访问方式 | NODATA 行为 |
| --- | --- |
| `as_numpy()` | `ndarray` 本身不携带 NODATA 元数据。像素仍保留原始哨兵值；请从 `raster.bands_meta` 读取声明。 |
| `as_numpy_masked()` | 返回普通 `ndarray`，而不是 `numpy.ma.MaskedArray`。NODATA 像素会变为 `NaN`，但结果中不附带原始哨兵值；因此整数数据可能被提升为浮点 dtype。 |
| `as_rasterio()` | reader 不携带 Sedona 的 NODATA 元数据：`src.nodata` 为 `None`，`src.read_masks()` 会把所有像素报告为有效。请继续以 `raster.bands_meta` 为准，并将值或掩码显式传给 rasterio。 |

!!!warning
    `as_numpy()` 会把 NODATA 像素按原始哨兵值返回，算术与比较会把空洞当作普通数字 —— 例如 `band < 1400`
    这样的阈值判断会把 `-9999` 的空洞也归为陆地。只要输入可能携带 NODATA，就应通过 `as_numpy_masked()`
    读取，并在输出时重新标记空洞；具体写法见[双栅格](#two-rasters)。本页中使用 `as_numpy()` 的示例均假定
    输入不含空洞。

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

#### 设置输出的 NODATA

默认情况下，输出的每个波段都会从相同位置的输入波段继承 NODATA，超出输入波段数的波段则继承输入最后一个波段的
NODATA。对派生栅格来说这通常并不合适，因为输出的含义与它所来自的场景不同：如果某个波段的 NODATA 是 `0`，那么
由它得到的 0/1 掩膜中所有未置位的像素，都会被
[`RS_ZonalStats`](Raster-Band-Accessors/RS_ZonalStats.md)、
[`RS_Count`](Raster-Band-Accessors/RS_Count.md) 以及其他遵循 NODATA 的函数当作 NODATA 跳过。

用 `nodata=` 明确表达输出的含义。它等价于 `RS_MapAlgebra` 的 `noDataValue` 参数：

```python
NODATA = -9999.0


@udf(returnType=RasterType())
def mask_udf(raster):
    band1 = raster.as_numpy_masked()[0]  # 输入为 NODATA 处为 NaN
    mask = (band1 < 1400).astype(np.float32)
    return raster.with_bands(np.where(np.isnan(band1), NODATA, mask), nodata=NODATA)
```

传入标量会作用于所有输出波段；传入序列则可逐个设置，每个波段一项。若某个波段不应有 NODATA，用
`float("nan")`。

```python
return raster.with_bands(stacked, nodata=[-9999.0, float("nan")])
```

如果输出的 dtype 无法表示继承来的值 —— 比如把 NODATA 为 `-9999` 的 float64 场景收窄为 `uint8` ——
`with_bands()` 会直接抛错，而不是产出任何像素都永远无法匹配的元数据。这种情况请显式传入 `nodata=`，
或在转换 dtype 之前先清理掉空洞。

!!!note
    `nodata=` 是 `v1.9.1` 新增的。在此之前该值只能继承，需要事后用
    [`RS_SetBandNoDataValue`](Raster-Operators/RS_SetBandNoDataValue.md) 修正 —— 该方式目前依然可用。

{% raw %}

### NDVI：地图代数写法与 UDF 写法 {#ndvi-as-map-algebra-and-as-a-udf}

{% endraw %}

同一个计算的两种写法。用 `RS_MapAlgebra`：

```sql
SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') AS ndvi
FROM raster_table
```

用 UDF：

```python
@udf(returnType=RasterType())
def ndvi(raster):
    # 和上面的 Jiffle 脚本一样读取原始值。如果场景携带 NODATA，
    # 请改用 as_numpy_masked() 并重新标记空洞 —— 见下文"双栅格"。
    a = raster.as_numpy().astype(np.float64)
    red, nir = a[0], a[3]
    return raster.with_bands((nir - red) / (nir + red + 1e-10))


df.select(ndvi(col("rast")).alias("ndvi"))
```

两者都会在输入的网格上产生一个单波段 `double` 栅格。注意波段下标的差异：Jiffle 的 `rast[0]` 和 NumPy 的
`a[0]` 都表示第一个波段，但消费结果的 SQL 函数（`RS_BandAsArray`、`RS_BandNoDataValue` 等）的波段编号从 1 开始。

{% raw %}

### 双栅格 {#two-rasters}

{% endraw %}

UDF 可以接收任意多个栅格列，这覆盖了 `RS_MapAlgebra` 五参数形式的用途。元数据由你调用 `with_bands()` 的那个
栅格提供，因此要选结果所属网格对应的那一个：

```python
NODATA = -9999.0


@udf(returnType=RasterType())
def delta(after, before):
    # as_numpy_masked() 会把 NODATA 替换为 NaN，因此无效像素在参与运算后仍然保持无效，
    # 而不会把哨兵值带进计算结果。
    diff = after.as_numpy_masked()[0] - before.as_numpy_masked()[0]
    return after.with_bands(np.where(np.isnan(diff), NODATA, diff), nodata=NODATA)


df.select(delta(col("after"), col("before")).alias("delta"))
```

!!!warning
    只要可能携带 NODATA 的栅格参与算术或比较 —— 无论一个输入还是多个 —— 就应使用
    [`as_numpy_masked()`](#reading-pixel-data) 而不是 `as_numpy()`。`as_numpy()` 返回的是原始的 NODATA 哨兵值：某个输入上的空洞会变成一个很大的虚假差值，而两个
    输入上同时存在的空洞则会相互抵消、得到一个看似合理的 0。`nodata=` 只是给输出打标签，并不会标记哪些像素
    无效，因此还需要像上面的 `np.where` 那样把哨兵值真正写进数组。

两个栅格必须已经位于同一网格上 —— 参见[限制](#limits)。如果不是，先用
[`RS_ReprojectMatch`](Raster-Operators/RS_ReprojectMatch.md) 对齐。

### 在 UDF 中使用 rasterio

`as_rasterio()` 返回一个 `rasterio.DatasetReader`，它复用同一份像素缓冲区，因此 rasterio 与 GDAL 的算法可以
直接作用于栅格列。该 dataset 是只读的；若要返回栅格，需要把得到的数组再交给 `with_bands()`：

```python
import rasterio.fill


@udf(returnType=RasterType())
def fill_udf(raster):
    # NODATA 必须来自 SedonaRaster，而不是 GDAL dataset —— 见下面的提示。
    nodata = raster.bands_meta[0].nodata
    valid = ~np.isnan(raster.as_numpy_masked()[0])
    with raster.as_rasterio() as src:
        filled = rasterio.fill.fillnodata(src.read(1), mask=valid.astype(np.uint8))
    # fillnodata 只在距有效数据 max_search_distance（默认 100 像素）范围内插值 ——
    # 空洞更深处的像素仍保留哨兵值。保留 NODATA 声明可以让这些像素继续保持无效。
    # 只有当确定所有空洞都足够小、能被完全填充时，才改用 nodata=float("nan")。
    return raster.with_bands(filled, nodata=nodata)


df.select(fill_udf(col("rast")).alias("filled"))
```

!!!warning
    `as_rasterio()` 返回的 dataset **不会**携带栅格的 NODATA 值：`src.nodata` 始终为 `None`，
    `src.read_masks()` 也会把所有像素都报告为有效。因此，任何依据 dataset 自身 NODATA 来决定行为的 rasterio
    调用，都会把无效区域当成正常数据。请改为从 `SedonaRaster` 获取该值 —— 用 `raster.bands_meta[i].nodata`，
    或用会把无效值替换为 `NaN` 的 `raster.as_numpy_masked()` —— 并像上面的 `mask=` 参数那样显式传给 rasterio。

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

#### 并非所有 NumPy dtype 都能保留

传给 `with_bands()` 的数组会被映射到 Java 的 data buffer 类型，其中有三种情况需要注意：

| NumPy dtype | 结果 |
|---|---|
| `uint8`、`int16`、`uint16`、`int32`、`float32`、`float64` | 直接映射 |
| `uint32` | 存储为有符号 32 位；超过 2<sup>31</sup>−1 的值会静默溢出 |
| `int8` | 存储为无符号字节；负值会被重新解释 —— `-2` 读回来是 `254` |
| `int64`、`uint64` | 抛出 `ValueError` 拒绝 |

不确定时，转成 `float64`。

`nodata=` 的取值遵循同样的存储规则：在 `int8` 波段上，`nodata=-2` 会以 `254` 存储 —— 并由
[`RS_BandNoDataValue`](Raster-Band-Accessors/RS_BandNoDataValue.md) 报告为 `254`，与像素一致；在 `uint32`
波段上，超过 2<sup>31</sup>−1 的值会以其带符号的重解释形式报告；在 `float32` 波段上，该值会被舍入到最接近的
float32，因为像素本身持有的就是 float32。

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
