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

## 地图代数（Map Algebra）

地图代数是一种使用数学表达式来执行栅格计算的方法。表达式可以是简单的算术运算，也可以是多种运算的复杂组合。该表达式可以作用在单个栅格波段，也可以作用在多个栅格波段上。表达式的结果是一个新的栅格。

!!!tip
    要在 Jupyter notebook 中直观地查看地图代数运算的结果，可以使用 `SedonaUtils.display_image(df)`。它会自动识别栅格列并以图像形式渲染。详情请参阅 [Raster Output 文档](Raster-Functions.md#raster-output)。

Apache Sedona 提供了两种执行地图代数运算的方法：

1. 使用 `RS_MapAlgebra` 函数。
2. 使用 `RS_BandAsArray` 及基于数组的地图代数函数，例如 `RS_Add`、`RS_Multiply` 等。

一般来说，`RS_MapAlgebra` 函数更灵活，可用于执行更复杂的运算。该函数接收 3 到 4 个参数：

```sql
RS_MapAlgebra(rast: Raster, pixelType: String, script: String, [noDataValue: Double])
```

* `rast`：要应用地图代数表达式的栅格。
* `pixelType`：输出栅格的数据类型。可选值为 `D`（double）、`F`（float）、`I`（integer）、`S`（short）、`US`（unsigned short）或 `B`（byte）。如果指定为 `NULL`，则输出栅格与输入栅格使用相同的数据类型。
* `script`：地图代数脚本。[格式的更多说明请参见此处。](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle)
* `noDataValue`：（可选）输出栅格的 nodata 值。

从 `v1.5.1` 版本起，`RS_MapAlgebra` 函数支持两个栅格列输入，并支持多波段栅格。该函数接受 5 个参数：

```sql
RS_MapAlgebra(rast0: Raster, rast1: Raster, pixelType: String, script: String, noDataValue: Double)
```

* `rast0`：第一个要应用地图代数表达式的栅格。
* `rast1`：第二个要应用地图代数表达式的栅格。
* `pixelType`：输出栅格的数据类型。可选值为 `D`（double）、`F`（float）、`I`（integer）、`S`（short）、`US`（unsigned short）或 `B`（byte）。如果指定为 `NULL`，则输出栅格与输入栅格使用相同的数据类型。
* `script`：地图代数脚本。[格式的更多说明请参见此处。](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle)
* `noDataValue`：（非可选）输出栅格的 nodata 值，允许传 `null`。

双栅格输入的 `RS_MapAlgebra` 的 Spark SQL 示例：

```sql
RS_MapAlgebra(rast0, rast1, 'D', 'out = rast0[0] * 0.5 + rast1[0] * 0.5;', null)
```

`RS_MapAlgebra` 的性能也很不错，因为它由 [Jiffle](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle) 提供支持，可以被编译为 Java 字节码以
执行。下面我们将演示如何用这两种方式实现常见的地图代数运算。

!!!Note
    `RS_MapAlgebra` 函数可以将输出栅格转换为 `pixelType` 指定的另一种数据类型：

    - 如果 `pixelType` 比输入栅格的数据类型更小，会执行窄化转换，可能导致数据丢失。

    - 如果 `pixelType` 更大，会执行扩宽转换，从而保留数据精度。

    - 如果 `pixelType` 与输入栅格的数据类型一致，则不进行任何转换。

    这使得用户可以控制输出像素的数据类型。在强制转换为较小类型时，应注意可能带来的精度影响。

### NDVI

归一化植被指数（NDVI, Normalized Difference Vegetation Index）是一种简单的图形化指标，可用于分析遥感测量数据（通常但并非必然来自太空平台），评估目标区域是否含有活的绿色植被。NDVI 已成为判断给定区域是否含有活绿色植被的事实标准。NDVI 的计算公式如下：

```
NDVI = (NIR - Red) / (NIR + Red)
```

其中 NIR 表示近红外波段，Red 表示红光波段。

假设我们有若干个 4 波段的栅格，分别对应红、绿、蓝、近红外四个波段。我们希望为每个栅格计算 NDVI。可以使用 `RS_MapAlgebra` 函数来实现：

```sql
SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') as ndvi FROM raster_table
```

这里的 Jiffle 脚本是 `out = (rast[3] - rast[0]) / (rast[3] + rast[0]);`。`rast` 变量始终绑定到输入栅格，
而 `out` 变量绑定到输出栅格。Jiffle 会迭代输入栅格的每个像素，并为每个像素执行该脚本。`rast[3]` 与 `rast[0]`
分别指代当前像素中近红外波段和红光波段的取值，`out` 变量则是当前的输出像素值。

`RS_MapAlgebra` 函数的结果是一个单波段栅格。由于我们将 `pixelType` 指定为 `D`，该波段的类型为 double。

也可以使用基于数组的地图代数函数实现相同的 NDVI 计算：

```sql
SELECT RS_Divide(
        RS_Subtract(RS_BandAsArray(rast, 1), RS_BandAsArray(rast, 4)),
        RS_Add(RS_BandAsArray(rast, 1), RS_BandAsArray(rast, 4))) as ndvi FROM raster_table
```

`RS_BandAsArray` 函数会将输入栅格中指定的波段提取为一个 double 数组，而 `RS_Add`、`RS_Subtract` 与 `RS_Divide` 函数则在数组上执行算术运算。使用基于数组的地图代数函数的代码相对更冗长。不过，还有一个 `RS_NormalizedDifference` 函数可以更简洁地计算 NDVI：

```sql
SELECT RS_NormalizedDifference(RS_BandAsArray(rast, 1), RS_BandAsArray(rast, 4)) as ndvi FROM raster_table
```

基于数组的地图代数函数的结果是一个 double 数组。用户可以使用 `RS_AddBandFromArray` 将该数组作为新的波段添加到栅格中。

### AWEI

自动水体提取指数（AWEI, Automated Water Extraction Index）是一种光谱指数，可用于从遥感影像中提取水体。AWEI 的计算公式如下：

```
AWEI = 4 * (Green - SWIR2) - (0.25 * NIR + 2.75 * SWIR1)
```

使用 `RS_MapAlgebra` 可以很容易地实现 AWEI：

```sql
-- Assume that the raster includes all 13 Sentinel-2 bands
SELECT RS_MapAlgebra(rast, 'D', 'out = 4 * (rast[2] - rast[11]) - (0.25 * rast[7] + 2.75 * rast[12]);') as awei FROM raster_table
```

也可以使用基于数组的地图代数函数实现同样的 AWEI 计算，代码看起来更冗长：

```sql
SELECT RS_Subtract(
    RS_Add(RS_MultiplyFactor(band_nir, 0.25), RS_MultiplyFactor(band_swir1, 2.75)),
    RS_MultiplyFactor(RS_Subtract(band_swir2, band_green), 4)) as awei
FROM (
SELECT RS_BandAsArray(rast, 3) AS band_green,
       RS_BandAsArray(rast, 12) AS band_swir2,
       RS_BandAsArray(rast, 13) AS band_swir1,
       RS_BandAsArray(rast, 8) AS band_nir
FROM raster_table) t
```

### 延伸阅读

* [Jiffle language summary](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle---language-summary)
* [Raster operators](Raster-Functions.md#raster-operators)
