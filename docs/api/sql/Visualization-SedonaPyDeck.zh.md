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

SedonaPyDeck 提供了一组 API，便于在 Jupyter notebook/lab 环境中对地理空间数据进行快速、交互式的可视化。

要开始使用 SedonaPyDeck，只需通过下面的方式导入 Sedona：

```python
from sedona.spark import *
```

也可以使用以下方式导入：

```python
from sedona.spark import SedonaPyDeck
```

!!!Note
    关于可选参数的更多说明，请访问 [PyDeck 文档](https://deckgl.readthedocs.io/en/latest/deck.html)。

    当用户为 `map_style` 选择 'salellite' 选项时，SedonaPyDeck 默认假定地图提供方为 Mapbox。

下面是 SedonaPyDeck 暴露的所有 API 的详细说明：

### **几何对象地图（Geometry Map）**

```python
def create_geometry_map(
    df,
    fill_color="[85, 183, 177, 255]",
    line_color="[85, 183, 177, 255]",
    elevation_col=0,
    initial_view_state=None,
    map_style=None,
    map_provider=None,
    api_keys=None,
    stroked=True,
): ...
```

参数 `fill_color` 可以传入一组 RGB/RGBA 值的列表，也可以传入一个根据某列值生成 RGB/RGBA 取值的字符串，用于为地图中的多边形或点几何对象着色。

参数 `line_color` 可以传入一组 RGB/RGBA 值的列表，也可以传入一个根据某列值生成 RGB/RGBA 取值的字符串，用于为地图中的线几何对象着色。

参数 `elevation_col` 可以传入一个静态高程值，也可以像 `fill_color` 一样基于列值给出高程；仅对地图中的多边形几何对象生效。

参数 `stroked` 决定是否在多边形和点周围绘制描边，接受布尔值。更多信息请参阅 [deck.gl 的相关文档](https://deck.gl/docs/api-reference/layers/geojson-layer#:~:text=%27circle%27.-,stroked,-(boolean%2C%20optional))。

可选地，可以传入 `initial_view_state`、`map_style`、`map_provider`、`api_keys` 等参数，按用户喜好配置地图。
关于参数及其默认值的更多信息，可以在 PyDeck 网站，以及 deck.gl 的[相关页面](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/layers/geojson-layer.md)中找到。

### **分级统计地图（Choropleth Map）**

```python
def create_choropleth_map(
    df,
    fill_color=None,
    plot_col=None,
    initial_view_state=None,
    map_style=None,
    map_provider=None,
    api_keys=None,
    elevation_col=0,
    stroked=True,
): ...
```

参数 `fill_color` 可以传入一组 RGB/RGBA 值的列表，也可以传入一个根据某列值生成 RGB/RGBA 取值的字符串。

参数 `stroked` 决定是否在多边形和点周围绘制描边，接受布尔值。更多信息请参阅 [deck.gl 的相关文档](https://deck.gl/docs/api-reference/layers/geojson-layer#:~:text=%27circle%27.-,stroked,-(boolean%2C%20optional))。

例如，下面这些都是 fill_color 的合法取值：

```python
fill_color = [255, 12, 250]
fill_color = [0, 12, 250, 255]
fill_color = (
    "[0, 12, 240, AirportCount * 10]"  ## AirportCount is a column in the passed df
)
```

除传入 `fill_color` 参数外，也可以传入 'plot_col'，指定用于决定分级统计的列。
随后 SedonaPyDeck 会根据该列的取值创建一个默认的配色方案。

参数 `elevation_col` 可以传入数值，或包含列名（及其上的运算）的字符串值，用于为所绘制的多边形（如有）设置 3D 高程。

可选地，可以传入 `initial_view_state`、`map_style`、`map_provider`、`api_keys` 等参数，按用户喜好配置地图。
关于参数及其默认值的更多信息可以在 PyDeck 网站上找到。

### **散点图（Scatterplot）**

```python
def create_scatterplot_map(
    df,
    fill_color="[255, 140, 0]",
    radius_col=1,
    radius_min_pixels=1,
    radius_max_pixels=10,
    radius_scale=1,
    initial_view_state=None,
    map_style=None,
    map_provider=None,
    api_keys=None,
): ...
```

参数 `fill_color` 可以传入一组 RGB/RGBA 值的列表，也可以传入一个根据某列值生成 RGB/RGBA 取值的字符串。

参数 `radius_col` 可以传入数值，或包含列名上各种运算的字符串值，用于指定绘制点的半径。

参数 `radius_min_pixels` 可以传入数值，用于设定以像素为单位的最小半径。可以防止在缩小时绘制的圆变得过小。

参数 `radius_max_pixels` 可以传入数值，用于设定以像素为单位的最大半径。可以防止在放大时圆变得过大。

参数 `radius_scale` 可以传入数值，作用于所有点的全局半径乘数。

可选地，可以传入 `initial_view_state`、`map_style`、`map_provider`、`api_keys` 等参数，按用户喜好配置地图。
关于参数及其默认值的更多信息，可以在 PyDeck 网站，以及 deck.gl 的[相关页面](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/layers/scatterplot-layer.md)中找到。

### **热力图（Heatmap）**

```python
def create_heatmap(
    df,
    color_range=None,
    weight=1,
    aggregation="SUM",
    initial_view_state=None,
    map_style=None,
    map_provider=None,
    api_keys=None,
): ...
```

参数 `color_range` 可以可选地传入一组 RGB 值的列表，SedonaPyDeck 默认使用 `6-class YlOrRd` 作为 color_range。
更多示例可参见 [colorbrewer](https://colorbrewer2.org/#type=sequential&scheme=YlOrRd&n=6)。

参数 `weight` 可以传入数值，或包含列名及其上运算的字符串值，用于在绘制热力图时确定每个点的权重。
默认情况下，SedonaPyDeck 将每个点的权重设为 1。

参数 `aggregation` 可用于定义热力图在缩小（聚合到更低分辨率）时所采用的聚合策略。
可选 "MEAN" 或 "SUM"。SedonaPyDeck 默认使用 "MEAN" 作为聚合策略。

可选地，可以传入 `initial_view_state`、`map_style`、`map_provider`、`api_keys` 等参数，按用户喜好配置地图。
关于参数及其默认值的更多信息，可以在 PyDeck 网站，以及 deck.gl 的[相关页面](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/aggregation-layers/heatmap-layer.md)中找到。
