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

SedonaKepler 提供了一组 API，便于在 Jupyter notebook/lab 环境中对地理空间数据进行快速、交互式的可视化。

要开始使用 SedonaKepler，只需通过下面的方式导入 Sedona：

```python
from sedona.spark import *
```

也可以使用以下方式导入：

```python
from sedona.spark import SedonaKepler
```

下面是 SedonaKepler 暴露的所有 API 的详细说明：

### **使用 SedonaKepler.create_map 创建地图对象**

SedonaKepler 暴露的 create_map API 的签名如下：

```python
def create_map(
    df: SedonaDataFrame = None, name: str = "unnamed", config: dict = None
) -> map: ...
```

参数 'name' 用于在地图对象中关联传入的 SedonaDataFrame，应用到该地图上的任何配置都会与该名称绑定。建议为 dataframe 传入唯一的标识符。

如果没有传入 SedonaDataFrame 对象，会返回一个空地图（如果传入了 config 则会应用该 config）。之后可以通过 `add_df` 方法添加 SedonaDataFrame。

也可以可选地传入一个 map config，对地图进行预设的定制。

!!!Note
    map config 是按显示的 SedonaDataFrame 的名称来引用每一项定制配置的，如果名称不匹配，则该 config 不会被应用到地图对象上。

!!! abstract "示例用法（参考自 Sedona Jupyter 示例）"

	=== "Python"
		```python
		map = SedonaKepler.create_map(df=groupedresult, name="AirportCount")
		map
		```

### **使用 SedonaKepler.add_df 向地图对象添加 SedonaDataFrame**

SedonaKepler 暴露的 add_df API 的签名如下：

```python
def add_df(map, df: SedonaDataFrame, name: str = "unnamed"): ...
```

该 API 可用于向已经创建的地图对象添加一个 SedonaDataFrame。传入的 map 对象会被直接修改，不返回任何值。

参数 name 的约束条件与 'create_map' 相同。

!!!Tip
    可以通过该方法向一个地图对象添加多个 dataframe，从而在同一张地图上一起可视化。

!!! abstract "示例用法（参考自 Sedona Jupyter 示例）"
    === "Python"
    ```python
    map = SedonaKepler.create_map()
    SedonaKepler.add_df(map, groupedresult, name="AirportCount")
    map
    ```

### **通过地图设置配置**

由 SedonaKepler 创建的地图对象在被访问渲染时，会附带一个 config 面板，可用于自定义地图

### **保存与设置 config**

可以通过访问地图对象的 'config' 属性（如 `map.config`）来获取其当前 config。如果每次都需要渲染完全相同的地图，可以将该 config 保存下来以备后续使用，或在不同 notebook 之间复用。

!!!Note
    map config 是按 dataframe 的名称来引用每一项已应用的定制配置的，因此它只在使用相同 dataframe 名称的地图上生效。
    更多细节请参见 keplerGl 文档：[这里](https://docs.kepler.gl/docs/keplergl-jupyter#6.-match-config-with-data)
