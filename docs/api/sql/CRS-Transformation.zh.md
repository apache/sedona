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

# CRS 转换

Sedona 通过 `ST_Transform` 函数提供坐标参考系（CRS）转换功能。自 v1.9.0 起，Sedona 使用 proj4sedona 库 —— 一个纯 Java 实现，支持多种 CRS 输入格式以及基于网格的转换。

## 支持的 CRS 格式

Sedona 支持以下用于指定源 CRS 与目标 CRS 的格式：

### 权威机构代码（Authority Code）

最常见的指定 CRS 的方式是使用形如 `AUTHORITY:CODE` 的权威机构代码。Sedona 使用 [spatialreference.org](https://spatialreference.org/projjson_index.json) 作为开源的 CRS 数据库，支持多个权威机构：

| Authority | 说明 | 示例 |
|-----------|-------------|---------|
| EPSG | European Petroleum Survey Group | `EPSG:4326`, `EPSG:3857` |
| ESRI | Esri 坐标系 | `ESRI:102008`, `ESRI:54012` |
| IAU | International Astronomical Union（行星 CRS） | `IAU:30100` |
| SR-ORG | 用户贡献的定义 | `SR-ORG:6864` |

```sql
-- Transform from WGS84 (EPSG:4326) to Web Mercator (EPSG:3857)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'EPSG:3857'
) AS transformed_point
```

输出：

```
POINT (-13627665.271218014 4548257.702387721)
```

```sql
-- Transform using ESRI authority code (North America Albers Equal Area Conic)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'ESRI:102008'
) AS transformed_point
```

```sql
-- Transform from WGS84 to UTM Zone 10N (EPSG:32610)
SELECT ST_Transform(
    ST_GeomFromText('POLYGON((-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5))'),
    'EPSG:4326',
    'EPSG:32610'
) AS transformed_polygon
```

你可以在 [spatialreference.org](https://spatialreference.org/projjson_index.json) 或 [EPSG.io](https://epsg.io/) 上浏览可用的 CRS 代码。

### WKT1（OGC Well-Known Text）

WKT1 是 OGC 提供的 CRS 定义的 Well-Known Text 格式。投影 CRS 以 `PROJCS[...]` 开头，地理 CRS 以 `GEOGCS[...]` 开头。

```sql
-- Transform using WKT1 format for target CRS
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'PROJCS["WGS 84 / Pseudo-Mercator",
        GEOGCS["WGS 84",
            DATUM["WGS_1984",
                SPHEROID["WGS 84",6378137,298.257223563]],
            PRIMEM["Greenwich",0],
            UNIT["degree",0.0174532925199433]],
        PROJECTION["Mercator_1SP"],
        PARAMETER["central_meridian",0],
        PARAMETER["scale_factor",1],
        PARAMETER["false_easting",0],
        PARAMETER["false_northing",0],
        UNIT["metre",1],
        AUTHORITY["EPSG","3857"]]'
) AS transformed_point
```

### WKT2（ISO 19162:2019）

WKT2 是现代的 ISO 19162:2019 标准格式。投影 CRS 以 `PROJCRS[...]` 开头，地理 CRS 以 `GEOGCRS[...]` 开头。

```sql
-- Transform using WKT2 format for target CRS
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'PROJCRS["WGS 84 / UTM zone 10N",
        BASEGEOGCRS["WGS 84",
            DATUM["World Geodetic System 1984",
                ELLIPSOID["WGS 84",6378137,298.257223563]]],
        CONVERSION["UTM zone 10N",
            METHOD["Transverse Mercator"],
            PARAMETER["Latitude of natural origin",0],
            PARAMETER["Longitude of natural origin",-123],
            PARAMETER["Scale factor at natural origin",0.9996],
            PARAMETER["False easting",500000],
            PARAMETER["False northing",0]],
        CS[Cartesian,2],
            AXIS["easting",east],
            AXIS["northing",north],
        UNIT["metre",1],
        ID["EPSG",32610]]'
) AS transformed_point
```

### PROJ 字符串

PROJ 字符串提供一种使用投影参数紧凑地定义 CRS 的方式。它以 `+proj=` 开头。

```sql
-- Transform using PROJ string for UTM Zone 10N
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    '+proj=longlat +datum=WGS84 +no_defs',
    '+proj=utm +zone=10 +datum=WGS84 +units=m +no_defs'
) AS transformed_point
```

```sql
-- Transform using PROJ string for Lambert Conformal Conic
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    '+proj=lcc +lat_1=33 +lat_2=45 +lat_0=39 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs'
) AS transformed_point
```

### PROJJSON

PROJJSON 是 CRS 的 JSON 表示形式，便于在基于 JSON 的工作流中使用。

```sql
-- Transform using PROJJSON for target CRS
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    '{
        "type": "ProjectedCRS",
        "name": "WGS 84 / UTM zone 10N",
        "base_crs": {
            "name": "WGS 84",
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": "World Geodetic System 1984",
                "ellipsoid": {
                    "name": "WGS 84",
                    "semi_major_axis": 6378137,
                    "inverse_flattening": 298.257223563
                }
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {"name": "Longitude", "abbreviation": "lon", "direction": "east", "unit": "degree"},
                    {"name": "Latitude", "abbreviation": "lat", "direction": "north", "unit": "degree"}
                ]
            }
        },
        "conversion": {
            "name": "UTM zone 10N",
            "method": {"name": "Transverse Mercator"},
            "parameters": [
                {"name": "Latitude of natural origin", "value": 0, "unit": "degree"},
                {"name": "Longitude of natural origin", "value": -123, "unit": "degree"},
                {"name": "Scale factor at natural origin", "value": 0.9996},
                {"name": "False easting", "value": 500000, "unit": "metre"},
                {"name": "False northing", "value": 0, "unit": "metre"}
            ]
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": [
                {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
                {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"}
            ]
        },
        "id": {"authority": "EPSG", "code": 32610}
    }'
) AS transformed_point
```

## URL CRS Provider

自 v1.9.0 起，Sedona 支持从远程 HTTP 服务器解析 CRS 定义。当你需要使用内置数据库中不包含的自定义或内部 CRS 定义，或希望使用你自己的 CRS 定义服务时，这一功能非常有用。

配置后，URL 提供者会**先于**内置 CRS 数据库被查询。如果 URL 提供者返回了有效的 CRS 定义，则直接使用该定义。如果 URL 返回 404 或错误，Sedona 会回退到内置定义。

### 托管 CRS 定义

你可以将自定义 CRS 定义托管在任何 HTTP 可访问的位置。两种常见做法：

- **GitHub 仓库**：把 CRS 定义文件放在一个公开的 GitHub 仓库中，并使用 raw 内容 URL。这种方式最容易上手 —— 无需自建服务器。
- **公开的 S3 桶**：把 CRS 定义文件上传到具有公共读权限的 Amazon S3 桶，并使用 S3 静态网站 URL 或 CloudFront 分发地址。

每个文件应包含一个 CRS 定义，其格式通过 `spark.sedona.crs.url.format` 指定（PROJJSON、PROJ 字符串、WKT1 或 WKT2）。

### 配置

在创建 Sedona 会话时设置以下 Spark 配置属性：

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.example.com")
    .config("spark.sedona.crs.url.pathTemplate", "/{authority}/{code}.json")
    .config("spark.sedona.crs.url.format", "projjson")
    .getOrCreate()
)
sedona = SedonaContext.create(config)
```

在默认的路径模板下，解析 `EPSG:4326` 时会请求：

```
https://crs.example.com/epsg/4326.json
```

只有 `spark.sedona.crs.url.base` 是必需的。另外两个属性都有合理的默认值（`/{authority}/{code}.json` 与 `projjson`）。

### 支持的响应格式

| Format 取值 | 说明 | 内容示例 |
|-------------|-------------|----------------|
| `projjson` | PROJJSON（默认） | `{"type": "GeographicCRS", ...}` |
| `proj` | PROJ 字符串 | `+proj=longlat +datum=WGS84 +no_defs` |
| `wkt1` | OGC WKT1 | `GEOGCS["WGS 84", ...]` |
| `wkt2` | ISO 19162 WKT2 | `GEOGCRS["WGS 84", ...]` |

### 示例：GitHub 仓库

假设你有一个 GitHub 仓库 `myorg/crs-definitions`，结构如下：

```
crs-definitions/
  epsg/
    990001.proj
    990002.proj
```

其中 `epsg/990001.proj` 包含一条 PROJ 字符串，例如：

```
+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs
```

将 Sedona 指向 GitHub raw 内容 URL：

```python
config = (
    SedonaContext.builder()
    .config(
        "spark.sedona.crs.url.base",
        "https://raw.githubusercontent.com/myorg/crs-definitions/main",
    )
    .config("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.proj")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Resolves EPSG:990001 from:
# https://raw.githubusercontent.com/myorg/crs-definitions/main/epsg/990001.proj
sedona.sql("""
    SELECT ST_Transform(
        ST_GeomFromText('POINT(-122.4194 37.7749)'),
        'EPSG:4326',
        'EPSG:990001'
    ) AS transformed_point
""").show()
```

### 示例：自建 CRS 服务器

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.mycompany.com")
    .config("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.proj")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Now ST_Transform will try https://crs.mycompany.com/epsg/3857.proj
# before falling back to built-in definitions
sedona.sql("""
    SELECT ST_Transform(
        ST_GeomFromText('POINT(-122.4194 37.7749)'),
        'EPSG:4326',
        'EPSG:3857'
    ) AS transformed_point
""").show()
```

### 示例：自定义权威机构代码

URL 提供者对于不存在于任何公开数据库中的自定义或内部权威机构代码尤其有用。使用默认的路径模板 `/{authority}/{code}.json` 时，`{authority}` 占位符会被 CRS 字符串中的权威机构名称（小写）替换：

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.mycompany.com")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Resolves MYORG:1001 from:
# https://crs.mycompany.com/myorg/1001.json
sedona.sql("""
    SELECT ST_Transform(
        ST_GeomFromText('POINT(-122.4194 37.7749)'),
        'EPSG:4326',
        'MYORG:1001'
    ) AS transformed_point
""").show()
```

### 示例：将几何对象的 SRID 与 URL 提供者一起使用

如果几何对象已设置了 SRID（例如通过 `ST_SetSRID`），可以省略源 CRS 参数。此时源 CRS 会从几何对象的 SRID 推导为 EPSG 代码：

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.mycompany.com")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# The source CRS is taken from the geometry's SRID (4326 → EPSG:4326).
# Only the target CRS string is needed.
sedona.sql("""
    SELECT ST_Transform(
        ST_SetSRID(ST_GeomFromText('POINT(-122.4194 37.7749)'), 4326),
        'EPSG:3857'
    ) AS transformed_point
""").show()
```

### 禁用 URL 提供者

要避免启用 URL 提供者，可以省略 `spark.sedona.crs.url.base` 或保持其为空字符串（默认值）。注意，一旦 URL 提供者已在某个 executor JVM 中注册，它在该 JVM 的生命周期内会一直保持激活。

参见：[Configuration parameters](Parameter.md#crs-transformation) 中 URL CRS 提供者的完整配置列表。

## 网格文件支持

网格文件支持高精度的基准面（datum）转换，例如 NAD27 到 NAD83 或 OSGB36 到 ETRS89。Sedona 支持从多种来源加载网格文件。

### 网格文件来源

可以在 PROJ 字符串中通过 `+nadgrids` 参数指定网格文件：

| 来源 | 格式 | 示例 |
|--------|--------|---------|
| 本地文件 | 绝对路径 | `+nadgrids=/path/to/grid.gsb` |
| PROJ CDN | `@` 前缀 | `+nadgrids=@us_noaa_conus.tif` |
| HTTPS URL | 完整 URL | `+nadgrids=https://cdn.proj.org/us_noaa_conus.tif` |

使用 `@` 前缀时，网格文件会自动从 [PROJ CDN](https://cdn.proj.org/) 获取。

### 可选网格与必需网格

- **`@` 前缀（可选）**：如果该网格不可用，转换会继续进行。当网格能提升精度但并非必需时使用。
- **无前缀（必需）**：如果找不到该网格文件，会抛出错误。

### 使用网格文件的 SQL 示例

```sql
-- Transform NAD27 to NAD83 using PROJ CDN grid (optional)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    '+proj=longlat +datum=NAD27 +no_defs +nadgrids=@us_noaa_conus.tif',
    'EPSG:4269'
) AS transformed_point
```

```sql
-- Transform using mandatory grid file (error if not found)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    '+proj=longlat +datum=NAD27 +no_defs +nadgrids=us_noaa_conus.tif',
    'EPSG:4269'
) AS transformed_point
```

```sql
-- Transform OSGB36 to ETRS89 using UK grid
SELECT ST_Transform(
    ST_GeomFromText('POINT(-0.1276 51.5074)'),
    '+proj=longlat +datum=OSGB36 +nadgrids=@uk_os_OSTN15_NTv2_OSGBtoETRS.gsb +no_defs',
    'EPSG:4258'
) AS transformed_point
```

## 坐标顺序

Sedona 期望几何对象使用 **经度/纬度（lon/lat）** 顺序。如果你的数据是 lat/lon 顺序，请在转换前使用 `ST_FlipCoordinates` 交换坐标。

```sql
-- If your data is in lat/lon order, flip first
SELECT ST_Transform(
    ST_FlipCoordinates(ST_GeomFromText('POINT(37.7749 -122.4194)')),
    'EPSG:4326',
    'EPSG:3857'
) AS transformed_point
```

Sedona 会自动处理 CRS 定义中的坐标顺序，确保源 CRS 与目标 CRS 在内部都使用 lon/lat 顺序。

## 使用几何对象的 SRID

如果几何对象已经设置了 SRID，可以省略源 CRS 参数：

```sql
-- Set SRID on geometry and transform using only target CRS
SELECT ST_Transform(
    ST_SetSRID(ST_GeomFromText('POINT(-122.4194 37.7749)'), 4326),
    'EPSG:3857'
) AS transformed_point
```

## 参见

- [ST_Transform](Spatial-Reference-System/ST_Transform.md) —— 函数参考
- [ST_SetSRID](Spatial-Reference-System/ST_SetSRID.md) —— 设置几何对象的 SRID
- [ST_SRID](Spatial-Reference-System/ST_SRID.md) —— 获取几何对象的 SRID
- [ST_FlipCoordinates](Geometry-Editors/ST_FlipCoordinates.md) —— 交换 X 与 Y 坐标
