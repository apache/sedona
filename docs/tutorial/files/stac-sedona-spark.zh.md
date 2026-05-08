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

# 在 Spark 上使用 Apache Sedona 处理 STAC catalog

STAC 数据源允许您从 SpatioTemporal Asset Catalog（STAC）API 读取数据。该数据源同时支持读取 STAC items 与 collections。

## 用法

要使用 STAC 数据源，可使用 `stac` 格式将 STAC catalog 加载到 Sedona DataFrame。`load` 的路径可以是本地的 STAC collection JSON 文件，也可以是 HTTP/HTTPS 端点。

从本地 collection 文件加载 STAC collection：

```python
df = sedona.read.format("stac").load("/user/stac_collection.json")
df.printSchema()
df.show()
```

从 S3 上的 collection 文件加载：

```python
df = sedona.read.format("stac").load(
    "s3a://example.com/stac_bucket/stac_collection.json"
)
df.printSchema()
df.show()
```

也可以从 HTTP/HTTPS 端点加载：

```python
df = sedona.read.format("stac").load(
    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a"
)
df.printSchema()
df.show()
```

输出：

```
root
 |-- stac_version: string (nullable = false)
 |-- stac_extensions: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- type: string (nullable = false)
 |-- id: string (nullable = false)
 |-- bbox: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- geometry: geometry (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)
 |-- datetime: timestamp (nullable = true)
 |-- start_datetime: timestamp (nullable = true)
 |-- end_datetime: timestamp (nullable = true)
 |-- created: timestamp (nullable = true)
 |-- updated: timestamp (nullable = true)
 |-- platform: string (nullable = true)
 |-- instruments: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- constellation: string (nullable = true)
 |-- mission: string (nullable = true)
 |-- grid:code: string (nullable = true)
 |-- gsd: double (nullable = true)
 |-- collection: string (nullable = true)
 |-- links: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- rel: string (nullable = true)
 |    |    |-- href: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- title: string (nullable = true)
 |-- assets: map (nullable = true)
 |    |-- key: string
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- href: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- title: string (nullable = true)
 |    |    |-- roles: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)

+------------+--------------------+-------+--------------------+--------------------+--------------------+-----+-----------+--------------------+--------------+------------+--------------------+--------------------+-----------+-----------+-------------+-------+----+--------------------+--------------------+--------------------+
|stac_version|     stac_extensions|   type|                  id|                bbox|            geometry|title|description|            datetime|start_datetime|end_datetime|             created|             updated|   platform|instruments|constellation|mission| gsd|          collection|               links|              assets|
+------------+--------------------+-------+--------------------+--------------------+--------------------+-----+-----------+--------------------+--------------+------------+--------------------+--------------------+-----------+-----------+-------------+-------+----+--------------------+--------------------+--------------------+
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NYC_202212...|[-55.202493, 1.71...|POLYGON ((-55.201...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-01 21:13:...|2024-05-01 21:13:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NZC_202212...|[-54.30394, 1.719...|POLYGON ((-54.302...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-03 00:39:...|2024-05-03 00:39:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T22NBH_202212...|[-53.698196, 2.63...|POLYGON ((-53.698...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-03 00:26:...|2024-05-03 00:26:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NYD_202212...|[-55.201423, 2.62...|POLYGON ((-55.199...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-01 21:10:...|2024-05-01 21:10:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NZD_202212...|[-54.302336, 2.62...|POLYGON ((-54.299...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-03 00:12:...|2024-05-03 00:12:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T22NBJ_202212...|[-53.700535, 2.63...|POLYGON ((-53.700...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-03 00:30:...|2024-05-03 00:30:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NYE_202212...|[-55.199906, 3.52...|POLYGON ((-55.197...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-01 21:24:...|2024-05-01 21:24:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NZE_202212...|[-54.300062, 3.52...|POLYGON ((-54.296...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-03 00:14:...|2024-05-03 00:14:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T22NBK_202212...|[-53.703548, 3.52...|POLYGON ((-53.703...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-03 00:32:...|2024-05-03 00:32:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
|       1.0.0|[https://stac-ext...|Feature|S2B_T21NYF_202212...|[-55.197941, 4.42...|POLYGON ((-55.195...| NULL|       NULL|2022-12-05 14:11:...|          NULL|        NULL|2024-05-01 21:43:...|2024-05-01 21:43:...|sentinel-2b|      [msi]|   sentinel-2|   NULL|NULL|sentinel-2-pre-c1...|[{self, https://e...|{red -> {https://...|
+------------+--------------------+-------+--------------------+--------------------+--------------------+-----+-----------+--------------------+--------------+------------+--------------------+--------------------+-----------+-----------+-------------+-------+----+--------------------+--------------------+--------------------+
```

## 谓词下推（Filter Pushdown）

STAC 数据源支持把空间和时间过滤下推到底层数据源，减少需要读取的数据量。

### 空间过滤下推

空间过滤下推让数据源能够直接在数据源层应用空间谓词（如 `st_intersects`），从而减少传输与处理的数据量。

### 时间过滤下推

时间过滤下推让数据源能够直接在数据源层应用时间谓词（如 `BETWEEN`、`>=`、`<=`），同样可以减少传输与处理的数据量。

## 示例

下面的示例假设 STAC 数据源已经被加载到名为 `STAC_TABLE` 的表中。

### 不带过滤的 SQL Select

```sql
SELECT id, datetime as dt, geometry, bbox FROM STAC_TABLE
```

### 带时间过滤的 SQL Select

```sql
  SELECT id, datetime as dt, geometry, bbox
  FROM STAC_TABLE
  WHERE datetime BETWEEN '2020-01-01' AND '2020-12-13'
```

该例中，数据源会把时间过滤下推到底层数据源。

### 带空间过滤的 SQL Select

```sql
  SELECT id, geometry
  FROM STAC_TABLE
  WHERE st_intersects(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)
```

该例中，数据源会把空间过滤下推到底层数据源。

### Sedona 的 STAC 读取器配置

在 Sedona 中使用 STAC 读取器时，可通过若干配置项控制读取行为，通常以 `Map[String, String]` 形式传入。下列是核心 Sedona 配置项：

- **spark.sedona.stac.load.maxPartitionItemFiles**：单个分区中可包含的最大 item 文件数量，用于控制分区大小。默认 `-1`，表示由系统自动决定。

- **spark.sedona.stac.load.numPartitions**：为 STAC 数据创建的分区数，便于控制数据分布与并行度。默认 `-1`，表示由系统自动决定。

下面是 STAC 读取器的常见 reader option：

- **itemsLimitMax**：从 STAC collection 加载的 item 数量上限，便于限制处理规模。默认 `-1`，表示加载全部 item。

- **itemsLoadProcessReportThreshold**：item 加载进度的报告阈值。默认 `1000000`，即每加载 100 万个 item 报告一次进度。

- **itemsLimitPerRequest**：单次 API 请求中获取的最大 item 数。默认 `10`。

- **headers**：STAC API 请求中携带的 HTTP 头，应是 JSON 编码的字符串，包含一系列键值对。可用于鉴权或自定义头。例如：`{"Authorization": "Basic <base64_credentials>"}`

可以将这些配置合并到一个 `Map[String, String]` 中传给 STAC 读取器：

```scala
  def defaultSparkConfig: Map[String, String] = Map(
    "spark.sedona.stac.load.maxPartitionItemFiles" -> "100",
    "spark.sedona.stac.load.numPartitions" -> "10",
    "spark.sedona.stac.load.itemsLimitMax" -> "20")

  val sparkSession: SparkSession = {
    val builder = SedonaContext
            .builder()
            .master("local[*]")
    defaultSparkConfig.foreach { case (key, value) => builder.config(key, value) }
    builder.getOrCreate()
  }

 df = sedona.read
      .format("stac")
      .option("itemsLimitMax", "100")
      .option("itemsLoadProcessReportThreshold", "2000000")
      .option("itemsLimitPerRequest", "100")
      .load("https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a")
```

通过以上选项，可以对 Sedona 中 STAC 数据的读取与处理过程进行细粒度的控制。

## Python API

Python API 让您可以通过 Client 类与 STAC API 交互。它提供了打开 STAC API 连接、获取 collection、按多种条件搜索 item 的方法。

### 示例代码

#### 初始化客户端

```python
from sedona.spark.stac import Client

# 初始化客户端
client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
```

#### 在某个 collection 中按年份搜索

```python
items = client.search(
    collection_id="aster-l1t", datetime="2020", return_dataframe=False
)
```

#### 在某个 collection 中按月份搜索并限制最大返回数

```python
items = client.search(
    collection_id="aster-l1t", datetime="2020-05", return_dataframe=False, max_items=5
)
```

#### 按 bbox 与时间区间搜索

```python
items = client.search(
    collection_id="aster-l1t",
    ids=["AST_L1T_00312272006020322_20150518201805"],
    bbox=[-180.0, -90.0, 180.0, 90.0],
    datetime=["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"],
    return_dataframe=False,
)
```

#### 按多个 bbox 搜索

```python
bbox_list = [[-180.0, -90.0, 180.0, 90.0], [-100.0, -50.0, 100.0, 50.0]]
items = client.search(collection_id="aster-l1t", bbox=bbox_list, return_dataframe=False)
```

#### 在多个时间区间内搜索并以 DataFrame 返回

```python
interval_list = [
    ["2020-01-01T00:00:00Z", "2020-06-01T00:00:00Z"],
    ["2020-07-01T00:00:00Z", "2021-01-01T00:00:00Z"],
]
df = client.search(
    collection_id="aster-l1t", datetime=interval_list, return_dataframe=True
)
df.show()
```

#### 同时使用 bbox 与时间将 item 写入 GeoParquet

```python
# 将 DataFrame 中的 item 写入 GeoParquet，同时指定 bbox 与时间
client.get_collection("aster-l1t").save_to_geoparquet(
    output_path="/path/to/output", bbox=bbox_list, datetime="2020-05"
)
```

以上示例演示了如何使用 Client 类按各种条件检索 STAC collection，并以 PyStacItem 迭代器或 Spark DataFrame 的形式返回。

### 鉴权

许多 STAC 服务都需要鉴权才能访问数据。STAC 客户端支持多种鉴权方式：HTTP Basic Authentication、Bearer Token Authentication，以及自定义 HTTP 头。

#### Basic 认证

Basic 认证通常用于 API key 或用户名/密码组合。许多服务（如 Planet Labs）会把 API key 作为用户名，密码留空。

```python
from sedona.spark.stac import Client

# 示例 1：使用 API key（常见模式）
client = Client.open("https://api.example.com/stac/v1")
client.with_basic_auth("your_api_key_here", "")

# 在认证状态下搜索 item
df = client.search(collection_id="example-collection", max_items=10)
df.show()

# 示例 2：使用用户名与密码
client = Client.open("https://api.example.com/stac/v1")
client.with_basic_auth("username", "password")

df = client.search(collection_id="example-collection", max_items=10)
df.show()

# 示例 3：链式调用
df = (
    Client.open("https://api.example.com/stac/v1")
    .with_basic_auth("your_api_key", "")
    .search(collection_id="example-collection", max_items=10)
)
df.show()
```

#### Bearer Token 认证

Bearer Token 认证常与 OAuth2 token 或 JWT token 一起使用。注意：某些服务可能仅支持特定的鉴权方式。

```python
from sedona.spark.stac import Client

# 使用 bearer token
client = Client.open("https://api.example.com/stac/v1")
client.with_bearer_token("your_access_token_here")

df = client.search(collection_id="example-collection", max_items=10)
df.show()

# 链式调用
df = (
    Client.open("https://api.example.com/stac/v1")
    .with_bearer_token("your_token")
    .search(collection_id="example-collection", max_items=10)
)
df.show()
```

#### 自定义 HTTP 头

也可以在创建客户端时直接传入自定义 HTTP 头，适用于鉴权方式较为特殊的服务。

```python
from sedona.spark.stac import Client

# 使用自定义 HTTP 头
headers = {"Authorization": "Bearer your_token_here", "X-Custom-Header": "custom_value"}
client = Client.open("https://api.example.com/stac/v1", headers=headers)

df = client.search(collection_id="example-collection", max_items=10)
df.show()
```

#### 在 Scala DataSource 中使用鉴权

直接在 Scala 或 Spark SQL 中使用 STAC 数据源时，可以把鉴权 HTTP 头作为 JSON 编码的 option 传入：

```python
import json
from pyspark.sql import SparkSession

# 准备鉴权 HTTP 头
headers = {"Authorization": "Basic <base64_encoded_credentials>"}
headers_json = json.dumps(headers)

# 在鉴权状态下加载 STAC 数据
df = (
    spark.read.format("stac")
    .option("headers", headers_json)
    .load("https://api.example.com/stac/v1/collections/example-collection")
)

df.show()
```

```scala
// Scala 示例
val headersJson = """{"Authorization":"Basic <base64_encoded_credentials>"}"""

val df = sparkSession.read
  .format("stac")
  .option("headers", headersJson)
  .load("https://api.example.com/stac/v1/collections/example-collection")

df.show()
```

#### 注意事项

- **鉴权方式互斥**：设置新的鉴权方式会覆盖此前的 Authorization 头，但不会影响其他自定义 HTTP 头。
- **HTTP 头会向下传播**：在 Client 上设置的 HTTP 头会自动应用到后续的 collection 与 item 请求。
- **不同服务有不同的要求**：不同 STAC 服务可能要求不同的鉴权方式。例如 Planet Labs 在访问 collection 时需要 Basic 认证，而非 Bearer Token。
- **向后兼容**：所有鉴权参数都是可选的。原本访问无需鉴权的公开 STAC 服务的代码不受影响，可继续使用。

### 方法

**`open(url: str, headers: Optional[dict] = None) -> Client`**
打开到指定 STAC API URL 的连接。

参数：

* `url` (*str*)：要连接的 STAC API URL。例如：`"https://planetarycomputer.microsoft.com/api/stac/v1"`
* `headers` (*Optional[dict]*)：可选的 HTTP 头字典，用于鉴权或自定义头。例如：`{"Authorization": "Bearer token123"}`

返回：

* `Client`：连接到指定 URL 的 `Client` 实例。

---

**`with_basic_auth(username: str, password: str) -> Client`**
为客户端启用 HTTP Basic 认证。

该方法会将用户名与密码进行 Base64 编码，并添加相应的 Authorization 头。

参数：

* `username` (*str*)：用户名。对于 API key，此项通常就是 API key 本身。例如 `"your_api_key"`。
* `password` (*str*)：密码。对于 API key 通常留空。例如 `""`。

返回：

* `Client`：返回自身以便链式调用。

---

**`with_bearer_token(token: str) -> Client`**
为客户端启用 Bearer Token 认证。

该方法会添加 Bearer Token 形式的 Authorization 头，常用于 OAuth2 与 API token 场景。

参数：

* `token` (*str*)：用于鉴权的 bearer token。例如 `"your_access_token_here"`。

返回：

* `Client`：返回自身以便链式调用。

---

**`get_collection(collection_id: str) -> CollectionClient`**
按 collection ID 获取对应的 CollectionClient。

参数：

* `collection_id` (*str*)：collection 的 ID。例如 `"aster-l1t"`。

返回：

* `CollectionClient`：对应 collection 的 `CollectionClient` 实例。

---

**`search(*ids: Union[str, list], collection_id: str, bbox: Optional[list] = None, datetime: Optional[Union[str, datetime.datetime, list]] = None, max_items: Optional[int] = None, return_dataframe: bool = True) -> Union[Iterator[PyStacItem], DataFrame]`**
在指定 collection 中按可选过滤条件搜索 item。

参数：

* `ids` (*Union[str, list]*)：可变数量的 item ID，用于过滤。例如 `"item_id1"` 或 `["item_id1", "item_id2"]`。
* `collection_id` (*str*)：要搜索的 collection ID。例如 `"aster-l1t"`。
* `bbox` (*Optional[list]*)：用于过滤 item 的边界框列表，每项形如 `[min_lon, min_lat, max_lon, max_lat]`。例如 `[[ -180.0, -90.0, 180.0, 90.0 ]]`。
* `datetime` (*Optional[Union[str, datetime.datetime, list]]*)：单个日期时间、符合 RFC 3339 的时间戳，或一组时间区间。例如 `"2020-01-01T00:00:00Z"`、`datetime.datetime(2020, 1, 1)`、`[["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]`。
* `max_items` (*Optional[int]*)：返回 item 的最大数量。例如 `100`。
* `return_dataframe` (*bool*)：若为 `True`（默认），返回 Spark DataFrame，而不是 `PyStacItem` 迭代器。例如 `True`。

返回：

* *Union[Iterator[PyStacItem], DataFrame]*：与过滤条件匹配的 `PyStacItem` 迭代器或 Spark DataFrame。

## 参考

- STAC 规范：https://stacspec.org/

- STAC Browser：https://github.com/radiantearth/stac-browser
