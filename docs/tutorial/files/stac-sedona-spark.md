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

# STAC catalog with Apache Sedona and Spark

The STAC data source allows you to read data from a SpatioTemporal Asset Catalog (STAC) API. The data source supports reading STAC items and collections.

## Usage

To use the STAC data source, you can load a STAC catalog into a Sedona DataFrame using the stac format. The path can be either a local STAC collection JSON file or an HTTP/HTTPS endpoint to retrieve the collection JSON file.

You can load a STAC collection from a local collection file:

```python
df = sedona.read.format("stac").load("/user/stac_collection.json")
df.printSchema()
df.show()
```

You can load a STAC collection from a s3 collection file object:

```python
df = sedona.read.format("stac").load(
    "s3a://example.com/stac_bucket/stac_collection.json"
)
df.printSchema()
df.show()
```

You can also load a STAC collection from an HTTP/HTTPS endpoint:

```python
df = sedona.read.format("stac").load(
    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a"
)
df.printSchema()
df.show()
```

output:

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

## Filter Pushdown

The STAC data source supports predicate pushdown for spatial and temporal filters. The data source can push down spatial and temporal filters to the underlying data source to reduce the amount of data that needs to be read.

### Spatial Filter Pushdown

Spatial filter pushdown allows the data source to apply spatial predicates (e.g., st_contains, st_intersects) directly at the data source level, reducing the amount of data transferred and processed.

### Temporal Filter Pushdown

Temporal filter pushdown allows the data source to apply temporal predicates (e.g., BETWEEN, >=, <=) directly at the data source level, similarly reducing the amount of data transferred and processed.

## Examples

Here are some examples demonstrating how to query a STAC data source that is loaded into a table named `STAC_TABLE`.

### SQL Select Without Filters

```sql
SELECT id, datetime as dt, geometry, bbox FROM STAC_TABLE
```

### SQL Select With Temporal Filter

```sql
  SELECT id, datetime as dt, geometry, bbox
  FROM STAC_TABLE
  WHERE datetime BETWEEN '2020-01-01' AND '2020-12-13'
```

In this example, the data source will push down the temporal filter to the underlying data source.

### SQL Select With Spatial Filter

```sql
  SELECT id, geometry
  FROM STAC_TABLE
  WHERE st_contains(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)
```

In this example, the data source will push down the spatial filter to the underlying data source.

### Sedona Configuration for STAC Reader

When using the STAC reader in Sedona, several configuration options can be set to control the behavior of the reader. These configurations are typically set in a `Map[String, String]` and passed to the reader. Below are the key sedona configuration options:

- **spark.sedona.stac.load.maxPartitionItemFiles**: This option specifies the maximum number of item files that can be included in a single partition. It helps in controlling the size of partitions. The default value is set to -1, meaning the system will automatically determine the number of item files per partition.

- **spark.sedona.stac.load.numPartitions**: This option sets the number of partitions to be created for the STAC data. It allows for better control over data distribution and parallel processing. The default value is set to -1, meaning the system will automatically determine the number of item files per partition.

Below are reader options that can be set to control the behavior of the STAC reader:

- **itemsLimitMax**: This option specifies the maximum number of items to be loaded from the STAC collection. It helps in limiting the amount of data processed. The default value is set to -1, meaning all items will be loaded.

- **itemsLoadProcessReportThreshold**: This option specifies the threshold for reporting the progress of item loading. It helps in monitoring the progress of the loading process. The default value is set to 1000000, meaning the progress will be reported every 1,000,000 items loaded.

- **itemsLimitPerRequest**: This option specifies the maximum number of items to be requested in a single API call. It helps in controlling the size of each request. The default value is set to 10.

These configurations can be combined into a single `Map[String, String]` and passed to the STAC reader as shown below:

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

These options above provide fine-grained control over how the STAC data is read and processed in Sedona.

## Python API

The Python API allows you to interact with a SpatioTemporal Asset Catalog (STAC) API using the Client class. This class provides methods to open a connection to a STAC API, retrieve collections, and search for items with various filters.

### Sample Code

#### Initialize the Client

```python
from sedona.stac.client import Client

# Initialize the client
client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
```

#### Search Items on a Collection Within a Year

```python
items = client.search(
    collection_id="aster-l1t", datetime="2020", return_dataframe=False
)
```

#### Search Items on a Collection Within a Month and Max Items

```python
items = client.search(
    collection_id="aster-l1t", datetime="2020-05", return_dataframe=False, max_items=5
)
```

#### Search Items with Bounding Box and Interval

```python
items = client.search(
    collection_id="aster-l1t",
    ids=["AST_L1T_00312272006020322_20150518201805"],
    bbox=[-180.0, -90.0, 180.0, 90.0],
    datetime=["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"],
    return_dataframe=False,
)
```

#### Search Multiple Items with Multiple Bounding Boxes

```python
bbox_list = [[-180.0, -90.0, 180.0, 90.0], [-100.0, -50.0, 100.0, 50.0]]
items = client.search(collection_id="aster-l1t", bbox=bbox_list, return_dataframe=False)
```

#### Search Items and Get DataFrame as Return with Multiple Intervals

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

#### Save Items in DataFrame to GeoParquet with Both Bounding Boxes and Intervals

```python
# Save items in DataFrame to GeoParquet with both bounding boxes and intervals
client.get_collection("aster-l1t").save_to_geoparquet(
    output_path="/path/to/output", bbox=bbox_list, datetime="2020-05"
)
```

These examples demonstrate how to use the Client class to search for items in a STAC collection with various filters and return the results as either an iterator of PyStacItem objects or a Spark DataFrame.

### Methods

**`open(url: str) -> Client`**
Opens a connection to the specified STAC API URL.

Parameters:
* `url` (*str*): The URL of the STAC API to connect to.
  * Example: `"https://planetarycomputer.microsoft.com/api/stac/v1"`

Returns:
* `Client`: An instance of the `Client` class connected to the specified URL.

---

**`get_collection(collection_id: str) -> CollectionClient`**
Retrieves a collection client for the specified collection ID.

Parameters:
* `collection_id` (*str*): The ID of the collection to retrieve.
  * Example: `"aster-l1t"`

Returns:
* `CollectionClient`: An instance of the `CollectionClient` class for the specified collection.

---

**`search(*ids: Union[str, list], collection_id: str, bbox: Optional[list] = None, datetime: Optional[Union[str, datetime.datetime, list]] = None, max_items: Optional[int] = None, return_dataframe: bool = True) -> Union[Iterator[PyStacItem], DataFrame]`**
Searches for items in the specified collection with optional filters.

Parameters:

* `ids` (*Union[str, list]*): A variable number of item IDs to filter the items.
  * Example: `"item_id1"` or `["item_id1", "item_id2"]`
* `collection_id` (*str*): The ID of the collection to search in.
  * Example: `"aster-l1t"`
* `bbox` (*Optional[list]*): A list of bounding boxes for filtering the items. Each bounding box is represented as a list of four float values: `[min_lon, min_lat, max_lon, max_lat]`.
  * Example: `[[ -180.0, -90.0, 180.0, 90.0 ]]`
* `datetime` (*Optional[Union[str, datetime.datetime, list]]*): A single datetime, RFC 3339-compliant timestamp, or a list of date-time ranges for filtering the items.
  * Examples:
    * `"2020-01-01T00:00:00Z"`
    * `datetime.datetime(2020, 1, 1)`
    * `[["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]`
* `max_items` (*Optional[int]*): The maximum number of items to return from the search, even if there are more matching results.
  * Example: `100`
* `return_dataframe` (*bool*): If `True` (default), return the result as a Spark DataFrame instead of an iterator of `PyStacItem` objects.
  * Example: `True`

Returns:
* *Union[Iterator[PyStacItem], DataFrame]*: An iterator of `PyStacItem` objects or a Spark DataFrame that matches the specified filters.

## References

- STAC Specification: https://stacspec.org/

- STAC Browser: https://github.com/radiantearth/stac-browser

- STAC YouTube Video: https://www.youtube.com/watch?v=stac-video
