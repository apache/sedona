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

# GeoPandas API for Apache Sedona

The GeoPandas API for Apache Sedona provides a familiar GeoPandas interface that scales your geospatial analysis beyond single-node limitations. This API combines the intuitive GeoPandas DataFrame syntax with the distributed processing power of Apache Sedona on Apache Spark, enabling you to work with planetary-scale datasets using the same code patterns you already know.

## Overview

### What is the GeoPandas API for Apache Sedona?

The GeoPandas API for Apache Sedona is a compatibility layer that allows you to use GeoPandas-style operations on distributed geospatial data. Instead of being limited to single-node processing, your GeoPandas code can leverage the full power of Apache Spark clusters for large-scale geospatial analysis.

### Key Benefits

- **Familiar API**: Use the same GeoPandas syntax and methods you're already familiar with
- **Distributed Processing**: Scale beyond single-node limitations to handle large datasets
- **Lazy Evaluation**: Benefit from Apache Sedona's query optimization and lazy execution
- **Performance**: Leverage distributed computing for complex geospatial operations
- **Seamless Migration**: Minimal code changes required to migrate existing GeoPandas workflows

## Setup

The GeoPandas API for Apache Sedona automatically handles SparkSession management through PySpark's pandas-on-Spark integration. You have two options for setup:

### Option 1: Automatic SparkSession (Recommended)

The GeoPandas API automatically uses the default SparkSession from PySpark:

```python
from sedona.spark.geopandas import GeoDataFrame, read_parquet

# No explicit SparkSession setup needed - uses default session
# The API automatically handles Sedona context initialization
```

### Option 2: Manual SparkSession Setup

If you need to configure a custom SparkSession or are working in an environment where you need explicit control:

```python
from sedona.spark.geopandas import GeoDataFrame, read_parquet
from sedona.spark import SedonaContext

# Create and configure SparkSession
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

# The GeoPandas API will use this configured session
```

### Option 3: Using Existing SparkSession

If you already have a SparkSession (e.g., in Databricks, EMR, or other managed environments):

```python
from sedona.spark.geopandas import GeoDataFrame, read_parquet
from sedona.spark import SedonaContext

# Use existing SparkSession (e.g., 'spark' in Databricks)
sedona = SedonaContext.create(spark)  # 'spark' is the existing session
```

### How SparkSession Management Works

The GeoPandas API leverages PySpark's pandas-on-Spark functionality, which automatically manages the SparkSession lifecycle:

1. **Default Session**: When you import `sedona.spark.geopandas`, it automatically uses PySpark's default session via `pyspark.pandas.utils.default_session()`

2. **Automatic Sedona Registration**: The API automatically registers Sedona's spatial functions and optimizations with the SparkSession when needed

3. **Transparent Integration**: All GeoPandas operations are translated to Spark SQL operations under the hood, using the configured SparkSession

4. **No Manual Context Management**: Unlike traditional Sedona usage, you don't need to explicitly call `SedonaContext.create()` unless you need custom configuration

This design makes the API more user-friendly by hiding the complexity of SparkSession management while still providing the full power of distributed processing.

### S3 Configuration

When working with S3 data, the GeoPandas API uses Spark's built-in S3 support rather than external libraries like s3fs. Configure anonymous access to public S3 buckets using Spark configuration:

```python
from sedona.spark import SedonaContext

# For anonymous access to public S3 buckets
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

For authenticated S3 access, use appropriate AWS credential providers:

```python
# For IAM roles (recommended for EC2/EMR)
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    )
    .getOrCreate()
)

# For access keys (not recommended for production)
config = (
    SedonaContext.builder()
    .config("spark.hadoop.fs.s3a.access.key", "your-access-key")
    .config("spark.hadoop.fs.s3a.secret.key", "your-secret-key")
    .getOrCreate()
)
```

## Basic Usage

### Importing the API

Instead of importing GeoPandas directly, import from the Sedona GeoPandas module:

```python
# Traditional GeoPandas import
# import geopandas as gpd

# Sedona GeoPandas API import
import sedona.spark.geopandas as gpd

# or
from sedona.spark.geopandas import GeoDataFrame, read_parquet
```

### Reading Data

The API supports reading from various geospatial formats, including Parquet files from cloud storage. For S3 access with anonymous credentials, configure Spark to use anonymous AWS credentials:

```python
from sedona.spark import SedonaContext

# Configure Spark for anonymous S3 access
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.bucket.wherobots-examples.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)

# Load GeoParquet file directly from S3
s3_path = "s3://wherobots-examples/data/onboarding_1/nyc_buildings.parquet"
nyc_buildings = gpd.read_parquet(s3_path)

# Display basic information
print(f"Dataset shape: {nyc_buildings.shape}")
print(f"Columns: {nyc_buildings.columns.tolist()}")
nyc_buildings.head()
```

### Spatial Filtering

Use spatial indexing and filtering methods. Note that `cx` spatial indexing is not yet implemented in the current version:

```python
from shapely.geometry import box

# Define bounding box for Central Park
central_park_bbox = box(
    -73.973,
    40.764,  # bottom-left (longitude, latitude)
    -73.951,
    40.789,  # top-right (longitude, latitude)
)

# Filter buildings within the bounding box using spatial index
# Note: This requires collecting data to driver for spatial filtering
# For large datasets, consider using spatial joins instead
buildings_sample = nyc_buildings.sample(1000)  # Sample for demonstration
central_park_buildings = buildings_sample[
    buildings_sample.geometry.intersects(central_park_bbox)
]

# Display results
print(
    central_park_buildings[["BUILD_ID", "PROP_ADDR", "height_val", "geometry"]].head()
)
```

**Alternative approach for large datasets using spatial joins:**

```python
# Create a GeoDataFrame with the bounding box
bbox_gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[central_park_bbox], crs="EPSG:4326")

# Use spatial join to filter buildings within the bounding box
central_park_buildings = nyc_buildings.sjoin(bbox_gdf, predicate="intersects")
```

## Advanced Operations

### Spatial Joins

Perform spatial joins using the same syntax as GeoPandas:

```python
# Load two datasets
left_df = gpd.read_parquet("s3://bucket/left_data.parquet")
right_df = gpd.read_parquet("s3://bucket/right_data.parquet")

# Spatial join with distance predicate
result = left_df.sjoin(right_df, predicate="dwithin", distance=50)

# Other spatial predicates
intersects_result = left_df.sjoin(right_df, predicate="intersects")
contains_result = left_df.sjoin(right_df, predicate="contains")
```

### Coordinate Reference System Operations

Transform geometries between different coordinate reference systems:

```python
# Set initial CRS
buildings = gpd.read_parquet("buildings.parquet")
buildings = buildings.set_crs("EPSG:4326")

# Transform to projected CRS for area calculations
buildings_projected = buildings.to_crs("EPSG:3857")

# Calculate areas
buildings_projected["area"] = buildings_projected.geometry.area
```

### Geometric Operations

Apply geometric transformations and analysis:

```python
# Buffer operations
buffered = buildings.geometry.buffer(100)  # 100 meter buffer

# Geometric properties
buildings["is_valid"] = buildings.geometry.is_valid
buildings["is_simple"] = buildings.geometry.is_simple
buildings["bounds"] = buildings.geometry.bounds

# Distance calculations
from shapely.geometry import Point

reference_point = Point(-73.9857, 40.7484)  # Times Square
buildings["distance_to_times_square"] = buildings.geometry.distance(reference_point)

# Area and length calculations (requires projected CRS)
buildings_projected = buildings.to_crs("EPSG:3857")  # Web Mercator
buildings_projected["area"] = buildings_projected.geometry.area
buildings_projected["perimeter"] = buildings_projected.geometry.length
```

## Performance Considerations

### Use Traditional GeoPandas when:

- Working with small datasets (< 1GB)
- Simple operations on local data
- Complete functional coverage is required
- Single-node processing is sufficient

### Use GeoPandas API for Apache Sedona when:

- Working with large datasets (> 1GB)
- Complex geospatial analyses
- Distributed processing is needed
- Data is stored in cloud storage (S3, HDFS, etc.)

## Supported Operations

The GeoPandas API for Apache Sedona has implemented **39 GeoSeries functions** and **10 GeoDataFrame functions**, covering the most commonly used GeoPandas operations:

### Data I/O

- `read_parquet()` - Read GeoParquet files
- `read_file()` - Read various geospatial formats
- `to_parquet()` - Write to Parquet format

### Spatial Operations

- `sjoin()` - Spatial joins with various predicates
- `buffer()` - Geometric buffering
- `distance()` - Distance calculations
- `intersects()`, `contains()`, `within()` - Spatial predicates
- `sindex` - Spatial indexing (limited functionality)

### CRS Operations

- `set_crs()` - Set coordinate reference system
- `to_crs()` - Transform between CRS
- `crs` - Access CRS information

### Geometric Properties

- `area`, `length`, `bounds` - Geometric measurements
- `is_valid`, `is_simple`, `is_empty` - Geometric validation
- `centroid`, `envelope`, `boundary` - Geometric properties
- `x`, `y`, `z`, `has_z` - Coordinate access
- `total_bounds`, `estimate_utm_crs` - Bounds and CRS utilities

### Spatial Operations

- `buffer()` - Geometric buffering
- `distance()` - Distance calculations
- `intersects()`, `contains()`, `within()` - Spatial predicates
- `intersection()` - Geometric intersection
- `make_valid()` - Geometry validation and repair
- `sindex` - Spatial indexing (limited functionality)

### Data Conversion

- `to_geopandas()` - Convert to traditional GeoPandas
- `to_wkb()`, `to_wkt()` - Convert to WKB/WKT formats
- `from_xy()` - Create geometries from coordinates
- `geom_type` - Get geometry types

## Complete Workflow Example

```python
import sedona.spark.geopandas as gpd
from sedona.spark import SedonaContext

# Configure Spark for anonymous S3 access
config = (
    SedonaContext.builder()
    .config(
        "spark.hadoop.fs.s3a.bucket.wherobots-examples.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)

# Load data
DATA_DIR = "s3://wherobots-examples/data/geopandas_blog/"
overture_size = "1M"
postal_codes_path = DATA_DIR + "postal-code/"
overture_path = DATA_DIR + overture_size + "/" + "overture-buildings/"

postal_codes = gpd.read_parquet(postal_codes_path)
buildings = gpd.read_parquet(overture_path)

# Spatial analysis
buildings = buildings.set_crs("EPSG:4326")
buildings_projected = buildings.to_crs("EPSG:3857")

# Calculate areas and filter
buildings_projected["area"] = buildings_projected.geometry.area
large_buildings = buildings_projected[buildings_projected["area"] > 1000]

result = large_buildings.sjoin(postal_codes, predicate="intersects")

# Aggregate by postal code
summary = (
    result.groupby("postal_code")
    .agg({"area": "sum", "BUILD_ID": "count"})
    .rename(columns={"BUILD_ID": "building_count"})
)

print(summary.head())
```

## Resources and Contributing

For detailed and up-to-date API documentation, including complete method signatures, parameters, and examples, see:

**📚 [GeoPandas API Documentation](https://sedona.apache.org/latest/api/pydocs/sedona.spark.geopandas.html)**

The GeoPandas API for Apache Sedona is an open-source project. Contributions are welcome through the [GitHub issue tracker](https://github.com/apache/sedona/issues/2230) for reporting bugs, requesting features, or contributing code. For more information on contributing, see the [Contributor Guide](../community/develop.md).
