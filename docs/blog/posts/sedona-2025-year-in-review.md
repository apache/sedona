---
date:
  created: 2026-01-11
links:
  - Release notes: https://sedona.apache.org/latest/setup/release-notes/
  - SedonaDB: https://sedona.apache.org/sedonadb/
  - SpatialBench: https://sedona.apache.org/spatialbench/
  - Apache Parquet and Iceberg native geo type: https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/
authors:
  - jia
title: "Apache Sedona 2025 Year in Review"
---

<!--
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
-->

2025 was a milestone year for **Apache Sedona**. We made major progress in distributed spatial analytics on Spark, Flink, and Snowflake, launched a new single-node engine called SedonaDB, and pushed forward benchmarking and open geospatial data standards.

This post summarizes the most important highlights from the Apache Sedona ecosystem in 2025.

<!-- more -->

## Apache Sedona Ecosystem Releases in 2025

Apache Sedona shipped four releases from January 2025 to January 2026: 1.7.1, 1.7.2, 1.8.0, and 1.8.1. In the same year, the Sedona ecosystem expanded in two major ways: we introduced SedonaDB for fast single-machine analytics and SpatialBench to make spatial performance comparisons reproducible.

- Apache Sedona releases: Ongoing improvements across distributed engines and integrations (Spark, Flink, Snowflake). See the release notes for details.
- SedonaDB: A new single-node spatial engine built for interactive analytics and developer workflows.
- SpatialBench: A benchmark suite designed to standardize how we evaluate spatial SQL performance across engines.

Release notes: [https://sedona.apache.org/latest/setup/release-notes/](https://sedona.apache.org/latest/setup/release-notes/)

## Distributed Engines Highlights

Across SedonaSpark, SedonaFlink, and SedonaSnow, 2025 brought major usability improvements, broader SQL coverage, and better support for modern open geospatial data formats:

* GeoPandas API on SedonaSpark: Write GeoPandas-style code, but run it on Spark through Sedona, so familiar workflows like spatial joins (`sjoin`), buffering, distance, and coordinate system transforms can scale beyond a single machine. Learn more: [GeoPandas API for Apache Sedona](../../tutorial/geopandas-api.md).
* GeoStats for clustering, outliers, and hot spots: Built-in tools for common spatial statistics workflows on DataFrames, including DBSCAN clustering, Local Outlier Factor (LOF), and Getis-Ord Gi/Gi* hot spot analysis. Learn more: [Stats module](../../api/stats/sql.md).
* Faster SedonaSpark to GeoPandas conversion with GeoArrow: Convert query results to GeoPandas more efficiently using Arrow/GeoArrow, such as `geopandas.GeoDataFrame.from_arrow(dataframe_to_arrow(df))`. Learn more: [GeoPandas + Shapely interoperability](../../tutorial/geopandas-shapely.md).
* STAC catalog reader: Load STAC collections from local files, S3, or HTTPS endpoints using `sedona.read.format("stac")`, and apply time/area filters early so you read less data. Supports authenticated STAC APIs too. Learn more: [STAC catalog with Apache Sedona and Spark](../../tutorial/files/stac-sedona-spark.md).
* More built-in data sources: Easier ingestion from formats people use in practice, including GeoPackage and OSM PBF (OpenStreetMap). Learn more: [SedonaSQL / DataFrame I/O tutorial](../../tutorial/sql.md).
* Vectorized UDFs (Python): A faster way to run Python UDFs by processing data in batches using Apache Arrow, including geometry-aware UDFs with Shapely or GeoPandas GeoSeries. Learn more: [Spatial vectorized UDFs (Python only)](../../tutorial/sql.md).
* More functions across engines: Function coverage kept expanding across Spark, Flink, and Snowflake. For example: ST_ApproximateMedialAxis, ST_StraightSkeleton, ST_Collect_Agg, and ST_OrientedEnvelope. See the function catalogs for [SedonaSpark SQL](../../api/sql/Overview.md), [SedonaFlink SQL](../../api/flink/Overview.md), and [SedonaSnow SQL](../../api/snowflake/vector-data/Overview.md).

## SedonaDB: A New Single-Node Spatial Engine

One of the biggest developments in 2025 was the introduction of SedonaDB, a new analytics engine designed for geospatial data on a single machine.

SedonaDB was announced in September 2025 and represents a new direction for the Sedona project family. It is written in Rust and built on Apache Arrow and DataFusion, enabling fast columnar execution with a lightweight deployment model.

SedonaDB shipped two releases in 2025: 0.1.0 (initial release) and 0.2.0 (major expansion).

The initial 0.1.0 release introduced the core engine with native geometry and geography types, built-in spatial indexing, and optimized spatial joins and nearest-neighbor queries, with Python and SQL interfaces and a zero-setup, embedded-style experience.

SedonaDB 0.2.0, released in December 2025, rapidly expanded the engine with broader spatial SQL coverage including raster, native support for reading GDAL and OGR compatible formats, GeoParquet 1.1 write support with bounding box metadata, Python UDF support, and initial raster data type support.

Blog posts:

* [Introducing SedonaDB](https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/)
* [SedonaDB 0.2.0 Release](https://sedona.apache.org/latest/blog/2025/12/01/sedonadb-020-release/)

## SpatialBench: Standardizing Spatial Performance Evaluation

Another major milestone in 2025 was the introduction of SpatialBench, a benchmark suite designed specifically for spatial SQL workloads.

Traditional database benchmarks often miss the patterns that matter most in geospatial analytics, such as spatial joins, distance filters, and spatial aggregations. SpatialBench was created to address this gap.

SpatialBench provides:

* Realistic spatial datasets
* Configurable scale factors
* Reproducible query workloads
* Comparable results across engines

The first SpatialBench release evaluated SedonaDB, DuckDB with spatial extensions, and GeoPandas, offering transparent and reproducible performance comparisons.

Blog post: [Introducing SpatialBench](https://sedona.apache.org/latest/blog/2025/12/11/introducing-spatialbench-performance-benchmarks-for-spatial-database-queries/)

## Advancing Open Geospatial Data Formats

2025 was also a turning point for geospatial interoperability. Apache Iceberg and Apache Parquet gained native geometry and geography type support, making it easier to store spatial data directly in open lakehouse tables.

This advancement enables:

* Open and vendor neutral spatial storage
* Reliable transactions for geospatial tables
* Filtering data early so engines can scan less
* Seamless interoperability across engines

Apache Sedona and the broader geospatial community played an active role in driving this effort forward.

Blog post: [Apache Iceberg and Parquet now support Geo](https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/)

## Community and Ecosystem Growth

Beyond technical milestones, 2025 saw continued growth in the Apache Sedona community:

* New committers and contributors joined the project
    - New committers: Pranav Toggi, Peter Nguyen, Dewey Dunnington
    - New PMC member: Feng Zhang
* More contributors participated across the year
    - In 2025, 27 new contributors made their first contribution to the Apache Sedona repository for SedonaSpark, SedonaFlink, and SedonaSnow, bringing the project to 155 total contributors. In total, 46 people contributed to Apache Sedona in 2025.
    - New contributors across the ecosystem:
        - SedonaDB: 26 new contributors in 2025
        - SpatialBench: 8 new contributors in 2025
* Adoption continued to grow
    - Total downloads of Apache Sedona have exceeded 65 million overall.
    - Monthly downloads are now more than 2 million.
    - Commit activity increased from 1,509 commits in 2024 to 2,137 commits in 2025.

Sedona’s evolution into a multi-engine, multi-deployment ecosystem reflects both community demand and sustained contributor effort.

## Looking Ahead to 2026

With strong momentum across distributed analytics, single-node engines, benchmarking, and open formats, Apache Sedona enters 2026 well positioned for further growth.

Areas of continued focus include:

* Deeper raster analytics support
* Expanded SpatialBench coverage
* Tighter integration with Iceberg native spatial features
* Improved developer experience across Python, SQL, and Rust

Spatial analytics is becoming a core capability in modern data platforms, and Apache Sedona is increasingly positioned as a foundational project in that landscape.

Thank you to everyone in the community who contributed to making 2025 such a productive year.

## References

* Sedona 1.7.1, 1.7.2, 1.8.0, and 1.8.1 release notes: [https://sedona.apache.org/latest/setup/release-notes/](https://sedona.apache.org/latest/setup/release-notes/)
* SedonaDB 0.1.0 release notes: [https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/](https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/)
* SedonaDB 0.2.0 release notes: [https://sedona.apache.org/latest/blog/2025/12/01/sedonadb-020-release/](https://sedona.apache.org/latest/blog/2025/12/01/sedonadb-020-release/)
* SpatialBench release notes: [https://sedona.apache.org/latest/blog/2025/12/11/introducing-spatialbench-performance-benchmarks-for-spatial-database-queries/](https://sedona.apache.org/latest/blog/2025/12/11/introducing-spatialbench-performance-benchmarks-for-spatial-database-queries/)
* Apache Parquet and Iceberg native geo type: [https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/](https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/)
