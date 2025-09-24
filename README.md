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

[![Apache Sedona](docs/image/sedona_logo.png)](https://sedona.apache.org/)

[![CodeQL Workflow Status](https://github.com/apache/sedona/actions/workflows/codeql.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/codeql.yml)
[![Docker image build](https://github.com/apache/sedona/actions/workflows/docker-build.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docker-build.yml)
[![Docs build](https://github.com/apache/sedona/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docs.yml)
[![Example project build](https://github.com/apache/sedona/actions/workflows/example.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/example.yml)
[![First Interaction Workflow Status](https://github.com/apache/sedona/actions/workflows/first-interaction.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/first-interaction.yml)
[![Labeler Workflow Status](https://github.com/apache/sedona/actions/workflows/labeler.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/labeler.yml)
[![Pre-commit Workflow Status](https://github.com/apache/sedona/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/pre-commit.yml)
[![Python build](https://github.com/apache/sedona/actions/workflows/python.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python.yml)
[![Python Extension build](https://github.com/apache/sedona/actions/workflows/python-extension.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python-extension.yml)
[![Pyflink build](https://github.com/apache/sedona/actions/workflows/pyflink.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/pyflink.yml)
[![Python Wheel build](https://github.com/apache/sedona/actions/workflows/python-wheel.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python-wheel.yml)
[![R build](https://github.com/apache/sedona/actions/workflows/r.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/r.yml)
[![Scala and Java build](https://github.com/apache/sedona/actions/workflows/java.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/java.yml)

[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/apache/sedona)](https://github.com/apache/sedona/graphs/commit-activity)
[![GitHub Issues marked as good first issue](https://img.shields.io/github/issues/apache/sedona/good%20first%20issue?color=%237057ff)](https://github.com/apache/sedona/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22)

## 🚀 **NEW: SedonaDB & SpatialBench - Latest Apache Sedona Subprojects**

**SedonaDB** - A single-node analytical database engine with geospatial as a first-class citizen. Perfect for developers who want Sedona's spatial analytics power without distributed system complexity.

**SpatialBench** - A comprehensive benchmark for assessing geospatial SQL analytics query performance across database systems.

**[Read the full announcement blog post →](https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/)** | **[SedonaDB →](https://sedona.apache.org/sedonadb)** | **[SpatialBench →](https://sedona.apache.org/spatialbench)**

---

| Download statistics        | **Maven**  | **PyPI**                                                                                                                                                                                                                                                                                                                                     | Conda-forge                                                                                                                                     | **CRAN**                                                                                                                                                                                                                                                                                                      | **DockerHub**                                                                                                                  |
|----------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Apache Sedona              | 330k/month | [![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-sedona)](https://pepy.tech/project/apache-sedona) [![Downloads](https://static.pepy.tech/personalized-badge/apache-sedona?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/apache-sedona) | [![Anaconda-Server Badge](https://anaconda.org/conda-forge/apache-sedona/badges/downloads.svg)](https://anaconda.org/conda-forge/apache-sedona) | [![CRAN downloads per month](https://cranlogs.r-pkg.org/badges/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) [![Total CRAN downloads](https://cranlogs.r-pkg.org/badges/grand-total/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) | [![Docker pulls](https://img.shields.io/docker/pulls/apache/sedona?color=brightgreen)](https://hub.docker.com/r/apache/sedona) |
| Archived GeoSpark releases | 10k/month  | [![PyPI - Downloads](https://img.shields.io/pypi/dm/geospark)](https://pepy.tech/project/geospark)[![Downloads](https://static.pepy.tech/personalized-badge/geospark?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/geospark)                      |                                                                                                                                                 |                                                                                                                                                                                                                                                                                                               |                                                                                                                                |

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Join the community](#join-the-community)
- [What is Apache Sedona?](#what-is-apache-sedona)
  - [Features](#features)
- [Apache Sedona subprojects](#apache-sedona-subprojects)
- [When to use Sedona?](#when-to-use-sedona)
  - [Use Cases:](#use-cases)
  - [Code Example:](#code-example)
    - [Load NYC taxi trips and taxi zones data from CSV Files Stored on AWS S3](#load-nyc-taxi-trips-and-taxi-zones-data-from-csv-files-stored-on-aws-s3)
    - [Spatial SQL query to only return Taxi trips in Manhattan](#spatial-sql-query-to-only-return-taxi-trips-in-manhattan)
    - [Spatial Join between Taxi Dataframe and Zone Dataframe to Find taxis in each zone](#spatial-join-between-taxi-dataframe-and-zone-dataframe-to-find-taxis-in-each-zone)
    - [Show a map of the loaded Spatial Dataframes using GeoPandas](#show-a-map-of-the-loaded-spatial-dataframes-using-geopandas)
- [Docker image](#docker-image)
- [Building Sedona](#building-sedona)
- [Documentation](#documentation)
- [Star History](#star-history)
- [Powered by](#powered-by)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Join the community

Everyone is welcome to join our community events. We have a community office hour every 4 weeks. Please register to the event you want to attend: https://bit.ly/3UBmxFY

Please join our Discord community!

[![Discord](https://dcbadge.limes.pink/api/server/https://discord.gg/9A3k5dEBsY)](https://discord.gg/9A3k5dEBsY)

* [Apache Sedona@LinkedIn](https://www.linkedin.com/company/apache-sedona)
* [Apache Sedona@X](https://X.com/ApacheSedona)
* [Sedona JIRA](https://issues.apache.org/jira/projects/SEDONA): bug reports and feature requests
* [Sedona GitHub Issues](https://github.com/apache/sedona/issues?q=sort%3Aupdated-desc+is%3Aissue+is%3Aopen): bug reports and feature requests
* [Sedona GitHub Discussion](https://github.com/apache/sedona/discussions): project development and general questions
* [Sedona Mailing Lists](https://lists.apache.org/list.html?sedona.apache.org): [dev@sedona.apache.org](https://lists.apache.org/list.html?dev@sedona.apache.org): project development and general questions

For the mailing list, Please first subscribe and then post emails. To subscribe, please send an email (leave the subject and content blank) to [dev-subscribe@sedona.apache.org](mailto:dev-subscribe@sedona.apache.org?subject=Subscribe&body=Subscribe)

## What is Apache Sedona?

Apache Sedona™ is a [spatial computing](https://en.wikipedia.org/wiki/Spatial_computing) engine that enables developers to easily process spatial data at any scale within modern cluster computing systems such as [Apache Spark](https://spark.apache.org/) and [Apache Flink](https://flink.apache.org/).
Sedona developers can express their spatial data processing tasks in [Spatial SQL](https://carto.com/spatial-sql), [Spatial Python](https://docs.scipy.org/doc/scipy/reference/spatial.html) or [Spatial R](https://r-spatial.org/). Internally, Sedona provides spatial data loading, indexing, partitioning, and query processing/optimization functionality that enable users to efficiently analyze spatial data at any scale.

![Sedona Ecosystem](docs/image/sedona-ecosystem.png "Sedona Ecosystem")

### Features

Some of the key features of Apache Sedona include:

* Support for a wide range of geospatial data formats, including [GeoJSON](https://en.wikipedia.org/wiki/GeoJSON), [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry), and [ESRI](https://www.esri.com) [Shapefile](https://en.wikipedia.org/wiki/Shapefile).
* Scalable distributed processing of large vector and raster datasets.
* Tools for spatial indexing, spatial querying, and spatial join operations.
* Integration with popular geospatial Python tools such as [GeoPandas](https://geopandas.org).
* Integration with popular big data tools, such as Spark, [Hadoop](https://hadoop.apache.org/), [Hive](https://hive.apache.org/), and Flink for data storage and querying.
* A user-friendly API for working with geospatial data in the [SQL](https://en.wikipedia.org/wiki/SQL), [Python](https://www.python.org/), [Scala](https://www.scala-lang.org/) and [Java](https://www.java.com) languages.
* Flexible deployment options, including standalone, local, and cluster modes.

These are some of the key features of Apache Sedona, but it may offer additional capabilities depending on the specific version and configuration.

## Apache Sedona subprojects

* **SedonaDB**: A single-node analytical database engine with geospatial as a first-class citizen - [GitHub](https://github.com/apache/sedona-db) | [Website](https://sedona.apache.org/sedonadb)
* **SpatialBench**: A benchmark for assessing geospatial SQL analytics query performance across database systems - [GitHub](https://github.com/apache/sedona-spatialbench) | [Website](https://sedona.apache.org/spatialbench)

## When to use Sedona?

### Use Cases:

Apache Sedona is a widely used framework for working with spatial data, and it has many different use cases and applications. Some of the main use cases for Apache Sedona include:

* Automotive data analytics: Apache Sedona is widely used in geospatial analytics applications, where it is used to perform spatial analysis and data mining on large and complex datasets collected from fleets.
* Urban planning and development: Apache Sedona is commonly used in urban planning and development applications to analyze and visualize spatial data sets related to urban environments, such as land use, transportation networks, and population density.
* Location-based services: Apache Sedona is often used in location-based services, such as mapping and navigation applications, where it is used to process and analyze spatial data to provide location-based information and services to users.
* Environmental modeling and analysis: Apache Sedona is used in many different environmental modeling and analysis applications, where it is used to process and analyze spatial data related to environmental factors, such as air quality, water quality, and weather patterns.
* Disaster response and management: Apache Sedona is used in disaster response and management applications to process and analyze spatial data related to disasters, such as floods, earthquakes, and other natural disasters, in order to support emergency response and recovery efforts.

### Code Example:

This example loads NYC taxi trip records and taxi zone information stored as .CSV files on AWS S3 into Sedona spatial dataframes. It then performs spatial SQL query on the taxi trip datasets to filter out all records except those within the Manhattan area of New York. The example also shows a spatial join operation that matches taxi trip records to zones based on whether the taxi trip lies within the geographical extents of the zone. Finally, the last code snippet integrates the output of Sedona with GeoPandas and plots the spatial distribution of both datasets.

#### Load NYC taxi trips and taxi zones data from CSV Files Stored on AWS S3

```python
taxidf = (
    sedona.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("s3a://your-directory/data/nyc-taxi-data.csv")
)
taxidf = taxidf.selectExpr(
    "ST_Point(CAST(Start_Lon AS Decimal(24,20)), CAST(Start_Lat AS Decimal(24,20))) AS pickup",
    "Trip_Pickup_DateTime",
    "Payment_Type",
    "Fare_Amt",
)
```

```python
zoneDf = (
    sedona.read.format("csv")
    .option("delimiter", ",")
    .load("s3a://your-directory/data/TIGER2018_ZCTA5.csv")
)
zoneDf = zoneDf.selectExpr("ST_GeomFromWKT(_c0) as zone", "_c1 as zipcode")
```

#### Spatial SQL query to only return Taxi trips in Manhattan

```python
taxidf_mhtn = taxidf.where(
    "ST_Contains(ST_PolygonFromEnvelope(-74.01,40.73,-73.93,40.79), pickup)"
)
```

#### Spatial Join between Taxi Dataframe and Zone Dataframe to Find taxis in each zone

```python
taxiVsZone = sedona.sql(
    "SELECT zone, zipcode, pickup, Fare_Amt FROM zoneDf, taxiDf WHERE ST_Contains(zone, pickup)"
)
```

#### Show a map of the loaded Spatial Dataframes using GeoPandas

```python
zoneGpd = gpd.GeoDataFrame(zoneDf.toPandas(), geometry="zone")
taxiGpd = gpd.GeoDataFrame(taxidf.toPandas(), geometry="pickup")

zone = zoneGpd.plot(color="yellow", edgecolor="black", zorder=1)
zone.set_xlabel("Longitude (degrees)")
zone.set_ylabel("Latitude (degrees)")

zone.set_xlim(-74.1, -73.8)
zone.set_ylim(40.65, 40.9)

taxi = taxiGpd.plot(ax=zone, alpha=0.01, color="red", zorder=3)
```

## Docker image

We provide a Docker image for Apache Sedona with Python JupyterLab and a single-node cluster. The images are available on [DockerHub](https://hub.docker.com/r/apache/sedona)

## Building Sedona

* To install the Python package:

  ```
  pip install apache-sedona
  ```

* To compile the source code, please refer to [Sedona website](https://sedona.apache.org/latest/setup/compile/)

* Modules in the source code

| Name             | API                                      | Introduction                                           |
|------------------|------------------------------------------|--------------------------------------------------------|
| common           | Java                                     | Core geometric operation logics, serialization, index  |
| spark            | Spark RDD/DataFrame Scala/Java/SQL       | Distributed geospatial data processing on Apache Spark |
| flink            | Flink DataStream/Table in Scala/Java/SQL | Distributed geospatial data processing on Apache Flink |
| snowflake        | Snowflake SQL                            | Distributed geospatial data processing on Snowflake    |
| spark-shaded     | No source code                           | shaded jar for Sedona Spark                            |
| flink-shaded     | No source code                           | shaded jar for Sedona Flink                            |
| snowflake-tester | Java                                     | tester program for Sedona Snowflake                    |
| python           | Spark RDD/DataFrame Python               | Distributed geospatial data processing on Apache Spark |
| R                | Spark RDD/DataFrame in R                 | R wrapper for Sedona                                   |
| Zeppelin         | Apache Zeppelin                          | Plugin for Apache Zeppelin 0.8.1+                      |

## Documentation

* [Spatial SQL in Sedona](https://sedona.apache.org/latest/tutorial/sql/)
* [Integrate with GeoPandas and Shapely](https://sedona.apache.org/latest/tutorial/geopandas-shapely/)
* [Working with Spatial R in Sedona](https://sedona.apache.org/latest/api/rdocs/)
* [Sedona Python API Documentation](https://sedona.apache.org/latest/api/pydocs/)

Please visit [Apache Sedona website](http://sedona.apache.org/) for detailed information

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=apache/sedona&type=Date)](https://www.star-history.com/#apache/sedona&Date)

## Powered by

<a href="https://www.apache.org/">
  <img alt="The Apache Software Foundation" class="center" src="https://www.apache.org/foundation/press/kit/asf_logo_wide.png"
    title="The Apache Software Foundation" width="500">
</a>
