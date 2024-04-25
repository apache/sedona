[![Apache Sedona](docs/image/sedona_logo.png)](https://sedona.apache.org/)

[![Scala and Java build](https://github.com/apache/sedona/actions/workflows/java.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/java.yml) [![Python build](https://github.com/apache/sedona/actions/workflows/python.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python.yml) [![R build](https://github.com/apache/sedona/actions/workflows/r.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/r.yml) [![Docker image build](https://github.com/apache/sedona/actions/workflows/docker-build.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docker-build.yml) [![Example project build](https://github.com/apache/sedona/actions/workflows/example.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/example.yml) [![Docs build](https://github.com/apache/sedona/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docs.yml)

| Download statistics        | **Maven**  | **PyPI**                                                                                                                                                                                                                                                                                                                                     | Conda-forge                                                                                                                                     | **CRAN**                                                                                                                                                                                                                                                          | **DockerHub**                                                                                                                  |
|----------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Apache Sedona              | 225k/month | [![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-sedona)](https://pepy.tech/project/apache-sedona) [![Downloads](https://static.pepy.tech/personalized-badge/apache-sedona?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/apache-sedona) | [![Anaconda-Server Badge](https://anaconda.org/conda-forge/apache-sedona/badges/downloads.svg)](https://anaconda.org/conda-forge/apache-sedona) | [![](https://cranlogs.r-pkg.org/badges/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) [![](https://cranlogs.r-pkg.org/badges/grand-total/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) | [![Docker pulls](https://img.shields.io/docker/pulls/apache/sedona?color=brightgreen)](https://hub.docker.com/r/apache/sedona) |
| Archived GeoSpark releases | 10k/month  | [![PyPI - Downloads](https://img.shields.io/pypi/dm/geospark)](https://pepy.tech/project/geospark)[![Downloads](https://static.pepy.tech/personalized-badge/geospark?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/geospark)                      |                                                                                                                                                 |                                                                                                                                                                                                                                                                   |                                                                                                                                |

* [Join the community](#join-the-community)
* [What is Apache Sedona?](#what-is-apache-sedona)
  * [Features](#features)
* [When to use Sedona?](#when-to-use-sedona)
  * [Use Cases](#use-cases)
  * [Code Example](#code-example)
* [Docker image](#docker-image)
* [Building Sedona](#building-sedona)
* [Documentation](#documentation)
* [Powered by](#powered-by)

## Join the community

Follow Sedona on Twitter for fresh news: [Sedona@Twitter](https://twitter.com/ApacheSedona)

Join the Sedona Discord community: [![](https://dcbadge.vercel.app/api/server/9A3k5dEBsY)](https://share.hsforms.com/1Ndql_ZigTdmLlVQc_d1o4gqga4q)

Join the Sedona monthly community office hour: [Google Calendar](https://calendar.google.com/calendar/event?action=TEMPLATE&tmeid=NjI0cWgwcTZndnI1anAzYnFrNHY5Y2wyaTRfMjAyNDA0MDlUMTUwMDAwWiBjX2VmN2Q1NGY1MzA4YTRiN2YyNWFjMzNkMGY3ZWViNTRhM2E3ZjExNWI2ODlmYWY0ZDgyNDI1ZjNjYjVlZGU5MzVAZw&tmsrc=c_ef7d54f5308a4b7f25ac33d0f7eeb54a3a7f115b689faf4d82425f3cb5ede935%40group.calendar.google.com&scp=ALL), Tuesdays from 8 AM to 9 AM Pacific Time, every 4 weeks

[Sedona JIRA](https://issues.apache.org/jira/projects/SEDONA): Bugs, Pull Requests, and other similar issues

[Sedona Mailing Lists](https://lists.apache.org/list.html?sedona.apache.org): [dev@sedona.apache.org](https://lists.apache.org/list.html?dev@sedona.apache.org): project development, general questions or tutorials.

* Please first subscribe and then post emails. To subscribe, please send an email (leave the subject and content blank) to dev-subscribe@sedona.apache.org

## What is Apache Sedona?

Apache Sedona™ is a spatial computing engine that enables developers to easily process spatial data at any scale within modern cluster computing systems such as Apache Spark and Apache Flink. Sedona developers can express their spatial data processing tasks in Spatial SQL, Spatial Python or Spatial R. Internally, Sedona provides spatial data loading, indexing, partitioning, and query processing/optimization functionality that enable users to efficiently analyze spatial data at any scale.

![](docs/image/sedona-ecosystem.png "Sedona Ecosystem")

### Features

Some of the key features of Apache Sedona include:

* Support for a wide range of geospatial data formats, including GeoJSON, WKT, and ESRI Shapefile.
* Scalable distributed processing of large vector and raster datasets.
* Tools for spatial indexing, spatial querying, and spatial join operations.
* Integration with popular geospatial python tools such as GeoPandas.
* Integration with popular big data tools, such as Spark, Hadoop, Hive, and Flink for data storage and querying.
* A user-friendly API for working with geospatial data in the SQL, Python, Scala and Java languages.
* Flexible deployment options, including standalone, local, and cluster modes.

These are some of the key features of Apache Sedona, but it may offer additional capabilities depending on the specific version and configuration.

Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=docs/usecases) and play the interactive Sedona Python Jupyter Notebook immediately!

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
taxidf = sedona.read.format('csv').option("header","true").option("delimiter", ",").load("s3a://your-directory/data/nyc-taxi-data.csv")
taxidf = taxidf.selectExpr('ST_Point(CAST(Start_Lon AS Decimal(24,20)), CAST(Start_Lat AS Decimal(24,20))) AS pickup', 'Trip_Pickup_DateTime', 'Payment_Type', 'Fare_Amt')
```

```python
zoneDf = sedona.read.format('csv').option("delimiter", ",").load("s3a://your-directory/data/TIGER2018_ZCTA5.csv")
zoneDf = zoneDf.selectExpr('ST_GeomFromWKT(_c0) as zone', '_c1 as zipcode')
```

#### Spatial SQL query to only return Taxi trips in Manhattan

```python
taxidf_mhtn = taxidf.where('ST_Contains(ST_PolygonFromEnvelope(-74.01,40.73,-73.93,40.79), pickup)')
```

#### Spatial Join between Taxi Dataframe and Zone Dataframe to Find taxis in each zone

```python
taxiVsZone = sedona.sql('SELECT zone, zipcode, pickup, Fare_Amt FROM zoneDf, taxiDf WHERE ST_Contains(zone, pickup)')
```

#### Show a map of the loaded Spatial Dataframes using GeoPandas

```python
zoneGpd = gpd.GeoDataFrame(zoneDf.toPandas(), geometry="zone")
taxiGpd = gpd.GeoDataFrame(taxidf.toPandas(), geometry="pickup")

zone = zoneGpd.plot(color='yellow', edgecolor='black', zorder=1)
zone.set_xlabel('Longitude (degrees)')
zone.set_ylabel('Latitude (degrees)')

zone.set_xlim(-74.1, -73.8)
zone.set_ylim(40.65, 40.9)

taxi = taxiGpd.plot(ax=zone, alpha=0.01, color='red', zorder=3)
```

## Docker image

We provide a Docker image for Apache Sedona with Python JupyterLab and a single-node cluster. The images are available on [DockerHub](https://hub.docker.com/r/apache/sedona)

## Building Sedona

* To install the Python package:

  ```
  pip install apache-sedona
  ```

* To compile the source code, please refer to [Sedona website](https://sedona.apache.org/latest-snapshot/setup/compile/)

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

* [Spatial SQL in Sedona](https://sedona.apache.org/latest-snapshot/tutorial/sql/)
* [Integrate with GeoPandas and Shapely](https://sedona.apache.org/latest-snapshot/tutorial/geopandas-shapely/)
* [Working with Spatial R in Sedona](https://sedona.apache.org/latest-snapshot/api/rdocs/)

Please visit [Apache Sedona website](http://sedona.apache.org/) for detailed information

## Powered by

<a href="https://www.apache.org/">
  <img alt="The Apache Software Foundation" src="https://www.apache.org/foundation/press/kit/asf_logo_wide.png" width="500" class="center">
</a>
