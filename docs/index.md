#

![GeoSpark Logo](https://github.com/DataSystemsLab/GeoSpark/raw/master/GeoSpark_logo.png)

|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](./download/GeoSpark-All-Modules-Maven-Central-Coordinates.md) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](./download/GeoSpark-All-Modules-Maven-Central-Coordinates.md) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU) || [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) || [![HitCount](http://hits.dwyl.io/DataSystemsLab/GeoSpark.svg)](http://hits.dwyl.io/DataSystemsLab/GeoSpark)(since Jan. 2018)

#### News
* GeoSpark 1.1.0 is released. This release contains new SQL functions, custom Quad-Tree/R-Tree index serializers and bug fixes. GeoSpark 1.1.0 supposrt Apache Spark 2.3. **Note, GeoSparkSQL Maven Coordinate changed** [Release notes](./download/GeoSpark-All-Modules-Release-notes.md) || [Maven Coordinate](./download/GeoSpark-All-Modules-Maven-Central-Coordinates.md) (Thanks for the index serializer patch contributed by Zongsi Zhang!)
* GeoSpark wiki is now moved to [GeoSpark new website](http://datasystemslab.github.io/GeoSpark/)! Users are welcome to contribute your tutorials and stories by making a PR!

## Introduction

GeoSpark is listed as **Infrastructure Project** on [**Apache Spark Official Third Party Project Page**](http://spark.apache.org/third-party-projects.html)

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark / SparkSQL with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs)/ SpatialSQL that efficiently load, process, and analyze large-scale spatial data across machines.

GeoSpark contains three modules:

| Name  |  API |  Spark compatibility|Dependency|
|---|---|---|---|
| GeoSpark-core  | RDD  | Spark 2.X/1.X  | Spark-core|
| GeoSpark-SQL  | SQL/DataFrame  | SparkSQL 2.1 and later | Spark-core, Spark-SQL, GeoSpark-core|
|  GeoSpark-Viz |  RDD | Spark 2.X/1.X |Spark-core, GeoSpark-core|

* Core: GeoSpark SpatialRDDs and Query Operators. 
* SQL: SQL interfaces for GeoSpark core.
* Viz: Visualization extension of GeoSpark core.

## Features

* ==Spatial RDD==
* ==Spatial SQL==
```SQL
SELECT superhero.name
FROM city, superhero
WHERE ST_Contains(city.geom, superhero.geom)
AND city.name = 'Gotham';
```
* ==Complex geometries / trajectories==: point, polygon, linestring, multi-point, multi-polygon, multi-linestring, GeometryCollection
* ==Various input formats==: CSV, TSV, WKT, WKB, GeoJSON, NASA NetCDF/HDF, Shapefile (.shp, .shx, .dbf): extension must be in lower case
* ==Spatial query==: range query, range join query, distance join query, K Nearest Neighbor query
* ==Spatial index==: R-Tree, Quad-Tree
* ==Spatial partitioning==: KDB-Tree, Quad-Tree, R-Tree, Voronoi diagram, Hilbert curve, Uniform grids
* ==Coordinate Reference System / Spatial Reference System Transformation==: for exmaple, from WGS84 (EPSG:4326, degree-based), to EPSG:3857 (meter-based)
* ==High resolution map==: Scatter plot, heat map, choropleth map

## Companies are using GeoSpark 

(incomplete list)

[<img src="https://www.bluedme.com/wp-content/uploads/2015/10/cropped-LOGO-Blue-DME-PNG-3.png" width="150">](https://www.bluedme.com/) [<img src="https://retailrecharged.com/wp-content/uploads/2017/10/logo.png" width="150">](https://www.gyana.co.uk/)

Please make a Pull Request to add yourself!



## GeoSpark Visualization Extension (GeoSparkViz)
GeoSparkViz is a large-scale in-memory geospatial visualization system.

GeoSparkViz provides native support for general cartographic design by extending GeoSpark to process large-scale spatial data. It can visulize Spatial RDD and Spatial Queries and render super high resolution image in parallel.

More details are available here: [GeoSpark Visualization Extension](https://github.com/DataSystemsLab/GeoSpark/tree/master/viz) 

**GeoSparkViz Gallery**


<img style="float: left;" src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="250">
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/ustweet.png" width="250">

[Watch the high resolution version on a real map](http://www.public.asu.edu/~jiayu2/geospark/picture/overlay.html)

<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/heatmapnycsmall.png" width="500">

