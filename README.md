<img src="./sedona_logo.png" width="400">

## We are working on importing GeoSpark to Apache Sedona infra. The structure of this repo may change frequently in the weeks to come.


|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[Sedona@Twitter](https://twitter.com/ApacheSedona) || [Sedona Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Apache Sedona (incubating) is a cluster computing system for processing large-scale spatial data. Sedona extends Apache Spark / SparkSQL with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs)/ SpatialSQL that efficiently load, process, and analyze large-scale spatial data across machines.

### Sedona contains several modules:

| Name  |  API |  Spark compatibility|Introduction|
|---|---|---|---|
|Core  | RDD  | Spark 2.X/1.X  |SpatialRDDs and Query Operators. |
|SQL  | SQL/DataFrame  | SparkSQL 2.1+ |SQL interfaces for GeoSpark core.|
|Viz |  RDD, SQL/DataFrame | RDD - Spark 2.X/1.X, SQL - Spark 2.1+|Visualization for Spatial RDD and DataFrame.|
|Zeppelin |  Apache Zeppelin | Spark 2.1+, Zeppelin 0.8.1+|GeoSpark plugin for Apache Zeppelin|

### Sedona supports several programming languages: Scala, Java, SQL, Python and R.

# Please visit [Apache Sedona website](https://jiayuasu.github.io/sedona/) for detailed documentations

## News!
* GeoSpark 1.3.1 is released. This version provides a complete Python wrapper to GeoSpark RDD and SQL API. It also contains a number of bug fixes and new functions from 12 contributors. See [Python tutorial: RDD](https://datasystemslab.github.io/GeoSpark/tutorial/geospark-core-python/), [Python tutorial: SQL](https://datasystemslab.github.io/GeoSpark/tutorial/geospark-sql-python/), [Release note](https://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Release-notes/)

## Powered by

<img src="./incubator_logo.png" width="400">
