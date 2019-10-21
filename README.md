![GeoSpark Logo](https://github.com/DataSystemsLab/GeoSpark/blob/master/GeoSpark_logo.png?raw=true)

|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU) || [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark / SparkSQL with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs)/ SpatialSQL that efficiently load, process, and analyze large-scale spatial data across machines.

### GeoSpark contains several modules:

| Name  |  API |  Spark compatibility|Introduction|
|---|---|---|---|
|Core  | RDD  | Spark 2.X/1.X  |SpatialRDDs and Query Operators. |
|SQL  | SQL/DataFrame  | SparkSQL 2.1+ |SQL interfaces for GeoSpark core.|
|Viz |  RDD, SQL/DataFrame | RDD - Spark 2.X/1.X, SQL - Spark 2.1+|Visualization for Spatial RDD and DataFrame.|
|Zeppelin |  Apache Zeppelin | Spark 2.1+, Zeppelin 0.8.1+|GeoSpark plugin for Apache Zeppelin|

### GeoSpark supports several programming languages: Scala, Java, SQL, Python and R.

# Please visit [GeoSpark website](http://datasystemslab.github.io/GeoSpark/) for detailed documentations

## News!

* A research paper about ["GeoSparkSim: A Microscopic Road Network Traffic Simulator in Apache Spark"](http://www.public.asu.edu/~jiayu2/geospark/publication/geosparksim-mdm-2019.pdf) is accepted to MDM 2019, Hong Kong China. The next release of GeoSpark will come with a built-in scalable traffic simulator. Please stay tuned!
* A 1.5-hour tutorial about "Geospatial Data Management in Apache Spark" was presented by Jia Yu and Mohamed Sarwat in ICDE 2019, Macau, China. Visit [our tutorial website](https://jiayuasu.github.io/geospatial-tutorial/) to learn how to craft your **"GeoSpark"** from scratch.
* GeoSpark 1.2.0 is released.

## Impact

### GeoSpark Downloads on Maven Central

GeoSpark ecosystem has around 10K downloads per month.

<img src="docs/image/geospark-stat.png" width="500"/>
