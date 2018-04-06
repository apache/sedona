![GeoSpark Logo](https://github.com/DataSystemsLab/GeoSpark/blob/master/GeoSpark_logo.png?raw=true)

|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU) || [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) || [![HitCount](http://hits.dwyl.io/DataSystemsLab/GeoSpark.svg)](http://hits.dwyl.io/DataSystemsLab/GeoSpark)(since Jan. 2018)

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

**Please visit [GeoSpark website](http://datasystemslab.github.io/GeoSpark/) for details and documentations.**

## News!
* GeoSpark 1.1.0 is released. This release contains new SQL functions, custom Quad-Tree/R-Tree index serializers and bug fixes. GeoSpark 1.1.0 supposrt Apache Spark 2.3. **Note, GeoSparkSQL Maven Coordinate changed** [Release notes](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Release-notes/) || [Maven Coordinate](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) (Thanks for the index serializer patch contributed by Zongsi Zhang!)
* GeoSpark wiki is now moved to [GeoSpark new website](http://datasystemslab.github.io/GeoSpark/)! Users are welcome to contribute your tutorials and stories by making a PR!
