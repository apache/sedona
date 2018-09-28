![GeoSpark Logo](https://github.com/DataSystemsLab/GeoSpark/blob/master/GeoSpark_logo.png?raw=true)

|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU) || [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) || [![HitCount](http://hits.dwyl.io/DataSystemsLab/GeoSpark.svg)](http://hits.dwyl.io/DataSystemsLab/GeoSpark)(since Jan. 2018)

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

GeoSpark development team has published four papers about GeoSpark. Please read [Publications](http://datasystemslab.github.io/GeoSpark/contact/publication/). 

GeoSpark received an evaluation from PVLDB 2018 paper ["How Good Are Modern Spatial Analytics Systems?"](http://www.vldb.org/pvldb/vol11/p1661-pandey.pdf) Varun Pandey, Andreas Kipf, Thomas Neumann, Alfons Kemper (Technical University of Munich): 
> GeoSpark comes close to a complete spatial analytics system. It also exhibits the best performance in most cases.


**Please visit [GeoSpark website](http://datasystemslab.github.io/GeoSpark/) for details and documentations.**

## News!
* The full [research paper](http://www.public.asu.edu/~jiayu2/geospark/publication/GeoSpark_Geoinformatica_2018.pdf) of GeoSpark has been accepted by Geoinformatica Journal. This paper has over 40 pages to dissect GeoSpark in details and compare it with many other existing systems such as Magellan, Simba, and SpatialHadoop.
* GeoSpark 1.1.3 is released. This release contains a critical bug fix for GeoSpark-core RDD API. [Release notes](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Release-notes/) || [Maven Coordinate](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/).
* GeoSpark 1.1.2 is released. This release contains several bug fixes. Thanks for the patch from Lucas C.! [Release notes](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Release-notes/) || [Maven Coordinate](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/).
