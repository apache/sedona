![GeoSpark Logo](https://github.com/DataSystemsLab/GeoSpark/blob/master/GeoSpark_logo.png?raw=true)

|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU) || [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) || [![HitCount](http://hits.dwyl.io/DataSystemsLab/GeoSpark.svg)](http://hits.dwyl.io/DataSystemsLab/GeoSpark)(since Jan. 2018)

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark / SparkSQL with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs)/ SpatialSQL that efficiently load, process, and analyze large-scale spatial data across machines.

GeoSpark contains several modules:

| Name  |  API |  Spark compatibility|Dependency|
|---|---|---|---|
| GeoSpark-core  | RDD  | Spark 2.X/1.X  | Spark-core|
| GeoSpark-SQL  | SQL/DataFrame  | SparkSQL 2.1 and later | Spark-core, Spark-SQL, GeoSpark-core|
|  GeoSpark-Viz |  RDD, SQL/DataFrame | RDD - Spark 2.X/1.X, SQL - Spark 2.1 and later|Spark-core, Spark-SQL, GeoSpark-core, GeoSpark-SQL|
|  GeoSpark-Zeppelin |  Apache Zeppelin | Spark 2.1+, Zeppelin 0.8.1+|Spark-core, Spark-SQL, GeoSpark-core, GeoSpark-SQL, GeoSpark-Viz|

* Core: GeoSpark SpatialRDDs and Query Operators. 
* SQL: SQL interfaces for GeoSpark core.
* Viz: Visualization extension of GeoSpark Spatial RDD and DataFrame.
* GeoSpark-Zeppelin: GeoSpark visualization plugin for Apache Zeppelin

**Please visit [GeoSpark website](http://datasystemslab.github.io/GeoSpark/) for details and documentations.**

## News!

* GeoSpark 1.2.0 is released.
	* Tons of bug fixes and new functions! Please read [GeoSpark release note](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Release-notes/).
	* GeoSparkViz now supports DataFrame API. Please read [Visualize Spatial DataFrame/RDD](http://datasystemslab.github.io/GeoSpark/tutorial/viz/).
	* GeoSpark-Zeppelin can connect GeoSpark to [Apache Zeppelin](https://zeppelin.apache.org/). Please read [Interact with GeoSpark via Zeppelin](http://datasystemslab.github.io/GeoSpark/tutorial/zeppelin/)
	* GeoSparkViz Maven coordinate change. Please read [Maven coordinate](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/#geospark-viz-120-and-later).
	* This release includes the PR from 13 contributors. Please read [GeoSpark release note](download/GeoSpark-All-Modules-Release-notes/) to learn their names.
* The full [research paper](http://www.public.asu.edu/~jiayu2/geospark/publication/GeoSpark_Geoinformatica_2018.pdf) of GeoSpark has been accepted by Geoinformatica Journal. This paper has over 40 pages to dissect GeoSpark in details and compare it with many other existing systems such as Magellan, Simba, and SpatialHadoop.

## Social impact

GeoSpark development team has published four papers about GeoSpark. Please read [Publications](http://datasystemslab.github.io/GeoSpark/contact/publication/). 

GeoSpark received an evaluation from PVLDB 2018 paper ["How Good Are Modern Spatial Analytics Systems?"](http://www.vldb.org/pvldb/vol11/p1661-pandey.pdf) Varun Pandey, Andreas Kipf, Thomas Neumann, Alfons Kemper (Technical University of Munich), quoted as follows: 
> GeoSpark comes close to a complete spatial analytics system. It also exhibits the best performance in most cases.
