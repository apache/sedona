![GeoSpark Logo](https://github.com/DataSystemsLab/GeoSpark/blob/master/GeoSpark_logo.png?raw=true)

|     Stable    | Latest | Source code|
|:-------------:|:------|:------:|
|[![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU) || [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board) || [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) || [![HitCount](http://hits.dwyl.io/DataSystemsLab/GeoSpark.svg)](http://hits.dwyl.io/DataSystemsLab/GeoSpark)(since Jan. 2018)

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark / SparkSQL with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs)/ SpatialSQL that efficiently load, process, and analyze large-scale spatial data across machines.

### GeoSpark contains several modules:

| Name  |  API |  Spark compatibility|Introduction|
|---|---|---|---|
|Core  | RDD  | Spark 2.X/1.X  |SpatialRDDs and Query Operators. |
|SQL  | SQL/DataFrame  | SparkSQL 2.1+ |SQL interfaces for GeoSpark core.|
|Viz |  RDD, SQL/DataFrame | RDD - Spark 2.X/1.X, SQL - Spark 2.1+|Visualization for Spatial RDD and DataFrame.|
|Zeppelin |  Apache Zeppelin | Spark 2.1+, Zeppelin 0.8.1+|GeoSpark plugin for Apache Zeppelin|

### GeoSpark supports several programming languages: Scala, Java, and R.

### Please visit [GeoSpark website](http://datasystemslab.github.io/GeoSpark/) for details and documentations.

## News!

* A research paper about ["GeoSparkSim: A Microscopic Road Network Traffic Simulator in Apache Spark"](http://www.public.asu.edu/~jiayu2/geospark/publication/geosparksim-mdm-2019.pdf) is accepted to MDM 2019, Hong Kong China. The next release of GeoSpark will come with a built-in scalable traffic simulator. Please stay tuned!
* A 1.5-hour tutorial about "Geospatial Data Management in Apache Spark" was presented by Jia Yu and Mohamed Sarwat in ICDE 2019, Macau, China. Visit [our tutorial website](https://jiayuasu.github.io/geospatial-tutorial/) to learn how to craft your **"GeoSpark"** from scratch.
* GeoSpark 1.2.0 is released.
	* Tons of bug fixes and new functions! Please read [GeoSpark release note](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Release-notes/).
	* GeoSparkViz now supports DataFrame API. Please read [Visualize Spatial DataFrame/RDD](http://datasystemslab.github.io/GeoSpark/tutorial/viz/).
	* GeoSpark-Zeppelin can connect GeoSpark to [Apache Zeppelin](https://zeppelin.apache.org/). Please read [Interact with GeoSpark via Zeppelin](http://datasystemslab.github.io/GeoSpark/tutorial/zeppelin/)
	* GeoSparkViz Maven coordinate change. Please read [Maven coordinate](http://datasystemslab.github.io/GeoSpark/download/GeoSpark-All-Modules-Maven-Central-Coordinates/#geospark-viz-120-and-later).
	* This release includes the PR from 13 contributors. Please read [GeoSpark release note](download/GeoSpark-All-Modules-Release-notes/) to learn their names.

## Impact

GeoSpark development team has published many papers about GeoSpark. Please read [Publications](http://datasystemslab.github.io/GeoSpark/contact/publication/). 

GeoSpark received an evaluation from PVLDB 2018 paper ["How Good Are Modern Spatial Analytics Systems?"](http://www.vldb.org/pvldb/vol11/p1661-pandey.pdf) Varun Pandey, Andreas Kipf, Thomas Neumann, Alfons Kemper (Technical University of Munich), quoted as follows: 
> GeoSpark comes close to a complete spatial analytics system. It also exhibits the best performance in most cases.
