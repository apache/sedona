![GeoSpark Logo](http://www.public.asu.edu/~jiayu2/geospark/logo.png)

[![Build Status](https://travis-ci.org/jiayuasu/GeoSpark.svg?branch=master)](https://travis-ci.org/jiayuasu/GeoSpark) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.datasyslab/geospark/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.datasyslab/geospark)
[![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

``` Supported Apache Spark version: 2.0+(Master branch), 1.0+(1.X branch) ```

GeoSpark is listed as **Infrastructure Project** on [**Apache Spark Official Third Party Project Page**](http://spark.apache.org/third-party-projects.html)

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs) that efficiently load, process, and analyze large-scale spatial data across machines. GeoSpark provides APIs for Apache Spark programmer to easily develop their spatial analysis programs with Spatial Resilient Distributed Datasets (SRDDs) which have in house support for geometrical and Spatial Queries (Range, K Nearest Neighbors, Join).



GeoSpark artifacts are hosted in Maven Central: [**Maven Central Coordinates**](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Maven-Central-Coordinates)



#  Version information ([more](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Full-Version-Release-notes))


|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.5.2| **Bug fix:** Fix [Issue #58](https://github.com/DataSystemsLab/GeoSpark/issues/58) and [Issue #60](https://github.com/DataSystemsLab/GeoSpark/issues/60); **Performance enhancement:**: (1) Deprecate all old Spatial RDD constructors. See the JavaDoc [here](http://www.public.asu.edu/~jiayu2/geospark/javadoc/0.5.2/). (2) Recommend the new SRDD constructors which take an additional RDD storage level and automatically cache rawSpatialRDD to accelerate internal SRDD analyze step|
|0.5.1| **Bug fix:** (1) GeoSpark: Fix inaccurate KNN result when K is large (2) GeoSpark: Replace incompatible Spark API call [Issue #55](https://github.com/DataSystemsLab/GeoSpark/issues/55); (3) Babylon: Remove JPG output format temporarily due to the lack of OpenJDK support|
| 0.5.0| **Major updates:** We are pleased to announce the initial version of [Babylon](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon) a large-scale in-memory geospatial visualization system extending GeoSpark. Babylon and GeoSpark are integrated together. You can just import GeoSpark and enjoy! More details are available here: [Babylon GeoSpatial Visualization](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon)|


# Important features ([more](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Important-Features))
## Spatial Resilient Distributed Datasets (SRDDs)
Supported Spatial RDDs: PointRDD, RectangleRDD, PolygonRDD, LineStringRDD

## Supported data format
**Native input format support**: CSV, TSV, WKT, GeoJSON

**User-supplied input format mapper**: Any input formats

## Spatial Partitioning
Supported Spatial Partitioning techniques: R-Tree, Voronoi diagram

## Spatial Index
Supported Spatial Indexes: Quad-Tree and R-Tree. Quad-Tree doesn't support Spatial K Nearest Neighbors query.

## Geometrical operation
Inside, Overlap, DatasetBoundary, Minimum Bounding Rectangl, Polygon Union

## Spatial Operation
Spatial Range Query, Spatial Join Query, and Spatial K Nearest Neighbors Query.

# GeoSpark Tutorial ([more](https://github.com/DataSystemsLab/GeoSpark/wiki))
GeoSpark full tutorial is available at GeoSpark GitHub Wiki: [https://github.com/DataSystemsLab/GeoSpark/wiki](https://github.com/DataSystemsLab/GeoSpark/wiki)

# Babylon Visualization Framework on GeoSpark
Babylon is a large-scale in-memory geospatial visualization system.

Babylon provides native support for general cartographic design by extending GeoSpark to process large-scale spatial data. It can visulize Spatial RDD and Spatial Queries and render super high resolution image in parallel.

Babylon and GeoSpark are integrated together. You just need to import GeoSpark and enjoy them! More details are available here: [Babylon GeoSpatial Visualization](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon) 

## Babylon Gallery
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="250">
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/nycheatmap.png" width="250">
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/ustweet.png" width="250">

# Publication

Jia Yu, Jinxuan Wu, Mohamed Sarwat. ["A Demonstration of GeoSpark: A Cluster Computing Framework for Processing Big Spatial Data"](). (demo paper) In Proceeding of IEEE International Conference on Data Engineering ICDE 2016, Helsinki, FI, May 2016

Jia Yu, Jinxuan Wu, Mohamed Sarwat. ["GeoSpark: A Cluster Computing Framework for Processing Large-Scale Spatial Data"](http://www.public.asu.edu/~jiayu2/geospark/publication/GeoSpark_ShortPaper.pdf). (short paper) In Proceeding of the ACM International Conference on Advances in Geographic Information Systems ACM SIGSPATIAL GIS 2015, Seattle, WA, USA November 2015


# Acknowledgement

GeoSpark makes use of JTS Plus (An extended JTS Topology Suite Version 1.14) for some geometrical computations.

Please refer to [JTS Topology Suite website](http://tsusiatsoftware.net/jts/main.html) and [JTS Plus](https://github.com/jiayuasu/JTSplus) for more details.



# Contact

## Questions

* Please join [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

* Email us!

## Contact
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

## Project website
Please visit [GeoSpark project wesbite](http://geospark.datasyslab.org) for latest news and releases.

## Data Systems Lab
GeoSpark is one of the projects under [Data Systems Lab](http://www.datasyslab.org/) at Arizona State University. The mission of Data Systems Lab is designing and developing experimental data management systems (e.g., database systems).

# Thanks for the help from GeoSpark community
We appreciate the help and suggestions from GeoSpark users: [**Thanks List**](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Community-Thanks-List)

