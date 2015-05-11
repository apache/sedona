# GeoSpark
A Custer Computing System for Processing Large-Scale Spatial and Spatio-Temporal Data

## Introduction
GeoSpark consists of three layers: Apache Spark Layer, Spatial RDD Layer and Spatial Query Processing Layer. Apache Spark Layer provides basic Apache Spark functionalities that include loading / storing data to disk as well as regular RDD operations. Spatial RDD Layer consists of three novel Spatial Resilient Distributed Datasets (SRDDs) which extend regular Apache Spark RDD to support geometrical and spatial objects.

## How to get started

### Prerequisites (For Java version)

1. Apache Hadoop 2.4.0
2. Apache Spark 1.2.1
3. JDK 1.7
4. Maven
5. JTS Topology Suite version 1.13

### Steps
1. Create Java Maven project
2. Add the dependecies of Apache Hadoop, Spark and JTS Topology Suite
3. Put the java files in your project or add the jar package into your Maven project
4. Use spatial RDDs to store spatial data and call needed functions

### One quick start program
Please check the "QuickStartProgram.java" in GeoSpark root folder for a sample program with GeoSpark.

## Function checklist
1. PointRDD

  1)
  
2. RectangleRDD
3. PolygonRDD
