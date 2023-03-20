# Scala and Java Examples

[Scala and Java Examples](https://github.com/apache/sedona/tree/master/examples) contains template projects for Sedona Spark (RDD, SQL and Viz) and Sedona Flink. The template projects have been configured properly.

Note that, although the template projects are written in Scala, the same APIs can be  used in Java as well.

## Folder structure
The folder structure of this repository is as follows.

* spark-rdd-colocation-mining: a scala template shows how to use Sedona RDD API in Spatial Data Mining in Apache Spark
* spark-sql: a scala template shows how to use Sedona DataFrame and SQL API in Apache Spark
* spark-viz: a scala template shows how to use Sedona Viz RDD and SQL API in Apache Spark
* flink-sql: a Java template shows how to use Sedona SQL in Apache Flink

## Compile and package

### Prerequisites
Please make sure you have the following software installed on your local machine:

* For Scala: Scala 2.12, SBT
* For Java: JDK 1.8, Apache Maven 3

### Compile

Run a terminal command `sbt assembly` within the folder of each template


### Submit your fat jar to Spark or Flink
After running the command mentioned above, you are able to see a fat jar in `./target` folder. Please take it and use `./bin/spark-submit` or `/bin/flink`to submit this jar.

To run the jar in this way, you need to:

* For Spark: either change Spark Master Address in template projects or simply delete it. Currently, they are hard coded to `local[*]` which means run locally with all cores.

* Change the dependency packaging scope of Apache Spark from "compile" to "provided". This is a common packaging strategy in Maven and SBT which means do not package Spark into your fat jar. Otherwise, this may lead to a huge jar and version conflicts!

* Make sure the dependency versions in build.sbt/pom.xml are consistent with your Spark/Flink version.

## Run template projects locally

We highly suggest you use IDEs to run template projects on your local machine. For Scala, we recommend IntelliJ IDEA with Scala plug-in. For Java, we recommend IntelliJ IDEA and Eclipse. With the help of IDEs, **you don't have to prepare anything** (even don't need to download and set up Spark!). As long as you have Scala and Java, everything works properly!

### Scala
Import the Scala template project as SBT project. Then run the Main file in this project.

### Java
Import the Java template project as Maven project. Then run the Main file in this project.