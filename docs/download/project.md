# Self-contained Spark projects

A self-contained project allows you to create multiple Scala / Java files and write complex logics in one place. To use GeoSpark in your self-contained Spark project, you just need to add GeoSpark as a dependency in your POM.xml or build.sbt.

## Quick start

1. To add GeoSpark as dependencies, please read [GeoSpark Maven Central coordinates](GeoSpark-All-Modules-Maven-Central-Coordinates.md)
2. Use GeoSpark Template project to start: [GeoSpark Template Project](https://github.com/jiayuasu/GeoSparkTemplateProject)
3. Compile your project using SBT or Maven. Make sure you obtain the fat jar which packages all dependencies.
4. Submit your compiled fat jar to Spark cluster. Make sure you are in the root folder of Spark distribution. Then run the following command:
```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```

!!!note
	The detailed explanation of spark-submit is available on [Spark website](https://spark.apache.org/docs/latest/submitting-applications.html).

## How to use GeoSpark in an IDE

### Select an IDE
To develop a complex GeoSpark project, we suggest you use IntelliJ IDEA. It supports JVM languages, Scala and Java, and many dependency management systems, Maven and SBT.

Eclipse is also fine if you just want to use Java and Maven.

### Open GeoSpark template project
Select a proper GeoSpark project you want from [GeoSpark Template Project](https://github.com/jiayuasu/GeoSparkTemplateProject). In this tutorial, we use GeoSparkSQL Scala project as an example.

Open the folder that contains `build.sbt` file in your IDE. The IDE may take a while to index dependencies and source code.

### Try GeoSpark SQL functions
In your IDE, run [ScalaExample.scala](https://github.com/jiayuasu/GeoSparkTemplateProject/blob/master/geospark-sql/scala/src/main/scala/ScalaExample.scala) file.

You don't need to change anything in this file. The IDE will run all SQL queries in this example in local mode.

### Package the project
To run this project in cluster mode, you have to package this project to a JAR and then run it using `spark-submit` command.

Before packaging this project, you always need to check two places:

* Remove the hardcoded Master IP `master("local[*]")`. This hardcoded IP is only needed when you run this project in an IDE.
```scala
var sparkSession:SparkSession = SparkSession.builder()
	.config("spark.serializer",classOf[KryoSerializer].getName)
	.config("spark.kryo.registrator",classOf[GeoSparkKryoRegistrator].getName)
	.master("local[*]")
	.appName("GeoSparkSQL-demo").getOrCreate()
```

* In build.sbt (or POM.xml), set Spark dependency scope to `provided` instead of `compile`. `compile` is only needed when you run this project in an IDE.
```
org.apache.spark" %% "spark-core" % SparkVersion % "compile,
org.apache.spark" %% "spark-sql" % SparkVersion % "compile
```

!!!warning
	Forgetting to change the package scope will lead to a very big fat JAR and dependency conflicts when call `spark-submit`. For more details, please visit [Maven Dependency Scope](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Dependency_Scope).

* Make sure your downloaded Spark binary distribution is the same version with the Spark used in your `build.sbt` or `POM.xml`.

### Submit the compiled jar
1. Go to `./target/scala-2.11` folder and find a jar called `GeoSparkSQLScalaTemplate-0.1.0.jar`. Note that, this JAR normally is larger than 1MB. (If you use POM.xml, the jar is under `./target` folder)
2. Submit this JAR using `spark-submit`.

* Local mode:
```
./bin/spark-submit /Path/To/YourJar.jar
```

* Cluster mode:
```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```