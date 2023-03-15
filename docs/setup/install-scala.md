Before starting the Sedona journey, you need to make sure your Apache Spark cluster is ready.

There are two ways to use a Scala or Java library with Apache Spark. You can user either one to run Sedona.

* Spark interactive Scala or SQL shell: easy to start, good for new learners to try simple functions
* Self-contained Scala / Java project: a steep learning curve of package management, but good for large projects

## Spark Scala shell

### Download Sedona jar automatically

1. Have your Spark cluster ready.

2. Run Spark shell with `--packages` option. This command will automatically download Sedona jars from Maven Central.
```
./bin/spark-shell --packages MavenCoordinates
```

* Local mode: test Sedona without setting up a cluster
```
./bin/spark-shell --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},org.apache.sedona:sedona-viz-3.0_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
```
  
* Cluster mode: you need to specify Spark Master IP
```
./bin/spark-shell --master spark://localhost:7077 --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},org.apache.sedona:sedona-viz-3.0_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
```
  
### Download Sedona jar manually
1. Have your Spark cluster ready.

2. Download Sedona jars:
	* Download the pre-compiled jars from [Sedona Releases](../download.md)
	* Download / Git clone Sedona source code and compile the code by yourself (see [Compile Sedona](../compile))
3. Run Spark shell with `--jars` option.
```
./bin/spark-shell --jars /Path/To/SedonaJars.jar
```
 
* Local mode: test Sedona without setting up a cluster
```
./bin/spark-shell --jars org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},org.apache.sedona:sedona-viz-3.0_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
```
  
* Cluster mode: you need to specify Spark Master IP  
```
./bin/spark-shell --master spark://localhost:7077 --jars org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},org.apache.sedona:sedona-viz-3.0_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
```

## Spark SQL shell

Please see [Use Sedona in a pure SQL environment](../../tutorial/sql-pure-sql/)

## Self-contained Spark projects

A self-contained project allows you to create multiple Scala / Java files and write complex logics in one place. To use Sedona in your self-contained Spark project, you just need to add Sedona as a dependency in your POM.xml or build.sbt.

1. To add Sedona as dependencies, please read [Sedona Maven Central coordinates](maven-coordinates.md)
2. Use Sedona Template project to start: [Sedona Template Project](../../tutorial/demo/)
3. Compile your project using SBT. Make sure you obtain the fat jar which packages all dependencies.
4. Submit your compiled fat jar to Spark cluster. Make sure you are in the root folder of Spark distribution. Then run the following command:
```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```

!!!note
	The detailed explanation of spark-submit is available on [Spark website](https://spark.apache.org/docs/latest/submitting-applications.html).