# Spark Scala shell
Spark distribution provides an interactive Scala shell that allows a user to execute Scala code in a terminal.

This mode currently works with GeoSpark-core and GeoSparkViz.

## Download GeoSpark jar automatically

1. Have your Spark cluster ready.

2. Run Spark shell with `--packages` option. This command will automatically download GeoSpark jars from Maven Central.
```
./bin/spark-shell --packages org.datasyslab:geospark:GEOSPARK_VERSION
```

* Local mode: test GeoSpark without setting up a cluster
```
./bin/spark-shell --packages org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0,org.datasyslab:geospark-viz_2.3:1.2.0
```
  
* Cluster mode: you need to specify Spark Master IP
```
./bin/spark-shell --master spark://localhost:7077 --packages org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0,org.datasyslab:geospark-viz_2.3:1.2.0
```
  
## Download GeoSpark jar manually
1. Have your Spark cluster ready.

2. Download GeoSpark jars:
	* Download the pre-compiled jars from [GeoSpark Releases on GitHub](https://github.com/DataSystemsLab/GeoSpark/releases)
	* Download / Git clone GeoSpark source code and compile the code by yourself:
```
mvn clean install -DskipTests
```
3. Run Spark shell with `--jars` option.
```
./bin/spark-shell --jars /Path/To/GeoSparkJars.jar
```
 
* Local mode: test GeoSpark without setting up a cluster
```
./bin/spark-shell --jars geospark-1.0.1.jar,geospark-viz-1.0.1.jar
```
  
* Cluster mode: you need to specify Spark Master IP  
```
./bin/spark-shell --master spark://localhost:7077 --jars geospark-1.0.1.jar,geospark-viz-1.0.1.jar
```
