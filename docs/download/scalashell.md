# Spark Scala shell
Spark distribution provides an interactive Scala shell that allows a user to execute Scala code in a terminal.

## Download Sedona jar automatically

1. Have your Spark cluster ready.

2. Run Spark shell with `--packages` option. This command will automatically download Sedona jars from Maven Central.
```
./bin/spark-shell --packages MavenCoordiantes
```

* Local mode: test Sedona without setting up a cluster
```
./bin/spark-shell --packages org.apache.sedona:sedona-core-3.0_2.12:1.0.1-incubating,org.apache.sedona:sedona-sql-3.0_2.12:1.0.1-incubating,org.apache.sedona:sedona-viz-3.0_2.12:1.0.1-incubating
```
  
* Cluster mode: you need to specify Spark Master IP
```
./bin/spark-shell --master spark://localhost:7077 --packages org.apache.sedona:sedona-core-3.0_2.12:1.0.1-incubating,org.apache.sedona:sedona-sql-3.0_2.12:1.0.1-incubating,org.apache.sedona:sedona-viz-3.0_2.12:1.0.1-incubating
```
  
## Download Sedona jar manually
1. Have your Spark cluster ready.

2. Download Sedona jars:
	* Download the pre-compiled jars from [Sedona Releases on GitHub](https://github.com/apache/incubator-sedona/releases)
	* Download / Git clone Sedona source code and compile the code by yourself (see [Compile Sedona](/download/compile))
3. Run Spark shell with `--jars` option.
```
./bin/spark-shell --jars /Path/To/SedonaJars.jar
```
 
* Local mode: test Sedona without setting up a cluster
```
./bin/spark-shell --jars sedona-core-3.0_2.12-1.0.1-incubating.jar,sedona-sql-3.0_2.12-1.0.1-incubating.jar,sedona-viz-3.0_2.12-1.0.1-incubating.jar
```
  
* Cluster mode: you need to specify Spark Master IP  
```
./bin/spark-shell --master spark://localhost:7077 --jars sedona-core-3.0_2.12-1.0.1-incubating.jar,sedona-sql-3.0_2.12-1.0.1-incubating.jar,sedona-viz-3.0_2.12-1.0.1-incubating.jar
```

## Spark SQL shell

Please see [Use Sedona in a pure SQL environment](../../tutorial/sql-pure-sql/)