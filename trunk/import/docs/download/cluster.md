# Set up your Apache Spark cluster

Download a Spark distribution from [Spark download page](http://spark.apache.org/downloads.html).

## Preliminary
1. Set up password-less SSH on your cluster. Each master-worker pair should have bi-directional password-less SSH.
2. Make sure you have installed JRE 1.8 or later.
3. Add the list of your workers' IP address in ./conf/slaves
4. Besides the necessary Spark settings, you may need to add the following lines in Spark configuration files to avoid GeoSpark memory errors:

In `./conf/spark-defaults.conf`

```
spark.driver.memory 10g
spark.network.timeout 1000s
spark.driver.maxResultSize 5g
```

* `spark.driver.memory` tells Spark to allocate enough memory for the driver program because GeoSpark needs to build global grid files (global index) on the driver program. If you have a large amount of data (normally, over 100 GB), set this parameter to 2~5 GB will be good. Otherwise, you may observe "out of memory" error.
* `spark.network.timeout` is the default timeout for all network interactions. Sometimes, spatial join query takes longer time to shuffle data. This will ensure Spark has enough patience to wait for the result.
* `spark.driver.maxResultSize` is the limit of total size of serialized results of all partitions for each Spark action. Sometimes, the result size of spatial queries is large. The "Collect" operation may throw errors.

For more details of Spark parameters, please visit [Spark Website](https://spark.apache.org/docs/latest/configuration.html).

## Start your cluster
Go the root folder of the uncompressed Apache Spark folder. Start your spark cluster via a terminal

```
./sbin/start-all.sh
```
