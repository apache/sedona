<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Set up your Apache Spark cluster

Download a Spark distribution from [Spark download page](http://spark.apache.org/downloads.html).

## Preliminary

1. Set up a password-less SSH on your cluster. Each master-worker pair should have bidirectional password-less SSH.
2. Make sure you have installed JRE 1.8 or later.
3. Add the list of your workers' IP address in ./conf/slaves
4. Besides the necessary Spark settings, you may need to add the following lines in the Spark configuration files to avoid Sedona memory errors:

In `./conf/spark-defaults.conf`

```
spark.driver.memory 10g
spark.network.timeout 1000s
spark.driver.maxResultSize 5g
```

- `spark.driver.memory` tells Spark to allocate enough memory for the driver program because Sedona needs to build global grid files (global index) on the driver program. If you have a large amount of data (normally, over 100 GB), set this parameter to 2~5 GB will be good. Otherwise, you may observe "out of memory" error.
- `spark.network.timeout` is the default timeout for all network interactions. Sometimes, spatial join query takes longer time to shuffle data. This will ensure Spark has enough patience to wait for the result.
- `spark.driver.maxResultSize` is the limit of total size of serialized results of all partitions for each Spark action. Sometimes, the result size of spatial queries is large. The "Collect" operation may throw errors.

For more details of Spark parameters, please visit [Spark Website](https://spark.apache.org/docs/latest/configuration.html).

## Start your cluster

Go the root folder of the uncompressed Apache Spark folder. Start your Spark cluster via a terminal

```
./sbin/start-all.sh
```
