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

Please refer to [Sedona Maven Central coordinates](maven-coordinates.md) to select the corresponding Sedona packages for your Spark version.

    * Local mode: test Sedona without setting up a cluster
    ```
    ./bin/spark-shell --packages org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
    ```

    * Cluster mode: you need to specify Spark Master IP
    ```
    ./bin/spark-shell --master spark://localhost:7077 --packages org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
    ```

### Download Sedona jar manually

1. Have your Spark cluster ready.

2. Download Sedona jars:
	* Download the pre-compiled jars from [Sedona Releases](../download.md)
	* Download / Git clone Sedona source code and compile the code by yourself (see [Compile Sedona](compile.md))
3. Run Spark shell with `--jars` option.

```
./bin/spark-shell --jars /Path/To/SedonaJars.jar
```

Please use jars with Spark major.minor versions in the filename, such as `sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}`.

    * Local mode: test Sedona without setting up a cluster
    ```
    ./bin/spark-shell --jars /path/to/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar,/path/to/geotools-wrapper-{{ sedona.current_geotools }}.jar
    ```

    * Cluster mode: you need to specify Spark Master IP
    ```
    ./bin/spark-shell --master spark://localhost:7077 --jars /path/to/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar,/path/to/geotools-wrapper-{{ sedona.current_geotools }}.jar
    ```

## Spark SQL shell

Please see [Use Sedona in a pure SQL environment](../tutorial/sql-pure-sql.md)

## Self-contained Spark projects

A self-contained project allows you to create multiple Scala / Java files and write complex logics in one place. To use Sedona in your self-contained Spark project, you just need to add Sedona as a dependency in your pom.xml or build.sbt.

1. To add Sedona as dependencies, please read [Sedona Maven Central coordinates](maven-coordinates.md)
2. Use Sedona Template project to start: [Sedona Template Project](../tutorial/demo.md)
3. Compile your project using SBT. Make sure you obtain the fat jar which packages all dependencies.
4. Submit your compiled fat jar to Spark cluster. Make sure you are in the root folder of Spark distribution. Then run the following command:

```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```

!!!note
	The detailed explanation of spark-submit is available on [Spark website](https://spark.apache.org/docs/latest/submitting-applications.html).
