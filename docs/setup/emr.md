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

In the tutorial, we use AWS Elastic MapReduce (EMR) 7.9.0. It has the following applications installed: Hadoop 3.4.1, JupyterEnterpriseGateway 2.6.0, Livy 0.8.0-incubating, Spark 3.5.5. Any EMR 7.x release works, since they all ship Spark 3.5.

This tutorial is tested on EMR on EC2 with EMR Studio (notebooks). EMR on EC2 uses YARN to manage resources.

!!!note
	Use the artifact whose Spark major.minor version matches your cluster: `sedona-spark-shaded-3.5_2.12` for Spark 3.5, `sedona-spark-shaded-4.0_2.13` for Spark 4.0, `sedona-spark-shaded-4.1_2.13` for Spark 4.1. Spark 4.0 and above are built with Scala 2.13 only.

## Prepare initialization script

In your S3 bucket, add a script that has the following content:

```bash
#!/bin/bash

# EMR clusters only have ephemeral local storage. It does not really matter where we store the jars.
sudo mkdir /jars

# Download Sedona jar
sudo curl -o /jars/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar"

# Download GeoTools jar
sudo curl -o /jars/geotools-wrapper-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar"

# Install necessary python libraries
sudo python3 -m pip install pandas
sudo python3 -m pip install shapely
sudo python3 -m pip install geopandas
sudo python3 -m pip install keplergl==0.3.2
sudo python3 -m pip install pydeck==0.8.0
sudo python3 -m pip install attrs matplotlib descartes apache-sedona=={{ sedona.current_version }}
```

When you create an EMR cluster, in the `bootstrap action`, specify the location of this script.

## Add software configuration

When you create an EMR cluster, in the software configuration, add the following content:

```bash
[
  {
    "Classification":"spark-defaults",
    "Properties":{
      "spark.yarn.dist.jars": "/jars/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar,/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
      "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
      "spark.sql.extensions": "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
      }
  }
]
```

## Verify installation

After the cluster is created, you can verify the installation by running the following code in a Jupyter notebook:

```python
spark.sql("SELECT ST_Point(0, 0)").show()
```

Note that: you don't need to run the `SedonaRegistrator.registerAll(spark)` or `SedonaContext.create(spark)` because `org.apache.sedona.sql.SedonaSqlExtensions` in the config will take care of that.

## Use Sedona in R

The [`apache.sedona`](https://cran.r-project.org/package=apache.sedona) R package is a [`sparklyr`](https://spark.rstudio.com) extension. Attaching it before `spark_connect()` is enough to register Sedona's serializers, UDTs and UDFs, so there is no R equivalent of `SedonaContext.create()` to call by hand.

!!!note
	The `apache.sedona` 1.9.1 release on CRAN supports Spark 3.x only. Every EMR 7.x release ships Spark 3.5, so any of them works.

### Extend the initialization script

Add the following to the bootstrap script above. Like the rest of the script, it runs on every node in the cluster. Spark SQL queries from R only need the R packages on the node you run R from; worker nodes need them only if you run R code on executors with `spark_apply()`.

```bash
# Install R and the Sedona R interface. libcurl-devel is needed to build the R curl package that sparklyr depends on.
sudo yum install -y R libcurl-devel
sudo R -e 'install.packages(c("sparklyr", "apache.sedona"), repos = "https://cloud.r-project.org", Ncpus = parallel::detectCores()); stopifnot(all(c("sparklyr", "apache.sedona") %in% rownames(installed.packages())))'
```

`install.packages()` only warns when a package fails to build, so the `stopifnot()` call is what makes the `R` command fail instead of reporting success. Expect the R installation to add several minutes to the bootstrap of each node.

### Connect to the cluster from R

EMR installs Spark under `/usr/lib/spark`. When you connect from R, the Spark driver runs in YARN client mode on the node where R runs. The `spark.yarn.dist.jars` setting above only ships jars to the executors, so it does not put Sedona on the driver's classpath. `apache.sedona` adds the jars to the driver itself, either by downloading them from Maven Central or from the local files listed in `SEDONA_JAR_FILES`. Point `SEDONA_JAR_FILES` at the jars the bootstrap script already downloaded into `/jars`:

```r
library(sparklyr)
library(apache.sedona)

Sys.setenv(
  "SEDONA_JAR_FILES" = paste(
    "/jars/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar",
    "/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
    sep = ":"
  )
)

sc <- spark_connect(master = "yarn", spark_home = "/usr/lib/spark")
```

The Jupyter example above does not need this step because EMR runs Livy in cluster deploy mode, where the driver runs on a YARN container that receives the jars.

!!!note
	`SEDONA_JAR_FILES` holds a `:`-separated list of jars, and setting it only replaces the Sedona Maven coordinate. `apache.sedona` still requests `org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}` through `--packages`, so the driver needs access to Maven Central, or that jar already in its Ivy cache, either way. Listing the GeoTools wrapper jar in `SEDONA_JAR_FILES` is harmless. If you leave `SEDONA_JAR_FILES` unset, `apache.sedona` also requests `org.apache.sedona:sedona-spark-shaded-<spark version>_<scala version>:{{ sedona.current_version }}`. Ivy caches the downloaded jars per user, so only the first connection downloads them, but on a slow network that first connection can exceed the default `sparklyr.connect.timeout`.

### Verify the R installation

```r
sdf_sql(sc, "SELECT ST_Point(0.0, 0.0) AS geom") %>% collect()
```

For more on what the R interface offers, see the [Sedona R documentation](https://sedona.apache.org/latest/api/rdocs/).
