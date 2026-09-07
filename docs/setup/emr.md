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
	The R interface supports Spark 3.x only. Make sure the EMR release you pick ships a Spark 3 version.

### Extend the initialization script

Add the following to the bootstrap script above, so that R and the two R packages are available on the node you run R from:

```bash
# Install R and the Sedona R interface
sudo yum install -y R
sudo R -e 'install.packages(c("sparklyr", "apache.sedona"), repos = "https://cloud.r-project.org")'
```

### Connect to the cluster from R

EMR installs Spark under `/usr/lib/spark`. Point `SEDONA_JAR_FILES` at the jars the bootstrap script already downloaded into `/jars` so that `sparklyr` uses them instead of resolving the Sedona coordinates from Maven Central every time you connect:

```r
library(sparklyr)
library(apache.sedona)

Sys.setenv(
  "SEDONA_JAR_FILES" = paste(
    "/jars/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar",
    "/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
    sep = ":"
  )
)

sc <- spark_connect(master = "yarn", spark_home = "/usr/lib/spark")
```

!!!note
	`SEDONA_JAR_FILES` holds a `:`-separated list and replaces *both* Maven coordinates that `apache.sedona` would otherwise request, which is why the GeoTools wrapper jar has to be listed next to the Sedona jar. If you leave `SEDONA_JAR_FILES` unset, every connection downloads `org.apache.sedona:sedona-spark-shaded-<spark version>_<scala version>:{{ sedona.current_version }}` and `org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}`. That requires outbound internet access from the driver and can take long enough to exceed the default `sparklyr.connect.timeout`.

### Verify the R installation

```r
sdf_sql(sc, "SELECT ST_Point(0.0, 0.0) AS geom") %>% collect()
```

For more on what the R interface offers, see the [Sedona R documentation](https://sedona.apache.org/latest/api/rdocs/).
