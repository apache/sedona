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

In Databricks advanced editions, you need to install Sedona via [cluster init-scripts](https://docs.databricks.com/clusters/init-scripts.html) as described below. Sedona is not guaranteed to be 100% compatible with `Databricks photon acceleration`. Sedona requires Spark internal APIs to inject many optimization strategies, which sometimes is not accessible in `Photon`.

The following steps use DBR including Apache Spark 3.5.x as an example. Please change the Spark version according to your DBR version. Please pay attention to the Spark version postfix and Scala version postfix on our [Maven Coordinate page](maven-coordinates.md). Databricks Spark and Apache Spark's compatibility can be found [here](https://docs.databricks.com/en/release-notes/runtime/index.html).

!!! bug
    Databricks Runtime 16.2 (non-LTS) introduces a change in the json4s dependency, which may lead to compatibility issues with Apache Sedona. We recommend using a currently supported LTS version, such as Databricks Runtime 15.4 LTS or 14.3 LTS, to ensure stability. A patch will be provided once an official Databricks Runtime 16 LTS version is released.

### Download Sedona jars

Download the Sedona jars to a DBFS location. You can do that manually via UI or from a notebook by executing this code in a cell:

```bash
%sh
# Create JAR directory for Sedona
mkdir -p /Workspace/Shared/sedona/{{ sedona.current_version }}

# Download the dependencies from Maven into DBFS
curl -o /Workspace/Shared/sedona/{{ sedona.current_version }}/geotools-wrapper-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar"

curl -o /Workspace/Shared/sedona/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar"
```

Of course, you can also do the steps above manually.

### Create an init script

!!!note
    If you are creating a Shared cluster, you won't be able to use init scripts and jars stored under `Workspace`. Please instead store them in `Volumes`. The overall process should be the same.

Create an init script in `Workspace` that loads the Sedona jars into the cluster's default jar directory. You can create that from any notebook by running:

```bash
%sh

# Create init script directory for Sedona
mkdir -p /Workspace/Shared/sedona/

# Create init script
cat > /Workspace/Shared/sedona/sedona-init.sh <<'EOF'
#!/bin/bash
#
# File: sedona-init.sh
#
# On cluster startup, this script will copy the Sedona jars to the cluster's default jar directory.

cp /Workspace/Shared/sedona/{{ sedona.current_version }}/*.jar /databricks/jars

EOF
```

Of course, you can also do the steps above manually.

### Set up cluster config

From your cluster configuration (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options` -> `Spark`) activate the Sedona functions and the kryo serializer by adding to the Spark Config

```
spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
spark.sedona.enableParserExtension false
```

From your cluster configuration (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options` -> `Init Scripts`) add the newly created `Workspace` init script

| Type | File path |
|------|-----------|
| Workspace | /Shared/sedona/sedona-init.sh |

For enabling python support, from the Libraries tab install from PyPI

```
apache-sedona=={{ sedona.current_version }}
geopandas==1.0.1
keplergl==0.3.7
pydeck==0.9.1
```

!!!tips
	You need to install the Sedona libraries via init script because the libraries installed via UI are installed after the cluster has already started, and therefore the classes specified by the config `spark.sql.extensions`, `spark.serializer`, and `spark.kryo.registrator` are not available at startup time.*

### Verify installation

After you have started the cluster, you can verify that Sedona is correctly installed by running the following code in a notebook:

```python
spark.sql("SELECT ST_Point(1, 1)").show()
```

Note that: you don't need to run the `SedonaRegistrator.registerAll(spark)` or `SedonaContext.create(spark)` in the advanced edition because `org.apache.sedona.sql.SedonaSqlExtensions` in the Cluster Config will take care of that.
