## Community edition (free-tier)

You just need to install the Sedona jars and Sedona Python on Databricks using Databricks default web UI. Then everything will work.

## Advanced editions

* Sedona 1.0.1 & 1.1.0 is compiled against Spark 3.1 (~ Databricks DBR 9 LTS, DBR 7 is Spark 3.0)
* Sedona 1.1.1, 1.2.0 are compiled against Spark 3.2 (~ DBR 10 & 11)
* Sedona 1.2.1, 1.3.1, 1.4.0 are complied against Spark 3.3

> In Spark 3.2, `org.apache.spark.sql.catalyst.expressions.Generator` class added a field `nodePatterns`. Any SQL functions that rely on Generator class may have issues if compiled for a runtime with a differing spark version. For Sedona, those functions are:
>    * ST_MakeValid
>    * ST_SubDivideExplode

__Sedona `1.1.1-incubating` and above is overall the recommended version to use. It is generally backwards compatible with earlier Spark releases but you should be aware of what Spark version Sedona was compiled against versus which is being executed in case you hit issues.__

#### Databricks 10.x+ (Recommended)

* You need to use Sedona version `1.1.1-incubating` or higher. 
* In order to activate the Kryo serializer (this speeds up the serialization and deserialization of geometry types) you need to install the libraries via init script as described below.

#### Databricks DBR 7.x - 9.x

* If you are using the commercial version of Databricks you can install the Sedona jars and Sedona Python using the Databricks default web UI. DBR 7 matches with Sedona `1.1.0-incubating` and DBR 9 matches better with Sedona `1.1.1-incubating` due to Databricks cherry-picking some Spark 3.2 private APIs.

## Install Sedona from the web UI

1) From the Libraries tab install from Maven Coordinates
    ```
    org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }}
    org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
    ```

2) For enabling python support, from the Libraries tab install from PyPI
    ```
    apache-sedona
    ```

3) (Only for DBR up to 7.3 LTS) You can speed up the serialization of geometry types by adding to your spark configurations (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options`) the following lines:
    ```
    spark.serializer org.apache.spark.serializer.KryoSerializer
    spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
    ```
    > For DBRs after 7.3, use the Init Script method described further down.


## Initialise

After you have installed the libraries and started the cluster, you can initialize the Sedona `ST_*` functions and types by running from your code: 

(scala)
```scala
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
SedonaSQLRegistrator.registerAll(spark)
```

(or python)
```python
from sedona.register.geo_registrator import SedonaRegistrator
SedonaRegistrator.registerAll(spark)
```

## Pure SQL environment
 
In order to use the Sedona `ST_*` functions from SQL without having to register the Sedona functions from a python/scala cell, you need to install the Sedona libraries from the [cluster init-scripts](https://docs.databricks.com/clusters/init-scripts.html) as follows.

## Install Sedona via init script (for DBRs > 7.3)

Download the Sedona jars to a DBFS location. You can do that manually via UI or from a notebook by executing this code in a cell:

```bash
%sh 
# Create JAR directory for Sedona
mkdir -p /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}

# Download the dependencies from Maven into DBFS
curl -o /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/geotools-wrapper-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar"

curl -o /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar"

curl -o /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/sedona-viz-3.0_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-viz-3.0_2.12/{{ sedona.current_version }}/sedona-viz-3.0_2.12-{{ sedona.current_version }}.jar"
```

Create an init script in DBFS that loads the Sedona jars into the cluster's default jar directory. You can create that from any notebook by running: 

```bash
%sh 

# Create init script directory for Sedona
mkdir -p /dbfs/FileStore/sedona/

# Create init script
cat > /dbfs/FileStore/sedona/sedona-init.sh <<'EOF'
#!/bin/bash
#
# File: sedona-init.sh
# Author: Erni Durdevic
# Created: 2021-11-01
# 
# On cluster startup, this script will copy the Sedona jars to the cluster's default jar directory.
# In order to activate Sedona functions, remember to add to your spark configuration the Sedona extensions: "spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"

cp /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/*.jar /databricks/jars

EOF
```

From your cluster configuration (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options` -> `Spark`) activate the Sedona functions and the kryo serializer by adding to the Spark Config 
```
spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
```

From your cluster configuration (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options` -> `Init Scripts`) add the newly created init script 
```
dbfs:/FileStore/sedona/sedona-init.sh
```

For enabling python support, from the Libraries tab install from PyPI
```
apache-sedona
```

*Note: You need to install the Sedona libraries via init script because the libraries installed via UI are installed after the cluster has already started, and therefore the classes specified by the config `spark.sql.extensions`, `spark.serializer`, and `spark.kryo.registrator` are not available at startup time.*

