## Community edition (free-tier)

You just need to install the Sedona jars and Sedona Python on Databricks using Databricks default web UI. Then everything will work.

## Advanced editions


### Databricks DBR 7.x

If you are using the commercial version of Databricks up to version 7.x you can install the Sedona jars and Sedona Python using the Databricks default web UI and everything should work.

### Databricks DBR 8.x, 9.x, 10.x

If you are using the commercial version of Databricks for DBR 8.x+

* You need to use sedona version `1.1.1-incubating` or higher. 
* In order to activate the Kryo serializer (this speeds up the serialization and deserialization of geometry types) you need to install the libraries via init script as described below.

## Install Sedona from the web UI

1) From the Libraries tab install from Maven Coordinates
    ```
    org.apache.sedona:sedona-python-adapter-3.0_2.12:{{ sedona.current_version }}
    org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
    ```

2) From the Libraries tab install from PyPI
    ```
    apache-sedona
    ```

3) (For DBR up to 7.3 LTS) You can speed up the serialization of geometry types by adding to your spark configurations (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options`) the following lines:

    ```
    spark.serializer org.apache.spark.serializer.KryoSerializer
    spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
    ```

    In order to activate this options for DBR versions 8.x+, you need to install the Sedona libraries via init script because libraries installed via UI are not yet available at cluster startup when this options are regiestered.

## Initialise

After you have installed the libraries and started the cluster, you can initialize the Sedona `ST_*` functions and types by running from your code: 

(scala)
```Scala
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
SedonaSQLRegistrator.registerAll(spark)
```

(or python)
```Python
from sedona.register.geo_registrator import SedonaRegistrator
SedonaRegistrator.registerAll(spark)
```

## Pure SQL environment
 
In order to use the Sedona `ST_*` functions from SQL without having to register the Sedona functions from a python/scala cell, you need to install the sedona libraries from the [cluster init-scripts](https://docs.databricks.com/clusters/init-scripts.html) as follows.

## Install Sedona via init script

Download the Sedona jars to a DBFS location. You can do that manually via UI or from a notebook with

```bash
%sh 
# Create JAR directory for Sedona
mkdir -p /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}

# Download the dependencies from Maven into DBFS
curl -o /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/geotools-wrapper-geotools-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/geotools-{{ sedona.current_geotools }}/geotools-wrapper-geotools-{{ sedona.current_geotools }}.jar"

curl -o /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/sedona-python-adapter-3.0_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-python-adapter-3.0_2.12/{{ sedona.current_version }}/sedona-python-adapter-3.0_2.{{ sedona.current_version }}.jar"

curl -o /dbfs/FileStore/jars/sedona/{{ sedona.current_version }}/sedona-viz-2.4_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-viz-2.4_2.12/{{ sedona.current_version }}/sedona-viz-2.4_2.12-{{ sedona.current_version }}.jar"
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
/dbfs/FileStore/sedona/sedona-init.sh
```

*Note: You need to install the sedona libraries via init script because the libraries installed via UI are installed after the cluster has already started, and therefore the classes specified by the config `spark.sql.extensions`, `spark.serializer`, and `spark.kryo.registrator` are not available at startup time.*

