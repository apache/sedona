Please pay attention to the Spark version postfix and Scala version postfix on our [Maven Coordinate page](maven-coordinates.md). Databricks Spark and Apache Spark's compatibility can be found [here](https://docs.databricks.com/en/release-notes/runtime/index.html).

## Community edition (free-tier)

You just need to install the Sedona jars and Sedona Python on Databricks using Databricks default web UI. Then everything will work.

### Install libraries

1) From the Libraries tab install from Maven Coordinates

```
org.apache.sedona:sedona-spark-shaded-3.4_2.12:{{ sedona.current_version }}
org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
```

2) For enabling python support, from the Libraries tab install from PyPI

```
apache-sedona=={{ sedona.current_version }}
keplergl==0.3.2
pydeck==0.8.0
```

### Initialize

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

## Advanced editions

In Databricks advanced editions, you need to install Sedona via [cluster init-scripts](https://docs.databricks.com/clusters/init-scripts.html) as described below. We recommend Databricks 10.x+. Sedona is not guaranteed to be 100% compatible with `Databricks photon acceleration`. Sedona requires Spark internal APIs to inject many optimization strategies, which sometimes is not accessible in `Photon`.

In Spark 3.2, `org.apache.spark.sql.catalyst.expressions.Generator` class added a field `nodePatterns`. Any SQL functions that rely on Generator class may have issues if compiled for a runtime with a differing spark version. For Sedona, those functions are:

* ST_MakeValid
* ST_SubDivideExplode

!!!note
    The following steps use DBR including Apache Spark 3.4.x as an example. Please change the Spark version according to your DBR version.

### Download Sedona jars

Download the Sedona jars to a DBFS location. You can do that manually via UI or from a notebook by executing this code in a cell:

```bash
%sh
# Create JAR directory for Sedona
mkdir -p /Workspace/Shared/sedona/{{ sedona.current_version }}

# Download the dependencies from Maven into DBFS
curl -o /Workspace/Shared/sedona/{{ sedona.current_version }}/geotools-wrapper-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar"

curl -o /Workspace/Shared/sedona/{{ sedona.current_version }}/sedona-spark-shaded-3.4_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.4_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.4_2.12-{{ sedona.current_version }}.jar"
```

Of course, you can also do the steps above manually.

### Create an init script

!!!warning
    Starting from December 2023, Databricks has disabled all DBFS based init script (/dbfs/XXX/<script-name>.sh).  So you will have to store the init script from a workspace level (`/Workspace/Users/<user-name>/<script-name>.sh`) or Unity Catalog volume (`/Volumes/<catalog>/<schema>/<volume>/<path-to-script>/<script-name>.sh`). Please see [Databricks init scripts](https://docs.databricks.com/en/init-scripts/cluster-scoped.html#configure-a-cluster-scoped-init-script-using-the-ui) for more information.

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
```

From your cluster configuration (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options` -> `Init Scripts`) add the newly created `Workspace` init script

| Type | File path |
|------|-----------|
| Workspace | /Shared/sedona/sedona-init.sh |

For enabling python support, from the Libraries tab install from PyPI

```
apache-sedona=={{ sedona.current_version }}
geopandas==0.11.1
keplergl==0.3.2
pydeck==0.8.0
```

!!!tips
	You need to install the Sedona libraries via init script because the libraries installed via UI are installed after the cluster has already started, and therefore the classes specified by the config `spark.sql.extensions`, `spark.serializer`, and `spark.kryo.registrator` are not available at startup time.*

### Verify installation

After you have started the cluster, you can verify that Sedona is correctly installed by running the following code in a notebook:

```python
spark.sql("SELECT ST_Point(1, 1)").show()
```

Note that: you don't need to run the `SedonaRegistrator.registerAll(spark)` or `SedonaContext.create(spark)` in the advanced edition because `org.apache.sedona.sql.SedonaSqlExtensions` in the Cluster Config will take care of that.
