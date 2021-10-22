
In order to use Sedona on a Databricks clister you need to install the Sedona jars (and Sedona Python if you are using python) on Databricks using Databricks default web UI.

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

3) (Optional) If you are using an older version of Databricks Runtime (DBR <= 7.3) you can also speed up the serialization of geometry types by adding to your spark configurations (`Cluster` -> `Edit` -> `Configuration` -> `Advanced options`) the following lines:

    ```
    spark.serializer org.apache.spark.serializer.KryoSerializer
    spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
    ```

    *This options are not compatible with newer DBR versions (8+).*

## Initialise

After you have installed the libraries and started the cluster, you can initialize the Sedona `ST_*` functions and types by running from your code: 

(scala)
```Scala
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
SedonaSQLRegistrator.registerAll(sparkSession)
```

(or python)
```Python
from sedona.register.geo_registrator import SedonaRegistrator
SedonaRegistrator.registerAll(spark)
```

## Pure SQL environment
 
In order to use the Sedona `ST_*` functions from SQL, you need to register the Sedona bindings. There are two ways to do that:

1) Insert a python (or scala) cell at the beginning of your SQL notebook to activate the bindings

    ```Python
    %python
    from sedona.register.geo_registrator import SedonaRegistrator
    SedonaRegistrator.registerAll(spark)
    ```

2) Install the sedona libraries from the [cluster init-scripts](https://docs.databricks.com/clusters/init-scripts.html) and activate the bindings by adding `spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions` to your cluster's spark configuration. This way you can activate the Sedona bindings without typing any python or scala code. 

    Note: You need to install the sedona libraries via init script because the libraries installed via UI are installed after the cluster has already started, and therefore the classes specified by the config `spark.sql.extensions` are not available at startup time.

