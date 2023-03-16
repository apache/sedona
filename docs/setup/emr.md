We recommend Sedona-1.3.1-incuabting and above for EMR. In the tutorial, we use AWS Elastic MapReduce (EMR) 6.9.0. It has the following applications installed: Hadoop 3.3.3, JupyterEnterpriseGateway 2.6.0, Livy 0.7.1, Spark 3.3.0.

This tutorial is tested on EMR on EC2 with EMR Studio (notebooks). EMR on EC2 uses YARN to manage resources.

## Prepare initialization script

In your S3 bucket, add a script that has the following content:

```bash
#!/bin/bash

# EMR clusters only have ephemeral local storage. It does not really matter where we store the jars.
sudo mkdir /jars

# Download Sedona jar
sudo curl -o /jars/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar"

# Download GeoTools jar
sudo curl -o /jars/geotools-wrapper-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar"

# Install necessary python libraries
sudo python3 -m pip install pandas shapely==1.8.5
sudo python3 -m pip install pandas geopandas==0.10.2
sudo python3 -m pip install attrs matplotlib descartes apache-sedona==1.4.0
```

When you create a EMR cluster, in the `bootstrap action`, specify the location of this script.

## Add software configuration

When you create a EMR cluster, in the software configuration, add the following content:

```bash
[
  {
    "Classification":"spark-defaults", 
    "Properties":{
      "spark.yarn.dist.jars": "/jars/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar,/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
      "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
      "spark.sql.extensions": "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
      }
  }
]
```

!!!note
	If you use Sedona 1.3.1-incubating, please use `sedona-python-adpater-3.0_2.12` jar in the content above, instead of `sedona-spark-shaded-3.0_2.12`.