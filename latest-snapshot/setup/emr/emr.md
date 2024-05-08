We recommend Sedona-1.3.1-incubating and above for EMR. In the tutorial, we use AWS Elastic MapReduce (EMR) 6.9.0. It has the following applications installed: Hadoop 3.3.3, JupyterEnterpriseGateway 2.6.0, Livy 0.7.1, Spark 3.3.0.

This tutorial is tested on EMR on EC2 with EMR Studio (notebooks). EMR on EC2 uses YARN to manage resources.

!!!note
	If you are using Spark 3.4+ and Scala 2.12, please use `sedona-spark-shaded-3.4_2.12`. Please pay attention to the Spark version postfix and Scala version postfix.

## JDK 11+ requirement

Sedona 1.6.0+ requires JDK 11+ to run. For Amazon EMR 7.x, the default JVM is Java 17. For Amazon EMR 5.x and 6.x, the default JVM is Java 8 but you can configure the cluster to use Java 11 or Java 17. For more information, see [EMR JVM versions](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/configuring-java8.html#configuring-java8-override-spark).

When you use Spark with Amazon EMR releases 6.12 and higher, if you write a driver for submission in cluster mode, the driver uses Java 8, but you can set the environment so that the executors use Java 11 or 17. To override the JVM for Spark, AWS EMR recommends that you set both the Hadoop and Spark classifications.

However, it is unclear that if the following will work on EMR below 6.12.

```
{
"Classification": "hadoop-env",
        "Configurations": [
            {
"Classification": "export",
                "Configurations": [],
                "Properties": {
"JAVA_HOME": "/usr/lib/jvm/java-1.11.0"
                }
            }
        ],
        "Properties": {}
    },
    {
"Classification": "spark-env",
        "Configurations": [
            {
"Classification": "export",
                "Configurations": [],
                "Properties": {
"JAVA_HOME": "/usr/lib/jvm/java-1.11.0"
                }
            }
        ],
        "Properties": {}
    }
```

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
