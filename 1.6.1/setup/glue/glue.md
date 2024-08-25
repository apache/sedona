
This tutorial will cover how to configure both a glue notebook and a glue ETL job. The tutorial is written assuming you
have a working knowledge of AWS Glue jobs.

In the tutorial, we use
Sedona {{ sedona.current_version }} and [Glue 4.0](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html) which runs on Spark 3.3.0, Java 8, Scala 2.12,
and Python 3.10. We recommend Sedona-1.3.1-incubating and above for Glue.

## Gather Maven Links

You will need to point your glue job to the Sedona and Geotools jars. We recommend using the jars available from maven. The links below are those intended for Glue 4.0

Sedona Jar: [Maven Central](https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar)

Geotools Jar: [Maven Central](https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar)

!!!note
    If you use Sedona 1.3.1-incubating, please use `sedona-python-adpater-3.0_2.12` jar in the content above, instead
    of `sedona-spark-shaded-3.0_2.12`. Ensure you pick a version for Scala 2.12 and Spark 3.0. The Spark 3.4 and Scala
    2.13 jars are not compatible with Glue 4.0.

## Configure Glue Job

Once you have your jar links, you can configure your Glue job to use them, as well as the apache-sedona Python
package. How you do this varies slightly between the notebook and the script job types.

!!!note
    Always ensure that the Sedona version of the jars and the Python package match.

### Notebook Job

Add the following cell magics before starting your sparkContext or glueContext. The first points to the jars,
and the second installs the Sedona Python package directly from pip.

```python
# Sedona Config
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar, https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar
%additional_python_modules apache-sedona=={{ sedona.current_version }}
```

If you are using the example notebook from glue, the first cell should now look like this:

```python
%idle_timeout 2880
%glue_version 4.0
%worker_type G.1X
%number_of_workers 5

# Sedona Config
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar, https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar
%additional_python_modules apache-sedona=={{ sedona.current_version }}


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
```

You can confirm your installation by running the following cell:

```python
from sedona.spark import *

sedona = SedonaContext.create(spark)
sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

### ETL Job

Glue also calls these Scripts. From your job's page, navigate to the "Job details" tab. At the bottom of the page expand
the "Advanced properties" section. In the "Dependent JARs path" field, add the paths to the jars, separated by a comma.

To add the Sedona Python package, navigate to the "Job Parameters" section and add a new parameter with the key
`--additional-python-modules` and the value `apache-sedona=={{ sedona.current_version }}`.

To confirm the installation add the follow code to the script:

```python
from sedona.spark import *

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

Once added to the script, save and run the job. If the job runs successfully, the installation was successful.
