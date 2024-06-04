
This tutorial will cover how to configure both a glue notebook and a glue ETL job. The tutorial is written assuming you
have a working knowledge of AWS Glue jobs and S3.

In the tutorial, we use
Sedona 1.6.0 and [Glue 4](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html) which runs on Spark 3.3.0, Java 8, Scala 2.12,
and Python 3.10. We recommend Sedona-1.3.1-incubating and above for Glue.

## Stage Sedona Jar in S3

In an AWS S3 bucket you will need the sedona-spark-shaded and geotools-wrapper jars. There are two options to get these.
Ensure the locations of the jars are accessible to your glue job.

### Option 1: Use the Wherobots-hosted jars

[Wherobots](https://wherobots.com/) provides a public S3 bucket with the necessary jars. You can point to these directly in your glue jobs'
configurations. For 1.6.0, the sedona and geotools jars are available at the following locations:

* `s3://wherobots-sedona-jars/1.6.0/sedona-spark-shaded-3.0_2.12-1.6.0.jar`
* `s3://wherobots-sedona-jars/1.6.0/geotools-wrapper-1.6.0-28.2.jar`

### Option 2: Stage your own Jars

In your S3 bucket, add the Sedona Jar
from [Maven Central](https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.6.0/sedona-spark-shaded-3.0_2.12-1.6.0.jar). Ensure you pick a
version for Scala 2.12.
Similarly, add the Geotools Wrapper Jar
from [Maven Central](https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.6.0-28.2/geotools-wrapper-1.6.0-28.2.jar).

!!!note
    If you use Sedona 1.3.1-incubating, please use `sedona-python-adpater-3.0_2.12` jar in the content above, instead
    of `sedona-spark-shaded-3.0_2.12`.

## Configure Glue Job

Once your jars are staged in S3, you can configure your Glue job to use them, as well as the apache-sedona python
package. How you do this varies slightly between the notebook and the script job types.

### Notebook Job

Add the following cell magics before starting your sparkContext of glueContext. The first points to the jars put in s3,
and the second installs the sedona python package directly from pip.

```python
# Sedona Config
%extra_jars s3://path/to/my-sedona.jar, s3://path/to/my-geotools.jar
%additional_python_modules apache-sedona
```

If using the Wherobots-provided jars, the cell magics would look like this:

```python
# Sedona Config
%extra_jars s3://wherobots-sedona-jars/1.6.0/sedona-spark-shaded-3.0_2.12-1.6.0.jar, s3://wherobots-sedona-jars/1.6.0/geotools-wrapper-1.6.0-28.2.jar
%additional_python_modules apache-sedona
```

If you are using the example notebook from glue, the first cell should now look like this:

```python
%idle_timeout 2880
%glue_version 4.0
%worker_type G.1X
%number_of_workers 5

# Sedona Config
%extra_jars s3://wherobots-sedona-jars/1.6.0/sedona-spark-shaded-3.0_2.12-1.6.0.jar, s3://wherobots-sedona-jars/1.6.0/geotools-wrapper-1.6.0-28.2.jar
%additional_python_modules apache-sedona


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

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

### ETL Job

Glue also calls these Scripts. From your job's page, navigate to the "Job details" tab. At the bottom of the page expand
the "Advanced properties" section. In the "Dependent JARs path" field, add the paths to the jars in S3, separated by a comma.

To add the sedona python package, navigate to the "Job Parameters" section and add a new parameter with the key
`--additional-python-modules` and the value `apache-sedona`.

To confirm the installation add the follow code to the script:

```python
from sedona.spark import *

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

Once added to the script, save and run the job. If the job runs successfully, the installation was successful.
