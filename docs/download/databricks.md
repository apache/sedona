# Installation on Databricks

## Community edition (free-tier)

You just need to install the Sedona jars and Sedona Python on Databricks using Databricks default web UI. Then everything will work.

## Advanced editions

If you are not using the free version of Databricks, there is an issue with the path where Sedona Python looks for the jar. Thanks to the report from Sedona user @amoyrand.

Two steps to fix this:

1. Upload the jars in `/dbfs/FileStore/jars/`
2. Add the jar path to Spark session config. For example, `.config("spark.jars", "/dbfs/FileStore/jars/sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar") \`. 

## Pure SQL environment

Sedona cannot be used in [a pure SQL environment](../../tutorial/sql-pure-sql) (e.g., an SQL notebook) on Databricks. You have to mix it with Scala or Python in order to call `SedonaSQLRegistrator.registerAll(sparkSession)`. Please see a similar report on [Stackoverflow](https://stackoverflow.com/questions/66721168/sparksessionextensions-injectfunction-in-databricks-environment).