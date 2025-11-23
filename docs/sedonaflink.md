# SedonaFlink

SedonaFlink integrates geospatial functions into Apache Flink, making it an excellent option for streaming pipelines that utilize geospatial data.

Here are some example SedonaFlink use cases:

* Read geospatial data from Kafka and write to Iceberg
* [Analyze real-time traffic density](https://www.alibabacloud.com/help/en/flink/realtime-flink/use-cases/analyze-traffic-density-with-flink-and-apache-sedona)
* Real-time network planning and optimization for telecommunication

Here are some example code snippets:

=== "Java"

    ```java
    sedona.createTemporaryView("myTable", tbl)
    Table geomTbl = sedona.sqlQuery("SELECT ST_GeomFromWKT(geom_polygon) as geom_polygon, name_polygon FROM myTable")
    geomTbl.execute().print()
    ```

=== "PyFlink"

    ```python
    table_env.sql_query("SELECT ST_ASBinary(ST_Point(1.0, 2.0))").execute().collect()
    ```

## Key features

* **Real-time geospatial stream processing** for low-latency processing needs.
* **Scalable** processing suitable for large streaming pipelines.
* **Event time processing** with Flink’s time-windowing.
* **Exactly once** processing guarantees.
* **Portable** and easy to run in any Flink runtime.
* **Open source** and managed according to the Apache Software Foundation's guidelines.

## Why Sedona on Flink?

Flink is built for streaming data, and Sedona enhances it with geospatial functionality.

Most geospatial processing occurs in batch systems such as Spark or PostGIS, which is fine for lower-latency use cases.

Sedona on Flink shines when you need to process geospatial data in real-time.

Flink can deliver millisecond-level latency for geospatial queries.

Flink has solid fault tolerance, so your geospatial pipelines won't lose data, even when things break.

Sedona on Flink runs anywhere Flink runs, including Kubernetes, YARN, and standalone clusters.

## How It Works

Sedona integrates directly into Flink's Table API and SQL engine.

You register Sedona's spatial functions when you set up your Flink environment. Then, you can use functions such as `ST_Point`, `ST_Contains`, and `ST_Distance` in your SQL queries.

Sedona works with both Flink's DataStream API and Table API. Use whichever fits your workflow.

The spatial operations run as part of Flink's distributed execution, so your geospatial computations are automatically parallelized across your cluster.

Sedona stores geometries as binary data in Flink's internal format. This keeps memory usage low and processing fast.

When you perform spatial joins, Sedona utilizes spatial indexing under the hood, enabling it to execute queries quickly.

Flink's checkpointing system handles fault tolerance.  If a node crashes, your geospatial state is restored from the last checkpoint.

You read geospatial data from sources such as Kafka or file systems, process it using Sedona's spatial functions, and write the results to sinks such as Iceberg.

The entire SedonaFlink pipeline runs continuously, allowing new events to flow through your spatial transformations in real-time.

## Comparison with alternatives

For small datasets, you may not need a distributed cluster and can use SedonaDB.

For large batch pipelines, you can use SedonaSpark.

Here are some direct comparisons of SedonaFlink vs. streaming alternatives.

**SedonaFlink vs. Sedona on Spark Structured Streaming**

Spark Streaming uses micro-batches, whereas Flink processes events one at a time.  This can provide Flink with lower latency for some workflows.

Flink's state management is also more sophisticated.

Use Spark if you're already invested in the Spark ecosystem and the Spark Structured Streaming latency is sufficiently low for your use case. Use Flink if you need very low latency.

**Sedona on Flink vs. PostGIS**

PostGIS is great for storing and querying geospatial data for OLTP workflows. But it's not built for streaming.

If you use PostGIS for streaming workflows, you need to constantly query the database from your stream processor, which adds latency and puts load on your database.

SedonaFlink processes geospatial data in-flight, eliminating the need for database round-trips.
