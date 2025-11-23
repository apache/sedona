# SedonaSpark

SedonaSpark extends Apache Spark with a rich set of out-of-the-box distributed Spatial Datasets and functions that efficiently load, process, and analyze large-scale spatial data across machines. SedonaSpark is an excellent option for datasets too large for a single machine.

=== "SQL"

    ```sql
    SELECT superhero.name
    FROM city, superhero
    WHERE ST_Contains(city.geom, superhero.geom)
    AND city.name = 'Gotham'
    ```

=== "PySpark"

    ```python
    sedona.sql("""
        SELECT superhero.name
        FROM city, superhero
        WHERE ST_Contains(city.geom, superhero.geom)
        AND city.name = 'Gotham'
    """)
    ```

=== "Java"

    ```java
    Dataset<Row> result = spark.sql(
    "SELECT superhero.name " +
    "FROM city, superhero " +
    "WHERE ST_Contains(city.geom, superhero.geom) " +
    "AND city.name = 'Gotham'"
    );
    ```

=== "Scala"

    ```scala
    sedona.sql("""
        SELECT superhero.name
        FROM city, superhero
        WHERE ST_Contains(city.geom, superhero.geom)
        AND city.name = 'Gotham'
    """)
    ```

=== "R"

    ```r
    result <- sql("
    SELECT superhero.name
    FROM city, superhero
    WHERE ST_Contains(city.geom, superhero.geom)
    AND city.name = 'Gotham'
    ")
    ```

## Key features

* **Blazing fast**: SedonaSpark executes computations in parallel on many nodes in a cluster so that large computations can run fast.
* Supports **various file formats**, including GeoJSON, Shapefile, GeoParquet, STAC, JDBC, OSM PBF, CSV, and PostGIS.
* Exposes several **language APIs,** including SQL, Python, Java, Scala, and R.
* **Scalable**: Horizontally scale to tens, hundreds, or thousands of nodes depending on the size of your data.  You can process massive spatial datasets with SedonaSpark.
* **Portable**: Easy to run in a custom environment, locally or in the cloud with AWS EMR, Microsoft Fabric, or Google DataProc.
* **Extensible**: You can extend SedonaSpark with your custom logic that suits your specific geospatial data analysis needs.
* **Open source**: Apache Sedona is an open-source project managed in accordance with the Apache Software Foundation's guidelines.
* Extra functionality like [nearest neighbor searching](https://sedona.apache.org/latest/api/sql/NearestNeighbourSearching/) and geostats like [DBSCAN](https://sedona.apache.org/latest/tutorial/sql/#cluster-with-dbscan)

## Installation

Here’s how to install SedonaSpark with various build tools:

=== "pip"

    ```bash
    pip install apache-sedona
    ```

=== "SBT"

    ```
    libraryDependencies += "org.apache.sedona" % "sedona-common" % "{{ sedona.current_version }}"
    ```

=== "Gradle"

    ```
    implementation("org.apache.sedona:sedona-common:{{ sedona.current_version }}")
    ```

=== "Maven"

    ```
    <dependency>
        <groupId>org.apache.sedona</groupId>
        <artifactId>sedona-common</artifactId>
        <version>{{ sedona.current_version }}</version>
    </dependency>
    ```

=== "R"

    ```r
    install.packages("apache.sedona")
    ```

## Portability

It’s easy to run SedonaSpark locally, with Docker, or on any popular cloud.

SedonaSpark is designed to be run in any environment where Spark can run.  Many cloud vendors have Spark runtimes, and Sedona can be added as a library dependency.

Running Sedona locally is handy, allowing you to iterate on code before deploying it to production datasets.

## Spark and Sedona example with vector data

Let’s take a look at how to perform a workflow on a vector dataset with Spark and Sedona.

Let’s use the base water data supplied by the Overture Maps Foundation to map all the bodies of water in the New York City area.  Start by reading the data and creating a view:

```
base_water = sedona.table("open_data.overture_maps_foundation.base_water")
base_water.createOrReplaceTempView("base_water_view")
```

Now filter the dataset to include the bodies of water in the New York City area.

```python
spot = "POLYGON ((-74.174194 40.509623, -73.635864 40.509623, -73.635864 40.93634, -74.174194 40.93634, -74.174194 40.509623))"
query = f"""
select id, geometry from base_water_view
where ST_Contains(ST_GeomFromWKT('{spot}'), geometry)
"""
res = sedona.sql(query)
```

Sedona integrates seamlessly with popular graphing libraries, making it easy to create graphs from a Sedona DataFrame.  You can build a map with just two lines of code:

```python
kepler_map = SedonaKepler.create_map()
SedonaKepler.add_df(kepler_map, df=res, name="Tri-state water")
```

The map looks amazing!

![New York City water](../image/nyc_base_water.png)

You can easily see all of the rivers, lakes, and swimming pools in the New York City area with this map.

## Have questions?

Feel free to start a GitHub Discussion or join the Discord community to ask the developers any questions you may have.

We look forward to collaborating with you!
