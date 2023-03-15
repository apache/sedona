Starting from ==Sedona v1.0.1==, you can use Sedona in a pure Spark SQL environment. The example code is written in SQL.

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. Detailed SedonaSQL APIs are available here: [SedonaSQL API](../api/sql/Overview.md)


## Initiate Session

Start `spark-sql` as following (replace `<VERSION>` with actual version, like, `1.0.1-incubating`):

```sh
spark-sql --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:<VERSION>,org.apache.sedona:sedona-viz-3.0_2.12:<VERSION>,org.datasyslab:geotools-wrapper:geotools-24.0 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrator=org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator \
  --conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions
```


This will register all User Defined Tyeps, functions and optimizations in SedonaSQL and SedonaViz.

## Load data

Let use data from `examples/sql`.  To load data from CSV file we need to execute two commands:


Use the following code to load the data and create a raw DataFrame:

```sql
CREATE TABLE IF NOT EXISTS pointraw (_c0 string, _c1 string) 
USING csv 
OPTIONS(header='false') 
LOCATION '<some path>/sedona/examples/sql/src/test/resources/testpoint.csv';

CREATE TABLE IF NOT EXISTS polygonraw (_c0 string, _c1 string, _c2 string, _c3 string) 
USING csv 
OPTIONS(header='false') 
LOCATION '<some path>/sedona/examples/sql/src/test/resources/testenvelope.csv';

```

## Transform the data

We need to transform our point and polygon data into respective types:

```sql
CREATE OR REPLACE TEMP VIEW pointdata AS
  SELECT ST_Point(cast(pointraw._c0 as Decimal(24,20)), cast(pointraw._c1 as Decimal(24,20))) AS pointshape
  FROM pointraw;

CREATE OR REPLACE TEMP VIEW polygondata AS
  select ST_PolygonFromEnvelope(cast(polygonraw._c0 as Decimal(24,20)),
        cast(polygonraw._c1 as Decimal(24,20)), cast(polygonraw._c2 as Decimal(24,20)), 
        cast(polygonraw._c3 as Decimal(24,20))) AS polygonshape 
  FROM polygonraw;
```

## Work with data

For example, let join polygon and test data:

```sql
SELECT * from polygondata, pointdata 
WHERE ST_Contains(polygondata.polygonshape, pointdata.pointshape) 
      AND ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondata.polygonshape)
LIMIT 5;
```

