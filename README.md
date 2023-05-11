<img src="https://www.apache.org/logos/res/sedona/sedona.png" width="400">

[![Scala and Java build](https://github.com/apache/sedona/actions/workflows/java.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/java.yml) [![Python build](https://github.com/apache/sedona/actions/workflows/python.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python.yml) [![R build](https://github.com/apache/sedona/actions/workflows/r.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/r.yml) [![Example project build](https://github.com/apache/sedona/actions/workflows/example.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/example.yml) [![Docs build](https://github.com/apache/sedona/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docs.yml)

Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder) and play the interactive Sedona Python Jupyter Notebook immediately!

[![](https://dcbadge.vercel.app/api/server/9A3k5dEBsY)](https://discord.gg/9A3k5dEBsY)

Apache Sedona™ is a spatial computing engine that enables developers to easily process spatial data at any scale within modern cluster computing systems such as Apache Spark and Apache Flink. Sedona developers can express their spatial data processing tasks in Spatial SQL, Spatial Python or Spatial R. Internally, Sedona provides spaital data loading, indexing, partitioning, and query processing functionality that enable user analyze large-scale spatial data.


<img src="https://github.com/MoSarwat/sedona/blob/master/docs/image/SedonaNewFig.001.png" width="800">

## Processing Spatial Data with Sedona

### Load NYC taxi trip and taxi zones data from AWS S3
```
taxidf = spark.read.format('csv').option("header","true").option("delimiter", ",").load("s3a://your-directory/data/nyc-taxi-data.csv")
taxidf = taxidf.selectExpr('ST_Point(CAST(Start_Lon AS Decimal(24,20)), CAST(Start_Lat AS Decimal(24,20))) AS pickup', 'Trip_Pickup_DateTime', 'Payment_Type', 'Fare_Amt')
taxidf.createOrReplaceTempView('taxiDf')

```
```
zoneDf = spark.read.format('csv').option("delimiter", ",").load("s3a://wherobots-examples/data/TIGER2018_ZCTA5.csv")
zoneDf = zoneDf.selectExpr('ST_GeomFromWKT(_c0) as zone', '_c1 as zipcode')
zoneDf.createOrReplaceTempView('zoneDf')
```


### Spatial SQL query to only keep records in Manhattan

```

taxidf_mhtn = taxidf.where('ST_Contains(ST_PolygonFromEnvelope(-74.01,40.73,-73.93,40.79), pickup)')

```

### Show a map of the loaded Spatial Dataframes using GeoPandas

```
zoneGpd = gpd.GeoDataFrame(zoneDf.toPandas(), geometry="zone")
taxiGpd = gpd.GeoDataFrame(taxidf.toPandas(), geometry="pickup")

zone = zoneGpd.plot(color='yellow', edgecolor='black', zorder=1)
zone.set_xlabel('Longitude (degrees)')
zone.set_ylabel('Latitude (degrees)')

# Local view
zone.set_xlim(-74.1, -73.8)
zone.set_ylim(40.65, 40.9)

taxi = taxiGpd.plot(ax=zone, alpha=0.01, color='red', zorder=3)
```

|Download statistics| **Maven** | **PyPI** | **CRAN** |
|:-------------:|:------------------:|:--------------:|:---------:|
| Apache Sedona |         180k/month        |[![Downloads](https://static.pepy.tech/personalized-badge/apache-sedona?period=month&units=international_system&left_color=black&right_color=brightgreen&left_text=downloads/month)](https://pepy.tech/project/apache-sedona) [![Downloads](https://static.pepy.tech/personalized-badge/apache-sedona?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/apache-sedona)|[![](https://cranlogs.r-pkg.org/badges/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) [![](https://cranlogs.r-pkg.org/badges/grand-total/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona)|
|    Archived GeoSpark releases   |10k/month|[![Downloads](https://static.pepy.tech/personalized-badge/geospark?period=month&units=international_system&left_color=black&right_color=brightgreen&left_text=downloads/month)](https://pepy.tech/project/geospark)[![Downloads](https://static.pepy.tech/personalized-badge/geospark?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/geospark)|           |


## Our users and code contributors are from ...

<img src="docs/image/sedona-community.png" width="600">

## Modules in the source code

| Name  |  API |  Introduction|
|---|---|---|
|Core  | Scala/Java  | Distributed Spatial Datasets and Query Operators |
|SQL  | Spark RDD/DataFrame in Scala/Java/SQL  |Geospatial data processing on Apache Spark|
|Flink | Flink DataStream/Table in Scala/Java/SQL | Geospatial data processing on Apache Flink
|Viz |  Spark RDD/DataFrame in Scala/Java/SQL | Geospatial data visualization on Apache Spark|
|Python | Spark RDD/DataFrame in Python | Python wrapper for Sedona |
|R | Spark RDD/DataFrame in R | R wrapper for Sedona |
|Zeppelin |  Apache Zeppelin | Plugin for Apache Zeppelin 0.8.1+|

### Sedona supports several programming languages: Scala, Java, SQL, Python and R.

## Compile the source code

Please refer to [Sedona website](https://sedona.apache.org/latest-snapshot/setup/compile/)

## Contact

Twitter: [Sedona@Twitter](https://twitter.com/ApacheSedona)

[![](https://dcbadge.vercel.app/api/server/9A3k5dEBsY)](https://discord.gg/9A3k5dEBsY)


[Sedona JIRA](https://issues.apache.org/jira/projects/SEDONA): Bugs, Pull Requests, and other similar issues

[Sedona Mailing Lists](https://lists.apache.org/list.html?sedona.apache.org): [dev@sedona.apache.org](https://lists.apache.org/list.html?dev@sedona.apache.org): project development, general questions or tutorials.

* Please first subscribe and then post emails. To subscribe, please send an email (leave the subject and content blank) to dev-subscribe@sedona.apache.org

# Please visit [Apache Sedona website](http://sedona.apache.org/) for detailed information

## Powered by

<img src="https://www.apache.org/foundation/press/kit/asf_logo_wide.png" width="500">
