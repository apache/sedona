## Companies are using Sedona 

(incomplete list)

[<img src="https://www.dataiku.com/static/img/partners/LOGO-Blue-DME-PNG-3.png" width="200">](https://www.bluedme.com/) &nbsp;&nbsp; [<img src="https://images.ukfast.co.uk/comms/news/businesscloud/photos/14-08-2018/gyana.jpg" width="150">](https://www.gyana.co.uk/) &nbsp;&nbsp; [<img src="https://mobike.com/global/public/invitation__footer__logo.png" width="150">](https://mobike.com)

Please make a Pull Request to add yourself!

## Sedona modules

| Name  |  API |  Spark compatibility|Introduction|
|---|---|---|---|
|Core  | RDD  | Spark 3.X/2.X/1.X  |SpatialRDDs and Query Operators. |
|SQL  | SQL/DataFrame  | SparkSQL 3.X/2.1+ |SQL interfaces for Sedona core.|
|Viz |  RDD, SQL/DataFrame | RDD - Spark 3.X/2.X/1.X, SQL - Spark 3.X/2.1+|Visualization for Spatial RDD and DataFrame.|
|Zeppelin |  Apache Zeppelin | Spark 2.1+, Zeppelin 0.8.1+| Plugin for Apache Zeppelin|

## Features

* ==Spatial RDD==
* ==Spatial SQL==
```SQL
SELECT superhero.name
FROM city, superhero
WHERE ST_Contains(city.geom, superhero.geom)
AND city.name = 'Gotham';
```
* ==Complex geometries / trajectories==: point, polygon, linestring, multi-point, multi-polygon, multi-linestring, GeometryCollection
* ==Various input formats==: CSV, TSV, WKT, WKB, GeoJSON, NASA NetCDF/HDF, Shapefile (.shp, .shx, .dbf): extension must be in lower case
* ==Spatial query==: range query, range join query, distance join query, K Nearest Neighbor query
* ==Spatial index==: R-Tree, Quad-Tree
* ==Spatial partitioning==: KDB-Tree, Quad-Tree, R-Tree, Voronoi diagram, Hilbert curve, Uniform grids
* ==Coordinate Reference System / Spatial Reference System Transformation==: for exmaple, from WGS84 (EPSG:4326, degree-based), to EPSG:3857 (meter-based)
* ==High resolution map==: Scatter plot, heat map, choropleth map

## Sedona language support

| Language | Supported Sedona modules | Tutorial |
|:--------:|:--------------------------:|:--------:|
|   Scala  |   RDD, SQL, Viz, Zeppelin   | [RDD](../../tutorial/rdd), [SQL](../../tutorial/sql), [Viz](../../tutorial/viz), [Zeppelin](../../tutorial/zeppelin)|
|   Java   |        RDD, SQL, Viz       |[RDD](../../tutorial/rdd), [SQL](../../tutorial/sql), [Viz](../../tutorial/viz)|
|    SQL   |          SQL, Viz, Zeppelin          |[SQL](../../tutorial/sql), [Viz](../../tutorial/viz), [Zeppelin](../../tutorial/zeppelin)|
|  Python  |             RDD, SQL            |[RDD](../../tutorial/geospark-core-python), [SQL](../../tutorial/geospark-sql-python)|
|     R    |             SQL            ||

## Sedona Visualization Extension (GeoSparkViz)
GeoSparkViz is a large-scale in-memory geospatial visualization system.

GeoSparkViz provides native support for general cartographic design by extending GeoSpark to process large-scale spatial data. It can visulize Spatial RDD and Spatial Queries and render super high resolution image in parallel.

More details are available here: [Visualize Spatial DataFrame/RDD](../../tutorial/viz)

**GeoSparkViz Gallery**


<img style="float: left;" src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="250">
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/ustweet.png" width="250">

[Watch the high resolution version on a real map](http://www.public.asu.edu/~jiayu2/geospark/picture/overlay.html)

<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/heatmapnycsmall.png" width="500">

