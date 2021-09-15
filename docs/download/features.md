# Don't have time? Read [Quick Start](/download/overview)

Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/incubator-sedona/HEAD?filepath=binder) and play the interactive Sedona Python Jupyter Notebook immediately!

## Companies are using Sedona 

[<img src="https://www.dataiku.com/static/img/partners/LOGO-Blue-DME-PNG-3.png" width="200">](https://www.bluedme.com/) &nbsp;&nbsp; 
[<img src="https://images.ukfast.co.uk/comms/news/businesscloud/photos/14-08-2018/gyana.jpg" width="150">](https://www.gyana.co.uk/) &nbsp;&nbsp; 
[<img src="https://149448277.v2.pressablecdn.com/wp-content/uploads/2021/03/guy-carpenter-logo-1.png" width="200">](https://guycarp.com/) &nbsp;&nbsp; 
[<img src="https://mobike.com/global/public/invitation__footer__logo.png" width="150">](https://mobike.com) and more!

Please make a Pull Request to add yourself

## Sedona modules

| Name  |  API |  Introduction|
|---|---|---|
|Core  | RDD  | SpatialRDDs and Query Operators. |
|SQL  | SQL/DataFrame  |SQL interfaces for Sedona core.|
|Viz |  RDD, SQL/DataFrame | Visualization for Spatial RDD and DataFrame|
|Zeppelin |  Apache Zeppelin | Plugin for Apache Zeppelin 0.8.1+|

## Features

* ==Spatial RDD==
* ==Spatial SQL==
* ==Complex geometries / trajectories==: point, polygon, linestring, multi-point, multi-polygon, multi-linestring, GeometryCollection
* ==Various input formats==: CSV, TSV, WKT, WKB, GeoJSON, NASA NetCDF/HDF, Shapefile (.shp, .shx, .dbf): extension must be in lower case
* ==Spatial query==: range query, range join query, distance join query, K Nearest Neighbor query
* ==Spatial index==: R-Tree, Quad-Tree
* ==Spatial partitioning==: KDB-Tree, Quad-Tree, R-Tree, Voronoi diagram, Hilbert curve, Uniform grids
* ==Coordinate Reference System / Spatial Reference System Transformation==: for exmaple, from WGS84 (EPSG:4326, degree-based), to EPSG:3857 (meter-based)
* ==High resolution map generation==: [Visualize Spatial DataFrame/RDD](../../tutorial/viz)

<img style="float: left;" src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="250">
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/ustweet.png" width="250">

[Watch the high resolution version on a real map](http://www.public.asu.edu/~jiayu2/geospark/picture/overlay.html)

<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/heatmapnycsmall.png" width="500">

