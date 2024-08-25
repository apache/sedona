# Download statistics

| Download statistics        | **Maven**  | **PyPI**                                                                                                                                                                                                                                                                                                                                     | Conda-forge                                                                                                                                     | **CRAN**                                                                                                                                                                                                                                                          | **DockerHub**                                                                                                                  |
|----------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Apache Sedona              | 225k/month | [![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-sedona)](https://pepy.tech/project/apache-sedona) [![Downloads](https://static.pepy.tech/personalized-badge/apache-sedona?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/apache-sedona) | [![Anaconda-Server Badge](https://anaconda.org/conda-forge/apache-sedona/badges/downloads.svg)](https://anaconda.org/conda-forge/apache-sedona) | [![](https://cranlogs.r-pkg.org/badges/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) [![](https://cranlogs.r-pkg.org/badges/grand-total/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) | [![Docker pulls](https://img.shields.io/docker/pulls/apache/sedona?color=brightgreen)](https://hub.docker.com/r/apache/sedona) |
| Archived GeoSpark releases | 10k/month  | [![PyPI - Downloads](https://img.shields.io/pypi/dm/geospark)](https://pepy.tech/project/geospark)[![Downloads](https://static.pepy.tech/personalized-badge/geospark?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/geospark)                      |                                                                                                                                                 |                                                                                                                                                                                                                                                                   |                                                                                                                                |

# What can Sedona do?

## Distributed spatial datasets

- [x] Spatial RDD on Spark
- [x] Spatial DataFrame/SQL on Spark
- [x] Spatial DataStream on Flink
- [x] Spatial Table/SQL on Flink
- [x] Spatial SQL on Snowflake

## Complex spatial objects

- [x] Vector geometries / trajectories
- [x] Raster images with Map Algebra
- [x] Various input formats: CSV, TSV, WKT, WKB, GeoJSON, Shapefile, GeoTIFF, ArcGrid, NetCDF/HDF

## Distributed spatial queries

- [x] Spatial query: range query, range join query, distance join query, K Nearest Neighbor query
- [x] Spatial index: R-Tree, Quad-Tree

## Rich spatial analytics tools

- [x] Coordinate Reference System / Spatial Reference System Transformation
- [x] Apache Zeppelin dashboard integration
- [X] Integrate with a variety of Python tools including Jupyter notebook, GeoPandas, Shapely
- [X] Integrate with a variety of visualization tools including KeplerGL, DeckGL
- [x] High resolution and scalable map generation: [Visualize Spatial DataFrame/RDD](../tutorial/viz.md)
- [x] Support Scala, Java, Python, R
