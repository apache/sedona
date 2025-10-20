.. sedona-python documentation master file, created by
   sphinx-quickstart on Sat Jul  5 08:38:02 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

Apache Sedona™ Python API Documentation
========================================

.. raw:: html

   <div class="sedona-hero">
       <h1>🌍 Apache Sedona™ Python API</h1>
       <p class="hero-subtitle">A cluster computing system for processing large-scale spatial data</p>
   </div>

Welcome to Apache Sedona™
==========================

Apache Sedona™ is a cluster computing system for processing large-scale spatial data. Sedona extends existing cluster computing systems, such as Apache Spark, Apache Flink, and Snowflake, with a set of out-of-the-box distributed Spatial Datasets and Spatial SQL that efficiently load, process, and analyze large-scale spatial data across machines.

This documentation covers the Python API for Apache Sedona, providing comprehensive guides and references for:

- **Spatial RDDs**: Distributed spatial data structures
- **Spatial DataFrames**: Spark DataFrame integration with spatial operations
- **Spatial Functions**: Built-in spatial analysis and processing functions
- **Visualization**: Map creation and spatial data visualization tools
- **GeoPandas Integration**: Seamless integration with the GeoPandas ecosystem

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules
   sedona.flink
   sedona.spark
   sedona.spark.geopandas
   sedona.stac
   sedona.stats
   sedona.utils
