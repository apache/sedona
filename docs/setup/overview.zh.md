<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 下载统计

| 下载统计                  | **Maven**  | **PyPI**                                                                                                                                                                                                                                                                                                                                     | Conda-forge                                                                                                                                     | **CRAN**                                                                                                                                                                                                                                                                                                      | **DockerHub**                                                                                                                  |
|----------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Apache Sedona              | 33 万次/月 | [![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-sedona)](https://pepy.tech/project/apache-sedona) [![Downloads](https://static.pepy.tech/personalized-badge/apache-sedona?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/apache-sedona) | [![Anaconda-Server Badge](https://anaconda.org/conda-forge/apache-sedona/badges/downloads.svg)](https://anaconda.org/conda-forge/apache-sedona) | [![CRAN downloads per month](https://cranlogs.r-pkg.org/badges/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) [![Total CRAN downloads](https://cranlogs.r-pkg.org/badges/grand-total/apache.sedona?color=brightgreen)](https://cran.r-project.org/package=apache.sedona) | [![Docker pulls](https://img.shields.io/docker/pulls/apache/sedona?color=brightgreen)](https://hub.docker.com/r/apache/sedona) |
| 已归档的 GeoSpark 版本     | 1 万次/月  | [![PyPI - Downloads](https://img.shields.io/pypi/dm/geospark)](https://pepy.tech/project/geospark)[![Downloads](https://static.pepy.tech/personalized-badge/geospark?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=total%20downloads)](https://pepy.tech/project/geospark)                      |                                                                                                                                                 |                                                                                                                                                                                                                                                                                                               |                                                                                                                                |

# Sedona 能做什么？

## 分布式空间数据集

- [x] Spark 上的空间 RDD
- [x] Spark 上的空间 DataFrame/SQL
- [x] Flink 上的空间 DataStream
- [x] Flink 上的空间 Table/SQL
- [x] Snowflake 上的空间 SQL

## 复杂空间对象

- [x] 矢量几何对象 / 轨迹
- [x] 支持地图代数（Map Algebra）的栅格图像
- [x] 多种输入格式：CSV、TSV、WKT、WKB、GeoJSON、Shapefile、GeoTIFF、ArcGrid、NetCDF/HDF

## 分布式空间查询

- [x] 空间查询：范围查询、范围连接查询、距离连接查询、K 最近邻查询
- [x] 空间索引：R-Tree、Quad-Tree

## 丰富的空间分析工具

- [x] 坐标参考系（CRS）/ 空间参考系（SRS）转换
- [x] Apache Zeppelin 仪表盘集成
- [X] 与 Jupyter Notebook、GeoPandas、Shapely 等多种 Python 工具集成
- [X] 与 KeplerGL、DeckGL 等多种可视化工具集成
- [x] 高分辨率、可扩展的地图生成：[可视化空间 DataFrame/RDD](../tutorial/viz.md)
- [x] 支持 Scala、Java、Python、R
