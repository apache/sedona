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

# Apache Spark 上的 Sedona 模块

| 名称           |  API                  |  介绍                                                |
|----------------|-----------------------|------------------------------------------------------|
| spark          | RDD / SQL / DataFrame | SpatialRDD 与空间 DataFrame                          |
| spark-shaded   |                       | Shaded 版本                                          |
| python         |                       | SpatialRDD 与空间 DataFrame 的 Python 接口           |
| Zeppelin       | Apache Zeppelin       | 适用于 Apache Zeppelin 0.8.1+ 的插件                 |

## API 可用性

|            | **Core/RDD** | **DataFrame/SQL** | **Viz RDD/SQL** |
|:----------:|:------------:|:-----------------:|:---------------:|
| Scala/Java |✅|✅|✅|
|   Python   |✅|✅|仅 SQL|
|      R     |✅|✅|✅|
