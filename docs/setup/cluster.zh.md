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

# 搭建您的 Apache Spark 集群

请从 [Spark 下载页面](http://spark.apache.org/downloads.html) 下载 Spark 发行版。

## 前置准备

1. 在集群上配置免密 SSH。每对 master-worker 之间需要双向免密 SSH。
2. 确保已安装 JRE 1.8 或更高版本。
3. 将所有 worker 的 IP 地址列入 `./conf/slaves`。
4. 除必要的 Spark 配置外，您可能还需要在 Spark 配置文件中加入以下设置以避免 Sedona 内存相关错误：

在 `./conf/spark-defaults.conf` 中：

```
spark.driver.memory 10g
spark.network.timeout 1000s
spark.driver.maxResultSize 5g
```

* `spark.driver.memory` 用于为 driver 程序分配足够的内存。Sedona 需要在 driver 上构建全局网格文件（全局索引）。如果数据量较大（通常超过 100 GB），将该参数设为 2~5 GB 较为合适，否则可能出现 “out of memory” 错误。
* `spark.network.timeout` 是所有网络交互的默认超时时间。空间连接查询有时需要更长的 shuffle 时间，调大此参数可让 Spark 有足够的耐心等待结果。
* `spark.driver.maxResultSize` 是每个 Spark action 中所有分区的序列化结果总大小限制。空间查询的结果有时较大，`Collect` 操作可能会抛出错误。

更多 Spark 参数细节请参阅 [Spark 官方文档](https://spark.apache.org/docs/latest/configuration.html)。

## 启动集群

进入解压后的 Apache Spark 目录的根目录，通过终端启动 Spark 集群：

```
./sbin/start-all.sh
```
