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

安装 Apache Sedona Python，请按以下步骤操作：

1. 安装所需的 Python 包。

```
pip install apache-sedona[flink] shapely attr
```

2. 从 Maven Central 下载所需的 JAR 文件：

* sedona-flink-shaded_2.12:jar:{{ sedona.current_version }}
* geotools-wrapper-{{ sedona.current_geotools }}.jar

要将这些 JAR 文件安装到您的 Flink 集群或 PyFlink 应用中，请参阅 Flink 官方文档：
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/
