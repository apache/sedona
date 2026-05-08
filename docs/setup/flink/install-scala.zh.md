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

在开始 Sedona 之旅之前，请确保您的 Apache Flink 集群已就绪。

随后即可创建一个独立的 Scala / Java 项目。独立项目可以让您创建多个 Scala / Java 文件，并将复杂的逻辑集中实现。

要在独立的 Flink 项目中使用 Sedona，只需在 `pom.xml` 或 `build.sbt` 中将 Sedona 添加为依赖。

1. 关于如何添加 Sedona 依赖，请参阅 [Sedona Maven Central 坐标](../maven-coordinates.md)
2. 阅读 [Sedona Flink 指南](../../tutorial/flink/sql.md)，并使用 Sedona 模板项目快速上手：[Sedona 模板项目](../../tutorial/demo.md)
3. 使用 Maven 编译您的项目，并确保得到打包了所有依赖的 fat jar。
4. 将编译生成的 fat jar 提交到 Flink 集群。请确保位于 Flink 发行版的根目录，然后运行以下命令：

```
./bin/flink run /Path/To/YourJar.jar
```
