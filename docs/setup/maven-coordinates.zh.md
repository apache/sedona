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

# Maven 坐标

## 使用 Sedona shaded（fat）jar

!!!warning
	对于 Scala/Java/Python 用户，这是在您的环境中使用 Sedona 最常见的方式。除非确认确实不需要 shaded jar，否则不要使用单独的 Sedona jar。

!!!warning
	对于 R 用户，这是在您的环境中使用 Sedona 的唯一方式。

Apache Sedona 针对每个受支持的 Spark 版本提供了不同的包。

请使用 artifact 名称中包含 Spark major.minor 版本号的 artifact。例如，对于 Spark 3.4，应使用 `sedona-spark-shaded-3.4_2.12`。

如果使用基于 Scala 2.13 编译的 Spark，则应使用对应的 Scala 2.13 版本，其后缀为 `_2.13`。

可选的 GeoTools 库仅在使用栅格算子时才需要。矢量 CRS 转换（ST_Transform）现在默认使用内置的 proj4sedona，无需 GeoTools。该 wrapper 库是 GeoTools 官方 jar 的再分发，目的仅是把 GeoTools jar 从 OSGEO 仓库带入 Maven Central。该库基于 GNU Lesser General Public License (LGPL) 协议，因此无法打包进 Sedona 官方发行版中。

!!! abstract "Sedona 与 Apache Spark + Scala 2.12"

	=== "Spark 3.4 与 Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.4_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 与 Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.5_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

	=== "Spark 4.0 与 Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-4.0_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona 与 Apache Spark + Scala 2.13"

	=== "Spark 3.4 与 Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.4_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 与 Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.5_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

	=== "Spark 4.0 与 Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-4.0_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

	=== "Spark 4.1 与 Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-4.1_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona 与 Apache Flink"

	=== "Flink 1.12+ 与 Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-flink-shaded_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona 与 Snowflake"

	=== "Snowflake 7.0+（2023 年及以后）"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-snowflake</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- 可选：https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

## 使用 Sedona unshaded jar

!!!warning
	Scala、Java、Python 用户仅在同时满足以下条件时才考虑使用下面的 jar：(1) 您知道如何在复杂应用中排除传递依赖；(2) 您的环境可以访问公网；(3) 您正在使用某种 Maven 包解析器，或 pom.xml/build.sbt，它通常以 `GroupID:ArtifactID:Version` 的形式接收输入。如果您不清楚以上含义，那么下面的 jar 不适合您。

Apache Sedona 针对每个受支持的 Spark 版本提供了不同的包。

请使用 artifact 名称中包含 Spark major.minor 版本号的 artifact。例如，对于 Spark 3.4，应使用 `sedona-spark-3.4_2.12`。

如果使用基于 Scala 2.13 编译的 Spark，则应使用对应的 Scala 2.13 版本（后缀为 `_2.13`）。

可选的 GeoTools 库仅在使用栅格算子时才需要。矢量 CRS 转换（ST_Transform）现在默认使用内置的 proj4sedona，无需 GeoTools。该 wrapper 库是 GeoTools 官方 jar 的再分发，目的仅是把 GeoTools jar 从 OSGEO 仓库带入 Maven Central。该库基于 GNU Lesser General Public License (LGPL) 协议，因此无法打包进 Sedona 官方发行版中。

!!! abstract "Sedona 与 Apache Spark + Scala 2.12"

	=== "Spark 3.4 与 Scala 2.12"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.4_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 与 Scala 2.12"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.5_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 4.0 与 Scala 2.12"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-4.0_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona 与 Apache Spark + Scala 2.13"

	=== "Spark 3.4 与 Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.4_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 与 Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.5_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 4.0 与 Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-4.0_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 4.1 与 Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-4.1_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona 与 Apache Flink"

	=== "Flink 1.12+ 与 Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-flink_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

Sedona Snowflake 没有 unshaded 版本。

### netCDF-Java 5.4.2

仅当您希望使用 `RS_FromNetCDF` 读取 HDF/NetCDF 文件时才需要此依赖。注意该 JAR 并未发布到 Maven Central，需要在 pom.xml 或 build.sbt 中加入对应仓库，或在 Spark 配置 `spark.jars.repositories` / `spark-submit --repositories` 中指定其 URL。

许可协议：BSD 3-clause（与 Apache 2.0 兼容）。

!!! abstract "添加 HDF/NetCDF 依赖"

	=== "Sedona 1.3.1+"

		在 pom.xml 中加入 unidata 仓库：

		```
		<repositories>
		    <repository>
		        <id>unidata-all</id>
		        <name>Unidata All</name>
		        <url>https://artifacts.unidata.ucar.edu/repository/unidata-all/</url>
		    </repository>
		</repositories>
		```

		然后将 cdm-core 添加到 POM 依赖中：

		```xml
		<dependency>
		    <groupId>edu.ucar</groupId>
		    <artifactId>cdm-core</artifactId>
		    <version>5.4.2</version>
		</dependency>
		```

	=== "Sedona 1.3.1 之前"

		```xml
		<!-- https://mvnrepository.com/artifact/org.datasyslab/sernetcdf -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>sernetcdf</artifactId>
		    <version>0.1.0</version>
		</dependency>
		```

## SNAPSHOT 版本

有时 Sedona 会为即将发布的版本提供 SNAPSHOT 版本。其命名规则与正式版相同，仅在版本号中以 “SNAPSHOT” 作为后缀，例如 `{{ sedona_create_release.current_snapshot }}`。

要下载 SNAPSHOT，需要在 pom.xml 或 build.sbt 中添加以下仓库：

### build.sbt

resolvers +=
  "Apache Software Foundation Snapshots" at "https://repository.apache.org/content/groups/snapshots"

### pom.xml

```xml
<repositories>
    <repository>
        <id>snapshots-repo</id>
        <url>https://repository.apache.org/content/groups/snapshots</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>
```
