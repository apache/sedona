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

To install Apache Sedona Python, you need to install the following steps:

Install the required Python packages.

```
pip install apache-sedona[flink] shapely attr
```

Download the required JAR files from Maven Central:

* sedona-flink-shaded_2.12:jar:1.7.1
* geotools-wrapper-{{ sedona.current_geotools }}.jar

Follow the official Flink documentation to install the JAR files in your Flink cluster or PyFlink application.
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/
