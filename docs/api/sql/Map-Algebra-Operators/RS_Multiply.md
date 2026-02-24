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

# RS_Multiply

Introduction: Multiply two spectral bands in a Geotiff image

Format: `RS_Multiply (Band1: ARRAY[Double], Band2: ARRAY[Double])`

Since: `v1.1.0`

SQL Example

```scala
val multiplyDF = spark.sql("select RS_Multiply(band1, band2) as multiplyBands from dataframe")
```
