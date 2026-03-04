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

# RS_NetCDFInfo

Introduction: Returns a string containing names of the variables in a given netCDF file along with its dimensions.

Format: `RS_NetCDFInfo(netCDF: ARRAY[Byte])`

Return type: `String`

Since: `1.5.1`

Spark Example:

```scala
val df = sedona.read.format("binaryFile").load("/some/path/test.nc")
recordInfo = df.selectExpr("RS_NetCDFInfo(content) as record_info").first().getString(0)
print(recordInfo)
```

Output:

```text
O3(time=2, z=2, lat=48, lon=80)

NO2(time=2, z=2, lat=48, lon=80)
```
