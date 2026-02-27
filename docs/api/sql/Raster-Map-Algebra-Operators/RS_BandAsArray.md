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

# RS_BandAsArray

Introduction: Extract a band from a raster as an array of doubles.

Format: `RS_BandAsArray (raster: Raster, bandIndex: Integer)`.

Since: `v1.4.1`

BandIndex is 1-based and must be between 1 and RS_NumBands(raster). It returns null if the bandIndex is out of range or the raster is null.

SQL Example

```sql
SELECT RS_BandAsArray(raster, 1) FROM raster_table
```

Output:

```
+--------------------+
|                band|
+--------------------+
|[0.0, 0.0, 0.0, 0...|
+--------------------+
```
