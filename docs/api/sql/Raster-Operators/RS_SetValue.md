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

# RS_SetValue

Introduction: Returns a raster by replacing the value of pixel specified by `colX` and `rowY`.

Format:

```
RS_SetValue(raster: Raster, bandIndex: Integer = 1, colX: Integer, rowY: Integer, newValue: Double)
```

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_BandAsArray(
               RS_SetValue(
                       RS_AddBandFromArray(
                               RS_MakeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0),
                           [1,1,1,0,0,0,1,2,3,3,5,6,7,0,0,3,0,0,3,0,0,0,0,0,0], 1, 0d
                           ),
                       1, 2, 2, 255
                   )
           )
```

Output:

```
[1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 255.0, 2.0, 3.0, 3.0, 5.0, 6.0, 7.0, 0.0, 0.0, 3.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
```
