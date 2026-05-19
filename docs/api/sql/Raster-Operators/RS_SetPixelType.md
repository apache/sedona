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

# RS_SetPixelType

Introduction: Returns a modified raster with the desired pixel data type.

The `dataType` parameter accepts one of the following strings.

- "D" - 64 bits Double
- "F" - 32 bits Float
- "I" - 32 bits signed Integer
- "S" - 16 bits signed Short
- "US" - 16 bits unsigned Short
- "B" - 8 bits unsigned Byte

!!!note
    If the specified `dataType` is narrower than the original data type, the function will truncate the pixel values to fit the new data type range.

Format:

```
RS_SetPixelType(raster: Raster, dataType: String)
```

Return type: `Raster`

Since: `v1.6.0`

SQL Example:

```sql
RS_SetPixelType(raster, "I")
```
