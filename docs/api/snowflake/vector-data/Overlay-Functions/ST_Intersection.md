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

# ST_Intersection

Introduction: Return the intersection geometry of A and B

Format: `ST_Intersection (A:geometry, B:geometry)`

Return type: `Geometry`

!!!note
    If you encounter a `TopologyException` with the message "found non-noded intersection", this is a known issue with the legacy overlay implementation in JTS. The OverlayNG algorithm resolves this. To enable it, add the following JVM flag:

    ```
    -Djts.overlay=ng
    ```

SQL example:

```sql
SELECT ST_Intersection(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```
