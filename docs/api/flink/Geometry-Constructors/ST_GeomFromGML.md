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

# ST_GeomFromGML

Introduction: Construct a Geometry from GML.

!!!note
    This function only supports GML1 and GML2. GML3 is not supported.

Format:
`ST_GeomFromGML (gml: String)`

Return type: `Geometry`

Since: `v1.3.0`

Example:

```sql
SELECT ST_GeomFromGML('
    <gml:LineString srsName="EPSG:4269">
    	<gml:coordinates>
        	-71.16028,42.258729
        	-71.160837,42.259112
        	-71.161143,42.25932
    	</gml:coordinates>
    </gml:LineString>
')
```

Output:

```
LINESTRING (-71.16028 42.258729, -71.160837 42.259112, -71.161143 42.25932)
```
