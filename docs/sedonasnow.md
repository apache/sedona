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

# SedonaSnow

SedonaSnow brings 200+ Apache Sedona geospatial functions directly into your Snowflake environment to complement the native Snowflake spatial functions.

## Key Advantages

* **200+ spatial functions**: Such as 3D distance, geometry validation, precision reduction
* **Fast spatial joins**: Sedona has special optimizations for performant spatial joins
* **Seamless integration**: Works alongside Snowflake's native functions
* **No data movement**: Everything stays in Snowflake

## Get Started

Here’s an example of how to run some queries on Snowflake tables with SedonaSnow.

```sql
USE DATABASE SEDONASNOW;

SELECT SEDONA.ST_GeomFromWKT(wkt) AS geom
FROM your_table;

SELECT SEDONA.ST_3DDistance(geom1, geom2) FROM spatial_data;
```

Here’s an example of a spatial join:

```sql
SELECT * FROM lefts, rights
WHERE lefts.cellId = rights.cellId;
```

You can see how SedonaSnow seamlessly integrates into your current Snowflake environment.

## Next steps

SedonaSnow is an excellent option if you're doing serious spatial analysis in Snowflake.  It is fast and provides a wide range of spatial functions.  SedonaSnow removes the limitations of Snowflake's built-in spatial functions without forcing you to move your data to another platform.
