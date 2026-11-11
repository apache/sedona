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

# Python API reference

The [Python API reference](pydocs/index.html) documents Sedona's Python modules, classes, and functions, including their parameters and return types.

For functions used with PySpark DataFrames, see the [SQL function reference](pydocs/sedona.spark.sql.html). For example, [ST_Length](pydocs/sedona.spark.sql.html#sedona.spark.sql.st_functions.ST_Length) calculates the length of a linestring geometry:

```python
from sedona.spark.sql.st_functions import ST_Length

df.select(ST_Length("geometry").alias("length"))
```

This example assumes a DataFrame named `df` with a geometry column named `geometry` and a Spark session configured with Sedona. See the [Python installation guide](../setup/install-python.md) for setup instructions and the [DataFrame API guide](sql/DataFrameAPI.md) for argument conventions and more examples.

## Recognizing spatial column types

Since v2.0.0, `is_geometry_type` and `is_geography_type` recognize both Sedona UDTs and Spark native spatial types when those classes are available in PySpark. They inspect schema type instances without requiring a Spark session or changing the column's representation.

Both helpers are available from `sedona.spark`, `sedona.spark.sql` and `sedona.spark.sql.types`.

```python
from sedona.spark import is_geography_type, is_geometry_type

geometry_columns = [
    field.name for field in df.schema.fields if is_geometry_type(field.dataType)
]
geography_columns = [
    field.name for field in df.schema.fields if is_geography_type(field.dataType)
]
```

The predicates distinguish geometry from geography. Arrays and structs containing spatial values are not themselves spatial column types. `LegacyGeometryType` and `LegacyGeographyType` in `sedona.spark.sql.types` explicitly name Sedona's UDTs; the predicates recognize these too. These helpers do not enable native spatial support or change the existing `GeometryType` and `GeographyType` defaults.
