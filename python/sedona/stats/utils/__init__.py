#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from pyspark.sql import DataFrame, Column, SparkSession
from sedona.sql.types import GeometryType


def get_geometry_column_name(df: DataFrame) -> Column:
    geom_fields = [
        field.name for field in df.schema.fields if field.dataType == GeometryType()
    ]
    if len(geom_fields) > 1:
        if "geometry" in geom_fields:
            return "geometry"
        else:
            raise ValueError("Multiple geometry columns found in DataFrame")
    if len(geom_fields) == 0:
        raise ValueError("No geometry column found in DataFrame")
    return geom_fields[0]
