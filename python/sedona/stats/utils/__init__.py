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

    if len(geom_fields) == 0:
        raise ValueError("No GeometryType column found. Provide a dataframe containing a geometry column.")

    if len(geom_fields) == 1:
        return geom_fields[0]

    if len(geom_fields) > 1 and "geometry" not in geom_fields:
        raise ValueError("Multiple GeometryType columns found. Provide the column name as an argument.")

    return "geometry"
