# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Schema inference must preserve positions without starting a Spark session."""

import pytest
import geopandas as gpd
from pyspark import SparkContext
from pyspark.sql.types import LongType
from shapely.geometry import Point

from sedona.spark.geoarrow.geoarrow import infer_schema
from sedona.spark.sql.types import GeometryType


@pytest.mark.parametrize(
    "columns", [["first", "second", "id"], ["first", "id", "second"]]
)
def test_multiple_geometry_column_positions(columns):
    gateway = SparkContext._gateway
    local = gpd.GeoDataFrame(
        {
            "first": gpd.GeoSeries([Point(1, 2), None], crs="EPSG:4326"),
            "id": [1, 2],
            "second": gpd.GeoSeries([Point(3, 4), None], crs="EPSG:3857"),
        },
        geometry="first",
    )[columns]
    schema = infer_schema(local)
    assert schema.names == columns
    assert isinstance(schema["id"].dataType, LongType)
    assert isinstance(schema["first"].dataType, GeometryType)
    assert isinstance(schema["second"].dataType, GeometryType)
    assert SparkContext._gateway is gateway
