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

import pytest
from pyspark.sql import types as spark_types

import sedona.spark as sedona
import sedona.spark.sql as sedona_sql
from sedona.spark.sql import types


def _type_instance(data_type):
    return (
        data_type()
        if issubclass(data_type, spark_types.UserDefinedType)
        else data_type(4326)
    )


@pytest.mark.parametrize("kind", ["geometry", "geography"])
def test_recognizes_sedona_spatial_schema_types(kind):
    predicate = getattr(types, "is_" + kind + "_type")
    name = kind.capitalize() + "Type"
    assert predicate(_type_instance(getattr(types, name))) is True
    assert predicate(getattr(types, "Legacy" + name)()) is True
    other = "GeographyType" if kind == "geometry" else "GeometryType"
    assert predicate(_type_instance(getattr(types, other))) is False
    assert predicate(getattr(types, "Legacy" + other)()) is False


@pytest.mark.parametrize("kind", ["geometry", "geography"])
def test_recognizes_available_spark_native_types(kind):
    name = kind.capitalize() + "Type"
    native_type = getattr(spark_types, name, None)
    if native_type is None:
        pytest.skip("This PySpark version has no native spatial schema types")
    predicate = getattr(types, "is_" + kind + "_type")
    assert predicate(native_type(4326)) is True
    assert predicate(native_type("ANY")) is True
    other = "GeographyType" if kind == "geometry" else "GeometryType"
    assert predicate(getattr(spark_types, other)(4326)) is False


@pytest.mark.parametrize("kind", ["geometry", "geography"])
@pytest.mark.parametrize(
    "value",
    [
        None,
        "geometry",
        object(),
        types.GeometryType,
        spark_types.StringType(),
        spark_types.BinaryType(),
        types.RasterType(),
        types.Box2DType(),
        types.Box3DType(),
        spark_types.ArrayType(_type_instance(types.GeometryType)),
        spark_types.StructType(
            [spark_types.StructField("geom", _type_instance(types.GeometryType))]
        ),
    ],
)
def test_rejects_non_spatial_schema_types(kind, value):
    assert getattr(types, "is_" + kind + "_type")(value) is False


@pytest.mark.parametrize("name", ["is_geometry_type", "is_geography_type"])
@pytest.mark.parametrize("module", [sedona, sedona_sql])
def test_predicates_are_exported_from_public_packages(name, module):
    assert getattr(module, name) is getattr(types, name)
