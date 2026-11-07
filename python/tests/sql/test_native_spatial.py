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


"""Native spatial boundaries through the actual Classic or Connect runtime."""

import json
import os
import pytest
import math

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import Point
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sedona.spark.core.geom.geography import Geography as LegacyGeography
from sedona.spark.sql import types as types
from sedona.spark.sql import st_constructors as C, st_functions as S
from sedona.spark.sql.functions import sedona_vectorized_udf, SedonaUDFType
from tests.test_base import TestBase

pytestmark = pytest.mark.skipif(
    not types.USES_NATIVE_SPATIAL_TYPES,
    reason="requires Spark 4.2 native spatial types",
)


@pytest.fixture(scope="module")
def spark():
    # The same fixture configures Classic and the repository's SPARK_REMOTE=local lane.
    return TestBase.spark


@pytest.fixture(scope="module")
def mode():
    return "connect" if os.environ.get("SPARK_REMOTE") else "classic"


@pytest.fixture(autouse=True)
def restore_arrow_settings(spark):
    keys = (
        "spark.sql.execution.arrow.pyspark.enabled",
        "spark.sql.execution.arrow.pyspark.fallback.enabled",
    )
    previous = {key: spark.conf.get(key) for key in keys}
    yield
    for key, value in previous.items():
        spark.conf.set(key, value)


def assert_shape(actual, expected):
    if expected is None:
        assert actual is None
        return
    actual = types.to_shapely(actual)
    assert shapely.get_srid(actual) == shapely.get_srid(expected)
    assert shapely.to_wkb(
        actual, flavor="iso", byte_order=1, output_dimension=4
    ) == shapely.to_wkb(expected, flavor="iso", byte_order=1, output_dimension=4)


def shapes():
    return [
        Point(1, 2),
        shapely.set_srid(shapely.from_wkt("POINT Z (3 4 5)"), 4326),
        shapely.set_srid(shapely.from_wkt("POINT M (6 7 8)"), 3857),
        shapely.set_srid(shapely.from_wkt("POINT ZM (9 10 11 12)"), 4326),
        shapely.set_srid(Point(), 4326),
        None,
    ]


def native_frame(spark):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("g", types.geometry_type(), True),
        ]
    )
    return spark.createDataFrame(
        [(i, types.to_spark_geometry(value)) for i, value in enumerate(shapes())],
        schema,
    )


def test_native_values(spark, mode):
    frame = native_frame(spark)
    assert isinstance(frame.schema["g"].dataType, T.GeometryType)
    assert frame.schema["g"].dataType.srid == -1
    for row, expected in zip(frame.orderBy("id").collect(), shapes()):
        if row.g is not None:
            assert isinstance(row.g, T.Geometry)
        assert_shape(row.g, expected)
    parsed = T.StructType.fromJson(json.loads(frame.schema.json()))
    assert parsed == frame.schema


def test_fixed_crs(spark, mode):
    schema = T.StructType([T.StructField("g", T.GeometryType(4326), True)])
    good = types.to_spark_geometry(shapely.set_srid(Point(1, 2), 4326))
    assert spark.createDataFrame([(good,)], schema).collect()[0].g.getSrid() == 4326
    bad = types.to_spark_geometry(shapely.set_srid(Point(1, 2), 3857))
    try:
        spark.createDataFrame([(bad,)], schema).collect()
    except Exception as error:
        assert any(
            word in str(error).lower() for word in ["srid", "crs", "3857", "4326"]
        ), str(error)
    else:
        raise AssertionError("Fixed geometry schema accepted an incompatible CRS")


def test_native_geography(spark, mode):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("g", types.geography_type(), True),
        ]
    )
    source = [
        shapely.set_srid(Point(1, 2), 4326),
        shapely.set_srid(Point(1, 2), 4269),
        None,
    ]
    frame = spark.createDataFrame(
        [(i, types.to_spark_geography(value)) for i, value in enumerate(source)], schema
    )
    assert isinstance(frame.schema["g"].dataType, T.GeographyType)
    for row, expected in zip(frame.orderBy("id").collect(), source):
        assert_shape(row.g, expected)
    rows = frame.selectExpr("id", "ST_Distance(g, g) as d").orderBy("id").collect()
    assert rows[0].d == rows[1].d == 0.0 and rows[2].d is None
    bad = types.to_spark_geography(shapely.set_srid(Point(1, 2), 3857))
    try:
        spark.createDataFrame([(0, bad)], schema).collect()
    except Exception as error:
        assert any(
            word in str(error).lower() for word in ["srid", "crs", "3857", "geography"]
        ), str(error)
    else:
        raise AssertionError("Native geography accepted projected EPSG3857")


def test_builtin_wrappers(spark, mode):
    wkb = shapely.to_wkb(Point(1, 2), flavor="iso")
    frame = spark.createDataFrame(
        [(bytearray(wkb), 4326), (None, 4326)], "wkb binary, srid int"
    )
    for make, builtin in [
        (C.ST_GeomFromWKB, F.st_geomfromwkb),
        (C.ST_GeogFromWKB, F.st_geogfromwkb),
    ]:
        created = frame.select(
            make("wkb").alias("g"), builtin("wkb").alias("expected"), "srid"
        )
        rows = created.select(
            S.ST_AsBinary("g").alias("a"),
            F.st_asbinary("expected").alias("b"),
            S.ST_SRID("g").alias("x"),
            F.st_srid("expected").alias("y"),
            S.ST_SRID(S.ST_SetSRID("g", F.col("srid"))).alias("changed"),
        ).collect()
        assert bytes(rows[0].a) == bytes(rows[0].b) == wkb
        assert rows[0].x == rows[0].y
        assert rows[0].changed == 4326
        assert all(value is None for value in rows[1])
    for name in [
        "st_geomfromwkb",
        "st_geogfromwkb",
        "st_asbinary",
        "st_srid",
        "st_setsrid",
    ]:
        description = "\n".join(
            str(row[0])
            for row in spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}").collect()
        )
        assert "org.apache.spark.sql.catalyst.expressions." in description, (
            name,
            description,
        )
        assert "sedona_sql" not in description, (name, description)
    area = (
        spark.sql("SELECT ST_Area(ST_Buffer(ST_GeomFromWKT('POINT (0 0)'), 1.0)) AS a")
        .first()
        .a
    )
    assert math.isclose(area, math.pi, abs_tol=0.03)


def test_explicit_udts(spark, mode):
    for geography in [False, True]:
        dtype = types.LegacyGeographyType() if geography else types.LegacyGeometryType()
        wrap = LegacyGeography if geography else lambda value: value
        source = [shapely.set_srid(Point(1, 2), 4326), Point(), None]
        values = [
            (i, wrap(value) if value is not None else None)
            for i, value in enumerate(source)
        ]
        schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), False),
                T.StructField("g", dtype, True),
            ]
        )
        kwargs = {"verifySchema": False} if mode == "classic" else {}
        frame = spark.createDataFrame(values, schema, **kwargs)
        assert isinstance(frame.schema["g"].dataType, type(dtype))
        parsed = T.StructType.fromJson(json.loads(frame.schema.json()))
        assert isinstance(parsed["g"].dataType, type(dtype))
        assert (
            "LegacyGeographyType" if geography else "LegacyGeometryType"
        ) in frame.schema.json()
        for row, expected in zip(frame.orderBy("id").collect(), source):
            if geography and row.g is not None:
                assert isinstance(row.g, LegacyGeography)
            assert_shape(row.g, expected)


def test_ordinary_udfs(spark, mode):
    frame = native_frame(spark)

    def native_identity(value):
        return types.to_spark_geometry(types.to_shapely(value))

    def legacy_identity(value):
        return types.to_shapely(value)

    native = F.udf(native_identity, types.geometry_type())
    legacy = F.udf(legacy_identity, types.LegacyGeometryType())
    result = frame.select("id", native("g").alias("n"), legacy("g").alias("u"))
    assert isinstance(result.schema["n"].dataType, T.GeometryType)
    assert isinstance(result.schema["u"].dataType, types.LegacyGeometryType)
    for row, expected in zip(result.orderBy("id").collect(), shapes()):
        assert_shape(row.n, expected)
        assert_shape(row.u, expected)


def test_arrow_creation(spark, mode):
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    values = shapes()
    local = pd.DataFrame(
        {
            "id": range(len(values)),
            "g": pd.Series(
                [types.to_spark_geometry(value) for value in values], dtype=object
            ),
        }
    )
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), False),
            T.StructField("g", types.geometry_type(), True),
        ]
    )
    result = spark.createDataFrame(local, schema)
    for row, expected in zip(result.orderBy("id").collect(), values):
        assert_shape(row.g, expected)


def test_vectorized_udfs(spark, mode):
    frame = native_frame(spark)
    from shapely.geometry.base import BaseGeometry

    @sedona_vectorized_udf(return_type=types.geometry_type())
    def scalar(value: BaseGeometry) -> BaseGeometry:
        return value

    @sedona_vectorized_udf(
        return_type=types.geometry_type(), udf_type=SedonaUDFType.GEO_SERIES
    )
    def batch(values: gpd.GeoSeries) -> gpd.GeoSeries:
        return values

    @sedona_vectorized_udf(return_type=types.LegacyGeometryType())
    def legacy(value: BaseGeometry) -> BaseGeometry:
        return value

    for source in [frame, frame.where("g IS NULL"), frame.limit(0)]:
        result = source.select(
            "id", scalar("g").alias("a"), batch("g").alias("b"), legacy("g").alias("c")
        )
        assert isinstance(result.schema["a"].dataType, T.GeometryType)
        assert isinstance(result.schema["b"].dataType, T.GeometryType)
        assert isinstance(result.schema["c"].dataType, types.LegacyGeometryType)
        expected = {row.id: types.to_shapely(row.g) for row in source.collect()}
        rows = result.collect()
        assert len(rows) == len(expected)
        for row in rows:
            for value in [row.a, row.b, row.c]:
                assert_shape(value, expected[row.id])
