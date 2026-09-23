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

import os

import pytest
import shapely
from pyspark.sql import SparkSession, functions as f, types as spark_types
from shapely.geometry import Point

from sedona.spark.sql import types
from sedona.spark.sql.st_constructors import ST_GeogFromWKB, ST_GeomFromWKB
from sedona.spark.sql.st_functions import ST_AsBinary, ST_SetSRID, ST_SRID

pytestmark = pytest.mark.skipif(
    tuple(int(n) for n in __import__("pyspark").__version__.split(".")[:2]) < (4, 2),
    reason="Native spatial values require Spark 4.2",
)


def test_export_native_spatial_types():
    assert types.GeometryType is spark_types.GeometryType
    assert types.GeographyType is spark_types.GeographyType
    assert types.geometry_type().srid == -1
    assert types.geography_type().srid == -1
    assert not hasattr(Point, "__UDT__")


@pytest.mark.parametrize(
    "wkt",
    [
        "POINT (1 2)",
        "POINT Z EMPTY",
        "POINT ZM (1 2 3 4)",
        "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING EMPTY)",
    ],
)
def test_native_shapely_roundtrip(wkt):
    original = shapely.set_srid(shapely.from_wkt(wkt), 4326)
    for convert in (types.to_spark_geometry, types.to_spark_geography):
        value = convert(original)
        assert value.getSrid() == 4326
        actual = types.to_shapely(value)
        assert shapely.to_wkb(actual, include_srid=True) == shapely.to_wkb(
            original, include_srid=True
        )
    assert types.to_shapely(None) is None
    assert types.to_spark_geometry(None) is None
    assert types.to_spark_geography(None) is None


@pytest.fixture(scope="session")
def spark():
    builder = SparkSession.builder.appName("SedonaNativePythonTypes")
    if os.environ.get("SEDONA_NATIVE_TEST_CONNECT") == "1":
        builder = builder.remote("local[1]")
    else:
        builder = builder.master("local[1]")
    jars = os.environ.get("SEDONA_PYTHON_EXTRA_JARS")
    if jars:
        builder = builder.config("spark.jars", jars)
        if os.environ.get("SEDONA_NATIVE_TEST_CONNECT") == "1":
            builder = builder.config(
                "spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions"
            )
    session = (
        builder.config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_native_create_collect_nested(spark):
    values = [
        types.to_spark_geometry(shapely.set_srid(Point(1, 2), srid))
        for srid in (0, 4326)
    ]
    schema = spark_types.StructType(
        [
            spark_types.StructField("geom", types.geometry_type()),
            spark_types.StructField(
                "nested", spark_types.ArrayType(types.geometry_type())
            ),
        ]
    )
    rows = spark.createDataFrame(
        [(values[0], [None, values[1]]), (None, [])], schema
    ).collect()
    assert rows[0].geom.getSrid() == 0
    assert rows[0].nested[0] is None
    assert rows[0].nested[1].getSrid() == 4326
    assert rows[1].geom is None
    assert rows[1].nested == []


def test_native_function_wrappers_without_sedona_registration(spark):
    wkb = Point(1, 2).wkb
    frame = spark.range(1).select(
        ST_SetSRID(ST_GeomFromWKB(f.lit(wkb)), 4326).alias("geom"),
        ST_GeogFromWKB(f.lit(wkb)).alias("geog"),
        ST_GeogFromWKB(f.lit(wkb), 4269).alias("nad83"),
    )
    row = frame.select(
        ST_SRID("geom").alias("geom_srid"),
        ST_SRID("geog").alias("geog_srid"),
        ST_SRID("nad83").alias("nad83_srid"),
        ST_AsBinary("geom").alias("wkb"),
    ).first()
    assert (row.geom_srid, row.geog_srid, row.nad83_srid) == (4326, 4326, 4269)
    assert bytes(row.wkb) == wkb


def test_native_arrow_and_connect_schema():
    from pyspark.sql.pandas.types import to_arrow_type, from_arrow_type
    from pyspark.sql.connect.types import (
        pyspark_types_to_proto_types,
        proto_schema_to_pyspark_data_type,
    )

    for datatype in (types.geometry_type(), types.geography_type()):
        assert from_arrow_type(to_arrow_type(datatype)) == datatype
        assert (
            proto_schema_to_pyspark_data_type(pyspark_types_to_proto_types(datatype))
            == datatype
        )


def test_create_spatial_dataframe_native_arrow(spark):
    import geopandas as gpd
    from sedona.spark.geoarrow import create_spatial_dataframe

    local = gpd.GeoDataFrame(
        {"id": [1, 2], "geom": [shapely.set_srid(Point(1, 2), 4326), None]},
        geometry="geom",
    )
    frame = create_spatial_dataframe(spark, local)
    assert isinstance(frame.schema["geom"].dataType, spark_types.GeometryType)
    rows = frame.orderBy("id").collect()
    assert rows[0].geom.srid == 4326
    assert types.to_shapely(rows[0].geom).equals(Point(1, 2))
    assert rows[1].geom is None


def test_local_geoseries_native_roundtrip(spark):
    import pandas as pd
    from sedona.spark.geopandas import GeoSeries

    local = pd.Series(
        [None, shapely.set_srid(Point(1, 2), 4326)], index=["a", "a"], name="geom"
    )
    series = GeoSeries(local)
    result = series.to_geopandas()
    assert list(result.index) == ["a", "a"]
    assert result.name == "geom"
    assert result.iloc[0] is None
    assert shapely.get_srid(result.iloc[1]) == 4326
    assert result.iloc[1].equals(Point(1, 2))


@pytest.mark.parametrize("wkt", ["POINT Z EMPTY", "POINT ZM (1 2 3 4)"])
def test_local_geoseries_native_dimensions(spark, wkt):
    import pandas as pd
    from sedona.spark.geopandas import GeoSeries

    geometry = shapely.set_srid(shapely.from_wkt(wkt), 4326)
    index = pd.MultiIndex.from_tuples([("a", 1), ("a", 1)], names=["letter", "number"])
    result = GeoSeries(
        pd.Series([None, geometry], index=index, name="shape")
    ).to_geopandas()
    assert result.index.equals(index)
    assert result.iloc[0] is None
    assert shapely.to_wkb(result.iloc[1], include_srid=True) == shapely.to_wkb(
        geometry, include_srid=True
    )


def test_local_geodataframe_native_metadata(spark):
    import geopandas as gpd
    from sedona.spark.geopandas import GeoDataFrame

    local = gpd.GeoDataFrame(
        {"label": ["a", "b"], "geom": [None, Point(1, 2)]},
        geometry="geom",
        crs=4326,
        index=[2, 2],
    )
    distributed = GeoDataFrame(local)
    result = distributed.to_geopandas()
    assert result.index.equals(local.index)
    assert result.crs == local.crs
    assert result.geometry.name == "geom"
    assert list(result.label) == ["a", "b"]
    assert result.geometry.iloc[0] is None
    assert result.geometry.iloc[1].equals(Point(1, 2))
    assert shapely.get_srid(result.geometry.iloc[1]) == 4326


def test_local_geoseries_rejects_non_geometry(spark):
    from sedona.spark.geopandas import GeoSeries

    with pytest.raises(TypeError, match="non-geometry"):
        GeoSeries([Point(1, 2), 4])


@pytest.mark.parametrize("batch", [False, True])
def test_native_sedona_vectorized_udf(spark, batch):
    import geopandas as gpd
    from shapely.geometry.base import BaseGeometry
    from sedona.spark.sql.functions import sedona_vectorized_udf, SedonaUDFType

    if batch:

        def identity(values: gpd.GeoSeries) -> gpd.GeoSeries:
            return values

        kind = SedonaUDFType.GEO_SERIES
    else:

        def identity(value: BaseGeometry) -> BaseGeometry:
            return value

        kind = SedonaUDFType.SHAPELY_SCALAR
    udf = sedona_vectorized_udf(types.geometry_type(), kind)(identity)
    assert udf.evalType != 5200
    geometry = shapely.set_srid(shapely.from_wkt("POINT ZM (1 2 3 4)"), 4326)
    frame = spark.createDataFrame(
        [(types.to_spark_geometry(geometry),), (None,)],
        spark_types.StructType(
            [spark_types.StructField("geom", types.geometry_type())]
        ),
    )
    values = frame.select(udf("geom").alias("geom")).collect()
    assert shapely.to_wkb(
        types.to_shapely(values[0].geom), include_srid=True
    ) == shapely.to_wkb(geometry, include_srid=True)
    assert values[1].geom is None
    null_values = (
        frame.where(f.col("geom").isNull()).select(udf("geom").alias("geom")).collect()
    )
    assert len(null_values) == 1 and null_values[0].geom is None


def test_legacy_udt_schema_json_on_native_runtime():
    from sedona.spark.core.geom.geography import Geography

    for name, value in (
        ("LegacyGeometryType", Point(1, 2)),
        ("LegacyGeographyType", Geography(Point(1, 2))),
    ):
        datatype = getattr(types, name)()
        schema = spark_types.StructType([spark_types.StructField("legacy", datatype)])
        actual = spark_types.StructType.fromJson(schema.jsonValue())
        assert type(actual["legacy"].dataType) is type(datatype)
        assert (
            actual["legacy"].dataType.jsonValue()["pyClass"]
            == "sedona.spark.sql.types." + name
        )
        assert types.to_shapely(datatype.deserialize(datatype.serialize(value))).equals(
            Point(1, 2)
        )


def test_legacy_geography_jvm_wkb_format():
    import struct
    from sedona.spark.core.geom.geography import Geography

    geometry = shapely.set_srid(Point(1, 2), 4326)
    encoded = struct.pack(">i", 4326) + shapely.to_wkb(geometry, flavor="iso")
    datatype = types.LegacyGeographyType()
    actual = datatype.deserialize(encoded)
    assert actual.geometry.equals(geometry)
    assert shapely.get_srid(actual.geometry) == 4326
    assert datatype.serialize(Geography(geometry)) == encoded


@pytest.fixture(scope="session")
def sedona_spark(spark):
    if not os.environ.get("SEDONA_PYTHON_EXTRA_JARS"):
        pytest.skip("Set SEDONA_PYTHON_EXTRA_JARS to the Spark 4.2 Sedona jar")
    from sedona.spark import SedonaContext

    return SedonaContext.create(spark)


def test_native_geography_wkt_wrapper_defaults(sedona_spark):
    from sedona.spark.sql.st_constructors import (
        ST_GeogFromWKT,
        ST_GeogFromText,
        ST_GeogCollFromText,
    )

    for constructor, wkt in (
        (ST_GeogFromWKT, "POINT (1 2)"),
        (ST_GeogFromText, "POINT (1 2)"),
        (ST_GeogCollFromText, "GEOMETRYCOLLECTION (POINT (1 2))"),
    ):
        frame = sedona_spark.range(1).select(constructor(f.lit(wkt)).alias("geog"))
        assert frame.select(ST_SRID("geog")).first()[0] == 4326


def test_native_geoarrow_export_crs_and_nulls(sedona_spark):
    import geopandas as gpd
    from sedona.spark.geoarrow import create_spatial_dataframe, dataframe_to_arrow

    local = gpd.GeoDataFrame(
        {"geom": [shapely.set_srid(Point(1, 2), 4326), None]}, geometry="geom", crs=4326
    )
    frame = create_spatial_dataframe(sedona_spark, local)
    result = gpd.GeoDataFrame.from_arrow(dataframe_to_arrow(frame))
    assert result.geometry.iloc[0].equals(Point(1, 2))
    assert result.geometry.iloc[1] is None
    assert result.crs.to_epsg() == 4326


def test_native_geopandas_geometry_and_scalar_methods(sedona_spark):
    from sedona.spark.geopandas import GeoSeries

    series = GeoSeries([Point(0, 0), None], crs=4326)
    buffered = series.buffer(1).to_geopandas()
    assert 3 < buffered.iloc[0].area < 3.2
    assert buffered.iloc[1] is None
    centroid = series.centroid.to_geopandas()
    assert centroid.iloc[0].equals(Point(0, 0))
    assert centroid.iloc[1] is None
    boundary = series.boundary.to_geopandas()
    assert boundary.iloc[0].is_empty
    assert boundary.iloc[1] is None
    assert series.area.to_pandas().iloc[0] == 0
    assert series.is_empty.to_pandas().tolist() == [False, False]


def test_native_maps_require_geoarrow_capable_geopandas(spark, monkeypatch):
    import geopandas as gpd
    from sedona.spark.maps.SedonaMapUtils import SedonaMapUtils

    frame = spark.createDataFrame(
        [(types.to_spark_geometry(Point(1, 2)),)],
        spark_types.StructType(
            [spark_types.StructField("geom", types.geometry_type())]
        ),
    )
    monkeypatch.setattr(gpd, "__version__", "0.14.4")
    with pytest.raises(ImportError, match="GeoPandas 1.0 or later"):
        SedonaMapUtils.__convert_to_gdf_or_pdf__(frame)
