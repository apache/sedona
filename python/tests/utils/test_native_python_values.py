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

"""Pure Python native-boundary checks: no Spark session or JVM gateway."""

import pytest
import pyspark
import shapely
from pyspark import SparkContext
from pyspark.sql import types as spark_types
from shapely.geometry import Point

from sedona.spark.core.geom.geography import Geography as SedonaGeography
from sedona.spark.sql import types


@pytest.fixture(autouse=True)
def no_gateway():
    # Broad older-runtime suites may already own a gateway. These pure checks
    # must not create or replace it; dedicated pure CI runs begin with None.
    gateway = SparkContext._gateway
    yield
    assert SparkContext._gateway is gateway


NATIVE = tuple(int(n) for n in pyspark.__version__.split(".")[:2]) >= (4, 2)
native_only = pytest.mark.skipif(not NATIVE, reason="requires native Spark 4.2 API")


def test_type_ownership_and_legacy_registration():
    assert types.USES_NATIVE_SPATIAL_TYPES == NATIVE
    if NATIVE:
        assert types.GeometryType is spark_types.GeometryType
        assert types.GeographyType is spark_types.GeographyType
        assert types.geometry_type().srid == -1
        assert types.geography_type().srid == -1
        assert not hasattr(Point, "__UDT__")
        assert not hasattr(SedonaGeography, "__UDT__")
    else:
        assert isinstance(types.geometry_type(), spark_types.UserDefinedType)
        assert isinstance(types.geography_type(), spark_types.UserDefinedType)
        assert Point.__UDT__ == types.GeometryType()
        assert SedonaGeography.__UDT__ == types.GeographyType()


def test_legacy_explicit_geometry_udt_uses_sql_ewkb_transport():
    import struct

    # Pin the SQL payload independently of either implementation.
    ewkb = struct.pack("<BIi2d", 1, 0x20000001, 3857, 1.0, 2.0)
    result = types.LegacyGeometryType().deserialize(ewkb)
    assert result.wkt == "POINT (1 2)"
    assert types.LegacyGeometryType().serialize(result) == ewkb
    explicit_type = types.LegacyGeometryType()
    assert (
        spark_types.UserDefinedType.fromJson(explicit_type.jsonValue()) == explicit_type
    )


def test_rdd_geometry_transport_retains_independent_compact_bytes():
    import struct
    from sedona.spark.utils import geometry_serde

    compact = struct.pack("=BBBBidd", 0x12, 0, 0, 0, 1, 1.0, 2.0)
    result, _ = geometry_serde.deserialize(compact)
    assert result.wkt == "POINT (1 2)"
    assert bytes(geometry_serde.serialize(result)) == compact


def test_legacy_explicit_geography_udt_keeps_published_prefix_wkb_fix():
    # Big-endian SRID4326 prefix followed by little-endian XY WKB.
    payload = bytes.fromhex("000010e60101000000000000000000f03f0000000000000040")
    result = types.LegacyGeographyType().deserialize(payload)
    assert isinstance(result, SedonaGeography)
    assert result.geometry.wkt == "POINT (1 2)"
    assert types.LegacyGeographyType().serialize(result) == payload
    assert types.LegacyGeographyType().serialize(None) is None
    assert types.LegacyGeographyType().deserialize(None) is None
    explicit_type = types.LegacyGeographyType()
    assert (
        spark_types.UserDefinedType.fromJson(explicit_type.jsonValue()) == explicit_type
    )


def test_legacy_type_aliases_match_registered_udts_on_older_spark():
    if NATIVE:
        assert types.LegacyGeometryType.__name__ == "LegacyGeometryType"
        assert types.LegacyGeographyType.__name__ == "LegacyGeographyType"
        assert (
            types.LegacyGeometryType()
            .jsonValue()["pyClass"]
            .endswith(".LegacyGeometryType")
        )
        assert (
            types.LegacyGeographyType()
            .jsonValue()["pyClass"]
            .endswith(".LegacyGeographyType")
        )
    else:
        assert types.LegacyGeometryType is types.GeometryType
        assert types.LegacyGeographyType is types.GeographyType
        spark_types._make_type_verifier(types.LegacyGeometryType())(Point(1, 2))
        spark_types._make_type_verifier(types.LegacyGeographyType())(
            SedonaGeography(Point(1, 2))
        )


def test_null_and_legacy_value_helpers():
    assert types.to_spark_geometry(None) is None
    assert types.to_spark_geography(None) is None
    assert types.to_shapely(None) is None
    value = Point(1, 2)
    wrapper = SedonaGeography(value)
    assert types.to_shapely(value) is value
    assert types.to_shapely(wrapper) is value
    if not NATIVE:
        assert types.to_spark_geometry(value) is value
        assert types.to_spark_geography(wrapper) is wrapper
        assert types.to_spark_geography(value).geometry is value


@native_only
@pytest.mark.parametrize(
    "wkt",
    [
        "POINT (1 2)",
        "POINT Z EMPTY",
        "POINT M (1 2 9)",
        "POINT ZM (1 2 3 4)",
        "LINESTRING ZM (1 2 3 4, 5 6 7 8)",
        "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING EMPTY)",
    ],
)
def test_native_roundtrip_preserves_coordinates_layout_and_srid(wkt):
    original = shapely.set_srid(shapely.from_wkt(wkt), 4326)
    expected = shapely.to_wkb(original, include_srid=True)
    for convert in (types.to_spark_geometry, types.to_spark_geography):
        native = convert(original)
        assert native.getSrid() == 4326
        assert shapely.to_wkb(types.to_shapely(native), include_srid=True) == expected
        assert convert(native) is native


@native_only
def test_spark_default_transport_has_separate_wkb_and_srid():
    original = shapely.set_srid(Point(1, 2), 4326)
    native = types.to_spark_geometry(original)
    # ISO WKB has no custom prefix or EWKB SRID flag.
    expected_wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
    assert native.getBytes() == expected_wkb
    assert types.geometry_type().toInternal(native) == {
        "wkb": expected_wkb,
        "srid": 4326,
    }
    assert types.to_shapely(
        types.geometry_type().fromInternal({"bytes": expected_wkb, "srid": 4326})
    ).equals(original)


@native_only
@pytest.mark.parametrize("srid", [0, 4326, 4269])
def test_geography_normalizes_only_unset_srid(srid):
    original = shapely.set_srid(Point(1, 2), srid)
    result = types.to_spark_geography(SedonaGeography(original))
    assert result.getSrid() == (4326 if srid == 0 else srid)
    assert shapely.get_srid(original) == srid


@native_only
def test_geography_preserves_projected_crs_for_spark_to_reject():
    # Spark's public Python value object does not validate WKB or CRS. Keep an
    # explicit CRS intact, so the JVM default serializer can reject it instead
    # of silently turning it into WGS84. End-to-end validation is a JVM test.
    value = types.to_spark_geography(shapely.set_srid(Point(1, 2), 3857))
    assert value.getSrid() == 3857


@native_only
def test_default_arrow_and_connect_schema_roundtrips():
    from pyspark.sql.pandas.types import from_arrow_type, to_arrow_type
    from pyspark.sql.connect.types import (
        pyspark_types_to_proto_types,
        proto_schema_to_pyspark_data_type,
    )

    for dtype in [
        types.geometry_type(),
        types.geography_type(),
        types.GeometryType(4326),
        types.GeographyType(4326),
    ]:
        assert from_arrow_type(to_arrow_type(dtype)) == dtype
        assert (
            proto_schema_to_pyspark_data_type(pyspark_types_to_proto_types(dtype))
            == dtype
        )


@native_only
def test_five_builtin_wrappers_build_connect_expressions_without_jvm(monkeypatch):
    from sedona.spark.sql.st_functions import ST_AsBinary, ST_SRID, ST_SetSRID
    from sedona.spark.sql.st_constructors import ST_GeomFromWKB, ST_GeogFromWKB

    monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "1")
    expressions = [
        (ST_AsBinary("g"), "st_asbinary"),
        (ST_SRID("g"), "st_srid"),
        (ST_SetSRID("g", 4269), "st_setsrid"),
        (ST_GeomFromWKB("wkb"), "st_geomfromwkb"),
        (ST_GeogFromWKB("wkb"), "st_geogfromwkb"),
    ]
    for column, name in expressions:
        assert column._expr._name == name
    explicit = ST_GeogFromWKB("wkb", 4269)._expr
    assert explicit._name == "st_setsrid"
    assert explicit._args[0]._name == "st_geogfromwkb"
    assert explicit._args[1]._value == 4269


@native_only
@pytest.mark.parametrize("style", ["SHAPELY_SCALAR", "GEO_SERIES"])
@pytest.mark.parametrize("values", [[Point(1, 2), None, Point()], [None], []])
def test_native_vectorized_callbacks_preserve_nulls_and_typed_empty(style, values):
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry.base import BaseGeometry
    from sedona.spark.sql.functions import SedonaUDFType, sedona_vectorized_udf

    def scalar(value: BaseGeometry) -> BaseGeometry:
        return value

    def vector(values: gpd.GeoSeries) -> gpd.GeoSeries:
        return values

    udf_type = getattr(SedonaUDFType, style)
    udf = sedona_vectorized_udf(types.geometry_type(), udf_type)(
        scalar if style == "SHAPELY_SCALAR" else vector
    )
    incoming = pd.Series([types.to_spark_geometry(x) for x in values], dtype=object)
    actual = udf.func(incoming)
    assert actual.dtype == object
    assert len(actual) == len(values)
    for expected, value in zip(values, actual):
        if expected is None:
            assert value is None
        else:
            assert isinstance(value, spark_types.Geometry)
            assert shapely.to_wkb(types.to_shapely(value)) == shapely.to_wkb(expected)


@native_only
@pytest.mark.parametrize("geography", [False, True])
def test_default_arrow_value_transport_with_nulls(geography):
    import pandas as pd
    import pyarrow as pa
    from pyspark.sql.pandas.types import (
        _create_converter_from_pandas,
        _create_converter_to_pandas,
        to_arrow_type,
    )

    convert = types.to_spark_geography if geography else types.to_spark_geometry
    dtype = types.geography_type() if geography else types.geometry_type()
    values = pd.Series([convert(Point(1, 2)), None, convert(Point())], dtype=object)
    storage = _create_converter_from_pandas(dtype)(values)
    arrow = pa.array(storage, type=to_arrow_type(dtype))
    result = _create_converter_to_pandas(dtype, timezone="UTC")(arrow.to_pandas())
    assert result.tolist() == values.tolist()


@native_only
def test_geoarrow_input_uses_default_spark_values_and_preserves_local_input():
    import geopandas as gpd
    import pandas as pd
    from sedona.spark.geoarrow import create_spatial_dataframe

    original = shapely.set_srid(Point(1, 2), 4326)
    local = gpd.GeoDataFrame(
        {"id": [1, 2], "geom": [original, None]},
        geometry="geom",
        index=pd.Index(["same", "same"]),
    )

    class CaptureSparkBoundary:
        def createDataFrame(self, values, schema):
            return values, schema

    values, schema = create_spatial_dataframe(CaptureSparkBoundary(), local)
    assert isinstance(schema["geom"].dataType, spark_types.GeometryType)
    assert values.index.equals(local.index)
    assert values.geom.iloc[0].getSrid() == 4326
    assert values.geom.iloc[1] is None
    assert local.geom.iloc[0] is original
    assert str(local.geom.dtype) == "geometry"


@native_only
@pytest.mark.parametrize("values", [[Point(1, 2), None], [None], []])
def test_geoarrow_keeps_metadata_only_crs_in_values_and_all_null_schema(values):
    import geopandas as gpd
    import pandas as pd
    from sedona.spark.geoarrow import create_spatial_dataframe

    local = gpd.GeoDataFrame(
        {"geom": gpd.GeoSeries(values, crs="EPSG:3857")}, geometry="geom"
    )

    class CaptureSparkBoundary:
        def createDataFrame(self, values, schema):
            return values, schema

    output, schema = create_spatial_dataframe(CaptureSparkBoundary(), local)
    assert schema["geom"].dataType.srid == 3857
    assert output.index.equals(local.index)
    assert len(output) == len(values)
    for original, native in zip(values, output.geom):
        if original is None:
            assert native is None
        else:
            assert native.getSrid() == 3857
            assert shapely.get_srid(original) == 0
    assert local.crs.to_epsg() == 3857


@native_only
def test_geoarrow_rejects_conflicting_embedded_and_metadata_crs():
    import geopandas as gpd
    from sedona.spark.geoarrow import create_spatial_dataframe

    original = shapely.set_srid(Point(1, 2), 4326)
    local = gpd.GeoDataFrame({"geom": [original]}, geometry="geom", crs="EPSG:3857")
    with pytest.raises(ValueError, match="conflicts"):
        create_spatial_dataframe(None, local)
    assert shapely.get_srid(original) == 4326


@native_only
def test_geoarrow_rejects_metadata_crs_without_integer_srid():
    import geopandas as gpd
    from sedona.spark.geoarrow import create_spatial_dataframe

    local = gpd.GeoDataFrame(
        {"geom": [None]},
        geometry="geom",
        crs="+proj=aeqd +lat_0=12.345 +lon_0=67.89 +datum=WGS84 +units=m +no_defs",
    )
    assert local.crs.to_epsg() is None
    with pytest.raises(ValueError, match="EPSG"):
        create_spatial_dataframe(None, local)
