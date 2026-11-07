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


"""Native pandas-on-Spark boundaries, including metadata-only geometry columns."""

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from geopandas.array import GeometryDtype
from shapely.geometry import Point
from pyspark.sql import types as spark_types
from sedona.spark.sql import types
from tests.test_base import TestBase

pytestmark = pytest.mark.skipif(
    not types.USES_NATIVE_SPATIAL_TYPES,
    reason="requires Spark 4.2 native spatial types",
)


@pytest.fixture(params=[False, True], ids=["without-arrow", "with-arrow"])
def native_geopandas(request):
    # Configure/register Spark before this package reads pandas-on-Spark options.
    spark = TestBase.spark
    from sedona.spark import geopandas as sgpd

    keys = (
        "spark.sql.execution.arrow.pyspark.enabled",
        "spark.sql.execution.arrow.pyspark.fallback.enabled",
    )
    previous = {key: spark.conf.get(key) for key in keys}
    spark.conf.set(keys[0], str(request.param).lower())
    spark.conf.set(keys[1], "false")
    yield sgpd
    for key, value in previous.items():
        spark.conf.set(key, value)


@pytest.mark.parametrize("values", [[], [None, None]], ids=["empty", "all-null"])
@pytest.mark.parametrize("crs", [None, "EPSG:3857"])
@pytest.mark.parametrize(
    "typed", [False, True], ids=["declared-column", "geometry-dtype"]
)
def test_metadata_only_geoseries(native_geopandas, values, crs, typed):
    source = (
        gpd.GeoSeries(values, crs=crs, name="shape")
        if typed
        else pd.Series(values, dtype=object, name="shape")
    )
    actual = native_geopandas.GeoSeries(source, crs=crs)
    assert isinstance(actual.spark.data_type, spark_types.GeometryType)
    local = actual.to_geopandas()
    assert local.tolist() == values
    assert local.crs == gpd.GeoSeries([], crs=crs).crs


@pytest.mark.parametrize("values", [[], [None, None]], ids=["empty", "all-null"])
def test_each_geometry_column_keeps_its_crs(native_geopandas, values):
    if not values:
        # Spark's empty ordinary NullType column needs its standard Arrow fallback.
        # The geometry-only cases and populated frames remain strict Arrow checks.
        TestBase.spark.conf.set(
            "spark.sql.execution.arrow.pyspark.fallback.enabled", "true"
        )
    expected = gpd.GeoDataFrame(
        {
            "shape": gpd.GeoSeries(values, crs="EPSG:3857"),
            "other_shape": gpd.GeoSeries(values, crs="EPSG:4326"),
            "ordinary": pd.Series(values, dtype=object),
        },
        geometry="shape",
    )
    actual = native_geopandas.GeoDataFrame(expected)
    local = actual.to_geopandas()
    for name, epsg in [("shape", 3857), ("other_shape", 4326)]:
        assert isinstance(actual[name].spark.data_type, spark_types.GeometryType)
        assert isinstance(local[name].dtype, GeometryDtype)
        assert local[name].crs.to_epsg() == epsg
    assert local["ordinary"].dtype == object
    assert local.columns.equals(expected.columns)


@pytest.mark.parametrize("values", [[], [None, None]], ids=["empty", "all-null"])
def test_explicit_geometry_differs_from_plain_null_objects(native_geopandas, values):
    from sedona.spark.geopandas._native import from_pandas

    plain = pd.DataFrame({"shape": pd.Series(values, dtype=object)})
    declared = native_geopandas.GeoDataFrame(plain, geometry="shape", crs="EPSG:3857")
    assert isinstance(declared["shape"].spark.data_type, spark_types.GeometryType)
    assert declared.to_geopandas()["shape"].crs.to_epsg() == 3857
    ordinary = from_pandas(plain)
    assert not isinstance(
        ordinary._internal.data_fields[0].spark_type, spark_types.GeometryType
    )


def test_duplicate_multiindex_and_scratch_column_names(native_geopandas):
    index = pd.MultiIndex.from_tuples(
        [("same", 1), ("same", 1)], names=["label", "number"]
    )
    columns = pd.MultiIndex.from_tuples(
        [("geo", "shape"), ("meta", "tag"), ("__sedona_native_srid_0__", "")],
        names=["group", "field"],
    )
    frame = pd.DataFrame(
        [[None, "a", 11], [None, "b", 12]], index=index, columns=columns
    )
    result = native_geopandas.GeoDataFrame(
        frame, geometry=("geo", "shape"), crs="EPSG:3857"
    ).to_geopandas()
    assert result.index.equals(index)
    assert result.columns.equals(columns)
    assert result[("meta", "tag")].tolist() == ["a", "b"]
    assert result[("__sedona_native_srid_0__", "")].tolist() == [11, 12]
    assert result[("geo", "shape")].crs.to_epsg() == 3857


def test_declared_metadata_overrides_embedded_srid_without_mutating_input(
    native_geopandas,
):
    shape = shapely.set_srid(Point(1, 2), 3857)
    result = native_geopandas.GeoSeries([shape, None], crs="EPSG:4326").to_geopandas()
    assert shapely.get_srid(shape) == 3857
    assert result.crs.to_epsg() == 4326
    assert shapely.get_srid(result.iloc[0]) == 4326
    assert result.iloc[0].equals(shape)


@pytest.mark.parametrize("label", ["value", 0, ""])
def test_scalar_collection_preserves_anchor_and_duplicate_index(
    native_geopandas, label
):
    import pyspark.pandas as ps
    from geopandas.testing import assert_geodataframe_equal
    from pandas.testing import assert_series_equal

    index = pd.Index([7, 7, 9], name="id")
    local = gpd.GeoDataFrame(
        {"geometry": [Point(1, 2), None, Point(3, 4)], label: [3, 5, 8]},
        index=index,
        crs=4326,
    )
    with ps.option_context("compute.ops_on_diff_frames", False):
        frame = native_geopandas.GeoDataFrame(local)
        series = frame[label]

        assert series._anchor is frame
        assert frame[label] is series
        assert_series_equal(series.to_pandas(), local[label])
        assert_series_equal((series + 2).to_pandas(), local[label] + 2)
        frame["copied"] = series
        expected = local.assign(copied=local[label])
        assert_geodataframe_equal(frame.to_geopandas(), expected)


def test_to_geopandas_keeps_native_columns_and_scalar_nulls(native_geopandas):
    from geopandas.testing import assert_geodataframe_equal
    from pandas.testing import assert_series_equal

    local = gpd.GeoDataFrame(
        {
            "geometry": [Point(1, 2), None],
            "secondary": gpd.GeoSeries([None, Point(3, 4)], crs=3857),
            "value": [2.0, None],
            "name": ["a", None],
        },
        crs=4326,
    )
    frame = native_geopandas.GeoDataFrame(local)

    assert_series_equal(frame["name"].to_pandas(), local["name"])
    assert_series_equal(frame["value"].to_pandas(), local["value"])
    assert_geodataframe_equal(frame.to_geopandas(), local)


def _assert_native_crs(series, srid):
    from sedona.spark.sql import st_functions as stf

    assert series.spark.data_type == spark_types.GeometryType("ANY")
    assert series.crs.to_epsg() == srid
    rows = series._internal.spark_frame.select(
        stf.ST_SRID(series.spark.column).alias("srid"),
        stf.ST_X(series.spark.column).alias("x"),
        stf.ST_Y(series.spark.column).alias("y"),
    ).collect()
    assert sorted((r.srid, r.x, r.y) for r in rows if r.srid is not None) == [
        (srid, 1.0, 2.0)
    ]
    assert sum(r.srid is None for r in rows) == 1


def test_set_crs_keeps_native_any_and_embedded_srid(native_geopandas):
    source = native_geopandas.GeoSeries([Point(1, 2), None], crs=4326)
    result = source.set_crs(26909, allow_override=True)
    _assert_native_crs(result, 26909)
    _assert_native_crs(source, 4326)
    result.set_crs(3857, inplace=True, allow_override=True)
    _assert_native_crs(result, 3857)


def test_local_frame_override_preserves_independent_geometry_crs(native_geopandas):
    source = pd.DataFrame(
        {
            "geometry": gpd.GeoSeries([Point(1, 2), None], crs=4326).array,
            "secondary": gpd.GeoSeries([Point(1, 2), None], crs=3857).array,
        }
    )
    result = native_geopandas.GeoDataFrame(source, geometry="geometry", crs=26909)
    _assert_native_crs(result.geometry, 26909)
    _assert_native_crs(result["secondary"], 3857)
    assert source["geometry"].array.crs.to_epsg() == 4326
    assert source["secondary"].array.crs.to_epsg() == 3857


def test_sql_fixed_type_semantics_are_unchanged(native_geopandas):
    # Scalar outputs avoid the upstream PySpark fixed-CRS JSON mapper while
    # checking Spark's actual SQL type and stored SRID.
    result = TestBase.spark.sql(
        "SELECT typeof(g) AS spatial_type, ST_SRID(g) AS srid "
        "FROM (SELECT ST_SetSRID(ST_GeomFromWKT('POINT (1 2)'), 26909) AS g)"
    ).first()
    assert "26909" in result.spatial_type
    assert "any" not in result.spatial_type.lower()
    assert result.srid == 26909


@pytest.mark.parametrize("label", ["value", 0, ""])
def test_scalar_display_preserves_anchor_and_pyspark_format(native_geopandas, label):
    import pyspark.pandas as ps
    from pandas.testing import assert_series_equal

    local = gpd.GeoDataFrame(
        {"geometry": [Point(1, 2), None, Point(3, 4)], label: [3, 5, 8]},
        index=pd.Index([7, 7, 9], name="id"),
        crs=4326,
    )
    frame = native_geopandas.GeoDataFrame(local)
    series = frame[label]
    expected = ps.from_pandas(local[label])
    with ps.option_context("compute.ops_on_diff_frames", False):
        for max_rows in [2, 5, None]:
            with ps.option_context("display.max_rows", max_rows):
                display = repr(series)
                assert display == repr(expected)
                assert repr(series.head(2)) == repr(expected.head(2))
                assert ("Showing only the first 2" in display) == (max_rows == 2)
            assert series._anchor is frame
            assert frame[label] is series
        frame["copied"] = series
        assert_series_equal(frame["copied"].to_pandas(), local[label].rename("copied"))


@pytest.mark.parametrize("kind", ["frame", "series"])
def test_geometry_display_preserves_format_and_limits_collection(
    native_geopandas, monkeypatch, kind
):
    import pyspark.pandas as ps
    from geopandas.testing import assert_geodataframe_equal, assert_geoseries_equal
    from sedona.spark.geopandas import _native

    index = pd.Index([n // 2 for n in range(12)], name="id")
    shape = [Point(n, n + 1) if n % 3 else None for n in range(12)]
    local = gpd.GeoDataFrame(
        {
            "shape": gpd.GeoSeries(shape, index=index, crs=4326),
            "other": gpd.GeoSeries(shape, index=index, crs=3857),
            "value": list(range(12)),
        },
        index=index,
        geometry="shape",
    )
    frame = native_geopandas.GeoDataFrame(local)
    subject = frame if kind == "frame" else frame["shape"]
    anchor = subject._internal if kind == "frame" else subject._anchor
    metadata = [field.metadata for field in subject._internal.data_fields]

    # Shapely's text and HTML cell representation matches the WKT string control.
    # PySpark remains responsible for table formatting and truncation footers.
    text = pd.DataFrame(local)
    for label in ["shape", "other"]:
        text[label] = [
            str(value) if value is not None else None for value in local[label]
        ]
    expected = ps.from_pandas(text if kind == "frame" else text["shape"])
    collected = []
    original_to_pandas = _native.to_pandas

    def collect(internal):
        result = original_to_pandas(internal)
        collected.append(len(result))
        return result

    monkeypatch.setattr(_native, "to_pandas", collect)
    for max_rows in [2, 20, None]:
        with ps.option_context("display.max_rows", max_rows):
            collected.clear()
            assert repr(subject) == repr(expected)
            assert collected == [12 if max_rows is None else min(12, max_rows + 1)]
            if kind == "frame":
                collected.clear()
                assert subject._repr_html_() == expected._repr_html_()
                assert collected == [12 if max_rows is None else min(12, max_rows + 1)]

    assert (subject._internal if kind == "frame" else subject._anchor) is anchor
    assert [field.metadata for field in subject._internal.data_fields] == metadata
    collected.clear()
    head = subject.head(5)
    assert isinstance(head, type(subject))
    assert collected == []
    assert [field.metadata for field in head._internal.data_fields] == metadata
    with ps.option_context("display.max_rows", 2):
        assert repr(head) == repr(expected.head(5))
        assert collected == [3]
        if kind == "frame":
            collected.clear()
            assert head._repr_html_() == expected.head(5)._repr_html_()
            assert collected == [3]
    if kind == "frame":
        assert head.active_geometry_name == "shape"
        assert_geodataframe_equal(head.to_geopandas(), local.head(5))
    else:
        assert_geoseries_equal(head.to_geopandas(), local["shape"].head(5))
