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

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import pyspark.pandas as ps
import shapely
from packaging.version import parse as parse_version
from pyspark.sql import functions as F

try:
    from shapely.errors import EmptyPartError
except ImportError:
    EmptyPartError = ValueError
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPoint,
    Point,
    Polygon,
)

from sedona.spark.geopandas import GeoDataFrame, GeoSeries
from sedona.spark.geopandas.tools import collect
from sedona.spark.sql import st_functions as stf
from tests.geopandas.test_geopandas_base import TestGeopandasBase

SHAPELY_GE_20 = parse_version(shapely.__version__) >= parse_version("2.0.0")
GEOPANDAS_HAS_UNION_ALL = hasattr(gpd.GeoSeries, "union_all")


class TestGeometryAggregation(TestGeopandasBase):
    def test_dissolve_grouped_matches_geopandas(self):
        data = {
            "zone": ["b", "a", "b", None],
            "label": ["first-b", "only-a", "second-b", "missing"],
            "value": [1, 2, 3, 4],
            "shape": [Point(0, 0), Point(5, 5), Point(1, 1), None],
        }
        expected_source = gpd.GeoDataFrame(data, geometry="shape", crs="EPSG:3857")
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(expected_source)
        result = source.dissolve(
            "zone", aggfunc={"label": "first", "value": "sum"}, dropna=False
        )
        spark_frame = result._internal.spark_frame
        if hasattr(spark_frame, "_jdf"):
            plan = spark_frame._jdf.queryExecution().executedPlan().toString()
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
        expected = expected_source.dissolve(
            "zone",
            aggfunc={"label": "first", "value": "sum"},
            dropna=False,
        )

        self.check_sgpd_df_equals_gpd_df(result, expected)
        assert result.active_geometry_name == "shape"
        assert result.crs == expected.crs
        collected = result.to_geopandas()
        null_group = collected[collected.index.isna()].iloc[0]
        assert null_group["label"] == "missing"
        if GEOPANDAS_HAS_UNION_ALL:
            assert null_group["shape"].is_empty
        else:
            assert null_group["shape"] is None

        srids = (
            result.geometry._internal.spark_frame.select(
                stf.ST_SRID(result.geometry.spark.column).alias("srid")
            )
            .distinct()
            .collect()
        )
        expected_srids = {3857} if GEOPANDAS_HAS_UNION_ALL else {3857, None}
        assert {row.srid for row in srids} == expected_srids

    def test_dissolve_all_null_geometry_matches_geopandas_and_preserves_crs(self):
        expected_source = gpd.GeoDataFrame(
            {
                "zone": ["a", "a"],
                "geometry": [None, None],
            },
            crs="EPSG:4326",
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(expected_source)

        assert source.crs == expected_source.crs
        result = source.dissolve("zone")
        expected = expected_source.dissolve("zone")

        self.check_sgpd_df_equals_gpd_df(result, expected)
        assert result.crs == expected.crs
        srids = result.geometry._internal.spark_frame.select(
            stf.ST_SRID(result.geometry.spark.column).alias("srid")
        ).collect()
        expected_srid = 4326 if GEOPANDAS_HAS_UNION_ALL else None
        assert [row.srid for row in srids] == [expected_srid]

    def test_dissolve_series_grouper_retains_data_column(self):
        local = gpd.GeoDataFrame(
            {
                "zone": ["a", "a", "b"],
                "label": ["first-a", "second-a", "only-b"],
                "value": [1, 2, 3],
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            crs="EPSG:4326",
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(local)

        label_result = source.dissolve(by="zone")
        series_result = source.dissolve(by=source["zone"])
        expected = local.dissolve(by=local["zone"])

        assert "zone" not in label_result.columns
        assert list(series_result.columns) == [
            "geometry",
            "zone",
            "label",
            "value",
        ]
        collected = series_result.to_geopandas()
        assert collected.index.name == expected.index.name == "zone"
        assert collected.geometry.name == "geometry"
        assert collected.crs == expected.crs == source.crs
        assert collected.loc["a", "zone"] == "a"
        assert collected.loc["a", "label"] == "first-a"
        assert collected.loc["a", "value"] == 1
        self.check_sgpd_df_equals_gpd_df(series_result, expected)

    def test_dissolve_all_rows_and_as_index_false(self):
        data = {
            "label": ["first", "second"],
            "value": [2, 3],
            "geometry": [Point(0, 0), Point(1, 1)],
        }
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(data, crs="EPSG:4326")
        result = source.dissolve(
            aggfunc={"label": "first", "value": "sum"},
            as_index=False,
        )

        assert list(result.columns) == ["index", "geometry", "label", "value"]
        row = result.to_geopandas().iloc[0]
        assert row["index"] == 0
        assert row["label"] == "first"
        assert row["value"] == 5
        assert row["geometry"].equals(MultiPoint([(0, 0), (1, 1)]))
        assert result.crs == source.crs
        assert result["index"].dtype == np.dtype("int64")

    def test_dissolve_level_multikey_and_sort(self):
        index = pd.MultiIndex.from_tuples(
            [("z", 2), ("a", 1), ("z", 1)],
            names=["letter", "number"],
        )
        source = gpd.GeoDataFrame(
            {
                "kind": ["q", "q", "r"],
                "value": [1, 2, 3],
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            index=index,
            crs="EPSG:4326",
        )

        with ps.option_context("compute.ops_on_diff_frames", True):
            distributed = GeoDataFrame(source)

        level_result = distributed.dissolve(level="letter")
        level_expected = source.dissolve(level="letter")
        self.check_sgpd_df_equals_gpd_df(level_result, level_expected)
        assert level_result.index.names == ["letter"]

        multikey_result = distributed.dissolve(
            by=["kind", "value"],
            sort=False,
        )
        multikey_expected = source.dissolve(
            by=["kind", "value"],
            sort=False,
        )
        self.check_sgpd_df_equals_gpd_df(multikey_result, multikey_expected)
        assert multikey_result.index.names == ["kind", "value"]

    def test_dissolve_multiple_aggregations_including_grouping_column(self):
        local = gpd.GeoDataFrame(
            {
                "zone": ["a", "a", "b"],
                "value": [1, 3, 2],
                "label": ["x", "y", "z"],
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
            }
        )
        local.columns.name = "attributes"
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(local)
        result = source.dissolve(
            "zone",
            aggfunc={
                "zone": "first",
                "value": ("sum", "max"),
                "label": "first",
            },
        )
        collected = result.to_geopandas()

        assert isinstance(result.columns, pd.MultiIndex)
        assert result.columns.names == ["attributes", None]
        assert result.active_geometry_name == ("geometry", "")
        assert collected.loc["a", ("zone", "first")] == "a"
        assert collected.loc["a", ("value", "sum")] == 4
        assert collected.loc["a", ("value", "max")] == 3
        assert collected.loc["a", ("label", "first")] == "x"

    def test_dissolve_first_last_skip_nulls_in_source_order(self):
        source = GeoDataFrame(
            {
                "zone": ["a"] * 6 + ["b"] * 2,
                "label": [
                    None,
                    "first",
                    "middle",
                    None,
                    "last",
                    None,
                    None,
                    None,
                ],
                "value": [
                    np.nan,
                    1.0,
                    2.0,
                    np.nan,
                    3.0,
                    np.nan,
                    np.nan,
                    np.nan,
                ],
                "geometry": [Point(i, i) for i in range(8)],
            }
        )
        distributed_result = source.dissolve(
            "zone",
            aggfunc={
                "label": ["first", "last", "count", "nunique"],
                "value": ["first", "last", "count", "nunique"],
            },
        )
        spark_frame = distributed_result._internal.spark_frame
        if hasattr(spark_frame, "_jdf"):
            plan = spark_frame._jdf.queryExecution().analyzed().toString().lower()
            assert "min_by" in plan
            assert "max_by" in plan
        result = distributed_result.to_geopandas()

        assert result.loc["a", ("label", "first")] == "first"
        assert result.loc["a", ("label", "last")] == "last"
        assert result.loc["a", ("label", "count")] == 3
        assert result.loc["a", ("label", "nunique")] == 3
        assert result.loc["a", ("value", "first")] == 1.0
        assert result.loc["a", ("value", "last")] == 3.0
        assert result.loc["a", ("value", "count")] == 3
        assert result.loc["a", ("value", "nunique")] == 3
        assert result.loc["b", ("label", "first")] is None
        assert result.loc["b", ("label", "last")] is None
        assert result.loc["b", ("label", "count")] == 0
        assert result.loc["b", ("label", "nunique")] == 0
        assert pd.isna(result.loc["b", ("value", "first")])
        assert pd.isna(result.loc["b", ("value", "last")])

    def test_dissolve_numeric_boolean_aggregation_semantics(self):
        local = gpd.GeoDataFrame(
            {
                "zone": ["a", "a", "b", "b"],
                "integer": [1, 2, 3, 4],
                "number": [1.5, 2.5, np.nan, np.nan],
                "flag": pd.Series([True, False, None, None], dtype="boolean"),
                "geometry": [Point(i, i) for i in range(4)],
            }
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(local)

        result = source.dissolve(
            "zone",
            aggfunc={
                "integer": [np.sum, np.mean],
                "number": "sum",
                "flag": ["sum", "mean", "median", "std", "var", "min", "max"],
            },
        ).to_geopandas()

        assert result.loc["a", ("integer", "sum")] == 3
        assert result.loc["a", ("integer", "mean")] == 1.5
        assert result[("integer", "sum")].dtype == np.dtype("int64")
        assert result.loc["b", ("number", "sum")] == 0.0
        assert result.loc["a", ("flag", "sum")] == 1
        assert result.loc["b", ("flag", "sum")] == 0
        assert result[("flag", "sum")].dtype == np.dtype("int64")
        assert result.loc["a", ("flag", "mean")] == 0.5
        assert result.loc["a", ("flag", "median")] == 0.5
        assert result.loc["a", ("flag", "std")] == pytest.approx(2**-0.5)
        assert result.loc["a", ("flag", "var")] == 0.5
        assert not bool(result.loc["a", ("flag", "min")])
        assert bool(result.loc["a", ("flag", "max")])
        for function_name in ["mean", "median", "std", "var"]:
            assert result[("flag", function_name)].dtype == np.dtype("float64")

    def test_dissolve_empty_preserves_crs(self):
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(
                gpd.GeoDataFrame(
                    {
                        "zone": pd.Series(dtype="object"),
                        "geometry": gpd.GeoSeries([], crs="EPSG:3857"),
                    },
                    geometry="geometry",
                    crs="EPSG:3857",
                )
            )
        assert source.crs == "EPSG:3857"

        result = source.dissolve("zone")
        assert result.to_geopandas().empty
        assert result.crs == source.crs
        assert result.active_geometry_name == "geometry"

        empty_geometry = result.geometry
        empty_geometry.set_crs(None, inplace=True)
        assert empty_geometry.crs is None

    def test_dissolve_validation(self):
        source = GeoDataFrame({"zone": ["a"], "geometry": [Point(0, 0)]})

        with pytest.raises(NotImplementedError, match="method='unary'"):
            source.dissolve("zone", method="coverage")
        with pytest.raises(NotImplementedError, match="grid_size"):
            source.dissolve("zone", grid_size=1)
        with pytest.raises(NotImplementedError, match="known built-in"):
            source.dissolve("zone", aggfunc=lambda values: values.iloc[0])
        with pytest.raises(NotImplementedError, match="grouping vectors"):
            source.dissolve(["a"])
        with pytest.raises(NotImplementedError, match="numeric_only"):
            source.dissolve(
                "zone",
                aggfunc={"zone": "first"},
                numeric_only=True,
            )
        with pytest.raises(NotImplementedError, match="prod aggregation"):
            source.dissolve("zone", aggfunc="prod")

        strings = GeoDataFrame(
            {
                "zone": ["a", "a"],
                "label": ["x", "y"],
                "geometry": [Point(0, 0), Point(1, 1)],
            }
        )
        for aggregation in ["min", "max", "sum", "mean", "median", "std", "var"]:
            with pytest.raises(
                NotImplementedError,
                match=rf"'{aggregation}'.*column 'label'.*'string'",
            ):
                strings.dissolve("zone", aggfunc=aggregation)

        def sum(values):
            return values.sum()

        with pytest.raises(NotImplementedError, match="known built-in"):
            source.dissolve("zone", aggfunc=sum)

        geometry_only = GeoDataFrame({"geometry": [Point(0, 0)]})
        assert list(geometry_only.dissolve().columns) == ["geometry"]
        with pytest.raises(ValueError, match="No objects to concatenate"):
            geometry_only.dissolve(aggfunc=["first"])

        empty_dict_functions = GeoDataFrame(
            {"value": [1], "geometry": [Point(0, 0)]}
        ).dissolve(aggfunc={"value": []})
        assert list(empty_dict_functions.columns) == ["geometry"]

        with ps.option_context("compute.ops_on_diff_frames", True):
            categorical = GeoDataFrame(
                gpd.GeoDataFrame(
                    {
                        "zone": pd.Categorical(["a"], categories=["a", "b"]),
                        "geometry": [Point(0, 0)],
                    }
                )
            )
        with pytest.raises(NotImplementedError, match="observed=True"):
            categorical.dissolve("zone")

        categorical_values = gpd.GeoDataFrame(
            {
                "zone": ["a", "a"],
                "category": pd.Categorical(["x", None]),
                "geometry": [Point(0, 0), Point(1, 1)],
            }
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            distributed_categories = GeoDataFrame(categorical_values)
        category_result = distributed_categories.dissolve(
            "zone",
            aggfunc={"category": "first"},
        ).to_geopandas()
        assert category_result.loc["a", "category"] == "x"
        assert isinstance(category_result["category"].dtype, pd.CategoricalDtype)
        with pytest.raises(
            NotImplementedError,
            match="'nunique'.*'category'",
        ):
            distributed_categories.dissolve(
                "zone",
                aggfunc={"category": "nunique"},
            )

    def test_collect_scalar_and_distributed_singletons(self):
        point = Point(0, 0)
        assert collect(point) is point
        assert collect(point, multi=True).equals(MultiPoint([(0, 0)]))

        assert collect(GeoSeries([point])).equals(point)
        assert collect(GeoSeries([point]), multi=True).equals(MultiPoint([(0, 0)]))

        multi_point = MultiPoint([(0, 0), (1, 1)])
        assert collect(GeoSeries([multi_point]), multi=True).equals(multi_point)

    @pytest.mark.parametrize(
        "geometries, crs, expected_srid",
        [
            ([Point(0, 0), Point(1, 1)], None, 0),
            pytest.param(
                [
                    Polygon(),
                    Polygon([(0, 0), (1, 0), (0, 1)]),
                ],
                None,
                0,
                marks=pytest.mark.skipif(
                    not SHAPELY_GE_20,
                    reason="Shapely 1 does not retain typed empty geometries",
                ),
            ),
            pytest.param(
                [Polygon(), Polygon()],
                "EPSG:4326",
                4326,
                marks=pytest.mark.skipif(
                    not SHAPELY_GE_20,
                    reason="Shapely 1 does not retain typed empty geometries",
                ),
            ),
        ],
    )
    def test_collect_uses_one_spark_action(
        self,
        monkeypatch,
        geometries,
        crs,
        expected_srid,
    ):
        first_calls = 0
        result_srids = []
        series = GeoSeries(geometries, crs=crs)
        spark_frame_type = type(series._internal.spark_frame)
        original_first = spark_frame_type.first

        def count_first(frame):
            nonlocal first_calls
            first_calls += 1
            enriched = frame.select(
                "*",
                stf.ST_SRID(F.col("__collect_geometry__")).alias(
                    "__collect_result_srid__"
                ),
            )
            result = original_first(enriched)
            result_srids.append(result["__collect_result_srid__"])
            return result

        # Spark 4 exposes the classic DataFrame as a concrete subclass of
        # pyspark.sql.DataFrame, so patch the runtime class used by this frame.
        monkeypatch.setattr(spark_frame_type, "first", count_first)

        collect(series)

        assert first_calls == 1
        assert result_srids == [expected_srid]

    def test_collect_distributed_geometry_families(self):
        assert collect(GeoSeries([Point(0, 0), Point(1, 1)])).equals(
            MultiPoint([(0, 0), (1, 1)])
        )

        lines = [
            LineString([(0, 0), (1, 1)]),
            LineString([(2, 2), (3, 3)]),
        ]
        collected_lines = collect(GeoSeries(lines))
        assert collected_lines.geom_type == "MultiLineString"
        assert len(collected_lines.geoms) == 2

        if SHAPELY_GE_20:
            empty_polygon = Polygon()
            polygon = Polygon([(0, 0), (1, 0), (0, 1)])
            for polygons in (
                [empty_polygon, polygon],
                [polygon, empty_polygon],
            ):
                collected_polygons = collect(GeoSeries(polygons))
                assert collected_polygons.geom_type == "MultiPolygon"
                assert len(collected_polygons.geoms) == 1

            singleton_empty = collect(GeoSeries([empty_polygon]))
            assert singleton_empty.geom_type == "Polygon"
            assert singleton_empty.is_empty

            for polygons in (
                [empty_polygon],
                [empty_polygon, empty_polygon],
            ):
                collected_empty = collect(
                    GeoSeries(polygons, crs="EPSG:4326"),
                    multi=True,
                )
                assert collected_empty.geom_type == "MultiPolygon"
                assert collected_empty.is_empty
                assert len(collected_empty.geoms) == 0
        else:
            polygons = [
                Polygon([(0, 0), (1, 0), (0, 1)]),
                Polygon([(2, 0), (3, 0), (2, 1)]),
            ]
            collected_polygons = collect(GeoSeries(polygons))
            assert collected_polygons.geom_type == "MultiPolygon"
            assert len(collected_polygons.geoms) == 2
            with pytest.raises(ValueError, match="homogeneous"):
                collect(GeoSeries([Polygon(), polygons[0]]))
            with pytest.raises(KeyError, match="GeometryCollection"):
                collect(GeoSeries([Polygon()]), multi=True)

        with pytest.raises(ValueError, match="homogeneous"):
            collect(
                GeoSeries(
                    [
                        GeometryCollection(),
                        Polygon([(0, 0), (1, 0), (0, 1)]),
                    ]
                )
            )

    def test_collect_validation_matches_geopandas(self):
        with pytest.raises(IndexError, match="list index out of range"):
            collect(GeoSeries([]))
        with pytest.raises(AttributeError, match="geom_type"):
            collect(GeoSeries([Point(0, 0), None]))
        with pytest.raises(ValueError, match="homogeneous"):
            collect(GeoSeries([Point(0, 0), LineString([(0, 0), (1, 1)])]))
        with pytest.raises(ValueError, match="Cannot collect MultiPoint"):
            collect(
                GeoSeries(
                    [
                        MultiPoint([(0, 0)]),
                        MultiPoint([(1, 1)]),
                    ]
                )
            )
        if SHAPELY_GE_20:
            for empty, non_empty in (
                (Point(), Point(0, 0)),
                (LineString(), LineString([(0, 0), (1, 1)])),
            ):
                singleton_empty = collect(GeoSeries([empty]))
                assert singleton_empty.geom_type == empty.geom_type
                assert singleton_empty.is_empty
                with pytest.raises(EmptyPartError, match="empty component"):
                    collect(GeoSeries([empty]), multi=True)
                with pytest.raises(EmptyPartError, match="empty component"):
                    collect(GeoSeries([empty, non_empty]))
        else:
            with pytest.raises(KeyError, match="GeometryCollection"):
                collect(GeoSeries([Point()]), multi=True)
