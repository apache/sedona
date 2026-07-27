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
import pandas as pd
import pytest
import pyspark.pandas as ps
from shapely.errors import EmptyPartError
from shapely.geometry import (
    LineString,
    MultiPoint,
    Point,
    Polygon,
)

from sedona.spark.geopandas import GeoDataFrame, GeoSeries
from sedona.spark.geopandas.tools import collect
from sedona.spark.sql import st_functions as stf
from tests.geopandas.test_geopandas_base import TestGeopandasBase


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
        assert null_group["shape"].is_empty

        srids = (
            result.geometry._internal.spark_frame.select(
                stf.ST_SRID(result.geometry.spark.column).alias("srid")
            )
            .distinct()
            .collect()
        )
        assert [row.srid for row in srids] == [3857]

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

    def test_dissolve_multiple_aggregations(self):
        source = GeoDataFrame(
            {
                "zone": ["a", "a", "b"],
                "value": [1, 3, 2],
                "label": ["x", "y", "z"],
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
            }
        )
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
        assert result.active_geometry_name == ("geometry", "")
        assert collected.loc["a", ("zone", "first")] == "a"
        assert collected.loc["a", ("value", "sum")] == 4
        assert collected.loc["a", ("value", "max")] == 3
        assert collected.loc["a", ("label", "first")] == "x"

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
        with pytest.raises(NotImplementedError, match="named Spark"):
            source.dissolve("zone", aggfunc=lambda values: values.iloc[0])
        with pytest.raises(NotImplementedError, match="grouping vectors"):
            source.dissolve(["a"])
        with pytest.raises(NotImplementedError, match="numeric_only"):
            source.dissolve(
                "zone",
                aggfunc={"zone": "first"},
                numeric_only=True,
            )

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

    def test_collect_scalar_and_distributed_singletons(self):
        point = Point(0, 0)
        assert collect(point) is point
        assert collect(point, multi=True).equals(MultiPoint([(0, 0)]))

        assert collect(GeoSeries([point])).equals(point)
        assert collect(GeoSeries([point]), multi=True).equals(MultiPoint([(0, 0)]))

        multi_point = MultiPoint([(0, 0), (1, 1)])
        assert collect(GeoSeries([multi_point]), multi=True).equals(multi_point)

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

        polygons = [
            Polygon(),
            Polygon([(0, 0), (1, 0), (0, 1)]),
        ]
        collected_polygons = collect(GeoSeries(polygons))
        assert collected_polygons.geom_type == "MultiPolygon"
        assert len(collected_polygons.geoms) == 1
        assert collect(GeoSeries([Polygon()]), multi=True).equals(
            gpd.tools.collect(Polygon(), multi=True)
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
        with pytest.raises(EmptyPartError, match="empty component"):
            collect(GeoSeries([Point()]), multi=True)
