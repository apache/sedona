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

import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import pyspark.pandas as ps
from packaging.version import parse as parse_version
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    box,
)

import sedona.spark.geopandas as sgpd
from sedona.spark.geopandas import GeoDataFrame, GeoSeries
from sedona.spark.sql import st_functions as stf
from tests.geopandas.test_geopandas_base import TestGeopandasBase


class TestDistributedSpatialSubsetting(TestGeopandasBase):
    @staticmethod
    def _assert_srid(series, expected):
        internal = series._internal.resolved_copy
        geometry = internal.data_spark_columns[0]
        srids = {
            row.srid
            for row in internal.spark_frame.select(
                stf.ST_SRID(geometry).alias("srid")
            ).collect()
            if row.srid is not None
        }
        assert srids == {expected}

    def test_cx_series_matches_geopandas_and_uses_exact_intersection(self):
        geometries = [
            Point(0, 0),
            LineString([(-2, 2), (2, -2)]),
            # Its envelope intersects the query rectangle, but the geometry
            # itself does not.
            MultiPoint([(-2, -2), (2, 2)]),
            Point(),
            None,
        ]
        index = pd.MultiIndex.from_tuples(
            [("a", 2), ("a", 1), ("b", 2), ("b", 1), ("c", 1)],
            names=["group", "position"],
        )
        expected_source = gpd.GeoSeries(geometries, index=index, name="shape", crs=4326)
        source = GeoSeries(expected_source, crs=4326)

        result = source.cx[-0.5:0.5, -0.5:0.5]
        expected = expected_source.cx[-0.5:0.5, -0.5:0.5]

        self.check_sgpd_equals_gpd(result, expected)
        assert list(result.to_geopandas().index) == list(expected.index)
        assert result.crs == expected.crs
        self._assert_srid(result, 4326)

    def test_cx_open_reversed_numeric_and_step_slices(self):
        geometries = [
            Point(0, 0),
            Point(1, 1),
            Point(2, 2),
            Point(),
            None,
        ]
        expected_source = gpd.GeoSeries(geometries, name="shape", crs=3857)
        source = GeoSeries(expected_source, crs=3857)

        for key in [
            (slice(1, None), slice(None)),
            (slice(2, 1), slice(None)),
            (1, 1),
            (slice(None), slice(None)),
        ]:
            self.check_sgpd_equals_gpd(source.cx[key], expected_source.cx[key])

        with pytest.warns(UserWarning, match="Ignoring step"):
            result = source.cx[0:2:10, 0:2]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            expected = expected_source.cx[0:2:10, 0:2]
        self.check_sgpd_equals_gpd(result, expected)

        with pytest.raises(TypeError):
            source.cx[0:1]
        with pytest.raises(TypeError, match="numeric"):
            source.cx["left":"right", :]

    def test_cx_all_null_series_preserves_crs(self):
        expected_source = gpd.GeoSeries([None], name="shape", crs=4326)
        source = GeoSeries([None], name="shape", crs=4326)

        result = source.cx[0:1, 0:1]
        expected = expected_source.cx[0:1, 0:1]

        self.check_sgpd_equals_gpd(result, expected)
        assert len(result) == 0
        assert source.crs == expected_source.crs
        assert result.crs == expected.crs

    def test_cx_geodataframe_preserves_columns_active_geometry_and_crs(self):
        index = pd.MultiIndex.from_tuples(
            [("b", 2), ("a", 2), ("a", 1)], names=["group", "position"]
        )
        expected_source = gpd.GeoDataFrame(
            {
                "value": [10, 20, 30],
                "backup": [Point(10, 10), Point(11, 11), Point(12, 12)],
                "shape": [Point(2, 2), Point(0, 0), Point(1, 1)],
            },
            geometry="shape",
            index=index,
            crs=4326,
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(expected_source)

        result = source.cx[:1, :1]
        expected = expected_source.cx[:1, :1]

        self.check_sgpd_df_equals_gpd_df(result, expected)
        assert result.active_geometry_name == "shape"
        assert result.crs == expected.crs
        self._assert_srid(result.geometry, 4326)

    @pytest.mark.parametrize("use_top_level", [False, True])
    def test_clip_series_scalar_polygon_matches_geopandas(self, use_top_level):
        geometries = [
            Point(0, 0),
            LineString([(-1, 0.5), (2, 0.5)]),
            Polygon([(0.5, -1), (2, -1), (2, 2), (0.5, 2)]),
            Point(3, 3),
            Point(),
            None,
        ]
        index = pd.Index([6, 5, 4, 3, 2, 1], name="feature_id")
        expected_source = gpd.GeoSeries(geometries, index=index, name="shape", crs=4326)
        source = GeoSeries(expected_source, crs=4326)
        mask = box(0, 0, 1, 1)

        if use_top_level:
            result = sgpd.clip(source, mask)
        else:
            result = source.clip(mask)
        expected = gpd.clip(expected_source, mask)

        self.check_sgpd_equals_gpd(result, expected)
        assert result.crs == expected.crs
        self._assert_srid(result, 4326)

    def test_clip_distributed_mask_is_dissolved_on_cluster(self):
        expected_source = gpd.GeoSeries(
            [
                LineString([(-1, 0.5), (2, 0.5)]),
                Point(0.25, 0.25),
                Point(2, 2),
            ],
            index=pd.Index([3, 2, 1], name="feature_id"),
            name="shape",
            crs=3857,
        )
        expected_mask = gpd.GeoSeries(
            [box(0, 0, 0.5, 1), box(0.5, 0, 1, 1)],
            name="mask",
            crs=3857,
        )
        source = GeoSeries(expected_source, crs=3857)
        mask = GeoSeries(expected_mask, crs=3857)

        result = source.clip(mask)
        expected = gpd.clip(expected_source, expected_mask)

        self.check_sgpd_equals_gpd(result, expected)
        plan = result._internal.spark_frame._jdf.queryExecution().toString()
        assert "union_aggr" in plan.lower()
        assert "PythonUDF" not in plan

    def test_clip_rectangle_and_empty_results_preserve_crs(self):
        expected_source = gpd.GeoSeries(
            [
                LineString([(-1, 0.5), (2, 0.5)]),
                Point(0.25, 0.25),
                Point(2, 2),
            ],
            name="shape",
            crs=4326,
        )
        source = GeoSeries(expected_source, crs=4326)
        rectangle = (0, 0, 1, 1)

        self.check_sgpd_equals_gpd(
            source.clip(rectangle),
            gpd.clip(expected_source, rectangle),
        )

        for invalid_rectangle in [
            (1, 0, 0, 1),
            (0, 1, 1, 0),
            (0, 0, 0, 1),
            (0, 0, 1, 0),
            (np.nan, 0, np.nan, 1),
        ]:
            invalid_result = source.clip(invalid_rectangle)
            assert len(invalid_result) == 0
            assert invalid_result.crs == expected_source.crs

        empty_series = source.clip(box(10, 10, 11, 11))
        assert len(empty_series) == 0
        assert empty_series.crs == expected_source.crs

        source_frame = source.to_geoframe("geometry")
        empty_frame = source_frame.clip(box(10, 10, 11, 11))
        assert len(empty_frame) == 0
        assert empty_frame.crs == expected_source.crs
        assert empty_frame.active_geometry_name == "geometry"

    def test_clip_all_null_series_preserves_crs(self):
        expected_source = gpd.GeoSeries([None], name="shape", crs=4326)
        source = GeoSeries([None], name="shape", crs=4326)

        result = source.clip(box(0, 0, 1, 1))
        expected = expected_source.clip(box(0, 0, 1, 1))

        self.check_sgpd_equals_gpd(result, expected)
        assert len(result) == 0
        assert source.crs == expected_source.crs
        assert result.crs == expected.crs

    def test_clip_geodataframe_preserves_index_columns_and_active_geometry(self):
        index = pd.MultiIndex.from_tuples(
            [("b", 2), ("a", 2), ("a", 1), ("c", 1)],
            names=["group", "position"],
        )
        expected_source = gpd.GeoDataFrame(
            {
                "value": [10, 20, 30, 40],
                "label": ["ten", "twenty", "thirty", "forty"],
                "shape": [
                    Point(2, 2),
                    LineString([(-1, 0.5), (2, 0.5)]),
                    Point(0.5, 0.5),
                    Point(3, 3),
                ],
            },
            geometry="shape",
            index=index,
            crs=4326,
        )
        expected_mask = gpd.GeoDataFrame(
            {"mask_id": [1, 2]},
            geometry=[box(0, 0, 0.5, 1), box(0.5, 0, 1, 1)],
            crs=4326,
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            source = GeoDataFrame(expected_source)
            mask = GeoDataFrame(expected_mask)

        result = source.clip(mask, sort=True)
        if parse_version(gpd.__version__) >= parse_version("1.0.0"):
            expected = expected_source.clip(expected_mask, sort=True)
        else:
            expected = expected_source.clip(expected_mask)
            expected = expected.loc[
                expected_source.index[expected_source.index.isin(expected.index)]
            ]

        self.check_sgpd_df_equals_gpd_df(result, expected)
        assert result.active_geometry_name == "shape"
        assert list(result.to_geopandas().index) == list(expected.index)
        assert result.crs == expected.crs
        self._assert_srid(result.geometry, 4326)

    def test_clip_keep_geom_type_and_mixed_type_warnings(self):
        mask = box(0, 0, 1, 1)
        expected_lines = gpd.GeoSeries(
            [
                LineString([(1, 0.5), (2, 0.5)]),
                LineString([(-1, 0.25), (2, 0.25)]),
            ],
            index=pd.Index([5, 3], name="feature_id"),
            crs=4326,
        )
        lines = GeoSeries(expected_lines, crs=4326)

        self.check_sgpd_equals_gpd(
            lines.clip(mask, keep_geom_type=True),
            expected_lines.clip(mask, keep_geom_type=True),
        )

        collapsed_line = gpd.GeoSeries([LineString([(1, 0.5), (2, 0.5)])], crs=4326)
        self.check_sgpd_equals_gpd(
            GeoSeries(collapsed_line, crs=4326).clip(mask, keep_geom_type=True),
            collapsed_line.clip(mask, keep_geom_type=True),
        )

        expected_mixed = gpd.GeoSeries(
            [Point(0.5, 0.5), LineString([(-1, 0.5), (2, 0.5)])],
            crs=4326,
        )
        mixed = GeoSeries(expected_mixed, crs=4326)
        with pytest.warns(UserWarning, match="mixed type"):
            result = mixed.clip(mask, keep_geom_type=True)
        with pytest.warns(UserWarning, match="mixed type"):
            expected = expected_mixed.clip(mask, keep_geom_type=True)
        self.check_sgpd_equals_gpd(result, expected)

        expected_collection = gpd.GeoSeries(
            [GeometryCollection([Point(0.5, 0.5)])], crs=4326
        )
        collection = GeoSeries(expected_collection, crs=4326)
        with pytest.warns(UserWarning, match="GeometryCollection"):
            collection.clip(mask, keep_geom_type=True)

    def test_clip_keep_geom_type_explodes_all_multipart_rows(self):
        mask = box(0, 0, 2, 2)
        expected_source = gpd.GeoSeries(
            [
                MultiPolygon(
                    [
                        box(0.5, 0.5, 1, 1),
                        box(2, 0.5, 3, 1),
                    ]
                ),
                MultiPolygon(
                    [
                        box(0.2, 1.2, 0.4, 1.4),
                        box(0.6, 1.2, 0.8, 1.4),
                    ]
                ),
            ],
            index=pd.Index(["collection", "multipart"], name="feature_id"),
            crs=4326,
        )
        source = GeoSeries(expected_source, crs=4326)

        result = source.clip(mask, keep_geom_type=True)
        expected = expected_source.clip(mask, keep_geom_type=True)
        # GeoPandas 0.13 drops the index name when clip explodes collections.
        # Sedona intentionally preserves the source index metadata.
        expected.index.names = expected_source.index.names

        self.check_sgpd_equals_gpd(result, expected)
        # Preserve source-row order even though GeoPandas 0.13 places exploded
        # multipart rows before the collection row.
        assert list(result.to_geopandas().index) == [
            "collection",
            "multipart",
            "multipart",
        ]
        assert list(result.geom_type.to_pandas()) == ["Polygon"] * 3
        self._assert_srid(result, 4326)

    def test_clip_keep_geom_type_preserves_multipart_without_collection(self):
        mask = box(0, 0, 2, 2)
        expected_source = gpd.GeoSeries(
            [
                MultiPolygon(
                    [
                        box(0.2, 0.2, 0.4, 0.4),
                        box(0.6, 0.6, 0.8, 0.8),
                    ]
                ),
                box(2, 0.5, 3, 1),
            ],
            index=pd.Index(["multi", "edge"], name="feature_id"),
            crs=4326,
        )
        source = GeoSeries(expected_source, crs=4326)

        result = source.clip(mask, keep_geom_type=True)
        expected = expected_source.clip(mask, keep_geom_type=True)

        self.check_sgpd_equals_gpd(result, expected)
        assert list(result.to_geopandas().index) == ["multi"]
        assert list(result.geom_type.to_pandas()) == ["MultiPolygon"]
        self._assert_srid(result, 4326)

    def test_clip_keep_geom_type_explodes_collection_with_null_first_row(self):
        mask = box(0, 0, 2, 2)
        expected_source = gpd.GeoSeries(
            [
                None,
                MultiPolygon(
                    [
                        box(0.5, 0.5, 1, 1),
                        box(2, 0.5, 3, 1),
                    ]
                ),
            ],
            index=pd.Index(["null", "collection"], name="feature_id"),
            crs=4326,
        )
        source_seed = expected_source.copy()
        source_seed.iloc[0] = box(0, 0, 0.1, 0.1)
        source = GeoSeries(source_seed, crs=4326)
        source.iloc[0] = None

        result = source.clip(mask, keep_geom_type=True)
        expected = expected_source.clip(mask, keep_geom_type=True)
        # GeoPandas 0.13 drops the index name when clip explodes collections.
        # Sedona intentionally preserves the source index metadata.
        expected.index.names = expected_source.index.names

        self.check_sgpd_equals_gpd(result, expected)
        assert list(result.to_geopandas().index) == ["collection", "collection"]
        assert set(result.geom_type.to_pandas()) == {"Polygon", "LineString"}
        self._assert_srid(result, 4326)

    def test_clip_keep_geom_type_uses_first_geometry_type_including_null(self):
        mask = box(0, 0, 1, 1)
        expected_source = gpd.GeoSeries(
            [
                None,
                LineString([(-1, 0.25), (2, 0.25)]),
                LineString([(1, 0.5), (2, 0.5)]),
            ],
            index=pd.Index(["null", "line", "point"], name="feature_id"),
            crs=4326,
        )
        source_seed = expected_source.copy()
        source_seed.iloc[0] = LineString([(0, 0), (1, 1)])
        source = GeoSeries(source_seed, crs=4326)
        source.iloc[0] = None

        result = source.clip(mask, keep_geom_type=True)
        expected = expected_source.clip(mask, keep_geom_type=True)

        self.check_sgpd_equals_gpd(result, expected)
        assert set(result.geom_type.to_pandas()) == {"LineString", "Point"}

    def test_clip_crs_mismatch_warns_and_invalid_inputs_fail(self):
        source = GeoSeries([Point(0.5, 0.5)], crs=4326)
        mask = GeoSeries([box(0, 0, 1, 1)], crs=3857)

        with pytest.warns(UserWarning, match="CRS mismatch between the CRS"):
            source.clip(mask)

        with pytest.raises(TypeError, match="'gdf'"):
            sgpd.clip([Point(0, 0)], box(0, 0, 1, 1))
        with pytest.raises(TypeError, match="four values"):
            source.clip((0, 0, 1))
        with pytest.raises(TypeError, match="Rectangle mask values"):
            source.clip((0, 0, "right", 1))
        with pytest.raises(TypeError, match="'mask'"):
            source.clip(Point(0, 0))
        with pytest.raises(TypeError, match="keep_geom_type"):
            source.clip(box(0, 0, 1, 1), keep_geom_type="yes")
        with pytest.raises(TypeError, match="'sort'"):
            source.clip(box(0, 0, 1, 1), sort="yes")
