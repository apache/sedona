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

from decimal import Decimal

import geopandas as gpd
import numpy as np
import pandas as pd
import pyspark.pandas as ps
import pytest
from geopandas.testing import assert_geoseries_equal
from pyspark.pandas.internal import NATURAL_ORDER_COLUMN_NAME
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import scol_for
from pyspark.sql import functions as F
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import unary_union

from sedona.spark.geopandas import GeoDataFrame, GeoSeries
from tests.geopandas.test_geopandas_base import TestGeopandasBase


class TestSimplifyCoverage(TestGeopandasBase):
    def test_long_ring_with_locked_boundary(self):
        rows = self.spark.range(1).selectExpr(
            "id",
            "ST_MakePolygon(ST_MakeLine(transform(sequence(0, 10000), i -> "
            "CASE WHEN i = 10000 THEN ST_Point(1D, 0D) ELSE "
            "ST_Point(cos(2*pi()*i/10000), sin(2*pi()*i/10000)) END))) geometry",
        )
        original = rows.selectExpr("ST_AsBinary(geometry) wkb").first().wkb
        source = GeoSeries(rows.pandas_api(index_col="id")["geometry"])

        result = source.simplify_coverage(0, simplify_boundary=False)
        internal = result._internal.resolved_copy
        output = (
            internal.spark_frame.select(
                internal.data_spark_columns[0].alias("geometry")
            )
            .selectExpr("ST_NPoints(geometry) points", "ST_AsBinary(geometry) wkb")
            .collect()
        )

        assert len(output) == 1
        assert output[0].points == 10001
        assert output[0].wkb == original

    @pytest.mark.parametrize("tolerance", [0, np.float64(0), Decimal("0")])
    def test_zero_tolerance_removes_collinear_vertices(self, tolerance):
        _ = self.spark
        source = GeoSeries([Polygon([(0, 0), (0.5, 0), (1, 0), (1, 1), (0, 1)])])

        result = source.simplify_coverage(tolerance).to_geopandas()

        assert result.iloc[0].equals(box(0, 0, 1, 1))
        assert len(result.iloc[0].exterior.coords) == 5

    def test_shared_boundary_and_multilevel_index(self):
        _ = self.spark
        left = Polygon([(0, 0), (1, 0), (1.1, 0.5), (1, 1), (0, 1)])
        right = Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1.1, 0.5)])
        index = pd.MultiIndex.from_tuples(
            [("same", 2), ("same", 2), ("missing", 0)],
            names=["group", "feature"],
        )
        source = GeoSeries([left, right, None], index=index, name="shape", crs=3857)

        result = source.simplify_coverage(0.3, simplify_boundary=False)
        first = result.to_geopandas()
        second = result.to_geopandas()

        expected = gpd.GeoSeries(
            [box(0, 0, 1, 1), box(1, 0, 2, 1), None],
            index=index,
            crs=3857,
        )
        assert_geoseries_equal(first, expected, normalize=True)
        assert_geoseries_equal(second, first)
        assert unary_union(first.dropna()).equals(unary_union([left, right]))
        assert first.iloc[0].intersection(first.iloc[1]).area == 0
        assert source.name == "shape"

    def test_geodataframe_uses_active_geometry(self):
        _ = self.spark
        geometry = Polygon([(0, 0), (0.5, 0), (1, 0), (1, 1), (0, 1)])
        frame = GeoDataFrame({"shape": [geometry], "value": [17]}, geometry="shape")

        result = frame.simplify_coverage(0).to_geopandas()

        assert result.name is None
        assert len(result.iloc[0].exterior.coords) == 5
        assert frame["value"].to_pandas().tolist() == [17]

    def test_empty_and_null_only_input(self):
        rows = self.spark.createDataFrame(
            [(0, "POLYGON EMPTY"), (1, None)], "id long, wkt string"
        ).selectExpr("id", "ST_GeomFromWKT(wkt) geometry")
        source = GeoSeries(rows.pandas_api(index_col="id")["geometry"])

        result = source.simplify_coverage(0.1).to_geopandas()

        assert result.index.tolist() == [0, 1]
        assert result.iloc[0].is_empty
        assert result.iloc[1] is None

    def test_boundary_option_requires_boolean(self):
        _ = self.spark
        with pytest.raises(TypeError, match="boolean"):
            GeoSeries([box(0, 0, 1, 1)]).simplify_coverage(
                0.1, simplify_boundary="False"
            )

    def test_empty_multipart_members_and_srid_survive_public_wrapper(self):
        rows = self.spark.createDataFrame(
            [
                (
                    7,
                    "SRID=3857;MULTIPOLYGON (EMPTY, ((0 0, 1 0, 2 0, 2 2, 0 2, 0 0)), EMPTY)",
                )
            ],
            "id long, ewkt string",
        ).selectExpr("id", "ST_GeomFromEWKT(ewkt) geometry")
        assert (
            rows.selectExpr("ST_NumGeometries(geometry) members").first().members == 3
        )
        source = GeoSeries(rows.pandas_api(index_col="id")["geometry"])

        result = source.simplify_coverage(0)
        internal = result._internal.resolved_copy
        geometry = (
            internal.spark_frame.select(
                internal.data_spark_columns[0].alias("geometry")
            )
            .selectExpr(
                "ST_NumGeometries(geometry) members",
                "ST_SRID(geometry) srid",
                "ST_NPoints(geometry) points",
            )
            .first()
        )

        assert geometry.members == 3
        assert geometry.srid == 3857
        assert geometry.points == 5
        assert result.crs.to_epsg() == 3857

    @pytest.mark.parametrize("tolerance", [-1, float("inf"), float("nan")])
    def test_rejects_invalid_tolerance(self, tolerance):
        _ = self.spark
        with pytest.raises(ValueError, match="finite.*non-negative"):
            GeoSeries([box(0, 0, 1, 1)]).simplify_coverage(tolerance)

    @pytest.mark.parametrize("tolerance", [[1], np.array([1]), "1", None])
    def test_rejects_non_scalar_tolerance(self, tolerance):
        _ = self.spark
        with pytest.raises(TypeError, match="numeric scalar"):
            GeoSeries([box(0, 0, 1, 1)]).simplify_coverage(tolerance)

    def test_boundary_lock_preserves_a_single_polygon(self):
        _ = self.spark
        geometry = Polygon([(0, 0), (0.5, 0), (1, 0), (1, 1), (0, 1)])

        result = (
            GeoSeries([geometry])
            .simplify_coverage(10, simplify_boundary=False)
            .to_geopandas()
        )

        assert result.iloc[0].equals_exact(geometry, 0)

    def test_duplicate_and_null_natural_order_do_not_merge_rows(self):
        _ = self.spark
        source = GeoSeries(
            [box(0, 0, 1, 1), box(2, 0, 3, 1), None],
            index=pd.Index(["duplicate", "duplicate", "missing"], name="feature"),
        )
        internal = source._internal.resolved_copy
        frame = internal.spark_frame.withColumn(
            NATURAL_ORDER_COLUMN_NAME,
            F.when(internal.data_spark_columns[0].isNotNull(), F.lit(7)).cast("long"),
        )
        internal = internal.copy(
            spark_frame=frame,
            index_spark_columns=[
                scol_for(frame, name) for name in internal.index_spark_column_names
            ],
            data_spark_columns=[
                scol_for(frame, name) for name in internal.data_spark_column_names
            ],
        )
        source = GeoSeries(first_series(ps.DataFrame(internal)))

        result = source.simplify_coverage(0, simplify_boundary=False).to_geopandas()

        assert len(result) == 3
        assert result.loc["missing"] is None
        assert sorted(geometry.bounds for geometry in result.loc["duplicate"]) == [
            (0.0, 0.0, 1.0, 1.0),
            (2.0, 0.0, 3.0, 1.0),
        ]

    def test_conflicting_edits_do_not_erase_a_lens(self):
        _ = self.spark
        geometries = [
            Polygon([(0, 0), (5, 1), (10, 0), (10, 5), (0, 5)]),
            Polygon([(0, 0), (5, -1), (10, 0), (5, 1)]),
            Polygon([(0, 0), (0, -5), (10, -5), (10, 0), (5, -1)]),
        ]

        result = (
            GeoSeries(geometries)
            .simplify_coverage(3, simplify_boundary=False)
            .to_geopandas()
        )

        assert all(geometry.is_valid for geometry in result)
        assert result.iloc[1].area > 0
        assert sum(len(geometry.exterior.coords) for geometry in result) < 17
        assert unary_union(result).equals(unary_union(geometries))
        assert sum(geometry.area for geometry in result) == unary_union(result).area

    def test_disconnected_obstacle_blocks_boundary_shortcut(self):
        _ = self.spark
        polygon = Polygon([(0, 0), (5, 1), (10, 0), (10, 5), (0, 5)])
        obstacle = box(4.9, 0.2, 5.1, 0.4)

        result = GeoSeries([polygon, obstacle]).simplify_coverage(3).to_geopandas()

        assert result.iloc[0].equals(polygon)
        assert result.iloc[1].is_valid and not result.iloc[1].is_empty
        assert result.iloc[0].intersection(result.iloc[1]).area == 0

    def test_shared_hole_boundary_and_multipart_are_preserved(self):
        _ = self.spark
        ring = [(1, 1), (2, 1), (2.1, 1.5), (2, 2), (1, 2)]
        geometries = [
            Polygon([(0, 0), (3, 0), (3, 3), (0, 3)], [ring]),
            MultiPolygon([Polygon(ring), box(5, 0, 6, 1)]),
        ]

        result = (
            GeoSeries(geometries)
            .simplify_coverage(0.3, simplify_boundary=False)
            .to_geopandas()
        )

        assert result.iloc[0].geom_type == "Polygon"
        assert len(result.iloc[0].interiors) == 1
        assert len(result.iloc[0].interiors[0].coords) == 5
        assert result.iloc[1].geom_type == "MultiPolygon"
        assert len(result.iloc[1].geoms) == 2
        assert all(geometry.is_valid for geometry in result)
        assert result.iloc[0].intersection(result.iloc[1]).area == 0
        assert unary_union(result).equals(unary_union(geometries))
