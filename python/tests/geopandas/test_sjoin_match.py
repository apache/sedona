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
import shapely
from packaging.version import parse as parse_version
from shapely.geometry import Point

from sedona.spark.geopandas import GeoDataFrame, sjoin
from tests.geopandas.test_geopandas_base import TestGeopandasBase


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
@pytest.mark.skipif(
    parse_version(gpd.__version__) < parse_version("1.0.0"),
    reason=f"GeoPandas {gpd.__version__} does not support dwithin spatial joins",
)
class TestSJoinDWithinMatch(TestGeopandasBase):
    def setup_method(self):
        super().setup_method()

        left_data = {
            "id": ["origin", "middle", "far"],
            "geometry": [Point(0, 0), Point(3, 0), Point(10, 0)],
        }
        right_data = {
            "id": ["east", "west", "near_middle", "remote"],
            "geometry": [
                Point(0.25, 0),
                Point(-0.4, 0),
                Point(3.75, 0),
                Point(20, 0),
            ],
        }

        self.sedona_left = GeoDataFrame(left_data)
        self.sedona_right = GeoDataFrame(right_data)
        self.geopandas_left = gpd.GeoDataFrame(left_data)
        self.geopandas_right = gpd.GeoDataFrame(right_data)

    @staticmethod
    def _normalized_rows(joined):
        if isinstance(joined, GeoDataFrame):
            joined = joined.to_geopandas()

        rows = []
        for row in joined[["id_left", "id_right", "geometry"]].itertuples(index=False):
            right_id = None if pd.isna(row.id_right) else row.id_right
            geometry = None if row.geometry is None else row.geometry.wkb_hex
            rows.append((row.id_left, right_id, geometry))

        return sorted(
            rows,
            key=lambda row: tuple("" if value is None else str(value) for value in row),
        )

    @pytest.mark.parametrize(
        ("distance", "expected_count"),
        [(0.1, 0), (0.5, 2), (1.0, 3)],
    )
    def test_inner_join_matches_geopandas(self, distance, expected_count):
        actual = sjoin(
            self.sedona_left,
            self.sedona_right,
            predicate="dwithin",
            distance=distance,
        )
        expected = gpd.sjoin(
            self.geopandas_left,
            self.geopandas_right,
            predicate="dwithin",
            distance=distance,
        )

        assert type(actual) is GeoDataFrame
        assert set(actual.columns) == set(expected.columns)
        assert len(actual) == expected_count
        assert self._normalized_rows(actual) == self._normalized_rows(expected)

    def test_left_join_preserves_unmatched_rows(self):
        actual = sjoin(
            self.sedona_left,
            self.sedona_right,
            how="left",
            predicate="dwithin",
            distance=0.5,
        )
        expected = gpd.sjoin(
            self.geopandas_left,
            self.geopandas_right,
            how="left",
            predicate="dwithin",
            distance=0.5,
        )

        actual_rows = self._normalized_rows(actual)
        assert actual_rows == self._normalized_rows(expected)
        assert {
            left_id for left_id, right_id, _ in actual_rows if right_id is None
        } == {
            "middle",
            "far",
        }
