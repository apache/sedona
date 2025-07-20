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

from sedona.spark.stats.autocorrelation.moran import Moran
from sedona.spark.stats.weighting import add_binary_distance_band_column
from tests.test_base import TestBase


class TestMoran(TestBase):

    data = [
        (1, 1.0, 1.0, 8.5),
        (2, 1.5, 1.2, 8.2),
        (3, 1.3, 1.8, 8.7),
        (4, 1.7, 1.6, 7.9),
        (5, 4.0, 1.5, 6.2),
        (6, 4.2, 1.7, 6.5),
        (7, 4.5, 1.3, 5.9),
        (8, 4.7, 1.8, 6.0),
        (9, 1.8, 4.3, 3.1),
        (10, 1.5, 4.5, 3.4),
        (11, 1.2, 4.7, 3.0),
        (12, 1.6, 4.2, 3.3),
        (13, 1.9, 4.8, 2.8),
        (14, 4.3, 4.2, 1.2),
        (15, 4.5, 4.5, 1.5),
        (16, 4.7, 4.8, 1.0),
        (17, 4.1, 4.6, 1.3),
        (18, 4.8, 4.3, 1.1),
        (19, 4.2, 4.9, 1.4),
        (20, 4.6, 4.1, 1.6),
    ]

    def test_moran_integration(self):
        df = (
            self.spark.createDataFrame(self.data)
            .selectExpr("_1 as id", "_2 AS x", "_3 AS y", "_4 AS value")
            .selectExpr("id", "ST_MakePoint(x, y) AS geometry", "value")
        )

        result = add_binary_distance_band_column(
            df, 1.0, saved_attributes=["id", "value"]
        )

        moran_i_result = Moran.get_global(result)

        assert 0 < moran_i_result.p_norm < 0.00001
        assert 0.9614 < moran_i_result.i < 0.9615
        assert 7.7523 < moran_i_result.z_norm < 7.7524

        two_tailed_result = Moran.get_global(result, False)
        assert 0 < two_tailed_result.p_norm < 0.00001
        assert 0.9614 < two_tailed_result.i < 0.9615
        assert 7.7523 < two_tailed_result.z_norm < 7.7524

    def test_moran_with_different_column_names(self):
        df = (
            self.spark.createDataFrame(self.data)
            .selectExpr("_1 as index", "_2 AS x", "_3 AS y", "_4 AS feature_value")
            .selectExpr("index", "ST_MakePoint(x, y) AS geometry", "feature_value")
        )

        result = add_binary_distance_band_column(
            df, threshold=1.0, saved_attributes=["index", "feature_value"]
        )

        moran_i_result = Moran.get_global(
            result, id_column="index", value_column="feature_value"
        )

        assert 0 < moran_i_result.p_norm < 0.00001
        assert 0.9614 < moran_i_result.i < 0.9615
        assert 7.7523 < moran_i_result.z_norm < 7.7524

        two_tailed_result = Moran.get_global(
            result, id_column="index", value_column="feature_value", two_tailed=False
        )

        assert 0 < two_tailed_result.p_norm < 0.00001
        assert 0.9614 < two_tailed_result.i < 0.9615
        assert 7.7523 < two_tailed_result.z_norm < 7.7524
