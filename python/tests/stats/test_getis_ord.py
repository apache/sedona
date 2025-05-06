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

from esda.getisord import G_Local
from libpysal.weights import DistanceBand
from pyspark.sql import functions as f

from sedona.spark.stats import g_local
from sedona.spark.stats import add_distance_band_column
from tests.test_base import TestBase

from sedona.spark.sql.st_constructors import ST_MakePoint


class TestGetisOrd(TestBase):
    def get_data(self):
        return [
            {"id": 0, "x": 2.0, "y": 2.0, "val": 0.9},
            {"id": 1, "x": 2.0, "y": 3.0, "val": 1.2},
            {"id": 2, "x": 3.0, "y": 3.0, "val": 1.2},
            {"id": 3, "x": 3.0, "y": 2.0, "val": 1.2},
            {"id": 4, "x": 3.0, "y": 1.0, "val": 1.2},
            {"id": 5, "x": 2.0, "y": 1.0, "val": 2.2},
            {"id": 6, "x": 1.0, "y": 1.0, "val": 1.2},
            {"id": 7, "x": 1.0, "y": 2.0, "val": 0.2},
            {"id": 8, "x": 1.0, "y": 3.0, "val": 1.2},
            {"id": 9, "x": 0.0, "y": 2.0, "val": 1.0},
            {"id": 10, "x": 4.0, "y": 2.0, "val": 1.2},
        ]

    def get_dataframe(self):
        return self.spark.createDataFrame(self.get_data()).select(
            ST_MakePoint("x", "y").alias("geometry"), "id", "val"
        )

    def test_gi_results_match_pysal(self):
        # actual
        input_dataframe = add_distance_band_column(self.get_dataframe(), 1.0)
        actual_df = g_local(input_dataframe, "val", "weights")

        # expected_results
        data = self.get_data()
        points = [(datum["x"], datum["y"]) for datum in data]
        w = DistanceBand(points, threshold=1.0)
        y = [datum["val"] for datum in data]
        expected_data = G_Local(y, w, transform="B")

        # assert
        actuals = actual_df.orderBy(f.col("id").asc()).collect()
        self.assert_almost_equal(expected_data.Gs.tolist(), [row.G for row in actuals])
        self.assert_almost_equal(
            expected_data.EGs.tolist(), [row.EG for row in actuals]
        )
        self.assert_almost_equal(
            expected_data.VGs.tolist(), [row.VG for row in actuals]
        )
        self.assert_almost_equal(expected_data.Zs.tolist(), [row.Z for row in actuals])
        self.assert_almost_equal(
            expected_data.p_norm.tolist(), [row.P for row in actuals]
        )

    def test_gistar_results_match_pysal(self):
        # actual
        input_dataframe = add_distance_band_column(
            self.get_dataframe(), 1.0, include_self=True
        )
        actual_df = g_local(input_dataframe, "val", "weights", star=True)

        # expected_results
        data = self.get_data()
        points = [(datum["x"], datum["y"]) for datum in data]
        w = DistanceBand(points, threshold=1.0)
        y = [datum["val"] for datum in data]
        expected_data = G_Local(y, w, transform="B", star=True)

        # assert
        actuals = actual_df.orderBy(f.col("id").asc()).collect()
        self.assert_almost_equal(expected_data.Gs.tolist(), [row.G for row in actuals])
        self.assert_almost_equal(
            expected_data.EGs.tolist(), [row.EG for row in actuals]
        )
        self.assert_almost_equal(
            expected_data.VGs.tolist(), [row.VG for row in actuals]
        )
        self.assert_almost_equal(expected_data.Zs.tolist(), [row.Z for row in actuals])
        self.assert_almost_equal(
            expected_data.p_norm.tolist(), [row.P for row in actuals]
        )

    def test_gi_results_match_pysal_nb(self):
        # actual
        input_dataframe = add_distance_band_column(
            self.get_dataframe(), 1.0, binary=False
        )
        actual_df = g_local(input_dataframe, "val", "weights")

        # expected_results
        data = self.get_data()
        points = [(datum["x"], datum["y"]) for datum in data]
        w = DistanceBand(points, threshold=1.0, binary=False)
        y = [datum["val"] for datum in data]
        expected_data = G_Local(y, w, transform="B")

        # assert
        actuals = actual_df.orderBy(f.col("id").asc()).collect()
        self.assert_almost_equal(expected_data.Gs.tolist(), [row.G for row in actuals])
        self.assert_almost_equal(
            expected_data.EGs.tolist(), [row.EG for row in actuals]
        )
        self.assert_almost_equal(
            expected_data.VGs.tolist(), [row.VG for row in actuals]
        )
        self.assert_almost_equal(expected_data.Zs.tolist(), [row.Z for row in actuals])
        self.assert_almost_equal(
            expected_data.p_norm.tolist(), [row.P for row in actuals]
        )

    def test_gistar_results_match_pysal_nb(self):
        # actual
        input_dataframe = add_distance_band_column(
            self.get_dataframe(), 1.0, include_self=True, binary=False
        )
        actual_df = g_local(input_dataframe, "val", "weights", star=True)

        # expected_results
        data = self.get_data()
        points = [(datum["x"], datum["y"]) for datum in data]
        w = DistanceBand(points, threshold=1.0, binary=False)
        y = [datum["val"] for datum in data]
        expected_data = G_Local(y, w, transform="B", star=True)

        # assert
        actuals = actual_df.orderBy(f.col("id").asc()).collect()
        self.assert_almost_equal(expected_data.Gs.tolist(), [row.G for row in actuals])
        self.assert_almost_equal(
            expected_data.EGs.tolist(), [row.EG for row in actuals]
        )
        self.assert_almost_equal(
            expected_data.VGs.tolist(), [row.VG for row in actuals]
        )
        self.assert_almost_equal(expected_data.Zs.tolist(), [row.Z for row in actuals])
        self.assert_almost_equal(
            expected_data.p_norm.tolist(), [row.P for row in actuals]
        )
