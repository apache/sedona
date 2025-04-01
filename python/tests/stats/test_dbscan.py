#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import pyspark.sql.functions as f
import pytest
from sklearn.cluster import DBSCAN as sklearnDBSCAN
from tests.test_base import TestBase

from sedona.sql.st_constructors import ST_MakePoint
from sedona.sql.st_functions import ST_Buffer
from sedona.stats.clustering.dbscan import dbscan


class TestDBScan(TestBase):

    @pytest.fixture
    def sample_data(self):
        return [
            {"id": 1, "x": 1.0, "y": 2.0},
            {"id": 2, "x": 3.0, "y": 4.0},
            {"id": 3, "x": 2.5, "y": 4.0},
            {"id": 4, "x": 1.5, "y": 2.5},
            {"id": 5, "x": 3.0, "y": 5.0},
            {"id": 6, "x": 12.8, "y": 4.5},
            {"id": 7, "x": 2.5, "y": 4.5},
            {"id": 8, "x": 1.2, "y": 2.5},
            {"id": 9, "x": 1.0, "y": 3.0},
            {"id": 10, "x": 1.0, "y": 5.0},
            {"id": 11, "x": 1.0, "y": 2.5},
            {"id": 12, "x": 5.0, "y": 6.0},
            {"id": 13, "x": 4.0, "y": 3.0},
        ]

    @pytest.fixture
    def sample_dataframe(self, sample_data):
        return (
            self.spark.createDataFrame(sample_data)
            .select(ST_MakePoint("x", "y").alias("arealandmark"), "id")
            .repartition(9)
        )

    def get_expected_result(self, input_data, epsilon, min_pts, include_outliers=True):
        labels = (
            sklearnDBSCAN(eps=epsilon, min_samples=min_pts)
            .fit([[datum["x"], datum["y"]] for datum in input_data])
            .labels_
        )
        expected = [(x[0] + 1, x[1]) for x in list(enumerate(labels))]
        clusters = [x for x in set(labels) if (x != -1 or include_outliers)]
        cluster_members = {
            frozenset([y[0] for y in expected if y[1] == x]) for x in clusters
        }
        return cluster_members

    def get_actual_results(
        self,
        input_data,
        epsilon,
        min_pts,
        geometry=None,
        id=None,
        include_outliers=True,
    ):
        result = dbscan(
            input_data, epsilon, min_pts, geometry, include_outliers=include_outliers
        )

        result.show()

        id = id or "id"
        clusters_members = [
            (x[id], x.cluster)
            for x in result.collect()
            if x.cluster != -1 or include_outliers
        ]

        result.unpersist()

        clusters = {
            frozenset([y[0] for y in clusters_members if y[1] == x])
            for x in {y[1] for y in clusters_members}
        }

        return clusters

    @pytest.mark.parametrize("epsilon", [0.6, 0.7, 0.8])
    @pytest.mark.parametrize("min_pts", [3, 4, 5])
    def test_dbscan_valid_parameters(
        self, sample_data, sample_dataframe, epsilon, min_pts
    ):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        assert self.get_expected_result(
            sample_data, epsilon, min_pts
        ) == self.get_actual_results(sample_dataframe, epsilon, min_pts)

    def test_dbscan_valid_parameters_default_column_name(
        self, sample_data, sample_dataframe
    ):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        df = sample_dataframe.select(
            "id", f.col("arealandmark").alias("geometryFieldName")
        )
        epsilon = 0.6
        min_pts = 4

        assert self.get_expected_result(
            sample_data, epsilon, min_pts
        ) == self.get_actual_results(df, epsilon, min_pts)

    def test_dbscan_valid_parameters_polygons(self, sample_data, sample_dataframe):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        df = sample_dataframe.select(
            "id", ST_Buffer(f.col("arealandmark"), 0.000001).alias("geometryFieldName")
        )
        epsilon = 0.6
        min_pts = 4

        assert self.get_expected_result(
            sample_data, epsilon, min_pts
        ) == self.get_actual_results(df, epsilon, min_pts)

    def test_dbscan_supports_other_distance_function(self, sample_dataframe):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        df = sample_dataframe.select(
            "id", ST_Buffer(f.col("arealandmark"), 0.000001).alias("geometryFieldName")
        )
        epsilon = 0.6
        min_pts = 4

        dbscan(
            df,
            epsilon,
            min_pts,
            "geometryFieldName",
            use_spheroid=True,
        )

    def test_dbscan_invalid_epsilon(self, sample_dataframe):
        with pytest.raises(Exception):
            dbscan(sample_dataframe, -0.1, 5, "arealandmark")

    def test_dbscan_invalid_min_pts(self, sample_dataframe):
        with pytest.raises(Exception):
            dbscan(sample_dataframe, 0.1, -5, "arealandmark")

    def test_dbscan_invalid_geometry_column(self, sample_dataframe):
        with pytest.raises(Exception):
            dbscan(sample_dataframe, 0.1, 5, "invalid_column")

    def test_return_empty_df_when_no_clusters(self, sample_data, sample_dataframe):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        epsilon = 0.1
        min_pts = 10000

        assert (
            dbscan(
                sample_dataframe,
                epsilon,
                min_pts,
                "arealandmark",
                include_outliers=False,
            ).count()
            == 0
        )
        # picked some coefficient we know yields clusters and thus hit the happy case
        assert (
            dbscan(
                sample_dataframe,
                epsilon,
                min_pts,
                "arealandmark",
                include_outliers=False,
            ).schema
            == dbscan(sample_dataframe, 0.6, 3, "arealandmark").schema
        )

    def test_dbscan_doesnt_duplicate_border_points_in_two_clusters(self):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        input_df = self.spark.createDataFrame(
            [
                {"id": 10, "x": 1.0, "y": 1.8},
                {"id": 11, "x": 1.0, "y": 1.9},
                {"id": 12, "x": 1.0, "y": 2.0},
                {"id": 13, "x": 1.0, "y": 2.1},
                {"id": 14, "x": 2.0, "y": 2.0},
                {"id": 15, "x": 3.0, "y": 1.9},
                {"id": 16, "x": 3.0, "y": 2.0},
                {"id": 17, "x": 3.0, "y": 2.1},
                {"id": 18, "x": 3.0, "y": 2.2},
            ]
        ).select(ST_MakePoint("x", "y").alias("geometry"), "id")

        # make sure no id occurs more than once
        output_df = dbscan(input_df, 1.0, 4)

        assert output_df.count() == 9
        assert output_df.select("cluster").distinct().count() == 2

    def test_return_outliers_false_doesnt_return_outliers(
        self, sample_data, sample_dataframe
    ):
        # repeated broadcast joins with this small data size use a lot of RAM on broadcast references
        self.spark.conf.set("sedona.join.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        epsilon = 0.6
        min_pts = 3

        assert self.get_expected_result(
            sample_data, epsilon, min_pts, include_outliers=False
        ) == self.get_actual_results(
            sample_dataframe, epsilon, min_pts, include_outliers=False
        )
