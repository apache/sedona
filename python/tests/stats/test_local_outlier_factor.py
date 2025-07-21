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

import numpy as np
import pyspark.sql.functions as f
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
from sklearn.neighbors import LocalOutlierFactor
from tests.test_base import TestBase

from sedona.spark.sql.st_constructors import ST_MakePoint
from sedona.spark.sql.st_functions import ST_X, ST_Y
from sedona.spark.stats import local_outlier_factor
import os
import pyspark


@pytest.mark.skipif(
    os.getenv("SPARK_REMOTE") is not None and pyspark.__version__ < "4.0.0",
    reason="DBSCAN requires checkpoint directory which is not available in Spark Connect mode before Spark 4.0.0",
)
class TestLOF(TestBase):
    def get_small_data(self) -> DataFrame:
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("x", DoubleType(), True),
                StructField("y", DoubleType(), True),
            ]
        )
        return self.spark.createDataFrame(
            [
                (1, 1.0, 2.0),
                (2, 2.0, 2.0),
                (3, 3.0, 3.0),
            ],
            schema,
        ).select("id", ST_MakePoint("x", "y").alias("geometry"))

    def get_medium_data(self):
        np.random.seed(42)

        X_inliers = 0.3 * np.random.randn(100, 2)
        X_inliers = np.r_[X_inliers + 2, X_inliers - 2]
        X_outliers = np.random.uniform(low=-4, high=4, size=(20, 2))
        return np.r_[X_inliers, X_outliers]

    def get_medium_dataframe(self, data):
        schema = StructType(
            [StructField("x", DoubleType(), True), StructField("y", DoubleType(), True)]
        )

        return (
            self.spark.createDataFrame(data, schema)
            .select(ST_MakePoint("x", "y").alias("geometry"))
            .withColumn("anotherColumn", f.rand())
        )

    def compare_results(self, actual, expected, k):
        assert len(actual) == len(expected)
        missing = set(expected.keys()) - set(actual.keys())
        assert len(missing) == 0
        big_diff = {
            k: (v, expected[k], abs(1 - v / expected[k]))
            for k, v in actual.items()
            if abs(1 - v / expected[k]) > 0.0000000001
        }
        assert len(big_diff) == 0

    @pytest.mark.parametrize("k", [5, 21, 3])
    def test_lof_matches_sklearn(self, k):
        data = self.get_medium_data()
        actual = {
            tuple(x[0]): x[1]
            for x in local_outlier_factor(self.get_medium_dataframe(data.tolist()), k)
            .select(f.array(ST_X("geometry"), ST_Y("geometry")), "lof")
            .collect()
        }
        clf = LocalOutlierFactor(n_neighbors=k, contamination="auto")
        clf.fit_predict(data)
        expected = dict(
            zip(
                [tuple(x) for x in data],
                [float(-x) for x in clf.negative_outlier_factor_],
            )
        )
        self.compare_results(actual, expected, k)

    # TODO uncomment when KNN join supports empty dfs
    # def test_handle_empty_dataframe(self):
    #     empty_df = self.spark.createDataFrame([], self.get_small_data().schema)
    #     result_df = local_outlier_factor(empty_df, 2)
    #
    #     assert 0 == result_df.count()

    def test_raise_error_for_invalid_k_value(self):
        with pytest.raises(Exception):
            local_outlier_factor(self.get_small_data(), -1)
