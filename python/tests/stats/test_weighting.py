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
from tests.test_base import TestBase

from sedona.sql.st_constructors import ST_MakePoint
from sedona.stats.weighting import (
    add_binary_distance_band_column,
    add_distance_band_column,
)


class TestWeighting(TestBase):
    def get_dataframe(self):
        data = [[0, 1, 1], [1, 1, 2]]

        return (
            self.spark.createDataFrame(data)
            .select(ST_MakePoint("_1", "_2").alias("geometry"))
            .withColumn("anotherColumn", f.rand())
        )

    def test_calling_weighting_works(self):
        df = self.get_dataframe()
        add_distance_band_column(df, 1.0)

    def test_calling_binary_weighting_matches_expected(self):
        df = self.get_dataframe()
        self.assert_dataframes_equal(
            add_distance_band_column(
                df, 1.0, binary=True, include_zero_distance_neighbors=True
            ),
            add_binary_distance_band_column(df, 1.0),
        )
