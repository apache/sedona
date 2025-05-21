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

import pytest
from tests.test_base import TestBase


class TestSpider(TestBase):
    def test_spider_uniform(self):
        df = self.spark.read.format("spider").load(
            n=1000,
            distribution="uniform",
            geometryType="box",
            maxWidth=0.03,
            maxHeight=0.02,
            translateX=0.5,
            translateY=0.5,
            scaleX=2,
            scaleY=2,
        )
        assert df.count() == 1000

        # Convert geometries to coordinates for bound checking
        bounds_df = df.selectExpr(
            "ST_XMin(geometry) as min_x",
            "ST_XMax(geometry) as max_x",
            "ST_YMin(geometry) as min_y",
            "ST_YMax(geometry) as max_y",
        ).collect()[0]

        # Check geometry bounds (should be within [0.5, 2.5] * [0.5, 2.5] with some tolerance)
        assert bounds_df.min_x >= 0.4
        assert bounds_df.max_x <= 2.6
        assert bounds_df.min_y >= 0.4
        assert bounds_df.max_y <= 2.6

        # Check box dimensions
        dimensions_df = df.selectExpr(
            "ST_XMax(geometry) - ST_XMin(geometry) as width",
            "ST_YMax(geometry) - ST_YMin(geometry) as height",
        ).collect()

        for row in dimensions_df:
            assert row.width <= 0.06, f"Box width {row.width} exceeds maximum 0.06"
            assert row.height <= 0.04, f"Box height {row.height} exceeds maximum 0.04"

        # Check mean width and height
        mean_width = (
            df.selectExpr("AVG(ST_XMax(geometry) - ST_XMin(geometry)) as mean_width")
            .collect()[0]
            .mean_width
        )
        mean_height = (
            df.selectExpr("AVG(ST_YMax(geometry) - ST_YMin(geometry)) as mean_height")
            .collect()[0]
            .mean_height
        )
        assert abs(mean_width - 0.03) < 0.005
        assert abs(mean_height - 0.02) < 0.005
