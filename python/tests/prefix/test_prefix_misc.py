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
import os
from unittest import mock

import pytest
from pyspark.sql import functions as F
from tests.test_base import TestBase

from sedona.spark import SedonaUtils
from sedona.spark.sql import st_constructors as stc
from sedona.spark.stac.collection_client import CollectionClient

# The prefix is JVM-wide and fixed by the first Sedona registrator, so these tests need a
# pytest process of their own whose session was built with SEDONA_TEST_FUNCTION_PREFIX.
PREFIX = os.getenv("SEDONA_TEST_FUNCTION_PREFIX")
pytestmark = pytest.mark.skipif(not PREFIX, reason="needs SEDONA_TEST_FUNCTION_PREFIX")


class TestPrefixMisc(TestBase):
    def test_stac_spatial_filter(self):
        df = self.spark.createDataFrame(
            [
                ("inside", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"),
                ("outside", "POLYGON((50 50, 51 50, 51 51, 50 51, 50 50))"),
            ],
            ["id", "wkt"],
        ).select("id", stc.ST_GeomFromText(F.col("wkt")).alias("geometry"))

        by_bbox = CollectionClient._apply_spatial_temporal_filters(
            df, bbox=[[-1.0, -1.0, 2.0, 2.0]]
        )
        assert [row["id"] for row in by_bbox.collect()] == ["inside"]

        by_geometry = CollectionClient._apply_spatial_temporal_filters(
            df, geometry=["POLYGON((-1 -1, 2 -1, 2 2, -1 2, -1 -1))"]
        )
        assert [row["id"] for row in by_geometry.collect()] == ["inside"]

    def test_display_image_on_raster_column(self):
        raster_df = self.spark.range(1).selectExpr(
            f"{PREFIX}RS_MakeEmptyRaster(1, 4, 4, 0.0, 4.0, 1.0) AS raster"
        )
        with mock.patch("IPython.display.display") as display:
            SedonaUtils.display_image(raster_df)
        display.assert_called_once()
        assert "<img" in display.call_args[0][0].data
