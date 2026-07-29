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

import pyspark
import pytest
from packaging.version import parse as parse_version

from tests.test_base import TestBase


@pytest.mark.skipif(
    os.getenv("SPARK_REMOTE") is None
    or parse_version(pyspark.__version__) < parse_version("3.5.0"),
    reason="requires pandas-on-Spark running through Spark Connect 3.5+",
)
class TestGeoPandasSparkConnect(TestBase):
    def test_sample_points_stateful_distributed_size(self, capsys):
        # Configure the remote session before importing pandas-on-Spark, whose
        # global options otherwise create an unconfigured default session.
        spark = self.spark

        import numpy as np
        from pyspark.sql import functions as F
        from pyspark.sql.utils import is_remote

        from sedona.spark.geopandas import GeoSeries
        from sedona.spark.sql import st_constructors as stc

        assert is_remote()

        geometry_frame = (
            spark.range(0, 4, 1, 2)
            .select(
                F.col("id").alias("feature_id"),
                stc.ST_GeomFromWKT(F.lit("LINESTRING (0 0, 4 0)")).alias("geometry"),
            )
            .pandas_api(index_col="feature_id")
        )
        size_frame = (
            spark.range(0, 4, 1, 2)
            .select(
                F.col("id").alias("feature_id"),
                (F.col("id") + F.lit(1)).cast("int").alias("size"),
            )
            .pandas_api(index_col="feature_id")
        )

        sampled = GeoSeries(geometry_frame["geometry"]).sample_points(
            size_frame["size"],
            rng=np.random.default_rng(7),
        )
        sampled._internal.spark_frame.explain(mode="simple")
        captured = capsys.readouterr()
        assert "AttachDistributedSequence" in captured.out + captured.err

        actual = sampled.to_geopandas().sort_index()

        assert actual.index.tolist() == [0, 1, 2, 3]
        assert actual.index.name == "feature_id"
        assert [geometry.geom_type for geometry in actual] == ["MultiPoint"] * 4
        assert [len(geometry.geoms) for geometry in actual] == [1, 2, 3, 4]
