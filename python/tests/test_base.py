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
import os
from tempfile import mkdtemp
from typing import Union

import pyspark
from sedona.spark import *
from sedona.utils.decorators import classproperty

SPARK_REMOTE = os.getenv("SPARK_REMOTE")

from shapely import wkt
from shapely.geometry.base import BaseGeometry


class TestBase:

    @classproperty
    def spark(self):
        if not hasattr(self, "__spark"):

            builder = SedonaContext.builder()
            if SPARK_REMOTE:
                builder = (
                    builder.remote(SPARK_REMOTE)
                    .config(
                        "spark.jars.packages",
                        f"org.apache.spark:spark-connect_2.12:{pyspark.__version__}",
                    )
                    .config(
                        "spark.sql.extensions",
                        "org.apache.sedona.sql.SedonaSqlExtensions",
                    )
                )
            else:
                builder = builder.master("local[*]")

            spark = SedonaContext.create(builder.getOrCreate())

            if not SPARK_REMOTE:
                spark.sparkContext.setCheckpointDir(mkdtemp())

            setattr(self, "__spark", spark)
        return getattr(self, "__spark")

    @classproperty
    def sc(self):
        if not hasattr(self, "__spark"):
            setattr(self, "__sc", self.spark._sc)
        return getattr(self, "__sc")

    @classmethod
    def assert_geometry_almost_equal(
        cls,
        left_geom: Union[str, BaseGeometry],
        right_geom: Union[str, BaseGeometry],
        tolerance=1e-6,
    ):
        expected_geom = (
            wkt.loads(left_geom) if isinstance(left_geom, str) else left_geom
        )
        actual_geom = (
            wkt.loads(right_geom) if isinstance(right_geom, str) else right_geom
        )

        if not actual_geom.equals_exact(expected_geom, tolerance=tolerance):
            # If the exact equals check fails, perform a buffer check with tolerance
            if actual_geom.buffer(tolerance).contains(
                expected_geom
            ) and expected_geom.buffer(tolerance).contains(actual_geom):
                return
            else:
                # fail the test with error message
                raise ValueError(
                    f"Geometry equality check failed for {left_geom} and {right_geom}"
                )
