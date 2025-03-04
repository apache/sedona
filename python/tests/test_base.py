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
from typing import Iterable, Union


import pyspark
from pyspark.sql import DataFrame

from sedona.spark import *
from sedona.utils.decorators import classproperty

SPARK_REMOTE = os.getenv("SPARK_REMOTE")
EXTRA_JARS = os.getenv("SEDONA_PYTHON_EXTRA_JARS")

from shapely import wkt
from shapely.geometry.base import BaseGeometry


class TestBase:

    @classproperty
    def spark(self):
        if not hasattr(self, "__spark"):
            # This lets a caller override the value of SPARK_HOME to just use whatever
            # is provided by pyspark. Otherwise, export SPARK_HOME="" has no effect.
            if "SPARK_HOME" in os.environ and not os.environ["SPARK_HOME"]:
                del os.environ["SPARK_HOME"]

            builder = SedonaContext.builder().appName("SedonaSparkTest")
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
                    .config(
                        "spark.sedona.stac.load.itemsLimitMax",
                        "20",
                    )
                )
            else:
                builder = builder.master("local[*]").config(
                    "spark.sedona.stac.load.itemsLimitMax",
                    "20",
                )

            # Allows the Sedona .jar to be explicitly set by the caller (e.g, to run
            # pytest against a freshly-built development version of Sedona)
            if EXTRA_JARS:
                builder.config("spark.jars", EXTRA_JARS)

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
    def assert_almost_equal(
        self,
        a: Union[Iterable[float], float],
        b: Union[Iterable[float], float],
        tolerance: float = 0.00001,
    ):
        assert type(a) is type(b)
        if isinstance(a, Iterable):
            assert len(a) == len(b)
            for i in range(len(a)):
                self.assert_almost_equal(a[i], b[i], tolerance)
        elif isinstance(b, float):
            assert abs(a - b) < tolerance
        else:
            raise TypeError("this function is only for floats and iterables of floats")

    @classmethod
    def assert_dataframes_equal(self, df1: DataFrame, df2: DataFrame):
        df_diff1 = df1.exceptAll(df2)
        df_diff2 = df2.exceptAll(df1)

        assert df_diff1.isEmpty and df_diff2.isEmpty

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
