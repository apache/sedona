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

import pytest

from tests.test_base import TestBase
from pyspark.sql.functions import expr, pandas_udf
from pyspark.sql.types import IntegerType
import pyspark
import pandas as pd
import numpy as np
import rasterio

from tests import world_map_raster_input_location

class TestRasterPandasUDF(TestBase):
    @pytest.mark.skipif(pyspark.__version__ < '3.4', reason="requires Spark 3.4 or higher")
    def test_raster_as_param(self):
        spark = TestRasterPandasUDF.spark
        df = spark.range(10).withColumn("rast", expr("RS_MakeRasterForTesting(1, 'I', 'PixelInterleavedSampleModel', 4, 3, 100, 100, 10, -10, 0, 0, 3857)"))

        # A Python Pandas UDF that takes a raster as input
        @pandas_udf(IntegerType())
        def pandas_udf_raster_as_param(s: pd.Series) -> pd.Series:
            from sedona.raster import raster_serde

            def func(x):
                with raster_serde.deserialize(x) as raster:
                    arr = raster.as_numpy()
                    return int(np.sum(arr))

            return s.apply(func)

        # A Python Pandas UDF that takes a raster as input
        @pandas_udf(IntegerType())
        def pandas_udf_raster_as_param_2(s: pd.Series) -> pd.Series:
            from sedona.raster import raster_serde

            def func(x):
                with raster_serde.deserialize(x) as raster:
                    ds = raster.as_rasterio()
                    return int(np.sum(ds.read(1)))

            # wrap s.apply() with a rasterio env to get rid of the overhead of repeated
            # env initialization in as_rasterio()
            with rasterio.Env():
                return s.apply(func)

        spark.udf.register("pandas_udf_raster_as_param", pandas_udf_raster_as_param)
        spark.udf.register("pandas_udf_raster_as_param_2", pandas_udf_raster_as_param_2)

        df_result = df.selectExpr("pandas_udf_raster_as_param(rast) as res")
        rows = df_result.collect()
        assert len(rows) == 10
        for row in rows:
            assert row['res'] == 66

        df_result = df.selectExpr("pandas_udf_raster_as_param_2(rast) as res")
        rows = df_result.collect()
        assert len(rows) == 10
        for row in rows:
            assert row['res'] == 66
