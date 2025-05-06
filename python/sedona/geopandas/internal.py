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

import pandas as pd
from pyspark._typing import F
from pyspark.pandas.internal import InternalFrame as InternalPySparkFrame


class InternalGeoFrame(InternalPySparkFrame):

    @staticmethod
    def from_pandas(pdf: pd.DataFrame) -> "InternalGeoFrame":
        internal_frame = InternalPySparkFrame.from_pandas(pdf)
        sdf = internal_frame.spark_frame.withColumn("geometry", F.lit(None))
        return InternalGeoFrame(
            spark_frame=sdf,
            index_spark_columns=internal_frame.index_spark_columns,
            data_spark_columns=internal_frame.data_spark_columns,
        )
