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
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


@dataclass
class MoranResult:
    i: float
    p_norm: float
    z_norm: float


class Moran:

    @staticmethod
    def get_global(
        df: DataFrame,
        two_tailed: bool = True,
        id_column: str = "id",
        value_column: str = "value",
    ) -> MoranResult:
        sedona = SparkSession.getActiveSession()

        _jvm = sedona._jvm
        moran_result = (
            sedona._jvm.org.apache.sedona.stats.autocorrelation.Moran.getGlobal(
                df._jdf, two_tailed, id_column, value_column
            )
        )

        return MoranResult(
            i=moran_result.getI(),
            p_norm=moran_result.getPNorm(),
            z_norm=moran_result.getZNorm(),
        )
