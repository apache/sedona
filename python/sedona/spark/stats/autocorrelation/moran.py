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
    """Result of the global Moran's I test.

    Attributes:
        i: Moran's I. Values near 1 mean that similar values are close to each other, values near -1
            mean that neighbors tend to have dissimilar values, and values near 0 mean that the values
            are randomly distributed.
        p_norm: p-value of ``z_norm`` under the normality assumption. It is two-tailed unless
            ``Moran.get_global`` was called with ``two_tailed=False``.
        z_norm: z-score of ``i`` under the normality assumption.
    """

    i: float
    p_norm: float
    z_norm: float


class Moran:
    """Global Moran's I spatial autocorrelation test."""

    @staticmethod
    def get_global(
        df: DataFrame,
        two_tailed: bool = True,
        id_column: str = "id",
        value_column: str = "value",
    ) -> MoranResult:
        """Computes the global Moran's I of a value column.

        The dataframe needs an id column that uniquely identifies each row, a numeric value column, and a
        column named ``weights`` that lists each row's neighbors and their weights, such as the one added by
        ``sedona.spark.stats.weighting.add_distance_band_column``. Each neighbor must include the id and value
        columns, so pass them to the weighting function in ``saved_attributes``.

        Args:
            df: dataframe with the id, value, and weights columns
            two_tailed: whether to return a two-tailed p-value. Default is True
            id_column: name of the column that identifies each row. Default is "id"
            value_column: name of the numeric column to test. Default is "value"

        Returns:
            A MoranResult with Moran's I and its z-score and p-value under the normality assumption.
        """
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
