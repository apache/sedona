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

"""Getis Ord functions. From the 1992 paper by Getis & Ord.

Getis, A., & Ord, J. K. (1992). The analysis of spatial association by use of distance statistics.
Geographical Analysis, 24(3), 189-206. https://doi.org/10.1111/j.1538-4632.1992.tb00261.x
"""

from pyspark.sql import Column, DataFrame, SparkSession

# todo change weights and x type to string


def g_local(
    dataframe: DataFrame,
    x: str,
    weights: str = "weights",
    permutations: int = 0,
    star: bool = False,
    island_weight: float = 0.0,
) -> DataFrame:
    """Performs the Gi or Gi* statistic on the x column of the dataframe.

    Weights should be the neighbors of this row. The members of the weights should be comprised of structs containing a
    value column and a neighbor column. The neighbor column should be the contents of the neighbors with the same types
    as the parent row (minus neighbors). You can use `wherobots.weighing.add_distance_band_column` to achieve this. To
    calculate the Gi* statistic, ensure the focal observation is in the neighbors array (i.e. the row is in the weights
    column) and `star=true`. Significance is calculated with a z score. Permutation tests are not yet implemented and
    thus island weight does nothing. The following columns will be added: G, E[G], V[G], Z, P.

    Args:
        dataframe: the dataframe to perform the G statistic on
        x: The column name we want to perform hotspot analysis on
        weights: The column name containing the neighbors array. The neighbor column should be the contents of
            the neighbors with the same types as the parent row (minus neighbors). You can use
            `wherobots.weighing.add_distance_band_column` to achieve this.
        permutations: Not used. Permutation tests are not supported yet. The number of permutations to use for the
            significance test.
        star: Whether the focal observation is in the neighbors array. If true this calculates Gi*, otherwise Gi
        island_weight: Not used. The weight for the simulated neighbor used for records without a neighbor in perm tests
    Returns:
        A dataframe with the original columns plus the columns G, E[G], V[G], Z, P.
    """
    sedona = SparkSession.getActiveSession()

    result_df = sedona._jvm.org.apache.sedona.stats.hotspotDetection.GetisOrd.gLocal(
        dataframe, x, weights, permutations, star, island_weight
    )

    return DataFrame(result_df, sedona)
