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

"""Functions related to calculating the local outlier factor of a dataset."""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

ID_COLUMN_NAME = "__id"
CONTENTS_COLUMN_NAME = "__contents"


def local_outlier_factor(
    dataframe: DataFrame,
    k: int = 20,
    geometry: Optional[str] = None,
    handle_ties: bool = False,
    use_spheroid=False,
    result_column_name: str = "lof",
):
    """Annotates a dataframe with a column containing the local outlier factor for each data record.

    The dataframe should contain at least one GeometryType column. Rows must be unique. If one geometry column is
    present it will be used automatically. If two are present, the one named 'geometry' will be used. If more than one
    are present and neither is named 'geometry', the column name must be provided.

    Args:
        dataframe: apache sedona idDataframe containing the point geometries
        k: number of nearest neighbors that will be considered for the LOF calculation
        geometry: name of the geometry column
        handle_ties: whether to handle ties in the k-distance calculation. Default is false
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. Default is false
        result_column_name: the name of the column containing the lof for each row. Default is "lof"

    Returns:
        A PySpark DataFrame containing the lof for each row
    """
    sedona = SparkSession.getActiveSession()

    result_df = sedona._jvm.org.apache.sedona.stats.outlierDetection.LocalOutlierFactor.localOutlierFactor(
        dataframe._jdf,
        k,
        geometry,
        handle_ties,
        use_spheroid,
        result_column_name,
    )

    return DataFrame(result_df, sedona)
