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

"""DBSCAN is a popular clustering algorithm for spatial data.

It identifies groups of data where enough records are close enough to each other. This implementation leverages spark,
sedona and graphframes to support large scale datasets and various, heterogeneous geometric feature types.
"""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

ID_COLUMN_NAME = "__id"


def dbscan(
    dataframe: DataFrame,
    epsilon: float,
    min_pts: int,
    geometry: Optional[str] = None,
    include_outliers: bool = True,
    use_spheroid=False,
    is_core_column_name="isCore",
    cluster_column_name="cluster",
):
    """Annotates a dataframe with a cluster label for each data record using the DBSCAN algorithm.

    The dataframe should contain at least one GeometryType column. Rows must be unique. If one geometry column is
    present it will be used automatically. If two are present, the one named 'geometry' will be used. If more than one
    are present and neither is named 'geometry', the column name must be provided.

    Args:
        dataframe: spark dataframe containing the geometries
        epsilon: minimum distance parameter of DBSCAN algorithm
        min_pts: minimum number of points parameter of DBSCAN algorithm
        geometry: name of the geometry column
        include_outliers: whether to return outlier points. If True, outliers are returned with a cluster value of -1.
            Default is False
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. Default is false
        is_core_column_name: what the name of the column indicating if this is a core point should be. Default is "isCore"
        cluster_column_name: what the name of the column indicating the cluster id should be. Default is "cluster"

    Returns:
        A PySpark DataFrame containing the cluster label for each row
    """
    sedona = SparkSession.getActiveSession()

    result_df = sedona._jvm.org.apache.sedona.stats.clustering.DBSCAN.dbscan(
        dataframe._jdf,
        float(epsilon),
        min_pts,
        geometry,
        include_outliers,
        use_spheroid,
        is_core_column_name,
        cluster_column_name,
    )

    return DataFrame(result_df, sedona)
