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

"""Weighting functions for spatial data."""

from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession


def add_distance_band_column(
    dataframe: DataFrame,
    threshold: float,
    binary: bool = True,
    alpha: float = -1.0,
    include_zero_distance_neighbors: bool = False,
    include_self: bool = False,
    self_weight: float = 1.0,
    geometry: Optional[str] = None,
    use_spheroid: bool = False,
    saved_attributes: List[str] = None,
    result_name: str = "weights",
) -> DataFrame:
    """Annotates a dataframe with a weights column containing the other records within the threshold and their weight.

    The dataframe should contain at least one GeometryType column. Rows must be unique. If one
    geometry column is present it will be used automatically. If two are present, the one named
    'geometry' will be used. If more than one are present and neither is named 'geometry', the
    column name must be provided. The new column will be named 'cluster'.

    Args:
        dataframe: DataFrame with geometry column
        threshold: Distance threshold for considering neighbors
        binary: whether to use binary weights or inverse distance weights for neighbors (dist^alpha)
        alpha: alpha to use for inverse distance weights ignored when binary is true
        include_zero_distance_neighbors: whether to include neighbors that are 0 distance. If 0 distance neighbors are
            included and binary is false, values are infinity as per the floating point spec (divide by 0)
        include_self: whether to include self in the list of neighbors
        self_weight: the value to use for the self weight
        geometry: name of the geometry column
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. Default is false
        saved_attributes: the attributes to save in the neighbor column. Default is all columns.
        result_name: the name of the resulting column. Default is 'weights'.
    Returns:
        The input DataFrame with a weight column added containing neighbors and their weights added to each row.

    """
    sedona = SparkSession.getActiveSession()
    return sedona._jvm.org.apache.sedona.stats.Weighting.addDistanceBandColumn(
        dataframe._jdf,
        float(threshold),
        binary,
        float(alpha),
        include_zero_distance_neighbors,
        include_self,
        float(self_weight),
        geometry,
        use_spheroid,
        saved_attributes,
        result_name,
    )


def add_binary_distance_band_column(
    dataframe: DataFrame,
    threshold: float,
    include_zero_distance_neighbors: bool = True,
    include_self: bool = False,
    geometry: Optional[str] = None,
    use_spheroid: bool = False,
    saved_attributes: List[str] = None,
    result_name: str = "weights",
) -> DataFrame:
    """Annotates a dataframe with a weights column containing the other records within the threshold and their weight.

    Weights will always be 1.0. The dataframe should contain at least one GeometryType column. Rows must be unique. If
    one geometry column is present it will be used automatically. If two are present, the one named 'geometry' will be
    used. If more than one are present and neither is named 'geometry', the column name must be provided. The new column
    will be named 'cluster'.

    Args:
        dataframe: DataFrame with geometry column
        threshold: Distance threshold for considering neighbors
        include_zero_distance_neighbors: whether to include neighbors that are 0 distance. If 0 distance neighbors are
            included and binary is false, values are infinity as per the floating point spec (divide by 0)
        include_self: whether to include self in the list of neighbors
        geometry: name of the geometry column
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. Default is false
        saved_attributes: the attributes to save in the neighbor column. Default is all columns.
        result_name: the name of the resulting column. Default is 'weights'.

    Returns:
        The input DataFrame with a weight column added containing neighbors and their weights (always 1) added to each
        row.

    """
    sedona = SparkSession.getActiveSession()

    return sedona._jvm.org.apache.sedona.stats.Weighting.addBinaryDistanceBandColumn(
        dataframe._jdf,
        float(threshold),
        include_zero_distance_neighbors,
        include_self,
        geometry,
        use_spheroid,
        saved_attributes,
        result_name,
    )


def add_weighted_distance_band_column(
    dataframe: DataFrame,
    threshold: float,
    alpha: float,
    include_zero_distance_neighbors: bool = True,
    include_self: bool = False,
    self_weight: float = 1.0,
    geometry: Optional[str] = None,
    use_spheroid: bool = False,
    saved_attributes: List[str] = None,
    result_name: str = "weights",
) -> DataFrame:
    """Annotates a dataframe with a weights column containing the other records within the threshold and their weight.

    Weights will be distance^alpha. The dataframe should contain at least one GeometryType column. Rows must be unique. If
    one geometry column is present it will be used automatically. If two are present, the one named 'geometry' will be
    used. If more than one are present and neither is named 'geometry', the column name must be provided. The new column
    will be named 'cluster'.

    Args:
        dataframe: DataFrame with geometry column
        threshold: Distance threshold for considering neighbors
        alpha: alpha to use for inverse distance weights. Computation is dist^alpha. Default is -1.0
        include_zero_distance_neighbors: whether to include neighbors that are 0 distance. If 0 distance neighbors are
            included and binary is false, values are infinity as per the floating point spec (divide by 0)
        include_self: whether to include self in the list of neighbors
        self_weight: the value to use for the self weight. Default is 1.0
        geometry: name of the geometry column
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. Default is false
        saved_attributes: the attributes to save in the neighbor column. Default is all columns.
        result_name: the name of the resulting column. Default is 'weights'.

    Returns:
        The input DataFrame with a weight column added containing neighbors and their weights (always 1) added to each
        row.

    """
    sedona = SparkSession.getActiveSession()

    return sedona._jvm.org.apache.sedona.stats.Weighting.addBinaryDistanceBandColumn(
        dataframe._jdf,
        float(threshold),
        float(alpha),
        include_zero_distance_neighbors,
        include_self,
        float(self_weight),
        geometry,
        use_spheroid,
        saved_attributes,
        result_name,
    )
