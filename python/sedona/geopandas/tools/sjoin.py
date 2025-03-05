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
from pyspark.pandas.internal import InternalFrame
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import scol_for
from pyspark.sql.functions import expr

from sedona.geopandas import GeoDataFrame, GeoSeries
from sedona.geopandas.geoseries import _to_geo_series
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame


def _frame_join(left_df, right_df):
    """Join the GeoDataFrames at the DataFrame level.

    Parameters
    ----------
    left_df : GeoDataFrame
    right_df : GeoDataFrame

    Returns
    -------
    GeoDataFrame
        Joined GeoDataFrame.

    TODO: Implement this function with more details and parameters.
    """
    # Get the internal Spark DataFrames
    left_sdf = left_df._internal.spark_frame
    right_sdf = right_df._internal.spark_frame

    # Convert WKB to geometry
    left_geo_df = left_sdf.selectExpr("ST_GeomFromWKB(`0`) as l_geometry")
    right_geo_df = right_sdf.selectExpr("ST_GeomFromWKB(`0`) as r_geometry")

    # Perform Spatial Join using ST_Intersects
    spatial_join_df = left_geo_df.alias("l").join(
        right_geo_df.alias("r"), expr("ST_Intersects(l_geometry, r_geometry)")
    )

    # Use the provided code template to create an InternalFrame and return a GeoSeries
    internal = InternalFrame(
        spark_frame=spatial_join_df,
        index_spark_columns=None,
        column_labels=[left_df._col_label],
        data_spark_columns=[scol_for(spatial_join_df, "l_geometry")],
        data_fields=[left_df._internal.data_fields[0]],
        column_label_names=left_df._internal.column_label_names,
    )
    return _to_geo_series(first_series(PandasOnSparkDataFrame(internal)))


def sjoin(
    left_df,
    right_df,
    how="inner",
    predicate="intersects",
    lsuffix="left",
    rsuffix="right",
    distance=None,
    on_attribute=None,
    **kwargs,
):
    """Spatial join of two GeoDataFrames.

    See the User Guide page :doc:`../../user_guide/mergingdata` for details.


    Parameters
    ----------
    left_df, right_df : GeoDataFrames
    how : string, default 'inner'
        The type of join:

        * 'left': use keys from left_df; retain only left_df geometry column
        * 'right': use keys from right_df; retain only right_df geometry column
        * 'inner': use intersection of keys from both dfs; retain only
          left_df geometry column
    predicate : string, default 'intersects'
        Binary predicate. Valid values are determined by the spatial index used.
        You can check the valid values in left_df or right_df as
        ``left_df.sindex.valid_query_predicates`` or
        ``right_df.sindex.valid_query_predicates``
        Replaces deprecated ``op`` parameter.
    lsuffix : string, default 'left'
        Suffix to apply to overlapping column names (left GeoDataFrame).
    rsuffix : string, default 'right'
        Suffix to apply to overlapping column names (right GeoDataFrame).
    distance : number or array_like, optional
        Distance(s) around each input geometry within which to query the tree
        for the 'dwithin' predicate. If array_like, must be
        one-dimesional with length equal to length of left GeoDataFrame.
        Required if ``predicate='dwithin'``.
    on_attribute : string, list or tuple
        Column name(s) to join on as an additional join restriction on top
        of the spatial predicate. These must be found in both DataFrames.
        If set, observations are joined only if the predicate applies
        and values in specified columns match.

    Examples
    --------
    >>> groceries_w_communities = geopandas.sjoin(groceries, chicago)
    >>> groceries_w_communities.head()  # doctest: +SKIP
       OBJECTID       community                           geometry
    0        16          UPTOWN  MULTIPOINT ((-87.65661 41.97321))
    1        18     MORGAN PARK  MULTIPOINT ((-87.68136 41.69713))
    2        22  NEAR WEST SIDE  MULTIPOINT ((-87.63918 41.86847))
    3        23  NEAR WEST SIDE  MULTIPOINT ((-87.65495 41.87783))
    4        27         CHATHAM  MULTIPOINT ((-87.62715 41.73623))
    [5 rows x 95 columns]

    Notes
    -----
    Every operation in GeoPandas is planar, i.e. the potential third
    dimension is not taken into account.
    """
    if kwargs:
        first = next(iter(kwargs.keys()))
        raise TypeError(f"sjoin() got an unexpected keyword argument '{first}'")

    on_attribute = _maybe_make_list(on_attribute)

    _basic_checks(left_df, right_df, how, lsuffix, rsuffix, on_attribute=on_attribute)

    joined = _frame_join(
        left_df,
        right_df,
    )

    return joined


def _maybe_make_list(obj):
    if isinstance(obj, tuple):
        return list(obj)
    if obj is not None and not isinstance(obj, list):
        return [obj]
    return obj


def _basic_checks(left_df, right_df, how, lsuffix, rsuffix, on_attribute=None):
    """Checks the validity of join input parameters.

    `how` must be one of the valid options.
    `'index_'` concatenated with `lsuffix` or `rsuffix` must not already
    exist as columns in the left or right data frames.

    Parameters
    ------------
    left_df : GeoDataFrame
    right_df : GeoData Frame
    how : str, one of 'left', 'right', 'inner'
        join type
    lsuffix : str
        left index suffix
    rsuffix : str
        right index suffix
    on_attribute : list, default None
        list of column names to merge on along with geometry
    """
    if not isinstance(left_df, GeoSeries):
        raise ValueError(f"'left_df' should be GeoSeries, got {type(left_df)}")

    if not isinstance(right_df, GeoSeries):
        raise ValueError(f"'right_df' should be GeoSeries, got {type(right_df)}")

    allowed_hows = ["inner"]
    if how not in allowed_hows:
        raise ValueError(f'`how` was "{how}" but is expected to be in {allowed_hows}')
