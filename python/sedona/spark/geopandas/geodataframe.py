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
from __future__ import annotations

from typing import Any, Literal, Callable, Union
import typing

import os
import shapely
import warnings
import numpy as np
import geopandas as gpd
import pandas as pd
import pyspark.pandas as pspd
import sedona.spark.geopandas as sgpd
from pyspark.pandas import Series as PandasOnSparkSeries
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.utils import log_advice

from sedona.spark.geopandas._typing import Label
from sedona.spark.geopandas.base import GeoFrame

from pandas.api.extensions import register_extension_dtype
from geopandas.geodataframe import crs_mismatch_error
from geopandas.array import GeometryDtype

register_extension_dtype(GeometryDtype)


# ============================================================================
# IMPLEMENTATION STATUS TRACKING
# ============================================================================

IMPLEMENTATION_PRIORITY = {
    "HIGH": [
        "to_geopandas",
        "_to_geopandas",
        "contains",
        "contains_properly",
        "convex_hull",
        "count_coordinates",
        "count_geometries",
        "is_ring",
        "is_closed",
        "make_valid",
    ],
    "MEDIUM": [
        "force_2d",
        "force_3d",
        "transform",
        "segmentize",
        "line_merge",
        "union_all",
        "intersection_all",
        "reverse",
        "normalize",
        "get_geometry",
    ],
    "LOW": [
        "delaunay_triangles",
        "voronoi_polygons",
        "minimum_bounding_circle",
        "representative_point",
        "extract_unique_points",
        "offset_curve",
        "minimum_rotated_rectangle",
        "concave_hull",
    ],
}


def _not_implemented_error(method_name: str, additional_info: str = "") -> str:
    """
    Generate a standardized NotImplementedError message for GeoDataFrame methods.

    Parameters
    ----------
    method_name : str
        The name of the method that is not implemented.
    additional_info : str, optional
        Additional information about the method or workarounds.

    Returns
    -------
    str
        Formatted error message.
    """
    base_message = (
        f"GeoDataFrame.{method_name}() is not implemented yet.\n"
        f"This method will be added in a future release."
    )

    if additional_info:
        base_message += f"\n\n{additional_info}"

    workaround = (
        "\n\nTemporary workaround - use GeoPandas:\n"
        "  gpd_df = sedona_gdf.to_geopandas()\n"
        f"  result = gpd_df.{method_name}(...)\n"
        "  # Note: This will collect all data to the driver."
    )

    return base_message + workaround


class GeoDataFrame(GeoFrame, pspd.DataFrame):
    """
    A pandas-on-Spark DataFrame for geospatial data with geometry columns.

    GeoDataFrame extends pyspark.pandas.DataFrame to provide geospatial operations
    using Apache Sedona's spatial functions. It maintains compatibility with
    GeoPandas GeoDataFrame while operating on distributed datasets.

    Parameters
    ----------
    data : dict, array-like, DataFrame, or GeoDataFrame
        Data to initialize the GeoDataFrame. Can be a dictionary, array-like structure,
        pandas DataFrame, GeoPandas GeoDataFrame, or another GeoDataFrame.
    geometry : str, array-like, or GeoSeries, optional
        Column name, array of geometries, or GeoSeries to use as the active geometry.
        If None, will look for existing geometry columns.
    crs : pyproj.CRS, optional
        Coordinate Reference System for the geometries.
    columns : Index or array-like, optional
        Column labels to use for the resulting frame.
    index : Index or array-like, optional
        Index to use for the resulting frame.

    Examples
    --------
    >>> from shapely.geometry import Point, Polygon
    >>> from sedona.spark.geopandas import GeoDataFrame
    >>> import pandas as pd
    >>>
    >>> # Create from dictionary with geometry
    >>> data = {
    ...     'name': ['A', 'B', 'C'],
    ...     'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
    ... }
    >>> gdf = GeoDataFrame(data, crs='EPSG:4326')
    >>> gdf
         name   geometry
    0       A   POINT (0 0)
    1       B   POINT (1 1)
    2       C   POINT (2 2)
    >>>
    >>> # Spatial operations
    >>> buffered = gdf.buffer(0.1)
    >>> buffered.area
    0    0.031416
    1    0.031416
    2    0.031416
    dtype: float64
    >>>
    >>> # Spatial joins
    >>> polygons = GeoDataFrame({
    ...     'region': ['Region1', 'Region2'],
    ...     'geometry': [
    ...         Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)]),
    ...         Polygon([(0.5, 0.5), (2.5, 0.5), (2.5, 2.5), (0.5, 2.5)])
    ...     ]
    ... })
    >>> result = gdf.sjoin(polygons, how='left', predicate='within')
    >>> result['region']
    0    Region1
    1    Region2
    2    Region2
    dtype: object

    Notes
    -----
    This implementation differs from GeoPandas in several ways:
    - Uses Spark for distributed processing
    - Geometries are stored in WKB (Well-Known Binary) format internally
    - Some methods may have different performance characteristics
    - Not all GeoPandas methods are implemented yet (see Sedona GeoPandas docs).

    Performance Considerations:
    - Operations are distributed across Spark cluster
    - Avoid converting to GeoPandas (.to_geopandas()) on large datasets
    - Use .sample() for testing with large datasets
    - Spatial joins are optimized for distributed processing

    Geometry Column Management:
    - Supports multiple geometry columns
    - One geometry column is designated as 'active' at a time
    - Active geometry is used for spatial operations and plotting
    - Use set_geometry() to change the active geometry column

    See Also
    --------
    geopandas.GeoDataFrame : The GeoPandas equivalent
    sedona.spark.geopandas.GeoSeries : Series with geometry data
    """

    def __getitem__(self, key: Any) -> Any:
        """
        Get item from GeoDataFrame by key.

        Parameters
        ----------
        key : str, list, slice, ndarray or Series
            - If key is a string, returns a Series for that column
            - If key is a list of strings, returns a new GeoDataFrame with selected columns
            - If key is a slice or array, returns rows in the GeoDataFrame

        Returns
        -------
        Any
            Series, GeoDataFrame, or other objects depending on the key type.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>>
        >>> data = {'geometry': [Point(0, 0), Point(1, 1)], 'value': [1, 2]}
        >>> gdf = GeoDataFrame(data)
        >>> gdf['value']
        0    1
        1    2
        Name: value, dtype: int64
        """

        # Here we are getting a ps.Series with the same underlying anchor (ps.DataFrame).
        # This is important so we don't unnecessarily try to perform operations on different dataframes.
        item = pspd.DataFrame.__getitem__(self, key)

        if isinstance(item, pspd.DataFrame):
            # Don't specify crs=self.crs here because it might not include the geometry column.
            # If it does include the geometry column, we don't need to set crs anyways.
            return GeoDataFrame(item)
        elif isinstance(item, pspd.Series):
            ps_series: pspd.Series = item
            try:
                return sgpd.GeoSeries(ps_series)
            except TypeError:
                return ps_series
        else:
            raise Exception(f"Logical Error: Unexpected type: {type(item)}")

    _geometry_column_name = None

    # ============================================================================
    # CONSTRUCTION AND INITIALIZATION
    # ============================================================================

    def __init__(
        self,
        data=None,
        index=None,
        columns=None,
        dtype=None,
        copy=False,
        geometry: Any | None = None,
        crs: Any | None = None,
        **kwargs,
    ):
        assert data is not None

        self._anchor: GeoDataFrame
        self._col_label: Label

        from sedona.spark.geopandas import GeoSeries
        from pyspark.sql import DataFrame as SparkDataFrame

        if isinstance(data, (GeoDataFrame, GeoSeries)):
            if crs:
                data.crs = crs

            # For each of these super().__init__() calls, we let pyspark decide which inputs are valid or not
            # instead of calling e.g assert not dtype ourselves.
            # This way, if Spark adds support later, we inherit those changes naturally.
            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)

        elif isinstance(data, (PandasOnSparkDataFrame, SparkDataFrame)):

            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)
        elif isinstance(data, PandasOnSparkSeries):

            try:
                data = GeoSeries(data, crs=crs)
            except TypeError:
                pass

            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)
        else:
            # Below are not distributed dataframe types
            if isinstance(data, gpd.GeoDataFrame):
                # We can use GeoDataFrame.active_geometry_name once we drop support for geopandas < 1.0.0.
                # Below is the equivalent, since active_geometry_name simply calls _geometry_column_name.
                if data._geometry_column_name:
                    # GeoPandas stores CRS as metadata instead of inside shapely objects, so we must save it and set it manually later.
                    if not crs:
                        crs = data.crs
                    if not geometry:
                        geometry = data.geometry.name

            pd_df = pd.DataFrame(
                data,
                index=index,
                columns=columns,
                dtype=dtype,
                copy=copy,
            )

            # Spark complains if it's left as a geometry type.
            geom_type_cols = pd_df.select_dtypes(include=["geometry"]).columns
            pd_df[geom_type_cols] = pd_df[geom_type_cols].astype(object)

            # Initialize the parent class pyspark DataFrame with the pandas DataFrame.
            super().__init__(
                data=pd_df,
                index=index,
                dtype=dtype,
                copy=copy,
            )

        # Set geometry column name
        if isinstance(data, (GeoDataFrame, gpd.GeoDataFrame)):
            self._geometry_column_name = data._geometry_column_name
            if crs is not None and data.crs != crs:
                raise ValueError(crs_mismatch_error)

        if geometry:
            self.set_geometry(geometry, inplace=True, crs=crs)

        if geometry is None and "geometry" in self.columns:

            if (self.columns == "geometry").sum() > 1:
                raise ValueError(
                    "GeoDataFrame does not support multiple columns "
                    "using the geometry column name 'geometry'."
                )

            geometry: pspd.Series = self["geometry"]
            if isinstance(geometry, sgpd.GeoSeries):
                if crs is not None:
                    self.set_geometry(geometry, inplace=True, crs=crs)

            # No need to call set_geometry() here since it's already part of the df, just set the name.
            self._geometry_column_name = "geometry"

        if geometry is None and crs:
            raise ValueError(
                "Assigning CRS to a GeoDataFrame without a geometry column is not "
                "supported. Supply geometry using the 'geometry=' keyword argument, "
                "or by providing a DataFrame with column name 'geometry'",
            )

    # ============================================================================
    # GEOMETRY COLUMN MANAGEMENT
    # ============================================================================

    def _get_geometry(self) -> sgpd.GeoSeries:
        if self._geometry_column_name not in self:
            if self._geometry_column_name is None:
                msg = (
                    "You are calling a geospatial method on the GeoDataFrame, "
                    "but the active geometry column to use has not been set. "
                )
            else:
                msg = (
                    "You are calling a geospatial method on the GeoDataFrame, "
                    f"but the active geometry column ('{self._geometry_column_name}') "
                    "is not present. "
                )
            geo_cols = list(self.columns[self.dtypes == "geometry"])
            if len(geo_cols) > 0:
                msg += (
                    f"\nThere are columns with geometry data type ({geo_cols}), and "
                    "you can either set one as the active geometry with "
                    'df.set_geometry("name") or access the column as a '
                    'GeoSeries (df["name"]) and call the method directly on it.'
                )
            else:
                msg += (
                    "\nThere are no existing columns with geometry data type. You can "
                    "add a geometry column as the active geometry column with "
                    "df.set_geometry. "
                )

            raise MissingGeometryColumnError(msg)
        return self[self._geometry_column_name]

    def _set_geometry(self, col):
        # This check is included in the original geopandas. Note that this prevents assigning a str to the property
        # e.g. df.geometry = "geometry"
        # However, the user can still specify a str in the public .set_geometry() method
        # i.e. df.geometry = "geometry1" errors, but df.set_geometry("geometry1") works
        if not pd.api.types.is_list_like(col):
            raise ValueError("Must use a list-like to set the geometry property")
        self.set_geometry(col, inplace=True)

    geometry = property(
        fget=_get_geometry, fset=_set_geometry, doc="Geometry data for GeoDataFrame"
    )

    @typing.overload
    def set_geometry(
        self,
        col,
        drop: bool | None = ...,
        inplace: Literal[True] = ...,
        crs: Any | None = ...,
    ) -> None: ...

    @typing.overload
    def set_geometry(
        self,
        col,
        drop: bool | None = ...,
        inplace: Literal[False] = ...,
        crs: Any | None = ...,
    ) -> GeoDataFrame: ...

    def set_geometry(
        self,
        col,
        drop: bool | None = None,
        inplace: bool = False,
        crs: Any | None = None,
    ) -> GeoDataFrame | None:
        """
        Set the GeoDataFrame geometry using either an existing column or
        the specified input. By default yields a new object.

        The original geometry column is replaced with the input.

        Parameters
        ----------
        col : column label or array-like
            An existing column name or values to set as the new geometry column.
            If values (array-like, (Geo)Series) are passed, then if they are named
            (Series) the new geometry column will have the corresponding name,
            otherwise the existing geometry column will be replaced. If there is
            no existing geometry column, the new geometry column will use the
            default name "geometry".
        drop : boolean, default False
            When specifying a named Series or an existing column name for `col`,
            controls if the previous geometry column should be dropped from the
            result. The default of False keeps both the old and new geometry column.

            .. deprecated:: 1.0.0

        inplace : boolean, default False
            Modify the GeoDataFrame in place (do not create a new object)
        crs : pyproj.CRS, optional
            Coordinate system to use. The value can be anything accepted
            by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
            If passed, overrides both DataFrame and col's crs.
            Otherwise, tries to get crs from passed col values or DataFrame.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> from shapely.geometry import Point
        >>> d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> gdf = GeoDataFrame(d, crs="EPSG:4326")
        >>> gdf
            col1     geometry
        0  name1  POINT (1 2)
        1  name2  POINT (2 1)

        Passing an array:

        >>> df1 = gdf.set_geometry([Point(0,0), Point(1,1)])
        >>> df1
            col1     geometry
        0  name1  POINT (0 0)
        1  name2  POINT (1 1)

        Using existing column:

        >>> gdf["buffered"] = gdf.buffer(2)
        >>> df2 = gdf.set_geometry("buffered")
        >>> df2.geometry
        0    POLYGON ((3 2, 2.99037 1.80397, 2.96157 1.6098...
        1    POLYGON ((4 1, 3.99037 0.80397, 3.96157 0.6098...
        Name: buffered, dtype: geometry

        Returns
        -------
        GeoDataFrame

        See also
        --------
        GeoDataFrame.rename_geometry : rename an active geometry column
        """
        # Most of the code here is taken from DataFrame.set_index()
        if inplace:
            frame = self
        else:
            frame = self.copy()

        geo_column_name = self._geometry_column_name
        new_series = False

        if geo_column_name is None:
            geo_column_name = "geometry"

        if isinstance(
            col, (pspd.Series, pd.Series, list, np.ndarray, gpd.array.GeometryArray)
        ):
            if drop:
                msg = (
                    "The `drop` keyword argument is deprecated and has no effect when "
                    "`col` is an array-like value. You should stop passing `drop` to "
                    "`set_geometry` when this is the case."
                )
                warnings.warn(msg, category=FutureWarning, stacklevel=2)
            if isinstance(col, (pspd.Series, pd.Series)):
                if col.name is not None:
                    geo_column_name = col.name
                    level = col
                else:
                    level = col.rename(geo_column_name)
            else:
                level = pspd.Series(col, name=geo_column_name)

            if not isinstance(level, sgpd.GeoSeries):
                # Set the crs later, so we can allow_override=True
                level = sgpd.GeoSeries(level)

            new_series = True
        elif hasattr(col, "ndim") and col.ndim > 1:
            raise ValueError("Must pass array with one dimension only.")
        else:  # should be a colname
            try:
                level = frame[col]
            except KeyError:
                raise ValueError(f"Unknown column {col}")
            if isinstance(level, (sgpd.GeoDataFrame, gpd.GeoDataFrame)):
                raise ValueError(
                    "GeoDataFrame does not support setting the geometry column where "
                    "the column name is shared by multiple columns."
                )

            given_colname_drop_msg = (
                "The `drop` keyword argument is deprecated and in future the only "
                "supported behaviour will match drop=False. To silence this "
                "warning and adopt the future behaviour, stop providing "
                "`drop` as a keyword to `set_geometry`. To replicate the "
                "`drop=True` behaviour you should update your code to:\n"
                "`geo_col_name = gdf.active_geometry_name; "
                "gdf.set_geometry(new_geo_col).drop(columns=geo_col_name)"
                ".rename_geometry(geo_col_name)`."
            )

            if drop is False:  # specifically False, not falsy i.e. None
                # User supplied False explicitly, but arg is deprecated
                warnings.warn(
                    given_colname_drop_msg,
                    category=FutureWarning,
                    stacklevel=2,
                )
            if drop:
                raise NotImplementedError(
                    _not_implemented_error(
                        "set_geometry",
                        "Setting geometry with drop=True parameter is not supported.",
                    )
                )
            else:
                # If not dropping, set the active geometry name to the given col name.
                geo_column_name = col

        # This operation throws a warning to the user asking them to set pspd.set_option('compute.ops_on_diff_frames', True)
        # to allow operations on different frames. We pass these warnings on to the user so they must manually set it themselves.
        if crs:
            level.set_crs(crs, inplace=True, allow_override=True)
            new_series = True

        frame._geometry_column_name = geo_column_name
        if new_series:
            # Note: This casts GeoSeries back into pspd.Series, so we lose any metadata that's not serialized.
            frame[geo_column_name] = level

        if not inplace:
            return frame

    @typing.overload
    def rename_geometry(
        self,
        col: str,
        inplace: Literal[True] = ...,
    ) -> None: ...

    @typing.overload
    def rename_geometry(
        self,
        col: str,
        inplace: Literal[False] = ...,
    ) -> GeoDataFrame: ...

    def rename_geometry(self, col: str, inplace: bool = False) -> GeoDataFrame | None:
        """
        Renames the GeoDataFrame geometry column to
        the specified name. By default yields a new object.

        The original geometry column is replaced with the input.

        Parameters
        ----------
        col : new geometry column label
        inplace : boolean, default False
            Modify the GeoDataFrame in place (without creating a new object)

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> from shapely.geometry import Point
        >>> d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> df = GeoDataFrame(d, crs="EPSG:4326")
        >>> df1 = df.rename_geometry('geom1')
        >>> df1.geometry.name
        'geom1'
        >>> df.rename_geometry('geom1', inplace=True)
        >>> df.geometry.name
        'geom1'


        See also
        --------
        GeoDataFrame.set_geometry : set the active geometry
        """
        geometry_col = self.geometry.name
        if col in self.columns:
            raise ValueError(f"Column named {col} already exists")
        else:
            mapper = {col: col for col in list(self.columns)}
            mapper[geometry_col] = col

            if inplace:
                self.rename(columns=mapper, inplace=True, errors="raise")
                self.set_geometry(col, inplace=True)
                return None

            df = self.copy()
            df.rename(columns=mapper, inplace=True, errors="raise")
            df.set_geometry(col, inplace=True)
            return df

    # ============================================================================
    # PROPERTIES AND ATTRIBUTES
    # ============================================================================

    @property
    def active_geometry_name(self) -> Any:
        """Return the name of the active geometry column

        Returns a name if a GeoDataFrame has an active geometry column set,
        otherwise returns None. The return type is usually a string, but may be
        an integer, tuple or other hashable, depending on the contents of the
        dataframe columns.

        You can also access the active geometry column using the
        ``.geometry`` property. You can set a GeoSeries to be an active geometry
        using the :meth:`~GeoDataFrame.set_geometry` method.

        Returns
        -------
        str or other index label supported by pandas
            name of an active geometry column or None

        See also
        --------
        GeoDataFrame.set_geometry : set the active geometry
        """
        return self._geometry_column_name

    def to_geopandas(self) -> gpd.GeoDataFrame:
        """
        Note: Unlike in pandas and geopandas, Sedona will always return a general Index.
        This differs from pandas and geopandas, which will return a RangeIndex by default.

        e.g pd.Index([0, 1, 2]) instead of pd.RangeIndex(start=0, stop=3, step=1)
        """

        log_advice(
            "`to_geopandas` loads all data into the driver's memory. "
            "It should only be used if the resulting geopandas GeoSeries is expected to be small."
        )
        return self._to_geopandas()

    def _to_geopandas(self) -> gpd.GeoDataFrame:
        pd_df = self._internal.to_pandas_frame

        for col_name in pd_df.columns:
            series: pspd.Series = self[col_name]
            if isinstance(series, sgpd.GeoSeries):
                # Use _to_geopandas instead of to_geopandas to avoid logging extra warnings
                pd_df[col_name] = series._to_geopandas()
            else:
                pd_df[col_name] = series._to_pandas()

        return gpd.GeoDataFrame(pd_df, geometry=self._geometry_column_name)

    def to_spark_pandas(self) -> pspd.DataFrame:
        """
        Convert the GeoDataFrame to a Spark Pandas DataFrame.
        """
        return pspd.DataFrame(self._internal)

    def copy(self, deep=False) -> GeoDataFrame:
        """
        Make a copy of this GeoDataFrame object.

        Parameters
        ----------
        deep : bool, default False
            This parameter is not supported but just a dummy parameter to match pandas.

        Returns
        -------
        GeoDataFrame
            A copy of this GeoDataFrame object.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoDataFrame

        >>> gdf = GeoDataFrame([{"geometry": Point(1, 1), "value1": 2, "value2": 3}])
        >>> gdf_copy = gdf.copy()
        >>> print(gdf_copy)
           geometry  value1  value2
        0  POINT (1 1)       2       3
        """
        # Note: The deep parameter is a dummy parameter just as it is in PySpark pandas.
        return GeoDataFrame(
            pspd.DataFrame(self._internal.copy()), geometry=self.active_geometry_name
        )

    def _safe_get_crs(self):
        """
        Helper method for getting the CRS of the GeoDataFrame safely.

        Returns None if no geometry column is set instead of raising an error.
        This is useful for operations that need to check the CRS without
        requiring a geometry column to be present.

        Returns
        -------
        Any or None
            The CRS of the active geometry column, or None if no geometry column is set.
        """
        try:
            return self.geometry.crs
        except MissingGeometryColumnError:
            return None

    @property
    def crs(self):
        return self.geometry.crs

    @crs.setter
    def crs(self, value):
        # Since PySpark DataFrames are immutable, we can't modify in place, so we create the new GeoSeries and replace it.
        self.geometry = self.geometry.set_crs(value)

    def set_crs(self, crs, inplace=False, allow_override=True):
        """
        Set the Coordinate Reference System (CRS) of the ``GeoDataFrame``.

        If there are multiple geometry columns within the GeoDataFrame, only
        the CRS of the active geometry column is set.

        Pass ``None`` to remove CRS from the active geometry column.

        Notes
        -----
        The underlying geometries are not transformed to this CRS. To
        transform the geometries to a new CRS, use the ``to_crs`` method.

        Parameters
        ----------
        crs : pyproj.CRS | None, optional
            The value can be anything accepted
            by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        epsg : int, optional
            EPSG code specifying the projection.
        inplace : bool, default False
            If True, the CRS of the GeoDataFrame will be changed in place
            (while still returning the result) instead of making a copy of
            the GeoDataFrame.
        allow_override : bool, default True
            If the GeoDataFrame already has a CRS, allow to replace the
            existing CRS, even when both are not equal. In Sedona, setting this to True
            will lead to eager evaluation instead of lazy evaluation. Unlike Geopandas,
            True is the default value in Sedona for performance reasons.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> from shapely.geometry import Point
        >>> d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> gdf = GeoDataFrame(d)
        >>> gdf
            col1     geometry
        0  name1  POINT (1 2)
        1  name2  POINT (2 1)

        Setting CRS to a GeoDataFrame without one:

        >>> gdf.crs is None
        True

        >>> gdf = gdf.set_crs('epsg:3857')
        >>> gdf.crs  # doctest: +SKIP
        <Projected CRS: EPSG:3857>
        Name: WGS 84 / Pseudo-Mercator
        Axis Info [cartesian]:
        - X[east]: Easting (metre)
        - Y[north]: Northing (metre)
        Area of Use:
        - name: World - 85°S to 85°N
        - bounds: (-180.0, -85.06, 180.0, 85.06)
        Coordinate Operation:
        - name: Popular Visualisation Pseudo-Mercator
        - method: Popular Visualisation Pseudo Mercator
        Datum: World Geodetic System 1984
        - Ellipsoid: WGS 84
        - Prime Meridian: Greenwich

        Overriding existing CRS:

        >>> gdf = gdf.set_crs(4326, allow_override=True)

        Without ``allow_override=True``, ``set_crs`` returns an error if you try to
        override CRS.

        See Also
        --------
        GeoDataFrame.to_crs : re-project to another CRS
        """
        # Since PySpark DataFrames are immutable, we can't modify in place, so we create the new GeoSeries and replace it.
        new_geometry = self.geometry.set_crs(crs, allow_override=allow_override)
        if inplace:
            self.geometry = new_geometry
        else:
            df = self.copy()
            df.geometry = new_geometry
            return df

    def to_crs(
        self,
        crs: Any | None = None,
        epsg: int | None = None,
        inplace: bool = False,
    ) -> GeoDataFrame | None:
        """Transform geometries to a new coordinate reference system.

        Transform all geometries in an active geometry column to a different coordinate
        reference system.  The ``crs`` attribute on the current GeoSeries must
        be set.  Either ``crs`` or ``epsg`` may be specified for output.

        This method will transform all points in all objects. It has no notion
        of projecting entire geometries.  All segments joining points are
        assumed to be lines in the current projection, not geodesics. Objects
        crossing the dateline (or other projection boundary) will have
        undesirable behavior.

        Parameters
        ----------
        crs : pyproj.CRS, optional if `epsg` is specified
            The value can be anything accepted by
            :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        epsg : int, optional if `crs` is specified
            EPSG code specifying output projection.
        inplace : bool, optional, default: False
            Whether to return a new GeoDataFrame or do the transformation in
            place.

        Returns
        -------
        GeoDataFrame

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> gdf = GeoDataFrame(d, crs=4326)
        >>> gdf
            col1     geometry
        0  name1  POINT (1 2)
        1  name2  POINT (2 1)
        >>> gdf.crs  # doctest: +SKIP
        <Geographic 2D CRS: EPSG:4326>
        Name: WGS 84
        Axis Info [ellipsoidal]:
        - Lat[north]: Geodetic latitude (degree)
        - Lon[east]: Geodetic longitude (degree)
        Area of Use:
        - name: World
        - bounds: (-180.0, -90.0, 180.0, 90.0)
        Datum: World Geodetic System 1984
        - Ellipsoid: WGS 84
        - Prime Meridian: Greenwich

        >>> gdf = gdf.to_crs(3857)
        >>> gdf
            col1                       geometry
        0  name1  POINT (111319.491 222684.209)
        1  name2  POINT (222638.982 111325.143)
        >>> gdf.crs  # doctest: +SKIP
        <Projected CRS: EPSG:3857>
        Name: WGS 84 / Pseudo-Mercator
        Axis Info [cartesian]:
        - X[east]: Easting (metre)
        - Y[north]: Northing (metre)
        Area of Use:
        - name: World - 85°S to 85°N
        - bounds: (-180.0, -85.06, 180.0, 85.06)
        Coordinate Operation:
        - name: Popular Visualisation Pseudo-Mercator
        - method: Popular Visualisation Pseudo Mercator
        Datum: World Geodetic System 1984
        - Ellipsoid: WGS 84
        - Prime Meridian: Greenwich

        See Also
        --------
        GeoDataFrame.set_crs : assign CRS without re-projection
        """
        new_geometry = self.geometry.to_crs(crs=crs, epsg=epsg)
        if inplace:
            df = self
            df.geometry = new_geometry
            return None
        else:
            df = self.copy()
            df.geometry = new_geometry
            return df

    @classmethod
    def from_dict(
        cls,
        data: dict,
        geometry=None,
        crs: Any | None = None,
        **kwargs,
    ) -> GeoDataFrame:
        raise NotImplementedError("from_dict() is not implemented yet.")

    @classmethod
    def from_features(
        cls, features, crs: Any | None = None, columns: Iterable[str] | None = None
    ) -> GeoDataFrame:
        raise NotImplementedError("from_features() is not implemented yet.")

    @classmethod
    def from_postgis(
        cls,
        sql: str | sqlalchemy.text,
        con,
        geom_col: str = "geom",
        crs: Any | None = None,
        index_col: str | list[str] | None = None,
        coerce_float: bool = True,
        parse_dates: list | dict | None = None,
        params: list | tuple | dict | None = None,
        chunksize: int | None = None,
    ) -> GeoDataFrame:
        raise NotImplementedError("from_postgis() is not implemented yet.")

    @classmethod
    def from_arrow(
        cls, table, geometry: str | None = None, to_pandas_kwargs: dict | None = None
    ):
        """
        Construct a GeoDataFrame from a Arrow table object based on GeoArrow
        extension types.

        See https://geoarrow.org/ for details on the GeoArrow specification.

        This functions accepts any tabular Arrow object implementing
        the `Arrow PyCapsule Protocol`_ (i.e. having an ``__arrow_c_array__``
        or ``__arrow_c_stream__`` method).

        .. _Arrow PyCapsule Protocol: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

        .. versionadded:: 1.0

        Parameters
        ----------
        table : pyarrow.Table or Arrow-compatible table
            Any tabular object implementing the Arrow PyCapsule Protocol
            (i.e. has an ``__arrow_c_array__`` or ``__arrow_c_stream__``
            method). This table should have at least one column with a
            geoarrow geometry type.
        geometry : str, default None
            The name of the geometry column to set as the active geometry
            column. If None, the first geometry column found will be used.
        to_pandas_kwargs : dict, optional
            Arguments passed to the `pa.Table.to_pandas` method for non-geometry
            columns. This can be used to control the behavior of the conversion of the
            non-geometry columns to a pandas DataFrame. For example, you can use this
            to control the dtype conversion of the columns. By default, the `to_pandas`
            method is called with no additional arguments.

        Returns
        -------
        GeoDataFrame

        See Also
        --------
        GeoDataFrame.to_arrow
        GeoSeries.from_arrow

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> import geoarrow.pyarrow as ga  # requires: pip install geoarrow-pyarrow
        >>> import pyarrow as pa  # requires: pip install pyarrow
        >>> table = pa.Table.from_arrays([
        ...     ga.as_geoarrow([None, "POLYGON ((0 0, 1 1, 0 1, 0 0))", "LINESTRING (0 0, -1 1, 0 -1)"]),
        ...     pa.array([1, 2, 3]),
        ...     pa.array(["a", "b", "c"]),
        ... ], names=["geometry", "id", "value"])
        >>> gdf = GeoDataFrame.from_arrow(table)
        >>> gdf
                                   geometry   id  value
        0                              None    1      a
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))    2      b
        2      LINESTRING (0 0, -1 1, 0 -1)    3      c
        """
        if to_pandas_kwargs is None:
            to_pandas_kwargs = {}

        gpd_df = gpd.GeoDataFrame.from_arrow(
            table, geometry=geometry, **to_pandas_kwargs
        )
        return GeoDataFrame(gpd_df)

    def to_json(
        self,
        na: Literal["null", "drop", "keep"] = "null",
        show_bbox: bool = False,
        drop_id: bool = False,
        to_wgs84: bool = False,
        **kwargs,
    ) -> str:
        """
        Returns a GeoJSON representation of the ``GeoDataFrame`` as a string.

        Parameters
        ----------
        na : {'null', 'drop', 'keep'}, default 'null'
            Dictates how to represent missing (NaN) values in the output.
            - ``null``: Outputs missing entries as JSON `null`.
            - ``drop``: Removes the entire property from a feature if its
            value is missing.
            - ``keep``: Outputs missing entries as ``NaN``.
        show_bbox : bool, default False
            If True, the `bbox` (bounds) of the geometries is included in the
            output.
        drop_id : bool, default False
            If True, the GeoDataFrame index is not written to the 'id' field
            of each GeoJSON Feature.
        to_wgs84 : bool, default False
            If True, all geometries are transformed to WGS84 (EPSG:4326) to
            meet the `2016 GeoJSON specification
            <https://tools.ietf.org/html/rfc7946>`_. When False, the current
            CRS is exported if it's set.
        **kwargs
            Additional keyword arguments passed to `json.dumps()`.

        Returns
        -------
        str
            A GeoJSON representation of the GeoDataFrame.

        See Also
        --------
        GeoDataFrame.to_file : Write a ``GeoDataFrame`` to a file, which can be
                            used for GeoJSON format.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> from shapely.geometry import Point
        >>> d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> gdf = GeoDataFrame(d, crs="EPSG:3857")
        >>> gdf.to_json()
        '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"col1": "name1"}, "geometry": {"type": "Point", "coordinates": [1.0, 2.0]}}, {"id": "1", "type": "Feature", "properties": {"col1": "name2"}, "geometry": {"type": "Point", "coordinates": [2.0, 1.0]}}], "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::3857"}}}'


        See also
        --------
        GeoDataFrame.to_file : write GeoDataFrame to file
        """
        # Because this function returns the GeoJSON string in memory,
        # we simply rely on GeoPandas's implementation.
        # Additionally, Spark doesn't seem to have a straightforward way to get the string
        # without writing to a file first by using sdf.write.format("geojson").save(path, **kwargs).
        # ST_AsGeoJSON() works only for one column.
        result = self.to_geopandas()
        return result.to_json(na, show_bbox, drop_id, to_wgs84, **kwargs)

    @property
    def __geo_interface__(self) -> dict:
        raise NotImplementedError("__geo_interface__() is not implemented yet.")

    def iterfeatures(
        self, na: str = "null", show_bbox: bool = False, drop_id: bool = False
    ) -> typing.Generator[dict]:
        raise NotImplementedError("iterfeatures() is not implemented yet.")

    def to_geo_dict(
        self, na: str | None = "null", show_bbox: bool = False, drop_id: bool = False
    ) -> dict:
        raise NotImplementedError("to_geo_dict() is not implemented yet.")

    def to_wkb(self, hex: bool = False, **kwargs) -> pd.DataFrame:
        raise NotImplementedError("to_wkb() is not implemented yet.")

    def to_wkt(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError("to_wkt() is not implemented yet.")

    def to_arrow(
        self,
        *,
        index: bool | None = None,
        geometry_encoding="WKB",
        interleaved: bool = True,
        include_z: bool | None = None,
    ):
        """Encode a GeoDataFrame to GeoArrow format.
        See https://geoarrow.org/ for details on the GeoArrow specification.
        This function returns a generic Arrow data object implementing
        the `Arrow PyCapsule Protocol`_ (i.e. having an ``__arrow_c_stream__``
        method). This object can then be consumed by your Arrow implementation
        of choice that supports this protocol.
        .. _Arrow PyCapsule Protocol: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

        Note: Requires geopandas versions >= 1.0.0 to use with Sedona.

        Parameters
        ----------
        index : bool, default None
            If ``True``, always include the dataframe's index(es) as columns
            in the file output.
            If ``False``, the index(es) will not be written to the file.
            If ``None``, the index(ex) will be included as columns in the file
            output except `RangeIndex` which is stored as metadata only.

            Note: Unlike in geopandas, ``None`` will include the index in the column because Sedona always
            converts `RangeIndex` into a general `Index`.

        geometry_encoding : {'WKB', 'geoarrow' }, default 'WKB'
            The GeoArrow encoding to use for the data conversion.
        interleaved : bool, default True
            Only relevant for 'geoarrow' encoding. If True, the geometries'
            coordinates are interleaved in a single fixed size list array.
            If False, the coordinates are stored as separate arrays in a
            struct type.
        include_z : bool, default None
            Only relevant for 'geoarrow' encoding (for WKB, the dimensionality
            of the individual geometries is preserved).
            If False, return 2D geometries. If True, include the third dimension
            in the output (if a geometry has no third dimension, the z-coordinates
            will be NaN). By default, will infer the dimensionality from the
            input geometries. Note that this inference can be unreliable with
            empty geometries (for a guaranteed result, it is recommended to
            specify the keyword).
        Returns
        -------
        ArrowTable
            A generic Arrow table object with geometry columns encoded to
            GeoArrow.
        Examples
        --------
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> from shapely.geometry import Point
        >>> data = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> gdf = GeoDataFrame(data)
        >>> gdf
            col1     geometry
        0  name1  POINT (1 2)
        1  name2  POINT (2 1)
        >>> arrow_table = gdf.to_arrow(index=False)
        >>> arrow_table
        <geopandas.io._geoarrow.ArrowTable object at ...>
        The returned data object needs to be consumed by a library implementing
        the Arrow PyCapsule Protocol. For example, wrapping the data as a
        pyarrow.Table (requires pyarrow >= 14.0):
        >>> import pyarrow as pa  # requires: pip install pyarrow
        >>> table = pa.table(arrow_table)
        >>> table
        pyarrow.Table
        col1: string
        geometry: binary
        ----
        col1: [["name1","name2"]]
        geometry: [[0101000000000000000000F03F0000000000000040,\
01010000000000000000000040000000000000F03F]]
        """
        # Because this function returns the Arrow table in memory, we simply rely on GeoPandas's implementation.
        # This also returns a GeoPandas-specific data type, which can be converted to an actual PyArrow table,
        # so there is no direct Sedona equivalent. This way we also get all of the arguments implemented for free.
        return self.to_geopandas().to_arrow(
            index=index,
            geometry_encoding=geometry_encoding,
            interleaved=interleaved,
            include_z=include_z,
        )

    def to_feather(
        self,
        path,
        index: bool | None = None,
        compression: str | None = None,
        schema_version=None,
        **kwargs,
    ):
        raise NotImplementedError("to_feather() is not implemented yet.")

    @property
    def type(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error("type", "Returns numeric geometry type codes.")
        )

    def plot(self, *args, **kwargs):
        """
        Plot a GeoDataFrame.

        Generate a plot of a GeoDataFrame with matplotlib.  If a
        column is specified, the plot coloring will be based on values
        in that column.

        Note: This method is not scalable and requires collecting all data to the driver.

        Parameters
        ----------
        column : str, np.array, pd.Series, pd.Index (default None)
            The name of the dataframe column, np.array, pd.Series, or pd.Index
            to be plotted. If np.array, pd.Series, or pd.Index are used then it
            must have same length as dataframe. Values are used to color the plot.
            Ignored if `color` is also set.
        kind: str
            The kind of plots to produce. The default is to create a map ("geo").
            Other supported kinds of plots from pandas:

            - 'line' : line plot
            - 'bar' : vertical bar plot
            - 'barh' : horizontal bar plot
            - 'hist' : histogram
            - 'box' : BoxPlot
            - 'kde' : Kernel Density Estimation plot
            - 'density' : same as 'kde'
            - 'area' : area plot
            - 'pie' : pie plot
            - 'scatter' : scatter plot
            - 'hexbin' : hexbin plot.
        cmap : str (default None)
            The name of a colormap recognized by matplotlib.
        color : str, np.array, pd.Series (default None)
            If specified, all objects will be colored uniformly.
        ax : matplotlib.pyplot.Artist (default None)
            axes on which to draw the plot
        cax : matplotlib.pyplot Artist (default None)
            axes on which to draw the legend in case of color map.
        categorical : bool (default False)
            If False, cmap will reflect numerical values of the
            column being plotted.  For non-numerical columns, this
            will be set to True.
        legend : bool (default False)
            Plot a legend. Ignored if no `column` is given, or if `color` is given.
        scheme : str (default None)
            Name of a choropleth classification scheme (requires mapclassify).
            A mapclassify.MapClassifier object will be used
            under the hood. Supported are all schemes provided by mapclassify (e.g.
            'BoxPlot', 'EqualInterval', 'FisherJenks', 'FisherJenksSampled',
            'HeadTailBreaks', 'JenksCaspall', 'JenksCaspallForced',
            'JenksCaspallSampled', 'MaxP', 'MaximumBreaks',
            'NaturalBreaks', 'Quantiles', 'Percentiles', 'StdMean',
            'UserDefined'). Arguments can be passed in classification_kwds.
        k : int (default 5)
            Number of classes (ignored if scheme is None)
        vmin : None or float (default None)
            Minimum value of cmap. If None, the minimum data value
            in the column to be plotted is used.
        vmax : None or float (default None)
            Maximum value of cmap. If None, the maximum data value
            in the column to be plotted is used.
        markersize : str or float or sequence (default None)
            Only applies to point geometries within a frame.
            If a str, will use the values in the column of the frame specified
            by markersize to set the size of markers. Otherwise can be a value
            to apply to all points, or a sequence of the same length as the
            number of points.
        figsize : tuple of integers (default None)
            Size of the resulting matplotlib.figure.Figure. If the argument
            axes is given explicitly, figsize is ignored.
        legend_kwds : dict (default None)
            Keyword arguments to pass to :func:`matplotlib.pyplot.legend` or
            :func:`matplotlib.pyplot.colorbar`.
            Additional accepted keywords when `scheme` is specified:

            fmt : string
                A formatting specification for the bin edges of the classes in the
                legend. For example, to have no decimals: ``{"fmt": "{:.0f}"}``.
            labels : list-like
                A list of legend labels to override the auto-generated labels.
                Needs to have the same number of elements as the number of
                classes (`k`).
            interval : boolean (default False)
                An option to control brackets from mapclassify legend.
                If True, open/closed interval brackets are shown in the legend.
        categories : list-like
            Ordered list-like object of categories to be used for categorical plot.
        classification_kwds : dict (default None)
            Keyword arguments to pass to mapclassify
        missing_kwds : dict (default None)
            Keyword arguments specifying color options (as style_kwds)
            to be passed on to geometries with missing values in addition to
            or overwriting other style kwds. If None, geometries with missing
            values are not plotted.
        aspect : 'auto', 'equal', None or float (default 'auto')
            Set aspect of axis. If 'auto', the default aspect for map plots is 'equal'; if
            however data are not projected (coordinates are long/lat), the aspect is by
            default set to 1/cos(df_y * pi/180) with df_y the y coordinate of the middle of
            the GeoDataFrame (the mean of the y range of bounding box) so that a long/lat
            square appears square in the middle of the plot. This implies an
            Equirectangular projection. If None, the aspect of `ax` won't be changed. It can
            also be set manually (float) as the ratio of y-unit to x-unit.
        autolim : bool (default True)
            Update axes data limits to contain the new geometries.
        **style_kwds : dict
            Style options to be passed on to the actual plot function, such
            as ``edgecolor``, ``facecolor``, ``linewidth``, ``markersize``,
            ``alpha``.

        Returns
        -------
        ax : matplotlib axes instance

        Examples
        --------
        >>> import geodatasets  # requires: pip install geodatasets
        >>> import geopandas as gpd
        >>> df = gpd.read_file(geodatasets.get_path("nybb"))
        >>> df.head()  # doctest: +SKIP
        BoroCode  ...                                           geometry
        0         5  ...  MULTIPOLYGON (((970217.022 145643.332, 970227....
        1         4  ...  MULTIPOLYGON (((1029606.077 156073.814, 102957...
        2         3  ...  MULTIPOLYGON (((1021176.479 151374.797, 102100...
        3         1  ...  MULTIPOLYGON (((981219.056 188655.316, 980940....
        4         2  ...  MULTIPOLYGON (((1012821.806 229228.265, 101278...

        >>> df.plot("BoroName", cmap="Set1")  # doctest: +SKIP
        """
        return self.to_geopandas().plot(*args, **kwargs)

    # ============================================================================
    # SPATIAL OPERATIONS
    # ============================================================================

    def sjoin(
        self,
        other,
        how="inner",
        predicate="intersects",
        lsuffix="left",
        rsuffix="right",
        distance=None,
        on_attribute=None,
        **kwargs,
    ):
        """
        Spatial join of two GeoDataFrames.

        Parameters
        ----------
        other : GeoDataFrame
            The right GeoDataFrame to join with.
        how : str, default 'inner'
            The type of join:
            * 'left': use keys from left_df; retain only left_df geometry column
            * 'right': use keys from right_df; retain only right_df geometry column
            * 'inner': use intersection of keys from both dfs; retain only left_df geometry column
        predicate : str, default 'intersects'
            Binary predicate. Valid values: 'intersects', 'contains', 'within', 'dwithin', 'touches', 'crosses', 'overlaps', 'covers', 'covered_by'
        lsuffix : str, default 'left'
            Suffix to apply to overlapping column names (left GeoDataFrame).
        rsuffix : str, default 'right'
            Suffix to apply to overlapping column names (right GeoDataFrame).
        distance : float, optional
            Distance for 'dwithin' predicate. Required if predicate='dwithin'.
        on_attribute : str, list or tuple, optional
            Column name(s) to join on as an additional join restriction.
            These must be found in both DataFrames.
        **kwargs
            Additional keyword arguments passed to the spatial join function.

        Returns
        -------
        GeoDataFrame
            A GeoDataFrame with the results of the spatial join.

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon
        >>> from sedona.spark.geopandas import GeoDataFrame

        >>> polygons = GeoDataFrame({
        ...     'geometry': [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])],
        ...     'value': [1]
        ... })
        >>> points = GeoDataFrame({
        ...     'geometry': [Point(0.5, 0.5), Point(2, 2)],
        ...     'value': [1, 2]
        ... })
        >>> joined = points.sjoin(polygons)
        >>> joined
            geometry_left  value_left            geometry_right  value_right
        0  POINT (0.5 0.5)           1  POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))            1
        """
        from sedona.spark.geopandas.tools.sjoin import sjoin as sjoin_tool

        return sjoin_tool(
            self,
            other,
            how=how,
            predicate=predicate,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            distance=distance,
            on_attribute=on_attribute,
            **kwargs,
        )

    # ============================================================================
    # I/O OPERATIONS
    # ============================================================================

    @classmethod
    def from_file(
        cls, filename: str, format: str | None = None, **kwargs
    ) -> GeoDataFrame:
        """Alternate constructor to create a ``GeoDataFrame`` from a file.

        Parameters
        ----------
        filename : str
            File path or file handle to read from. If the path is a directory,
            Sedona will read all files in that directory.
        format : str, optional
            The format of the file to read, by default None. If None, Sedona
            infers the format from the file extension. Note that format
            inference is not supported for directories. Available formats are
            "shapefile", "geojson", "geopackage", and "geoparquet".
        table_name : str, optional
            The name of the table to read from a GeoPackage file, by default
            None. This is required if ``format`` is "geopackage".
        **kwargs
            Additional keyword arguments passed to the file reader.

        Returns
        -------
        GeoDataFrame
            A new GeoDataFrame created from the file.

        See Also
        --------
        GeoDataFrame.to_file : Write a ``GeoDataFrame`` to a file.
        """
        return sgpd.io.read_file(filename, format, **kwargs)

    def to_file(
        self,
        path: str,
        driver: str | None = None,
        schema: dict | None = None,
        index: bool | None = None,
        **kwargs,
    ):
        """
        Write the ``GeoDataFrame`` to a file.

        Parameters
        ----------
        path : str
            File path or file handle to write to.

        driver : str, default None
            The format driver used to write the file.
            If not specified, it attempts to infer it from the file extension.
            If no extension is specified, Sedona will error.

            Options: "geojson", "geopackage", "geoparquet"

        schema : dict, default None
            Not applicable to Sedona's implementation.

        index : bool, default None
            If True, write index into one or more columns (for MultiIndex).
            Default None writes the index into one or more columns only if
            the index is named, is a MultiIndex, or has a non-integer data
            type. If False, no index is written.

        **kwargs
            Additional keyword arguments:

            mode : str, default 'w'
                The write mode, 'w' to overwrite the existing file and 'a' to append.
                'overwrite' and 'append' are equivalent to 'w' and 'a' respectively.

            crs : pyproj.CRS, default None
                If specified, the CRS is passed to Fiona to better control how the file is written.
                If None, GeoPandas will determine the CRS based on the ``crs`` attribute.
                The value can be anything accepted by
                :meth:`pyproj.CRS.from_user_input <pyproj.crs.CRS.from_user_input>`,
                such as an authority string (e.g., "EPSG:4326") or a WKT string.

            engine : str
                Not applicable to Sedona's implementation.

            metadata : dict[str, str], default None
                Optional metadata to be stored in the file. Keys and values must be
                strings. Supported only for "GPKG" driver. Not supported by Sedona.

        Examples
        --------
        >>> from shapely.geometry import Point, LineString
        >>> from sedona.spark.geopandas import GeoDataFrame

        >>> gdf = GeoDataFrame({
        ...     "geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])],
        ...     "int": [1, 2]
        ... })
        >>> gdf.to_file("output.parquet", driver="geoparquet")

        With selected drivers you can also append to a file with ``mode="a"``:

        >>> gdf.to_file("output.geojson", driver="geojson", mode="a")

        When the index is of non-integer dtype, ``index=None`` (default) is treated as True,
        writing the index to the file.

        >>> gdf = GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1)]}, index=["a", "b"])
        >>> gdf.to_file("output_with_index.parquet", driver="geoparquet")
        """
        sgpd.io._to_file(self, path, driver, index, **kwargs)

    def to_parquet(self, path, **kwargs):
        """
        Write the GeoDataFrame to a GeoParquet file.

        Parameters
        ----------
        path : str
            The file path where the GeoParquet file will be written.
        **kwargs
            Additional arguments to pass to the Sedona DataFrame output function.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>> gdf = GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1)], "value": [1, 2]})
        >>> gdf.to_parquet("output.parquet")
        """
        self.to_file(path, driver="geoparquet", **kwargs)


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def _ensure_geometry(data, crs: Any | None = None) -> sgpd.GeoSeries:
    """
    Ensure the data is of geometry dtype or converted to it.

    If the input is a GeometryDtype with a set CRS, `crs` is ignored.
    """
    if isinstance(data, sgpd.GeoSeries):
        if data.crs is None and crs is not None:
            # Avoids caching issues/crs sharing issues
            data = data.copy()
            data.crs = crs
        return data
    else:
        return sgpd.GeoSeries(data, crs=crs)


# We don't raise AttributeError because that would be caught by pyspark's __getattr__ creating a misleading error message
class MissingGeometryColumnError(Exception):
    pass
