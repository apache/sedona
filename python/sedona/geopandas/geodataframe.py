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
import shapely
import geopandas as gpd
import pandas as pd
import pyspark.pandas as pspd
import sedona.geopandas as sgpd
from pyspark.pandas import Series as PandasOnSparkSeries
from pyspark.pandas._typing import Dtype
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import InternalFrame

from sedona.geopandas._typing import Label
from sedona.geopandas.base import GeoFrame
from sedona.geopandas.sindex import SpatialIndex

from pandas.api.extensions import register_extension_dtype
from geopandas.geodataframe import crs_mismatch_error
from geopandas.array import GeometryDtype
from shapely.geometry.base import BaseGeometry

register_extension_dtype(GeometryDtype)


class GeoDataFrame(GeoFrame, pspd.DataFrame):
    """
    A class representing a GeoDataFrame, inheriting from GeoFrame and pyspark.pandas.DataFrame.
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
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> data = {'geometry': [Point(0, 0), Point(1, 1)], 'value': [1, 2]}
        >>> gdf = GeoDataFrame(data)
        >>> gdf['value']
        0    1
        1    2
        Name: value, dtype: int64
        """

        # Handle column access by name
        if isinstance(key, str):
            # Access column directly from the spark DataFrame
            column_name = key

            # Check if column exists
            if column_name not in self.columns:
                raise KeyError(f"Column '{column_name}' does not exist")

            # Here we are getting a ps.Series with the same underlying anchor (ps.Dataframe).
            # This is important so we don't unnecessarily try to perform operations on different dataframes
            ps_series = pspd.DataFrame.__getitem__(self, column_name)

            try:
                # Relies on sgpd.GeoSeries error-ing if ps_series is not a geometry type
                return sgpd.GeoSeries(ps_series)
            except:
                return ps_series

        # Handle list of column names
        elif isinstance(key, list) and all(isinstance(k, str) for k in key):
            # Check if all columns exist
            missing_cols = [k for k in key if k not in self.columns]
            if missing_cols:
                raise KeyError(f"Columns {missing_cols} do not exist")

            # Select columns from the spark DataFrame
            spark_df = self._internal.spark_frame.select(*key)
            pandas_df = spark_df.toPandas()

            # Return as GeoDataFrame
            return GeoDataFrame(pandas_df)

        # Handle row selection via slice or boolean indexing
        else:
            # For now, convert to pandas first for row-based operations
            # This could be optimized later for better performance
            pandas_df = self._internal.spark_frame.toPandas()
            selected_rows = pandas_df[key]
            return GeoDataFrame(selected_rows)

    _geometry_column_name = None

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

        from sedona.geopandas import GeoSeries
        from pyspark.sql import DataFrame as SparkDataFrame

        # Simplified version of the function from GeoSeries.__init__()
        def try_geom_to_ewkb(x) -> bytes:
            if isinstance(x, BaseGeometry):
                kwargs = {}
                if crs:
                    from pyproj import CRS

                    srid = CRS.from_user_input(crs)
                    kwargs["srid"] = srid.to_epsg()

                return shapely.wkb.dumps(x, **kwargs)
            elif isinstance(x, bytearray):
                return bytes(x)
            elif x is None or isinstance(x, bytes):
                return x
            else:
                raise TypeError(f"expected geometry or bytes, got {type(x)}: {x}")

        def safe_apply_to_each_column(
            df: pd.DataFrame | pspd.DataFrame, func: Callable
        ):
            # try except won't work here for ps.DataFrame case, so we use first_valid_index() instead
            for col in df.columns:
                first_idx = df[col].first_valid_index()
                if first_idx is not None and isinstance(
                    df[col][first_idx], BaseGeometry
                ):
                    df[col] = df[col].apply(try_geom_to_ewkb)
            return df

        if isinstance(data, GeoDataFrame):
            if data._safe_get_crs() is None:
                data.crs = crs

            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)

        elif isinstance(data, GeoSeries):
            if data.crs is None:
                data.crs = crs

            # For each of these super().__init__() calls, we let pyspark decide which inputs are valid or not
            # instead of calling e.g assert not dtype ourselves.
            # This way, if Spark adds support later, than we inherit those changes naturally
            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)
        elif isinstance(data, PandasOnSparkDataFrame):

            data = safe_apply_to_each_column(data, try_geom_to_ewkb)

            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)
        elif isinstance(data, PandasOnSparkSeries):

            try:
                data = GeoSeries(data)
            except TypeError:
                pass

            super().__init__(data, index=index, columns=columns, dtype=dtype, copy=copy)
        elif isinstance(data, SparkDataFrame):
            assert columns is None
            assert dtype is None
            assert not copy

            if index is None:
                internal = InternalFrame(spark_frame=data, index_spark_columns=None)
                object.__setattr__(self, "_internal_frame", internal)
        else:
            # below are not distributed dataframe types
            if isinstance(data, pd.DataFrame):
                assert index is None
                assert dtype is None
                assert not copy
                df = data
            else:
                df = pd.DataFrame(
                    data=data,
                    index=index,
                    dtype=dtype,
                    copy=copy,
                )
            gdf = gpd.GeoDataFrame(df)
            # convert each geometry column to wkb type

            # It's possible we get a list, dict, pd.Series, gpd.GeoSeries, etc of shapely.Geometry objects.
            gdf = safe_apply_to_each_column(gdf, try_geom_to_ewkb)

            pdf = pd.DataFrame(gdf)

            # initialize the parent class pyspark Dataframe with the pandas Series
            super().__init__(
                data=pdf,
                index=index,
                dtype=dtype,
                copy=copy,
            )

        # Set geometry column name
        if isinstance(data, (GeoDataFrame, gpd.GeoDataFrame)):
            self._geometry_column_name = data._geometry_column_name
            if crs is not None and data.crs != crs:
                raise ValueError(crs_mismatch_error)

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

            raise AttributeError(msg)
        return self[self._geometry_column_name]

    def _set_geometry(self, col):
        # This check is included in the original geopandas. Note that this prevents assigning a str to the property
        # e.g. df.geometry = "geometry"
        # However the user can still use specify a str in the public .set_geometry() method
        # ie. df.geometry = "geometry1" errors, but df.set_geometry("geometry1") works
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
        >>> from sedona.geopandas import GeoDataFrame
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
            frame = self.copy(deep=False)

        geo_column_name = self._geometry_column_name

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
            if isinstance(col, (pspd.Series, pd.Series)) and col.name is not None:
                geo_column_name = col.name

            level = col

            if not isinstance(level, pspd.Series):
                level = pspd.Series(level)
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
                "`drop=True` behaviour you should update "
                "your code to\n`geo_col_name = gdf.active_geometry_name;"
                " gdf.set_geometry(new_geo_col).drop("
                "columns=geo_col_name).rename_geometry(geo_col_name)`."
            )

            if drop is False:  # specifically False, not falsy i.e. None
                # User supplied False explicitly, but arg is deprecated
                warnings.warn(
                    given_colname_drop_msg,
                    category=FutureWarning,
                    stacklevel=2,
                )
            if drop:
                raise NotImplementedError("Not implemented.")
            else:
                # if not dropping, set the active geometry name to the given col name
                geo_column_name = col

        if not crs:
            crs = getattr(level, "crs", None)

        # Check that we are using a listlike of geometries
        level = _ensure_geometry(level, crs=crs)
        # ensure_geometry only sets crs on level if it has crs==None

        # This operation throws a warning to the user asking them to set pspd.set_option('compute.ops_on_diff_frames', True)
        # to allow operations on different frames. We pass these warnings on to the user so they must manually set it themselves.
        if level.crs != crs:
            level.set_crs(crs, inplace=True, allow_override=True)

        frame._geometry_column_name = geo_column_name
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
        >>> from sedona.geopandas import GeoDataFrame
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
            if inplace:
                self.rename(columns={geometry_col: col}, inplace=inplace)
                self.set_geometry(col, inplace=inplace)
                return None

            # The same .rename().set_geometry() logic errors for this case, so we do it manually instead
            ps_series = self._psser_for((geometry_col,)).rename(col)
            sdf = self.copy()
            sdf[col] = ps_series
            sdf = sdf.set_geometry(col)
            return sdf

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

    def _process_geometry_columns(
        self, operation: str, rename_suffix: str = "", *args, **kwargs
    ) -> GeoDataFrame:
        """
        Helper method to process geometry columns with a specified operation.

        Parameters
        ----------
        operation : str
            The spatial operation to apply (e.g., 'ST_Area', 'ST_Buffer').
        rename_suffix : str, default ""
            Suffix to append to the resulting column name.
        args : tuple
            Positional arguments for the operation.
        kwargs : dict
            Keyword arguments for the operation.

        Returns
        -------
        GeoDataFrame
            A new GeoDataFrame with the operation applied to geometry columns.
        """
        select_expressions = []

        for field in self._internal.spark_frame.schema.fields:
            col_name = field.name

            # Skip index and order columns
            if col_name in ("__index_level_0__", "__natural_order__"):
                continue

            if field.dataType.typeName() in ("geometrytype", "binary"):
                # Prepare arguments for the operation
                positional_params = ", ".join([repr(v) for v in args])
                keyword_params = ", ".join([repr(v) for v in kwargs.values()])
                params = ", ".join(filter(None, [positional_params, keyword_params]))

                if field.dataType.typeName() == "binary":
                    expr = f"{operation}(ST_GeomFromWKB(`{col_name}`){', ' + params if params else ''}) as {col_name}{rename_suffix}"
                else:
                    expr = f"{operation}(`{col_name}`{', ' + params if params else ''}) as {col_name}{rename_suffix}"
                select_expressions.append(expr)
            else:
                # Keep non-geometry columns as they are
                select_expressions.append(f"`{col_name}`")

        sdf = self._internal.spark_frame.selectExpr(*select_expressions)
        return GeoDataFrame(sdf)

    def to_geopandas(self) -> gpd.GeoDataFrame:
        """
        Note: Unlike in pandas and geopandas, Sedona will always return a general Index.
        This differs from pandas and geopandas, which will return a RangeIndex by default.

        e.g pd.Index([0, 1, 2]) instead of pd.RangeIndex(start=0, stop=3, step=1)
        """
        from pyspark.pandas.utils import log_advice

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
                pd_df[col_name] = series.to_pandas()

        return gpd.GeoDataFrame(pd_df, geometry=self._geometry_column_name)

    @property
    def sindex(self) -> SpatialIndex | None:
        """
        Returns a spatial index for the GeoDataFrame.
        The spatial index allows for efficient spatial queries. If the spatial
        index cannot be created (e.g., no geometry column is present), this
        property will return None.
        Returns:
        - SpatialIndex: The spatial index for the GeoDataFrame.
        - None: If the spatial index is not supported.
        """
        if "geometry" in self.columns:
            return SpatialIndex(self._internal.spark_frame, column_name="geometry")
        return None

    def copy(self, deep=False):
        """
        Make a copy of this GeoDataFrame object.

        Parameters:
        - deep: bool, default False
            If True, a deep copy of the data is made. Otherwise, a shallow copy is made.

        Returns:
        - GeoDataFrame: A copy of this GeoDataFrame object.

        Examples:
        >>> from shapely.geometry import Point
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeoDataFrame

        >>> gdf = GeoDataFrame([{"geometry": Point(1, 1), "value1": 2, "value2": 3}])
        >>> gdf_copy = gdf.copy()
        >>> print(gdf_copy)
           geometry  value1  value2
        0  POINT (1 1)       2       3
        """
        if deep:
            return GeoDataFrame(
                self._anchor.copy(), dtype=self.dtypes, index=self._col_label
            )
        else:
            return self  # GeoDataFrame(self._internal.spark_frame.copy())  "this parameter is not supported but just dummy parameter to match pandas."

    @property
    def area(self) -> GeoDataFrame:
        """
        Returns a GeoDataFrame containing the area of each geometry expressed in the units of the CRS.

        Returns
        -------
        GeoDataFrame
            A GeoDataFrame with the areas of the geometries.

        Examples
        --------
        >>> from shapely.geometry import Polygon
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> data = {
        ...     'geometry': [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        ...     'value': [1, 2]
        ... }
        >>> gdf = GeoDataFrame(data)
        >>> gdf.area
           geometry_area  value
        0           1.0      1
        1           4.0      2
        """
        return self._process_geometry_columns("ST_Area", rename_suffix="_area")

    def _safe_get_crs(self):
        """
        Helper method for getting the crs of the GeoDataframe safely.
        Returns None if no geometry column is set instead of raising an error.
        """
        try:
            return self.geometry.crs
        except AttributeError:
            return None

    @property
    def crs(self):
        return self.geometry.crs

    @crs.setter
    def crs(self, value):
        # Avoid trying to access the geometry column (which might be missing) if crs is None
        if value is None:
            return
        self.geometry.crs = value

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
    def from_file(cls, filename: os.PathLike | typing.IO, **kwargs) -> GeoDataFrame:
        raise NotImplementedError("from_file() is not implemented yet.")

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

        >>> from sedona.geopandas import GeoDataFrame
        >>> import geoarrow.pyarrow as ga
        >>> import pyarrow as pa
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
            Indicates how to output missing (NaN) values in the GeoDataFrame.
            See below.
        show_bbox : bool, optional, default: False
            Include bbox (bounds) in the geojson
        drop_id : bool, default: False
            Whether to retain the index of the GeoDataFrame as the id property
            in the generated GeoJSON. Default is False, but may want True
            if the index is just arbitrary row numbers.
        to_wgs84: bool, optional, default: False
            If the CRS is set on the active geometry column it is exported as
            WGS84 (EPSG:4326) to meet the `2016 GeoJSON specification
            <https://tools.ietf.org/html/rfc7946>`_.
            Set to True to force re-projection and set to False to ignore CRS. False by
            default.
        Notes
        -----
        The remaining *kwargs* are passed to json.dumps().
        Missing (NaN) values in the GeoDataFrame can be represented as follows:
        - ``null``: output the missing entries as JSON null.
        - ``drop``: remove the property from the feature. This applies to each
          feature individually so that features may have different properties.
        - ``keep``: output the missing entries as NaN.
        If the GeoDataFrame has a defined CRS, its definition will be included
        in the output unless it is equal to WGS84 (default GeoJSON CRS) or not
        possible to represent in the URN OGC format, or unless ``to_wgs84=True``
        is specified.
        Examples
        --------
        >>> from sedona.geopandas import GeoDataFrame
        >>> from shapely.geometry import Point
        >>> d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
        >>> gdf = GeoDataFrame(d, crs="EPSG:3857")
        >>> gdf
            col1     geometry
        0  name1  POINT (1 2)
        1  name2  POINT (2 1)
        >>> gdf.to_json()
        '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", \
"properties": {"col1": "name1"}, "geometry": {"type": "Point", "coordinates": [1.0,\
 2.0]}}, {"id": "1", "type": "Feature", "properties": {"col1": "name2"}, "geometry"\
: {"type": "Point", "coordinates": [2.0, 1.0]}}], "crs": {"type": "name", "properti\
es": {"name": "urn:ogc:def:crs:EPSG::3857"}}}'
        Alternatively, you can write GeoJSON to file:
        >>> gdf.to_file(path, driver="GeoJSON")  # doctest: +SKIP
        See also
        --------
        GeoDataFrame.to_file : write GeoDataFrame to file
        """
        # Because this function returns the geojson string in memory,
        # we simply rely on geopandas's implementation.
        # Additionally, spark doesn't seem to have a straight forward way to get the string
        # without writing to a file first by using sdf.write.format("geojson").save(path, **kwargs)
        # return self.to_geopandas().to_json(na, show_bbox, drop_id, to_wgs84, **kwargs)
        # ST_AsGeoJSON() works only for one column
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
        >>> from sedona.geopandas import GeoDataFrame
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
        >>> import pyarrow as pa
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
        # Because this function returns the arrow table in memory, we simply rely on geopandas's implementation.
        # This also returns a geopandas specific data type, which can be converted to an actual pyarrow table,
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

    def to_file(
        self,
        filename: str,
        driver: str | None = None,
        schema: dict | None = None,
        index: bool | None = None,
        **kwargs,
    ):
        raise NotImplementedError("to_file() is not implemented yet.")

    @property
    def geom_type(self) -> str:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def type(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def length(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_valid(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def is_valid_reason(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_empty(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def count_coordinates(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def count_geometries(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def count_interior_rings(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_simple(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_ring(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_ccw(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_closed(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def has_z(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def get_precision(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def get_geometry(self, index):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def boundary(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def centroid(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def concave_hull(self, ratio=0.0, allow_holes=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def convex_hull(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def delaunay_triangles(self, tolerance=0.0, only_edges=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def voronoi_polygons(self, tolerance=0.0, extend_to=None, only_edges=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def envelope(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_rotated_rectangle(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def exterior(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def extract_unique_points(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def offset_curve(self, distance, quad_segs=8, join_style="round", mitre_limit=5.0):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def interiors(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def remove_repeated_points(self, tolerance=0.0):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def set_precision(self, grid_size, mode="valid_output"):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def representative_point(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_bounding_circle(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_bounding_radius(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_clearance(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def normalize(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def make_valid(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def reverse(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def segmentize(self, max_segment_length):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def transform(self, transformation, include_z=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def force_2d(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def force_3d(self, z=0):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def line_merge(self, directed=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def unary_union(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def union_all(self, method="unary", grid_size=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def intersection_all(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def contains(self, other, align=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def contains_properly(self, other, align=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def buffer(
        self,
        distance,
        resolution=16,
        cap_style="round",
        join_style="round",
        mitre_limit=5.0,
        single_sided=False,
        **kwargs,
    ) -> GeoDataFrame:
        """
        Returns a GeoDataFrame with all geometries buffered by the specified distance.

        Parameters
        ----------
        distance : float
            The distance to buffer by. Negative distances will create inward buffers.
        resolution : int, default 16
            The number of segments used to approximate curves.
        cap_style : str, default "round"
            The style of the buffer cap. One of 'round', 'flat', 'square'.
        join_style : str, default "round"
            The style of the buffer join. One of 'round', 'mitre', 'bevel'.
        mitre_limit : float, default 5.0
            The mitre limit ratio for joins when join_style='mitre'.
        single_sided : bool, default False
            Whether to create a single-sided buffer.

        Returns
        -------
        GeoDataFrame
            A new GeoDataFrame with buffered geometries.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> data = {
        ...     'geometry': [Point(0, 0), Point(1, 1)],
        ...     'value': [1, 2]
        ... }
        >>> gdf = GeoDataFrame(data)
        >>> buffered = gdf.buffer(0.5)
        """
        return self._process_geometry_columns(
            "ST_Buffer", rename_suffix="_buffered", distance=distance
        )

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
            * 'inner': use intersection of keys from both dfs; retain only
              left_df geometry column
        predicate : str, default 'intersects'
            Binary predicate. Valid values: 'intersects', 'contains', 'within', 'dwithin'
        lsuffix : str, default 'left'
            Suffix to apply to overlapping column names (left GeoDataFrame).
        rsuffix : str, default 'right'
            Suffix to apply to overlapping column names (right GeoDataFrame).
        distance : float, optional
            Distance for 'dwithin' predicate. Required if predicate='dwithin'.
        on_attribute : str, list or tuple, optional
            Column name(s) to join on as an additional join restriction.
            These must be found in both DataFrames.

        Returns
        -------
        GeoDataFrame
            A GeoDataFrame with the results of the spatial join.

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> polygons = GeoDataFrame({
        ...     'geometry': [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])],
        ...     'value': [1]
        ... })
        >>> points = GeoDataFrame({
        ...     'geometry': [Point(0.5, 0.5), Point(2, 2)],
        ...     'value': [1, 2]
        ... })
        >>> joined = points.sjoin(polygons)
        """
        from sedona.geopandas.tools.sjoin import sjoin as sjoin_tool

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

    def to_parquet(self, path, **kwargs):
        """
        Write the GeoSeries to a GeoParquet file.

        Parameters:
        - path: str
            The file path where the GeoParquet file will be written.
        - kwargs: Any
            Additional arguments to pass to the Sedona DataFrame output function.
        """
        # Use the Spark DataFrame's write method to write to GeoParquet format
        self._internal.spark_frame.write.format("geoparquet").save(path, **kwargs)


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def _ensure_geometry(data, crs: Any | None = None) -> sgpd.GeoSeries:
    """
    Ensure the data is of geometry dtype or converted to it.

    If input is a (Geo)Series, output is a GeoSeries, otherwise output
    is GeometryArray.

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
