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

import typing
from typing import Any, Union, Literal, List

import numpy as np
import geopandas as gpd
import sedona.spark.geopandas as sgpd
import pandas as pd
import pyspark.pandas as pspd
import pyspark
from pyspark.pandas import Series as PandasOnSparkSeries
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import InternalFrame
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import scol_for
from pyspark.sql.types import NullType
from sedona.spark.sql.types import GeometryType

from sedona.spark.sql import st_aggregates as sta
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp

from pyspark.sql import Column as PySparkColumn
from pyspark.sql import functions as F

import shapely
from shapely.geometry.base import BaseGeometry

from sedona.spark.geopandas._typing import Label
from sedona.spark.geopandas.base import GeoFrame
from sedona.spark.geopandas.geodataframe import GeoDataFrame
from sedona.spark.geopandas.sindex import SpatialIndex
from packaging.version import parse as parse_version

from pyspark.pandas.internal import (
    SPARK_DEFAULT_INDEX_NAME,  # __index_level_0__
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_DEFAULT_SERIES_NAME,  # '0'
)


# ============================================================================
# IMPLEMENTATION STATUS TRACKING
# ============================================================================

IMPLEMENTATION_STATUS = {
    "IMPLEMENTED": [
        "area",
        "buffer",
        "bounds",
        "centroid",
        "contains",
        "crs",
        "distance",
        "envelope",
        "geometry",
        "intersection",
        "intersects",
        "is_empty",
        "is_simple",
        "is_valid",
        "is_valid_reason",
        "length",
        "make_valid",
        "relate",
        "set_crs",
        "to_crs",
        "to_geopandas",
        "to_wkb",
        "to_wkt",
        "x",
        "y",
        "z",
        "has_z",
        "get_geometry",
        "boundary",
        "total_bounds",
        "estimate_utm_crs",
        "isna",
        "isnull",
        "notna",
        "notnull",
        "from_xy",
        "copy",
        "geom_type",
        "sindex",
    ],
    "NOT_IMPLEMENTED": [
        "clip",
        "contains_properly",
        "convex_hull",
        "count_coordinates",
        "count_geometries",
        "count_interior_rings",
        "explode",
        "force_2d",
        "force_3d",
        "from_file",
        "from_shapely",
        "from_arrow",
        "line_merge",
        "reverse",
        "segmentize",
        "to_json",
        "to_arrow",
        "to_file",
        "transform",
        "unary_union",
        "union_all",
        "intersection_all",
        "type",
        "is_ring",
        "is_ccw",
        "is_closed",
        "get_precision",
        "concave_hull",
        "delaunay_triangles",
        "voronoi_polygons",
        "minimum_rotated_rectangle",
        "exterior",
        "extract_unique_points",
        "offset_curve",
        "interiors",
        "remove_repeated_points",
        "set_precision",
        "representative_point",
        "minimum_bounding_circle",
        "minimum_bounding_radius",
        "minimum_clearance",
        "normalize",
        "m",
    ],
    "PARTIALLY_IMPLEMENTED": [
        "fillna",  # Limited parameter support (no 'limit' parameter)
        "from_wkb",
        "from_wkt",  # Limited error handling options (only 'raise' supported)
    ],
}

IMPLEMENTATION_PRIORITY = {
    "HIGH": [
        "contains",
        "contains_properly",
        "convex_hull",
        "explode",
        "clip",
        "from_shapely",
        "count_coordinates",
        "count_geometries",
        "is_ring",
        "is_closed",
        "reverse",
    ],
    "MEDIUM": [
        "force_2d",
        "force_3d",
        "transform",
        "segmentize",
        "line_merge",
        "unary_union",
        "union_all",
        "to_json",
        "from_file",
        "count_interior_rings",
    ],
    "LOW": [
        "delaunay_triangles",
        "voronoi_polygons",
        "minimum_bounding_circle",
        "representative_point",
        "extract_unique_points",
        "from_arrow",
        "to_arrow",
    ],
}


def _not_implemented_error(method_name: str, additional_info: str = "") -> str:
    """
    Generate a standardized NotImplementedError message.

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
        f"GeoSeries.{method_name}() is not implemented yet.\n"
        f"This method will be added in a future release."
    )

    if additional_info:
        base_message += f"\n\n{additional_info}"

    workaround = (
        "\n\nTemporary workaround - use GeoPandas:\n"
        "  gpd_series = sedona_series.to_geopandas()\n"
        f"  result = gpd_series.{method_name}(...)\n"
        "  # Note: This will collect all data to the driver."
    )

    return base_message + workaround


class GeoSeries(GeoFrame, pspd.Series):
    """
    A pandas-on-Spark Series for geometric/spatial operations.

    GeoSeries extends pyspark.pandas.Series to provide spatial operations
    using Apache Sedona's spatial functions. It maintains compatibility
    with GeoPandas GeoSeries while operating on distributed datasets.

    Parameters
    ----------
    data : array-like, Iterable, dict, or scalar value
        Contains the data for the GeoSeries. Can be geometries, WKB bytes,
        or other GeoSeries/GeoDataFrame objects.
    index : array-like or Index (1d), optional
        Values must be hashable and have the same length as `data`.
    crs : pyproj.CRS, optional
        Coordinate Reference System for the geometries.
    dtype : dtype, optional
        Data type for the GeoSeries.
    name : str, optional
        Name of the GeoSeries.
    copy : bool, default False
        Whether to copy the input data.

    Examples
    --------
    >>> from shapely.geometry import Point, Polygon
    >>> from sedona.spark.geopandas import GeoSeries
    >>>
    >>> # Create from geometries
    >>> s = GeoSeries([Point(0, 0), Point(1, 1)], crs='EPSG:4326')
    >>> s
    0    POINT (0 0)
    1    POINT (1 1)
    dtype: geometry
    >>>
    >>> # Spatial operations
    >>> s.buffer(0.1).area
    0    0.031416
    1    0.031416
    dtype: float64
    >>>
    >>> # CRS operations
    >>> s_utm = s.to_crs('EPSG:32633')
    >>> s_utm.crs
    <Projected CRS: EPSG:32633>
    Name: WGS 84 / UTM zone 33N
    ...

    Notes
    -----
    This implementation differs from GeoPandas in several ways:
    - Uses Spark for distributed processing
    - Geometries are stored in WKB (Well-Known Binary) format internally
    - Some methods may have different performance characteristics
    - Not all GeoPandas methods are implemented yet (see IMPLEMENTATION_STATUS)

    Performance Considerations:
    - Operations are distributed across Spark cluster
    - Avoid calling .to_geopandas() on large datasets
    - Use .sample() for testing with large datasets

    See Also
    --------
    geopandas.GeoSeries : The GeoPandas equivalent
    sedona.spark.geopandas.GeoDataFrame : DataFrame with geometry column
    """

    def __getitem__(self, key: Any) -> Any:
        return pspd.Series.__getitem__(self, key)

    def __init__(
        self,
        data=None,
        index=None,
        dtype=None,
        name=None,
        copy=False,
        fastpath=False,
        crs=None,
        **kwargs,
    ):
        """
        Initialize a GeoSeries object.

        Parameters
        ----------
        data : array-like, GeoDataFrame, GeoSeries, or pandas Series
            The input data for the GeoSeries.
        index : array-like, optional
            The index for the GeoSeries.
        crs : pyproj.CRS, optional
            Coordinate Reference System for the GeoSeries.
        dtype : dtype, optional
            Data type for the GeoSeries.
        name : str, optional
            Name of the GeoSeries.
        copy : bool, default False
            Whether to copy the input data.
        fastpath : bool, default False
            Internal parameter for fast initialization.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> import geopandas as gpd
        >>> import pandas as pd
        >>> from sedona.spark.geopandas import GeoSeries

        # Example 1: Initialize with GeoDataFrame
        >>> gdf = gpd.GeoDataFrame({'geometry': [Point(1, 1), Point(2, 2)]})
        >>> gs = GeoSeries(data=gdf)
        >>> print(gs)
        0    POINT (1 1)
        1    POINT (2 2)
        Name: geometry, dtype: geometry

        # Example 2: Initialize with GeoSeries
        >>> gseries = gpd.GeoSeries([Point(1, 1), Point(2, 2)])
        >>> gs = GeoSeries(data=gseries)
        >>> print(gs)
        0    POINT (1 1)
        1    POINT (2 2)
        dtype: geometry

        # Example 3: Initialize with pandas Series
        >>> pseries = pd.Series([Point(1, 1), Point(2, 2)])
        >>> gs = GeoSeries(data=pseries)
        >>> print(gs)
        0    POINT (1 1)
        1    POINT (2 2)
        dtype: geometry
        """
        assert data is not None

        self._anchor: GeoDataFrame
        self._col_label: Label
        self._sindex: SpatialIndex = None

        if isinstance(
            data, (GeoDataFrame, GeoSeries, PandasOnSparkSeries, PandasOnSparkDataFrame)
        ):
            assert dtype is None
            assert name is None
            assert not copy
            assert not fastpath

            # We don't check CRS validity to keep the operation lazy.
            # Keep the original code for now.
            # data_crs = None
            # if hasattr(data, "crs"):
            #     data_crs = data.crs
            # if data_crs is not None and crs is not None and data_crs != crs:
            #     raise ValueError(
            #         "CRS mismatch between CRS of the passed geometries "
            #         "and 'crs'. Use 'GeoSeries.set_crs(crs, "
            #         "allow_override=True)' to overwrite CRS or "
            #         "'GeoSeries.to_crs(crs)' to reproject geometries. "
            #     )

            # PySpark Pandas' ps.Series.__init__() does not support construction from a
            # ps.Series input. For now, we manually implement the logic.

            index = data._col_label if index is None else index
            ps_df = pspd.DataFrame(data._anchor)

            super().__init__(
                data=ps_df,
                index=index,
                dtype=dtype,
                name=name,
                copy=copy,
                fastpath=fastpath,
            )
        else:
            if isinstance(data, pd.Series):
                assert index is None
                assert dtype is None
                assert name is None
                assert not copy
                assert not fastpath
                pd_series = data
            else:
                pd_series = pd.Series(
                    data=data,
                    index=index,
                    dtype=dtype,
                    name=name,
                    copy=copy,
                    fastpath=fastpath,
                )

            pd_series = pd_series.astype(object)

            # Initialize the parent class PySpark Series with the pandas Series.
            super().__init__(data=pd_series)

        # Ensure we're storing geometry types.
        if (
            self.spark.data_type != GeometryType()
            and self.spark.data_type != NullType()
        ):
            raise TypeError(
                "Non geometry data passed to GeoSeries constructor, "
                f"received data of dtype '{self.spark.data_type.typeName()}'"
            )

        if crs:
            self.set_crs(crs, inplace=True)

    # ============================================================================
    # COORDINATE REFERENCE SYSTEM (CRS) OPERATIONS
    # ============================================================================

    @property
    def crs(self) -> Union["CRS", None]:
        """The Coordinate Reference System (CRS) as a ``pyproj.CRS`` object.

        Returns None if the CRS is not set, and to set the value it
        :getter: Returns a ``pyproj.CRS`` or None. When setting, the value
        can be anything accepted by
        :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
        such as an authority string (eg "EPSG:4326") or a WKT string.

        Note: This assumes all records in the GeoSeries are assumed to have the same CRS.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoSeries
        >>> s = GeoSeries([Point(1, 1), Point(2, 2)], crs='EPSG:4326')
        >>> s.crs  # doctest: +SKIP
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

        See Also
        --------
        GeoSeries.set_crs : assign CRS
        GeoSeries.to_crs : re-project to another CRS
        """
        from pyproj import CRS

        if len(self) == 0:
            return None

        # F.first is non-deterministic, but it doesn't matter because all non-null values should be the same.
        spark_col = stf.ST_SRID(F.first(self.spark.column, ignorenulls=True))

        # Set this to avoid error complaining that we don't have a groupby column.
        tmp_series = self._query_geometry_column(
            spark_col,
            returns_geom=False,
            is_aggr=True,
        )

        # All geometries should have the same SRID,
        # so we just take the SRID of the first non-null element.
        srid = tmp_series.item()
        # Turn np.nan to 0 to avoid error.
        srid = 0 if np.isnan(srid) else srid

        # Sedona returns 0 if SRID doesn't exist.
        return CRS.from_user_input(srid) if srid != 0 else None

    @crs.setter
    def crs(self, value: Union["CRS", None]):
        self.set_crs(value, inplace=True)

    @typing.overload
    def set_crs(
        self,
        crs: Union[Any, None] = None,
        epsg: Union[int, None] = None,
        inplace: Literal[True] = True,
        allow_override: bool = False,
    ) -> None: ...

    @typing.overload
    def set_crs(
        self,
        crs: Union[Any, None] = None,
        epsg: Union[int, None] = None,
        inplace: Literal[False] = False,
        allow_override: bool = False,
    ) -> "GeoSeries": ...

    def set_crs(
        self,
        crs: Union[Any, None] = None,
        epsg: Union[int, None] = None,
        inplace: bool = False,
        allow_override: bool = True,
    ) -> Union["GeoSeries", None]:
        """
        Set the Coordinate Reference System (CRS) of a ``GeoSeries``.

        Pass ``None`` to remove CRS from the ``GeoSeries``.

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
        epsg : int, optional if `crs` is specified
            EPSG code specifying the projection.
        inplace : bool, default False
            If True, the CRS of the GeoSeries will be changed in place
            (while still returning the result) instead of making a copy of
            the GeoSeries.
        allow_override : bool, default True
            If the GeoSeries already has a CRS, allow to replace the
            existing CRS, even when both are not equal. In Sedona, setting this to True
            will lead to eager evaluation instead of lazy evaluation. Unlike Geopandas,
            True is the default value in Sedona for performance reasons.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry

        Setting CRS to a GeoSeries without one:

        >>> s.crs is None
        True

        >>> s = s.set_crs('epsg:3857')
        >>> s.crs  # doctest: +SKIP
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

        >>> s = s.set_crs(4326, allow_override=True)

        Without ``allow_override=True``, ``set_crs`` returns an error if you try to
        override CRS.

        See Also
        --------
        GeoSeries.to_crs : re-project to another CRS

        """
        from pyproj import CRS

        if crs is not None:
            crs = CRS.from_user_input(crs)
        elif epsg is not None:
            crs = CRS.from_epsg(epsg)

        # The below block for the not allow_override case is eager due to the self.crs call.
        # This hurts performance and user experience, hence the default being set to True in Sedona.
        if not allow_override:
            curr_crs = self.crs

            if curr_crs is not None and not curr_crs == crs:
                raise ValueError(
                    "The GeoSeries already has a CRS which is not equal to the passed "
                    "CRS. Specify 'allow_override=True' to allow replacing the existing "
                    "CRS without doing any transformation. If you actually want to "
                    "transform the geometries, use 'GeoSeries.to_crs' instead."
                )

        # 0 indicates no SRID in Sedona.
        new_epsg = crs.to_epsg() if crs else 0

        spark_col = stf.ST_SetSRID(self.spark.column, new_epsg)
        result = self._query_geometry_column(spark_col, keep_name=True)

        if inplace:
            self._update_inplace(result, invalidate_sindex=False)
            return None

        return result

    # ============================================================================
    # INTERNAL HELPER METHODS
    # ============================================================================

    def _query_geometry_column(
        self,
        spark_col: PySparkColumn,
        df: pyspark.sql.DataFrame = None,
        returns_geom: bool = True,
        is_aggr: bool = False,
        keep_name: bool = False,
    ) -> Union["GeoSeries", pspd.Series]:
        """
        Helper method to query a single geometry column with a specified operation.

        Parameters
        ----------
        spark_col : str
            The query to apply to the geometry column.
        df : pyspark.sql.DataFrame
            The dataframe to query. If not provided, the internal dataframe will be used.
        returns_geom : bool, default True
            If True, the geometry column will be converted back to EWKB format.
        is_aggr : bool, default False
            If True, the query is an aggregation query.

        Returns
        -------
        GeoSeries
            A GeoSeries with the operation applied to the geometry column.
        """

        df = self._internal.spark_frame if df is None else df

        rename = SPARK_DEFAULT_SERIES_NAME

        if keep_name and self.name:
            rename = self.name

        col_expr = spark_col.alias(rename)

        exprs = [col_expr]

        index_spark_columns = []
        index_fields = []
        if not is_aggr:
            # We always select NATURAL_ORDER_COLUMN_NAME, to avoid having to regenerate it in the result.
            # We always select SPARK_DEFAULT_INDEX_NAME, to retain series index info.

            exprs.append(scol_for(df, SPARK_DEFAULT_INDEX_NAME))
            exprs.append(scol_for(df, NATURAL_ORDER_COLUMN_NAME))

            index_spark_columns = [scol_for(df, SPARK_DEFAULT_INDEX_NAME)]
            index_fields = [self._internal.index_fields[0]]
            sdf = df.select(
                col_expr,
                scol_for(df, SPARK_DEFAULT_INDEX_NAME),
                scol_for(df, NATURAL_ORDER_COLUMN_NAME),
            )
        # Otherwise, if is_aggr, we don't select the index columns.
        else:
            sdf = df.select(*exprs)

        internal = self._internal.copy(
            spark_frame=sdf,
            index_fields=index_fields,
            index_spark_columns=index_spark_columns,
            data_spark_columns=[scol_for(sdf, rename)],
            data_fields=[self._internal.data_fields[0].copy(name=rename)],
            column_label_names=[(rename,)],
        )
        ps_series = first_series(PandasOnSparkDataFrame(internal))

        # Convert Spark series default name to pandas series default name (None) if needed.
        series_name = None if rename == SPARK_DEFAULT_SERIES_NAME else rename
        ps_series = ps_series.rename(series_name)

        result = GeoSeries(ps_series) if returns_geom else ps_series
        return result

    # ============================================================================
    # CONVERSION AND SERIALIZATION METHODS
    # ============================================================================

    def to_geopandas(self) -> gpd.GeoSeries:
        """
        Convert the GeoSeries to a GeoPandas GeoSeries.

        Returns
        -------
        geopandas.GeoSeries
            A GeoPandas GeoSeries.
        """
        from pyspark.pandas.utils import log_advice

        log_advice(
            "`to_geopandas` loads all data into the driver's memory. "
            "It should only be used if the resulting GeoPandas GeoSeries is expected to be small."
        )
        return self._to_geopandas()

    def _to_geopandas(self) -> gpd.GeoSeries:
        """
        Same as `to_geopandas()`, without issuing the advice log for internal usage.
        """
        pd_series = self._to_internal_pandas()
        return gpd.GeoSeries(pd_series, crs=self.crs)

    def to_spark_pandas(self) -> pspd.Series:
        return pspd.Series(pspd.DataFrame(self._psdf._internal))

    # ============================================================================
    # PROPERTIES AND ATTRIBUTES
    # ============================================================================

    @property
    def geometry(self) -> "GeoSeries":
        return self

    @property
    def sindex(self) -> SpatialIndex:
        geometry_column = _get_series_col_name(self)
        if geometry_column is None:
            raise ValueError("No geometry column found in GeoSeries")
        if self._sindex is None:
            self._sindex = SpatialIndex(self)
        return self._sindex

    @property
    def has_sindex(self):
        return self._sindex is not None

    def copy(self, deep=False):
        """Make a copy of this GeoSeries object.

        Parameters
        ----------
        deep : bool, default False
            If True, a deep copy of the data is made. Otherwise, a shallow
            copy is made.

        Returns
        -------
        GeoSeries
            A copy of this GeoSeries object.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoSeries
        >>> gs = GeoSeries([Point(1, 1), Point(2, 2)])
        >>> gs_copy = gs.copy()
        >>> print(gs_copy)
        0    POINT (1 1)
        1    POINT (2 2)
        dtype: geometry
        """
        if deep:
            return GeoSeries(
                self._anchor.copy(), dtype=self.dtype, index=self._col_label
            )
        else:
            return self

    @property
    def area(self) -> pspd.Series:
        spark_col = stf.ST_Area(self.spark.column)

        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def geom_type(self) -> pspd.Series:
        spark_col = stf.ST_GeometryType(self.spark.column)
        result = self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

        # ST_GeometryType returns string as 'ST_Point'
        # we crop the prefix off to get 'Point'
        result = result.map(lambda x: x[3:])
        return result

    @property
    def type(self):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error("type", "Returns numeric geometry type codes.")
        )

    @property
    def length(self) -> pspd.Series:
        spark_expr = (
            F.when(
                stf.GeometryType(self.spark.column).isin(
                    ["LINESTRING", "MULTILINESTRING"]
                ),
                stf.ST_Length(self.spark.column),
            )
            .when(
                stf.GeometryType(self.spark.column).isin(["POLYGON", "MULTIPOLYGON"]),
                stf.ST_Perimeter(self.spark.column),
            )
            .when(
                stf.GeometryType(self.spark.column).isin(["POINT", "MULTIPOINT"]),
                0.0,
            )
            .when(
                stf.GeometryType(self.spark.column).isin(["GEOMETRYCOLLECTION"]),
                stf.ST_Length(self.spark.column) + stf.ST_Perimeter(self.spark.column),
            )
        )
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    @property
    def is_valid(self) -> pspd.Series:
        spark_col = stf.ST_IsValid(self.spark.column)
        result = self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )
        return _to_bool(result)

    def is_valid_reason(self) -> pspd.Series:
        spark_col = stf.ST_IsValidReason(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def is_empty(self) -> pspd.Series:
        spark_expr = stf.ST_IsEmpty(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    def count_coordinates(self):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error(
                "count_coordinates",
                "Counts the number of coordinate tuples in each geometry.",
            )
        )

    def count_geometries(self):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error(
                "count_geometries",
                "Counts the number of geometries in each multi-geometry or collection.",
            )
        )

    def count_interior_rings(self):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error(
                "count_interior_rings",
                "Counts the number of interior rings (holes) in each polygon.",
            )
        )

    def dwithin(self, other, distance, align=None):
        if not isinstance(distance, (float, int)):
            raise NotImplementedError(
                "Array-like distance for dwithin not implemented yet."
            )

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_DWithin(F.col("L"), F.col("R"), F.lit(distance))
        return self._row_wise_operation(
            spark_expr,
            other_series,
            align=align,
            returns_geom=False,
            default_val=False,
        )

    def difference(self, other, align=None) -> "GeoSeries":
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_Difference(F.col("L"), F.col("R"))
        return self._row_wise_operation(
            spark_expr,
            other_series,
            align=align,
            returns_geom=True,
        )

    def symmetric_difference(self, other, align=None) -> "GeoSeries":
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_SymDifference(F.col("L"), F.col("R"))
        return self._row_wise_operation(
            spark_expr,
            other_series,
            align=align,
            returns_geom=True,
        )

    def union(self, other, align=None) -> "GeoSeries":
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_Union(F.col("L"), F.col("R"))
        return self._row_wise_operation(
            spark_expr,
            other_series,
            align=align,
            returns_geom=True,
        )

    @property
    def is_simple(self) -> pspd.Series:
        spark_expr = stf.ST_IsSimple(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    @property
    def is_ring(self):
        spark_expr = stf.ST_IsRing(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    @property
    def is_ccw(self):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error(
                "is_ccw",
                "Tests if LinearRing geometries are oriented counter-clockwise.",
            )
        )

    @property
    def is_closed(self):
        # Only check LineStrings; return False for all other geometry types
        spark_expr = F.when(
            stf.ST_GeometryType(self.spark.column) == "ST_LineString",
            stf.ST_IsClosed(self.spark.column),
        ).otherwise(False)

        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    @property
    def has_z(self) -> pspd.Series:
        spark_expr = stf.ST_HasZ(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def get_precision(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def get_geometry(self, index) -> "GeoSeries":
        # Sedona errors on negative indexes, so we use a case statement to handle it ourselves.
        spark_expr = stf.ST_GeometryN(
            F.col("L"),
            F.when(
                stf.ST_NumGeometries(F.col("L")) + F.col("R") < 0,
                None,
            )
            .when(F.col("R") < 0, stf.ST_NumGeometries(F.col("L")) + F.col("R"))
            .otherwise(F.col("R")),
        )

        other, _ = self._make_series_of_val(index)

        # align = False either way
        align = False

        return self._row_wise_operation(
            spark_expr,
            other,
            align=align,
            returns_geom=True,
            default_val=None,
        )

    @property
    def boundary(self) -> "GeoSeries":
        # Geopandas and shapely return NULL for GeometryCollections, so we handle it separately
        # https://shapely.readthedocs.io/en/stable/reference/shapely.boundary.html
        spark_expr = F.when(
            stf.GeometryType(self.spark.column).isin(["GEOMETRYCOLLECTION"]),
            None,
        ).otherwise(stf.ST_Boundary(self.spark.column))
        return self._query_geometry_column(
            spark_expr,
        )

    @property
    def centroid(self) -> "GeoSeries":
        spark_expr = stf.ST_Centroid(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=True,
        )

    def concave_hull(self, ratio=0.0, allow_holes=False):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def convex_hull(self):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error(
                "convex_hull", "Computes the convex hull of each geometry."
            )
        )

    def delaunay_triangles(self, tolerance=0.0, only_edges=False):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def voronoi_polygons(self, tolerance=0.0, extend_to=None, only_edges=False):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def envelope(self) -> "GeoSeries":
        spark_expr = stf.ST_Envelope(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=True,
        )

    def minimum_rotated_rectangle(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def exterior(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def extract_unique_points(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def offset_curve(self, distance, quad_segs=8, join_style="round", mitre_limit=5.0):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def interiors(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def remove_repeated_points(self, tolerance=0.0):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def set_precision(self, grid_size, mode="valid_output"):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def representative_point(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_bounding_circle(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_bounding_radius(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_clearance(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def normalize(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def make_valid(self, *, method="linework", keep_collapsed=True) -> "GeoSeries":
        if method != "structure":
            raise ValueError(
                "Sedona only supports the 'structure' method for make_valid"
            )

        spark_expr = stf.ST_MakeValid(self.spark.column, keep_collapsed)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=True,
        )

    def reverse(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def segmentize(self, max_segment_length):
        other_series, extended = self._make_series_of_val(max_segment_length)
        align = False if extended else align

        spark_expr = stf.ST_Segmentize(F.col("L"), F.col("R"))
        return self._row_wise_operation(
            spark_expr,
            other_series,
            align=align,
            returns_geom=True,
        )

    def transform(self, transformation, include_z=False):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def force_2d(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def force_3d(self, z=0):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def line_merge(self, directed=False):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    # ============================================================================
    # GEOMETRIC OPERATIONS
    # ============================================================================

    @property
    def unary_union(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def union_all(self, method="unary", grid_size=None) -> BaseGeometry:
        if grid_size is not None:
            raise NotImplementedError("Sedona does not support the grid_size argument")
        if method != "unary":
            import warnings

            warnings.warn(
                f"Sedona does not support manually specifying different union methods. Ignoring non-default method argument of {method}"
            )

        if len(self) == 0:
            # While it's not explicitly defined in GeoPandas docs, this is what GeoPandas returns for empty GeoSeries.
            # If it ever changes for some reason, we'll catch that with the test
            from shapely.geometry import GeometryCollection

            return GeometryCollection()

        spark_expr = sta.ST_Union_Aggr(self.spark.column)
        tmp = self._query_geometry_column(spark_expr, returns_geom=False, is_aggr=True)

        ps_series = tmp.take([0])
        geom = ps_series.iloc[0]
        return geom

    def crosses(self, other, align=None) -> pspd.Series:
        # Sedona does not support GeometryCollection (errors), so we return NULL for now to avoid error.
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = F.when(
            (stf.GeometryType(F.col("L")) == "GEOMETRYCOLLECTION")
            | (stf.GeometryType(F.col("R")) == "GEOMETRYCOLLECTION"),
            None,
        ).otherwise(stp.ST_Crosses(F.col("L"), F.col("R")))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )

        return _to_bool(result)

    def disjoint(self, other, align=None):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    def intersects(
        self, other: Union["GeoSeries", BaseGeometry], align: Union[bool, None] = None
    ) -> pspd.Series:
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Intersects(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return _to_bool(result)

    def overlaps(self, other, align=None) -> pspd.Series:
        # Note: We cannot efficiently match GeoPandas behavior because Sedona's ST_Overlaps returns True for equal geometries.
        # ST_Overlaps(`L`, `R`) AND ST_Equals(`L`, `R`) does not work because ST_Equals errors on invalid geometries

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Overlaps(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return _to_bool(result)

    def touches(self, other, align=None) -> pspd.Series:
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Touches(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return _to_bool(result)

    def within(self, other, align=None) -> pspd.Series:
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Within(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return _to_bool(result)

    def covers(self, other, align=None) -> pspd.Series:
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Covers(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return _to_bool(result)

    def covered_by(self, other, align=None) -> pspd.Series:
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_CoveredBy(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return _to_bool(result)

    def distance(self, other, align=None) -> pspd.Series:
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_Distance(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=None,
        )
        return result

    def intersection(
        self, other: Union["GeoSeries", BaseGeometry], align: Union[bool, None] = None
    ) -> "GeoSeries":
        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_Intersection(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            returns_geom=True,
            default_val=None,
        )
        return result

    def snap(self, other, tolerance, align=None) -> "GeoSeries":
        if not isinstance(tolerance, (float, int)):
            raise NotImplementedError(
                "Array-like values for tolerance are not supported yet."
            )

        # Both sgpd and gpd implementations simply call the snap functions
        # in JTS and GEOs, respectively. The results often differ slightly, but these
        # must be differences inside of the engines themselves.

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_Snap(F.col("L"), F.col("R"), tolerance)
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            returns_geom=True,
        )
        return result

    def _row_wise_operation(
        self,
        spark_col: PySparkColumn,
        other: pspd.Series,
        align: Union[bool, None],
        returns_geom: bool = False,
        default_val: Any = None,
        keep_name: bool = False,
    ):
        """
        Helper function to perform a row-wise operation on two GeoSeries.
        The self column and other column are aliased to `L` and `R`, respectively.

        align : bool or None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.
            Note: align should also be set to False when 'other' a geoseries created from a single object
            (e.g. GeoSeries([Point(0, 0) * len(self)])), so that we align based on natural ordering in case
            the index is not the default range index from 0.
            Alternatively, we could create 'other' using the same index as self,
            but that would require index=self.index.to_pandas() which is less scalable.

        default_val : Any (default None)
            The value to use if either L or R is null. If None, nulls are not handled.
        """
        from pyspark.sql.functions import col

        # Note: this is specifically False. None is valid since it defaults to True similar to GeoPandas.
        index_col = (
            NATURAL_ORDER_COLUMN_NAME if align is False else SPARK_DEFAULT_INDEX_NAME
        )

        # This code assumes there is only one index (SPARK_DEFAULT_INDEX_NAME)
        # and would need to be updated if Sedona later supports multi-index.

        sdf = self._internal.spark_frame
        other_sdf = other._internal.spark_frame

        if index_col == NATURAL_ORDER_COLUMN_NAME:
            # NATURAL_ORDER_COLUMN_NAME is not deterministic or sequential, so we instead replace it with a
            # new column of row_numbers to perform the alignment. This mimics the following code.
            # sdf.withColumn(index_col, F.row_number().over(Window.orderBy(NATURAL_ORDER_COLUMN_NAME)))
            sdf = sdf.drop(index_col)
            other_sdf = other_sdf.drop(index_col)
            sdf = self._internal.attach_distributed_sequence_column(sdf, index_col)
            other_sdf = other._internal.attach_distributed_sequence_column(
                other_sdf, index_col
            )

        sdf = sdf.select(
            self.spark.column.alias("L"),
            # For the left side:
            # - We always select NATURAL_ORDER_COLUMN_NAME, to avoid having to regenerate it in the result
            # - We always select SPARK_DEFAULT_INDEX_NAME, to retain series index info
            col(NATURAL_ORDER_COLUMN_NAME),
            col(SPARK_DEFAULT_INDEX_NAME),
        )
        other_sdf = other_sdf.select(
            other.spark.column.alias("R"),
            # for the right side, we only need the column that we are joining on
            col(index_col),
        )

        joined_df = sdf.join(other_sdf, on=index_col, how="outer")

        if default_val is not None:
            # ps.Series.fillna() doesn't always work for the output for some reason
            # so we manually handle the nulls here.
            spark_col = F.when(
                F.col("L").isNull() | F.col("R").isNull(),
                default_val,
            ).otherwise(spark_col)

        return self._query_geometry_column(
            spark_col,
            joined_df,
            returns_geom=returns_geom,
            keep_name=keep_name,
        )

    def intersection_all(self):
        # Implementation of the abstract method.
        raise NotImplementedError("This method is not implemented yet.")

    # ============================================================================
    # Binary Predicates
    # ============================================================================
    def relate(self, other, align=None) -> pspd.Series:
        other, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_col = stp.ST_Relate(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_col,
            other,
            align,
            returns_geom=False,
            default_val=None,
        )
        return result

    # ============================================================================
    # SPATIAL PREDICATES
    # ============================================================================

    def contains(self, other, align=None) -> pspd.Series:
        other, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_col = stp.ST_Contains(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_col,
            other,
            align,
            returns_geom=False,
            default_val=False,
        )
        return _to_bool(result)

    def contains_properly(self, other, align=None):
        # Implementation of the abstract method.
        raise NotImplementedError(
            _not_implemented_error(
                "contains_properly",
                "Tests if geometries properly contain other geometries (no boundary contact).",
            )
        )

    def buffer(
        self,
        distance,
        resolution=16,
        cap_style="round",
        join_style="round",
        mitre_limit=5.0,
        single_sided=False,
        **kwargs,
    ) -> "GeoSeries":
        if single_sided:
            # Reverse the following logic in common/src/main/java/org/apache/sedona/common/Functions.java buffer() to avoid negating the distance.
            #   if (bufferParameters.isSingleSided()
            #       && (params.toLowerCase().contains("left") && radius < 0
            #           || params.toLowerCase().contains("right") && radius > 0)) {
            #     radius = -radius;
            #   }
            side = "left" if distance >= 0 else "right"
        else:
            side = "both"
        assert side in [
            "left",
            "right",
            "both",
        ], "single-sided must be one of 'left', 'right', or 'both', True, or False"

        parameters = F.lit(
            f"quad_segs={resolution} endcap={cap_style} join={join_style} mitre_limit={mitre_limit} side={side}"
        )
        spark_col = stf.ST_Buffer(
            self.spark.column, distance, useSpheroid=False, parameters=parameters
        )
        return self._query_geometry_column(
            spark_col,
            returns_geom=True,
        )

    def simplify(self, tolerance=None, preserve_topology=True) -> "GeoSeries":
        spark_expr = (
            stf.ST_SimplifyPreserveTopology(self.spark.column, tolerance)
            if preserve_topology
            else stf.ST_Simplify(self.spark.column, tolerance)
        )

        return self._query_geometry_column(spark_expr)

    def plot(self, *args, **kwargs):
        """
        Plot a GeoSeries.

        Generate a plot of a GeoSeries geometry with matplotlib.

        Note: This method is not scalable and requires collecting all data to the driver.

        Parameters
        ----------
        s : Series
            The GeoSeries to be plotted. Currently Polygon,
            MultiPolygon, LineString, MultiLineString, Point and MultiPoint
            geometries can be plotted.
        cmap : str (default None)
            The name of a colormap recognized by matplotlib. Any
            colormap will work, but categorical colormaps are
            generally recommended. Examples of useful discrete
            colormaps include:

                tab10, tab20, Accent, Dark2, Paired, Pastel1, Set1, Set2

        color : str, np.array, pd.Series, List (default None)
            If specified, all objects will be colored uniformly.
        ax : matplotlib.pyplot.Artist (default None)
            axes on which to draw the plot
        figsize : pair of floats (default None)
            Size of the resulting matplotlib.figure.Figure. If the argument
            ax is given explicitly, figsize is ignored.
        aspect : 'auto', 'equal', None or float (default 'auto')
            Set aspect of axis. If 'auto', the default aspect for map plots is 'equal'; if
            however data are not projected (coordinates are long/lat), the aspect is by
            default set to 1/cos(s_y * pi/180) with s_y the y coordinate of the middle of
            the GeoSeries (the mean of the y range of bounding box) so that a long/lat
            square appears square in the middle of the plot. This implies an
            Equirectangular projection. If None, the aspect of `ax` won't be changed. It can
            also be set manually (float) as the ratio of y-unit to x-unit.
        autolim : bool (default True)
            Update axes data limits to contain the new geometries.
        **style_kwds : dict
            Color options to be passed on to the actual plot function, such
            as ``edgecolor``, ``facecolor``, ``linewidth``, ``markersize``,
            ``alpha``.

        Returns
        -------
        ax : matplotlib axes instance
        """
        return self.to_geopandas().plot(*args, **kwargs)

    @property
    def geometry(self) -> "GeoSeries":
        return self

    # GeoSeries-only (not in GeoDataFrame)
    @property
    def x(self) -> pspd.Series:
        """Return the x location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s.x
        0    1.0
        1    2.0
        2    3.0
        dtype: float64

        See Also
        --------

        GeoSeries.y
        GeoSeries.z

        """
        spark_col = stf.ST_X(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    # GeoSeries-only (not in GeoDataFrame)
    @property
    def y(self) -> pspd.Series:
        """Return the y location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s.y
        0    1.0
        1    2.0
        2    3.0
        dtype: float64

        See Also
        --------

        GeoSeries.x
        GeoSeries.z
        GeoSeries.m

        """
        spark_col = stf.ST_Y(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    # GeoSeries-only (not in GeoDataFrame)
    @property
    def z(self) -> pspd.Series:
        """Return the z location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(1, 1, 1), Point(2, 2, 2), Point(3, 3, 3)])
        >>> s.z
        0    1.0
        1    2.0
        2    3.0
        dtype: float64

        See Also
        --------

        GeoSeries.x
        GeoSeries.y
        GeoSeries.m

        """
        spark_col = stf.ST_Z(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    # GeoSeries-only (not in GeoDataFrame)
    @property
    def m(self) -> pspd.Series:
        raise NotImplementedError("GeoSeries.m() is not implemented yet.")

    # ============================================================================
    # CONSTRUCTION METHODS
    # ============================================================================

    # GeoSeries-only (not in GeoDataFrame)
    @classmethod
    def from_file(
        cls, filename: str, format: Union[str, None] = None, **kwargs
    ) -> "GeoSeries":
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

        See Also
        --------
        GeoDataFrame.to_file : Write a ``GeoDataFrame`` to a file.
        """
        df = sgpd.io.read_file(filename, format, **kwargs)
        return GeoSeries(df.geometry, crs=df.crs)

    # GeoSeries-only (not in GeoDataFrame)
    @classmethod
    def from_wkb(
        cls,
        data,
        index=None,
        crs: Union[Any, None] = None,
        on_invalid="raise",
        **kwargs,
    ) -> "GeoSeries":
        r"""
        Alternate constructor to create a ``GeoSeries``
        from a list or array of WKB objects

        Parameters
        ----------
        data : array-like or Series
            Series, list or array of WKB objects
        index : array-like or Index
            The index for the GeoSeries.
        crs : value, optional
            Coordinate Reference System of the geometry objects. Can be anything
            accepted by
            :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        on_invalid: {"raise", "warn", "ignore"}, default "raise"
            - raise: an exception will be raised if a WKB input geometry is invalid.
            - warn: a warning will be raised and invalid WKB geometries will be returned
              as None.
            - ignore: invalid WKB geometries will be returned as None without a warning.
            - fix: an effort is made to fix invalid input geometries (e.g. close
              unclosed rings). If this is not possible, they are returned as ``None``
              without a warning. Requires GEOS >= 3.11 and shapely >= 2.1.

        kwargs
            Additional arguments passed to the Series constructor,
            e.g. ``name``.

        Returns
        -------
        GeoSeries

        See Also
        --------
        GeoSeries.from_wkt

        Examples
        --------

        >>> wkbs = [
        ... (
        ...     b"\x01\x01\x00\x00\x00\x00\x00\x00\x00"
        ...     b"\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
        ... ),
        ... (
        ...     b"\x01\x01\x00\x00\x00\x00\x00\x00\x00"
        ...     b"\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@"
        ... ),
        ... (
        ...    b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00"
        ...    b"\x00\x08@\x00\x00\x00\x00\x00\x00\x08@"
        ... ),
        ... ]
        >>> s = GeoSeries.from_wkb(wkbs)
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry
        """
        if on_invalid != "raise":
            raise NotImplementedError(
                "GeoSeries.from_wkb(): only on_invalid='raise' is implemented"
            )

        from pyspark.sql.types import StructType, StructField, BinaryType

        schema = StructType([StructField("data", BinaryType(), True)])
        return cls._create_from_select(
            stc.ST_GeomFromWKB(F.col("data")),
            data,
            schema,
            index,
            crs,
            **kwargs,
        )

    # GeoSeries-only (not in GeoDataFrame)
    @classmethod
    def from_wkt(
        cls,
        data,
        index=None,
        crs: Union[Any, None] = None,
        on_invalid="raise",
        **kwargs,
    ) -> "GeoSeries":
        """
        Alternate constructor to create a ``GeoSeries``
        from a list or array of WKT objects

        Parameters
        ----------
        data : array-like, Series
            Series, list, or array of WKT objects
        index : array-like or Index
            The index for the GeoSeries.
        crs : value, optional
            Coordinate Reference System of the geometry objects. Can be anything
            accepted by
            :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        on_invalid : {"raise", "warn", "ignore"}, default "raise"
            - raise: an exception will be raised if a WKT input geometry is invalid.
            - warn: a warning will be raised and invalid WKT geometries will be
              returned as ``None``.
            - ignore: invalid WKT geometries will be returned as ``None`` without a
              warning.
            - fix: an effort is made to fix invalid input geometries (e.g. close
              unclosed rings). If this is not possible, they are returned as ``None``
              without a warning. Requires GEOS >= 3.11 and shapely >= 2.1.

        kwargs
            Additional arguments passed to the Series constructor,
            e.g. ``name``.

        Returns
        -------
        GeoSeries

        See Also
        --------
        GeoSeries.from_wkb

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> wkts = [
        ... 'POINT (1 1)',
        ... 'POINT (2 2)',
        ... 'POINT (3 3)',
        ... ]
        >>> s = GeoSeries.from_wkt(wkts)
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry
        """
        if on_invalid != "raise":
            raise NotImplementedError(
                "GeoSeries.from_wkt(): only on_invalid='raise' is implemented"
            )

        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([StructField("data", StringType(), True)])
        return cls._create_from_select(
            stc.ST_GeomFromText(F.col("data")),
            data,
            schema,
            index,
            crs,
            **kwargs,
        )

    # GeoSeries-only (not in GeoDataFrame)
    @classmethod
    def from_xy(cls, x, y, z=None, index=None, crs=None, **kwargs) -> "GeoSeries":
        """
        Alternate constructor to create a :class:`~geopandas.GeoSeries` of Point
        geometries from lists or arrays of x, y(, z) coordinates

        In case of geographic coordinates, it is assumed that longitude is captured
        by ``x`` coordinates and latitude by ``y``.

        Parameters
        ----------
        x, y, z : iterable
        index : array-like or Index, optional
            The index for the GeoSeries. If not given and all coordinate inputs
            are Series with an equal index, that index is used.
        crs : value, optional
            Coordinate Reference System of the geometry objects. Can be anything
            accepted by
            :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        **kwargs
            Additional arguments passed to the Series constructor,
            e.g. ``name``.

        Returns
        -------
        GeoSeries

        See Also
        --------
        GeoSeries.from_wkt
        points_from_xy

        Examples
        --------

        >>> x = [2.5, 5, -3.0]
        >>> y = [0.5, 1, 1.5]
        >>> s = GeoSeries.from_xy(x, y, crs="EPSG:4326")
        >>> s
        0    POINT (2.5 0.5)
        1    POINT (5 1)
        2    POINT (-3 1.5)
        dtype: geometry
        """
        from pyspark.sql.types import StructType, StructField, DoubleType

        schema = StructType(
            [StructField("x", DoubleType(), True), StructField("y", DoubleType(), True)]
        )

        # Spark doesn't automatically cast ints to floats for us
        x = [float(num) for num in x]
        y = [float(num) for num in y]
        z = [float(num) for num in z] if z else None

        if z:
            data = list(zip(x, y, z))
            select = stc.ST_PointZ(F.col("x"), F.col("y"), F.col("z"))
            schema.add(StructField("z", DoubleType(), True))
        else:
            data = list(zip(x, y))
            select = stc.ST_Point(F.col("x"), F.col("y"))

        geoseries = cls._create_from_select(
            select,
            data,
            schema,
            index,
            crs,
            **kwargs,
        )

        if crs:
            from pyproj import CRS

            geoseries.crs = CRS.from_user_input(crs).to_epsg()

        return geoseries

    @classmethod
    def from_shapely(
        cls, data, index=None, crs: Union[Any, None] = None, **kwargs
    ) -> "GeoSeries":
        raise NotImplementedError(
            _not_implemented_error(
                "from_shapely", "Creates GeoSeries from Shapely geometry objects."
            )
        )

    @classmethod
    def from_arrow(cls, arr, **kwargs) -> "GeoSeries":
        """
        Construct a GeoSeries from a Arrow array object with a GeoArrow
        extension type.

        See https://geoarrow.org/ for details on the GeoArrow specification.

        This functions accepts any Arrow array object implementing
        the `Arrow PyCapsule Protocol`_ (i.e. having an ``__arrow_c_array__``
        method).

        .. _Arrow PyCapsule Protocol: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

        Note: Requires geopandas versions >= 1.0.0 to use with Sedona.

        Parameters
        ----------
        arr : pyarrow.Array, Arrow array
            Any array object implementing the Arrow PyCapsule Protocol
            (i.e. has an ``__arrow_c_array__`` or ``__arrow_c_stream__``
            method). The type of the array should be one of the
            geoarrow geometry types.
        **kwargs
            Other parameters passed to the GeoSeries constructor.

        Returns
        -------
        GeoSeries

        See Also
        --------
        GeoSeries.to_arrow
        GeoDataFrame.from_arrow

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> import geoarrow.pyarrow as ga
        >>> array = ga.as_geoarrow([None, "POLYGON ((0 0, 1 1, 0 1, 0 0))", "LINESTRING (0 0, -1 1, 0 -1)"])
        >>> geoseries = GeoSeries.from_arrow(array)
        >>> geoseries
        0                              None
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))
        2      LINESTRING (0 0, -1 1, 0 -1)
        dtype: geometry

        """
        gpd_series = gpd.GeoSeries.from_arrow(arr, **kwargs)
        return GeoSeries(gpd_series)

    @classmethod
    def _create_from_select(
        cls, spark_col: PySparkColumn, data, schema, index, crs, **kwargs
    ) -> "GeoSeries":

        from pyspark.pandas.utils import default_session
        from pyspark.pandas.internal import InternalField

        if isinstance(data, list) and not isinstance(data[0], (tuple, list)):
            data = [(obj,) for obj in data]

        name = kwargs.get("name", SPARK_DEFAULT_SERIES_NAME)

        if isinstance(data, pspd.Series):
            spark_df = data._internal.spark_frame
            assert len(schema) == 1
            spark_df = spark_df.withColumnRenamed(
                _get_series_col_name(data), schema[0].name
            )
        else:
            spark_df = default_session().createDataFrame(data, schema=schema)

        spark_df = spark_df.select(spark_col.alias(name))

        internal = InternalFrame(
            spark_frame=spark_df,
            index_spark_columns=None,
            column_labels=[(name,)],
            data_spark_columns=[scol_for(spark_df, name)],
            data_fields=[InternalField(np.dtype("object"), spark_df.schema[name])],
            column_label_names=[(name,)],
        )
        ps_series = first_series(PandasOnSparkDataFrame(internal))
        name = None if name == SPARK_DEFAULT_SERIES_NAME else name
        ps_series.rename(name, inplace=True)
        return GeoSeries(ps_series, index, crs=crs)

    # ============================================================================
    # DATA ACCESS AND MANIPULATION
    # ============================================================================

    # GeoSeries-only (not in GeoDataFrame)
    def isna(self) -> pspd.Series:
        """
        Detect missing values.

        Returns
        -------
        A boolean Series of the same size as the GeoSeries,
        True where a value is NA.

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon
        >>> s = GeoSeries(
        ...     [Polygon([(0, 0), (1, 1), (0, 1)]), None, Polygon([])]
        ... )
        >>> s
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1                              None
        2                     POLYGON EMPTY
        dtype: geometry

        >>> s.isna()
        0    False
        1     True
        2    False
        dtype: bool

        See Also
        --------
        GeoSeries.notna : inverse of isna
        GeoSeries.is_empty : detect empty geometries
        """
        spark_expr = F.isnull(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    # GeoSeries-only (not in GeoDataFrame)
    def isnull(self) -> pspd.Series:
        """Alias for `isna` method. See `isna` for more detail."""
        return self.isna()

    # GeoSeries-only (not in GeoDataFrame)
    def notna(self) -> pspd.Series:
        """
        Detect non-missing values.

        Returns
        -------
        A boolean pandas Series of the same size as the GeoSeries,
        False where a value is NA.

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon
        >>> s = GeoSeries(
        ...     [Polygon([(0, 0), (1, 1), (0, 1)]), None, Polygon([])]
        ... )
        >>> s
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1                              None
        2                     POLYGON EMPTY
        dtype: geometry

        >>> s.notna()
        0     True
        1    False
        2     True
        dtype: bool

        See Also
        --------
        GeoSeries.isna : inverse of notna
        GeoSeries.is_empty : detect empty geometries
        """
        # After Sedona's minimum Spark version is 3.5.0, we can use F.isnotnull(self.spark.column) instead.
        spark_expr = ~F.isnull(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    # GeoSeries-only (not in GeoDataFrame)
    def notnull(self) -> pspd.Series:
        """Alias for `notna` method. See `notna` for more detail."""
        return self.notna()

    # GeoSeries-only (not in GeoDataFrame)
    def fillna(
        self, value=None, inplace: bool = False, limit=None, **kwargs
    ) -> Union["GeoSeries", None]:
        """
        Fill NA values with geometry (or geometries).

        Parameters
        ----------
        value : shapely geometry or GeoSeries, default None
            If None is passed, NA values will be filled with GEOMETRYCOLLECTION EMPTY.
            If a shapely geometry object is passed, it will be
            used to fill all missing values. If a ``GeoSeries``
            is passed, missing values will be filled based on the corresponding index
            locations. If pd.NA or np.nan are passed, values will be filled with
            ``None`` (not GEOMETRYCOLLECTION EMPTY).
        limit : int, default None
            This is the maximum number of entries along the entire axis
            where NaNs will be filled. Must be greater than 0 if not None.

        Returns
        -------
        GeoSeries

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         None,
        ...         Polygon([(0, 0), (-1, 1), (0, -1)]),
        ...     ]
        ... )
        >>> s
        0      POLYGON ((0 0, 1 1, 0 1, 0 0))
        1                                None
        2    POLYGON ((0 0, -1 1, 0 -1, 0 0))
        dtype: geometry

        Filled with an empty polygon.

        >>> s.fillna()
        0      POLYGON ((0 0, 1 1, 0 1, 0 0))
        1            GEOMETRYCOLLECTION EMPTY
        2    POLYGON ((0 0, -1 1, 0 -1, 0 0))
        dtype: geometry

        Filled with a specific polygon.

        >>> s.fillna(Polygon([(0, 1), (2, 1), (1, 2)]))
        0      POLYGON ((0 0, 1 1, 0 1, 0 0))
        1      POLYGON ((0 1, 2 1, 1 2, 0 1))
        2    POLYGON ((0 0, -1 1, 0 -1, 0 0))
        dtype: geometry

        Filled with another GeoSeries.

        >>> from shapely.geometry import Point
        >>> s_fill = GeoSeries(
        ...     [
        ...         Point(0, 0),
        ...         Point(1, 1),
        ...         Point(2, 2),
        ...     ]
        ... )
        >>> s.fillna(s_fill)
        0      POLYGON ((0 0, 1 1, 0 1, 0 0))
        1                         POINT (1 1)
        2    POLYGON ((0 0, -1 1, 0 -1, 0 0))
        dtype: geometry

        See Also
        --------
        GeoSeries.isna : detect missing values
        """
        from shapely.geometry.base import BaseGeometry

        # TODO: Implement limit https://github.com/apache/sedona/issues/2068
        if limit:
            raise NotImplementedError(
                "GeoSeries.fillna() with limit is not implemented yet."
            )

        align = True

        if pd.isna(value) == True or isinstance(value, BaseGeometry):
            if (
                value is not None and pd.isna(value) == True
            ):  # ie. value is np.nan or pd.NA:
                value = None
            else:
                if value is None:
                    from shapely.geometry import GeometryCollection

                    value = GeometryCollection()

            other, extended = self._make_series_of_val(value)
            align = False if extended else align

        elif isinstance(value, (GeoSeries, gpd.GeoSeries)):

            if not isinstance(value, GeoSeries):
                value = GeoSeries(value)

            # Replace all None's with empty geometries (this is a recursive call)
            other = value.fillna(None)

        else:
            raise ValueError(f"Invalid value type: {type(value)}")

        # Coalesce: If the value in L is null, use the corresponding value in R for that row
        spark_expr = F.coalesce(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other,
            align=align,
            returns_geom=True,
            default_val=None,
            keep_name=True,
        )

        if inplace:
            self._update_inplace(result)
            return None

        return result

    def explode(self, ignore_index=False, index_parts=False) -> "GeoSeries":
        raise NotImplementedError(
            _not_implemented_error(
                "explode",
                "Explodes multi-part geometries into separate single-part geometries.",
            )
        )

    def to_crs(
        self, crs: Union[Any, None] = None, epsg: Union[int, None] = None
    ) -> "GeoSeries":
        """Returns a ``GeoSeries`` with all geometries transformed to a new
        coordinate reference system.

        Transform all geometries in a GeoSeries to a different coordinate
        reference system.  The ``crs`` attribute on the current GeoSeries must
        be set.  Either ``crs`` or ``epsg`` may be specified for output.

        This method will transform all points in all objects.  It has no notion
        of projecting entire geometries.  All segments joining points are
        assumed to be lines in the current projection, not geodesics.  Objects
        crossing the dateline (or other projection boundary) will have
        undesirable behavior.

        Parameters
        ----------
        crs : pyproj.CRS, optional if `epsg` is specified
            The value can be anything accepted
            by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        epsg : int, optional if `crs` is specified
            EPSG code specifying output projection.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoSeries
        >>> geoseries = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)], crs=4326)
        >>> geoseries.crs
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

        >>> geoseries = geoseries.to_crs(3857)
        >>> print(geoseries)
        0    POINT (111319.491 111325.143)
        1    POINT (222638.982 222684.209)
        2    POINT (333958.472 334111.171)
        dtype: geometry
        >>> geoseries.crs
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

        """

        from pyproj import CRS

        if crs is not None:
            crs = CRS.from_user_input(crs)
        elif epsg is not None:
            crs = CRS.from_epsg(epsg)
        else:
            raise ValueError("Must pass either crs or epsg.")

        spark_expr = stf.ST_Transform(
            self.spark.column,
            F.lit(f"EPSG:{crs.to_epsg()}"),
        )
        return self._query_geometry_column(
            spark_expr,
            keep_name=True,
        )

    @property
    def bounds(self) -> pspd.DataFrame:
        selects = [
            stf.ST_XMin(self.spark.column).alias("minx"),
            stf.ST_YMin(self.spark.column).alias("miny"),
            stf.ST_XMax(self.spark.column).alias("maxx"),
            stf.ST_YMax(self.spark.column).alias("maxy"),
        ]

        df = self._internal.spark_frame

        sdf = df.select(*selects)
        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=None,
            column_labels=[("minx",), ("miny",), ("maxx",), ("maxy",)],
            data_spark_columns=[
                scol_for(sdf, "minx"),
                scol_for(sdf, "miny"),
                scol_for(sdf, "maxx"),
                scol_for(sdf, "maxy"),
            ],
            column_label_names=None,
        )
        result = pspd.DataFrame(internal)
        # Convert max/min float values to NaN
        # e.g POINT EMPTY, represented as POINT (NaN NaN), should result in all NaN
        result = result.replace(np.finfo(np.float64).max, np.nan).replace(
            -np.finfo(np.float64).max, np.nan
        )
        return result

    @property
    def total_bounds(self):
        import warnings

        if len(self) == 0:
            # numpy 'min' cannot handle empty arrays
            # TODO with numpy >= 1.15, the 'initial' argument can be used
            return np.array([np.nan, np.nan, np.nan, np.nan])
        ps_df = self.bounds
        with warnings.catch_warnings():
            # if all rows are empty geometry / none, nan is expected
            warnings.filterwarnings(
                "ignore", r"All-NaN slice encountered", RuntimeWarning
            )

            minx = ps_df["minx"].min(skipna=True)
            miny = ps_df["miny"].min(skipna=True)

            # skipna=True doesn't work properly for max(), so we use dropna() as a workaround
            maxx = ps_df["maxx"].dropna()
            maxy = ps_df["maxy"].dropna()

            maxx = maxx.max(skipna=True) if not maxx.empty else np.nan
            maxy = maxy.max(skipna=True) if not maxy.empty else np.nan

            return np.array((minx, miny, maxx, maxy))

    # GeoSeries-only (not in GeoDataFrame)
    def estimate_utm_crs(self, datum_name: str = "WGS 84") -> "CRS":
        """Returns the estimated UTM CRS based on the bounds of the dataset.

        Parameters
        ----------
        datum_name : str, optional
            The name of the datum to use in the query. Default is WGS 84.

        Returns
        -------
        pyproj.CRS

        Examples
        --------
        >>> import geodatasets  # requires: pip install geodatasets
        >>> import geopandas as gpd
        >>> df = gpd.read_file(
        ...     geodatasets.get_path("geoda.chicago_commpop")
        ... )
        >>> df.geometry.values.estimate_utm_crs()  # doctest: +SKIP
        <Derived Projected CRS: EPSG:32616>
        Name: WGS 84 / UTM zone 16N
        Axis Info [cartesian]:
        - E[east]: Easting (metre)
        - N[north]: Northing (metre)
        Area of Use:
        - name: Between 90°W and 84°W, northern hemisphere between equator and 84°N,...
        - bounds: (-90.0, 0.0, -84.0, 84.0)
        Coordinate Operation:
        - name: UTM zone 16N
        - method: Transverse Mercator
        Datum: World Geodetic System 1984 ensemble
        - Ellipsoid: WGS 84
        - Prime Meridian: Greenwich
        """
        import numpy as np
        from pyproj import CRS
        from pyproj.aoi import AreaOfInterest
        from pyproj.database import query_utm_crs_info

        # This implementation replicates the implementation in GeoPandas's implementation exactly.
        # https://github.com/geopandas/geopandas/blob/main/geopandas/array.py
        # The only difference is that we use Sedona's total_bounds property which is more efficient and scalable
        # than the GeoPandas implementation. The rest of the implementation always executes on 4 points (minx, miny, maxx, maxy),
        # so the numpy and pyproj implementations are reasonable.

        if not self.crs:
            raise RuntimeError("crs must be set to estimate UTM CRS.")

        minx, miny, maxx, maxy = self.total_bounds
        if self.crs.is_geographic:
            x_center = np.mean([minx, maxx])
            y_center = np.mean([miny, maxy])
        # ensure using geographic coordinates
        else:
            from pyproj import Transformer
            from functools import lru_cache

            TransformerFromCRS = lru_cache(Transformer.from_crs)

            transformer = TransformerFromCRS(self.crs, "EPSG:4326", always_xy=True)
            minx, miny, maxx, maxy = transformer.transform_bounds(
                minx, miny, maxx, maxy
            )
            y_center = np.mean([miny, maxy])
            # crossed the antimeridian
            if minx > maxx:
                # shift maxx from [-180,180] to [0,360]
                # so both numbers are positive for center calculation
                # Example: -175 to 185
                maxx += 360
                x_center = np.mean([minx, maxx])
                # shift back to [-180,180]
                x_center = ((x_center + 180) % 360) - 180
            else:
                x_center = np.mean([minx, maxx])

        utm_crs_list = query_utm_crs_info(
            datum_name=datum_name,
            area_of_interest=AreaOfInterest(
                west_lon_degree=x_center,
                south_lat_degree=y_center,
                east_lon_degree=x_center,
                north_lat_degree=y_center,
            ),
        )
        try:
            return CRS.from_epsg(utm_crs_list[0].code)
        except IndexError:
            raise RuntimeError("Unable to determine UTM CRS")

    def to_json(
        self,
        show_bbox: bool = True,
        drop_id: bool = False,
        to_wgs84: bool = False,
        **kwargs,
    ) -> str:
        """
        Returns a GeoJSON string representation of the GeoSeries.

        Parameters
        ----------
        show_bbox : bool, optional, default: True
            Include bbox (bounds) in the geojson
        drop_id : bool, default: False
            Whether to retain the index of the GeoSeries as the id property
            in the generated GeoJSON. Default is False, but may want True
            if the index is just arbitrary row numbers.
        to_wgs84: bool, optional, default: False
            If the CRS is set on the active geometry column it is exported as
            WGS84 (EPSG:4326) to meet the `2016 GeoJSON specification
            <https://tools.ietf.org/html/rfc7946>`_.
            Set to True to force re-projection and set to False to ignore CRS. False by
            default.

        *kwargs* that will be passed to json.dumps().

        Note: Unlike geopandas, Sedona's implementation will replace 'LinearRing'
        with 'LineString' in the GeoJSON output.

        Returns
        -------
        JSON string

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry

        >>> s.to_json()
        '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "pr\
operties": {}, "geometry": {"type": "Point", "coordinates": [1.0, 1.0]}, "bbox": [1.0,\
 1.0, 1.0, 1.0]}, {"id": "1", "type": "Feature", "properties": {}, "geometry": {"type"\
: "Point", "coordinates": [2.0, 2.0]}, "bbox": [2.0, 2.0, 2.0, 2.0]}, {"id": "2", "typ\
e": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [3.0, 3.\
0]}, "bbox": [3.0, 3.0, 3.0, 3.0]}], "bbox": [1.0, 1.0, 3.0, 3.0]}'

        See Also
        --------
        GeoSeries.to_file : write GeoSeries to file
        """
        return self.to_geoframe(name="geometry").to_json(
            na="null", show_bbox=show_bbox, drop_id=drop_id, to_wgs84=to_wgs84, **kwargs
        )

    def to_wkb(self, hex: bool = False, **kwargs) -> pspd.Series:
        """
        Convert GeoSeries geometries to WKB

        Parameters
        ----------
        hex : bool
            If true, export the WKB as a hexadecimal string.
            The default is to return a binary bytes object.
        kwargs
            Additional keyword args will be passed to
            :func:`shapely.to_wkb`.

        Returns
        -------
        Series
            WKB representations of the geometries

        See also
        --------
        GeoSeries.to_wkt

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon
        >>> s = GeoSeries(
        ...     [
        ...         Point(0, 0),
        ...         Polygon(),
        ...         Polygon([(0, 0), (1, 1), (1, 0)]),
        ...         None,
        ...     ]
        ... )

        >>> s.to_wkb()
        0    b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00...
        1              b'\x01\x03\x00\x00\x00\x00\x00\x00\x00'
        2    b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00...
        3                                                 None
        dtype: object

        >>> s.to_wkb(hex=True)
        0           010100000000000000000000000000000000000000
        1                                   010300000000000000
        2    0103000000010000000400000000000000000000000000...
        3                                                 None
        dtype: object

        """
        spark_expr = stf.ST_AsBinary(self.spark.column)

        if hex:
            spark_expr = F.hex(spark_expr)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def to_wkt(self, **kwargs) -> pspd.Series:
        """
        Convert GeoSeries geometries to WKT

        Note: Using shapely < 1.0.0 may return different geometries for empty geometries.

        Parameters
        ----------
        kwargs
            Keyword args will be passed to :func:`shapely.to_wkt`.

        Returns
        -------
        Series
            WKT representations of the geometries

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry

        >>> s.to_wkt()
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: object

        See also
        --------
        GeoSeries.to_wkb
        """
        spark_expr = stf.ST_AsText(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def to_arrow(self, geometry_encoding="WKB", interleaved=True, include_z=None):
        """Encode a GeoSeries to GeoArrow format.

        See https://geoarrow.org/ for details on the GeoArrow specification.

        This functions returns a generic Arrow array object implementing
        the `Arrow PyCapsule Protocol`_ (i.e. having an ``__arrow_c_array__``
        method). This object can then be consumed by your Arrow implementation
        of choice that supports this protocol.

        .. _Arrow PyCapsule Protocol: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

        Note: Requires geopandas versions >= 1.0.0 to use with Sedona.

        Parameters
        ----------
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
        GeoArrowArray
            A generic Arrow array object with geometry data encoded to GeoArrow.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> gser = GeoSeries([Point(1, 2), Point(2, 1)])
        >>> gser
        0    POINT (1 2)
        1    POINT (2 1)
        dtype: geometry

        >>> arrow_array = gser.to_arrow()
        >>> arrow_array
        <geopandas.io._geoarrow.GeoArrowArray object at ...>

        The returned array object needs to be consumed by a library implementing
        the Arrow PyCapsule Protocol. For example, wrapping the data as a
        pyarrow.Array (requires pyarrow >= 14.0):

        >>> import pyarrow as pa
        >>> array = pa.array(arrow_array)
        >>> array
        <pyarrow.lib.BinaryArray object at ...>
        [
          0101000000000000000000F03F0000000000000040,
          01010000000000000000000040000000000000F03F
        ]

        """
        # Because this function returns the Arrow array in memory, we simply rely on GeoPandas's implementation.
        # This also returns a GeoPandas-specific data type, which can be converted to an actual PyArrow array,
        # so there is no direct Sedona equivalent. This way we also get all of the arguments implemented for free.
        return self.to_geopandas().to_arrow(
            geometry_encoding=geometry_encoding,
            interleaved=interleaved,
            include_z=include_z,
        )

    def clip(self, mask, keep_geom_type: bool = False, sort=False) -> "GeoSeries":
        raise NotImplementedError(
            _not_implemented_error(
                "clip", "Clips geometries to the bounds of a mask geometry."
            )
        )

    def to_file(
        self,
        path: str,
        driver: Union[str, None] = None,
        schema: Union[dict, None] = None,
        index: Union[bool, None] = None,
        **kwargs,
    ):
        """Write the ``GeoSeries`` to a file.

        Parameters
        ----------
        path : str
            File path or file handle to write to.
        driver : str, optional
            The format driver used to write the file, by default None. If not
            specified, it's inferred from the file extension. Available formats
            are "geojson", "geopackage", and "geoparquet".
        index : bool, optional
            If True, writes the index as a column. If False, no index is
            written. By default None, the index is written only if it is named,
            is a MultiIndex, or has a non-integer data type.
        mode : str, default 'w'
            The write mode: 'w' to overwrite the existing file or 'a' to append.
        crs : pyproj.CRS, optional
            The coordinate reference system to write. If None, it is determined
            from the ``GeoSeries`` `crs` attribute. The value can be anything
            accepted by :meth:`pyproj.CRS.from_user_input()`, such as an
            authority string (e.g., "EPSG:4326") or a WKT string.
        **kwargs
            Additional keyword arguments passed to the underlying writing engine.

        Examples
        --------
        >>> from shapely.geometry import Point, LineString
        >>> from sedona.spark.geopandas import GeoSeries
        >>> # Note: Examples write to temporary files for demonstration
        >>> import tempfile
        >>> import os

        Create a GeoSeries:
        >>> gs = GeoSeries(
        ...     [Point(0, 0), LineString([(1, 1), (2, 2)])],
        ...     index=["a", "b"]
        ... )

        Save to a GeoParquet file:
        >>> path_parquet = os.path.join(tempfile.gettempdir(), "data.parquet")
        >>> gs.to_file(path_parquet, driver="geoparquet")

        Append to a GeoJSON file:
        >>> path_json = os.path.join(tempfile.gettempdir(), "data.json")
        >>> gs.to_file(path_json, driver="geojson", mode='a')
        """
        self.to_geoframe().to_file(path, driver, index=index, **kwargs)

    def to_parquet(self, path, **kwargs):
        """Write the GeoSeries to a GeoParquet file.

        Parameters
        ----------
        path : str
            The file path where the GeoParquet file will be written.
        **kwargs
            Additional keyword arguments passed to the underlying writing function.

        Returns
        -------
        None

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoSeries
        >>> import tempfile
        >>> import os
        >>> gs = GeoSeries([Point(1, 1), Point(2, 2)])
        >>> file_path = os.path.join(tempfile.gettempdir(), "my_geodata.parquet")
        >>> gs.to_parquet(file_path)
        """
        self.to_geoframe().to_file(path, driver="geoparquet", **kwargs)

    # -----------------------------------------------------------------------------
    # # Utils
    # -----------------------------------------------------------------------------

    def _update_inplace(self, result: "GeoSeries", invalidate_sindex: bool = True):
        self.rename(result.name, inplace=True)
        self._update_anchor(result._anchor)
        if invalidate_sindex:
            self._sindex = None

    def _make_series_of_val(self, value: Any):
        """
        A helper method to turn single objects into series (ps.Series or GeoSeries when possible)
        Returns:
            tuple[pspd.Series, bool]:
                - The series of the value
                - Whether returned value was a single object extended into a series (useful for row-wise 'align' parameter)
        """
        # generator instead of a in-memory list
        if isinstance(value, GeoDataFrame):
            return value.geometry, False
        elif not isinstance(value, pspd.Series):
            lst = [value for _ in range(len(self))]
            if isinstance(value, BaseGeometry):
                return GeoSeries(lst), True
            else:
                # e.g int input
                return pspd.Series(lst), True
        else:
            return value, False

    def to_geoframe(self, name=None):
        if name is not None:
            renamed = self.rename(name)
        elif self._column_label is None or self._column_label == (None,):
            renamed = self.rename("geometry")
        else:
            renamed = self

        index_col = SPARK_DEFAULT_INDEX_NAME
        result = GeoDataFrame(
            pspd.DataFrame(renamed._internal).to_spark(index_col).pandas_api(index_col)
        )
        result.index.name = self.index.name
        return result


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def _get_series_col_name(ps_series: pspd.Series) -> str:
    return ps_series.name if ps_series.name else SPARK_DEFAULT_SERIES_NAME


def _to_bool(ps_series: pspd.Series, default: bool = False) -> pspd.Series:
    """
    Cast a ps.Series to bool type if it's not one, converting None values to the default value.
    """
    if ps_series.dtype.name != "bool":
        # fill None values with the default value
        ps_series.fillna(default, inplace=True)

    return ps_series
