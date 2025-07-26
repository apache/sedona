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

import os
import typing
from typing import Any, Union, Literal, List

import geopandas as gpd
import sedona.geopandas as sgpd
import pandas as pd
import pyspark.pandas as pspd
import pyspark
from pyspark.pandas import Series as PandasOnSparkSeries
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import InternalFrame
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import scol_for, log_advice
from pyspark.sql.types import BinaryType, NullType
from sedona.spark.sql.types import GeometryType

from sedona.spark.sql import st_aggregates as sta
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp

from pyspark.sql import Column as PySparkColumn
from pyspark.sql import functions as F

import shapely
from shapely.geometry.base import BaseGeometry

from sedona.geopandas._typing import Label

# from sedona.geopandas.base import GeoFrame
# from sedona.geopandas.geodataframe import GeoDataFrame  # avoid circular import
from sedona.geopandas.sindex import SpatialIndex
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
        f"GeometryArray.{method_name}() is not implemented yet.\n"
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


class GeometryArray(pspd.Series):
    """
    A pandas-on-Spark Series for geometric/spatial operations.

    GeometryArray extends pyspark.pandas.Series to provide spatial operations
    using Apache Sedona's spatial functions. It maintains compatibility
    with GeoPandas GeometryArray while operating on distributed datasets.

    Parameters
    ----------
    data : array-like, Iterable, dict, or scalar value
        Contains the data for the GeometryArray. Can be geometries, WKB bytes,
        or other GeometryArray/GeoDataFrame objects.
    index : array-like or Index (1d), optional
        Values must be hashable and have the same length as `data`.
    crs : pyproj.CRS, optional
        Coordinate Reference System for the geometries.
    dtype : dtype, optional
        Data type for the GeometryArray.
    name : str, optional
        Name of the GeometryArray.
    copy : bool, default False
        Whether to copy the input data.

    Attributes
    ----------
    crs : pyproj.CRS
        The Coordinate Reference System (CRS) for the geometries.
    area : Series
        Area of each geometry in CRS units.
    length : Series
        Length/perimeter of each geometry in CRS units.
    bounds : DataFrame
        Bounding box coordinates for each geometry.
    geometry : GeometryArray
        The geometry column (returns self).
    sindex : SpatialIndex
        Spatial index for the geometries.

    Methods
    -------
    buffer(distance)
        Buffer geometries by specified distance.
    intersection(other)
        Compute intersection with other geometries.
    intersects(other)
        Test if geometries intersect with other geometries.
    to_geopandas()
        Convert to GeoPandas GeometryArray.
    to_crs(crs)
        Transform geometries to a different CRS.
    set_crs(crs)
        Set the CRS without transforming geometries.

    Examples
    --------
    >>> from shapely.geometry import Point, Polygon
    >>> from sedona.geopandas import GeometryArray
    >>>
    >>> # Create from geometries
    >>> s = GeometryArray([Point(0, 0), Point(1, 1)], crs='EPSG:4326')
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
    geopandas.GeometryArray : The GeoPandas equivalent
    sedona.geopandas.GeoDataFrame : DataFrame with geometry column
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
        Initialize a GeometryArray object.

        Parameters:
        - data: The input data for the GeometryArray. It can be a GeoDataFrame, GeometryArray, or pandas Series.
        - index: The index for the GeometryArray.
        - crs: Coordinate Reference System for the GeometryArray.
        - dtype: Data type for the GeometryArray.
        - name: Name of the GeometryArray.
        - copy: Whether to copy the input data.
        - fastpath: Internal parameter for fast initialization.

        Examples:
        >>> from shapely.geometry import Point
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeometryArray

        # Example 1: Initialize with GeoDataFrame
        >>> gdf = gpd.GeoDataFrame({'geometry': [Point(1, 1), Point(2, 2)]})
        >>> gs = GeometryArray(data=gdf)
        >>> print(gs)
        0    POINT (1 1)
        1    POINT (2 2)
        Name: geometry, dtype: geometry

        # Example 2: Initialize with GeometryArray
        >>> gseries = gpd.GeometryArray([Point(1, 1), Point(2, 2)])
        >>> gs = GeometryArray(data=gseries)
        >>> print(gs)
        0    POINT (1 1)
        1    POINT (2 2)
        dtype: geometry

        # Example 3: Initialize with pandas Series
        >>> pseries = pd.Series([Point(1, 1), Point(2, 2)])
        >>> gs = GeometryArray(data=pseries)
        >>> print(gs)
        0    POINT (1 1)
        1    POINT (2 2)
        dtype: geometry
        """
        from sedona.geopandas.geodataframe import GeoDataFrame

        assert data is not None

        self._anchor: GeoDataFrame
        self._col_label: Label

        if isinstance(
            data,
            (GeoDataFrame, GeometryArray, PandasOnSparkSeries, PandasOnSparkDataFrame),
        ):
            assert dtype is None
            assert name is None
            assert not copy
            assert not fastpath

            data_crs = None
            if hasattr(data, "crs"):
                data_crs = data.crs
            if data_crs is not None and crs is not None and data_crs != crs:
                raise ValueError(
                    "CRS mismatch between CRS of the passed geometries "
                    "and 'crs'. Use 'GeometryArray.set_crs(crs, "
                    "allow_override=True)' to overwrite CRS or "
                    "'GeometryArray.to_crs(crs)' to reproject geometries. "
                )

            # PySpark Pandas' ps.Series.__init__() does not construction from a
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

            # initialize the parent class pyspark Series with the pandas Series
            super().__init__(data=pd_series)

        # Ensure we're storing geometry types
        if (
            self.spark.data_type != GeometryType()
            and self.spark.data_type != NullType()
        ):
            raise TypeError(
                "Non geometry data passed to GeometryArray constructor, "
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

        Note: This assumes all records in the GeometryArray are assumed to have the same CRS.

        Examples
        --------
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
        GeometryArray.set_crs : assign CRS
        GeometryArray.to_crs : re-project to another CRS
        """
        from pyproj import CRS

        if len(self) == 0:
            return None

        spark_col = stf.ST_SRID(self.spark.column)
        tmp_series = self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

        # All geometries should have the same srid
        # so we just take the srid of the first non-null element
        first_idx = tmp_series.first_valid_index()
        srid = tmp_series[first_idx] if first_idx is not None else 0

        # Sedona returns 0 if doesn't exist
        return CRS.from_user_input(srid) if srid != 0 else None

    @crs.setter
    def crs(self, value: Union["CRS", None]):
        # Implementation of the abstract method
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
    ) -> "GeometryArray": ...

    def set_crs(
        self,
        crs: Union[Any, None] = None,
        epsg: Union[int, None] = None,
        inplace: bool = False,
        allow_override: bool = False,
    ) -> Union["GeometryArray", None]:
        """
        Set the Coordinate Reference System (CRS) of a ``GeometryArray``.

        Pass ``None`` to remove CRS from the ``GeometryArray``.

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
            If True, the CRS of the GeometryArray will be changed in place
            (while still returning the result) instead of making a copy of
            the GeometryArray.
        allow_override : bool, default False
            If the GeometryArray already has a CRS, allow to replace the
            existing CRS, even when both are not equal.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> s = GeometryArray([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry

        Setting CRS to a GeometryArray without one:

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
        GeometryArray.to_crs : re-project to another CRS

        """
        from pyproj import CRS

        if crs is not None:
            crs = CRS.from_user_input(crs)
        elif epsg is not None:
            crs = CRS.from_epsg(epsg)

        curr_crs = self.crs

        # If CRS is the same, do nothing
        if curr_crs == crs:
            return

        if not allow_override and curr_crs is not None and not curr_crs == crs:
            raise ValueError(
                "The GeometryArray already has a CRS which is not equal to the passed "
                "CRS. Specify 'allow_override=True' to allow replacing the existing "
                "CRS without doing any transformation. If you actually want to "
                "transform the geometries, use 'GeometryArray.to_crs' instead."
            )

        # 0 indicates no srid in sedona
        new_epsg = crs.to_epsg() if crs else 0

        spark_col = stf.ST_SetSRID(self.spark.column, new_epsg)
        result = self._query_geometry_column(spark_col, keep_name=True)

        if inplace:
            self._update_inplace(result)
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
    ) -> Union["GeometryArray", pspd.Series]:
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
        GeometryArray
            A GeometryArray with the operation applied to the geometry column.
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
            # We always select NATURAL_ORDER_COLUMN_NAME, to avoid having to regenerate it in the result
            # We always select SPARK_DEFAULT_INDEX_NAME, to retain series index info

            exprs.append(scol_for(df, SPARK_DEFAULT_INDEX_NAME))
            exprs.append(scol_for(df, NATURAL_ORDER_COLUMN_NAME))

            index_spark_columns = [scol_for(df, SPARK_DEFAULT_INDEX_NAME)]
            index_fields = [self._internal.index_fields[0]]
        # else if is_aggr, we don't select the index columns

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

        # Convert spark series default name to pandas series default name (None) if needed
        series_name = None if rename == SPARK_DEFAULT_SERIES_NAME else rename
        ps_series = ps_series.rename(series_name)

        result = GeometryArray(ps_series) if returns_geom else ps_series
        return result

    # ============================================================================
    # CONVERSION AND SERIALIZATION METHODS
    # ============================================================================

    def to_geopandas(self) -> gpd.GeoSeries:
        """
        Convert the GeometryArray to a geopandas GeometryArray.

        Returns:
        - geopandas.GeometryArray: A geopandas GeometryArray.
        """
        from pyspark.pandas.utils import log_advice

        log_advice(
            "`to_geopandas` loads all data into the driver's memory. "
            "It should only be used if the resulting geopandas GeometryArray is expected to be small."
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
    def geometry(self) -> "GeometryArray":
        return self

    @property
    def sindex(self) -> SpatialIndex:
        """
        Returns a spatial index built from the geometries.

        Returns
        -------
        SpatialIndex
            The spatial index for this GeoDataFrame.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> gdf = GeoDataFrame([{"geometry": Point(1, 1), "value": 1},
        ...                     {"geometry": Point(2, 2), "value": 2}])
        >>> index = gdf.sindex
        >>> index.size
        2
        """
        geometry_column = _get_series_col_name(self)
        if geometry_column is None:
            raise ValueError("No geometry column found in GeometryArray")
        return SpatialIndex(self._internal.spark_frame, column_name=geometry_column)

    def copy(self, deep=False):
        """
        Make a copy of this GeometryArray object.

        Parameters:
        - deep: bool, default False
            If True, a deep copy of the data is made. Otherwise, a shallow copy is made.

        Returns:
        - GeometryArray: A copy of this GeometryArray object.

        Examples:
        >>> from shapely.geometry import Point
        >>> from sedona.geopandas import GeometryArray

        >>> gs = GeometryArray([Point(1, 1), Point(2, 2)])
        >>> gs_copy = gs.copy()
        >>> print(gs_copy)
        0    POINT (1 1)
        1    POINT (2 2)
        dtype: geometry
        """
        if deep:
            return GeometryArray(
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
        spark_col = stf.GeometryType(self.spark.column)
        result = self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

        # Sedona returns the string in all caps unlike Geopandas
        sgpd_to_gpg_name_map = {
            "POINT": "Point",
            "LINESTRING": "LineString",
            "POLYGON": "Polygon",
            "MULTIPOINT": "MultiPoint",
            "MULTILINESTRING": "MultiLineString",
            "MULTIPOLYGON": "MultiPolygon",
            "GEOMETRYCOLLECTION": "GeometryCollection",
        }
        result = result.map(lambda x: sgpd_to_gpg_name_map.get(x, x))
        return result

    @property
    def type(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error("type", "Returns numeric geometry type codes.")
        )

    @property
    def length(self) -> pspd.Series:
        """
        Returns a Series containing the length of each geometry in the GeometryArray.

        In the case of a (Multi)Polygon it measures the length of its exterior (i.e. perimeter).

        For a GeometryCollection it measures sums the values for each of the individual geometries.

        Returns
        -------
        Series
            A Series containing the length of each geometry.

        Examples
        --------
        >>> from shapely.geometry import Polygon
        >>> from sedona.geopandas import GeometryArray

        >>> gs = GeometryArray([Point(0, 0), LineString([(0, 0), (1, 1)]), Polygon([(0, 0), (1, 0), (1, 1)]), GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)]), Polygon([(0, 0), (1, 0), (1, 1)])])])
        >>> gs.length
        0    0.000000
        1    1.414214
        2    3.414214
        3    4.828427
        dtype: float64
        """

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
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that are valid.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         Polygon([(0,0), (1, 1), (1, 0), (0, 1)]),  # bowtie geometry
        ...         Polygon([(0, 0), (2, 2), (2, 0)]),
        ...         None
        ...     ]
        ... )
        >>> s
        0         POLYGON ((0 0, 1 1, 0 1, 0 0))
        1    POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))
        2         POLYGON ((0 0, 2 2, 2 0, 0 0))
        3                                   None
        dtype: geometry

        >>> s.is_valid
        0     True
        1    False
        2     True
        3    False
        dtype: bool

        See also
        --------
        GeometryArray.is_valid_reason : reason for invalidity
        """

        spark_col = stf.ST_IsValid(self.spark.column)
        result = self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )
        return to_bool(result)

    def is_valid_reason(self) -> pspd.Series:
        """Returns a ``Series`` of strings with the reason for invalidity of
        each geometry.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         Polygon([(0,0), (1, 1), (1, 0), (0, 1)]),  # bowtie geometry
        ...         Polygon([(0, 0), (2, 2), (2, 0)]),
        ...         Polygon([(0, 0), (2, 0), (1, 1), (2, 2), (0, 2), (1, 1), (0, 0)]),
        ...         None
        ...     ]
        ... )
        >>> s
        0         POLYGON ((0 0, 1 1, 0 1, 0 0))
        1    POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))
        2         POLYGON ((0 0, 2 2, 2 0, 0 0))
        3                                   None
        dtype: geometry

        >>> s.is_valid_reason()
        0    Valid Geometry
        1    Self-intersection at or near point (0.5, 0.5, NaN)
        2    Valid Geometry
        3    Ring Self-intersection at or near point (1.0, 1.0)
        4    None
        dtype: object

        See also
        --------
        GeometryArray.is_valid : detect invalid geometries
        GeometryArray.make_valid : fix invalid geometries
        """
        spark_col = stf.ST_IsValidReason(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def is_empty(self) -> pspd.Series:
        """
        Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        empty geometries.

        Examples
        --------
        An example of a GeoDataFrame with one empty point, one point and one missing
        value:

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> geoseries = GeometryArray([Point(), Point(2, 1), None], crs="EPSG:4326")
        >>> geoseries
        0  POINT EMPTY
        1  POINT (2 1)
        2         None

        >>> geoseries.is_empty
        0     True
        1    False
        2    False
        dtype: bool

        See Also
        --------
        GeometryArray.isna : detect missing values
        """
        spark_expr = stf.ST_IsEmpty(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return to_bool(result)

    def count_coordinates(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "count_coordinates",
                "Counts the number of coordinate tuples in each geometry.",
            )
        )

    def count_geometries(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "count_geometries",
                "Counts the number of geometries in each multi-geometry or collection.",
            )
        )

    def count_interior_rings(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "count_interior_rings",
                "Counts the number of interior rings (holes) in each polygon.",
            )
        )

    def dwithin(self, other, distance, align=None):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that is within a set distance from ``other``.

        The operation works on a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : GeometryArray or geometric object
            The GeometryArray (elementwise) or geometric object to test for
            equality.
        distance : float, np.array, pd.Series
            Distance(s) to test if each geometry is within. A scalar distance will be
            applied to all geometries. An array or Series will be applied elementwise.
            If np.array or pd.Series are used then it must have same length as the
            GeometryArray.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices.
            If False, the order of elements is preserved. None defaults to True.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         LineString([(0, 0), (0, 1)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(0, 4),
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(1, 0), (4, 2), (2, 2)]),
        ...         Polygon([(2, 0), (3, 2), (2, 2)]),
        ...         LineString([(2, 0), (2, 2)]),
        ...         Point(1, 1),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1             LINESTRING (0 0, 0 2)
        2             LINESTRING (0 0, 0 1)
        3                       POINT (0 1)
        dtype: geometry

        >>> s2
        1    POLYGON ((1 0, 4 2, 2 2, 1 0))
        2    POLYGON ((2 0, 3 2, 2 2, 2 0))
        3             LINESTRING (2 0, 2 2)
        4                       POINT (1 1)
        dtype: geometry

        We can check if each geometry of GeometryArray contains a single
        geometry:

        >>> point = Point(0, 1)
        >>> s2.dwithin(point, 1.8)
        1     True
        2    False
        3    False
        4     True
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.dwithin(s2, distance=1, align=True)
        0    False
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        >>> s.dwithin(s2, distance=1, align=False)
        0     True
        1    False
        2    False
        3     True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray is within the set distance of *any* element of the other one.

        See also
        --------
        GeometryArray.within
        """

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

    def difference(self, other, align=None) -> "GeometryArray":
        """Returns a ``GeometryArray`` of the points in each aligned geometry that
        are not in `other`.

        The operation works on a 1-to-1 row-wise manner:

        Unlike Geopandas, Sedona does not support this operation for GeometryCollections.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            difference to.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(1, 0), (1, 3)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(1, 1),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 6),
        ... )

        >>> s
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1    POLYGON ((0 0, 2 2, 0 2, 0 0))
        2             LINESTRING (0 0, 2 2)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (0 1)
        dtype: geometry

        >>> s2
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))
        2             LINESTRING (1 0, 1 3)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (1 1)
        5                       POINT (0 1)
        dtype: geometry

        We can do difference of each geometry and a single
        shapely geometry:

        >>> s.difference(Polygon([(0, 0), (1, 1), (0, 1)]))
        0       POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        1         POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        2                       LINESTRING (1 1, 2 2)
        3    MULTILINESTRING ((2 0, 1 1), (1 1, 0 2))
        4                                 POINT EMPTY
        dtype: geometry

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.difference(s2, align=True)
        0                                        None
        1         POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        2    MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))
        3                            LINESTRING EMPTY
        4                                 POINT (0 1)
        5                                        None
        dtype: geometry

        >>> s.difference(s2, align=False)
        0         POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        1    POLYGON ((0 0, 0 2, 1 2, 2 2, 1 1, 0 0))
        2    MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))
        3                       LINESTRING (2 0, 0 2)
        4                                 POINT EMPTY
        dtype: geometry

        See Also
        --------
        GeometryArray.symmetric_difference
        GeometryArray.union
        GeometryArray.intersection
        """

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stf.ST_Difference(F.col("L"), F.col("R"))
        return self._row_wise_operation(
            spark_expr,
            other_series,
            align=align,
            returns_geom=True,
        )

    @property
    def is_simple(self) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that do not cross themselves.

        This is meaningful only for `LineStrings` and `LinearRings`.

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import LineString
        >>> s = GeometryArray(
        ...     [
        ...         LineString([(0, 0), (1, 1), (1, -1), (0, 1)]),
        ...         LineString([(0, 0), (1, 1), (1, -1)]),
        ...     ]
        ... )
        >>> s
        0    LINESTRING (0 0, 1 1, 1 -1, 0 1)
        1         LINESTRING (0 0, 1 1, 1 -1)
        dtype: geometry

        >>> s.is_simple
        0    False
        1     True
        dtype: bool
        """
        spark_expr = stf.ST_IsSimple(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return to_bool(result)

    @property
    def is_ring(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "is_ring", "Tests if LineString geometries are closed rings."
            )
        )

    @property
    def is_ccw(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "is_ccw",
                "Tests if LinearRing geometries are oriented counter-clockwise.",
            )
        )

    @property
    def is_closed(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "is_closed",
                "Tests if LineString geometries are closed (start equals end point).",
            )
        )

    @property
    def has_z(self) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        features that have a z-component.

        Notes
        -----
        Every operation in GeoPandas is planar, i.e. the potential third
        dimension is not taken into account.

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> s = GeometryArray(
        ...     [
        ...         Point(0, 1),
        ...         Point(0, 1, 2),
        ...     ]
        ... )
        >>> s
        0        POINT (0 1)
        1    POINT Z (0 1 2)
        dtype: geometry

        >>> s.has_z
        0    False
        1     True
        dtype: bool
        """
        spark_expr = stf.ST_HasZ(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def get_precision(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def get_geometry(self, index) -> "GeometryArray":
        """Returns the n-th geometry from a collection of geometries (0-indexed).

        If the index is non-negative, it returns the geometry at that index.
        If the index is negative, it counts backward from the end of the collection (e.g., -1 returns the last geometry).
        Returns None if the index is out of bounds.

        Note: Simple geometries act as length-1 collections

        Note: Using Shapely < 2.0, may lead to different results for empty simple geometries due to how
        shapely interprets them.

        Parameters
        ----------
        index : int or array_like
            Position of a geometry to be retrieved within its collection

        Returns
        -------
        GeometryArray

        Notes
        -----
        Simple geometries act as collections of length 1. Any out-of-range index value
        returns None.

        Examples
        --------
        >>> from shapely.geometry import Point, MultiPoint, GeometryCollection
        >>> s = geopandas.GeometryArray(
        ...     [
        ...         Point(0, 0),
        ...         MultiPoint([(0, 0), (1, 1), (0, 1), (1, 0)]),
        ...         GeometryCollection(
        ...             [MultiPoint([(0, 0), (1, 1), (0, 1), (1, 0)]), Point(0, 1)]
        ...         ),
        ...         Polygon(),
        ...         GeometryCollection(),
        ...     ]
        ... )
        >>> s
        0                                          POINT (0 0)
        1              MULTIPOINT ((0 0), (1 1), (0 1), (1 0))
        2    GEOMETRYCOLLECTION (MULTIPOINT ((0 0), (1 1), ...
        3                                        POLYGON EMPTY
        4                             GEOMETRYCOLLECTION EMPTY
        dtype: geometry

        >>> s.get_geometry(0)
        0                                POINT (0 0)
        1                                POINT (0 0)
        2    MULTIPOINT ((0 0), (1 1), (0 1), (1 0))
        3                              POLYGON EMPTY
        4                                       None
        dtype: geometry

        >>> s.get_geometry(1)
        0           None
        1    POINT (1 1)
        2    POINT (0 1)
        3           None
        4           None
        dtype: geometry

        >>> s.get_geometry(-1)
        0    POINT (0 0)
        1    POINT (1 0)
        2    POINT (0 1)
        3  POLYGON EMPTY
        4           None
        dtype: geometry

        """

        # Sedona errors on negative indexes, so we use a case statement to handle it ourselves
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
    def boundary(self) -> "GeometryArray":
        """Returns a ``GeometryArray`` of lower dimensional objects representing
        each geometry's set-theoretic `boundary`.

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(0, 0), (1, 1), (1, 0)]),
        ...         Point(0, 0),
        ...     ]
        ... )
        >>> s
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1        LINESTRING (0 0, 1 1, 1 0)
        2                       POINT (0 0)
        dtype: geometry

        >>> s.boundary
        0    LINESTRING (0 0, 1 1, 0 1, 0 0)
        1          MULTIPOINT ((0 0), (1 0))
        2           GEOMETRYCOLLECTION EMPTY
        dtype: geometry

        See also
        --------
        GeometryArray.exterior : outer boundary (without interior rings)

        """
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
    def centroid(self) -> "GeometryArray":
        spark_expr = stf.ST_Centroid(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=True,
        )

    def concave_hull(self, ratio=0.0, allow_holes=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def convex_hull(self):
        # Implementation of the abstract method
        raise NotImplementedError(
            _not_implemented_error(
                "convex_hull", "Computes the convex hull of each geometry."
            )
        )

    def delaunay_triangles(self, tolerance=0.0, only_edges=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def voronoi_polygons(self, tolerance=0.0, extend_to=None, only_edges=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def envelope(self) -> "GeometryArray":
        """Returns a ``GeometryArray`` of geometries representing the envelope of
        each geometry.

        The envelope of a geometry is the bounding rectangle. That is, the
        point or smallest rectangular polygon (with sides parallel to the
        coordinate axes) that contains the geometry.

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point, MultiPoint
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(0, 0), (1, 1), (1, 0)]),
        ...         MultiPoint([(0, 0), (1, 1)]),
        ...         Point(0, 0),
        ...     ]
        ... )
        >>> s
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1        LINESTRING (0 0, 1 1, 1 0)
        2         MULTIPOINT ((0 0), (1 1))
        3                       POINT (0 0)
        dtype: geometry

        >>> s.envelope
        0    POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))
        1    POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))
        2    POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))
        3                            POINT (0 0)
        dtype: geometry

        See also
        --------
        GeometryArray.convex_hull : convex hull geometry
        """
        spark_expr = stf.ST_Envelope(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=True,
        )

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

    def make_valid(self, *, method="linework", keep_collapsed=True) -> "GeometryArray":
        """Repairs invalid geometries.

        Returns a ``GeometryArray`` with valid geometries.

        If the input geometry is already valid, then it will be preserved.
        In many cases, in order to create a valid geometry, the input
        geometry must be split into multiple parts or multiple geometries.
        If the geometry must be split into multiple parts of the same type
        to be made valid, then a multi-part geometry will be returned
        (e.g. a MultiPolygon).
        If the geometry must be split into multiple parts of different types
        to be made valid, then a GeometryCollection will be returned.

        In Sedona, only the 'structure' method is available:

        * the 'structure' algorithm tries to reason from the structure of the
          input to find the 'correct' repair: exterior rings bound area,
          interior holes exclude area. It first makes all rings valid, then
          shells are merged and holes are subtracted from the shells to
          generate valid result. It assumes that holes and shells are correctly
          categorized in the input geometry.

        Parameters
        ----------
        method : {'linework', 'structure'}, default 'linework'
            Algorithm to use when repairing geometry. Sedona Geopandas only supports the 'structure' method.
            The default method is "linework" to match compatibility with Geopandas, but it must be explicitly set to
            'structure' to use the Sedona implementation.

        keep_collapsed : bool, default True
            For the 'structure' method, True will keep components that have
            collapsed into a lower dimensionality. For example, a ring
            collapsing to a line, or a line collapsing to a point.

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import MultiPolygon, Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (0, 2), (1, 1), (2, 2), (2, 0), (1, 1), (0, 0)]),
        ...         Polygon([(0, 2), (0, 1), (2, 0), (0, 0), (0, 2)]),
        ...         LineString([(0, 0), (1, 1), (1, 0)]),
        ...     ],
        ... )
        >>> s
        0    POLYGON ((0 0, 0 2, 1 1, 2 2, 2 0, 1 1, 0 0))
        1              POLYGON ((0 2, 0 1, 2 0, 0 0, 0 2))
        2                       LINESTRING (0 0, 1 1, 1 0)
        dtype: geometry

        >>> s.make_valid()
        0    MULTIPOLYGON (((1 1, 0 0, 0 2, 1 1)), ((2 0, 1...
        1                       POLYGON ((0 1, 2 0, 0 0, 0 1))
        2                           LINESTRING (0 0, 1 1, 1 0)
        dtype: geometry
        """

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

    # ============================================================================
    # GEOMETRIC OPERATIONS
    # ============================================================================

    @property
    def unary_union(self):
        # Implementation of the abstract method
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
            # While it's not explicitly defined in geopandas docs, this is what geopandas returns for empty GeometryArray
            # If it ever changes for some reason, we'll catch that with the test
            from shapely.geometry import GeometryCollection

            return GeometryCollection()

        spark_expr = sta.ST_Union_Aggr(self.spark.column)
        tmp = self._query_geometry_column(spark_expr, returns_geom=False, is_aggr=True)

        ps_series = tmp.take([0])
        geom = ps_series.iloc[0]
        return geom

    def crosses(self, other, align=None) -> pspd.Series:
        # Sedona does not support GeometryCollection (errors), so we return NULL for now to avoid error
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

        return to_bool(result)

    def disjoint(self, other, align=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def intersects(
        self,
        other: Union["GeometryArray", BaseGeometry],
        align: Union[bool, None] = None,
    ) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that intersects `other`.

        An object is said to intersect `other` if its `boundary` and `interior`
        intersects in any way with those of the other.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeometryArray or geometric object
            The GeometryArray (elementwise) or geometric object to test if is
            intersected.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         LineString([(1, 0), (1, 3)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(1, 1),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1             LINESTRING (0 0, 2 2)
        2             LINESTRING (2 0, 0 2)
        3                       POINT (0 1)
        dtype: geometry

        >>> s2
        1    LINESTRING (1 0, 1 3)
        2    LINESTRING (2 0, 0 2)
        3              POINT (1 1)
        4              POINT (0 1)
        dtype: geometry

        We can check if each geometry of GeometryArray crosses a single
        geometry:

        >>> line = LineString([(-1, 1), (3, 1)])
        >>> s.intersects(line)
        0    True
        1    True
        2    True
        3    True
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.intersects(s2, align=True)
        0    False
        1     True
        2     True
        3    False
        4    False
        dtype: bool

        >>> s.intersects(s2, align=False)
        0    True
        1    True
        2    True
        3    True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray ``crosses`` *any* element of the other one.

        See also
        --------
        GeometryArray.disjoint
        GeometryArray.crosses
        GeometryArray.touches
        GeometryArray.intersection
        """

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Intersects(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return to_bool(result)

    def overlaps(self, other, align=None) -> pspd.Series:
        """Returns True for all aligned geometries that overlap other, else False.

        In the original Geopandas, Geometries overlap if they have more than one but not all
        points in common, have the same dimension, and the intersection of the
        interiors of the geometries has the same dimension as the geometries
        themselves.

        However, in Sedona, we return True in the case where the geometries points match.

        Note: Sedona's behavior may also differ from Geopandas for GeometryCollections.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeometryArray or geometric object
            The GeometryArray (elementwise) or geometric object to test if
            overlaps.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, MultiPoint, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         MultiPoint([(0, 0), (0, 1)]),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 0), (0, 2)]),
        ...         LineString([(0, 1), (1, 1)]),
        ...         LineString([(1, 1), (3, 3)]),
        ...         Point(0, 1),
        ...     ],
        ... )

        We can check if each geometry of GeometryArray overlaps a single
        geometry:

        >>> polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        >>> s.overlaps(polygon)
        0     True
        1     True
        2    False
        3    False
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We align both GeometryArray
        based on index values and compare elements with the same index.

        >>> s.overlaps(s2)
        0    False
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        >>> s.overlaps(s2, align=False)
        0     True
        1    False
        2     True
        3    False
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray ``overlaps`` *any* element of the other one.

        See also
        --------
        GeometryArray.crosses
        GeometryArray.intersects

        """
        # Note: We cannot efficiently match geopandas behavior because Sedona's ST_Overlaps returns True for equal geometries
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
        return to_bool(result)

    def touches(self, other, align=None) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that touches `other`.

        An object is said to touch `other` if it has at least one point in
        common with `other` and its interior does not intersect with any part
        of the other. Overlapping features therefore do not touch.

        Note: Sedona's behavior may also differ from Geopandas for GeometryCollections.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeometryArray or geometric object
            The GeometryArray (elementwise) or geometric object to test if is
            touched.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, MultiPoint, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         MultiPoint([(0, 0), (0, 1)]),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (-2, 0), (0, -2)]),
        ...         LineString([(0, 1), (1, 1)]),
        ...         LineString([(1, 1), (3, 0)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1    POLYGON ((0 0, 2 2, 0 2, 0 0))
        2             LINESTRING (0 0, 2 2)
        3         MULTIPOINT ((0 0), (0 1))
        dtype: geometry

        >>> s2
        1    POLYGON ((0 0, -2 0, 0 -2, 0 0))
        2               LINESTRING (0 1, 1 1)
        3               LINESTRING (1 1, 3 0)
        4                         POINT (0 1)
        dtype: geometry

        We can check if each geometry of GeometryArray touches a single
        geometry:

        >>> line = LineString([(0, 0), (-1, -2)])
        >>> s.touches(line)
        0    True
        1    True
        2    True
        3    True
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.touches(s2, align=True)
        0    False
        1     True
        2     True
        3    False
        4    False
        dtype: bool

        >>> s.touches(s2, align=False)
        0     True
        1    False
        2     True
        3    False
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray ``touches`` *any* element of the other one.

        See also
        --------
        GeometryArray.overlaps
        GeometryArray.intersects

        """

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Touches(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return to_bool(result)

    def within(self, other, align=None) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that is within `other`.

        An object is said to be within `other` if at least one of its points is located
        in the `interior` and no points are located in the `exterior` of the other.
        If either object is empty, this operation returns ``False``.

        This is the inverse of `contains` in the sense that the
        expression ``a.within(b) == b.contains(a)`` always evaluates to
        ``True``.

        Note: Sedona's behavior may also differ from Geopandas for GeometryCollections and for geometries that are equal.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeometryArray or geometric object
            The GeometryArray (elementwise) or geometric object to test if each
            geometry is within.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)


        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (1, 2), (0, 2)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         LineString([(0, 0), (0, 1)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1    POLYGON ((0 0, 1 2, 0 2, 0 0))
        2             LINESTRING (0 0, 0 2)
        3                       POINT (0 1)
        dtype: geometry

        >>> s2
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))
        2             LINESTRING (0 0, 0 2)
        3             LINESTRING (0 0, 0 1)
        4                       POINT (0 1)
        dtype: geometry

        We can check if each geometry of GeometryArray is within a single
        geometry:

        >>> polygon = Polygon([(0, 0), (2, 2), (0, 2)])
        >>> s.within(polygon)
        0     True
        1     True
        2    False
        3    False
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s2.within(s)
        0    False
        1    False
        2     True
        3    False
        4    False
        dtype: bool

        >>> s2.within(s, align=False)
        1     True
        2    False
        3     True
        4     True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray is ``within`` any element of the other one.

        See also
        --------
        GeometryArray.contains
        """

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Within(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return to_bool(result)

    def covers(self, other, align=None) -> pspd.Series:
        """
        Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that is entirely covering `other`.

        An object A is said to cover another object B if no points of B lie
        in the exterior of A.
        If either object is empty, this operation returns ``False``.

        Note: Sedona's implementation instead returns False for identical geometries.
        Sedona's behavior may also differ from Geopandas for GeometryCollections.

        The operation works on a 1-to-1 row-wise manner.

        See
        https://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
        for reference.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to check is being covered.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         Point(0, 0),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
        ...         Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ...         LineString([(1, 1), (1.5, 1.5)]),
        ...         Point(0, 0),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))
        1         POLYGON ((0 0, 2 2, 0 2, 0 0))
        2                  LINESTRING (0 0, 2 2)
        3                            POINT (0 0)
        dtype: geometry

        >>> s2
        1    POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, ...
        2                  POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))
        3                            LINESTRING (1 1, 1.5 1.5)
        4                                          POINT (0 0)
        dtype: geometry

        We can check if each geometry of GeometryArray covers a single
        geometry:

        >>> poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        >>> s.covers(poly)
        0     True
        1    False
        2    False
        3    False
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.covers(s2, align=True)
        0    False
        1    False
        2    False
        3    False
        4    False
        dtype: bool

        >>> s.covers(s2, align=False)
        0     True
        1    False
        2     True
        3     True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray ``covers`` any element of the other one.

        See also
        --------
        GeometryArray.covered_by
        GeometryArray.overlaps
        """

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_Covers(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return to_bool(result)

    def covered_by(self, other, align=None) -> pspd.Series:
        """
        Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that is entirely covered by `other`.

        An object A is said to cover another object B if no points of B lie
        in the exterior of A.

        Note: Sedona's implementation instead returns False for identical geometries.
        Sedona's behavior may differ from Geopandas for GeometryCollections.

        The operation works on a 1-to-1 row-wise manner.

        See
        https://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
        for reference.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to check is being covered.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
        ...         Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ...         LineString([(1, 1), (1.5, 1.5)]),
        ...         Point(0, 0),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         Point(0, 0),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, ...
        1                  POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))
        2                            LINESTRING (1 1, 1.5 1.5)
        3                                          POINT (0 0)
        dtype: geometry
        >>>

        >>> s2
        1    POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))
        2         POLYGON ((0 0, 2 2, 0 2, 0 0))
        3                  LINESTRING (0 0, 2 2)
        4                            POINT (0 0)
        dtype: geometry

        We can check if each geometry of GeometryArray is covered by a single
        geometry:

        >>> poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        >>> s.covered_by(poly)
        0    True
        1    True
        2    True
        3    True
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.covered_by(s2, align=True)
        0    False
        1     True
        2     True
        3     True
        4    False
        dtype: bool

        >>> s.covered_by(s2, align=False)
        0     True
        1    False
        2     True
        3     True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray is ``covered_by`` any element of the other one.

        See also
        --------
        GeometryArray.covers
        GeometryArray.overlaps
        """

        other_series, extended = self._make_series_of_val(other)
        align = False if extended else align

        spark_expr = stp.ST_CoveredBy(F.col("L"), F.col("R"))
        result = self._row_wise_operation(
            spark_expr,
            other_series,
            align,
            default_val=False,
        )
        return to_bool(result)

    def distance(self, other, align=None) -> pspd.Series:
        """Returns a ``Series`` containing the distance to aligned `other`.

        The operation works on a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            distance to.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (float)

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 0), (1, 1)]),
        ...         Polygon([(0, 0), (-1, 0), (-1, 1)]),
        ...         LineString([(1, 1), (0, 0)]),
        ...         Point(0, 0),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
        ...         Point(3, 1),
        ...         LineString([(1, 0), (2, 0)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0      POLYGON ((0 0, 1 0, 1 1, 0 0))
        1    POLYGON ((0 0, -1 0, -1 1, 0 0))
        2               LINESTRING (1 1, 0 0)
        3                         POINT (0 0)
        dtype: geometry

        >>> s2
        1    POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, ...
        2                                          POINT (3 1)
        3                                LINESTRING (1 0, 2 0)
        4                                          POINT (0 1)
        dtype: geometry

        We can check the distance of each geometry of GeometryArray to a single
        geometry:

        >>> point = Point(-1, 0)
        >>> s.distance(point)
        0    1.0
        1    0.0
        2    1.0
        3    1.0
        dtype: float64

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and use elements with the same index using
        ``align=True`` or ignore index and use elements based on their matching
        order using ``align=False``:

        >>> s.distance(s2, align=True)
        0         NaN
        1    0.707107
        2    2.000000
        3    1.000000
        4         NaN
        dtype: float64

        >>> s.distance(s2, align=False)
        0    0.000000
        1    3.162278
        2    0.707107
        3    1.000000
        dtype: float64
        """

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
        self,
        other: Union["GeometryArray", BaseGeometry],
        align: Union[bool, None] = None,
    ) -> "GeometryArray":
        """Returns a ``GeometryArray`` of the intersection of points in each
        aligned geometry with `other`.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            intersection with.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(1, 0), (1, 3)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(1, 1),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 6),
        ... )

        >>> s
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1    POLYGON ((0 0, 2 2, 0 2, 0 0))
        2             LINESTRING (0 0, 2 2)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (0 1)
        dtype: geometry

        >>> s2
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))
        2             LINESTRING (1 0, 1 3)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (1 1)
        5                       POINT (0 1)
        dtype: geometry

        We can also do intersection of each geometry and a single
        shapely geometry:

        >>> s.intersection(Polygon([(0, 0), (1, 1), (0, 1)]))
        0    POLYGON ((0 0, 0 1, 1 1, 0 0))
        1    POLYGON ((0 0, 0 1, 1 1, 0 0))
        2             LINESTRING (0 0, 1 1)
        3                       POINT (1 1)
        4                       POINT (0 1)
        dtype: geometry

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.intersection(s2, align=True)
        0                              None
        1    POLYGON ((0 0, 0 1, 1 1, 0 0))
        2                       POINT (1 1)
        3             LINESTRING (2 0, 0 2)
        4                       POINT EMPTY
        5                              None
        dtype: geometry

        >>> s.intersection(s2, align=False)
        0    POLYGON ((0 0, 0 1, 1 1, 0 0))
        1             LINESTRING (1 1, 1 2)
        2                       POINT (1 1)
        3                       POINT (1 1)
        4                       POINT (0 1)
        dtype: geometry


        See Also
        --------
        GeometryArray.difference
        GeometryArray.symmetric_difference
        GeometryArray.union
        """

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

    def snap(self, other, tolerance, align=None) -> "GeometryArray":
        """Snap the vertices and segments of the geometry to vertices of the reference.

        Vertices and segments of the input geometry are snapped to vertices of the
        reference geometry, returning a new geometry; the input geometries are not
        modified. The result geometry is the input geometry with the vertices and
        segments snapped. If no snapping occurs then the input geometry is returned
        unchanged. The tolerance is used to control where snapping is performed.

        Where possible, this operation tries to avoid creating invalid geometries;
        however, it does not guarantee that output geometries will be valid. It is
        the responsibility of the caller to check for and handle invalid geometries.

        Because too much snapping can result in invalid geometries being created,
        heuristics are used to determine the number and location of snapped
        vertices that are likely safe to snap. These heuristics may omit
        some potential snaps that are otherwise within the tolerance.

        Note: Sedona's result may differ slightly from geopandas's snap() result
        because of small differences between the underlying engines being used.

        The operation works in a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : GeometryArray or geometric object
            The Geoseries (elementwise) or geometric object to snap to.
        tolerance : float or array like
            Maximum distance between vertices that shall be snapped
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        GeometryArray

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Point(0.5, 2.5),
        ...         LineString([(0.1, 0.1), (0.49, 0.51), (1.01, 0.89)]),
        ...         Polygon([(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]),
        ...     ],
        ... )
        >>> s
        0                               POINT (0.5 2.5)
        1    LINESTRING (0.1 0.1, 0.49 0.51, 1.01 0.89)
        2       POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))
        dtype: geometry

        >>> s2 = GeometryArray(
        ...     [
        ...         Point(0, 2),
        ...         LineString([(0, 0), (0.5, 0.5), (1.0, 1.0)]),
        ...         Point(8, 10),
        ...     ],
        ...     index=range(1, 4),
        ... )
        >>> s2
        1                       POINT (0 2)
        2    LINESTRING (0 0, 0.5 0.5, 1 1)
        3                      POINT (8 10)
        dtype: geometry

        We can snap each geometry to a single shapely geometry:

        >>> s.snap(Point(0, 2), tolerance=1)
        0                                     POINT (0 2)
        1      LINESTRING (0.1 0.1, 0.49 0.51, 1.01 0.89)
        2    POLYGON ((0 0, 0 2, 0 10, 10 10, 10 0, 0 0))
        dtype: geometry

        We can also snap two GeometryArray to each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and snap elements with the same index using
        ``align=True`` or ignore index and snap elements based on their matching
        order using ``align=False``:

        >>> s.snap(s2, tolerance=1, align=True)
        0                                                 None
        1           LINESTRING (0.1 0.1, 0.49 0.51, 1.01 0.89)
        2    POLYGON ((0.5 0.5, 1 1, 0 10, 10 10, 10 0, 0.5...
        3                                                 None
        dtype: geometry

        >>> s.snap(s2, tolerance=1, align=False)
        0                                      POINT (0 2)
        1                   LINESTRING (0 0, 0.5 0.5, 1 1)
        2    POLYGON ((0 0, 0 10, 8 10, 10 10, 10 0, 0 0))
        dtype: geometry
        """
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
        Helper function to perform a row-wise operation on two GeometryArray.
        The self column and other column are aliased to `L` and `R`, respectively.

        align : bool or None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.
            Note: align should also be set to False when 'other' a geoseries created from a single object
            (e.g. GeometryArray([Point(0, 0) * len(self)])), so that we align based on natural ordering in case
            the index is not the default range index from 0.
            Alternatively, we could create 'other' using the same index as self,
            but that would require index=self.index.to_pandas() which is less scalable.

        default_val : str or None (default "FALSE")
            The value to use if either L or R is null. If None, nulls are not handled.
        """
        from pyspark.sql.functions import col

        # Note: this is specifically False. None is valid since it defaults to True similar to geopandas
        index_col = (
            NATURAL_ORDER_COLUMN_NAME if align is False else SPARK_DEFAULT_INDEX_NAME
        )

        # This code assumes there is only one index (SPARK_DEFAULT_INDEX_NAME)
        # and would need to be updated if Sedona later supports multi-index

        df = self._internal.spark_frame.select(
            self.spark.column.alias("L"),
            # For the left side:
            # - We always select NATURAL_ORDER_COLUMN_NAME, to avoid having to regenerate it in the result
            # - We always select SPARK_DEFAULT_INDEX_NAME, to retain series index info
            col(NATURAL_ORDER_COLUMN_NAME),
            col(SPARK_DEFAULT_INDEX_NAME),
        )
        other_df = other._internal.spark_frame.select(
            other.spark.column.alias("R"),
            # for the right side, we only need the column that we are joining on
            col(index_col),
        )

        joined_df = df.join(other_df, on=index_col, how="outer")

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
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    # ============================================================================
    # SPATIAL PREDICATES
    # ============================================================================

    def contains(self, other, align=None) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that contains `other`.

        An object is said to contain `other` if at least one point of `other` lies in
        the interior and no points of `other` lie in the exterior of the object.
        (Therefore, any given polygon does not contain its own boundary - there is not
        any point that lies in the interior.)
        If either object is empty, this operation returns ``False``.

        This is the inverse of `within` in the sense that the expression
        ``a.contains(b) == b.within(a)`` always evaluates to ``True``.

        Note: Sedona's implementation instead returns False for identical geometries.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeometryArray or geometric object
            The GeometryArray (elementwise) or geometric object to test if it
            is contained.
        align : bool | None (default None)
            If True, automatically aligns GeometryArray based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         LineString([(0, 0), (0, 1)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(0, 4),
        ... )
        >>> s2 = GeometryArray(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (1, 2), (0, 2)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(1, 5),
        ... )

        >>> s
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1             LINESTRING (0 0, 0 2)
        2             LINESTRING (0 0, 0 1)
        3                       POINT (0 1)
        dtype: geometry

        >>> s2
        1    POLYGON ((0 0, 2 2, 0 2, 0 0))
        2    POLYGON ((0 0, 1 2, 0 2, 0 0))
        3             LINESTRING (0 0, 0 2)
        4                       POINT (0 1)
        dtype: geometry

        We can check if each geometry of GeometryArray contains a single
        geometry:

        >>> point = Point(0, 1)
        >>> s.contains(point)
        0    False
        1     True
        2    False
        3     True
        dtype: bool

        We can also check two GeometryArray against each other, row by row.
        The GeometryArray above have different indices. We can either align both GeometryArray
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s2.contains(s, align=True)
        0    False
        1    False
        2    False
        3     True
        4    False
        dtype: bool

        >>> s2.contains(s, align=False)
        1     True
        2    False
        3     True
        4     True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeometryArray ``contains`` any element of the other one.

        See also
        --------
        GeometryArray.contains_properly
        GeometryArray.within
        """

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
        return to_bool(result)

    def contains_properly(self, other, align=None):
        # Implementation of the abstract method
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
    ) -> "GeometryArray":
        """
        Returns a GeometryArray of geometries representing all points within a given distance of each geometric object.

        Parameters
        ----------
        distance : float
            The distance to buffer around each geometry.
        resolution : int, optional, default 16
            The resolution of the buffer around each geometry.
        cap_style : str, optional, default "round"
            The style of the buffer's cap (round, flat, or square).
        join_style : str, optional, default "round"
            The style of the buffer's join (round, mitre, or bevel).
        mitre_limit : float, optional, default 5.0
            The mitre limit for the buffer's join style.
        single_sided : bool, optional, default False
            Whether to create a single-sided buffer.

        Returns
        -------
        GeometryArray
            A GeometryArray of buffered geometries.
        """
        spark_col = stf.ST_Buffer(self.spark.column, distance)
        return self._query_geometry_column(
            spark_col,
            returns_geom=True,
        )

    def simplify(self, tolerance=None, preserve_topology=True) -> "GeometryArray":
        """Returns a ``GeometryArray`` containing a simplified representation of
        each geometry.

        The algorithm (Douglas-Peucker) recursively splits the original line
        into smaller parts and connects these parts' endpoints
        by a straight line. Then, it removes all points whose distance
        to the straight line is smaller than `tolerance`. It does not
        move any points and it always preserves endpoints of
        the original line or polygon.
        See https://shapely.readthedocs.io/en/latest/manual.html#object.simplify
        for details

        Simplifies individual geometries independently, without considering
        the topology of a potential polygonal coverage. If you would like to treat
        the ``GeometryArray`` as a coverage and simplify its edges, while preserving the
        coverage topology, see :meth:`simplify_coverage`.

        Parameters
        ----------
        tolerance : float
            All parts of a simplified geometry will be no more than
            `tolerance` distance from the original. It has the same units
            as the coordinate reference system of the GeometryArray.
            For example, using `tolerance=100` in a projected CRS with meters
            as units means a distance of 100 meters in reality.
        preserve_topology: bool (default True)
            False uses a quicker algorithm, but may produce self-intersecting
            or otherwise invalid geometries.

        Notes
        -----
        Invalid geometric objects may result from simplification that does not
        preserve topology and simplification may be sensitive to the order of
        coordinates: two geometries differing only in order of coordinates may be
        simplified differently.

        See also
        --------
        simplify_coverage : simplify geometries using coverage simplification

        Examples
        --------
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point, LineString
        >>> s = GeometryArray(
        ...     [Point(0, 0).buffer(1), LineString([(0, 0), (1, 10), (0, 20)])]
        ... )
        >>> s
        0    POLYGON ((1 0, 0.99518 -0.09802, 0.98079 -0.19...
        1                         LINESTRING (0 0, 1 10, 0 20)
        dtype: geometry

        >>> s.simplify(1)
        0    POLYGON ((0 1, 0 -1, -1 0, 0 1))
        1              LINESTRING (0 0, 0 20)
        dtype: geometry
        """

        spark_expr = (
            stf.ST_SimplifyPreserveTopology(self.spark.column, tolerance)
            if preserve_topology
            else stf.ST_Simplify(self.spark.column, tolerance)
        )

        return self._query_geometry_column(spark_expr)

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
        Perform a spatial join between two GeometryArray.
        Parameters:
        - other: GeometryArray
        - how: str, default 'inner'
            The type of join to perform.
        - predicate: str, default 'intersects'
            The spatial predicate to use for the join.
        - lsuffix: str, default 'left'
            Suffix to apply to the left GeometryArray' column names.
        - rsuffix: str, default 'right'
            Suffix to apply to the right GeometryArray' column names.
        - distance: float, optional
            The distance threshold for the join.
        - on_attribute: str, optional
            The attribute to join on.
        - kwargs: Any
            Additional arguments to pass to the join function.
        Returns:
        - GeometryArray
        """
        from sedona.geopandas import sjoin

        # Implementation of the abstract method
        return sjoin(
            self,
            other,
            how,
            predicate,
            lsuffix,
            rsuffix,
            distance,
            on_attribute,
            **kwargs,
        )

    @property
    def geometry(self) -> "GeometryArray":
        return self

    @property
    def x(self) -> pspd.Series:
        """Return the x location of point geometries in a GeometryArray

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> s = GeometryArray([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s.x
        0    1.0
        1    2.0
        2    3.0
        dtype: float64

        See Also
        --------

        GeometryArray.y
        GeometryArray.z

        """
        spark_col = stf.ST_X(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def y(self) -> pspd.Series:
        """Return the y location of point geometries in a GeometryArray

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> s = GeometryArray([Point(1, 1), Point(2, 2), Point(3, 3)])
        >>> s.y
        0    1.0
        1    2.0
        2    3.0
        dtype: float64

        See Also
        --------

        GeometryArray.x
        GeometryArray.z
        GeometryArray.m

        """
        spark_col = stf.ST_Y(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def z(self) -> pspd.Series:
        """Return the z location of point geometries in a GeometryArray

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> s = GeometryArray([Point(1, 1, 1), Point(2, 2, 2), Point(3, 3, 3)])
        >>> s.z
        0    1.0
        1    2.0
        2    3.0
        dtype: float64

        See Also
        --------

        GeometryArray.x
        GeometryArray.y
        GeometryArray.m

        """
        spark_col = stf.ST_Z(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def m(self) -> pspd.Series:
        raise NotImplementedError("GeometryArray.m() is not implemented yet.")

    # ============================================================================
    # CONSTRUCTION METHODS
    # ============================================================================

    @classmethod
    def from_file(
        cls, filename: str, format: Union[str, None] = None, **kwargs
    ) -> "GeometryArray":
        """
        Alternate constructor to create a ``GeoDataFrame`` from a file.

        Parameters
        ----------
        filename : str
            File path or file handle to read from. If the path is a directory,
            Sedona will read all files in the directory into a dataframe.
        format : str, default None
            The format of the file to read. If None, Sedona will infer the format
            from the file extension. Note, inferring the format from the file extension
            is not supported for directories.
            Options:
                - "shapefile"
                - "geojson"
                - "geopackage"
                - "geoparquet"

        table_name : str, default None
            The name of the table to read from a geopackage file. Required if format is geopackage.

        See also
        --------
        GeoDataFrame.to_file : write GeoDataFrame to file
        """
        df = sgpd.io.read_file(filename, format, **kwargs)
        return GeometryArray(df.geometry, crs=df.crs)

    @classmethod
    def from_wkb(
        cls,
        data,
        index=None,
        crs: Union[Any, None] = None,
        on_invalid="raise",
        **kwargs,
    ) -> "GeometryArray":
        r"""
        Alternate constructor to create a ``GeometryArray``
        from a list or array of WKB objects

        Parameters
        ----------
        data : array-like or Series
            Series, list or array of WKB objects
        index : array-like or Index
            The index for the GeometryArray.
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
        GeometryArray

        See Also
        --------
        GeometryArray.from_wkt

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
        >>> s = geopandas.GeometryArray.from_wkb(wkbs)
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry
        """
        if on_invalid != "raise":
            raise NotImplementedError(
                "GeometryArray.from_wkb(): only on_invalid='raise' is implemented"
            )

        from pyspark.sql.types import StructType, StructField, BinaryType

        schema = StructType([StructField("data", BinaryType(), True)])
        return cls._create_from_select(
            f"ST_GeomFromWKB(`data`)",
            data,
            schema,
            index,
            crs,
            **kwargs,
        )

    @classmethod
    def from_wkt(
        cls,
        data,
        index=None,
        crs: Union[Any, None] = None,
        on_invalid="raise",
        **kwargs,
    ) -> "GeometryArray":
        """
        Alternate constructor to create a ``GeometryArray``
        from a list or array of WKT objects

        Parameters
        ----------
        data : array-like, Series
            Series, list, or array of WKT objects
        index : array-like or Index
            The index for the GeometryArray.
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
        GeometryArray

        See Also
        --------
        GeometryArray.from_wkb

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> wkts = [
        ... 'POINT (1 1)',
        ... 'POINT (2 2)',
        ... 'POINT (3 3)',
        ... ]
        >>> s = GeometryArray.from_wkt(wkts)
        >>> s
        0    POINT (1 1)
        1    POINT (2 2)
        2    POINT (3 3)
        dtype: geometry
        """
        if on_invalid != "raise":
            raise NotImplementedError(
                "GeometryArray.from_wkt(): only on_invalid='raise' is implemented"
            )

        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([StructField("data", StringType(), True)])
        return cls._create_from_select(
            f"ST_GeomFromText(`data`)",
            data,
            schema,
            index,
            crs,
            **kwargs,
        )

    @classmethod
    def from_xy(cls, x, y, z=None, index=None, crs=None, **kwargs) -> "GeometryArray":
        """
        Alternate constructor to create a :class:`~geopandas.GeometryArray` of Point
        geometries from lists or arrays of x, y(, z) coordinates

        In case of geographic coordinates, it is assumed that longitude is captured
        by ``x`` coordinates and latitude by ``y``.

        Parameters
        ----------
        x, y, z : iterable
        index : array-like or Index, optional
            The index for the GeometryArray. If not given and all coordinate inputs
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
        GeometryArray

        See Also
        --------
        GeometryArray.from_wkt
        points_from_xy

        Examples
        --------

        >>> x = [2.5, 5, -3.0]
        >>> y = [0.5, 1, 1.5]
        >>> s = geopandas.GeometryArray.from_xy(x, y, crs="EPSG:4326")
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
            select = f"ST_PointZ(`x`, `y`, `z`)"
            schema.add(StructField("z", DoubleType(), True))
        else:
            data = list(zip(x, y))
            select = f"ST_Point(`x`, `y`)"

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
    ) -> "GeometryArray":
        raise NotImplementedError(
            _not_implemented_error(
                "from_shapely", "Creates GeometryArray from Shapely geometry objects."
            )
        )

    @classmethod
    def from_arrow(cls, arr, **kwargs) -> "GeometryArray":
        """
        Construct a GeometryArray from a Arrow array object with a GeoArrow
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
            Other parameters passed to the GeometryArray constructor.

        Returns
        -------
        GeometryArray

        See Also
        --------
        GeometryArray.to_arrow
        GeoDataFrame.from_arrow

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> import geoarrow.pyarrow as ga
        >>> array = ga.as_geoarrow([None, "POLYGON ((0 0, 1 1, 0 1, 0 0))", "LINESTRING (0 0, -1 1, 0 -1)"])
        >>> geoseries = GeometryArray.from_arrow(array)
        >>> geoseries
        0                              None
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))
        2      LINESTRING (0 0, -1 1, 0 -1)
        dtype: geometry

        """
        gpd_series = gpd.GeoSeries.from_arrow(arr, **kwargs)
        return GeometryArray(gpd_series)

    @classmethod
    def _create_from_select(
        cls, select: str, data, schema, index, crs, **kwargs
    ) -> "GeometryArray":

        from pyspark.pandas.utils import default_session
        from pyspark.pandas.internal import InternalField
        import numpy as np

        if isinstance(data, list) and not isinstance(data[0], (tuple, list)):
            data = [(obj,) for obj in data]

        name = kwargs.get("name", SPARK_DEFAULT_SERIES_NAME)

        select = f"{select} as `{name}`"

        if isinstance(data, pspd.Series):
            spark_df = data._internal.spark_frame
            assert len(schema) == 1
            spark_df = spark_df.withColumnRenamed(
                _get_series_col_name(data), schema[0].name
            )
        else:
            spark_df = default_session().createDataFrame(data, schema=schema)

        spark_df = spark_df.selectExpr(select)

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
        return GeometryArray(
            ps_series,
            index,
            crs=crs,
        )

    # ============================================================================
    # DATA ACCESS AND MANIPULATION
    # ============================================================================

    def isna(self) -> pspd.Series:
        """
        Detect missing values.

        Returns
        -------
        A boolean Series of the same size as the GeometryArray,
        True where a value is NA.

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon
        >>> s = GeometryArray(
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
        GeometryArray.notna : inverse of isna
        GeometryArray.is_empty : detect empty geometries
        """
        spark_expr = F.isnull(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return to_bool(result)

    def isnull(self) -> pspd.Series:
        """Alias for `isna` method. See `isna` for more detail."""
        return self.isna()

    def notna(self) -> pspd.Series:
        """
        Detect non-missing values.

        Returns
        -------
        A boolean pandas Series of the same size as the GeometryArray,
        False where a value is NA.

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon
        >>> s = GeometryArray(
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
        GeometryArray.isna : inverse of notna
        GeometryArray.is_empty : detect empty geometries
        """
        # After Sedona's minimum spark version is 3.5.0, we can use F.isnotnull(self.spark.column) instead
        spark_expr = ~F.isnull(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return to_bool(result)

    def notnull(self) -> pspd.Series:
        """Alias for `notna` method. See `notna` for more detail."""
        return self.notna()

    def fillna(
        self, value=None, inplace: bool = False, limit=None, **kwargs
    ) -> Union["GeometryArray", None]:
        """
        Fill NA values with geometry (or geometries).

        Parameters
        ----------
        value : shapely geometry or GeometryArray, default None
            If None is passed, NA values will be filled with GEOMETRYCOLLECTION EMPTY.
            If a shapely geometry object is passed, it will be
            used to fill all missing values. If a ``GeometryArray`` or ``GeometryArray``
            are passed, missing values will be filled based on the corresponding index
            locations. If pd.NA or np.nan are passed, values will be filled with
            ``None`` (not GEOMETRYCOLLECTION EMPTY).
        limit : int, default None
            This is the maximum number of entries along the entire axis
            where NaNs will be filled. Must be greater than 0 if not None.

        Returns
        -------
        GeometryArray

        Examples
        --------

        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Polygon
        >>> s = GeometryArray(
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

        Filled with another GeometryArray.

        >>> from shapely.geometry import Point
        >>> s_fill = GeometryArray(
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
        GeometryArray.isna : detect missing values
        """
        from shapely.geometry.base import BaseGeometry
        from geopandas.array import GeometryArray

        # TODO: Implement limit https://github.com/apache/sedona/issues/2068
        if limit:
            raise NotImplementedError(
                "GeometryArray.fillna() with limit is not implemented yet."
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

        elif isinstance(value, (GeometryArray, gpd.GeoSeries)):

            if not isinstance(value, GeometryArray):
                value = GeometryArray(value)

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

    def explode(self, ignore_index=False, index_parts=False) -> "GeometryArray":
        raise NotImplementedError(
            _not_implemented_error(
                "explode",
                "Explodes multi-part geometries into separate single-part geometries.",
            )
        )

    def to_crs(
        self, crs: Union[Any, None] = None, epsg: Union[int, None] = None
    ) -> "GeometryArray":
        """Returns a ``GeometryArray`` with all geometries transformed to a new
        coordinate reference system.

        Transform all geometries in a GeometryArray to a different coordinate
        reference system.  The ``crs`` attribute on the current GeometryArray must
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
        GeometryArray

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.geopandas import GeometryArray
        >>> geoseries = GeometryArray([Point(1, 1), Point(2, 2), Point(3, 3)], crs=4326)
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

        old_crs = self.crs
        if old_crs is None:
            raise ValueError(
                "Cannot transform naive geometries.  "
                "Please set a crs on the object first."
            )
        assert isinstance(old_crs, CRS)

        if crs is not None:
            crs = CRS.from_user_input(crs)
        elif epsg is not None:
            crs = CRS.from_epsg(epsg)
        else:
            raise ValueError("Must pass either crs or epsg.")

        # skip if the input CRS and output CRS are the exact same
        if old_crs.is_exact_same(crs):
            return self

        spark_expr = stf.ST_Transform(
            self.spark.column,
            F.lit(f"EPSG:{old_crs.to_epsg()}"),
            F.lit(f"EPSG:{crs.to_epsg()}"),
        )
        return self._query_geometry_column(
            spark_expr,
        )

    @property
    def bounds(self) -> pspd.DataFrame:
        """Returns a ``DataFrame`` with columns ``minx``, ``miny``, ``maxx``,
        ``maxy`` values containing the bounds for each geometry.

        See ``GeometryArray.total_bounds`` for the limits of the entire series.

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon, LineString
        >>> d = {'geometry': [Point(2, 1), Polygon([(0, 0), (1, 1), (1, 0)]),
        ... LineString([(0, 1), (1, 2)])]}
        >>> gdf = geopandas.GeoDataFrame(d, crs="EPSG:4326")
        >>> gdf.bounds
           minx  miny  maxx  maxy
        0   2.0   1.0   2.0   1.0
        1   0.0   0.0   1.0   1.0
        2   0.0   1.0   1.0   2.0

        You can assign the bounds to the ``GeoDataFrame`` as:

        >>> import pandas as pd
        >>> gdf = pd.concat([gdf, gdf.bounds], axis=1)
        >>> gdf
                                geometry  minx  miny  maxx  maxy
        0                     POINT (2 1)   2.0   1.0   2.0   1.0
        1  POLYGON ((0 0, 1 1, 1 0, 0 0))   0.0   0.0   1.0   1.0
        2           LINESTRING (0 1, 1 2)   0.0   1.0   1.0   2.0
        """
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
        return pspd.DataFrame(internal)

    @property
    def total_bounds(self):
        """Returns a tuple containing ``minx``, ``miny``, ``maxx``, ``maxy``
        values for the bounds of the series as a whole.

        See ``GeometryArray.bounds`` for the bounds of the geometries contained in
        the series.

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon, LineString
        >>> d = {'geometry': [Point(3, -1), Polygon([(0, 0), (1, 1), (1, 0)]),
        ... LineString([(0, 1), (1, 2)])]}
        >>> gdf = geopandas.GeoDataFrame(d, crs="EPSG:4326")
        >>> gdf.total_bounds
        array([ 0., -1.,  3.,  2.])
        """
        import numpy as np
        import warnings
        from pyspark.sql import functions as F

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
            total_bounds_df = ps_df.agg(
                {
                    "minx": ["min"],
                    "miny": ["min"],
                    "maxx": ["max"],
                    "maxy": ["max"],
                }
            )

            return np.array(
                (
                    np.nanmin(total_bounds_df["minx"]["min"]),  # minx
                    np.nanmin(total_bounds_df["miny"]["min"]),  # miny
                    np.nanmax(total_bounds_df["maxx"]["max"]),  # maxx
                    np.nanmax(total_bounds_df["maxy"]["max"]),  # maxy
                )
            )

    def estimate_utm_crs(self, datum_name: str = "WGS 84") -> "CRS":
        """Returns the estimated UTM CRS based on the bounds of the dataset.

        .. versionadded:: 0.9

        .. note:: Requires pyproj 3+

        Parameters
        ----------
        datum_name : str, optional
            The name of the datum to use in the query. Default is WGS 84.

        Returns
        -------
        pyproj.CRS

        Examples
        --------
        >>> import geodatasets
        >>> df = geopandas.read_file(
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

        # This implementation replicates the implementation in geopandas's implementation exactly.
        # https://github.com/geopandas/geopandas/blob/main/geopandas/array.py
        # The only difference is that we use Sedona's total_bounds property which is more efficient and scalable
        # than the geopandas implementation. The rest of the implementation always executes on 4 points (minx, miny, maxx, maxy),
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
        Returns a GeoJSON string representation of the GeometryArray.

        Parameters
        ----------
        show_bbox : bool, optional, default: True
            Include bbox (bounds) in the geojson
        drop_id : bool, default: False
            Whether to retain the index of the GeometryArray as the id property
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
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> s = GeometryArray([Point(1, 1), Point(2, 2), Point(3, 3)])
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
        GeometryArray.to_file : write GeometryArray to file
        """
        return self.to_geoframe(name="geometry").to_json(
            na="null", show_bbox=show_bbox, drop_id=drop_id, to_wgs84=to_wgs84, **kwargs
        )

    def to_wkb(self, hex: bool = False, **kwargs) -> pspd.Series:
        """
        Convert GeometryArray geometries to WKB

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
        GeometryArray.to_wkt

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon
        >>> s = GeometryArray(
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
        Convert GeometryArray geometries to WKT

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
        >>> s = GeometryArray([Point(1, 1), Point(2, 2), Point(3, 3)])
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
        GeometryArray.to_wkb
        """
        spark_expr = stf.ST_AsText(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def to_arrow(self, geometry_encoding="WKB", interleaved=True, include_z=None):
        """Encode a GeometryArray to GeoArrow format.

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
        >>> from sedona.geopandas import GeometryArray
        >>> from shapely.geometry import Point
        >>> gser = GeometryArray([Point(1, 2), Point(2, 1)])
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
        # Because this function returns the arrow array in memory, we simply rely on geopandas's implementation.
        # This also returns a geopandas specific data type, which can be converted to an actual pyarrow array,
        # so there is no direct Sedona equivalent. This way we also get all of the arguments implemented for free.
        return self.to_geopandas().to_arrow(
            geometry_encoding=geometry_encoding,
            interleaved=interleaved,
            include_z=include_z,
        )

    def clip(self, mask, keep_geom_type: bool = False, sort=False) -> "GeometryArray":
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
        """
        Write the ``GeometryArray`` to a file.

        Parameters
        ----------
        path : string
            File path or file handle to write to.
        driver : string, default None
            The format driver used to write the file.
            If not specified, it attempts to infer it from the file extension.
            If no extension is specified, Sedona will error.
            Options:
                - "geojson"
                - "geopackage"
                - "geoparquet"
        schema : dict, default None
            Not applicable to Sedona's implementation
        index : bool, default None
            If True, write index into one or more columns (for MultiIndex).
            Default None writes the index into one or more columns only if
            the index is named, is a MultiIndex, or has a non-integer data
            type. If False, no index is written.
        mode : string, default 'w'
            The write mode, 'w' to overwrite the existing file and 'a' to append.
            'overwrite' and 'append' are equivalent to 'w' and 'a' respectively.
        crs : pyproj.CRS, default None
            If specified, the CRS is passed to Fiona to
            better control how the file is written. If None, GeoPandas
            will determine the crs based on crs df attribute.
            The value can be anything accepted
            by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        engine : str
            Not applicable to Sedona's implementation
        metadata : dict[str, str], default None
            Optional metadata to be stored in the file. Keys and values must be
            strings. Supported only for "GPKG" driver. Not supported by Sedona
        **kwargs :
            Keyword args to be passed to the engine, and can be used to write
            to multi-layer data, store data within archives (zip files), etc.
            In case of the "pyogrio" engine, the keyword arguments are passed to
            `pyogrio.write_dataframe`. In case of the "fiona" engine, the keyword
            arguments are passed to fiona.open`. For more information on possible
            keywords, type: ``import pyogrio; help(pyogrio.write_dataframe)``.

        Examples
        --------

        >>> gdf = GeoDataFrame({"geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])], "int": [1, 2]}
        >>> gdf.to_file(filepath, format="geoparquet")

        With selected drivers you can also append to a file with `mode="a"`:

        >>> gdf.to_file(gdf, driver="geojson", mode="a")

        When the index is of non-integer dtype, index=None (default) is treated as True, writing the index to the file.

        >>> gdf = GeoDataFrame({"geometry": [Point(0, 0)]}, index=["a", "b"])
        >>> gdf.to_file(gdf, driver="geoparquet")
        """
        self.to_geoframe().to_file(path, driver, index=index, **kwargs)

    def to_parquet(self, path, **kwargs):
        """
        Write the GeometryArray to a GeoParquet file.
        Parameters:
        - path: str
            The file path where the GeoParquet file will be written.
        - kwargs: Any
            Additional arguments to pass to the Sedona DataFrame output function.
        """
        self.to_geoframe().to_file(path, driver="geoparquet", **kwargs)

    # -----------------------------------------------------------------------------
    # # Utils
    # -----------------------------------------------------------------------------

    def _update_inplace(self, result: "GeometryArray"):
        self.rename(result.name, inplace=True)
        self._update_anchor(result._anchor)

    def _make_series_of_val(self, value: Any):
        """
        A helper method to turn single objects into series (ps.Series or GeometryArray when possible)
        Returns:
            tuple[pspd.Series, bool]:
                - The series of the value
                - Whether returned value was a single object extended into a series (useful for row-wise 'align' parameter)
        """
        # generator instead of a in-memory list
        if not isinstance(value, pspd.Series):
            lst = [value for _ in range(len(self))]
            if isinstance(value, BaseGeometry):
                return GeometryArray(lst), True
            else:
                # e.g int input
                return pspd.Series(lst), True
        else:
            return value, False

    def to_geoframe(self, name=None):
        if name is not None:
            renamed = self.rename(name)
        elif self._column_label is None:
            renamed = self.rename("geometry")
        else:
            renamed = self

        # to_spark() is important here to ensure that the spark column names are set to the pandas column ones
        return GeoDataFrame(pspd.DataFrame(renamed._internal).to_spark())


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def _get_series_col_name(ps_series: pspd.Series) -> str:
    return ps_series.name if ps_series.name else SPARK_DEFAULT_SERIES_NAME


def to_bool(ps_series: pspd.Series, default: bool = False) -> pspd.Series:
    """
    Cast a ps.Series to bool type if it's not one, converting None values to the default value.
    """
    if ps_series.dtype.name != "bool":
        # fill None values with the default value
        ps_series.fillna(default, inplace=True)

    return ps_series


def _to_geo_series(df: PandasOnSparkSeries) -> GeometryArray:
    """
    Get the first Series from the DataFrame.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - GeometryArray: The first Series from the DataFrame.
    """
    return GeometryArray(data=df)
