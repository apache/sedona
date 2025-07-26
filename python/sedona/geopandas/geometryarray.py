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
    # PROPERTIES AND ATTRIBUTES
    # ============================================================================

    @property
    def geometry(self) -> "GeometryArray":
        return self

    @property
    def sindex(self) -> SpatialIndex:
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
        spark_expr = stf.ST_IsSimple(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

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
        spark_expr = stf.ST_HasZ(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def get_precision(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def get_geometry(self, index) -> "GeometryArray":
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

        return _to_bool(result)

    def disjoint(self, other, align=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def intersects(
        self,
        other: Union["GeometryArray", BaseGeometry],
        align: Union[bool, None] = None,
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
        self,
        other: Union["GeometryArray", BaseGeometry],
        align: Union[bool, None] = None,
    ) -> "GeometryArray":
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
        spark_expr = (
            stf.ST_SimplifyPreserveTopology(self.spark.column, tolerance)
            if preserve_topology
            else stf.ST_Simplify(self.spark.column, tolerance)
        )

        return self._query_geometry_column(spark_expr)

    @property
    def geometry(self) -> "GeometryArray":
        return self

    @property
    def x(self) -> pspd.Series:
        spark_col = stf.ST_X(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def y(self) -> pspd.Series:
        spark_col = stf.ST_Y(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def z(self) -> pspd.Series:
        spark_col = stf.ST_Z(self.spark.column)
        return self._query_geometry_column(
            spark_col,
            returns_geom=False,
        )

    @property
    def m(self) -> pspd.Series:
        raise NotImplementedError("GeometryArray.m() is not implemented yet.")

    @classmethod
    def from_shapely(
        cls, data, index=None, crs: Union[Any, None] = None, **kwargs
    ) -> "GeometryArray":
        raise NotImplementedError(
            _not_implemented_error(
                "from_shapely", "Creates GeometryArray from Shapely geometry objects."
            )
        )

    # ============================================================================
    # DATA ACCESS AND MANIPULATION
    # ============================================================================

    def isna(self) -> pspd.Series:
        spark_expr = F.isnull(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    def notna(self) -> pspd.Series:
        # After Sedona's minimum spark version is 3.5.0, we can use F.isnotnull(self.spark.column) instead
        spark_expr = ~F.isnull(self.spark.column)
        result = self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )
        return _to_bool(result)

    def fillna(
        self, value=None, inplace: bool = False, limit=None, **kwargs
    ) -> Union["GeometryArray", None]:
        from shapely.geometry.base import BaseGeometry
        from geopandas.array import GeometryArray as gpd_GeometryArray

        # TODO: Implement limit https://github.com/apache/sedona/issues/2068
        if limit:
            raise NotImplementedError(
                "GeometryArray.fillna() with limit is not implemented yet."
            )

        align = True

        if isinstance(value, sgpd.GeoSeries):
            value = GeometryArray(value)

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

        elif isinstance(value, (GeometryArray, gpd_GeometryArray, gpd.GeoSeries)):

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

    def to_wkb(self, hex: bool = False, **kwargs) -> pspd.Series:
        spark_expr = stf.ST_AsBinary(self.spark.column)

        if hex:
            spark_expr = F.hex(spark_expr)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def to_wkt(self, **kwargs) -> pspd.Series:
        spark_expr = stf.ST_AsText(self.spark.column)
        return self._query_geometry_column(
            spark_expr,
            returns_geom=False,
        )

    def clip(self, mask, keep_geom_type: bool = False, sort=False) -> "GeometryArray":
        raise NotImplementedError(
            _not_implemented_error(
                "clip", "Clips geometries to the bounds of a mask geometry."
            )
        )

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
