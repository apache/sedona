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
import pandas as pd
import pyspark.pandas as pspd
import pyspark
from pyspark.pandas import Series as PandasOnSparkSeries
from pyspark.pandas._typing import Dtype
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import InternalFrame
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import scol_for, log_advice
from pyspark.sql.types import BinaryType

import shapely
from shapely.geometry.base import BaseGeometry

from sedona.geopandas._typing import Label
from sedona.geopandas.base import GeoFrame
from sedona.geopandas.geodataframe import GeoDataFrame
from sedona.geopandas.geoindex import GeoIndex

from pyspark.pandas.internal import SPARK_DEFAULT_INDEX_NAME  # __index_level_0__


class GeoSeries(GeoFrame, pspd.Series):
    """
    A class representing a GeoSeries, inheriting from GeoFrame and pyspark.pandas.DataFrame.
    """

    def __getitem__(self, key: Any) -> Any:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def __repr__(self) -> str:
        """
        Return a string representation of the GeoSeries in WKT format.
        """
        gpd_series = self.to_geopandas()
        return gpd_series.__repr__()

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

        Parameters:
        - data: The input data for the GeoSeries. It can be a GeoDataFrame, GeoSeries, or pandas Series.
        - index: The index for the GeoSeries.
        - crs: Coordinate Reference System for the GeoSeries.
        - dtype: Data type for the GeoSeries.
        - name: Name of the GeoSeries.
        - copy: Whether to copy the input data.
        - fastpath: Internal parameter for fast initialization.

        Examples:
        >>> from shapely.geometry import Point
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeoSeries

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

        if isinstance(
            data, (GeoDataFrame, GeoSeries, PandasOnSparkSeries, PandasOnSparkDataFrame)
        ):
            assert dtype is None
            assert name is None
            assert not copy
            assert not fastpath

            self._anchor = data
            self._col_label = index

            data_crs = None
            if hasattr(data, "crs"):
                data_crs = data.crs
            if data_crs is not None and crs is not None and data_crs != crs:
                raise ValueError(
                    "CRS mismatch between CRS of the passed geometries "
                    "and 'crs'. Use 'GeoSeries.set_crs(crs, "
                    "allow_override=True)' to overwrite CRS or "
                    "'GeoSeries.to_crs(crs)' to reproject geometries. "
                )
        else:
            if isinstance(data, pd.Series):
                assert index is None
                assert dtype is None
                assert name is None
                assert not copy
                assert not fastpath
                s = data
            else:
                s = pd.Series(
                    data=data,
                    index=index,
                    dtype=dtype,
                    name=name,
                    copy=copy,
                    fastpath=fastpath,
                )
            gs = gpd.GeoSeries(s)
            pdf = pd.Series(
                gs.apply(lambda geom: geom.wkb if geom is not None else None)
            )
            # initialize the parent class pyspark Series with the pandas Series
            super().__init__(
                data=pdf,
                index=index,
                dtype=dtype,
                name=name,
                copy=copy,
                fastpath=fastpath,
            )

        if crs:
            self.set_crs(crs, inplace=True)

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

        tmp_df = self._process_geometry_column("ST_SRID", rename="crs")
        srid = tmp_df.take([0])[0]
        # Sedona returns 0 if doesn't exist
        return CRS.from_user_input(srid) if srid else None

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
    ) -> "GeoSeries": ...

    def set_crs(
        self,
        crs: Union[Any, None] = None,
        epsg: Union[int, None] = None,
        inplace: bool = False,
        allow_override: bool = False,
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
        allow_override : bool, default False
            If the GeoSeries already has a CRS, allow to replace the
            existing CRS, even when both are not equal.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> s = geopandas.GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
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

        curr_crs = self.crs

        if not allow_override and curr_crs is not None and not curr_crs == crs:
            raise ValueError(
                "The GeoSeries already has a CRS which is not equal to the passed "
                "CRS. Specify 'allow_override=True' to allow replacing the existing "
                "CRS without doing any transformation. If you actually want to "
                "transform the geometries, use 'GeoSeries.to_crs' instead."
            )

        # 0 indicates no srid in sedona
        new_epsg = crs.to_epsg() if crs else 0
        # Keep the same column name instead of renaming it
        result = self._process_geometry_column("ST_SetSRID", rename="", srid=new_epsg)

        if inplace:
            self._update_anchor(result._to_spark_pandas_df())
            return None

        return result

    def _process_geometry_column(
        self, operation: str, rename: str, *args, **kwargs
    ) -> "GeoSeries":
        """
        Helper method to process a single geometry column with a specified operation.
        This method wraps the _query_geometry_column method for simpler convenient use.

        Parameters
        ----------
        operation : str
            The spatial operation to apply (e.g., 'ST_Area', 'ST_Buffer').
        rename : str
            The name of the resulting column. If empty, the old column name is maintained.
        args : tuple
            Positional arguments for the operation.
        kwargs : dict
            Keyword arguments for the operation.

        Returns
        -------
        GeoSeries
            A GeoSeries with the operation applied to the geometry column.
        """
        # Find the first column with BinaryType or GeometryType
        first_col = self.get_first_geometry_column()  # TODO: fixme

        # Handle both positional and keyword arguments
        all_args = list(args)
        for k, v in kwargs.items():
            all_args.append(v)

        # Join all arguments as comma-separated values
        params = ""
        if all_args:
            params_list = [
                str(arg) if isinstance(arg, (int, float)) else repr(arg)
                for arg in all_args
            ]
            params = f", {', '.join(params_list)}"

        sql_expr = f"{operation}(`{first_col}`{params})"

        return self._query_geometry_column(sql_expr, first_col, rename)

    def _query_geometry_column(
        self,
        query: str,
        cols: Union[List[str], str],
        rename: str,
        df: pyspark.sql.DataFrame = None,
    ) -> "GeoSeries":
        """
        Helper method to query a single geometry column with a specified operation.

        Parameters
        ----------
        query : str
            The query to apply to the geometry column.
        cols : List[str] or str
            The names of the columns to query.
        rename : str
            The name of the resulting column.
        df : pyspark.sql.DataFrame
            The dataframe to query. If not provided, the internal dataframe will be used.

        Returns
        -------
        GeoSeries
            A GeoSeries with the operation applied to the geometry column.
        """
        if not cols:
            raise ValueError("No valid geometry column found.")

        if isinstance(cols, str):
            cols = [cols]

        df = self._internal.spark_frame if df is None else df

        for col in cols:
            data_type = df.schema[col].dataType

            rename = col if not rename else rename

            if isinstance(data_type, BinaryType):
                # the backticks here are important so we don't match strings that happen to be the same as the column name
                query = query.replace(f"`{col}`", f"ST_GeomFromWKB(`{col}`)")

        sql_expr = f"{query} as `{rename}`"

        sdf = df.selectExpr(sql_expr)
        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=None,
            column_labels=[self._column_label],
            data_spark_columns=[scol_for(sdf, rename)],
            data_fields=[self._internal.data_fields[0]],
            column_label_names=self._internal.column_label_names,
        )
        return _to_geo_series(first_series(PandasOnSparkDataFrame(internal)))

    @property
    def dtypes(self) -> Union[gpd.GeoSeries, pd.Series, Dtype]:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def to_geopandas(self) -> gpd.GeoSeries:
        """
        Convert the GeoSeries to a geopandas GeoSeries.

        Returns:
        - geopandas.GeoSeries: A geopandas GeoSeries.
        """
        from pyspark.pandas.utils import log_advice

        log_advice(
            "`to_geopandas` loads all data into the driver's memory. "
            "It should only be used if the resulting geopandas GeoSeries is expected to be small."
        )
        return self._to_geopandas()

    def _to_geopandas(self) -> gpd.GeoSeries:
        """
        Same as `to_geopandas()`, without issuing the advice log for internal usage.
        """
        pd_series = self._to_internal_pandas()
        try:
            return gpd.GeoSeries(
                pd_series.map(lambda wkb: shapely.wkb.loads(bytes(wkb)))
            )
        except Exception as e:
            return gpd.GeoSeries(pd_series)

    def to_spark_pandas(self) -> pspd.Series:
        return pspd.Series(self._psdf._to_internal_pandas())

    def _to_spark_pandas_df(self) -> pspd.DataFrame:
        return pspd.DataFrame(self._psdf._internal)

    @property
    def geometry(self) -> "GeoSeries":
        return self

    @property
    def geoindex(self) -> "GeoIndex":
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def copy(self, deep=False):
        """
        Make a copy of this GeoSeries object.

        Parameters:
        - deep: bool, default False
            If True, a deep copy of the data is made. Otherwise, a shallow copy is made.

        Returns:
        - GeoSeries: A copy of this GeoSeries object.

        Examples:
        >>> from shapely.geometry import Point
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeoSeries

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
        """
        Returns a Series containing the area of each geometry in the GeoSeries expressed in the units of the CRS.

        Returns
        -------
        Series
            A Series containing the area of each geometry.

        Examples
        --------
        >>> from shapely.geometry import Polygon
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeoSeries

        >>> gs = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])])
        >>> gs.area
        0    1.0
        1    4.0
        dtype: float64
        """
        return self._process_geometry_column("ST_Area", rename="area").to_spark_pandas()

    @property
    def geom_type(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def type(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def length(self) -> pspd.Series:
        """
        Returns a Series containing the length of each geometry in the GeoSeries.

        In the case of a (Multi)Polygon it measures the length of its exterior (i.e. perimeter).

        For a GeometryCollection it measures sums the values for each of the individual geometries.

        Returns
        -------
        Series
            A Series containing the length of each geometry.

        Examples
        --------
        >>> from shapely.geometry import Polygon
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeoSeries

        >>> gs = GeoSeries([Point(0, 0), LineString([(0, 0), (1, 1)]), Polygon([(0, 0), (1, 0), (1, 1)]), GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)]), Polygon([(0, 0), (1, 0), (1, 1)])])])
        >>> gs.length
        0    0.000000
        1    1.414214
        2    3.414214
        3    4.828427
        dtype: float64
        """
        col = self.get_first_geometry_column()
        select = f"""
            CASE
                WHEN GeometryType(`{col}`) IN ('LINESTRING', 'MULTILINESTRING') THEN ST_Length(`{col}`)
                WHEN GeometryType(`{col}`) IN ('POLYGON', 'MULTIPOLYGON') THEN ST_Perimeter(`{col}`)
                WHEN GeometryType(`{col}`) IN ('POINT', 'MULTIPOINT') THEN 0.0
                WHEN GeometryType(`{col}`) IN ('GEOMETRYCOLLECTION') THEN ST_Length(`{col}`) + ST_Perimeter(`{col}`)
            END"""
        return self._query_geometry_column(
            select, col, rename="length"
        ).to_spark_pandas()

    @property
    def is_valid(self) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that are valid.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from shapely.geometry import Polygon
        >>> s = geopandas.GeoSeries(
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
        GeoSeries.is_valid_reason : reason for invalidity
        """
        return (
            self._process_geometry_column("ST_IsValid", rename="is_valid")
            .to_spark_pandas()
            .astype("bool")
        )

    def is_valid_reason(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_empty(self) -> pspd.Series:
        """
        Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        empty geometries.

        Examples
        --------
        An example of a GeoDataFrame with one empty point, one point and one missing
        value:

        >>> from shapely.geometry import Point
        >>> d = {'geometry': [Point(), Point(2, 1), None]}
        >>> gdf = geopandas.GeoDataFrame(d, crs="EPSG:4326")
        >>> gdf
            geometry
        0  POINT EMPTY
        1  POINT (2 1)
        2         None

        >>> gdf.is_empty
        0     True
        1    False
        2    False
        dtype: bool

        See Also
        --------
        GeoSeries.isna : detect missing values
        """
        return (
            self._process_geometry_column("ST_IsEmpty", rename="is_empty")
            .to_spark_pandas()
            .astype("bool")
        )

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
    def is_simple(self) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that do not cross themselves.

        This is meaningful only for `LineStrings` and `LinearRings`.

        Examples
        --------
        >>> from shapely.geometry import LineString
        >>> s = geopandas.GeoSeries(
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
        return (
            self._process_geometry_column("ST_IsSimple", rename="is_simple")
            .to_spark_pandas()
            .astype("bool")
        )

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
    def has_z(self) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        features that have a z-component.

        Notes
        -----
        Every operation in GeoPandas is planar, i.e. the potential third
        dimension is not taken into account.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> s = geopandas.GeoSeries(
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
        return self._process_geometry_column(
            "ST_HasZ", rename="has_z"
        ).to_spark_pandas()

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

    def intersects(
        self, other: Union["GeoSeries", BaseGeometry], align: Union[bool, None] = None
    ) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that intersects `other`.

        An object is said to intersect `other` if its `boundary` and `interior`
        intersects in any way with those of the other.

        The operation works on a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if is
            intersected.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved. (not supported in Sedona Geopandas)

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = geopandas.GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = geopandas.GeoSeries(
        ...     [
        ...         LineString([(1, 0), (1, 3)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(1, 1),
        ...         Point(-100, -100),
        ...     ],
        ...     index=range(1, 5),
        ... )

        We can check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We align both GeoSeries
        based on index values and compare elements with the same index:

        >>> s.intersects(s2)
        0     True
        1     True
        2     True
        3    False
        dtype: bool

        We can also check if each geometry of GeoSeries intersects a single
        geometry:

        >>> line = LineString([(-1, 1), (3, 1)])
        >>> s.intersects(line)
        0    True
        1    True
        2    True
        3    True
        dtype: bool

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeoSeries ``crosses`` *any* element of the other one.

        See also
        --------
        GeoSeries.disjoint
        GeoSeries.crosses
        GeoSeries.touches
        GeoSeries.intersection
        """
        return (
            self._row_wise_operation(
                "ST_Intersects(`L`, `R`)", other, align, rename="intersects"
            )
            .to_spark_pandas()
            .astype("bool")
        )

    def intersection(
        self, other: Union["GeoSeries", BaseGeometry], align: Union[bool, None] = None
    ) -> "GeoSeries":
        """Returns a ``GeoSeries`` of the intersection of points in each
        aligned geometry with `other`.

        The operation works on a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            intersection with.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved. (not supported in Sedona Geopandas)

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = geopandas.GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = geopandas.GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(1, 0), (1, 3)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(1, 1),
        ...         Point(-100, -100),
        ...     ],
        ... )

        We can do an intersection of each geometry and a single
        shapely geometry:

        >>> geom = Polygon([(-0.5, -0.5), (-0.5, 2.5), (2.5, 2.5), (2.5, -0.5), (-0.5, -0.5)])
        >>> s.intersection(geom)
            Polygon([(0, 0), (2, 2), (0, 2)]),
            Polygon([(0, 0), (2, 2), (0, 2)]),
            LineString([(0, 0), (2, 2)]),
            LineString([(2, 0), (0, 2)]),
            Point(0, 1),
        dtype: geometry

        >>> geom = Polygon([(-0.5, -0.5), (-0.5, 2.5), (2.5, 2.5), (2.5, -0.5), (-0.5, -0.5)])
        >>> s.intersection(Polygon([(0, 0), (1, 1), (0, 1)]))
        0         POLYGON ((0 0, 2 2, 0 2))
        1         POLYGON ((0 0, 2 2, 0 2))
        2             LINESTRING (0 0, 2 2)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (0 1)
        dtype: geometry

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We align both GeoSeries
        based on index values and compare elements with the same index.

        >>> s.intersection(s2)
        0    POLYGON ((0 0, 1 1, 0 1, 0 0))
        1             LINESTRING (1 1, 1 2)
        2                       POINT (1 1)
        3                       POINT (1 1)
        4                     POLYGON EMPTY
        dtype: geometry

        See Also
        --------
        GeoSeries.difference
        GeoSeries.symmetric_difference
        GeoSeries.union
        """
        return self._row_wise_operation(
            "ST_Intersection(`L`, `R`)", other, align, rename="intersection"
        )

    def _row_wise_operation(
        self,
        select: str,
        other: Union["GeoSeries", BaseGeometry],
        align: Union[bool, None],
        rename: str,
    ):
        """
        Helper function to perform a row-wise operation on two GeoSeries.
        The self column and other column are aliased to `L` and `R`, respectively.
        """
        from pyspark.sql.functions import col

        # Note: this is specifically False. None is valid since it defaults to True similar to geopandas
        if align is False:
            raise NotImplementedError("Sedona Geopandas does not support align=False")

        if isinstance(other, BaseGeometry):
            other = GeoSeries([other] * len(self))

        assert isinstance(other, GeoSeries), f"Invalid type for other: {type(other)}"

        # TODO: this does not yet support multi-index
        df = self._internal.spark_frame.select(
            col(self.get_first_geometry_column()).alias("L"),
            col(SPARK_DEFAULT_INDEX_NAME),
        )
        other_df = other._internal.spark_frame.select(
            col(other.get_first_geometry_column()).alias("R"),
            col(SPARK_DEFAULT_INDEX_NAME),
        )
        joined_df = df.join(other_df, on=SPARK_DEFAULT_INDEX_NAME, how="outer")
        return self._query_geometry_column(
            select,
            cols=["L", "R"],
            rename=rename,
            df=joined_df,
        )

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
    ) -> "GeoSeries":
        """
        Returns a GeoSeries of geometries representing all points within a given distance of each geometric object.

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
        GeoSeries
            A GeoSeries of buffered geometries.
        """
        return self._process_geometry_column(
            "ST_Buffer", rename="buffer", distance=distance
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
        Perform a spatial join between two GeoSeries.
        Parameters:
        - other: GeoSeries
        - how: str, default 'inner'
            The type of join to perform.
        - predicate: str, default 'intersects'
            The spatial predicate to use for the join.
        - lsuffix: str, default 'left'
            Suffix to apply to the left GeoSeries' column names.
        - rsuffix: str, default 'right'
            Suffix to apply to the right GeoSeries' column names.
        - distance: float, optional
            The distance threshold for the join.
        - on_attribute: str, optional
            The attribute to join on.
        - kwargs: Any
            Additional arguments to pass to the join function.
        Returns:
        - GeoSeries
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
    def geometry(self) -> "GeoSeries":
        return self

    @property
    def x(self) -> pspd.Series:
        """Return the x location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from shapely.geometry import Point
        >>> s = geopandas.GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
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
        return self._process_geometry_column("ST_X", rename="x").to_spark_pandas()

    @property
    def y(self) -> pspd.Series:
        """Return the y location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from shapely.geometry import Point
        >>> s = geopandas.GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
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
        return self._process_geometry_column("ST_Y", rename="y").to_spark_pandas()

    @property
    def z(self) -> pspd.Series:
        """Return the z location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from shapely.geometry import Point
        >>> s = geopandas.GeoSeries([Point(1, 1, 1), Point(2, 2, 2), Point(3, 3, 3)])
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
        return self._process_geometry_column("ST_Z", rename="z").to_spark_pandas()

    @property
    def m(self) -> pspd.Series:
        raise NotImplementedError("GeoSeries.m() is not implemented yet.")

    @classmethod
    def from_file(
        cls, filename: Union[os.PathLike, typing.IO], **kwargs
    ) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_file() is not implemented yet.")

    @classmethod
    def from_wkb(
        cls,
        data,
        index=None,
        crs: Union[Any, None] = None,
        on_invalid="raise",
        **kwargs,
    ) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_wkb() is not implemented yet.")

    @classmethod
    def from_wkt(
        cls,
        data,
        index=None,
        crs: Union[Any, None] = None,
        on_invalid="raise",
        **kwargs,
    ) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_wkt() is not implemented yet.")

    @classmethod
    def from_xy(cls, x, y, z=None, index=None, crs=None, **kwargs) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_xy() is not implemented yet.")

    @classmethod
    def from_shapely(
        cls, data, index=None, crs: Union[Any, None] = None, **kwargs
    ) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_shapely() is not implemented yet.")

    @classmethod
    def from_arrow(cls, arr, **kwargs) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_arrow() is not implemented yet.")

    def to_file(
        self,
        filename: Union[os.PathLike, typing.IO],
        driver: Union[str, None] = None,
        index: Union[bool, None] = None,
        **kwargs,
    ):
        raise NotImplementedError("GeoSeries.to_file() is not implemented yet.")

    def isna(self) -> pspd.Series:
        raise NotImplementedError("GeoSeries.isna() is not implemented yet.")

    def isnull(self) -> pspd.Series:
        raise NotImplementedError("GeoSeries.isnull() is not implemented yet.")

    def notna(self) -> pspd.Series:
        raise NotImplementedError("GeoSeries.notna() is not implemented yet.")

    def notnull(self) -> pspd.Series:
        """Alias for `notna` method. See `notna` for more detail."""
        return self.notna()

    def fillna(self, value: Any) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.fillna() is not implemented yet.")

    def explode(self, ignore_index=False, index_parts=False) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.explode() is not implemented yet.")

    def to_crs(
        self, crs: Union[Any, None] = None, epsg: Union[int, None] = None
    ) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.to_crs() is not implemented yet.")

    def estimate_utm_crs(self, datum_name: str = "WGS 84"):
        raise NotImplementedError(
            "GeoSeries.estimate_utm_crs() is not implemented yet."
        )

    def to_json(
        self,
        show_bbox: bool = True,
        drop_id: bool = False,
        to_wgs84: bool = False,
        **kwargs,
    ) -> str:
        raise NotImplementedError("GeoSeries.to_json() is not implemented yet.")

    def to_wkb(self, hex: bool = False, **kwargs) -> pspd.Series:
        raise NotImplementedError("GeoSeries.to_wkb() is not implemented yet.")

    def to_wkt(self, **kwargs) -> pspd.Series:
        raise NotImplementedError("GeoSeries.to_wkt() is not implemented yet.")

    def to_arrow(self, geometry_encoding="WKB", interleaved=True, include_z=None):
        raise NotImplementedError("GeoSeries.to_arrow() is not implemented yet.")

    def clip(self, mask, keep_geom_type: bool = False, sort=False) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.clip() is not implemented yet.")

    # -----------------------------------------------------------------------------
    # # Utils
    # -----------------------------------------------------------------------------

    def get_first_geometry_column(self) -> Union[str, None]:
        first_binary_or_geometry_col = next(
            (
                field.name
                for field in self._internal.spark_frame.schema.fields
                if isinstance(field.dataType, BinaryType)
                or field.dataType.typeName() == "geometrytype"
            ),
            None,
        )
        return first_binary_or_geometry_col


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def _to_geo_series(df: PandasOnSparkSeries) -> GeoSeries:
    """
    Get the first Series from the DataFrame.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - GeoSeries: The first Series from the DataFrame.
    """
    return GeoSeries(data=df)
