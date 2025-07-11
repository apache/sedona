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
from sedona.geopandas.sindex import SpatialIndex

from pyspark.pandas.internal import (
    SPARK_DEFAULT_INDEX_NAME,  # __index_level_0__
    NATURAL_ORDER_COLUMN_NAME,
)


class GeoSeries(GeoFrame, pspd.Series):
    """
    A class representing a GeoSeries, inheriting from GeoFrame and pyspark.pandas.DataFrame.
    """

    def __getitem__(self, key: Any) -> Any:
        return pspd.Series.__getitem__(self, key)

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

        if isinstance(
            data, (GeoDataFrame, GeoSeries, PandasOnSparkSeries, PandasOnSparkDataFrame)
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
                    "and 'crs'. Use 'GeoSeries.set_crs(crs, "
                    "allow_override=True)' to overwrite CRS or "
                    "'GeoSeries.to_crs(crs)' to reproject geometries. "
                )
            # This is a temporary workaround since pyspark errors when creating a ps.Series from a ps.Series
            # This is NOT a scalable solution since we call to_pandas() on the data and is a hacky solution
            # but this should be resolved if/once https://github.com/apache/spark/pull/51300 is merged in.
            # For now, we reset self._anchor = data to have keep the geometry information (e.g crs) that's lost in to_pandas()

            pd_data = data.to_pandas()

            try:
                pd_data = pd_data.apply(try_geom_to_ewkb)
            except Exception as e:
                raise TypeError(f"Non-geometry column passed to GeoSeries: {e}")

            super().__init__(
                data=pd_data,
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

            try:
                pdf = s.apply(try_geom_to_ewkb)
            except Exception as e:
                raise TypeError(f"Non-geometry column passed to GeoSeries: {e}")

            # initialize the parent class pyspark Series with the pandas Series
            super().__init__(data=pdf)

        # manually set it to binary type
        col = next(
            field.name
            for field in self._internal.spark_frame.schema.fields
            if field.name not in (NATURAL_ORDER_COLUMN_NAME, SPARK_DEFAULT_INDEX_NAME)
        )
        self._internal.spark_frame.schema[col].dataType = BinaryType()

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

        tmp = self._process_geometry_column("ST_SRID", rename="crs", returns_geom=False)
        ps_series = tmp.take([0])
        srid = ps_series.iloc[0]
        # Sedona returns 0 if doesn't exist
        return CRS.from_user_input(srid) if srid != 0 and not pd.isna(srid) else None

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
        >>> from sedona.geopandas import GeoSeries
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
        col = self.get_first_geometry_column()

        select = f"ST_SetSRID(`{col}`, {new_epsg})"

        # Keep the same column name instead of renaming it
        result = self._query_geometry_column(select, col, rename="")

        if inplace:
            self._update_anchor(_to_spark_pandas_df(result))
            return None

        return result

    def _process_geometry_column(
        self, operation: str, rename: str, returns_geom: bool = True, *args, **kwargs
    ) -> Union["GeoSeries", pspd.Series]:
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

        return self._query_geometry_column(
            sql_expr, first_col, rename, returns_geom=returns_geom
        )

    def _query_geometry_column(
        self,
        query: str,
        cols: Union[List[str], str],
        rename: str,
        df: pyspark.sql.DataFrame = None,
        returns_geom: bool = True,
    ) -> Union["GeoSeries", pspd.Series]:
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
        returns_geom : bool, default True
            If True, the geometry column will be converted back to EWKB format.

        Returns
        -------
        GeoSeries
            A GeoSeries with the operation applied to the geometry column.
        """
        if not cols:
            raise ValueError("No valid geometry column found.")

        df = self._internal.spark_frame if df is None else df

        if isinstance(cols, str):
            col = cols
            data_type = df.schema[col].dataType
            if isinstance(data_type, BinaryType):
                query = query.replace(f"`{cols}`", f"ST_GeomFromWKB(`{cols}`)")

            # Convert back to EWKB format if the return type is a geometry
            if returns_geom:
                query = f"ST_AsEWKB({query})"

            rename = col if not rename else rename

            query = f"{query} as `{rename}`"

        elif isinstance(cols, list):
            for col in cols:
                data_type = df.schema[col].dataType

                if isinstance(data_type, BinaryType):
                    # the backticks here are important so we don't match strings that happen to be the same as the column name
                    query = query.replace(f"`{col}`", f"ST_GeomFromWKB(`{col}`)")

            # must have rename for multiple columns since we don't know which name to default to
            assert rename

            query = f"{query} as `{rename}`"

        # We always select NATURAL_ORDER_COLUMN_NAME, to avoid having to regenerate it in the result
        # We always select SPARK_DEFAULT_INDEX_NAME, to retain series index info
        sdf = df.selectExpr(query, SPARK_DEFAULT_INDEX_NAME, NATURAL_ORDER_COLUMN_NAME)

        internal = self._internal.copy(
            spark_frame=sdf,
            index_fields=[self._internal.index_fields[0]],
            index_spark_columns=[scol_for(sdf, SPARK_DEFAULT_INDEX_NAME)],
            data_spark_columns=[scol_for(sdf, rename)],
            data_fields=[self._internal.data_fields[0].copy(name=rename)],
            column_label_names=[(rename,)],
        )
        ps_series = first_series(PandasOnSparkDataFrame(internal))

        return GeoSeries(ps_series) if returns_geom else ps_series

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
                pd_series.map(
                    lambda wkb: (
                        shapely.wkb.loads(bytes(wkb)) if not pd.isna(wkb) else None
                    )
                ),
                crs=self.crs,
            )
        except TypeError:
            return gpd.GeoSeries(pd_series, crs=self.crs)

    def to_spark_pandas(self) -> pspd.Series:
        return pspd.Series(self._psdf._to_internal_pandas())

    @property
    def geometry(self) -> "GeoSeries":
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
        geometry_column = self.get_first_geometry_column()
        if geometry_column is None:
            raise ValueError("No geometry column found in GeoSeries")
        return SpatialIndex(self._internal.spark_frame, column_name=geometry_column)

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
        >>> from sedona.geopandas import GeoSeries

        >>> gs = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])])
        >>> gs.area
        0    1.0
        1    4.0
        dtype: float64
        """
        return self._process_geometry_column(
            "ST_Area", rename="area", returns_geom=False
        )

    @property
    def geom_type(self) -> pspd.Series:
        """
        Returns a series of strings specifying the geometry type of each geometry of each object.

        Note: Unlike Geopandas, Sedona returns LineString instead of LinearRing.

        Returns
        -------
        Series
            A Series containing the geometry type of each geometry.

        Examples
        --------
        >>> from shapely.geometry import Polygon, Point
        >>> from sedona.geopandas import GeoSeries

        >>> gs = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Point(0, 0)])
        >>> gs.geom_type
        0    POLYGON
        1    POINT
        dtype: object
        """
        result = self._process_geometry_column(
            "GeometryType", rename="geom_type", returns_geom=False
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
            select, col, rename="length", returns_geom=False
        )

    @property
    def is_valid(self) -> pspd.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that are valid.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon
        >>> s = GeoSeries(
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
        result = self._process_geometry_column(
            "ST_IsValid", rename="is_valid", returns_geom=False
        )
        return to_bool(result)

    def is_valid_reason(self) -> pspd.Series:
        """Returns a ``Series`` of strings with the reason for invalidity of
        each geometry.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon
        >>> s = GeoSeries(
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
        GeoSeries.is_valid : detect invalid geometries
        GeoSeries.make_valid : fix invalid geometries
        """
        return self._process_geometry_column(
            "ST_IsValidReason", rename="is_valid_reason", returns_geom=False
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

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> geoseries = GeoSeries([Point(), Point(2, 1), None], crs="EPSG:4326")
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
        GeoSeries.isna : detect missing values
        """
        result = self._process_geometry_column(
            "ST_IsEmpty", rename="is_empty", returns_geom=False
        )
        return to_bool(result)

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
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import LineString
        >>> s = GeoSeries(
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
        result = self._process_geometry_column(
            "ST_IsSimple", rename="is_simple", returns_geom=False
        )
        return to_bool(result)

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
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Point
        >>> s = GeoSeries(
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
            "ST_HasZ", rename="has_z", returns_geom=False
        )

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

    def make_valid(self, *, method="linework", keep_collapsed=True) -> "GeoSeries":
        """Repairs invalid geometries.

        Returns a ``GeoSeries`` with valid geometries.

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

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import MultiPolygon, Polygon, LineString, Point
        >>> s = GeoSeries(
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

        col = self.get_first_geometry_column()
        select = f"ST_MakeValid(`{col}`, {keep_collapsed})"
        return self._query_geometry_column(select, col, rename="make_valid")

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

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if is
            intersected.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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

        We can check if each geometry of GeoSeries crosses a single
        geometry:

        >>> line = LineString([(-1, 1), (3, 1)])
        >>> s.intersects(line)
        0    True
        1    True
        2    True
        3    True
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        of one GeoSeries ``crosses`` *any* element of the other one.

        See also
        --------
        GeoSeries.disjoint
        GeoSeries.crosses
        GeoSeries.touches
        GeoSeries.intersection
        """

        select = "ST_Intersects(`L`, `R`)"

        # ps.Series.fillna() call in to_bool, doesn't work for the output for
        # intersects here for some reason. So we manually handle the nulls here.
        select = f"""
            CASE
                WHEN `L` IS NULL OR `R` IS NULL THEN FALSE
                ELSE {select}
            END
        """

        result = self._row_wise_operation(
            select, other, align, rename="intersects", returns_geom=False
        )
        return to_bool(result)

    def intersection(
        self, other: Union["GeoSeries", BaseGeometry], align: Union[bool, None] = None
    ) -> "GeoSeries":
        """Returns a ``GeoSeries`` of the intersection of points in each
        aligned geometry with `other`.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            intersection with.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         LineString([(2, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        returns_geom: bool = True,
    ):
        """
        Helper function to perform a row-wise operation on two GeoSeries.
        The self column and other column are aliased to `L` and `R`, respectively.
        """
        from pyspark.sql.functions import col

        # Note: this is specifically False. None is valid since it defaults to True similar to geopandas
        index_col = (
            NATURAL_ORDER_COLUMN_NAME if align is False else SPARK_DEFAULT_INDEX_NAME
        )

        if isinstance(other, BaseGeometry):
            other = GeoSeries([other] * len(self))

        assert isinstance(other, GeoSeries), f"Invalid type for other: {type(other)}"

        # This code assumes there is only one index (SPARK_DEFAULT_INDEX_NAME)
        # and would need to be updated if Sedona later supports multi-index
        df = self._internal.spark_frame.select(
            col(self.get_first_geometry_column()).alias("L"),
            # For the left side:
            # - We always select NATURAL_ORDER_COLUMN_NAME, to avoid having to regenerate it in the result
            # - We always select SPARK_DEFAULT_INDEX_NAME, to retain series index info
            col(NATURAL_ORDER_COLUMN_NAME),
            col(SPARK_DEFAULT_INDEX_NAME),
        )
        other_df = other._internal.spark_frame.select(
            col(other.get_first_geometry_column()).alias("R"),
            # for the right side, we only need the column that we are joining on
            col(index_col),
        )
        joined_df = df.join(other_df, on=index_col, how="outer")
        return self._query_geometry_column(
            select,
            cols=["L", "R"],
            rename=rename,
            df=joined_df,
            returns_geom=returns_geom,
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
        col = self.get_first_geometry_column()

        # Convert WKB to Sedona geometry objects
        # Specify returns_geom=False to avoid turning it back into EWKB
        result = self._query_geometry_column(
            f"`{col}`",
            cols=col,
            rename="wkb",
            returns_geom=False,
        )

        # Use the Spark DataFrame's write method to write to GeoParquet format
        result._internal.spark_frame.write.format("geoparquet").save(path, **kwargs)

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

        >>> from sedona.geopandas import GeoSeries
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
        return self._process_geometry_column("ST_X", rename="x", returns_geom=False)

    @property
    def y(self) -> pspd.Series:
        """Return the y location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
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
        return self._process_geometry_column("ST_Y", rename="y", returns_geom=False)

    @property
    def z(self) -> pspd.Series:
        """Return the z location of point geometries in a GeoSeries

        Returns
        -------
        pandas.Series

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
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
        return self._process_geometry_column("ST_Z", rename="z", returns_geom=False)

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
        >>> s = geopandas.GeoSeries.from_wkb(wkbs)
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

        >>> wkts = [
        ... 'POINT (1 1)',
        ... 'POINT (2 2)',
        ... 'POINT (3 3)',
        ... ]
        >>> s = geopandas.GeoSeries.from_wkt(wkts)
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
            f"ST_GeomFromText(`data`)",
            data,
            schema,
            index,
            crs,
            **kwargs,
        )

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
        >>> s = geopandas.GeoSeries.from_xy(x, y, crs="EPSG:4326")
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
    ) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_shapely() is not implemented yet.")

    @classmethod
    def from_arrow(cls, arr, **kwargs) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.from_arrow() is not implemented yet.")

    @classmethod
    def _create_from_select(
        cls, select: str, data, schema, index, crs, **kwargs
    ) -> "GeoSeries":

        from pyspark.pandas.utils import default_session
        from pyspark.pandas.internal import InternalField
        import numpy as np

        if isinstance(data, list) and not isinstance(data[0], (tuple, list)):
            data = [(obj,) for obj in data]

        select = f"{select} as geometry"

        spark_df = default_session().createDataFrame(data, schema=schema)
        spark_df = spark_df.selectExpr(select)

        internal = InternalFrame(
            spark_frame=spark_df,
            index_spark_columns=None,
            column_labels=[("geometry",)],
            data_spark_columns=[scol_for(spark_df, "geometry")],
            data_fields=[
                InternalField(np.dtype("object"), spark_df.schema["geometry"])
            ],
            column_label_names=[("geometry",)],
        )
        return GeoSeries(
            first_series(PandasOnSparkDataFrame(internal)),
            index,
            crs=crs,
            name=kwargs.get("name", None),
        )

    def to_file(
        self,
        filename: Union[os.PathLike, typing.IO],
        driver: Union[str, None] = None,
        index: Union[bool, None] = None,
        **kwargs,
    ):
        raise NotImplementedError("GeoSeries.to_file() is not implemented yet.")

    def isna(self) -> pspd.Series:
        """
        Detect missing values.

        Returns
        -------
        A boolean Series of the same size as the GeoSeries,
        True where a value is NA.

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
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
        col = self.get_first_geometry_column()
        select = f"`{col}` IS NULL"
        result = self._query_geometry_column(
            select, col, rename="isna", returns_geom=False
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
        A boolean pandas Series of the same size as the GeoSeries,
        False where a value is NA.

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
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
        col = self.get_first_geometry_column()
        select = f"`{col}` IS NOT NULL"

        result = self._query_geometry_column(
            select, col, rename="notna", returns_geom=False
        )
        return to_bool(result)

    def notnull(self) -> pspd.Series:
        """Alias for `notna` method. See `notna` for more detail."""
        return self.notna()

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
            used to fill all missing values. If a ``GeoSeries`` or ``GeometryArray``
            are passed, missing values will be filled based on the corresponding index
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

        >>> from sedona.geopandas import GeoSeries
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
        from geopandas.array import GeometryArray

        # TODO: Implement limit https://github.com/apache/sedona/issues/2068
        if limit:
            raise NotImplementedError(
                "GeoSeries.fillna() with limit is not implemented yet."
            )

        col = self.get_first_geometry_column()
        if pd.isna(value) == True or isinstance(value, BaseGeometry):
            if (
                value is not None and pd.isna(value) == True
            ):  # ie. value is np.nan or pd.NA:
                value = "NULL"
            else:
                if value is None:
                    from shapely.geometry import GeometryCollection

                    value = GeometryCollection()

                value = f"ST_GeomFromText('{value.wkt}')"

            select = f"COALESCE(`{col}`, {value})"
            result = self._query_geometry_column(select, col, "")
        elif isinstance(value, (GeoSeries, GeometryArray, gpd.GeoSeries)):

            if isinstance(value, (gpd.GeoSeries, GeometryArray)):
                value = GeoSeries(value)

            # Replace all None's with empty geometries
            value = value.fillna(None)

            # Coalesce: If the value in L is null, use the corresponding value in R for that row
            select = f"COALESCE(`L`, `R`)"
            result = self._row_wise_operation(
                select, value, align=None, rename="fillna"
            )
        else:
            raise ValueError(f"Invalid value type: {type(value)}")

        if inplace:
            self._update_anchor(_to_spark_pandas_df(result))
            return None

        return result

    def explode(self, ignore_index=False, index_parts=False) -> "GeoSeries":
        raise NotImplementedError("GeoSeries.explode() is not implemented yet.")

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
        >>> from sedona.geopandas import GeoSeries
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

        col = self.get_first_geometry_column()
        return self._query_geometry_column(
            f"ST_Transform(`{col}`, 'EPSG:{old_crs.to_epsg()}', 'EPSG:{crs.to_epsg()}')",
            col,
            "",
        )

    @property
    def bounds(self) -> pspd.DataFrame:
        """Returns a ``DataFrame`` with columns ``minx``, ``miny``, ``maxx``,
        ``maxy`` values containing the bounds for each geometry.

        See ``GeoSeries.total_bounds`` for the limits of the entire series.

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
        col = self.get_first_geometry_column()

        selects = [
            f"ST_XMin(`{col}`) as minx",
            f"ST_YMin(`{col}`) as miny",
            f"ST_XMax(`{col}`) as maxx",
            f"ST_YMax(`{col}`) as maxy",
        ]

        df = self._internal.spark_frame

        data_type = df.schema[col].dataType

        if isinstance(data_type, BinaryType):
            selects = [
                select.replace(f"`{col}`", f"ST_GeomFromWKB(`{col}`)")
                for select in selects
            ]

        sdf = df.selectExpr(*selects)
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

        See ``GeoSeries.bounds`` for the bounds of the geometries contained in
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

    def get_first_geometry_column(self) -> str:
        first_binary_or_geometry_col = next(
            (
                field.name
                for field in self._internal.spark_frame.schema.fields
                if isinstance(field.dataType, BinaryType)
                or field.dataType.typeName() == "geometrytype"
            ),
            None,
        )
        if first_binary_or_geometry_col:
            return first_binary_or_geometry_col

        raise ValueError(
            "get_first_geometry_column: No geometry column found in the GeoSeries."
        )


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def _to_spark_pandas_df(ps_series: pspd.Series) -> pspd.DataFrame:
    return pspd.DataFrame(ps_series._psdf._internal)


def to_bool(ps_series: pspd.Series, default: bool = False) -> pspd.Series:
    """
    Cast a ps.Series to bool type if it's not one, converting None values to the default value.
    """
    if ps_series.dtype.name != "bool":
        # fill None values with the default value
        ps_series.fillna(default, inplace=True)

    return ps_series


def _to_geo_series(df: PandasOnSparkSeries) -> GeoSeries:
    """
    Get the first Series from the DataFrame.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - GeoSeries: The first Series from the DataFrame.
    """
    return GeoSeries(data=df)
