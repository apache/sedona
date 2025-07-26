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

"""
A base class of Sedona/Spark DataFrame/Column to behave like geopandas GeoDataFrame/GeoSeries.
"""
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    Callable,
    Optional,
    Union,
)
from shapely.geometry.base import BaseGeometry

import geopandas as gpd
import pandas as pd
import pyspark.pandas as ps
from pyspark.pandas._typing import (
    Axis,
    Dtype,
    Scalar,
)
from pyspark.sql import Column

from sedona.geopandas._typing import GeoFrameLike
from sedona.geopandas.geometryarray import GeometryArray

bool_type = bool


class GeoFrame(metaclass=ABCMeta):
    """
    A base class for both GeoDataFrame and GeoSeries.
    """

    @abstractmethod
    def __getitem__(self, key: Any) -> Any:
        raise NotImplementedError("This method is not implemented yet.")

    def _reduce_for_geostat_function(
        self,
        sfun: Callable[["GeoSeries"], Column],
        name: str,
        axis: Optional[Axis] = None,
        numeric_only: bool = True,
        skipna: bool = True,
        **kwargs: Any,
    ) -> Union["GeoSeries", Scalar]:
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def to_geopandas(self) -> Union[gpd.GeoDataFrame, pd.Series]:
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def _to_geopandas(self) -> Union[gpd.GeoDataFrame, pd.Series]:
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def sindex(self) -> "SpatialIndex":
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def copy(self: GeoFrameLike) -> GeoFrameLike:
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def area(self) -> ps.Series:
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
        return _delegate_property("area", self)

    # We need to implement these crs functions in the subclasses to avoid infinite recursion
    @property
    @abstractmethod
    def crs(self):
        raise NotImplementedError("This method is not implemented yet.")

    @crs.setter
    @abstractmethod
    def crs(self, value):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def geom_type(self):
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
        return _delegate_property("geom_type", self)

    @property
    @abstractmethod
    def type(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def length(self):
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
        return _delegate_property("length", self)

    @property
    def is_valid(self):
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
        return _delegate_property("is_valid", self)

    def is_valid_reason(self):
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
        return _delegate_property("is_valid_reason", self)

    @property
    def is_empty(self):
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
        GeoSeries.isna : detect missing geometries
        """
        return _delegate_property("is_empty", self)

    @abstractmethod
    def count_coordinates(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def count_geometries(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def count_interior_rings(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_simple(self):
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
        return _delegate_property("is_simple", self)

    @property
    @abstractmethod
    def is_ring(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def is_ccw(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def is_closed(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def has_z(self):
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
        return _delegate_property("has_z", self)

    @abstractmethod
    def get_precision(self):
        raise NotImplementedError("This method is not implemented yet.")

    def get_geometry(self, index):
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
        GeoSeries

        Notes
        -----
        Simple geometries act as collections of length 1. Any out-of-range index value
        returns None.

        Examples
        --------
        >>> from shapely.geometry import Point, MultiPoint, GeometryCollection
        >>> s = geopandas.GeoSeries(
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
        return _delegate_property("get_geometry", self, index)

    @property
    def boundary(self):
        """Returns a ``GeoSeries`` of lower dimensional objects representing
        each geometry's set-theoretic `boundary`.

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
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
        GeoSeries.exterior : outer boundary (without interior rings)

        """
        return _delegate_property("boundary", self)

    @property
    def centroid(self):
        """Returns a ``GeoSeries`` of points representing the centroid of each
        geometry.

        Note that centroid does not have to be on or within original geometry.

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
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

        >>> s.centroid
        0    POINT (0.33333 0.66667)
        1        POINT (0.70711 0.5)
        2                POINT (0 0)
        dtype: geometry

        See also
        --------
        GeoSeries.representative_point : point guaranteed to be within each geometry
        """
        return _delegate_property("centroid", self)

    @abstractmethod
    def concave_hull(self, ratio=0.0, allow_holes=False):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def convex_hull(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def delaunay_triangles(self, tolerance=0.0, only_edges=False):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def voronoi_polygons(self, tolerance=0.0, extend_to=None, only_edges=False):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def envelope(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def minimum_rotated_rectangle(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def exterior(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def extract_unique_points(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def offset_curve(self, distance, quad_segs=8, join_style="round", mitre_limit=5.0):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def interiors(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def remove_repeated_points(self, tolerance=0.0):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def set_precision(self, grid_size, mode="valid_output"):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def representative_point(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def minimum_bounding_circle(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def minimum_bounding_radius(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def minimum_clearance(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def normalize(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def make_valid(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def reverse(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def segmentize(self, max_segment_length):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def transform(self, transformation, include_z=False):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def force_2d(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def force_3d(self, z=0):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def line_merge(self, directed=False):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def unary_union(self):
        raise NotImplementedError("This method is not implemented yet.")

    def union_all(self, method="unary", grid_size=None) -> BaseGeometry:
        """Returns a geometry containing the union of all geometries in the
        ``GeoSeries``.

        Sedona does not support the method or grid_size argument, so the user does not need to manually
        decide the algorithm being used.

        Parameters
        ----------
        method : str (default ``"unary"``)
            Not supported in Sedona.

        grid_size : float, default None
            Not supported in Sedona.

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import box
        >>> s = GeoSeries([box(0, 0, 1, 1), box(0, 0, 2, 2)])
        >>> s
        0    POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))
        1    POLYGON ((2 0, 2 2, 0 2, 0 0, 2 0))
        dtype: geometry

        >>> s.union_all()
        <POLYGON ((0 1, 0 2, 2 2, 2 0, 1 0, 0 0, 0 1))>
        """
        return _delegate_property("union_all", self, method, grid_size)

    def crosses(self, other, align=None) -> ps.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that cross `other`.

        An object is said to cross `other` if its `interior` intersects the
        `interior` of the other but does not contain it, and the dimension of
        the intersection is less than the dimension of the one or the other.

        Note: Unlike Geopandas, Sedona's implementation always return NULL when GeometryCollection is involved.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if is
            crossed.
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
        >>> s.crosses(line)
        0     True
        1     True
        2     True
        3    False
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.crosses(s2, align=True)
        0    False
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        >>> s.crosses(s2, align=False)
        0     True
        1     True
        2    False
        3    False
        dtype: bool

        Notice that a line does not cross a point that it contains.

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeoSeries ``crosses`` *any* element of the other one.

        See also
        --------
        GeoSeries.disjoint
        GeoSeries.intersects

        """
        return _delegate_property("crosses", self, other, align)

    def dwithin(self, other, distance, align=None):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that is within a set distance from ``other``.

        The operation works on a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test for
            equality.
        distance : float, np.array, pd.Series
            Distance(s) to test if each geometry is within. A scalar distance will be
            applied to all geometries. An array or Series will be applied elementwise.
            If np.array or pd.Series are used then it must have same length as the
            GeoSeries.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices.
            If False, the order of elements is preserved. None defaults to True.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (1, 1), (0, 1)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         LineString([(0, 0), (0, 1)]),
        ...         Point(0, 1),
        ...     ],
        ...     index=range(0, 4),
        ... )
        >>> s2 = GeoSeries(
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

        We can check if each geometry of GeoSeries contains a single
        geometry:

        >>> point = Point(0, 1)
        >>> s2.dwithin(point, 1.8)
        1     True
        2    False
        3    False
        4     True
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        of one GeoSeries is within the set distance of *any* element of the other one.

        See also
        --------
        GeoSeries.within
        """
        return _delegate_property("dwithin", self, other, distance, align)

    def difference(self, other, align=None):
        """Returns a ``GeoSeries`` of the points in each aligned geometry that
        are not in `other`.

        The operation works on a 1-to-1 row-wise manner:

        Unlike Geopandas, Sedona does not support this operation for GeometryCollections.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            difference to.
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

        We can check if each geometry of GeoSeries contains a single
        geometry:

        >>> point = Point(0, 1)
        >>> s2.difference(point)
        1    POLYGON ((0 0, 1 1, 0 1, 0 0))
        2             LINESTRING (1 0, 1 3)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (1 1)
        5           GEOMETRYCOLLECTION EMPTY
        dtype: geometry

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.difference(s2, align=True)
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1    POLYGON ((0 0, 2 2, 0 2, 0 0))
        2             LINESTRING (0 0, 2 2)
        3             LINESTRING (2 0, 0 2)
        4                       POINT (0 1)
        5                       POINT (0 1)
        dtype: geometry

        >>> s.difference(s2, align=False)
        0    POLYGON ((0 0, 2 2, 0 2, 0 0))
        1    POLYGON ((0 0, 2 2, 0 2, 0 0))
        2           GEOMETRYCOLLECTION EMPTY
        3             LINESTRING (2 0, 0 2)
        4           GEOMETRYCOLLECTION EMPTY
        dtype: geometry

        Notes
        -----
        This method works in a row-wise manner. It does not check if an element
        of one GeoSeries is different from *any* element of the other one.

        See also
        --------
        GeoSeries.intersection
        """
        return _delegate_property("difference", self, other, align)

    @abstractmethod
    def intersection_all(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def contains(self, other, align=None):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def contains_properly(self, other, align=None):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def to_parquet(self, path, **kwargs):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def buffer(
        self,
        distance,
        resolution=16,
        cap_style="round",
        join_style="round",
        mitre_limit=5.0,
        single_sided=False,
        **kwargs,
    ):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def sjoin(self, other, predicate="intersects", **kwargs):
        raise NotImplementedError("This method is not implemented yet.")


def _delegate_property(op, this, *args, **kwargs):
    a_this = GeometryArray(this.geometry)
    if args or kwargs:
        data = getattr(a_this, op)(*args, **kwargs)
    else:
        data = getattr(a_this, op)
        # If it was a function instead of a property, call it
        if callable(data):
            data = data()

    if isinstance(data, GeometryArray):
        from .geoseries import GeoSeries

        return GeoSeries(data)
    else:
        return data
