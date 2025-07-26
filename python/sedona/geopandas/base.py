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
    def envelope(self):
        """Returns a ``GeoSeries`` of geometries representing the envelope of
        each geometry.

        The envelope of a geometry is the bounding rectangle. That is, the
        point or smallest rectangular polygon (with sides parallel to the
        coordinate axes) that contains the geometry.

        Examples
        --------

        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point, MultiPoint
        >>> s = GeoSeries(
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
        GeoSeries.convex_hull : convex hull geometry
        """
        return _delegate_property("envelope", self)

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

    def make_valid(self, *, method="linework", keep_collapsed=True):
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
        return _delegate_property(
            "make_valid", self, method=method, keep_collapsed=keep_collapsed
        )

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

    def intersects(self, other, align=None):
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
        return _delegate_property("intersects", self, other, align)

    def overlaps(self, other, align=None):
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
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if
            overlaps.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, MultiPoint, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         MultiPoint([(0, 0), (0, 1)]),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 0), (0, 2)]),
        ...         LineString([(0, 1), (1, 1)]),
        ...         LineString([(1, 1), (3, 3)]),
        ...         Point(0, 1),
        ...     ],
        ... )

        We can check if each geometry of GeoSeries overlaps a single
        geometry:

        >>> polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        >>> s.overlaps(polygon)
        0     True
        1     True
        2    False
        3    False
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We align both GeoSeries
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
        of one GeoSeries ``overlaps`` *any* element of the other one.

        See also
        --------
        GeoSeries.crosses
        GeoSeries.intersects

        """
        return _delegate_property("overlaps", self, other, align)

    def touches(self, other, align=None):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each aligned geometry that touches `other`.

        An object is said to touch `other` if it has at least one point in
        common with `other` and its interior does not intersect with any part
        of the other. Overlapping features therefore do not touch.

        Note: Sedona's behavior may also differ from Geopandas for GeometryCollections.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if is
            touched.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, MultiPoint, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         MultiPoint([(0, 0), (0, 1)]),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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

        We can check if each geometry of GeoSeries touches a single
        geometry:

        >>> line = LineString([(0, 0), (-1, -2)])
        >>> s.touches(line)
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
        of one GeoSeries ``touches`` *any* element of the other one.

        See also
        --------
        GeoSeries.overlaps
        GeoSeries.intersects

        """
        return _delegate_property("touches", self, other, align)

    def within(self, other, align=None):
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
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if each
            geometry is within.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)


        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (1, 2), (0, 2)]),
        ...         LineString([(0, 0), (0, 2)]),
        ...         Point(0, 1),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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
        3             LINESTRING (0 0, 0 1)]
        4                       POINT (0 1)
        dtype: geometry

        We can check if each geometry of GeoSeries is within a single
        geometry:

        >>> polygon = Polygon([(0, 0), (2, 2), (0, 2)])
        >>> s.within(polygon)
        0     True
        1     True
        2    False
        3    False
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        of one GeoSeries is ``within`` any element of the other one.

        See also
        --------
        GeoSeries.contains
        """
        return _delegate_property("within", self, other, align)

    def covers(self, other, align=None):
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
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ...         Polygon([(0, 0), (2, 2), (0, 2)]),
        ...         LineString([(0, 0), (2, 2)]),
        ...         Point(0, 0),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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

        We can check if each geometry of GeoSeries covers a single
        geometry:

        >>> poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        >>> s.covers(poly)
        0     True
        1    False
        2    False
        3    False
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        of one GeoSeries ``covers`` any element of the other one.

        See also
        --------
        GeoSeries.covered_by
        GeoSeries.overlaps
        """
        return _delegate_property("covers", self, other, align)

    def covered_by(self, other, align=None):
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
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
        ...         Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ...         LineString([(1, 1), (1.5, 1.5)]),
        ...         Point(0, 0),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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

        We can check if each geometry of GeoSeries is covered by a single
        geometry:

        >>> poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        >>> s.covered_by(poly)
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
        of one GeoSeries is ``covered_by`` any element of the other one.

        See also
        --------
        GeoSeries.covers
        GeoSeries.overlaps
        """
        return _delegate_property("covered_by", self, other, align)

    def distance(self, other, align=None):
        """Returns a ``Series`` containing the distance to aligned `other`.

        The operation works on a 1-to-1 row-wise manner:

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            distance to.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (float)

        Examples
        --------
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString, Point
        >>> s = GeoSeries(
        ...     [
        ...         Polygon([(0, 0), (1, 0), (1, 1)]),
        ...         Polygon([(0, 0), (-1, 0), (-1, 1)]),
        ...         LineString([(1, 1), (0, 0)]),
        ...         Point(0, 0),
        ...     ],
        ... )
        >>> s2 = GeoSeries(
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

        We can check the distance of each geometry of GeoSeries to a single
        geometry:

        >>> point = Point(-1, 0)
        >>> s.distance(point)
        0    1.0
        1    0.0
        2    1.0
        3    1.0
        dtype: float64

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        return _delegate_property("distance", self, other, align)

    def intersection(self, other, align=None):
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
        return _delegate_property("intersection", self, other, align)

    def snap(self, other, tolerance, align=None):
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
        other : GeoSeries or geometric object
            The Geoseries (elementwise) or geometric object to snap to.
        tolerance : float or array like
            Maximum distance between vertices that shall be snapped
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from sedona.geopandas import GeoSeries
        >>> from shapely import Polygon, LineString, Point
        >>> s = GeoSeries(
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

        >>> s2 = GeoSeries(
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

        We can also snap two GeoSeries to each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        return _delegate_property("snap", self, other, tolerance, align)

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
