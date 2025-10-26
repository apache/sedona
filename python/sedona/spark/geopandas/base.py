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

from sedona.spark.geopandas._typing import GeoFrameLike

bool_type = bool


class GeoFrame(metaclass=ABCMeta):
    """
    A base class for both GeoDataFrame and GeoSeries.
    """

    @property
    def sindex(self) -> "SpatialIndex":
        """
        Returns a spatial index for the GeoSeries.

        Note that the spatial index may not be fully
        initialized until the first use.

        Currently, sindex is not retained when calling this method from a GeoDataFrame.
        You can workaround this by first extracting the active geometry column as a GeoSeries,
        and calling this method.

        Returns
        -------
        SpatialIndex
            The spatial index.

        Examples
        --------
        >>> from shapely.geometry import Point, box
        >>> from sedona.spark.geopandas import GeoSeries
        >>>
        >>> s = GeoSeries([Point(x, x) for x in range(5)])
        >>> s.sindex.query(box(1, 1, 3, 3))
        [Point(1, 1), Point(2, 2), Point(3, 3)]
        >>> s.has_sindex
        True
        """
        return _delegate_to_geometry_column("sindex", self)

    @property
    def has_sindex(self):
        """Check the existence of the spatial index without generating it.

        Use the `.sindex` attribute on a GeoDataFrame or GeoSeries
        to generate a spatial index if it does not yet exist,
        which may take considerable time based on the underlying index
        implementation.

        Note that the underlying spatial index may not be fully
        initialized until the first use.

        Currently, sindex is not retained when calling this method from a GeoDataFrame.
        You can workaround this by first extracting the active geometry column as a GeoSeries,
        and calling this method.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> s = GeoSeries([Point(x, x) for x in range(5)])
        >>> s.has_sindex
        False
        >>> index = s.sindex
        >>> s.has_sindex
        True

        Returns
        -------
        bool
            `True` if the spatial index has been generated or
            `False` if not.
        """
        return _delegate_to_geometry_column("has_sindex", self)

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
        >>> from sedona.spark.geopandas import GeoSeries

        >>> gs = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])])
        >>> gs.area
        0    1.0
        1    4.0
        dtype: float64
        """
        return _delegate_to_geometry_column("area", self)

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
        >>> from sedona.spark.geopandas import GeoSeries

        >>> gs = GeoSeries([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Point(0, 0)])
        >>> gs.geom_type
        0    POLYGON
        1    POINT
        dtype: object
        """
        return _delegate_to_geometry_column("geom_type", self)

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
        >>> from sedona.spark.geopandas import GeoSeries

        >>> gs = GeoSeries([Point(0, 0), LineString([(0, 0), (1, 1)]), Polygon([(0, 0), (1, 0), (1, 1)]), GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)]), Polygon([(0, 0), (1, 0), (1, 1)])])])
        >>> gs.length
        0    0.000000
        1    1.414214
        2    3.414214
        3    4.828427
        dtype: float64
        """
        return _delegate_to_geometry_column("length", self)

    @property
    def is_valid(self):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that are valid.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("is_valid", self)

    def is_valid_reason(self):
        """Returns a ``Series`` of strings with the reason for invalidity of
        each geometry.

        Examples
        --------

        An example with one invalid polygon (a bowtie geometry crossing itself)
        and one missing geometry:

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("is_valid_reason", self)

    @property
    def is_empty(self):
        """
        Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        empty geometries.

        Examples
        --------
        An example of a GeoDataFrame with one empty point, one point and one missing
        value:

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("is_empty", self)

    # def count_coordinates(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def count_geometries(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def count_interior_rings(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_simple(self):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        geometries that do not cross themselves.

        This is meaningful only for `LineStrings` and `LinearRings`.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("is_simple", self)

    @property
    def is_ring(self):
        """Return a ``Series`` of ``dtype('bool')`` with value ``True`` for
        features that are closed.

        When constructing a LinearRing, the sequence of coordinates may be
        explicitly closed by passing identical values in the first and last indices.
        Otherwise, the sequence will be implicitly closed by copying the first tuple
        to the last index.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import LineString, LinearRing
        >>> s = GeoSeries(
        ...     [
        ...         LineString([(0, 0), (1, 1), (1, -1)]),
        ...         LineString([(0, 0), (1, 1), (1, -1), (0, 0)]),
        ...         LinearRing([(0, 0), (1, 1), (1, -1)]),
        ...     ]
        ... )
        >>> s
        0         LINESTRING (0 0, 1 1, 1 -1)
        1    LINESTRING (0 0, 1 1, 1 -1, 0 0)
        2    LINEARRING (0 0, 1 1, 1 -1, 0 0)
        dtype: geometry

        >>> s.is_ring
        0    False
        1     True
        2     True
        dtype: bool

        """
        return _delegate_to_geometry_column("is_ring", self)

    # @property
    # def is_ccw(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_closed(self):
        """Return a ``Series`` of ``dtype('bool')`` with value ``True`` if a
        LineString's or LinearRing's first and last points are equal.

        Returns False for any other geometry type.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import LineString, Point, Polygon
        >>> s = GeoSeries(
        ...     [
        ...         LineString([(0, 0), (1, 1), (0, 1), (0, 0)]),
        ...         LineString([(0, 0), (1, 1), (0, 1)]),
        ...         Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                    Point(3, 3)
        ...     ]
        ... )
        >>> s
        0    LINESTRING (0 0, 1 1, 0 1, 0 0)
        1         LINESTRING (0 0, 1 1, 0 1)
        2     POLYGON ((0 0, 0 1, 1 1, 0 0))
        3                        POINT (3 3)
        dtype: geometry

        >>> s.is_closed
        0     True
        1    False
        2    False
        3    False
        dtype: bool
        """
        return _delegate_to_geometry_column("is_closed", self)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("has_z", self)

    # def get_precision(self):
    #     raise NotImplementedError("This method is not implemented yet.")

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
        return _delegate_to_geometry_column("get_geometry", self, index)

    @property
    def boundary(self):
        """Returns a ``GeoSeries`` of lower dimensional objects representing
        each geometry's set-theoretic `boundary`.

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("boundary", self)

    @property
    def centroid(self):
        """Returns a ``GeoSeries`` of points representing the centroid of each
        geometry.

        Note that centroid does not have to be on or within original geometry.

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("centroid", self)

    # def concave_hull(self, ratio=0.0, allow_holes=False):
    #     raise NotImplementedError("This method is not implemented yet.")

    # @property
    # def convex_hull(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def delaunay_triangles(self, tolerance=0.0, only_edges=False):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def voronoi_polygons(self, tolerance=0.0, extend_to=None, only_edges=False):
    #     raise NotImplementedError("This method is not implemented yet.")

    @property
    def envelope(self):
        """Returns a ``GeoSeries`` of geometries representing the envelope of
        each geometry.

        The envelope of a geometry is the bounding rectangle. That is, the
        point or smallest rectangular polygon (with sides parallel to the
        coordinate axes) that contains the geometry.

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("envelope", self)

    # def minimum_rotated_rectangle(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # @property
    # def exterior(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def extract_unique_points(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def offset_curve(self, distance, quad_segs=8, join_style="round", mitre_limit=5.0):
    #     raise NotImplementedError("This method is not implemented yet.")

    # @property
    # def interiors(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def remove_repeated_points(self, tolerance=0.0):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def set_precision(self, grid_size, mode="valid_output"):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def representative_point(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def minimum_bounding_circle(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def minimum_bounding_radius(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def minimum_clearance(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def normalize(self):
    #     raise NotImplementedError("This method is not implemented yet.")

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

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column(
            "make_valid", self, method=method, keep_collapsed=keep_collapsed
        )

    # def reverse(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    def segmentize(self, max_segment_length):
        """Returns a ``GeoSeries`` with vertices added to line segments based on
        maximum segment length.

        Additional vertices will be added to every line segment in an input geometry so
        that segments are no longer than the provided maximum segment length. New
        vertices will evenly subdivide each segment. Only linear components of input
        geometries are densified; other geometries are returned unmodified.

        Parameters
        ----------
        max_segment_length : float | array-like
            Additional vertices will be added so that all line segments are no longer
            than this value. Must be greater than 0.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Polygon, LineString
        >>> s = GeoSeries(
        ...     [
        ...         LineString([(0, 0), (0, 10)]),
        ...         Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]),
        ...     ],
        ... )
        >>> s
        0                     LINESTRING (0 0, 0 10)
        1    POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))
        dtype: geometry

        >>> s.segmentize(max_segment_length=5)
        0                          LINESTRING (0 0, 0 5, 0 10)
        1    POLYGON ((0 0, 5 0, 10 0, 10 5, 10 10, 5 10, 0...
        dtype: geometry
        """
        return _delegate_to_geometry_column("segmentize", self, max_segment_length)

    # def transform(self, transformation, include_z=False):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def force_2d(self):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def force_3d(self, z=0):
    #     raise NotImplementedError("This method is not implemented yet.")

    # def line_merge(self, directed=False):
    #     raise NotImplementedError("This method is not implemented yet.")

    # @property
    # def unary_union(self):
    #     raise NotImplementedError("This method is not implemented yet.")

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

        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import box
        >>> s = GeoSeries([box(0, 0, 1, 1), box(0, 0, 2, 2)])
        >>> s
        0    POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))
        1    POLYGON ((2 0, 2 2, 0 2, 0 0, 2 0))
        dtype: geometry

        >>> s.union_all()
        <POLYGON ((0 1, 0 2, 2 2, 2 0, 1 0, 0 0, 0 1))>
        """
        return _delegate_to_geometry_column("union_all", self, method, grid_size)

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

        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("crosses", self, other, align)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("intersects", self, other, align)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("overlaps", self, other, align)

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
        return _delegate_to_geometry_column("touches", self, other, align)

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
        return _delegate_to_geometry_column("within", self, other, align)

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
        return _delegate_to_geometry_column("covers", self, other, align)

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
        return _delegate_to_geometry_column("covered_by", self, other, align)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("distance", self, other, align)

    def intersection(self, other, align=None):
        """Returns a ``GeoSeries`` of the intersection of points in each
        aligned geometry with `other`.

        The operation works on a 1-to-1 row-wise manner.

        Note: Unlike most functions, intersection may return the unordered with respect to the index.
        If this is important to you, you may call ``sort_index()`` on the result.

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("intersection", self, other, align)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("snap", self, other, tolerance, align)

    @property
    def bounds(self) -> ps.DataFrame:
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
        return _delegate_to_geometry_column("bounds", self)

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
        return _delegate_to_geometry_column("total_bounds", self)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("dwithin", self, other, distance, align)

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
        >>> from sedona.spark.geopandas import GeoSeries
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
        return _delegate_to_geometry_column("difference", self, other, align)

    def symmetric_difference(self, other, align=None):
        """Return a ``GeoSeries`` of the symmetric difference of points in
        each aligned geometry with `other`.

        For each geometry, the symmetric difference consists of points in the
        geometry not in `other`, and points in `other` not in the geometry.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            symmetric difference to.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices.
            If False, the order of elements is preserved. None defaults to True.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
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

        We can do symmetric difference of each geometry and a single
        shapely geometry:

        >>> s.symmetric_difference(Polygon([(0, 0), (1, 1), (0, 1)]))
        0                  POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        1                  POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        2    GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 1, 0...
        3    GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 1, 0...
        4                       POLYGON ((0 1, 1 1, 0 0, 0 1))
        dtype: geometry

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.symmetric_difference(s2, align=True)
        0                                                 None
        1                  POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        2    MULTILINESTRING ((0 0, 1 1), (1 1, 2 2), (1 0,...
        3                                     LINESTRING EMPTY
        4                            MULTIPOINT ((0 1), (1 1))
        5                                                 None
        dtype: geometry

        >>> s.symmetric_difference(s2, align=False)
        0                  POLYGON ((0 2, 2 2, 1 1, 0 1, 0 2))
        1    GEOMETRYCOLLECTION (POLYGON ((0 0, 0 2, 1 2, 2...
        2    MULTILINESTRING ((0 0, 1 1), (1 1, 2 2), (2 0,...
        3                                LINESTRING (2 0, 0 2)
        4                                          POINT EMPTY
        dtype: geometry

        See also
        --------
        GeoSeries.difference
        GeoSeries.union
        GeoSeries.intersection
        """
        return _delegate_to_geometry_column("symmetric_difference", self, other, align)

    def union(self, other, align=None):
        """Return a ``GeoSeries`` of the union of points in each aligned geometry
        with `other`.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : Geoseries or geometric object
            The Geoseries (elementwise) or geometric object to find the
            union with.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices.
            If False, the order of elements is preserved. None defaults to True.

        Returns
        -------
        GeoSeries

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
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

        We can do union of each geometry and a single shapely geometry:

        >>> s.union(Polygon([(0, 0), (1, 1), (0, 1)]))
        0             POLYGON ((0 0, 0 1, 0 2, 2 2, 1 1, 0 0))
        1             POLYGON ((0 0, 0 1, 0 2, 2 2, 1 1, 0 0))
        2    GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 1, 0...
        3    GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 1, 0...
        4                       POLYGON ((0 1, 1 1, 0 0, 0 1))
        dtype: geometry

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
        based on index values and compare elements with the same index using
        ``align=True`` or ignore index and compare elements based on their matching
        order using ``align=False``:

        >>> s.union(s2, align=True)
        0             POLYGON ((0 0, 0 1, 0 2, 2 2, 1 1, 0 0))
        1             POLYGON ((0 0, 0 1, 0 2, 2 2, 1 1, 0 0))
        2    MULTILINESTRING ((0 0, 1 1), (1 1, 2 2), (1 0,...
        3                                LINESTRING (2 0, 0 2)
        4                            MULTIPOINT ((0 1), (1 1))
        dtype: geometry

        >>> s.union(s2, align=False)
        0             POLYGON ((0 0, 0 1, 0 2, 2 2, 1 1, 0 0))
        1    GEOMETRYCOLLECTION (POLYGON ((0 0, 0 2, 1 2, 2...
        2    MULTILINESTRING ((0 0, 1 1), (1 1, 2 2), (2 0,...
        3                                LINESTRING (2 0, 0 2)
        4                                          POINT (0 1)
        dtype: geometry

        See Also
        --------
        GeoSeries.symmetric_difference
        GeoSeries.difference
        GeoSeries.intersection
        """
        return _delegate_to_geometry_column("union", self, other, align)

    def intersection_all(self):
        raise NotImplementedError("This method is not implemented yet.")

    def contains(self, other, align=None):
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
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to test if it
            is contained.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (bool)

        Examples
        --------

        >>> from sedona.spark.geopandas import GeoSeries
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

        We can check if each geometry of GeoSeries contains a single
        geometry:

        >>> point = Point(0, 1)
        >>> s.contains(point)
        0    False
        1     True
        2    False
        3     True
        dtype: bool

        We can also check two GeoSeries against each other, row by row.
        The GeoSeries above have different indices. We can either align both GeoSeries
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
        of one GeoSeries ``contains`` any element of the other one.

        See also
        --------
        GeoSeries.contains_properly
        GeoSeries.within
        """
        return _delegate_to_geometry_column("contains", self, other, align)

    def contains_properly(self, other, align=None):
        raise NotImplementedError("This method is not implemented yet.")

    def relate(self, other, align=None):
        """Returns the DE-9IM matrix string for the relationship between each geometry and `other`.

        The DE-9IM (Dimensionally Extended nine-Intersection Model) is a topological model
        that describes the spatial relationship between two geometries. The result is a
        9-character string describing the dimensions of the intersections between the
        interior, boundary, and exterior of the two geometries.

        The operation works on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : GeoSeries or geometric object
            The GeoSeries (elementwise) or geometric object to relate to.
        align : bool | None (default None)
            If True, automatically aligns GeoSeries based on their indices. None defaults to True.
            If False, the order of elements is preserved.

        Returns
        -------
        Series (str)
            A Series of DE-9IM matrix strings.

        Examples
        --------
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point, LineString, Polygon
        >>> s = GeoSeries(
        ...     [
        ...         Point(0, 0),
        ...         Point(0, 0),
        ...         LineString([(0, 0), (1, 1)]),
        ...     ]
        ... )
        >>> s2 = GeoSeries(
        ...     [
        ...         Point(0, 0),
        ...         Point(1, 1),
        ...         LineString([(0, 0), (1, 1)]),
        ...     ]
        ... )

        >>> s.relate(s2)
        0    0FFFFFFF2
        1    FF0FFF0F2
        2    1FFF0FFF2
        dtype: object

        Notes
        -----
        This method works in a row-wise manner. It does not check the relationship
        of an element of one GeoSeries with *all* elements of the other one.

        The DE-9IM string has 9 characters, one for each combination of:
        - Interior/Boundary/Exterior of the first geometry
        - Interior/Boundary/Exterior of the second geometry

        Each character can be:
        - '0': intersection is a point (dimension 0)
        - '1': intersection is a line (dimension 1)
        - '2': intersection is an area (dimension 2)
        - 'F': no intersection (empty set)

        See also
        --------
        GeoSeries.contains
        GeoSeries.intersects
        GeoSeries.within
        """
        return _delegate_to_geometry_column("relate", self, other, align)

    def to_parquet(self, path, **kwargs):
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
    ):
        """
        Returns a GeoSeries with all geometries buffered by the specified distance.

        Parameters
        ----------
        distance : float
            The distance to buffer by. Negative distances will create inward buffers.
        resolution : int, default 16
            The resolution of the buffer around each vertex. Specifies the number of
            linear segments in a quarter circle in the approximation of circular arcs.
        cap_style : str, default "round"
            The style of the buffer cap. One of 'round', 'flat', 'square'.
        join_style : str, default "round"
            The style of the buffer join. One of 'round', 'mitre', 'bevel'.
        mitre_limit : float, default 5.0
            The mitre limit ratio for joins when join_style='mitre'.
        single_sided : bool, default False
            Whether to create a single-sided buffer. In Sedona, True will default to left-sided buffer.
            However, 'right' may be specified to use a right-sided buffer.

        Returns
        -------
        GeoSeries
            A new GeoSeries with buffered geometries.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.spark.geopandas import GeoDataFrame
        >>>
        >>> data = {
        ...     'geometry': [Point(0, 0), Point(1, 1)],
        ...     'value': [1, 2]
        ... }
        >>> gdf = GeoDataFrame(data)
        >>> buffered = gdf.buffer(0.5)
        """
        return _delegate_to_geometry_column(
            "buffer",
            self,
            distance,
            resolution,
            cap_style,
            join_style,
            mitre_limit,
            single_sided,
            **kwargs,
        )

    def simplify(self, tolerance=None, preserve_topology=True):
        """Returns a ``GeoSeries`` containing a simplified representation of
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
        the ``GeoSeries`` as a coverage and simplify its edges, while preserving the
        coverage topology, see :meth:`simplify_coverage`.

        Parameters
        ----------
        tolerance : float
            All parts of a simplified geometry will be no more than
            `tolerance` distance from the original. It has the same units
            as the coordinate reference system of the GeoSeries.
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
        >>> from sedona.spark.geopandas import GeoSeries
        >>> from shapely.geometry import Point, LineString
        >>> s = GeoSeries(
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
        return _delegate_to_geometry_column(
            "simplify", self, tolerance, preserve_topology
        )

    @abstractmethod
    def to_geopandas(self) -> Union[gpd.GeoSeries, gpd.GeoDataFrame]: ...

    @abstractmethod
    def plot(self, *args, **kwargs): ...


def _delegate_to_geometry_column(op, this, *args, **kwargs):
    geom_column = this.geometry
    inplace = kwargs.pop("inplace", False)
    if args or kwargs:
        data = getattr(geom_column, op)(*args, **kwargs)
    else:
        data = getattr(geom_column, op)
        # If it was a function instead of a property, call it
        if callable(data):
            data = data()

    if inplace:
        # This assumes this is a GeoSeries
        this._update_inplace(geom_column)
        return None

    return data
