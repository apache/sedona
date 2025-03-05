#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

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

import geopandas as gpd
import pandas as pd
from pyspark.pandas._typing import (
    Axis,
    Dtype,
    Scalar,
)
from pyspark.sql import Column

from sedona.geopandas._typing import GeoFrameLike

bool_type = bool


class GeoFrame(object, metaclass=ABCMeta):
    """
    A base class for both GeoDataFrame and GeoSeries.
    """

    @abstractmethod
    def __getitem__(self, key: Any) -> Any:
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
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

    @property
    @abstractmethod
    def dtypes(self) -> Union[gpd.GeoSeries, pd.Series, Dtype]:
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def to_geopandas(self) -> Union[gpd.GeoDataFrame, pd.Series]:
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def _to_geopandas(self) -> Union[gpd.GeoDataFrame, pd.Series]:
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def geoindex(self) -> "GeoIndex":
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def copy(self: GeoFrameLike) -> GeoFrameLike:
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def area(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def crs(self):
        raise NotImplementedError("This method is not implemented yet.")

    @crs.setter
    @abstractmethod
    def crs(self, value):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def geom_type(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def type(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def length(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def is_valid(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def is_valid_reason(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def is_empty(self):
        raise NotImplementedError("This method is not implemented yet.")

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
    @abstractmethod
    def is_simple(self):
        raise NotImplementedError("This method is not implemented yet.")

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
    @abstractmethod
    def has_z(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def get_precision(self):
        raise NotImplementedError("This method is not implemented yet.")

    @abstractmethod
    def get_geometry(self, index):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def boundary(self):
        raise NotImplementedError("This method is not implemented yet.")

    @property
    @abstractmethod
    def centroid(self):
        raise NotImplementedError("This method is not implemented yet.")

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

    @abstractmethod
    def union_all(self, method="unary", grid_size=None):
        raise NotImplementedError("This method is not implemented yet.")

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
