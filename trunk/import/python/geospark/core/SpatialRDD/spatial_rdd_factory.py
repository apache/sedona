from abc import ABC

import attr
from pyspark import SparkContext

from geospark.utils.decorators import require


@attr.s
class SpatialRDDFactory(ABC):

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    @require(["PointRDD"])
    def create_point_rdd(self):
        return self._jvm.PointRDD

    @require(["PolygonRDD"])
    def create_polygon_rdd(self):
        return self._jvm.PolygonRDD

    @require(["LineStringRDD"])
    def create_linestring_rdd(self):
        return self._jvm.LineStringRDD

    @require(["RectangleRDD"])
    def create_rectangle_rdd(self):
        return self._jvm.RectangleRDD

    @require(["CircleRDD"])
    def create_circle_rdd(self):
        return self._jvm.CircleRDD

    @require(["SpatialRDD"])
    def create_spatial_rdd(self):
        return self._jvm.SpatialRDD
