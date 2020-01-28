from abc import ABC

import attr
from pyspark import SparkContext

from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib


@attr.s
class SpatialRDDFactory(ABC):

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    @require([GeoSparkLib.PointRDD])
    def create_point_rdd(self):
        return self._jvm.PointRDD

    @require([GeoSparkLib.PolygonRDD])
    def create_polygon_rdd(self):
        return self._jvm.PolygonRDD

    @require([GeoSparkLib.LineStringRDD])
    def create_linestring_rdd(self):
        return self._jvm.LineStringRDD

    @require([GeoSparkLib.RectangleRDD])
    def create_rectangle_rdd(self):
        return self._jvm.RectangleRDD

    @require([GeoSparkLib.CircleRDD])
    def create_circle_rdd(self):
        return self._jvm.CircleRDD

    @require([GeoSparkLib.SpatialRDD])
    def create_spatial_rdd(self):
        return self._jvm.SpatialRDD
