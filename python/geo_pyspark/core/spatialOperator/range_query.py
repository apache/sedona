from pyspark import RDD
from shapely.geometry.base import BaseGeometry

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.geometry_adapter import GeometryAdapter
from geo_pyspark.utils.rdd_pickling import GeoSparkPickler


class RangeQuery:

    @classmethod
    @require([GeoSparkLib.RangeQuery, GeoSparkLib.GeometryAdapter])
    def SpatialRangeQuery(self, spatialRDD: SpatialRDD, rangeQueryWindow: BaseGeometry, considerBoundaryIntersection: bool, usingIndex: bool):
        """

        :param spatialRDD:
        :param rangeQueryWindow:
        :param considerBoundaryIntersection:
        :param usingIndex:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(jvm, rangeQueryWindow)

        srdd = jvm.\
            RangeQuery.SpatialRangeQuery(
            spatialRDD._srdd,
            jvm_geom,
            considerBoundaryIntersection,
            usingIndex
        )

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())
