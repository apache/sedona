from pyspark import RDD
from shapely.geometry.base import BaseGeometry

from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geospark.utils.decorators import require
from geospark.utils.geometry_adapter import GeometryAdapter
from geospark.utils.spatial_rdd_parser import GeoSparkPickler


class RangeQuery:

    @classmethod
    @require(["RangeQuery", "GeometryAdapter", "GeoSerializerData"])
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
