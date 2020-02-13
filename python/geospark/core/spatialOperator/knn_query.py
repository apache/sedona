import attr
from shapely.geometry.base import BaseGeometry

from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geospark.utils.binary_parser import BinaryParser
from geospark.utils.decorators import require
from geospark.utils.geometry_adapter import GeometryAdapter
from geospark.utils.spatial_rdd_parser import SpatialRDDParserData


@attr.s
class KNNQuery:

    @classmethod
    @require(["KNNQuery", "GeometryAdapter"])
    def SpatialKnnQuery(self, spatialRDD: SpatialRDD, originalQueryPoint: BaseGeometry, k: int,  useIndex: bool):
        """

        :param spatialRDD: spatialRDD
        :param originalQueryPoint: shapely.geometry.Point
        :param k: int
        :param useIndex: bool
        :return: pyspark.RDD
        """
        jvm = spatialRDD._jvm
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(jvm, originalQueryPoint)

        knn_neighbours = jvm.KNNQuery.SpatialKnnQuery(spatialRDD._srdd, jvm_geom, k, useIndex)

        srdd = jvm.GeoSerializerData.serializeToPython(knn_neighbours)

        geoms_data = []
        for arr in srdd:
            binary_parser = BinaryParser(arr)
            geom = SpatialRDDParserData.deserialize(binary_parser)
            geoms_data.append(geom)

        return geoms_data
