from pyspark import RDD

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.spatialOperator.join_params import JoinParams
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.rdd_pickling import GeoSparkPickler


class JoinQuery:

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def SpatialJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> RDD:
        """

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        srdd = jvm.JoinQuery.SpatialJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        serlialized = jvm.GeoSerializerData.serializeToPythonHashSet(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def DistanceJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> RDD:
        """

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc
        srdd = jvm.JoinQuery.DistanceJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        serlialized = jvm.GeoSerializerData.serializeToPythonHashSet(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def spatialJoin(cls, queryWindowRDD: SpatialRDD, objectRDD: SpatialRDD, joinParams: JoinParams) -> RDD:
        """

        :param queryWindowRDD: SpatialRDD
        :param objectRDD: SpatialRDD
        :param joinParams: JoinParams
        :return:
        """

        jvm = queryWindowRDD._jvm
        sc = queryWindowRDD._sc

        jvm_join_params = joinParams.jvm_instance(jvm)

        srdd = jvm.JoinQuery.spatialJoin(queryWindowRDD._srdd, objectRDD._srdd, jvm_join_params)

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def DistanceJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> RDD:
        """

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool

        >> spatial_rdd =
        >> query_rdd =
        >> spatial_join_result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, query_rdd, True, True)
        >> spatial_join_result.collect()
        [GeoData(), GeoData()]
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.DistanceJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def SpatialJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool,
                              considerBoundaryIntersection: bool) -> RDD:
        """
        Function takes SpatialRDD and other SpatialRDD and based on two parameters
        - useIndex
        - considerBoundaryIntersection
        creates RDD with result of Spatial Join operation. It Returns RDD[GeoData, GeoData]

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool
        :return: RDD

        >> spatial_join_result = JoinQuery.SpatialJoinQueryFlat(
        >>      spatialRDD, queryRDD, useIndex, considerBoundaryIntersection
        >> )
        >> spatial_join_result.collect()
        [[GeoData(Polygon, ), GeoData()], [GeoData(), GeoData()], [GeoData(), GeoData()]]
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.SpatialJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())
