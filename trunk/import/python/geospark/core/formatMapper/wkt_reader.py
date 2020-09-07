import os

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geospark.core.formatMapper.geo_reader import GeoDataReader
from geospark.core.jvm.config import since
from geospark.utils.decorators import require
from geospark.utils.meta import MultipleMeta


class WktReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str, wktColumn: int, allowInvalidGeometries: bool, skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param sc: SparkContext
        :param inputPath: str
        :param wktColumn: int
        :param allowInvalidGeometries: bool
        :param skipSyntacticallyInvalidGeometries: bool
        :return:
        """
        WktReader.validate_imports()
        jvm = sc._jvm
        srdd = jvm.WktReader.readToGeometryRDD(sc._jsc, inputPath, wktColumn, allowInvalidGeometries,
                                        skipSyntacticallyInvalidGeometries)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, rawTextRDD: RDD, wktColumn: int, allowInvalidGeometries: bool, skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param rawTextRDD: RDD
        :param wktColumn: int
        :param allowInvalidGeometries: bool
        :param skipSyntacticallyInvalidGeometries: bool
        :return:
        """
        WktReader.validate_imports()
        sc = rawTextRDD.ctx
        jvm = sc._jvm
        srdd = jvm.WktReader.readToGeometryRDD(
            rawTextRDD._jrdd, wktColumn, allowInvalidGeometries, skipSyntacticallyInvalidGeometries
        )
        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    @since("1.2.0")
    @require(["WktReader"])
    def validate_imports(cls) -> bool:
        return True
