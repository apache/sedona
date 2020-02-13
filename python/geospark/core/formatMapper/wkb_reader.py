from pyspark import SparkContext, RDD

from geospark.core.SpatialRDD import SpatialRDD
from geospark.core.formatMapper.geo_reader import GeoDataReader
from geospark.core.jvm.config import since
from geospark.utils.decorators import require
from geospark.utils.meta import MultipleMeta


class WkbReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    @since("1.2.0")
    @require(["WkbReader"])
    def validate_imports(cls):
        pass

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str, wkbColumn: int, allowInvalidGeometries: bool, skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param sc:
        :param inputPath:
        :param wkbColumn:
        :param allowInvalidGeometries:
        :param skipSyntacticallyInvalidGeometries:
        :return:
        """
        WkbReader.validate_imports()
        jvm = sc._jvm
        spatial_rdd = SpatialRDD(sc)
        srdd = jvm.WkbReader.readToGeometryRDD(sc._jsc, inputPath, wkbColumn, allowInvalidGeometries, skipSyntacticallyInvalidGeometries)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, rawTextRDD: RDD, wkbColumn: int, allowInvalidGeometries: bool, skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param rawTextRDD:
        :param wkbColumn:
        :param allowInvalidGeometries:
        :param skipSyntacticallyInvalidGeometries:
        :return:
        """
        WkbReader.validate_imports()
        sc = rawTextRDD.ctx
        jvm = sc._jvm

        spatial_rdd = SpatialRDD(sc)
        srdd = jvm.WkbReader.readToGeometryRDD(rawTextRDD._jrdd, wkbColumn, allowInvalidGeometries, skipSyntacticallyInvalidGeometries)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd