from pyspark import SparkContext, RDD

from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geospark.core.formatMapper.geo_reader import GeoDataReader
from geospark.core.jvm.config import since
from geospark.utils.decorators import require
from geospark.utils.meta import MultipleMeta


class GeoJsonReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    @since("1.2.0")
    @require(["GeoJsonReader"])
    def validate_imports(cls) -> bool:
        return True

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str) -> SpatialRDD:
        """
        :param sc: SparkContext
        :param inputPath: str, file input location
        :return: SpatialRDD
        """
        GeoJsonReader.validate_imports()
        jvm = sc._jvm
        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            sc._jsc, inputPath
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str, allowInvalidGeometries: bool,
                          skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param sc: SparkContext
        :param inputPath: str, path to the file
        :param allowInvalidGeometries: bool
        :param skipSyntacticallyInvalidGeometries: bool
        :return: SpatialRDD
        """
        GeoJsonReader.validate_imports()
        jvm = sc._jvm
        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            sc._jsc, inputPath, allowInvalidGeometries, skipSyntacticallyInvalidGeometries
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, rawTextRDD: RDD) -> SpatialRDD:
        """

        :param rawTextRDD:  RDD
        :return: SpatialRDD
        """
        GeoJsonReader.validate_imports()
        sc = rawTextRDD.ctx
        jvm = sc._jvm

        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            rawTextRDD._jrdd
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, rawTextRDD: RDD, allowInvalidGeometries: bool, skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param rawTextRDD: RDD
        :param allowInvalidGeometries: bool
        :param skipSyntacticallyInvalidGeometries: bool
        :return: SpatialRDD
        """
        GeoJsonReader.validate_imports()
        sc = rawTextRDD.ctx
        jvm = sc._jvm

        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            rawTextRDD._jrdd, allowInvalidGeometries, skipSyntacticallyInvalidGeometries
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd
