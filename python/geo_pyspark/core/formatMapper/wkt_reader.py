import os

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.formatMapper.geo_reader import GeoDataReader
from geo_pyspark.core.jvm.config import since
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.decorators import require
from geo_pyspark.utils.meta import MultipleMeta
from tests.tools import tests_path


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
    @require([GeoSparkLib.WktReader])
    def validate_imports(cls) -> bool:
        return True


if __name__ == "__main__":
    from geo_pyspark.register import upload_jars, GeoSparkRegistrator

    upload_jars()
    spark = SparkSession. \
        builder. \
        master("local[*]"). \
        getOrCreate()

    GeoSparkRegistrator.registerAll(spark)

    spatial_rdd = WktReader.readToGeometryRDD(spark.sparkContext, os.path.join(tests_path, "resources/county_small.tsv"), 0, True, True)
    print(spatial_rdd.rawSpatialRDD.map(lambda x: x.geom).collect())

