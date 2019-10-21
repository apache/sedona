import attr
from pyspark.sql import SparkSession

from geo_pyspark.utils.prep import assign_all

assign_all()


@attr.s
class GeoSparkRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        """
        This is the core of whole package, It uses py4j to run wrapper which takes existing SparkSession
        and register all User Defined Functions by GeoSpark developers, for this SparkSession.

        :param spark: pyspark.sql.SparkSession, spark session instance
        :return: bool, True if registration was correct.
        """
        spark.sql("SELECT 1 as geom").count()
        cls.register(spark)
        return True

    @classmethod
    def register(cls, spark: SparkSession):
        spark._jvm. \
            org. \
            imbruced. \
            geo_pyspark. \
            GeoSparkWrapper. \
            registerAll()
