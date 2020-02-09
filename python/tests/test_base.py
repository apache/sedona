from pyspark.sql import SparkSession

from geospark.register import GeoSparkRegistrator
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geospark.utils.decorators import classproperty


class TestBase:

    @classproperty
    def spark(self):
        if not hasattr(self, "__spark"):
            spark = SparkSession. \
                builder. \
                config("spark.serializer", KryoSerializer.getName).\
                config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) .\
                master("local[*]").\
                getOrCreate()

            GeoSparkRegistrator.registerAll(spark)

            setattr(self, "__spark", spark)
        return getattr(self, "__spark")

    @classproperty
    def sc(self):
        if not hasattr(self, "__spark"):
            setattr(self, "__sc", self.spark._sc)
        return getattr(self, "__sc")

