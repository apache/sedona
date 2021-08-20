from pyspark.sql import SparkSession


class SparkConfGetter:

    def _get_serialization_type(self):
        current_spark = SparkSession.getActiveSession()
        if current_spark:
            spark_context = current_spark.sparkContext
            conf = spark_context.getConf()
            serializer_type = conf.get("sedona.serializer.type")
        else:
            serializer_type = "shape"

        return serializer_type

    @property
    def serialization(self):
        if not hasattr(self, "__serialization"):
            setattr(self, "__serialization", self._get_serialization_type())
        return getattr(self, "__serialization")
