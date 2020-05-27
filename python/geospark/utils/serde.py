from abc import ABC

from geospark.utils.decorators import classproperty


class Serializer(ABC):

    @classproperty
    def getName(self):
        raise NotImplementedError


class KryoSerializer(Serializer):

    @classproperty
    def getName(self):
        return "org.apache.spark.serializer.KryoSerializer"


class GeoSparkKryoRegistrator(Serializer):

    @classproperty
    def getName(self):
        return "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator"
