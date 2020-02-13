import os
from re import findall
from typing import Optional
import logging

from pyspark.sql import SparkSession

from geospark.utils.decorators import classproperty

FORMAT = '%(asctime) %(message)s'
logging.basicConfig(format=FORMAT)


def compare_versions(version_a: str, version_b: str) -> bool:
    if all([version_b, version_a]):
        version_numbers = version_a.split("."), version_b.split(".")
        if any([version[0] == "" for version in version_numbers]):
            return False

        for ver_a, ver_b in zip(*version_numbers):
            if int(ver_a) < int(ver_b):
                return False
    else:
        return False
    return True


def since(version: str):
    def wrapper(function):
        def applier(*args, **kwargs):
            geo_spark_version = GeoSparkMeta.version
            if not compare_versions(geo_spark_version, version):
                logging.warning(f"This function is not available for {geo_spark_version}, "
                                f"please use version higher than {version}")
                raise AttributeError(f"Not available before {version} geospark version")
            result = function(*args, **kwargs)
            return result
        return applier
    return wrapper


def depreciated(version: str, substitute: str):
    def wrapper(function):
        def applier(*args, **kwargs):
            result = function(*args, **kwargs)
            geo_spark_version = GeoSparkMeta.version
            if geo_spark_version >= version:
                logging.warning("Function is depreciated")
                if substitute:
                    logging.warning(f"Please use {substitute} instead")
            return result
        return applier
    return wrapper


class SparkJars:

    @staticmethod
    def get_used_jars():
        spark = SparkSession._instantiatedSession
        if spark is not None:
            spark_conf = spark.conf
        else:
            raise TypeError("SparkSession is not initiated")
        java_spark_conf = spark_conf._jconf
        try:
            used_jar_files = java_spark_conf.get("spark.jars")
        except Exception as e:
            used_jar_files = ",".join(os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars")))
        return used_jar_files

    @property
    def jars(self):
        if not hasattr(self, "__spark_jars"):
            setattr(self, "__spark_jars", self.get_used_jars())
        return getattr(self, "__spark_jars")


class GeoSparkMeta:

    @classmethod
    def get_version(cls, spark_jars: str) -> Optional[str]:
        geospark_version = findall(r"geospark-(\d{1}\.\d{1}\.\d{1}).*?jar", spark_jars)
        try:
            version = geospark_version[0]
        except IndexError:
            version = None
        return version

    @classproperty
    def version(cls):
        spark_jars = SparkJars.get_used_jars()
        if not hasattr(cls, "__version"):
            setattr(cls, "__version", cls.get_version(spark_jars))
        return getattr(cls, "__version")


if __name__ == "__main__":
    assert not compare_versions("1.2.0", "1.1.5")
    assert compare_versions("1.3.5", "1.2.0")
    assert not compare_versions("", "1.2.0")
    assert not compare_versions("1.3.5", "")
