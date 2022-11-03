#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import logging
import os
from re import findall
from typing import Any, Optional, Tuple

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from sedona.utils.decorators import classproperty


def is_greater_or_equal_version(version_a: str, version_b: str) -> bool:
    if all([version_b, version_a]):
        version_numbers = version_a.split("."), version_b.split(".")
        if any([version[0] == "" for version in version_numbers]):
            return False

        for ver_a, ver_b in zip(*version_numbers):
            if int(ver_a) > int(ver_b):
                return True
            elif int(ver_a) < int(ver_b):
                return False
    else:
        return False
    return True


def since(version: str):
    def wrapper(function):
        def applier(*args, **kwargs):
            sedona_version = SedonaMeta.version
            if not is_greater_or_equal_version(sedona_version, version):
                logging.warning(
                    f"This function is not available for {sedona_version}, "
                    f"please use version higher than {version}"
                )
                raise AttributeError(f"Not available before {version} sedona version")
            result = function(*args, **kwargs)
            return result

        return applier

    return wrapper


def depreciated(version: str, substitute: str):
    def wrapper(function):
        def applier(*args, **kwargs):
            result = function(*args, **kwargs)
            sedona_version = SedonaMeta.version
            if sedona_version >= version:
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

        # When deployed normally, Sedona appears in `spark.jars``, when it's submitted
        # via YARN it's in `spark.yarn.dist.jars`
        used_jar_files_lookup_with_errors = [
            SparkJars.get_spark_java_config(spark, config_id)
            for config_id in ("spark.jars", "spark.yarn.dist.jars")
        ]

        used_jar_files_lookup = [
            lookup_result for lookup_result, _ in used_jar_files_lookup_with_errors
        ]

        used_jar_files = (
            ",".join(jars for jars in used_jar_files_lookup if jars)
            if any(used_jar_files_lookup)
            else None
        )

        if not used_jar_files:

            for _, error in used_jar_files_lookup_with_errors:
                logging.warning(error)

            logging.info("Trying to get filenames from the $SPARK_HOME/jars directory")
            used_jar_files = ",".join(
                os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars"))
            )

        return used_jar_files

    @property
    def jars(self):
        if not hasattr(self, "__spark_jars"):
            setattr(self, "__spark_jars", self.get_used_jars())
        return getattr(self, "__spark_jars")

    @staticmethod
    def get_spark_java_config(
        spark: SparkSession, value: str
    ) -> Tuple[Optional[str], Optional[str]]:
        if spark is not None:
            spark_conf = spark.conf
        else:
            raise TypeError("SparkSession is not initiated")

        java_spark_conf = spark_conf._jconf
        used_jar_files = None
        error_message = None

        try:
            used_jar_files = java_spark_conf.get(value)
        except Py4JJavaError as java_error:
            error_message = "Failed to get the value of {} from SparkConf: {}".format(
                value, java_error
            )

        return used_jar_files, error_message


class SedonaMeta:
    @classmethod
    def get_version(cls, spark_jars: str) -> Optional[str]:
        # Find Spark version, Scala version and Sedona version.
        versions = findall(
            r"sedona-python-adapter-([^,\n]+)_([^,\n]+)-([^,\n]+)-incubating",
            spark_jars,
        )
        try:
            sedona_version = versions[0][2]
        except IndexError:
            sedona_version = None
        return sedona_version

    @classproperty
    def version(cls):
        spark_jars = SparkJars.get_used_jars()
        if not hasattr(cls, "__version"):
            setattr(cls, "__version", cls.get_version(spark_jars))
        return getattr(cls, "__version")


if __name__ == "__main__":
    assert not is_greater_or_equal_version("1.1.5", "1.2.0")
    assert is_greater_or_equal_version("1.2.0", "1.1.5")
    assert is_greater_or_equal_version("1.3.5", "1.2.0")
    assert not is_greater_or_equal_version("", "1.2.0")
    assert not is_greater_or_equal_version("1.3.5", "")
