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

import attr
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

from sedona.register.java_libs import SedonaJvmLib
from sedona.utils.prep import assign_all

assign_all()
jvm_import = str


@attr.s
class SedonaRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        """
        This is the core of whole package, It uses py4j to run wrapper which takes existing SparkSession
        and register all User Defined Functions by Apache Sedona developers, for this SparkSession.

        :param spark: pyspark.sql.SparkSession, spark session instance
        :return: bool, True if registration was correct.
        """
        spark.sql("SELECT 1 as geom").count()
        PackageImporter.import_jvm_lib(spark._jvm)
        cls.register(spark)
        return True

    @classmethod
    def register(cls, spark: SparkSession):
        return spark._jvm.SedonaSQLRegistrator.registerAll(spark._jsparkSession)


class PackageImporter:

    @staticmethod
    def import_jvm_lib(jvm) -> bool:
        from sedona.core.utils import ImportedJvmLib
        """
        Imports all the specified methods and functions in jvm
        :param jvm: Jvm gateway from py4j
        :return:
        """
        for lib in SedonaJvmLib:
            java_import(jvm, lib.value)
            ImportedJvmLib.import_lib(lib.name)

        return True
