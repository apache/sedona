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

from sedona.register.geo_registrator import PackageImporter
from sedona.utils import KryoSerializer, SedonaKryoRegistrator


@attr.s
class SedonaContext:
    @classmethod
    def create(cls, spark: SparkSession) -> SparkSession:
        """
        This is the core of whole package, It uses py4j to run wrapper which takes existing SparkSession
        and register the core logics of Apache Sedona, for this SparkSession.

        :param spark: pyspark.sql.SparkSession, spark session instance
        :return: SedonaContext which is an instance of SparkSession
        """
        spark.sql("SELECT 1 as geom").count()
        PackageImporter.import_jvm_lib(spark._jvm)
        spark._jvm.SedonaContext.create(spark._jsparkSession)
        return spark

    @classmethod
    def builder(cls) -> SparkSession.builder:
        """
        This method adds the basic Sedona configuration to the SparkSession builder.
        Usually the user does not need to call this method directly, as it is configured when a cluster is created.
        This method is needed when the user wants to manually configure Sedona
        :return: SparkSession.builder
        """
        return SparkSession.builder.config("spark.serializer", KryoSerializer.getName).\
            config("spark.kryo.registrator", SedonaKryoRegistrator.getName)