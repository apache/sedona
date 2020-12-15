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

from typing import List, Optional

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame

from sedona.core.SpatialRDD import SpatialRDD
from sedona.core.jvm.translate import JvmSedonaPythonConverter
from sedona.utils.spatial_rdd_parser import SedonaPickler


class SedonaRDD:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_df(self, spark: SparkSession, field_names: List[str] = None) -> DataFrame:
        from sedona.utils.adapter import Adapter
        srdd = SpatialRDD(self.sc)
        srdd.setRawSpatialRDD(self.jsrdd)
        if field_names:
            return Adapter.toDf(srdd, field_names, spark)
        else:
            return Adapter.toDf(srdd, spark)

    def to_rdd(self) -> RDD:
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm). \
            translate_spatial_rdd_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())


class SedonaPairRDD:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_df(self, spark: SparkSession,
              left_field_names: Optional[List] = None,
              right_field_names: Optional[List] = None) -> DataFrame:
        from sedona.utils.adapter import Adapter
        if left_field_names is not None and right_field_names is not None:
            df = Adapter.toDf(self, left_field_names, right_field_names, spark)
            return df

        elif left_field_names is None and right_field_names is None:
            df = Adapter.toDf(self, spark)
            return df
        else:
            raise AttributeError("when passing left_field_names you have also pass right_field_names and reverse")

    def to_rdd(self) -> RDD:
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm). \
            translate_spatial_pair_rdd_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())


class SedonaPairRDDList:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_rdd(self):
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm). \
            translate_spatial_pair_rdd_with_list_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())
