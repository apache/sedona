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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.spatialOperator.rdd import SedonaPairRDD


class StructuredAdapter:
    """
    Class which allow to convert between Spark DataFrame and SpatialRDD and reverse.
    This class is used to convert between PySpark DataFrame and SpatialRDD. Schema
    is lost during the conversion. This should be used if your data starts as a
    SpatialRDD and you want to convert it to a DataFrame for further processing.
    """

    @staticmethod
    def _create_dataframe(jdf, sparkSession: SparkSession) -> DataFrame:
        return DataFrame(jdf, sparkSession)

    @classmethod
    def toSpatialRdd(
        cls, dataFrame: DataFrame, geometryFieldName: str = None
    ) -> SpatialRDD:
        """
        Convert a DataFrame to a SpatialRDD
        :param dataFrame:
        :param geometryFieldName:
        :return:
        """
        sc = dataFrame._sc
        jvm = sc._jvm
        if geometryFieldName is None:
            srdd = jvm.StructuredAdapter.toSpatialRdd(dataFrame._jdf)
        else:
            srdd = jvm.StructuredAdapter.toSpatialRdd(dataFrame._jdf, geometryFieldName)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, sparkSession: SparkSession) -> DataFrame:
        """
        Convert a SpatialRDD to a DataFrame
        :param spatialRDD:
        :param sparkSession:
        :return:
        """
        sc = spatialRDD._sc
        jvm = sc._jvm

        jdf = jvm.StructuredAdapter.toDf(spatialRDD._srdd, sparkSession._jsparkSession)

        df = StructuredAdapter._create_dataframe(jdf, sparkSession)

        return df

    @classmethod
    def toSpatialPartitionedDf(
        cls, spatialRDD: SpatialRDD, sparkSession: SparkSession
    ) -> DataFrame:
        """
        Convert a SpatialRDD to a DataFrame. This DataFrame will be spatially partitioned
        :param spatialRDD:
        :param sparkSession:
        :return:
        """
        sc = spatialRDD._sc
        jvm = sc._jvm

        jdf = jvm.StructuredAdapter.toSpatialPartitionedDf(
            spatialRDD._srdd, sparkSession._jsparkSession
        )

        df = StructuredAdapter._create_dataframe(jdf, sparkSession)

        return df

    @classmethod
    def pairRddToDf(
        cls,
        rawPairRDD: SedonaPairRDD,
        left_schema: StructType,
        right_schema: StructType,
        sparkSession: SparkSession,
    ) -> DataFrame:
        """
        Convert a raw pair RDD to a DataFrame. This is useful when you have a Spatial join result
        Args:
            rawPairRDD:
            left_schema:
            right_schema:
            sparkSession:

        Returns:

        """
        jvm = sparkSession._jvm
        left_schema_json = left_schema.json()
        right_schema_json = right_schema.json()
        jdf = jvm.StructuredAdapter.toDf(
            rawPairRDD.jsrdd,
            left_schema_json,
            right_schema_json,
            sparkSession._jsparkSession,
        )
        df = StructuredAdapter._create_dataframe(jdf, sparkSession)
        return df
