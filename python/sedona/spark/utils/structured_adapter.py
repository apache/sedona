# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from sedona.spark.core.enums.grid_type import GridType
from sedona.spark.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.spark.core.spatialOperator.rdd import SedonaPairRDD


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

    @classmethod
    def repartitionBySpatialKey(
        cls,
        dataFrame: DataFrame,
        gridType: GridType = GridType.KDBTREE,
        geometryFieldName: str = None,
        numPartitions: int = 0,
    ) -> DataFrame:
        """
        Repartition a DataFrame using a spatial partitioning scheme (e.g., KDB-Tree).
        This is a convenience method that wraps the multi-step process of converting a
        DataFrame to a SpatialRDD, applying spatial partitioning without duplicates,
        and converting back to a DataFrame.

        Example usage::

            partitioned_df = StructuredAdapter.repartitionBySpatialKey(df, GridType.KDBTREE, "geometry", 16)
            partitioned_df.write.format("geoparquet").save("/path/to/output")

        Args:
            dataFrame: The input DataFrame containing a geometry column.
            gridType: The spatial partitioning grid type (default: GridType.KDBTREE).
            geometryFieldName: The name of the geometry column. If None, auto-detects.
            numPartitions: The target number of partitions. If 0, uses the current number.

        Returns:
            A spatially partitioned DataFrame.
        """
        sc = dataFrame._sc
        jvm = sc._jvm
        sparkSession = dataFrame.sparkSession

        jgrid_type = jvm.org.apache.sedona.core.enums.GridType.getGridType(
            gridType.value
        )

        if geometryFieldName is not None:
            jdf = jvm.StructuredAdapter.repartitionBySpatialKey(
                dataFrame._jdf, geometryFieldName, jgrid_type, numPartitions
            )
        else:
            jdf = jvm.StructuredAdapter.repartitionBySpatialKey(
                dataFrame._jdf, jgrid_type, numPartitions
            )

        return StructuredAdapter._create_dataframe(jdf, sparkSession)
