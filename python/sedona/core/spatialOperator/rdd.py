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
        srdd = SpatialRDD(self.sc)
        srdd.setRawSpatialRDD(self.jsrdd)
        jvm = spark._jvm
        jspark = spark._jsparkSession

        df = DataFrame(jvm.Adapter.toDf(srdd._srdd, jspark), spark._wrapped)
        return self.__assign_field_names(df, field_names)

    def to_rdd(self) -> RDD:
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm). \
            translate_spatial_rdd_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())

    def to_geometry_df(self, spark: SparkSession, field_names: Optional[List] = None):
        srdd = SpatialRDD(self.sc)
        srdd.setRawSpatialRDD(self.jsrdd)
        jvm = spark._jvm
        jspark = spark._jsparkSession

        df = DataFrame(jvm.Adapter.toGeometryDf(srdd._srdd, jspark), spark._wrapped)
        return self.__assign_field_names(df, field_names)

    def __assign_field_names(self, dataframe: DataFrame, field_names: Optional[List[str]] = None):
        if field_names:
            return dataframe.toDF(*field_names)
        else:
            return dataframe


class SedonaPairRDD:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_df(self, spark: SparkSession,
              left_field_names: Optional[List] = None,
              right_field_names: Optional[List] = None) -> DataFrame:
        jvm = spark._jvm
        df = DataFrame(jvm.Adapter.toDf(self.jsrdd, spark._jsparkSession), spark._wrapped)
        return self.__assign_field_names(df, left_field_names, right_field_names)

    def to_rdd(self) -> RDD:
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm). \
            translate_spatial_pair_rdd_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())

    def to_geometry_df(self, spark: SparkSession, left_field_names: Optional[List] = None,
                       right_field_names: Optional[List] = None):
        jvm = spark._jvm
        geometry_df = DataFrame(jvm.Adapter.toGeometryDf(self.jsrdd, spark._jsparkSession), spark._wrapped)

        return self.__assign_field_names(geometry_df, left_field_names, right_field_names)

    def __assign_field_names(self, dataframe: DataFrame, left_field_names: Optional[List] = None,
                              right_field_names: Optional[List] = None):
        if left_field_names is None and right_field_names is None:
            return dataframe
        elif left_field_names is not None and right_field_names is not None:
            column_names = [*left_field_names, *right_field_names]
            return dataframe.toDF(*column_names)
        else:
            raise AttributeError("when passing left_field_names you have also pass right_field_names and reverse")


class SedonaPairRDDList:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_rdd(self):
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm). \
            translate_spatial_pair_rdd_with_list_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())
