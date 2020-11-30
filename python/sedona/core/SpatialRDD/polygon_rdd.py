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

from pyspark import SparkContext, StorageLevel, RDD

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD, JvmSpatialRDD
from sedona.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from sedona.core.enums.file_data_splitter import FileSplitterJvm, FileDataSplitter
from sedona.core.jvm.translate import PythonRddToJavaRDDAdapter
from sedona.utils.jvm import JvmStorageLevel
from sedona.utils.meta import MultipleMeta


class PolygonRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self, rdd: RDD, newLevel: StorageLevel):
        super().__init__(rdd.ctx)

        spatial_rdd = PythonRddToJavaRDDAdapter(self._jvm).deserialize_to_polygon_raw_rdd(rdd._jrdd)

        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        srdd = self._jvm_spatial_rdd(spatial_rdd, new_level_jvm)
        self._srdd = srdd

    def __init__(self, rdd: RDD):
        super().__init__(rdd.ctx)
        spatial_rdd = PythonRddToJavaRDDAdapter(self._jvm).deserialize_to_polygon_raw_rdd(rdd._jrdd)
        srdd = self._jvm_spatial_rdd(spatial_rdd)
        self._srdd = srdd

    def __init__(self):
        self._do_init()
        self._srdd = self._jvm_spatial_rdd()

    def __init__(self, rawSpatialRDD: JvmSpatialRDD):
        """

        :param rawSpatialRDD:
        """
        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        self._srdd = self._jvm_spatial_rdd(jsrdd)

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param sourceEpsgCode:
        :param targetEpsgCode:
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        self._srdd = self._jvm_spatial_rdd(jsrdd, sourceEpsgCode, targetEpsgCode)

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, newLevel: StorageLevel):
        """
        :param rawSpatialRDD:
        :param sourceEpsgCode:
        :param targetEpsgCode:
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(jsrdd, new_level_jvm)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str,
                 splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str,
                 targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(jsrdd, new_level_jvm, sourceEpsgCRSCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel,
                 sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str,
                 targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData: bool,
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def MinimumBoundingRectangle(self):
        from sedona.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD()
        srdd = self._srdd.MinimumBoundingRectangle()

        rectangle_rdd.set_srdd(srdd)

        return rectangle_rdd

    @property
    def _jvm_spatial_rdd(self):
        spatial_factory = SpatialRDDFactory(self._sc)

        jvm_polygon_rdd = spatial_factory.create_polygon_rdd()

        return jvm_polygon_rdd
