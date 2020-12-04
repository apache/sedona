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


class PointRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self, rdd: RDD, newLevel: StorageLevel):
        """

        :param rdd: RDD
        :param newLevel: StorageLevel StorageLevel
        """
        super().__init__(rdd.ctx)

        spatial_rdd = PythonRddToJavaRDDAdapter(self._jvm).deserialize_to_point_raw_rdd(rdd._jrdd)

        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        srdd = self._jvm_spatial_rdd(spatial_rdd, new_level_jvm)
        self._srdd = srdd

    def __init__(self, rdd: RDD):
        """

        :param rdd: RDD
        """
        super().__init__(rdd.ctx)

        spatial_rdd = PythonRddToJavaRDDAdapter(self._jvm).deserialize_to_point_raw_rdd(rdd._jrdd)

        srdd = self._jvm_spatial_rdd(spatial_rdd)
        self._srdd = srdd

    def __init__(self):
        self._do_init()
        self._srdd = self._jvm_spatial_rdd()

    def __init__(self, rawSpatialRDD: JvmSpatialRDD):
        """

        :param rawSpatialRDD: JvmSpatialRDD, jvm representation of spatial rdd RDD
        """
        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        self._srdd = self._jvm_spatial_rdd(jsrdd)

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD: JvmSpatialRDD, jvm representation of spatial rdd RDD
        :param sourceEpsgCode: str
        :param targetEpsgCode: str, epsg code to transform SpatialRDD str
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        self._srdd = self._jvm_spatial_rdd(jsrdd, sourceEpsgCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param Offset: int, point offset int
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions int
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            sparkContext._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param Offset: int, point offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            sparkContext._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData
        )

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, newLevel: StorageLevel):
        """

        :param rawSpatialRDD:
        :param newLevel:
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(jsrdd, new_level_jvm)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param Offset: int, point offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        :param newLevel: StorageLevel
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param Offset: int, point offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param newLevel: StorageLevel
        """

        super().__init__(sparkContext)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        :param newLevel: StorageLevel
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param newLevel: StorageLevel
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str,
                 targetEpsgCode: str):
        """

        :param rawSpatialRDD: JvmSpatialRDD, jvm representation of spatial rdd
        :param newLevel: StorageLevel
        :param sourceEpsgCRSCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(jsrdd, new_level_jvm, sourceEpsgCRSCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int, newLevel: StorageLevel, sourceEpsgCRSCode: str,
                 targetEpsgCode: str):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param Offset: int, point offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        :param newLevel: StorageLevel
        :param sourceEpsgCRSCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """
        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param Offset: int, point offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param newLevel: StorageLevel
        :param sourceEpsgCRSCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        :param newLevel: StorageLevel
        :param sourceEpsgCRSCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param newLevel: StorageLevel
        :param sourceEpsgCRSCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def MinimumBoundingRectangle(self):
        raise NotImplementedError("PointRDD has not MinimumBoundingRectangle method.")

    @property
    def _jvm_spatial_rdd(self):
        spatial_factory = SpatialRDDFactory(self._sc)
        return spatial_factory.create_point_rdd()
