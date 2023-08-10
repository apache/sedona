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
from sedona.core.enums import FileDataSplitter
from sedona.core.enums.file_data_splitter import FileSplitterJvm
from sedona.core.jvm.translate import PythonRddToJavaRDDAdapter
from sedona.utils.meta import MultipleMeta


class LineStringRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self, rdd: RDD):
        super().__init__(rdd.ctx)

        spatial_rdd = PythonRddToJavaRDDAdapter(self._jvm).deserialize_to_linestring_raw_rdd(rdd._jrdd)

        srdd = self._jvm_spatial_rdd(spatial_rdd)
        self._srdd = srdd

    def __init__(self):
        self._do_init()
        self._srdd = self._jvm_spatial_rdd()

    def __init__(self, rawSpatialRDD: JvmSpatialRDD):
        """

        :param rawSpatialRDD: RDD
        """
        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        self._srdd = self._jvm_spatial_rdd(jsrdd)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param startOffset: int, starting offset
        :param endOffset: int, ending offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param startOffset: int, starting offset
        :param endOffset: int, ending offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        """

        super().__init__(sparkContext)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
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

    @property
    def _jvm_spatial_rdd(self):
        if self._sc is not None:
            spatial_factory = SpatialRDDFactory(self._sc)
        else:
            raise TypeError("Please initialize spark Session first")
        return spatial_factory.create_linestring_rdd()

    def MinimumBoundingRectangle(self):
        from sedona.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD()
        srdd = self._srdd.MinimumBoundingRectangle()

        rectangle_rdd.set_srdd(srdd)

        return rectangle_rdd
