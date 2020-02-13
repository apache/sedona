from pyspark import SparkContext, StorageLevel, RDD

from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD, JvmSpatialRDD
from geospark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geospark.core.enums import FileDataSplitter
from geospark.core.enums.file_data_splitter import FileSplitterJvm
from geospark.utils.jvm import JvmStorageLevel
from geospark.utils.meta import MultipleMeta


class LineStringRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self, rdd: RDD):
        super().__init__(rdd.ctx)

        spatial_rdd = self._jvm.GeoSerializerData.deserializeToLineStringRawRDD(rdd._jrdd)

        srdd = self._jvm_spatial_rdd(spatial_rdd)
        self._srdd = srdd

    def __init__(self, rdd: RDD, newLevel: StorageLevel):
        self._sc = rdd.ctx
        self._jvm = self._sc._jvm

        spatial_rdd = self._jvm.GeoSerializerData.deserializeToLineStringRawRDD(rdd._jrdd)

        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        srdd = self._jvm_spatial_rdd(spatial_rdd, new_level_jvm)
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

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD: RDD
        :param sourceEpsgCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """
        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        self._srdd = self._jvm_spatial_rdd(jsrdd, sourceEpsgCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter,  carryInputData: bool, partitions: int):
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

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool):
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
        :param rawSpatialRDD: RDD
        :param newLevel: StorageLevel
        """
        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(jsrdd, new_level_jvm)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):

        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param startOffset: int, starting offset
        :param endOffset: int, ending offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param partitions: int, number of partitions
        :param newLevel: StorageLevel
        """
        super().__init__(sparkContext)
        self._srdd = self._jvm_spatial_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            FileSplitterJvm(self._jvm, splitter).jvm_instance,
            carryInputData,
            partitions,
            JvmStorageLevel(self._jvm, newLevel).jvm_instance
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param startOffset: int, starting offset
        :param endOffset: int, ending offset
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
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int, newLevel: StorageLevel):
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

        :param rawSpatialRDD: RDD
        :param newLevel: StorageLevel
        :param sourceEpsgCRSCode: str, epsg code which loaded files is in, ex. epsg:4326 stands for WGS84
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
        """

        super().__init__(rawSpatialRDD.sc)
        jsrdd = rawSpatialRDD.jsrdd
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = self._jvm_spatial_rdd(jsrdd, new_level_jvm, sourceEpsgCRSCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel,
                 sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param startOffset: int, starting offset
        :param endOffset: int, ending offset
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
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

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param startOffset: int, starting offset
        :param endOffset: int, ending offset
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

        :param sparkContext: SparkContext instance
        :param InputLocation: str, location for loaded file
        :param splitter: FileDataSplitter, data file splitter
        :param carryInputData: bool, if spatial rdd should keep non geometry attributes
        :param targetEpsgCode: str, epsg code to transform SpatialRDD
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

    @property
    def _jvm_spatial_rdd(self):
        if self._sc is not None:
            spatial_factory = SpatialRDDFactory(self._sc)
        else:
            raise TypeError("Please initialize spark Session first")
        return spatial_factory.create_linestring_rdd()

    def MinimumBoundingRectangle(self):
        from geospark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD()
        srdd = self._srdd.MinimumBoundingRectangle()

        rectangle_rdd.set_srdd(srdd)

        return rectangle_rdd
