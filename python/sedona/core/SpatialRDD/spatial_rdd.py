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

import pickle
from typing import Optional, List, Union

import attr
from py4j.java_gateway import get_field
from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark import StorageLevel

from sedona.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from sedona.core.enums.grid_type import GridTypeJvm, GridType
from sedona.core.enums.index_type import IndexTypeJvm, IndexType
from sedona.core.enums.spatial import SpatialType
from sedona.core.geom.envelope import Envelope
from sedona.core.jvm.config import since
from sedona.core.jvm.translate import SedonaPythonConverter, JvmSedonaPythonConverter
from sedona.utils.decorators import require
from sedona.utils.jvm import JvmStorageLevel
from sedona.utils.spatial_rdd_parser import SedonaPickler
from sedona.utils.types import crs


@attr.s
class SpatialPartitioner:
    name = attr.ib()
    jvm_partitioner = attr.ib()

    @classmethod
    def from_java_class_name(cls, jvm_partitioner) -> 'SpatialPartitioner':
        if jvm_partitioner is not None:
            jvm_full_name = jvm_partitioner.toString()
            full_class_name = jvm_full_name.split("@")[0]
            partitioner = full_class_name.split(".")[-1]
        else:
            partitioner = None

        return cls(partitioner, jvm_partitioner)


@attr.s
class JvmSpatialRDD:
    jsrdd = attr.ib()
    sc = attr.ib(type=SparkContext)
    tp = attr.ib(type=SpatialType)

    def saveAsObjectFile(self, location: str):
        self.jsrdd.saveAsObjectFile(location)

    def persist(self, storage_level: StorageLevel):
        new_jsrdd = self.jsrdd.persist(JvmStorageLevel(self.sc._jvm, storage_level).jvm_instance)
        self.jsrdd = new_jsrdd

    def count(self):
        return self.jsrdd.count()

    def cache(self):
        return self.persist(StorageLevel.MEMORY_ONLY)

    def unpersist(self):
        return self.jsrdd.unpersist()


@attr.s
class JvmGrids:
    jgrid = attr.ib()
    sc = attr.ib(type=SparkContext)


class SpatialRDD:

    def __init__(self, sc: Optional[SparkContext] = None):
        self._do_init(sc)
        self._srdd = self._jvm_spatial_rdd()

    def _do_init(self, sc: Optional[SparkContext] = None):
        if sc is None:
            session = SparkSession._instantiatedSession
            if session is None or session._sc._jsc is None:
                raise TypeError("Please initialize spark session")
            else:
                sc = session._sc
        self._sc = sc
        self._jvm = sc._jvm
        self._jsc = self._sc._jsc
        self._spatial_partitioned = False
        self._is_analyzed = False

    def analyze(self) -> bool:
        """
        Analyze SpatialRDD
        :return: bool,
        """
        self._srdd.analyze()
        self._is_analyzed = True
        return self._is_analyzed

    def CRSTransform(self, sourceEpsgCRSCode: crs, targetEpsgCRSCode: crs) -> bool:
        """
        Function transforms coordinates from one crs to another one
        :param sourceEpsgCRSCode: crs,  Cooridnate Reference System to transform from
        :param targetEpsgCRSCode: crs, Coordinate Reference System to transform to
        :return: bool, True if transforming was correct
        """
        return self._srdd.CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode)

    def MinimumBoundingRectangle(self):
        raise NotImplementedError()

    @property
    def approximateTotalCount(self) -> int:
        """

        :return:
        """
        return get_field(self._srdd, "approximateTotalCount")

    def boundary(self) -> Envelope:
        """

        :return:
        """

        jvm_boundary = self._srdd.boundary()

        envelope = Envelope.from_jvm_instance(jvm_boundary)
        return envelope

    @property
    def boundaryEnvelope(self) -> Envelope:
        """

        :return:
        """
        if not self._is_analyzed:
            raise TypeError("Please use analyze before")
        java_boundary_envelope = get_field(self._srdd, "boundaryEnvelope")
        return Envelope.from_jvm_instance(java_boundary_envelope)

    def buildIndex(self, indexType: Union[str, IndexType], buildIndexOnSpatialPartitionedRDD: bool) -> bool:
        """

        :param indexType:
        :param buildIndexOnSpatialPartitionedRDD:
        :return:
        """

        if self._spatial_partitioned or not buildIndexOnSpatialPartitionedRDD:
            if type(indexType) == str:
                index_type = IndexTypeJvm(self._jvm, IndexType.from_string(indexType))
            elif type(indexType) == IndexType:
                index_type = IndexTypeJvm(self._jvm, indexType)
            else:
                raise TypeError("indexType should be str or IndexType")
            return self._srdd.buildIndex(
                index_type.jvm_instance,
                buildIndexOnSpatialPartitionedRDD
            )
        else:
            raise AttributeError("Please run spatial partitioning before")

    def countWithoutDuplicates(self) -> int:
        """

        :return:
        """
        return self._srdd.countWithoutDuplicates()

    def countWithoutDuplicatesSPRDD(self) -> int:
        """

        :return:
        """
        return self._srdd.countWithoutDuplicatesSPRDD()

    @property
    @since("1.0.0")
    def fieldNames(self) -> List[str]:
        """

        :return:
        """
        try:
            field_names = list(get_field(self._srdd, "fieldNames"))
        except TypeError:
            field_names = []
        return field_names

    def getCRStransformation(self):
        """

        :return:
        """
        return self._srdd.getCRStransformation()

    def getPartitioner(self) -> SpatialPartitioner:
        """

        :return:
        """
        return SpatialPartitioner.from_java_class_name(self._srdd.getPartitioner())

    @require(["GeoSerializerData"])
    def getRawSpatialRDD(self):
        """

        :return:
        """

        serialized_spatial_rdd = SedonaPythonConverter(self._jvm).translate_spatial_rdd_to_python(
            self._srdd.getRawSpatialRDD())

        if not hasattr(self, "_raw_spatial_rdd"):
            RDD.saveAsObjectFile = lambda x, path: x._jrdd.saveAsObjectFile(path)
            setattr(self, "_raw_spatial_rdd", RDD(serialized_spatial_rdd, self._sc, SedonaPickler()))
        else:
            self._raw_spatial_rdd._jrdd = serialized_spatial_rdd

        return getattr(self, "_raw_spatial_rdd")

    def getSampleNumber(self) -> int:
        """

        :return:
        """
        return self._srdd.getSampleNumber()

    def getSourceEpsgCode(self) -> str:
        """
        Function which returns source EPSG code when it is assigned. If not an empty String is returned.
        :return: str, source epsg code.
        """
        return self._srdd.getSourceEpsgCode()

    def getTargetEpsgCode(self) -> str:
        """
        Function which returns target EPSG code when it is assigned. If not an empty String is returned.
        :return: str, target epsg code.
        """
        return self._srdd.getTargetEpgsgCode()

    @property
    def grids(self) -> Optional[List[Envelope]]:
        """
        Returns grids for SpatialRDD, it is a list of Envelopes.

        >> spatial_rdd.grids
        >> [Envelope(minx=10.0, maxx=12.0, miny=10.0, maxy=12.0)]
        :return:
        """
        jvm_grids = self.jvm_grids.jgrid
        if jvm_grids:
            number_of_grids = jvm_grids.size()

            envelopes = [Envelope.from_jvm_instance(jvm_grids[index]) for index in range(number_of_grids)]

            return envelopes
        else:
            return None

    @property
    def jvm_grids(self) -> JvmGrids:
        jvm_grids = get_field(self._srdd, "grids")
        return JvmGrids(jgrid=jvm_grids, sc=self._sc)

    @jvm_grids.setter
    def jvm_grids(self, jvm_grid: JvmGrids):
        self._srdd.grids = jvm_grid.jgrid

    @property
    def indexedRDD(self):
        """

        :return:
        """
        jrdd = get_field(self._srdd, "indexedRDD")
        if not hasattr(self, "_indexed_rdd"):
            RDD.saveAsObjectFile = lambda x, path: x._jrdd.saveAsObjectFile(path)
            RDD.count = lambda x: x._jrdd.count()
            setattr(self, "_indexed_rdd", RDD(jrdd, self._sc))
        else:
            self._indexed_rdd._jrdd = jrdd
        return getattr(self, "_indexed_rdd")

    @indexedRDD.setter
    def indexedRDD(self, indexed_rdd: RDD):
        """

        :return:
        """
        self._indexed_rdd = indexed_rdd

    @property
    def indexedRawRDD(self):
        jrdd = get_field(self._srdd, "indexedRawRDD")
        if not hasattr(self, "_indexed_raw_rdd"):
            RDD.saveAsObjectFile = lambda x, path: x._jrdd.saveAsObjectFile(path)
            RDD.count = lambda x: x._jrdd.count()
            setattr(self, "_indexed_raw_rdd", RDD(jrdd, self._sc))
        else:
            self._indexed_raw_rdd._jrdd = jrdd
        return getattr(self, "_indexed_raw_rdd")

    @indexedRawRDD.setter
    def indexedRawRDD(self, indexed_raw_rdd: RDD):
        self._indexed_raw_rdd = indexed_raw_rdd

    @property
    def rawSpatialRDD(self):
        """

        :return:
        """
        return self.getRawSpatialRDD()

    @rawSpatialRDD.setter
    def rawSpatialRDD(self, spatial_rdd):
        if isinstance(spatial_rdd, SpatialRDD):
            self._srdd = spatial_rdd._srdd
            self._sc = spatial_rdd._sc
            self._jvm = spatial_rdd._jvm
            self._spatial_partitioned = spatial_rdd._spatial_partitioned
        elif isinstance(spatial_rdd, RDD):
            jrdd = JvmSedonaPythonConverter(self._jvm).translate_python_rdd_to_java(spatial_rdd._jrdd)
            self._srdd.setRawSpatialRDD(jrdd)
        else:
            self._srdd.setRawSpatialRDD(spatial_rdd)

    def saveAsGeoJSON(self, path: str):
        """

        :param path:
        :return:
        """
        return self._srdd.saveAsGeoJSON(path)

    def saveAsWKB(self, path: str):
        """

        :param path:
        :return:
        """
        return self._srdd.saveAsWKB(path)

    def saveAsWKT(self, path: str):
        """

        :param path:
        :return:
        """
        return self._srdd.saveAsWKT(path)

    def setRawSpatialRDD(self, jrdd):
        """

        :return:
        """
        return self._srdd.setRawSpatialRDD(jrdd)

    def setSampleNumber(self, sampleNumber: int) -> bool:
        """

        :return:
        """
        return self._srdd.setSampleNumber(sampleNumber)

    @property
    def spatialPartitionedRDD(self):
        """

        :return:
        """
        serialized_spatial_rdd = SedonaPythonConverter(self._jvm).translate_spatial_rdd_to_python(
            get_field(self._srdd, "spatialPartitionedRDD"))

        if not hasattr(self, "_spatial_partitioned_rdd"):
            setattr(self, "_spatial_partitioned_rdd", RDD(serialized_spatial_rdd, self._sc, SedonaPickler()))
        else:
            self._spatial_partitioned_rdd._jrdd = serialized_spatial_rdd

        return getattr(self, "_spatial_partitioned_rdd")

    def spatialPartitioning(self, partitioning: Union[str, GridType, SpatialPartitioner, List[Envelope]],
                            num_partitions: Optional[int] = None) -> bool:
        """

        :param partitioning: partitioning type
        :param num_partitions: number of partitions
        :return:
        """
        if type(partitioning) == str:
            grid = GridTypeJvm(self._jvm, GridType.from_str(partitioning)).jvm_instance
        elif type(partitioning) == GridType:
            grid = GridTypeJvm(self._jvm, partitioning).jvm_instance
        elif type(partitioning) == SpatialPartitioner:
            grid = partitioning.jvm_partitioner
        elif type(partitioning) == list:
            if isinstance(partitioning[0], Envelope):
                bytes_data = pickle.dumps(partitioning)
                jvm_envelopes = self._jvm.EnvelopeAdapter.getFromPython(bytes_data)
                grid = jvm_envelopes
            else:
                raise AttributeError("List should consists of Envelopes")
        else:
            raise TypeError("Grid does not have correct type")

        self._spatial_partitioned = True

        if num_partitions:
            return self._srdd.spatialPartitioning(grid, num_partitions)
        else:
            return self._srdd.spatialPartitioning(grid)

    def set_srdd(self, srdd):
        self._srdd = srdd

    def get_srdd(self):
        return self._srdd

    def getRawJvmSpatialRDD(self) -> JvmSpatialRDD:
        return JvmSpatialRDD(jsrdd=self._srdd.getRawSpatialRDD(), sc=self._sc, tp=SpatialType.from_str(self.name))

    @property
    def rawJvmSpatialRDD(self) -> JvmSpatialRDD:
        return self.getRawJvmSpatialRDD()

    @rawJvmSpatialRDD.setter
    def rawJvmSpatialRDD(self, jsrdd_p: JvmSpatialRDD):
        if jsrdd_p.tp.value.lower() != self.name:
            raise TypeError(f"value should be type {self.name} but {jsrdd_p.tp} was found")

        self._sc = jsrdd_p.sc
        self._jvm = self._sc._jvm
        self._jsc = self._sc._jsc
        self.setRawSpatialRDD(jsrdd_p.jsrdd)

    def getJvmSpatialPartitionedRDD(self) -> JvmSpatialRDD:
        return JvmSpatialRDD(jsrdd=get_field(
            self._srdd, "spatialPartitionedRDD"), sc=self._sc, tp=SpatialType.from_str(self.name)
        )

    @property
    def jvmSpatialPartitionedRDD(self) -> JvmSpatialRDD:
        return self.getJvmSpatialPartitionedRDD()

    @jvmSpatialPartitionedRDD.setter
    def jvmSpatialPartitionedRDD(self, jsrdd_p: JvmSpatialRDD):
        if jsrdd_p.tp.value.lower() != self.name:
            raise TypeError(f"value should be type {self.name} but {jsrdd_p.tp} was found")

        self._sc = jsrdd_p.sc
        self._jvm = self._sc._jvm
        self._jsc = self._sc._jsc
        self._srdd.jvmSpatialPartitionedRDD = jsrdd_p.jsrdd

    @property
    def name(self):
        name = self.__class__.__name__
        return name.replace("RDD", "").lower()

    @property
    def _jvm_spatial_rdd(self):
        spatial_factory = SpatialRDDFactory(self._sc)
        return spatial_factory.create_spatial_rdd()
