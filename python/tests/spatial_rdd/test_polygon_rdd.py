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

from tests.properties.polygon_properties import (
    grid_type,
    input_boundary,
    input_count,
    input_location,
    input_location_geo_json,
    input_location_wkb,
    input_location_wkt,
    num_partitions,
    polygon_rdd_end_offset,
    polygon_rdd_input_location,
    polygon_rdd_splitter,
    polygon_rdd_start_offset,
    query_envelope,
    splitter,
)
from tests.test_base import TestBase

from sedona.core.enums import FileDataSplitter, GridType, IndexType
from sedona.core.geom.envelope import Envelope
from sedona.core.SpatialRDD import PolygonRDD
from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD


class TestPolygonRDD(TestBase):

    def compare_spatial_rdd(self, spatial_rdd: SpatialRDD, envelope: Envelope) -> bool:

        spatial_rdd.analyze()

        assert input_count == spatial_rdd.approximateTotalCount
        assert envelope == spatial_rdd.boundaryEnvelope

        return True

    def test_constructor(self):
        spatial_rdd_core = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
        )
        self.compare_spatial_rdd(spatial_rdd_core, input_boundary)

        self.compare_spatial_rdd(spatial_rdd_core, input_boundary)
        spatial_rdd = PolygonRDD(rawSpatialRDD=spatial_rdd_core.rawJvmSpatialRDD)
        self.compare_spatial_rdd(spatial_rdd, input_boundary)

        query_window_rdd = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True,
            2,
        )
        assert query_window_rdd.analyze()
        assert query_window_rdd.approximateTotalCount == 3000

        query_window_rdd = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True,
        )
        assert query_window_rdd.analyze()
        assert query_window_rdd.approximateTotalCount == 3000

        spatial_rdd_core = PolygonRDD(
            self.sc, input_location, splitter, True, num_partitions
        )

        self.compare_spatial_rdd(spatial_rdd_core, input_boundary)

        spatial_rdd_core = PolygonRDD(self.sc, input_location, splitter, True)

        self.compare_spatial_rdd(spatial_rdd_core, input_boundary)

    def test_empty_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(grid_type)
        spatial_rdd.buildIndex(IndexType.RTREE, True)
        spatial_rdd_copy = PolygonRDD()
        spatial_rdd_copy.rawJvmSpatialRDD = spatial_rdd.rawJvmSpatialRDD
        spatial_rdd_copy.analyze()

    def test_geojson_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=input_location_geo_json,
            splitter=FileDataSplitter.GEOJSON,
            carryInputData=True,
            partitions=4,
        )
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 1001
        assert spatial_rdd.boundaryEnvelope is not None
        assert (
            spatial_rdd.rawSpatialRDD.take(1)[0].getUserData()
            == "01\t077\t011501\t5\t1500000US010770115015\t010770115015\t5\tBG\t6844991\t32636"
        )
        assert (
            spatial_rdd.rawSpatialRDD.take(2)[1].getUserData()
            == "01\t045\t021102\t4\t1500000US010450211024\t010450211024\t4\tBG\t11360854\t0"
        )
        assert spatial_rdd.fieldNames == [
            "STATEFP",
            "COUNTYFP",
            "TRACTCE",
            "BLKGRPCE",
            "AFFGEOID",
            "GEOID",
            "NAME",
            "LSAD",
            "ALAND",
            "AWATER",
        ]

    def test_wkt_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=input_location_wkt,
            splitter=FileDataSplitter.WKT,
            carryInputData=True,
        )

        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 103
        assert spatial_rdd.boundaryEnvelope is not None
        assert (
            spatial_rdd.rawSpatialRDD.take(1)[0].getUserData()
            == "31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168"
        )

    def test_wkb_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=input_location_wkb,
            splitter=FileDataSplitter.WKB,
            carryInputData=True,
        )
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 103
        assert spatial_rdd.boundaryEnvelope is not None
        assert (
            spatial_rdd.rawSpatialRDD.take(1)[0].getUserData()
            == "31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168"
        )

    def test_mbr(self):
        polygon_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=FileDataSplitter.CSV,
            carryInputData=True,
            partitions=num_partitions,
        )

        rectangle_rdd = polygon_rdd.MinimumBoundingRectangle()

        result = rectangle_rdd.rawSpatialRDD.collect()

        assert result.__len__() > -1
