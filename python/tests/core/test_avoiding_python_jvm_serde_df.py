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
from pyspark.sql.types import StructField, StructType

from sedona.core.SpatialRDD import CircleRDD
from sedona.core.enums import GridType, IndexType
from sedona.core.formatMapper import WktReader
from sedona.core.spatialOperator.join_params import JoinParams
from sedona.core.spatialOperator.join_query_raw import JoinQueryRaw
from sedona.core.spatialOperator.range_query_raw import RangeQueryRaw
from sedona.sql.types import GeometryType
from sedona.utils.adapter import Adapter
from tests.test_base import TestBase

import os

from tests.tools import tests_resource
from shapely.wkt import loads

bank_csv_path = os.path.join(tests_resource, "small/points.csv")
areas_csv_path = os.path.join(tests_resource, "small/areas.csv")


class TestOmitPythonJvmSerdeToDf(TestBase):
    expected_pois_within_areas_ids = [['4', '4'], ['1', '6'], ['2', '1'], ['3', '3'], ['3', '7']]

    def test_spatial_join_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.spatialJoin(poi_point_rdd, areas_polygon_rdd, JoinParams(considerBoundaryIntersection=True))
        sedona_df = Adapter.toDf(jvm_sedona_rdd, ["area_id", "area_name"], ["poi_id", "poi_name"], self.spark)

        assert sedona_df.count() == 5
        assert sedona_df.columns == ["leftgeometry", "area_id", "area_name", "rightgeometry",
                                     "poi_id", "poi_name"]

    def test_distance_join_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        circle_rdd = CircleRDD(poi_point_rdd, 2.0)

        circle_rdd.analyze()
        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.DistanceJoinQueryFlat(poi_point_rdd, circle_rdd, False, True)
        df_sedona_rdd = Adapter.toDf(
            jvm_sedona_rdd,
            ["poi_from_id", "poi_from_name"],
            ["poi_to_id", "poi_to_name"],
            self.spark
        )

        assert df_sedona_rdd.count() == 10
        assert df_sedona_rdd.columns == [
            "leftgeometry",
            "poi_from_id",
            "poi_from_name",
            "rightgeometry",
            "poi_to_id",
            "poi_to_name"
        ]

    def test_spatial_join_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.SpatialJoinQueryFlat(
            poi_point_rdd, areas_polygon_rdd, False, True)

        pois_within_areas_with_default_column_names = Adapter.toDf(jvm_sedona_rdd, self.spark)

        assert pois_within_areas_with_default_column_names.count() == 5

        pois_within_areas_with_passed_column_names = Adapter.toDf(
            jvm_sedona_rdd,
            ["area_id", "area_name"],
            ["poi_id", "poi_name"],
            self.spark
        )

        assert pois_within_areas_with_passed_column_names.count() == 5

        assert pois_within_areas_with_passed_column_names.columns == ["leftgeometry", "area_id", "area_name",
                                                                      "rightgeometry",
                                                                      "poi_id", "poi_name"]

        assert pois_within_areas_with_default_column_names.schema == StructType(
            [
                StructField("leftgeometry", GeometryType()),
                StructField("rightgeometry", GeometryType()),
            ]
        )

        left_geometries_raw = pois_within_areas_with_default_column_names. \
            selectExpr("ST_AsText(leftgeometry)"). \
            collect()

        left_geometries = self.__row_to_list(left_geometries_raw)

        right_geometries_raw = pois_within_areas_with_default_column_names. \
            selectExpr("ST_AsText(rightgeometry)"). \
            collect()

        right_geometries = self.__row_to_list(right_geometries_raw)

        # Ignore the ordering of these
        assert set(geom[0] for geom in left_geometries) == set([
            'POLYGON ((0 4, -3 3, -8 6, -6 8, -2 9, 0 4))',
            'POLYGON ((10 3, 10 6, 14 6, 14 3, 10 3))',
            'POLYGON ((2 2, 2 4, 3 5, 7 5, 9 3, 8 1, 4 1, 2 2))',
            'POLYGON ((-1 -1, -1 -3, -2 -5, -6 -8, -5 -2, -3 -2, -1 -1))',
            'POLYGON ((-1 -1, -1 -3, -2 -5, -6 -8, -5 -2, -3 -2, -1 -1))'
        ])
        assert set(geom[0] for geom in right_geometries) == set([
            'POINT (-3 5)',
            'POINT (11 5)',
            'POINT (4 3)',
            'POINT (-1 -1)',
            'POINT (-4 -5)'
        ])

    def test_range_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)

        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        poi_point_rdd.buildIndex(IndexType.QUADTREE, False)

        result = RangeQueryRaw.SpatialRangeQuery(
            poi_point_rdd, loads("POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))"), True, True
        )

        rdd = result.to_rdd()

        assert rdd.collect().__len__() == 4

        df_without_column_names = Adapter.toDf(result, self.spark)

        raw_geometries = self.__row_to_list(
            df_without_column_names.collect()
        )

        assert [point[0].wkt for point in raw_geometries] == [
            'POINT (9 8)', 'POINT (4 3)', 'POINT (12 1)', 'POINT (11 5)'
        ]
        assert df_without_column_names.count() == 4
        assert df_without_column_names.schema == StructType([StructField("geometry", GeometryType())])

        df = Adapter.toDf(result, self.spark, ["poi_id", "poi_name"])

        assert df.count() == 4
        assert df.columns == ["geometry", "poi_id", "poi_name"]

    def __row_to_list(self, row_list):
        return [[*element] for element in row_list]
