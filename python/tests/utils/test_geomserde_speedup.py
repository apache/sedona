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

import pytest
import shapely
from packaging.version import parse as parse_version
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads as wkt_loads

from sedona.spark.utils import geometry_serde
from sedona.spark.utils import geometry_serde_general


class TestGeomSerdeSpeedup:
    def test_speedup_enabled(self):
        assert geometry_serde.speedup_enabled

    def test_point(self):
        points = [wkt_loads("POINT EMPTY"), Point(10, 20), Point(10, 20, 30)]
        self._test_serde_roundtrip(points)

    def test_linestring(self):
        linestrings = [
            wkt_loads("LINESTRING EMPTY"),
            LineString([(10, 20), (30, 40)]),
            LineString([(10, 20), (30, 40), (50, 60)]),
            LineString([(10, 20, 30), (30, 40, 50), (50, 60, 70)]),
        ]
        self._test_serde_roundtrip(linestrings)

    def test_nan_first_z_serialization_keeps_dimension(self):
        geometry = wkt_loads("LINESTRING Z (0 0 NaN, 1 1 2)")

        buffer = geometry_serde.serialize(geometry)
        coordinate_type = (buffer[0] & 0x0F) >> 1

        assert coordinate_type == geometry_serde_general.CoordinateType.XYZ

    @pytest.mark.parametrize(
        "wkt",
        [
            "POINT EMPTY",
            "POINT Z EMPTY",
            "LINESTRING EMPTY",
            "LINESTRING Z EMPTY",
            "POLYGON EMPTY",
            "POLYGON Z EMPTY",
            "GEOMETRYCOLLECTION (LINESTRING EMPTY)",
            "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING EMPTY, POLYGON EMPTY)",
            "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (LINESTRING EMPTY))",
            "GEOMETRYCOLLECTION Z (POINT Z EMPTY, LINESTRING Z EMPTY, POLYGON Z EMPTY)",
            "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING Z EMPTY, POLYGON Z EMPTY)",
            "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION Z (POINT Z EMPTY, POLYGON Z EMPTY))",
        ],
    )
    def test_empty_geometry_roundtrip_keeps_dimension(self, wkt):
        geometry = wkt_loads(wkt)
        actual = self.serde_roundtrip(geometry)

        # Spatial equality does not distinguish the dimensions of empty geometries.
        assert actual.wkb == geometry.wkb

    @pytest.mark.parametrize("geom_type", ["POINT", "LINESTRING", "POLYGON"])
    @pytest.mark.parametrize(
        "coord_type", [1, 2, 3, 4], ids=["XY", "XYZ", "XYM", "XYZM"]
    )
    def test_decode_empty_geometry_keeps_stored_layout(self, geom_type, coord_type):
        if coord_type in (3, 4) and (
            parse_version(shapely.__version__) < parse_version("2.1")
            or getattr(shapely, "geos_version", (0, 0, 0)) < (3, 12, 0)
        ):
            pytest.skip("M coordinates require Shapely 2.1 and GEOS 3.12 or newer")

        # Build the stored header directly so the serializer cannot hide a decoder bug.
        buffer = geometry_serde_general.generate_header_bytes(
            getattr(geometry_serde_general.GeometryTypeID, geom_type), coord_type, 0
        )
        actual, offset = geometry_serde.deserialize(buffer)
        dimension = ["", "Z", "M", "ZM"][coord_type - 1]
        expected = wkt_loads(f"{geom_type} {dimension} EMPTY")

        assert offset == len(buffer)
        assert actual.wkb == expected.wkb
        assert geometry_serde.serialize(actual) == buffer

    def test_multi_point(self):
        multi_points = [
            wkt_loads("MULTIPOINT EMPTY"),
            MultiPoint([(10, 20)]),
            MultiPoint([(10, 20), (30, 40)]),
            MultiPoint([(10, 20), (30, 40), (50, 60)]),
            MultiPoint([(10, 20, 30), (30, 40, 50), (50, 60, 70)]),
        ]
        self._test_serde_roundtrip(multi_points)

    def test_multi_linestring(self):
        multi_linestrings = [
            wkt_loads("MULTILINESTRING EMPTY"),
            MultiLineString([[(10, 20), (30, 40)]]),
            MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
            MultiLineString(
                [[(10, 20, 30), (30, 40, 50)], [(50, 60, 70), (70, 80, 90)]]
            ),
        ]
        self._test_serde_roundtrip(multi_linestrings)

    def test_polygon(self):
        ext = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        int0 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]
        int1 = [(2, 2), (2, 2.5), (2.5, 2.5), (2.5, 2), (2, 2)]
        polygons = [
            wkt_loads("POLYGON EMPTY"),
            Polygon(ext),
            Polygon(ext, [int0]),
            Polygon(ext, [int0, int1]),
        ]
        self._test_serde_roundtrip(polygons)

    def test_multi_polygon(self):
        ext = [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)]
        int0 = [(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]
        int1 = [(2, 2), (2, 2.5), (2.5, 2.5), (2.5, 2), (2, 2)]
        multi_polygons = [
            wkt_loads("MULTIPOLYGON EMPTY"),
            MultiPolygon([Polygon(ext)]),
            MultiPolygon([Polygon(ext), Polygon(ext, [int0])]),
            MultiPolygon([Polygon(ext), Polygon(ext, [int0, int1])]),
            MultiPolygon(
                [Polygon(ext, [int1]), Polygon(ext), Polygon(ext, [int0, int1])]
            ),
        ]
        self._test_serde_roundtrip(multi_polygons)

    def test_geometry_collection(self):
        geometry_collections = [
            wkt_loads("GEOMETRYCOLLECTION EMPTY"),
            GeometryCollection(
                [Point(10, 20), LineString([(10, 20), (30, 40)]), Point(30, 40)]
            ),
            GeometryCollection(
                [
                    MultiPoint([(10, 20), (30, 40)]),
                    MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
                    MultiPolygon(
                        [
                            Polygon(
                                [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)],
                                [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]],
                            )
                        ]
                    ),
                    Point(100, 200),
                ]
            ),
            GeometryCollection(
                [
                    GeometryCollection(
                        [Point(10, 20), LineString([(10, 20), (30, 40)]), Point(30, 40)]
                    ),
                    GeometryCollection(
                        [
                            MultiPoint([(10, 20), (30, 40)]),
                            MultiLineString(
                                [[(10, 20), (30, 40)], [(50, 60), (70, 80)]]
                            ),
                            Point(10, 20),
                        ]
                    ),
                ]
            ),
        ]
        self._test_serde_roundtrip(geometry_collections)

    @pytest.mark.skipif(
        shapely.__version__ < "2", reason="SRID functions require Shapely >= 2.0"
    )
    def test_srid_roundtrip(self):
        point = wkt_loads("POINT (1 2)")
        point = shapely.set_srid(point, 1000)
        point2 = TestGeomSerdeSpeedup.serde_roundtrip(point)
        assert shapely.get_srid(point2) == 1000

    @pytest.mark.skipif(
        parse_version(shapely.__version__) < parse_version("2.1")
        or getattr(shapely, "geos_version", (0, 0, 0)) < (3, 12, 0),
        reason="M coordinates require Shapely 2.1 and GEOS 3.12 or newer",
    )
    @pytest.mark.parametrize(
        "wkt",
        [
            "POINT M (1 2 3)",
            "POINT ZM (1 2 3 4)",
            "POINT M EMPTY",
            "POINT ZM EMPTY",
            "LINESTRING M (0 0 1, 2 3 4)",
            "LINESTRING ZM (0 0 1 2, 3 4 5 6)",
            "LINESTRING M EMPTY",
            "LINESTRING ZM EMPTY",
            "POLYGON M ((0 0 1, 2 0 2, 0 2 3, 0 0 1))",
            "POLYGON M EMPTY",
            "POLYGON ZM EMPTY",
            "GEOMETRYCOLLECTION ZM (POINT ZM EMPTY, LINESTRING ZM EMPTY, POLYGON ZM EMPTY)",
            "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4), "
            "LINESTRING ZM (0 0 1 2, 3 4 5 6))",
        ],
    )
    def test_m_roundtrip(self, wkt):
        geometry = shapely.from_wkt(wkt)
        actual = TestGeomSerdeSpeedup.serde_roundtrip(geometry)

        assert shapely.to_wkt(actual) == shapely.to_wkt(geometry)
        assert actual.has_z == geometry.has_z
        assert actual.has_m == geometry.has_m
        assert actual.wkb == geometry.wkb

    @pytest.mark.skipif(
        parse_version(shapely.__version__) < parse_version("2.1")
        or getattr(shapely, "geos_version", (0, 0, 0)) < (3, 12, 0),
        reason="M coordinates require Shapely 2.1 and GEOS 3.12 or newer",
    )
    @pytest.mark.parametrize("wkt", ["POINT M (1 2 3)", "POINT ZM (1 2 3 4)"])
    def test_general_serializer_rejects_m_instead_of_losing_it(self, wkt):
        geometry = shapely.from_wkt(wkt)

        with pytest.raises(ValueError, match="requires geomserde_speedup"):
            geometry_serde_general.serialize(geometry)

    @pytest.mark.parametrize(
        "coord_type",
        [
            geometry_serde_general.CoordinateType.XYM,
            geometry_serde_general.CoordinateType.XYZM,
        ],
    )
    def test_general_deserializer_rejects_m_instead_of_losing_it(self, coord_type):
        buffer = geometry_serde_general.create_buffer_for_geom(
            geometry_serde_general.GeometryTypeID.POINT,
            coord_type,
            8 + geometry_serde_general.CoordinateType.bytes_per_coord(coord_type),
            1,
        )

        with pytest.raises(ValueError, match="requires geomserde_speedup"):
            geometry_serde_general.deserialize(buffer)

    def test_general_serializer_does_not_query_m_on_older_geos(self, monkeypatch):
        def fail_if_queried(_geometry):
            raise AssertionError("has_m should not be queried with GEOS < 3.12")

        monkeypatch.setattr(shapely, "geos_version", (3, 11, 0), raising=False)
        monkeypatch.setattr(
            BaseGeometry, "has_m", property(fail_if_queried), raising=False
        )
        monkeypatch.setattr(
            geometry_serde_general, "serialize_point", lambda _geometry: b"xy"
        )

        buffer = geometry_serde_general.serialize(Point(1, 2))

        assert buffer == b"xy"

    @staticmethod
    def _test_serde_roundtrip(geoms):
        for geom in geoms:
            geom_actual = TestGeomSerdeSpeedup.serde_roundtrip(geom)
            assert geom_actual.equals_exact(geom, 1e-6)
            assert geom.wkt == geom_actual.wkt

    @staticmethod
    def serde_roundtrip(geom: BaseGeometry) -> BaseGeometry:
        buffer = geometry_serde.serialize(geom)
        geom2, offset = geometry_serde.deserialize(buffer)
        return geom2
