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

import geopandas as gpd
import pytest
from shapely import wkb
import struct
from pyspark.sql import types as T
from shapely.geometry import Point

from sedona.spark.geoarrow.geoarrow import dataframe_to_arrow
from sedona.spark.maps.SedonaMapUtils import SedonaMapUtils
from sedona.spark.sql import types
from tests.test_base import TestBase


@pytest.fixture(params=[False, True], ids=["default", "explicit-udt"])
def spatial_frame(request):
    legacy = request.param
    dtype = types.LegacyGeometryType() if legacy else types.geometry_type()
    shape = wkb.loads(wkb.dumps(Point(1, 2), srid=4326))
    value = shape if legacy else types.to_spark_geometry(shape)
    return TestBase.spark.createDataFrame(
        [(0, value), (1, None)],
        T.StructType(
            [
                T.StructField("id", T.IntegerType(), False),
                T.StructField("shape", dtype, True),
            ]
        ),
        verifySchema=False,
    )


def test_geoarrow_recognizes_explicit_udt(spatial_frame):
    import pyarrow as pa

    table = dataframe_to_arrow(spatial_frame.orderBy("id"))
    assert table.column_names == ["id", "shape"]
    array = table["shape"].combine_chunks()
    if isinstance(array.type, pa.ExtensionType):
        assert array.type.extension_name == "geoarrow.wkb"
        array = array.storage
    else:
        assert (
            table.schema.field("shape").metadata[b"ARROW:extension:name"]
            == b"geoarrow.wkb"
        )
    values = array.to_pylist()
    assert values[1] is None
    shape = wkb.loads(values[0])
    assert shape.equals(Point(1, 2))
    assert (
        struct.unpack_from(
            "<i", wkb.dumps(shape, big_endian=False, include_srid=True), 5
        )[0]
        == 4326
    )


def test_map_helpers_recognize_explicit_udt(spatial_frame):
    result = SedonaMapUtils.__convert_to_gdf_or_pdf__(spatial_frame.orderBy("id"))
    assert isinstance(result, gpd.GeoDataFrame)
    assert result.geometry.name == "geometry"
    assert result.geometry.iloc[0].equals(Point(1, 2))
    assert result.geometry.iloc[1] is None


def test_geopandas_recognizes_explicit_udt(spatial_frame):
    from sedona.spark import geopandas as sgpd

    frame = sgpd.GeoDataFrame(spatial_frame, geometry="shape")
    series = frame["shape"]
    assert isinstance(series, sgpd.GeoSeries)
    assert series.crs.to_epsg() == 4326
    local = frame.to_geopandas().sort_values("id")
    assert local.geometry.iloc[0].equals(Point(1, 2))
    assert local.geometry.iloc[1] is None
    assert local.crs.to_epsg() == 4326
    assert frame.to_wkt().to_pandas().sort_values("id")["shape"].tolist() == [
        "POINT (1 2)",
        None,
    ]
    encoded = frame.to_wkb().to_pandas().sort_values("id")["shape"].tolist()
    assert bytes(encoded[0]) == bytes.fromhex(
        "0101000000000000000000F03F0000000000000040"
    )
    assert encoded[1] is None


@pytest.mark.skipif(
    not types.USES_NATIVE_SPATIAL_TYPES, reason="requires native/UDT coexistence"
)
@pytest.mark.parametrize(
    "layout,coordinates",
    [
        (0, (1.0, 2.0)),
        (1000, (1.0, 2.0, 3.0)),
        (2000, (1.0, 2.0, 3.0)),
        (3000, (1.0, 2.0, 3.0, 4.0)),
    ],
)
def test_udt_scalar_access_preserves_signed_srid_and_iso_layout(layout, coordinates):
    import shapely

    if layout >= 2000 and (
        not hasattr(shapely, "has_m") or shapely.geos_version < (3, 12, 0)
    ):
        pytest.skip("Shapely cannot retain M")
    from sedona.spark.geopandas._geometry import geometry_srid, geometry_wkb
    from pyspark.sql import functions as F

    payload = struct.pack("<BI", 1, layout + 1) + struct.pack(
        "<" + "d" * len(coordinates), *coordinates
    )
    dtype = types.LegacyGeometryType()
    srids = [-(2**31), -1, 0, 4326, 2**31 - 1]
    values = [
        (i, dtype.deserialize(struct.pack("<i", srid) + payload))
        for i, srid in enumerate(srids)
    ]
    values.append((len(values), None))
    frame = TestBase.spark.createDataFrame(
        values,
        T.StructType([T.StructField("id", T.IntegerType()), T.StructField("g", dtype)]),
        verifySchema=False,
    )
    result = (
        frame.select(
            "id",
            geometry_srid(F.col("g"), dtype).alias("srid"),
            geometry_wkb(F.col("g"), dtype).alias("wkb"),
        )
        .orderBy("id")
        .collect()
    )
    assert [row.srid for row in result] == srids + [None]
    assert [bytes(row.wkb) if row.wkb is not None else None for row in result] == [
        payload
    ] * len(srids) + [None]


@pytest.mark.skipif(
    not types.USES_NATIVE_SPATIAL_TYPES, reason="requires native/UDT coexistence"
)
def test_mixed_native_and_udt_columns_keep_values_and_crs():
    from sedona.spark import geopandas as sgpd

    primary = wkb.loads(wkb.dumps(Point(1, 2), srid=3857))
    secondary = wkb.loads(wkb.dumps(Point(-73, 40), srid=4326))
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("shape", types.geometry_type()),
            T.StructField("secondary", types.LegacyGeometryType()),
        ]
    )
    source = TestBase.spark.createDataFrame(
        [(0, types.to_spark_geometry(primary), secondary), (1, None, None)],
        schema,
        verifySchema=False,
    )
    frame = sgpd.GeoDataFrame(source, geometry="shape")
    result = frame.to_geopandas().sort_values("id")
    assert result.shape == (2, 3)
    assert result["shape"].crs.to_epsg() == 3857
    assert result["secondary"].crs.to_epsg() == 4326
    assert result["secondary"].iloc[0].equals(Point(-73, 40))
    assert result["secondary"].iloc[1] is None
    assert isinstance(frame["secondary"].spark.data_type, types.LegacyGeometryType)
    assert isinstance(source.schema["shape"].dataType, types.GeometryType)
    table = dataframe_to_arrow(source.orderBy("id"))
    assert table.column_names == ["id", "shape", "secondary"]
    for name in ["shape", "secondary"]:
        field = table.schema.field(name)
        assert (
            getattr(field.type, "extension_name", None) == "geoarrow.wkb"
            or field.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
        )
