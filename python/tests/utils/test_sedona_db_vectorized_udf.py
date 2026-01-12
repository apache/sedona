import time

import numpy as np

from sedona.spark.sql.functions import sedona_db_vectorized_udf
from sedona.spark.utils.udf import to_sedona_func, from_sedona_func
from tests.test_base import TestBase
import pyarrow as pa
import shapely
from sedona.sql import GeometryType
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import DoubleType, IntegerType, ByteType
from sedona.spark.sql import ST_X
from shapely._enum import ParamEnum

def test_m():
    on_invalid="raise"
    wkb = b'\x12\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'
    geometry = np.asarray([wkb, wkb], dtype=object)

    DecodingErrorOptions = ParamEnum(
        "DecodingErrorOptions", {"ignore": 0, "warn": 1, "raise": 2, "fix": 3}
    )

    # print("sss")


    # <class 'numpy.ndarray'>
    # object
    #   C_CONTIGUOUS : True
    #   F_CONTIGUOUS : True
    #   OWNDATA : False
    #   WRITEABLE : True
    #   ALIGNED : True
    #   WRITEBACKIFCOPY : False
    # print(type(geometry))
    # print(geometry.dtype)
    # print(geometry.flags)

    result = from_sedona_func(geometry)

    result2 = to_sedona_func(result)

# ensure the input has object dtype, to avoid numpy inferring it as a
# fixed-length string dtype (which removes trailing null bytes upon access
# of array elements)
    #
    # def from_sedona_func(arr):
    #     try:
    #         from . import sedonaserde_vectorized_udf_module
    #         print(sedonaserde_vectorized_udf_module.from_sedona_func_3(arr))
    #     except Exception as e:
    #         print("Cannot import sedonaserde_vectorized_udf_module:")
    #         print(e)
    # # print()
    # return None
#
# def from_wkb(geometry, on_invalid="raise", **kwargs):
#     r"""Create geometries from the Well-Known Binary (WKB) representation.
#
#     The Well-Known Binary format is defined in the `OGC Simple Features
#     Specification for SQL <https://www.opengeospatial.org/standards/sfs>`__.
#
#     Parameters
#     ----------
#     geometry : str or array_like
#         The WKB byte object(s) to convert.
#     on_invalid : {"raise", "warn", "ignore", "fix"}, default "raise"
#         Indicates what to do when an invalid WKB is encountered. Note that the
#         validations involved are very basic, e.g. the minimum number of points
#         for the geometry type. For a thorough check, use :func:`is_valid` after
#         conversion to geometries. Valid options are:
#
#         - raise: an exception will be raised if any input geometry is invalid.
#         - warn: a warning will be raised and invalid WKT geometries will be
#           returned as ``None``.
#         - ignore: invalid geometries will be returned as ``None`` without a
#           warning.
#         - fix: an effort is made to fix invalid input geometries (currently just
#           unclosed rings). If this is not possible, they are returned as
#           ``None`` without a warning. Requires GEOS >= 3.11.
#
#           .. versionadded:: 2.1.0
#     **kwargs
#         See :ref:`NumPy ufunc docs <ufuncs.kwargs>` for other keyword arguments.
#
#     Examples
#     --------
#     >>> import shapely
#     >>> shapely.from_wkb(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?')
#     <POINT (1 1)>
#
#     """  # noqa: E501
#     if not np.isscalar(on_invalid):
#         raise TypeError("on_invalid only accepts scalar values")
#
#     invalid_handler = np.uint8(DecodingErrorOptions.get_value(on_invalid))
#
#     # ensure the input has object dtype, to avoid numpy inferring it as a
#     # fixed-length string dtype (which removes trailing null bytes upon access
#     # of array elements)
#     geometry = np.asarray(geometry, dtype=object)
#     return lib.from_wkb(geometry, invalid_handler, **kwargs)

class TestSedonaDBArrowFunction(TestBase):
    def test_vectorized_udf(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[GeometryType(), IntegerType()])
        def my_own_function(geom, distance):
            geom_wkb = pa.array(geom.storage.to_array())
            distance = pa.array(distance.to_array())
            geom = shapely.from_wkb(geom_wkb)
            result_shapely = shapely.centroid(geom)

            return pa.array(shapely.to_wkb(result_shapely))

        df = self.spark.createDataFrame(
            [
                (1, "POINT (1 1)"),
                (2, "POINT (2 2)"),
                (3, "POINT (3 3)"),
            ],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        df.select(ST_X(my_own_function(df.wkt, lit(100)).alias("geom"))).show()

    def test_geometry_to_double(self):
        @sedona_db_vectorized_udf(return_type=DoubleType(), input_types=[GeometryType()])
        def geometry_to_non_geometry_udf(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            result_shapely = shapely.get_x(shapely.centroid(geom))

            return pa.array(result_shapely, pa.float64())

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        values = df.select(geometry_to_non_geometry_udf(df.wkt).alias("x_coord")) \
            .collect()

        values_list = [row["x_coord"] for row in values]

        assert values_list == [1.0, 2.0, 3.0]

    def test_geometry_to_int(self):
        @sedona_db_vectorized_udf(return_type=IntegerType(), input_types=[GeometryType()])
        def geometry_to_int(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            result_shapely = shapely.get_num_points(geom)

            return pa.array(result_shapely, pa.int32())

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        values = df.select(geometry_to_int(df.wkt)) \
            .collect()

        values_list = [row[0] for row in values]

        assert values_list == [0, 0, 0]

    def test_geometry_crs_preservation(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[GeometryType()])
        def return_same_geometry(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            return pa.array(shapely.to_wkb(geom))

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_SetSRID(ST_GeomFromWKT(wkt), 3857)"))

        result_df = df.select(return_same_geometry(df.wkt).alias("geom"))

        crs_list = result_df.selectExpr("ST_SRID(geom)").rdd.flatMap(lambda x: x).collect()

        assert crs_list == [3857, 3857, 3857]

    def test_geometry_to_geometry(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[ByteType()])
        def buffer_geometry(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geometry_array = np.asarray(geom_wkb, dtype=object)
            geom = from_sedona_func(geometry_array)

            result_shapely = shapely.buffer(geom, 10)

            return pa.array(to_sedona_func(result_shapely))

        df = self.spark.read.\
            format("geoparquet").\
            load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/book/source_data/transportation_barcelona_dupl_l1")

        # 1 045 770
        # print(df.count())

        # df.unionAll(df).unionAll(df).unionAll(df).unionAll(df).unionAll(df).\
        #     unionAll(df).unionAll(df).unionAll(df).unionAll(df).unionAll(df).\
        #     write.format("geoparquet").mode("overwrite").save("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/book/source_data/transportation_barcelona_dupl_l2")
        # 18 24
        # df.union(df).union(df).union(df).union(df).union(df).union(df).\
        #     write.format("geoparquet").mode("overwrite").save("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings_large_3")

        values = df.select(buffer_geometry(df.geometry).alias("geometry")).\
            selectExpr("ST_Area(geometry) as area").\
            selectExpr("Sum(area) as total_area")

        values.show()

        # for _ in range(4):
        #     start_time = time.time()
        #     values.show()
        #     end_time = time.time()
        #     print(f"Execution time: {end_time - start_time} seconds")

    def test_geometry_to_geometry_normal_udf(self):
        from pyspark.sql.functions import udf

        def create_buffer(geom):
            return geom.buffer(10)

        create_buffer_udf = udf(create_buffer, GeometryType())

        df = self.spark.read. \
            format("geoparquet"). \
            load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/book/source_data/transportation_barcelona_dupl_l2")

        # print(df.count())
        # df.limit(10).collect()
        values = df.select(create_buffer_udf(df.geometry).alias("geometry")). \
            selectExpr("ST_Area(geometry) as area"). \
            selectExpr("Sum(area) as total_area")

        values.show()

        # for _ in range(4):
        #     start_time = time.time()
        #     values.show()
        #     end_time = time.time()
        #     print(f"Execution time: {end_time - start_time} seconds")
# 1 045 770
