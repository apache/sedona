#!/usr/bin/env python
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

"""
Test script to reproduce issue #2240:
Error when reading back nested geometry from Parquet with pyspark 3.5
"""

import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, LongType
from pyspark.sql import functions as F
from sedona.spark.sql.types import GeometryType
from sedona.spark.config import SedonaContext
from shapely.geometry import Point

def test_nested_geometry_with_array():
    """Test writing and reading nested geometry within an array structure"""

    # Create Spark session with Sedona
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TestNestedGeometry") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()

    sedona = SedonaContext.create(spark)

    print(f"Testing with Spark version: {spark.version}")

    # Create a temporary directory for test output
    temp_dir = tempfile.mkdtemp(prefix="sedona_test_")

    try:
        # Test Case 1: Geometry nested in a simple struct (this should work)
        print("\nTest Case 1: Geometry in struct (no array)...")
        schema1 = StructType([
            StructField("id", LongType(), True),
            StructField("nested_struct", StructType([
                StructField("geom", GeometryType(), True)
            ]), True)
        ])

        data1 = [(1, (Point(1, 2),)), (2, (Point(3, 4),))]
        df1 = sedona.createDataFrame(data1, schema1)

        parquet_path1 = f"{temp_dir}/test_struct.parquet"
        df1.write.mode("overwrite").parquet(parquet_path1)
        df1_read = sedona.read.parquet(parquet_path1)

        print("  Writing and reading nested struct: SUCCESS")
        df1_read.show(truncate=False)

        # Test Case 2: Geometry nested in array of structs (this fails with PySpark 3.5)
        print("\nTest Case 2: Geometry in array of structs...")
        schema2 = StructType([
            StructField("id", LongType(), True),
            StructField("nested_array", ArrayType(
                StructType([
                    StructField("geom", GeometryType(), True)
                ])
            ), True)
        ])

        data2 = [
            (1, [(Point(1, 2),), (Point(3, 4),)]),
            (2, [(Point(5, 6),)])
        ]
        df2 = sedona.createDataFrame(data2, schema2)

        print("  DataFrame created successfully with schema:")
        df2.printSchema()
        df2.show(truncate=False)

        parquet_path2 = f"{temp_dir}/test_array.parquet"
        print(f"  Writing to parquet at: {parquet_path2}")
        df2.write.mode("overwrite").parquet(parquet_path2)
        print("  Write successful")

        print("  Attempting to read back the parquet file...")
        df2_read = sedona.read.parquet(parquet_path2)
        print("  Read successful")

        print("  Schema of read DataFrame:")
        df2_read.printSchema()

        print("  Data from read DataFrame:")
        df2_read.show(truncate=False)

        print("\nTest Case 2: SUCCESS - No error with nested array of geometries")

        # Test Case 3: Using GeoParquet format
        print("\nTest Case 3: Using GeoParquet format...")
        geoparquet_path = f"{temp_dir}/test_array.geoparquet"
        df2.write.mode("overwrite").format("geoparquet").save(geoparquet_path)
        print("  Write to GeoParquet successful")

        df2_geoparquet_read = sedona.read.format("geoparquet").load(geoparquet_path)
        print("  Read from GeoParquet successful")
        df2_geoparquet_read.show(truncate=False)

        print("\nAll tests completed successfully!")

    except Exception as e:
        print(f"\nERROR encountered: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        import traceback
        traceback.print_exc()

        # This is where we expect to see the error with PySpark 3.5
        if "UDT" in str(e) or "UserDefinedType" in str(e):
            print("\nThis appears to be the UDT bug mentioned in issue #2240")
            print("Related to Apache Spark issue SPARK-48942")

    finally:
        # Clean up
        spark.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"\nCleaned up temporary directory: {temp_dir}")

if __name__ == "__main__":
    test_nested_geometry_with_array()