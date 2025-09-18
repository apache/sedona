#!/usr/bin/env python
"""
Test script for issue #2240 using local development version
"""

import sys
import os

# Add the python module to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, struct, expr, col
from sedona.spark import SedonaContext
import tempfile

# Create Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TestNestedGeometry") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

# Initialize Sedona
sedona = SedonaContext.create(spark)

print(f"Testing with Spark version: {spark.version}")
print(f"Python version: {sys.version}")

# Create test data
print("\n=== Creating DataFrame with nested geometry in array ===")
df = spark.createDataFrame([{'id': 'something'}]).withColumn(
    "nested",
    array(struct(expr("ST_Point(0, 0)").alias("geometry")))
)

print("Schema:")
df.printSchema()

print("Data:")
df.show(truncate=False)

# Use temp directory for test
temp_dir = tempfile.mkdtemp(prefix="sedona_test_")
parquet_path = f"{temp_dir}/nested_geometry_test"

try:
    # Write to Parquet
    print(f"\n=== Writing to Parquet: {parquet_path} ===")
    df.write.mode("overwrite").parquet(parquet_path)
    print("Write successful")

    # Attempt to read back
    print("\n=== Reading from Parquet ===")
    df_read = spark.read.parquet(parquet_path)
    print("Read successful")

    print("Schema of read DataFrame:")
    df_read.printSchema()

    print("Attempting to collect data...")
    result = df_read.collect()
    print("Collect successful!")
    print(f"Result: {result}")

    # Try to access the nested geometry
    print("\n=== Accessing nested geometry ===")
    for row in result:
        print(f"ID: {row['id']}")
        print(f"Nested array: {row['nested']}")
        if row['nested'] and len(row['nested']) > 0:
            print(f"First element: {row['nested'][0]}")
            print(f"Geometry in first element: {row['nested'][0]['geometry']}")

except Exception as e:
    print(f"\n!!! ERROR ENCOUNTERED !!!")
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {str(e)}")

    # Check if this is the UDT issue
    if "UDT" in str(e) or "UserDefinedType" in str(e) or "does not exist" in str(e):
        print("\n=== This appears to be the UDT bug from issue #2240 ===")
        print("The error occurs when PySpark 3.5 tries to deserialize")
        print("geometry objects nested within array structures.")
        print("This is related to Apache Spark issue SPARK-48942")

    import traceback
    print("\nFull traceback:")
    traceback.print_exc()

finally:
    spark.stop()
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up: {temp_dir}")