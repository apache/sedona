#!/usr/bin/env python
"""
Reproduction script for issue #2240:
Error when reading back nested geometry from Parquet with pyspark 3.5

This script reproduces the exact error reported in the issue.
"""

from sedona.spark import SedonaContext
from pyspark.sql.functions import array, struct, expr

# Create Spark session with Sedona
config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-3.5_2.12:1.7.2,"
        "org.datasyslab:geotools-wrapper:1.7.2-28.5",
    )
    .config(
        "spark.jars.repositories",
        "https://artifacts.unidata.ucar.edu/repository/unidata-all",
    )
    .getOrCreate()
)

spark = SedonaContext.create(config)

print(f"Testing with Spark version: {spark.version}")
print("Creating DataFrame with nested geometry in array...")

# Create DataFrame with nested geometry in array structure
df = spark.createDataFrame([{'id': 'something'}]).withColumn(
    "nested",
    array(struct(expr("ST_Point(0, 0)").alias("geometry")))
)

print("DataFrame schema:")
df.printSchema()
print("DataFrame content:")
df.show()

# Write to Parquet
print("\nWriting to Parquet...")
df.write.mode("overwrite").parquet("/tmp/test_issue_2240")
print("Write successful")

# Attempt to read back - this fails with PySpark 3.5
print("\nReading from Parquet...")
try:
    df_read = spark.read.parquet("/tmp/test_issue_2240")
    print("Read successful")
    print("Collecting data...")
    result = df_read.collect()
    print("Result:", result)
except Exception as e:
    print(f"\nERROR: {type(e).__name__}")
    print(f"Message: {str(e)}")
    print("\nThis is the expected error with PySpark 3.5")
    print("The issue is related to UDT (UserDefinedType) handling in nested arrays")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()