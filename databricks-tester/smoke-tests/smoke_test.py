#!/usr/bin/env python3
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
Sedona Smoke Test Script for Databricks

This script contains very basic smoke tests for Apache Sedona functionality
on Databricks clusters. It can be run as a regular Python script with proper
error handling and reporting.
"""

import argparse
import os
import shutil
import sys
import traceback
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand


class SedonaSmokeTest:
    """Sedona smoke test runner"""

    def __init__(self, session_id: Optional[str] = None):
        self.spark: Optional[SparkSession] = None
        self.test_results: List[Tuple[str, bool, str]] = []
        self.session_id = session_id or "default"

    def setup(self) -> bool:
        """Initialize Spark session and basic setup."""
        try:
            self.spark = SparkSession.builder.getOrCreate()
            if self.spark is not None:
                print(f"✓ Spark session initialized (version: {self.spark.version})")
                return True
            else:
                print("✗ Failed to create Spark session")
                return False
        except Exception as e:
            print(f"✗ Failed to initialize Spark session: {e}")
            return False

    def run_test(self, test_name: str, test_func) -> None:
        """Run a single test with error handling."""
        print(f"\n--- Running {test_name} ---")
        try:
            test_func()
            print(f"✓ {test_name}: PASSED")
            self.test_results.append((test_name, True, ""))
        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            print(f"✗ {test_name}: FAILED")
            print(f"Error: {e}")
            self.test_results.append((test_name, False, error_msg))

    def test_basic_st_functions(self) -> None:
        """Test basic ST functions."""
        # Test basic ST functions
        test_df = self.spark.sql(
            """
            SELECT
                ST_Point(1.0, 2.0) as point1,
                ST_Point(3.0, 4.0) as point2
        """
        )

        # Verify we can collect results
        results = test_df.selectExpr("ST_AsText(point1)", "ST_AsEWKB(point2)").collect()
        assert len(results) == 1, "Expected 1 row from basic ST functions test"

        # Test ST_Distance
        distance_df = self.spark.sql(
            """
            SELECT
                ST_Distance(ST_Point(0.0, 0.0), ST_Point(3.0, 4.0)) as distance
        """
        )

        distance_row = distance_df.collect()[0]
        distance_result = distance_row["distance"]
        expected_distance = 5.0  # 3-4-5 triangle
        assert (
            abs(distance_result - expected_distance) < 0.001
        ), f"Expected distance ~5.0, got {distance_result}"
        print(f"  Distance calculation: {distance_result} (expected ~5.0)")

        # Test advanced geometric operations
        test_df = self.spark.sql(
            """
            SELECT
                ST_Area(ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')) as square_area,
                ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)')) as line_length,
                ST_Intersects(
                    ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
                    ST_GeomFromText('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
                ) as polygons_intersect
        """
        )

        result_row = test_df.collect()[0]

        # Verify results
        area = result_row["square_area"]
        length = result_row["line_length"]
        intersects = result_row["polygons_intersect"]

        assert abs(area - 25.0) < 0.001, f"Square area should be 25, got {area}"
        assert abs(length - 5.0) < 0.001, f"Line length should be 5, got {length}"
        assert intersects == True, f"Polygons should intersect, got {intersects}"

        print(f"  Square area: {area}")
        print(f"  Line length: {length}")
        print(f"  Polygons intersect: {intersects}")

    def test_spatial_operations(self) -> None:
        """Test spatial operations like ST_Buffer and ST_Contains."""
        buffer_test = self.spark.sql(
            """
            SELECT
                ST_Contains(
                    ST_Buffer(ST_Point(0.0, 0.0), 10.0),
                    ST_Point(3.0, 4.0)
                ) as point_in_buffer
        """
        )

        result_row = buffer_test.collect()[0]
        result = result_row["point_in_buffer"]
        assert (
            result == True
        ), f"Point (3,4) should be in buffer of radius 10 around origin, got {result}"

        print(f"  Buffer contains test: {result}")

    def test_spatial_join(self) -> None:
        """Test spatial join with generated data."""
        # Create test data using spark.range
        points_df = (
            self.spark.range(1000)
            .select(col("id"), (rand() * 10).alias("x"), (rand() * 10).alias("y"))
            .select(col("id"), expr("ST_Point(x, y)").alias("geometry"))
        )

        # Create a polygon for spatial join
        polygon_df = (
            self.spark.range(100)
            .select(col("id"), (rand() * 10).alias("x"), (rand() * 10).alias("y"))
            .select(
                col("id"),
                expr("ST_PolygonFromEnvelope(x, y, x + 1, y + 1)").alias("polygon"),
            )
        )

        # Register temp views
        points_df.createOrReplaceTempView("test_points")
        polygon_df.createOrReplaceTempView("test_polygon")

        # Perform spatial join
        for threshold in ["-1", None]:
            if threshold:
                print(f"  spark.sedona.join.autoBroadcastJoinThreshold: {threshold}")
                self.spark.conf.set(
                    "spark.sedona.join.autoBroadcastJoinThreshold", "-1"
                )
            else:
                print(f"  spark.sedona.join.autoBroadcastJoinThreshold: unset")
                self.spark.conf.unset("spark.sedona.join.autoBroadcastJoinThreshold")

            df_spatial_join = self.spark.sql(
                """
                SELECT p.id point_id, poly.id poly_id
                FROM test_points p, test_polygon poly
                WHERE ST_Contains(poly.polygon, p.geometry)
            """
            )
            df_spatial_join.explain(extended=True)

            results = df_spatial_join.collect()
            count = len(results)
            assert count >= 0, f"Point count should be non-negative, got {count}"

            print(f"  Points in polygon: {count}/1000")

        # Clean up temp views
        self.spark.catalog.dropTempView("test_points")
        self.spark.catalog.dropTempView("test_polygon")

    def test_geoparquet_io(self) -> None:
        """Test GeoParquet I/O operations."""
        # Use DBFS path with session_id for better isolation
        output_path = (
            f"dbfs:/tmp/sedona-smoke-test/{self.session_id}/sedona_test_geoparquet"
        )

        # Create sample spatial data
        sample_data = self.spark.range(100).select(
            col("id"),
            expr("ST_Point(rand() * 360 - 180, rand() * 180 - 90)").alias("geometry"),
            expr("concat('Location_', id)").alias("name"),
        )

        # Test writing to DBFS
        sample_data.write.mode("overwrite").format("geoparquet").save(output_path)

        # Test reading from DBFS
        read_back = self.spark.read.format("geoparquet").load(output_path)
        count = read_back.count()

        assert count == 100, f"Expected 100 records, got {count}"

        print(f"  Records written and read: {count}")
        print(f"  Test data path: {output_path}")
        read_back.show(5)

    def test_geojson_io(self) -> None:
        """Test GeoJSON I/O operations."""
        # Use DBFS path with session_id for better isolation
        output_path = (
            f"dbfs:/tmp/sedona-smoke-test/{self.session_id}/sedona_test_geojson"
        )

        # Create sample spatial data with different geometry types
        sample_data = self.spark.range(50).select(
            col("id"),
            expr(
                "CASE "
                "WHEN id % 4 = 0 THEN ST_Point(rand() * 360 - 180, rand() * 180 - 90) "
                "WHEN id % 4 = 1 THEN ST_MakeLine(array(ST_Point(rand() * 10, rand() * 10), ST_Point(rand() * 10 + 10, rand() * 10 + 10))) "
                "WHEN id % 4 = 2 THEN ST_PolygonFromEnvelope(rand() * 10, rand() * 10, rand() * 10 + 5, rand() * 10 + 5) "
                "ELSE ST_Buffer(ST_Point(rand() * 10, rand() * 10), 2.0) "
                "END"
            ).alias("geometry"),
            expr("concat('Feature_', id)").alias("name"),
            expr("CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END").alias("category"),
        )

        # Test writing to DBFS as GeoJSON
        sample_data.write.mode("overwrite").format("geojson").save(output_path)

        # Test reading from DBFS
        read_back = self.spark.read.format("geojson").load(output_path)
        count = read_back.count()

        assert count == 50, f"Expected 50 records, got {count}"

        print(f"  Records written and read: {count}")
        print(f"  Test data path: {output_path}")

        # Show schema and sample data
        print("  Schema:")
        read_back.printSchema()
        print("  Sample records:")
        read_back.show(3)

    def test_shapefile_io(self) -> None:
        """Test reading shapefile data from uploaded test data."""
        if self.spark is None:
            raise RuntimeError("Spark session not initialized")

        # Construct path to the uploaded shapefile data
        # The data manager uploads test data to /Volumes/wherobots/sedona_smoke_test/cluster_setup/{session_id}/data/
        shapefile_path = f"/Volumes/wherobots/sedona_smoke_test/cluster_setup/{self.session_id}/data/shp/ne_50m_airports"

        # Test reading shapefile using the shapefile format
        shapefile_df = self.spark.read.format("shapefile").load(shapefile_path)

        # Get record count
        count = shapefile_df.count()
        print(f"  Records read from shapefile: {count}")

        # Verify we have some data
        assert count > 0, f"Expected shapefile to contain records, got {count}"

        # Show schema and sample data
        print("  Schema:")
        shapefile_df.printSchema()
        print("  Sample records:")
        shapefile_df.show(3)

    def test_pbf_io(self) -> None:
        """Test reading PBF data from uploaded test data."""
        # Construct path to the uploaded PBF data
        pbf_path = f"/Volumes/wherobots/sedona_smoke_test/cluster_setup/{self.session_id}/data/pbf/monaco-latest.osm.pbf"

        # Test reading PBF using the osm format
        pbf_df = self.spark.read.format("osmpbf").load(pbf_path)

        # Get record count
        count = pbf_df.count()
        print(f"  Records read from PBF: {count}")

        # Verify we have some data
        assert count > 0, f"Expected PBF to contain records, got {count}"

        # Show schema and sample data
        print("  Schema:")
        pbf_df.printSchema()
        print("  Sample records:")
        pbf_df.show(3)

    def run_all_tests(self) -> bool:
        """Run all smoke tests."""
        print("Starting Sedona Smoke Tests")
        print(f"Session ID: {self.session_id}")
        print("=" * 60)

        if not self.setup():
            print("Failed to setup test environment. Exiting.")
            return False

        # Define all tests
        tests = [
            ("Basic ST Functions", self.test_basic_st_functions),
            ("Spatial Operations", self.test_spatial_operations),
            ("Spatial Join", self.test_spatial_join),
            ("GeoParquet I/O", self.test_geoparquet_io),
            ("GeoJSON I/O", self.test_geojson_io),
            ("Shapefile I/O", self.test_shapefile_io),
            ("PBF I/O", self.test_pbf_io),
        ]

        # Run all tests
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)

        # Print summary
        self.print_summary()

        # Clean up /dbfs/tmp/sedona-smoke-test/{self.session_id}
        test_dir = f"/dbfs/tmp/sedona-smoke-test/{self.session_id}"
        try:
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)
                print(f"Cleaned up test directory: {test_dir}")
        except Exception as e:
            print(f"Warning: Could not clean up test directory {test_dir}: {e}")

        # Return True if all tests passed
        return all(result[1] for result in self.test_results)

    def print_summary(self) -> None:
        """Print test summary."""
        print("\n" + "=" * 60)
        print("SEDONA SMOKE TEST SUMMARY")
        print("=" * 60)

        passed = len([r for r in self.test_results if r[1]])
        total = len(self.test_results)

        for test_name, success, error in self.test_results:
            status = "✓ PASSED" if success else "✗ FAILED"
            print(f"{status}: {test_name}")
            if not success and error:
                # Print first line of error for summary
                error_line = error.split("\n")[0]
                print(f"    Error: {error_line}")

        print("=" * 60)
        print(f"RESULTS: {passed}/{total} tests passed")

        if passed == total:
            print("✓ ALL TESTS PASSED!")
            print("Sedona is working correctly on this Databricks cluster.")
        else:
            print("✗ SOME TESTS FAILED!")
            print("Check the detailed error messages above.")

        print("=" * 60)


def main():
    """Main entry point for the smoke test."""
    parser = argparse.ArgumentParser(
        description="Sedona Smoke Test Script for Databricks"
    )
    parser.add_argument(
        "--session-id",
        type=str,
        help="Session ID for test isolation (affects file paths)",
        default=None,
    )

    args = parser.parse_args()

    tester = SedonaSmokeTest(session_id=args.session_id)
    success = tester.run_all_tests()

    # Exit with appropriate code for script execution
    if not success:
        raise RuntimeError("Sedona smoke tests failed")


if __name__ == "__main__":
    main()
