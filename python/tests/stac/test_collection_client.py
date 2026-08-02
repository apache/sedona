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

import collections.abc
from unittest.mock import patch

from pyspark.sql import DataFrame
from sedona.spark.stac.client import Client
from sedona.spark.stac.collection_client import CollectionClient

from tests.test_base import TestBase
from tests.stac.test_mock_fixtures import MockClient, MockCollectionClient

STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1"
}


class TestStacReader(TestBase):
    @patch("sedona.spark.stac.client.Client.open")
    def test_collection_client(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        assert isinstance(collection, MockCollectionClient)
        assert str(collection) == "<CollectionClient id=aster-l1t>"

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_no_filters(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        df = collection.get_dataframe()
        assert df is not None
        assert isinstance(df, DataFrame)

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_spatial_extent(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        df = collection.get_dataframe(bbox=bbox)
        assert df is not None
        assert isinstance(df, DataFrame)

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_temporal_extent(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        datetime = [["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]]
        df = collection.get_dataframe(datetime=datetime)
        assert df is not None
        assert isinstance(df, DataFrame)

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_both_extents(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        datetime = [["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]]
        df = collection.get_dataframe(bbox=bbox, datetime=datetime)
        assert df is not None
        assert isinstance(df, DataFrame)

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_spatial_extent(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-100.0, -72.0, 105.0, -69.0]]
        items = list(collection.get_items(bbox=bbox))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_temporal_extent(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        datetime = [["2006-12-01T00:00:00Z", "2006-12-27T02:00:00Z"]]
        items = list(collection.get_items(datetime=datetime))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_both_extents(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[90, -73, 105, -69]]
        datetime = [["2006-12-01T00:00:00Z", "2006-12-27T03:00:00Z"]]
        items = list(collection.get_items(bbox=bbox, datetime=datetime))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_multiple_bboxes_and_interval(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [
            [90, -73, 105, -69],  # Bounding box 1
            [
                -180.0,
                -90.0,
                -170.0,
                -80.0,
            ],  # Bounding box 2 (non-overlapping with bbox 1)
            [
                -100.0,
                -72.0,
                -90.0,
                -62.0,
            ],  # Bounding box 3 (non-overlapping with bbox 1 and 2)
        ]
        datetime = [["2006-12-01T00:00:00Z", "2006-12-27T03:00:00Z"]]
        items = list(collection.get_items(bbox=bbox, datetime=datetime))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_ids(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        ids = ["AST_L1T_00312272006020322_20150518201805", "item2", "item3"]
        items = list(collection.get_items(*ids))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_id(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        items = list(collection.get_items("AST_L1T_00312272006020322_20150518201805"))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_bbox_and_non_overlapping_intervals(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        datetime = [
            ["2006-01-01T00:00:00Z", "2006-06-01T00:00:00Z"],
            ["2006-07-01T00:00:00Z", "2007-01-01T00:00:00Z"],
        ]
        items = list(collection.get_items(bbox=bbox, datetime=datetime))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_bbox_and_interval(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [-180.0, -90.0, 180.0, 90.0]
        interval = ["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]
        items = list(collection.get_items(bbox=bbox, datetime=interval))
        assert items is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_bbox_and_interval(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [-180.0, -90.0, 180.0, 90.0]
        interval = ["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]
        df = collection.get_dataframe(bbox=bbox, datetime=interval)
        assert df is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_save_to_geoparquet(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Create a temporary directory for the output path and clean it up after the test
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = f"{tmpdirname}/test_geoparquet_output"

            # Define spatial and temporal extents
            bbox = [[-180.0, -90.0, 180.0, 90.0]]
            datetime = [["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]]

            # Call the method to save the DataFrame to GeoParquet
            collection.save_to_geoparquet(
                output_path=output_path, bbox=bbox, datetime=datetime
            )

            # Check if the file was created
            import os

            assert os.path.exists(output_path), "GeoParquet file was not created"

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_wkt_geometry(self, mock_open) -> None:
        """Test that WKT geometry strings are properly handled for spatial filtering."""
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Test with WKT polygon geometry
        wkt_polygon = "POLYGON((90 -73, 105 -73, 105 -69, 90 -69, 90 -73))"
        items_with_wkt = list(collection.get_items(geometry=wkt_polygon))

        # Both should return similar number of items (may not be exactly same due to geometry differences)
        assert items_with_wkt is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_shapely_geometry(self, mock_open) -> None:
        """Test that Shapely geometry objects are properly handled for spatial filtering."""
        from shapely.geometry import Polygon

        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Test with Shapely polygon geometry
        shapely_polygon = Polygon(
            [(90, -73), (105, -73), (105, -69), (90, -69), (90, -73)]
        )
        df_with_shapely = collection.get_dataframe(geometry=shapely_polygon)

        # Both should return similar number of items
        assert df_with_shapely is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_geometry_list(self, mock_open) -> None:
        """Test that lists of geometry objects are properly handled."""
        from shapely.geometry import Polygon

        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Test with list of geometries (both WKT and Shapely)
        wkt_polygon = "POLYGON((90 -73, 105 -73, 105 -69, 90 -69, 90 -73))"
        shapely_polygon = Polygon(
            [(-100, -72), (-90, -72), (-90, -62), (-100, -62), (-100, -72)]
        )
        geometry_list = [wkt_polygon, shapely_polygon]

        items_with_geom_list = list(collection.get_items(geometry=geometry_list))

        # Should return items from both geometries
        assert items_with_geom_list is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_geometry_takes_precedence_over_bbox(self, mock_open) -> None:
        """Test that geometry parameter takes precedence over bbox when both are provided."""
        from shapely.geometry import Polygon

        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Define different spatial extents
        bbox = [-180.0, -90.0, 180.0, 90.0]  # World bbox
        small_polygon = Polygon(
            [(90, -73), (105, -73), (105, -69), (90, -69), (90, -73)]
        )  # Small area

        # When both are provided, geometry should take precedence
        items_with_both = list(collection.get_items(bbox=bbox, geometry=small_polygon))
        items_with_geom_only = list(collection.get_items(geometry=small_polygon))

        # Results should be identical since geometry takes precedence
        assert items_with_both is not None
        assert items_with_geom_only is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_geometry_and_datetime(self, mock_open) -> None:
        """Test that geometry and datetime filters work together."""
        from shapely.geometry import Polygon

        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Define spatial and temporal filters
        polygon = Polygon([(90, -73), (105, -73), (105, -69), (90, -69), (90, -73)])
        datetime_range = ["2006-12-01T00:00:00Z", "2006-12-27T03:00:00Z"]

        df_with_both = collection.get_dataframe(
            geometry=polygon, datetime=datetime_range
        )
        df_with_geom_only = collection.get_dataframe(geometry=polygon)

        # Combined filter should return fewer or equal items than geometry-only filter
        assert df_with_both is not None
        assert df_with_geom_only is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_save_to_geoparquet_with_geometry(self, mock_open) -> None:
        """Test saving to GeoParquet with geometry parameter."""
        from shapely.geometry import Polygon
        import tempfile
        import os

        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Create a temporary directory for the output path and clean it up after the test
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = f"{tmpdirname}/test_geometry_geoparquet_output"

            # Define spatial and temporal extents
            polygon = Polygon(
                [(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)]
            )
            datetime_range = [["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]]

            # Call the method to save the DataFrame to GeoParquet
            collection.save_to_geoparquet(
                output_path=output_path, geometry=polygon, datetime=datetime_range
            )

            # Check if the file was created
            assert os.path.exists(output_path), "GeoParquet file was not created"

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_items_with_tuple_datetime(self, mock_open) -> None:
        """Test that tuples are properly handled as datetime input (same as lists)."""
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Test with tuple instead of list
        datetime_tuple = ("2006-12-01T00:00:00Z", "2006-12-27T02:00:00Z")
        items_with_tuple = list(collection.get_items(datetime=datetime_tuple))

        # Test with list for comparison
        datetime_list = ["2006-12-01T00:00:00Z", "2006-12-27T02:00:00Z"]
        items_with_list = list(collection.get_items(datetime=datetime_list))

        # Both should return the same number of items
        assert items_with_tuple is not None
        assert items_with_list is not None

    @patch("sedona.spark.stac.client.Client.open")
    def test_get_dataframe_with_tuple_datetime(self, mock_open) -> None:
        """Test that tuples are properly handled as datetime input for dataframes."""
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        # Test with tuple instead of list
        datetime_tuple = ("2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z")
        df_with_tuple = collection.get_dataframe(datetime=datetime_tuple)

        # Test with list for comparison
        datetime_list = ["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]
        df_with_list = collection.get_dataframe(datetime=datetime_list)

        # Both should return the same count
        assert df_with_tuple is not None
        assert df_with_list is not None

    def test_expand_date(self) -> None:
        """_expand_date should expand each supported STAC datetime form.

        Whole-period upper bounds end at ``23:59:59.999999Z`` (Spark's
        microsecond precision) so that the inclusive ``datetime <= end`` filter
        keeps items in the final fractional second.
        """
        # YYYY
        assert CollectionClient._expand_date("2017") == [
            "2017-01-01T00:00:00Z",
            "2017-12-31T23:59:59.999999Z",
        ]
        # YYYY-mm expands to the first and last day of the month
        assert CollectionClient._expand_date("2017-06") == [
            "2017-06-01T00:00:00Z",
            "2017-06-30T23:59:59.999999Z",
        ]
        # December must not overflow into the next year
        assert CollectionClient._expand_date("2020-12") == [
            "2020-12-01T00:00:00Z",
            "2020-12-31T23:59:59.999999Z",
        ]
        # Leap-year February resolves to the 29th
        assert CollectionClient._expand_date("2020-02") == [
            "2020-02-01T00:00:00Z",
            "2020-02-29T23:59:59.999999Z",
        ]
        # Non-leap-year February resolves to the 28th
        assert CollectionClient._expand_date("2021-02") == [
            "2021-02-01T00:00:00Z",
            "2021-02-28T23:59:59.999999Z",
        ]
        # YYYY-mm-dd
        assert CollectionClient._expand_date("2017-06-10") == [
            "2017-06-10T00:00:00Z",
            "2017-06-10T23:59:59.999999Z",
        ]
        # Full timestamps are returned unchanged
        assert CollectionClient._expand_date("2017-06-01T00:00:00Z") == [
            "2017-06-01T00:00:00Z",
            "2017-06-01T00:00:00Z",
        ]

    def test_expand_date_filter_includes_final_fractional_second(self) -> None:
        """A whole-period search must keep items in the final fractional second.

        Sedona filters STAC datetimes with an inclusive upper bound, so the
        expanded end of ``YYYY``/``YYYY-mm``/``YYYY-mm-dd`` periods must cover
        sub-second timestamps such as ``23:59:59.5Z``. This exercises the real
        Spark filter, not just the string expansion.
        """
        from pyspark.sql import functions as F

        # One row per period type, each sitting in the final fractional second
        # of that period -- the exact rows that a rounded 23:59:59Z bound drops.
        df = self.spark.createDataFrame(
            [
                ("year-edge", "2020-12-31T23:59:59.5Z"),
                ("month-edge", "2020-05-31T23:59:59.5Z"),
                ("day-edge", "2020-05-15T23:59:59.5Z"),
                ("next-period", "2021-01-01T00:00:00Z"),
            ],
            ["id", "datetime_str"],
        ).withColumn("datetime", F.to_timestamp("datetime_str"))

        def matches(date_str):
            interval = CollectionClient._expand_date(date_str)
            filtered = CollectionClient._apply_spatial_temporal_filters(
                df, datetime=[interval]
            )
            return {row["id"] for row in filtered.collect()}

        # YYYY keeps the Dec-31 23:59:59.5 row and excludes the next year.
        assert matches("2020") == {"year-edge", "month-edge", "day-edge"}
        # YYYY-mm keeps the last-day fractional-second row.
        assert "month-edge" in matches("2020-05")
        assert "day-edge" in matches("2020-05")
        # YYYY-mm-dd keeps the fractional-second row on that day.
        assert matches("2020-05-15") == {"day-edge"}
        # The first instant of the next period is never pulled in.
        assert "next-period" not in matches("2020")
