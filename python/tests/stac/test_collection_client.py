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

from sedona.stac.client import Client
from sedona.stac.collection_client import CollectionClient

from tests.test_base import TestBase

STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1"
}


class TestStacReader(TestBase):
    def test_collection_client(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")

        assert isinstance(collection, CollectionClient)
        assert str(collection) == "<CollectionClient id=aster-l1t>"

    def test_get_dataframe_no_filters(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        df = collection.get_dataframe()
        assert df is not None
        assert df.count() == 20

    def test_get_dataframe_with_spatial_extent(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        df = collection.get_dataframe(bbox=bbox)
        assert df is not None
        assert df.count() > 0

    def test_get_dataframe_with_temporal_extent(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        datetime = [["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]]
        df = collection.get_dataframe(datetime=datetime)
        assert df is not None
        assert df.count() > 0

    def test_get_dataframe_with_both_extents(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        datetime = [["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]]
        df = collection.get_dataframe(bbox=bbox, datetime=datetime)
        assert df is not None
        assert df.count() > 0

    def test_get_items_with_spatial_extent(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-100.0, -72.0, 105.0, -69.0]]
        items = list(collection.get_items(bbox=bbox))
        assert items is not None
        assert len(items) == 2

    def test_get_items_with_temporal_extent(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        datetime = [["2006-12-01T00:00:00Z", "2006-12-27T02:00:00Z"]]
        items = list(collection.get_items(datetime=datetime))
        assert items is not None
        assert len(items) == 16

    def test_get_items_with_both_extents(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[90, -73, 105, -69]]
        datetime = [["2006-12-01T00:00:00Z", "2006-12-27T03:00:00Z"]]
        items = list(collection.get_items(bbox=bbox, datetime=datetime))
        assert items is not None
        assert len(items) == 4

    def test_get_items_with_multiple_bboxes_and_interval(self) -> None:
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
        assert len(items) == 4

    def test_get_items_with_ids(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        ids = ["AST_L1T_00312272006020322_20150518201805", "item2", "item3"]
        items = list(collection.get_items(*ids))
        assert items is not None
        assert len(items) == 1
        for item in items:
            assert item.id in ids

    def test_get_items_with_id(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        items = list(collection.get_items("AST_L1T_00312272006020322_20150518201805"))
        assert items is not None
        assert len(items) == 1

    def test_get_items_with_bbox_and_non_overlapping_intervals(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        datetime = [
            ["2006-01-01T00:00:00Z", "2006-06-01T00:00:00Z"],
            ["2006-07-01T00:00:00Z", "2007-01-01T00:00:00Z"],
        ]
        items = list(collection.get_items(bbox=bbox, datetime=datetime))
        assert items is not None
        assert len(items) == 20

    def test_get_items_with_bbox_and_interval(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [-180.0, -90.0, 180.0, 90.0]
        interval = ["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]
        items = list(collection.get_items(bbox=bbox, datetime=interval))
        assert items is not None
        assert len(items) > 0

    def test_get_dataframe_with_bbox_and_interval(self) -> None:
        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        collection = client.get_collection("aster-l1t")
        bbox = [-180.0, -90.0, 180.0, 90.0]
        interval = ["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"]
        df = collection.get_dataframe(bbox=bbox, datetime=interval)
        assert df is not None
        assert df.count() > 0

    def test_save_to_geoparquet(self) -> None:
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

            # Optionally, you can load the file back and check its contents
            df_loaded = collection.spark.read.format("geoparquet").load(output_path)
            assert df_loaded.count() == 20, "Loaded GeoParquet file is empty"
