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
from sedona.spark.stac.client import Client
from pyspark.sql import DataFrame

from tests.test_base import TestBase
from tests.stac.test_mock_fixtures import MockClient

STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "EARTHVIEW-CATALOG": "https://satellogic-earthview.s3.us-west-2.amazonaws.com/stac/catalog.json",
}


class TestStacClient(TestBase):
    @patch("sedona.spark.stac.client.Client.open")
    def test_collection_client(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[-100.0, -72.0, 105.0, -69.0],
            datetime=["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"],
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_ids(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            *["AST_L1T_00312272006020322_20150518201805", "item2"],
            collection_id="aster-l1t",
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_single_id(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            "AST_L1T_00312272006020322_20150518201805",
            collection_id="aster-l1t",
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_bbox_and_datetime(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            datetime=["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"],
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_multiple_bboxes_and_intervals(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[
                [90, -73, 105, -69],
                [-180.0, -90.0, -170.0, -80.0],
                [-100.0, -72.0, -90.0, -62.0],
            ],
            datetime=[["2006-12-01T00:00:00Z", "2006-12-27T03:00:00Z"]],
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_bbox_and_non_overlapping_intervals(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            datetime=[
                ["2006-01-01T00:00:00Z", "2006-06-01T00:00:00Z"],
                ["2006-07-01T00:00:00Z", "2007-01-01T00:00:00Z"],
            ],
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_max_items(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            datetime=["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"],
            max_items=5,
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_single_datetime(self, mock_open) -> None:
        from datetime import datetime

        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            datetime=datetime(2006, 12, 26, 18, 3, 22),
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_YYYY(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        items = client.search(
            collection_id="aster-l1t",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            datetime="2006",
            return_dataframe=False,
        )
        assert items is not None
        assert isinstance(items, collections.abc.Iterator)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_return_dataframe(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["PLANETARY-COMPUTER"])

        client = Client.open(STAC_URLS["PLANETARY-COMPUTER"])
        df = client.search(
            collection_id="aster-l1t",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            datetime=["2006-01-01T00:00:00Z", "2007-01-01T00:00:00Z"],
        )
        assert df is not None
        assert isinstance(df, DataFrame)

    @patch("sedona.spark.stac.client.Client.open")
    def test_search_with_catalog_url(self, mock_open) -> None:
        mock_open.return_value = MockClient(STAC_URLS["EARTHVIEW-CATALOG"])

        client = Client.open(STAC_URLS["EARTHVIEW-CATALOG"])
        df = client.search(
            return_dataframe=True,
        )
        assert df is not None
        assert isinstance(df, DataFrame)
