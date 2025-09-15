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

from typing import Iterator, List, Optional, Union
from unittest.mock import create_autospec, MagicMock

from pyspark.sql import DataFrame


class MockItem:
    """Mock STAC Item"""

    def __init__(self, item_id: str = "test_item"):
        self.id = item_id
        self.properties = {
            "datetime": "2006-12-26T18:03:22Z",
            "collection": "aster-l1t",
        }
        self.geometry = {
            "type": "Polygon",
            "coordinates": [[[90, -73], [105, -73], [105, -69], [90, -69], [90, -73]]],
        }
        self.bbox = [90, -73, 105, -69]
        self.assets = {}


class MockIterator:
    """Mock iterator for STAC items"""

    def __init__(self, items: List[MockItem] = None):
        self.items = items or [MockItem(f"item_{i}") for i in range(5)]
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.items):
            item = self.items[self.index]
            self.index += 1
            return item
        raise StopIteration


def create_mock_dataframe(data=None):
    """Create a mock DataFrame that passes isinstance checks"""
    mock_df = create_autospec(DataFrame, instance=True)
    mock_df.data = data or []
    mock_df._count = len(mock_df.data) if data else 5
    mock_df.count.return_value = mock_df._count
    mock_df.collect.return_value = mock_df.data
    mock_df.show.return_value = None
    return mock_df


class MockCollectionClient:
    """Mock CollectionClient"""

    def __init__(self, url: str, collection_id: str):
        self.url = url
        self.collection_id = collection_id

    def __str__(self):
        return f"<CollectionClient id={self.collection_id}>"

    def get_items(self, *ids, **kwargs) -> Iterator:
        """Return mock iterator of items"""
        if ids:
            items = [MockItem(item_id) for item_id in ids if isinstance(item_id, str)]
        else:
            items = [MockItem(f"item_{i}") for i in range(5)]
        return MockIterator(items)

    def get_dataframe(self, **kwargs) -> DataFrame:
        """Return mock DataFrame"""
        # Return a mock DataFrame instead of creating a real Spark DataFrame
        return create_mock_dataframe()

    def save_to_geoparquet(self, output_path: str, **kwargs):
        """Mock save to geoparquet - just create an empty file"""
        import os

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            f.write("")


class MockClient:
    """Mock STAC Client"""

    def __init__(self, url: str):
        self.url = url

    @classmethod
    def open(cls, url: str):
        """Create mock client instance"""
        return cls(url)

    def get_collection(self, collection_id: str):
        """Return mock collection client"""
        return MockCollectionClient(self.url, collection_id)

    def search(
        self,
        *ids,
        collection_id: Optional[str] = None,
        bbox: Optional[list] = None,
        geometry: Optional[Union[str, object, List]] = None,
        datetime: Optional[Union[str, object, list]] = None,
        max_items: Optional[int] = None,
        return_dataframe: bool = True,
    ) -> Union[Iterator, DataFrame]:
        """Mock search method"""
        if return_dataframe:
            # Return mock DataFrame instead of creating a real Spark DataFrame
            return create_mock_dataframe()
        else:
            # Return mock iterator
            if ids and len(ids) > 0:
                if isinstance(ids[0], str):
                    items = [MockItem(ids[0])]
                else:
                    items = [
                        MockItem(item_id)
                        for item_id in ids[0]
                        if isinstance(item_id, str)
                    ]
            else:
                num_items = min(max_items, 5) if max_items else 5
                items = [MockItem(f"item_{i}") for i in range(num_items)]
            return MockIterator(items)


def create_mock_client(url: str) -> MockClient:
    """Factory function to create a mock client"""
    return MockClient(url)


def mock_client_open(monkeypatch):
    """Pytest fixture to mock Client.open"""

    def _mock_open(url: str):
        return MockClient(url)

    return _mock_open
