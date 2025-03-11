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
from typing import Union, Optional, Iterator

from sedona.stac.collection_client import CollectionClient

import datetime as python_datetime
from pystac import Item as PyStacItem

from pyspark.sql import DataFrame


class Client:
    def __init__(self, url: str):
        self.url = url

    @classmethod
    def open(cls, url: str):
        """
        Opens a connection to the specified STAC API URL.

        This class method creates an instance of the Client class with the given URL.

        Parameters:
        - url (str): The URL of the STAC API to connect to.
          Example: "https://planetarycomputer.microsoft.com/api/stac/v1"

        Returns:
        - Client: An instance of the Client class connected to the specified URL.
        """
        return cls(url)

    def get_collection(self, collection_id: str):
        """
        Retrieves a collection client for the specified collection ID.

        This method creates an instance of the CollectionClient class for the given collection ID,
        allowing interaction with the specified collection in the STAC API.

        Parameters:
        - collection_id (str): The ID of the collection to retrieve.
          Example: "aster-l1t"

        Returns:
        - CollectionClient: An instance of the CollectionClient class for the specified collection.
        """
        return CollectionClient(self.url, collection_id)

    def get_collection_from_catalog(self):
        """
        Retrieves the catalog from the STAC API.

        This method fetches the root catalog from the STAC API, providing access to all collections and items.

        Returns:
        - dict: The root catalog of the STAC API.
        """
        # Implement logic to fetch and return the root catalog
        return CollectionClient(self.url, None)

    def search(
        self,
        *ids: Union[str, list],
        collection_id: Optional[str] = None,
        bbox: Optional[list] = None,
        datetime: Optional[Union[str, python_datetime.datetime, list]] = None,
        max_items: Optional[int] = None,
        return_dataframe: bool = True,
    ) -> Union[Iterator[PyStacItem], DataFrame]:
        """
        Searches for items in the specified collection with optional filters.

        Parameters:
        - ids (Union[str, list]): A variable number of item IDs to filter the items.
          Example: "item_id1" or ["item_id1", "item_id2"]

        - collection_id (Optional[str]): The ID of the collection to search in.
          Example: "aster-l1t"

        - bbox (Optional[list]): A list of bounding boxes for filtering the items.
          Each bounding box is represented as a list of four float values: [min_lon, min_lat, max_lon, max_lat].
          Example: [[-180.0, -90.0, 180.0, 90.0]]  # This bounding box covers the entire world.

        - datetime (Optional[Union[str, python_datetime.datetime, list]]): A single datetime, RFC 3339-compliant timestamp,
          or a list of date-time ranges for filtering the items. The datetime can be specified in various formats:
          - "YYYY" expands to ["YYYY-01-01T00:00:00Z", "YYYY-12-31T23:59:59Z"]
          - "YYYY-mm" expands to ["YYYY-mm-01T00:00:00Z", "YYYY-mm-<last_day>T23:59:59Z"]
          - "YYYY-mm-dd" expands to ["YYYY-mm-ddT00:00:00Z", "YYYY-mm-ddT23:59:59Z"]
          - "YYYY-mm-ddTHH:MM:SSZ" remains as ["YYYY-mm-ddTHH:MM:SSZ", "YYYY-mm-ddTHH:MM:SSZ"]
          - A list of date-time ranges can be provided for multiple intervals.
          Example: "2020-01-01T00:00:00Z" or python_datetime.datetime(2020, 1, 1) or [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]

        - max_items (Optional[int]): The maximum number of items to return from the search, even if there are more matching results.
          Example: 100

        - return_dataframe (bool): If True, return the result as a Spark DataFrame instead of an iterator of PyStacItem objects.
          Example: True

        Returns:
        - Union[Iterator[PyStacItem], DataFrame]: An iterator of PyStacItem objects or a Spark DataFrame that match the specified filters.
        """
        if collection_id:
            client = self.get_collection(collection_id)
        else:
            client = self.get_collection_from_catalog()
        if return_dataframe:
            return client.get_dataframe(
                *ids, bbox=bbox, datetime=datetime, max_items=max_items
            )
        else:
            return client.get_items(
                *ids, bbox=bbox, datetime=datetime, max_items=max_items
            )
