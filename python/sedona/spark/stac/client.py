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
from typing import Union, Optional, Iterator, List

from sedona.spark.stac.collection_client import CollectionClient

import datetime as python_datetime
from shapely.geometry.base import BaseGeometry

from pyspark.sql import DataFrame


class Client:
    def __init__(self, url: str, headers: Optional[dict] = None):
        """
        Initializes a STAC client with optional authentication headers.

        :param url: The URL of the STAC API to connect to.
        :param headers: Optional dictionary of HTTP headers to include in requests.
            Can be used for authentication or custom headers.
        """
        self.url = url
        self.headers = headers if headers is not None else {}

    @classmethod
    def open(cls, url: str, headers: Optional[dict] = None):
        """
        Opens a connection to the specified STAC API URL.

        This class method creates an instance of the Client class with the given URL
        and optional authentication headers.

        :param url: The URL of the STAC API to connect to. Example: "https://planetarycomputer.microsoft.com/api/stac/v1"
        :param headers: Optional dictionary of HTTP headers for authentication.
            Example: {"Authorization": "Bearer token123"}
        :return: An instance of the Client class connected to the specified URL.

        Example usage:
            # Without authentication
            client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

            # With custom headers
            client = Client.open(
                "https://example.com/stac/v1",
                headers={"Authorization": "Bearer token123"}
            )

            # Using convenience methods
            client = Client.open("https://example.com/stac/v1")
            client.with_basic_auth("username", "password")
        """
        return cls(url, headers)

    def with_basic_auth(self, username: str, password: str):
        """
        Adds HTTP Basic Authentication to the client.

        This method encodes the username and password using Base64 and adds
        the appropriate Authorization header for HTTP Basic Authentication.

        :param username: The username for authentication. For API keys, this is typically the API key itself.
        :param password: The password for authentication. For API keys, this is often left empty.
        :return: Self for method chaining.

        Example usage:
            # Standard basic auth
            client = Client.open("https://example.com/stac/v1")
            client.with_basic_auth("user", "pass")

            # API key as username (common pattern)
            client.with_basic_auth("api_key_xyz", "")

            # Method chaining
            df = Client.open(url).with_basic_auth(api_key, "").search(collection_id="test")
        """
        import base64

        userpass = f"{username}:{password}"
        b64_userpass = base64.b64encode(userpass.encode()).decode()
        self.headers["Authorization"] = f"Basic {b64_userpass}"
        return self

    def with_bearer_token(self, token: str):
        """
        Adds Bearer Token Authentication to the client.

        This method adds the appropriate Authorization header for Bearer Token
        authentication, commonly used with OAuth2 and API tokens.

        :param token: The bearer token for authentication.
        :return: Self for method chaining.

        Example usage:
            # Bearer token auth
            client = Client.open("https://example.com/stac/v1")
            client.with_bearer_token("your_access_token_here")

            # Method chaining
            df = Client.open(url).with_bearer_token(token).search(collection_id="test")
        """
        self.headers["Authorization"] = f"Bearer {token}"
        return self

    def get_collection(self, collection_id: str):
        """
        Retrieves a collection client for the specified collection ID.

        This method creates an instance of the CollectionClient class for the given collection ID,
        allowing interaction with the specified collection in the STAC API.

        :param collection_id: The ID of the collection to retrieve. Example: "aster-l1t"
        :return: An instance of the CollectionClient class for the specified collection.
        """
        return CollectionClient(self.url, collection_id, headers=self.headers)

    def get_collection_from_catalog(self):
        """
        Retrieves the catalog from the STAC API.

        This method fetches the root catalog from the STAC API, providing access to all collections and items.

        Returns:
            dict: The root catalog of the STAC API.
        """
        # Implement logic to fetch and return the root catalog
        return CollectionClient(self.url, None, headers=self.headers)

    def search(
        self,
        *ids: Union[str, list],
        collection_id: Optional[str] = None,
        bbox: Optional[list] = None,
        geometry: Optional[
            Union[str, BaseGeometry, List[Union[str, BaseGeometry]]]
        ] = None,
        datetime: Optional[Union[str, python_datetime.datetime, list]] = None,
        max_items: Optional[int] = None,
        return_dataframe: bool = True,
    ) -> Union[Iterator, DataFrame]:
        """
        Searches for items in the specified collection with optional filters.

        :param ids: A variable number of item IDs to filter the items.
            Example: "item_id1" or ["item_id1", "item_id2"]
        :param collection_id: The ID of the collection to search in.
            Example: "aster-l1t"
        :param bbox: A list of bounding boxes for filtering the items.
            Each bounding box is represented as a list of four float values: [min_lon, min_lat, max_lon, max_lat].
            Example: [[-180.0, -90.0, 180.0, 90.0]]  # This bounding box covers the entire world.
        :param geometry: Shapely geometry object(s) or WKT string(s) for spatial filtering.
            Can be a single geometry, WKT string, or a list of geometries/WKT strings.
            If both bbox and geometry are provided, geometry takes precedence.
            Example: Polygon(...) or "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))" or [Polygon(...), Polygon(...)]
        :param datetime: A single datetime, RFC 3339-compliant timestamp,
            or a list of date-time ranges for filtering the items. The datetime can be specified in various formats:

            - "YYYY" expands to ["YYYY-01-01T00:00:00Z", "YYYY-12-31T23:59:59Z"]
            - "YYYY-mm" expands to ["YYYY-mm-01T00:00:00Z", "YYYY-mm-<last_day>T23:59:59Z"]
            - "YYYY-mm-dd" expands to ["YYYY-mm-ddT00:00:00Z", "YYYY-mm-ddT23:59:59Z"]
            - "YYYY-mm-ddTHH:MM:SSZ" remains as ["YYYY-mm-ddTHH:MM:SSZ", "YYYY-mm-ddTHH:MM:SSZ"]
            - A list of date-time ranges can be provided for multiple intervals.

            Example: "2020-01-01T00:00:00Z" or python_datetime.datetime(2020, 1, 1) or [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]
        :param max_items: The maximum number of items to return from the search, even if there are more matching results.
            Example: 100
        :param return_dataframe: If True, return the result as a Spark DataFrame instead of an iterator of PyStacItem objects.
            Example: True
        :return: An iterator of PyStacItem objects or a Spark DataFrame that match the specified filters.
        """
        if collection_id:
            client = self.get_collection(collection_id)
        else:
            client = self.get_collection_from_catalog()
        if return_dataframe:
            return client.get_dataframe(
                *ids,
                bbox=bbox,
                geometry=geometry,
                datetime=datetime,
                max_items=max_items,
            )
        else:
            return client.get_items(
                *ids,
                bbox=bbox,
                geometry=geometry,
                datetime=datetime,
                max_items=max_items,
            )
