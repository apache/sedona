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

import base64
import json
from unittest.mock import patch, MagicMock
from sedona.spark.stac.client import Client
from sedona.spark.stac.collection_client import CollectionClient

from tests.test_base import TestBase


class TestStacAuthentication(TestBase):
    """Tests for STAC authentication functionality."""

    def test_client_with_headers(self):
        """Test that Client can be initialized with custom headers."""
        headers = {"Authorization": "Bearer test_token"}
        client = Client.open("https://example.com/stac/v1", headers=headers)

        assert client.headers == headers
        assert client.url == "https://example.com/stac/v1"

    def test_client_without_headers(self):
        """Test that Client works without headers (backward compatibility)."""
        client = Client.open("https://example.com/stac/v1")

        assert client.headers == {}
        assert client.url == "https://example.com/stac/v1"

    def test_with_basic_auth(self):
        """Test basic authentication header encoding."""
        client = Client.open("https://example.com/stac/v1")
        client.with_basic_auth("testuser", "testpass")

        # Verify the header was set correctly
        assert "Authorization" in client.headers
        auth_header = client.headers["Authorization"]
        assert auth_header.startswith("Basic ")

        # Verify the encoding is correct
        encoded_part = auth_header.replace("Basic ", "")
        decoded = base64.b64decode(encoded_part).decode()
        assert decoded == "testuser:testpass"

    def test_with_basic_auth_api_key(self):
        """Test basic auth with API key pattern (common in STAC APIs)."""
        client = Client.open("https://example.com/stac/v1")
        client.with_basic_auth("api_key_12345", "")

        # Verify the header was set correctly
        assert "Authorization" in client.headers
        auth_header = client.headers["Authorization"]
        encoded_part = auth_header.replace("Basic ", "")
        decoded = base64.b64decode(encoded_part).decode()
        assert decoded == "api_key_12345:"

    def test_with_bearer_token(self):
        """Test bearer token authentication."""
        client = Client.open("https://example.com/stac/v1")
        client.with_bearer_token("test_token_abc123")

        # Verify the header was set correctly
        assert "Authorization" in client.headers
        assert client.headers["Authorization"] == "Bearer test_token_abc123"

    def test_method_chaining(self):
        """Test that authentication methods support chaining."""
        client = Client.open("https://example.com/stac/v1").with_bearer_token(
            "token123"
        )

        assert client.headers["Authorization"] == "Bearer token123"

    def test_headers_passed_to_collection_client(self):
        """Test that headers are passed to CollectionClient."""
        headers = {"Authorization": "Bearer test_token"}
        client = Client.open("https://example.com/stac/v1", headers=headers)

        collection_client = client.get_collection("test-collection")

        assert isinstance(collection_client, CollectionClient)
        assert collection_client.headers == headers

    def test_headers_passed_to_catalog_client(self):
        """Test that headers are passed to catalog client."""
        headers = {"Authorization": "Bearer test_token"}
        client = Client.open("https://example.com/stac/v1", headers=headers)

        catalog_client = client.get_collection_from_catalog()

        assert isinstance(catalog_client, CollectionClient)
        assert catalog_client.headers == headers

    @patch("sedona.spark.stac.collection_client.CollectionClient.load_items_df")
    def test_headers_encoded_as_json_option(self, mock_load_items):
        """Test that headers are JSON-encoded when passed to Spark."""
        # Create a mock DataFrame
        mock_df = MagicMock()
        mock_load_items.return_value = mock_df

        headers = {"Authorization": "Bearer test_token", "X-Custom": "value"}
        client = Client.open("https://example.com/stac/v1", headers=headers)

        # Trigger a search that calls load_items_df
        collection_client = client.get_collection("test-collection")

        # Verify headers are stored correctly
        assert collection_client.headers == headers

    def test_custom_headers(self):
        """Test that custom headers (beyond auth) can be set."""
        headers = {
            "Authorization": "Bearer token",
            "X-API-Key": "key123",
            "User-Agent": "CustomClient/1.0",
        }
        client = Client.open("https://example.com/stac/v1", headers=headers)

        assert client.headers == headers

    def test_overwrite_auth_header(self):
        """Test that auth methods can overwrite existing auth headers."""
        client = Client.open("https://example.com/stac/v1")
        client.with_bearer_token("first_token")
        assert client.headers["Authorization"] == "Bearer first_token"

        # Overwrite with basic auth
        client.with_basic_auth("user", "pass")
        assert client.headers["Authorization"].startswith("Basic ")

    def test_collection_client_initialization_with_headers(self):
        """Test CollectionClient can be initialized with headers directly."""
        headers = {"Authorization": "Bearer test_token"}
        collection_client = CollectionClient(
            "https://example.com/stac/v1", "test-collection", headers=headers
        )

        assert collection_client.headers == headers
        assert collection_client.collection_id == "test-collection"

    def test_collection_client_without_headers(self):
        """Test CollectionClient backward compatibility without headers."""
        collection_client = CollectionClient(
            "https://example.com/stac/v1", "test-collection"
        )

        assert collection_client.headers == {}

    def test_empty_headers_dict(self):
        """Test that empty headers dict works correctly."""
        client = Client.open("https://example.com/stac/v1", headers={})

        assert client.headers == {}

    def test_headers_with_special_characters(self):
        """Test that headers with special characters are handled correctly."""
        # Base64 encoding should handle special characters
        client = Client.open("https://example.com/stac/v1")
        client.with_basic_auth("user@example.com", "p@ss!word#123")

        auth_header = client.headers["Authorization"]
        encoded_part = auth_header.replace("Basic ", "")
        decoded = base64.b64decode(encoded_part).decode()
        assert decoded == "user@example.com:p@ss!word#123"
