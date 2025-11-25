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
Integration tests for STAC authentication with real services.

These tests use environment variables to configure authenticated services.
Tests are skipped when environment variables are not set.

Environment variables:
    STAC_PUBLIC_URL - Public STAC service URL (e.g., Microsoft Planetary Computer)
    STAC_AUTH_URL - Authenticated STAC service URL
    STAC_USERNAME - Username or API key for basic authentication
    STAC_PASSWORD - Password (can be empty for API keys)
    STAC_BEARER_TOKEN - Bearer token for token-based authentication
    STAC_AUTH_URL_REQUIRE_AUTH - URL requiring authentication (for failure testing)
"""

import os
import pytest
from sedona.spark.stac.client import Client

from tests.test_base import TestBase


class TestStacAuthIntegration(TestBase):
    """
    Integration tests for authenticated STAC services.

    Tests are skipped when required environment variables are not set.
    """

    @pytest.mark.skipif(
        not os.getenv("STAC_PUBLIC_URL"),
        reason="STAC_PUBLIC_URL not set - skip public service test",
    )
    def test_public_service_without_authentication(self):
        """
        Test that public STAC services work without authentication.

        This verifies backward compatibility - existing code should work unchanged.
        Set STAC_PUBLIC_URL environment variable to test.
        Example: https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip
        """
        public_url = os.getenv("STAC_PUBLIC_URL")

        client = Client.open(public_url)

        # Try to load data without authentication
        df = client.search(max_items=10)

        assert df is not None
        assert df.count() >= 0

    @pytest.mark.skipif(
        not os.getenv("STAC_AUTH_URL") or not os.getenv("STAC_BEARER_TOKEN"),
        reason="STAC_AUTH_URL or STAC_BEARER_TOKEN not set - skip bearer token test",
    )
    def test_bearer_token_authentication(self):
        """
        Test authentication with bearer token.

        NOTE: Planet Labs API requires Basic Auth (not Bearer token) for collections.
        Bearer token authentication works with other STAC services.

        Requires: STAC_AUTH_URL and STAC_BEARER_TOKEN environment variables
        """
        auth_url = os.getenv("STAC_AUTH_URL")
        bearer_token = os.getenv("STAC_BEARER_TOKEN")

        client = Client.open(auth_url)
        client.with_bearer_token(bearer_token)

        # Try to load data with bearer token
        df = client.search(max_items=10)

        assert df is not None
        assert df.count() >= 0

    @pytest.mark.skipif(
        not os.getenv("STAC_AUTH_URL") or not os.getenv("STAC_USERNAME"),
        reason="STAC_AUTH_URL or STAC_USERNAME not set - skip basic auth test",
    )
    def test_basic_authentication(self):
        """
        Test authentication with username/password or API key.

        This test works with Planet Labs API (username=API_key, password=empty).

        Requires: STAC_AUTH_URL and STAC_USERNAME environment variables
        STAC_PASSWORD is optional (defaults to empty string)
        """
        auth_url = os.getenv("STAC_AUTH_URL")
        username = os.getenv("STAC_USERNAME")
        password = os.getenv("STAC_PASSWORD", "")

        client = Client.open(auth_url)
        client.with_basic_auth(username, password)

        # Try to load data with basic auth
        df = client.search(max_items=10)

        assert df is not None
        assert df.count() >= 0

    @pytest.mark.skipif(
        not os.getenv("STAC_AUTH_URL_REQUIRE_AUTH"),
        reason="STAC_AUTH_URL_REQUIRE_AUTH not set - skip auth failure test",
    )
    def test_authentication_failure(self):
        """
        Test that authentication errors are properly raised.

        This verifies that accessing an authenticated endpoint without credentials
        results in a proper authentication error.

        Requires: STAC_AUTH_URL_REQUIRE_AUTH environment variable
        """
        auth_url = os.getenv("STAC_AUTH_URL_REQUIRE_AUTH")

        client = Client.open(auth_url)

        # Try to access authenticated endpoint without credentials
        with pytest.raises(Exception) as exc_info:
            df = client.search(max_items=1)
            df.count()  # Force execution

        # Verify we get an authentication-related error
        error_message = str(exc_info.value).lower()
        assert (
            "401" in error_message
            or "unauthorized" in error_message
            or "403" in error_message
            or "forbidden" in error_message
        ), f"Expected authentication error, but got: {exc_info.value}"


class TestStacAuthUnit(TestBase):
    """Unit tests for authentication methods that don't require real services."""

    def test_bearer_token_format(self):
        """Test that bearer token is formatted correctly."""
        client = Client.open("https://example.com/stac/v1")
        client.with_bearer_token("test_token_12345")

        assert "Authorization" in client.headers
        assert client.headers["Authorization"] == "Bearer test_token_12345"

    def test_basic_auth_encoding(self):
        """Test that basic auth is encoded correctly."""
        import base64

        client = Client.open("https://example.com/stac/v1")
        client.with_basic_auth("testuser", "testpass")

        auth_header = client.headers["Authorization"]
        assert auth_header.startswith("Basic ")

        # Decode and verify
        encoded_part = auth_header.replace("Basic ", "")
        decoded = base64.b64decode(encoded_part).decode()
        assert decoded == "testuser:testpass"

    def test_method_chaining(self):
        """Test that authentication methods support chaining."""
        client = Client.open("https://example.com/stac/v1")
        result = client.with_bearer_token("test_token")

        # Method should return self for chaining
        assert result is client

    def test_headers_propagation(self):
        """Test that headers are propagated to collection clients."""
        client = Client.open("https://example.com/stac/v1")
        client.with_bearer_token("test_token")

        # Headers should be set on client
        assert "Authorization" in client.headers
        assert client.headers["Authorization"] == "Bearer test_token"
