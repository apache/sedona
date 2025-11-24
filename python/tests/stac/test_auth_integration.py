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

These tests can be run with:
    RUN_AUTH_TESTS=1 pytest tests/stac/test_auth_integration.py

To test with real services, set the following environment variables:
    PL_API_KEY - Planet API key for testing basic auth
    SH_CLIENT_ID - Sentinel Hub client ID for OAuth testing
    SH_CLIENT_SECRET - Sentinel Hub client secret for OAuth testing
"""

import os
import pytest
from sedona.spark.stac.client import Client


class TestAuthIntegration:
    """
    Integration tests for authenticated STAC services.

    These tests are skipped by default and only run when RUN_AUTH_TESTS=1 is set.
    """

    @pytest.mark.skipif(
        not os.getenv("RUN_AUTH_TESTS"),
        reason="Set RUN_AUTH_TESTS=1 to run integration tests",
    )
    @pytest.mark.skipif(
        not os.getenv("PL_API_KEY"),
        reason="PL_API_KEY not set - cannot test Planet authentication",
    )
    def test_planet_basic_auth_real(self):
        """
        Test authentication with Planet STAC API using basic auth.

        Requires: PL_API_KEY environment variable
        """
        api_key = os.getenv("PL_API_KEY")

        # Planet uses API key as username with empty password
        client = Client.open("https://api.planet.com/x/data/")
        client.with_basic_auth(api_key, "")

        # Try a simple search with small bbox and date range
        try:
            df = client.search(
                collection_id="PSScene",
                bbox=[-122.5, 37.7, -122.3, 37.9],
                datetime="2024-01",
                max_items=1,
            )

            assert df is not None
            assert df.count() >= 0  # May be 0 if no items in this area/time
            print(f"✓ Planet authentication successful, found {df.count()} items")
        except Exception as e:
            pytest.fail(f"Planet authentication test failed: {str(e)}")

    @pytest.mark.skipif(
        not os.getenv("RUN_AUTH_TESTS"),
        reason="Set RUN_AUTH_TESTS=1 to run integration tests",
    )
    def test_planetary_computer_no_auth(self):
        """
        Test that public STAC services (no auth) still work correctly.

        This verifies backward compatibility - existing code should work unchanged.
        """
        client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

        # Search without authentication
        try:
            df = client.search(
                collection_id="naip",
                bbox=[-122.5, 37.7, -122.3, 37.9],
                datetime="2020",
                max_items=1,
            )

            assert df is not None
            print(f"✓ Public service access successful (backward compatible)")
        except Exception as e:
            pytest.fail(f"Public service test failed: {str(e)}")

    def test_bearer_token_format(self):
        """Test that bearer token is formatted correctly."""
        client = Client.open("https://example.com/stac/v1")
        client.with_bearer_token("test_token_12345")

        assert "Authorization" in client.headers
        assert client.headers["Authorization"] == "Bearer test_token_12345"
        print("✓ Bearer token format correct")

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
        print("✓ Basic auth encoding correct")
