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
Simple Scarf telemetry for sedona.db package.
"""

import os
import platform
import threading
import urllib.request


def _send_telemetry():
    """Send simple telemetry data to Scarf gateway."""
    # Check if telemetry is disabled
    if os.getenv("SCARF_NO_ANALYTICS", "").lower() in ("true", "1", "yes") or os.getenv(
        "DO_NOT_TRACK", ""
    ).lower() in ("true", "1", "yes"):
        return

    try:
        # Build URL with system info (replace spaces with underscores)
        arch = platform.machine().lower().replace(" ", "_")
        os_name = platform.system().lower().replace(" ", "_")
        language = "python"
        url = f"https://sedona.gateway.scarf.sh/sedona-db/{arch}/{os_name}/{language}"

        # Send simple GET request in background
        def _send():
            try:
                # Only allow HTTPS requests for security
                if url.startswith("https://"):
                    urllib.request.urlopen(url, timeout=5)  # nosec B310
            except Exception:  # nosec B110
                pass  # Silently fail

        thread = threading.Thread(target=_send, daemon=True)
        thread.start()

    except Exception:  # nosec B110
        pass  # Silently fail


def track_package_import():
    """Track when the sedona.db package is imported."""
    _send_telemetry()
