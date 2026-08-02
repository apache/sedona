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

"""CRS metadata helpers for the distributed GeoPandas API."""

from __future__ import annotations

from typing import Any

from pyproj import CRS
from pyspark.pandas.internal import InternalField

CRS_METADATA_KEY = "sedona.geopandas.crs_wkt"
NO_CRS_OVERRIDE = object()


def read_crs_metadata(field: InternalField) -> tuple[bool, CRS | None]:
    """Return whether CRS metadata is present and its decoded value."""
    metadata = field.metadata or {}
    if CRS_METADATA_KEY not in metadata:
        return False, None

    value = metadata[CRS_METADATA_KEY]
    return True, CRS.from_wkt(value) if value else None


def with_crs_metadata(field: InternalField, crs: Any | None) -> InternalField:
    """Return an InternalField carrying normalized CRS metadata."""
    metadata = dict(field.metadata or {})
    metadata[CRS_METADATA_KEY] = (
        CRS.from_user_input(crs).to_wkt() if crs is not None else ""
    )
    return field.copy(metadata=metadata)


def copy_crs_metadata(
    source: InternalField,
    target: InternalField,
) -> InternalField:
    """Copy only Sedona CRS metadata while retaining all target metadata."""
    source_metadata = source.metadata or {}
    target_metadata = dict(target.metadata or {})
    if CRS_METADATA_KEY in source_metadata:
        target_metadata[CRS_METADATA_KEY] = source_metadata[CRS_METADATA_KEY]
    else:
        target_metadata.pop(CRS_METADATA_KEY, None)
    return target.copy(metadata=target_metadata)
