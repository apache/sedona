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

from enum import Enum


class SpatialType(Enum):
    LINESTRING = "LINESTRING"
    POLYGON = "POLYGON"
    RECTANGLE = "RECTANGLE"
    POINT = "POINT"
    SPATIAL = "SPATIAL"
    CIRCLE = "CIRCLE"

    @classmethod
    def from_str(cls, spatial: str) -> 'SpatialType':
        try:
            spatial = getattr(cls, spatial.upper())
        except AttributeError:
            raise AttributeError(f"{cls.__class__.__name__} has no {spatial} attribute")
        return spatial
