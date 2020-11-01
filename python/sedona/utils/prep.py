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

from typing import List

from shapely.geometry import Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString, GeometryCollection
from shapely.geometry.base import BaseGeometry


def assign_all() -> bool:
    geoms = [Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString, GeometryCollection]
    assign_udt_shapely_objects(geoms=geoms)
    assign_user_data_to_shapely_objects(geoms=geoms)
    return True


def assign_udt_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    from sedona.sql.types import GeometryType
    for geom in geoms:
        geom.__UDT__ = GeometryType()
    return True


def assign_user_data_to_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    for geom in geoms:
        geom.getUserData = lambda geom_instance: geom_instance.userData
