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


class Box3D:
    """Planar 3D bounding box. Always a valid finite bbox; absence of a bbox
    is represented by ``None`` (SQL NULL) at the column level rather than by an
    in-band sentinel. Matches PostGIS ``box3d`` semantics. Geometries without a
    Z dimension contribute ``z = 0``; inverted bounds (``xmin > xmax`` etc.)
    are rejected by the Box3D predicates since Z has no wraparound
    convention."""

    __slots__ = ("xmin", "ymin", "zmin", "xmax", "ymax", "zmax")

    def __init__(
        self,
        xmin: float,
        ymin: float,
        zmin: float,
        xmax: float,
        ymax: float,
        zmax: float,
    ):
        self.xmin = float(xmin)
        self.ymin = float(ymin)
        self.zmin = float(zmin)
        self.xmax = float(xmax)
        self.ymax = float(ymax)
        self.zmax = float(zmax)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Box3D):
            return NotImplemented
        return (
            self.xmin == other.xmin
            and self.ymin == other.ymin
            and self.zmin == other.zmin
            and self.xmax == other.xmax
            and self.ymax == other.ymax
            and self.zmax == other.zmax
        )

    def __hash__(self) -> int:
        return hash((self.xmin, self.ymin, self.zmin, self.xmax, self.ymax, self.zmax))

    def __repr__(self) -> str:
        return (
            f"Box3D({self.xmin}, {self.ymin}, {self.zmin}, "
            f"{self.xmax}, {self.ymax}, {self.zmax})"
        )
