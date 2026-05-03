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


class Box2D:
    """Planar 2D bounding box. Always a valid finite bbox; absence of a bbox
    is represented by ``None`` (SQL NULL) at the column level rather than by an
    in-band sentinel. This matches PostGIS behavior and leaves ``xmin > xmax``
    free for a future antimeridian-wraparound semantics on geography bboxes."""

    __slots__ = ("xmin", "ymin", "xmax", "ymax")

    def __init__(self, xmin: float, ymin: float, xmax: float, ymax: float):
        self.xmin = float(xmin)
        self.ymin = float(ymin)
        self.xmax = float(xmax)
        self.ymax = float(ymax)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Box2D):
            return NotImplemented
        return (
            self.xmin == other.xmin
            and self.ymin == other.ymin
            and self.xmax == other.xmax
            and self.ymax == other.ymax
        )

    def __hash__(self) -> int:
        return hash((self.xmin, self.ymin, self.xmax, self.ymax))

    def __repr__(self) -> str:
        return f"Box2D({self.xmin}, {self.ymin}, {self.xmax}, {self.ymax})"
