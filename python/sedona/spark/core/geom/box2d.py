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

import math


class Box2D:
    """Planar 2D bounding box. Empty boxes are encoded as ``xmin > xmax`` or
    ``ymin > ymax`` (JTS Envelope convention)."""

    __slots__ = ("xmin", "ymin", "xmax", "ymax")

    def __init__(self, xmin: float, ymin: float, xmax: float, ymax: float):
        self.xmin = float(xmin)
        self.ymin = float(ymin)
        self.xmax = float(xmax)
        self.ymax = float(ymax)

    @classmethod
    def empty(cls) -> "Box2D":
        return cls(math.inf, math.inf, -math.inf, -math.inf)

    def is_empty(self) -> bool:
        return self.xmin > self.xmax or self.ymin > self.ymax

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Box2D):
            return NotImplemented
        if self.is_empty() and other.is_empty():
            return True
        return (
            self.xmin == other.xmin
            and self.ymin == other.ymin
            and self.xmax == other.xmax
            and self.ymax == other.ymax
        )

    def __hash__(self) -> int:
        if self.is_empty():
            return 0
        return hash((self.xmin, self.ymin, self.xmax, self.ymax))

    def __repr__(self) -> str:
        if self.is_empty():
            return "Box2D.empty()"
        return f"Box2D({self.xmin}, {self.ymin}, {self.xmax}, {self.ymax})"
