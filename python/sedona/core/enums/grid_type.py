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

import attr

from sedona.core.jvm.abstract import JvmObject
from sedona.utils.decorators import require


class GridType(Enum):
    QUADTREE = "QUADTREE"
    KDBTREE = "KDBTREE"

    @classmethod
    def from_str(cls, grid: str) -> 'GridType':
        try:
            grid = getattr(cls, grid.upper())
        except AttributeError:
            raise AttributeError(f"{cls.__class__.__name__} has no {grid} attribute")
        return grid


@attr.s
class GridTypeJvm(JvmObject):
    grid = attr.ib(type=GridType)

    def _create_jvm_instance(self):
        return self.jvm_grid(self.grid.value) if self.grid.value is not None else None

    @property
    @require(["GridType"])
    def jvm_grid(self):
        return self.jvm.GridType.getGridType
