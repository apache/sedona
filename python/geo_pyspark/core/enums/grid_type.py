from enum import Enum

import attr

from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib


class GridType(Enum):

    EQUALGRID = "EQUALGRID"
    HILBERT = "HILBERT"
    RTREE = "RTREE"
    VORONOI = "VORONOI"
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
    @require([GeoSparkLib.GridType])
    def jvm_grid(self):
        return self.jvm.GridType.getGridType
