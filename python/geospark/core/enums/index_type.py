from enum import Enum

import attr

from geospark.core.jvm.abstract import JvmObject
from geospark.utils.decorators import require


class IndexType(Enum):

    QUADTREE = "QUADTREE"
    RTREE = "RTREE"

    @classmethod
    def from_string(cls, index: str):
        try:
            index_type = getattr(cls, index)
        except AttributeError:
            raise AttributeError(f"Can not found {index}")
        return index_type


@attr.s
class IndexTypeJvm(JvmObject):

    index_type = attr.ib(type=IndexType)

    def _create_jvm_instance(self):
        return self.jvm_index(self.index_type.value) if self.index_type.value is not None else None

    @property
    @require(["FileDataSplitter"])
    def jvm_index(self):
        return self.jvm.IndexType.getIndexType
