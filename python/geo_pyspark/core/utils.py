import attr
from pyspark import StorageLevel

from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.decorators import require


class ImportedJvmLib:
    _imported_libs = []

    @classmethod
    def has_library(cls, library: GeoSparkLib) -> bool:
        return library in cls._imported_libs

    @classmethod
    def import_lib(cls, library: str) -> bool:
        if library not in cls._imported_libs:
            cls._imported_libs.append(library)
        else:
            return False
        return True


@attr.s
class JvmStorageLevel(JvmObject):
    storage_level = attr.ib(type=StorageLevel)

    @require([GeoSparkLib.StorageLevel])
    def _create_jvm_instance(self):
        return self.jvm.StorageLevel.apply(
            self.storage_level.useDisk, self.storage_level.useMemory,
            self.storage_level.useOffHeap, self.storage_level.deserialized,
            self.storage_level.replication
        )

