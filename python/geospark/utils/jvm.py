import attr

from geospark.core.jvm.abstract import JvmObject
from geospark.utils.decorators import require


@attr.s
class JvmStorageLevel(JvmObject):
    storage_level = attr.ib()

    @require(["StorageLevel"])
    def _create_jvm_instance(self):
        return self.jvm.StorageLevel.apply(
            self.storage_level.useDisk, self.storage_level.useMemory,
            self.storage_level.useOffHeap, self.storage_level.deserialized,
            self.storage_level.replication
        )