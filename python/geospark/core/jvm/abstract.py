from abc import ABC

import attr


@attr.s
class JvmObject(ABC):

    jvm = attr.ib()

    def _create_jvm_instance(self):
        raise NotImplemented("Instance has to implement create_jvm_instance")

    @property
    def jvm_instance(self):
        return self._create_jvm_instance()
