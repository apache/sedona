import attr

from geospark.core.enums import IndexType
from geospark.core.enums.join_build_side import JoinBuildSide
from geospark.core.jvm.abstract import JvmObject


@attr.s
class JoinParams:
    useIndex = attr.ib(type=bool, default=True)
    indexType = attr.ib(type=str, default=IndexType.RTREE)
    joinBuildSide = attr.ib(type=str, default=JoinBuildSide.LEFT)

    def jvm_instance(self, jvm):
        return JvmJoinParams(jvm, self.useIndex, self.indexType, self.joinBuildSide).jvm_instance


@attr.s
class JvmJoinParams(JvmObject):
    useIndex = attr.ib(type=bool, default=True)
    indexType = attr.ib(type=str, default=IndexType.RTREE)
    joinBuildSide = attr.ib(type=str, default=JoinBuildSide.LEFT)

    def _create_jvm_instance(self):
        return self.jvm_reference(self.useIndex, self.indexType.value, self.joinBuildSide)

    @property
    def jvm_reference(self):
        return self.jvm.JoinParams.createJoinParams
